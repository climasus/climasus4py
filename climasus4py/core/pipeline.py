"""Main pipeline — orchestrates the full ETL chain.

Mirrors R: pipeline.R + pipeline-fast.R
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..io.export import sus_export
from ..utils.data import detect_geo_column
from ._sql import sql_string
from .aggregate import sus_data_aggregate
from .clean import sus_data_clean_encoding
from .engine import get_connection
from .filter import sus_filter
from .importer import sus_data_import
from .standardize import sus_data_standardize
from .variables import _age_breaks_for_preset, sus_data_create_variables

# ---------------------------------------------------------------------------
# Fast path helpers (mirrors R pipeline-fast.R)
# ---------------------------------------------------------------------------

#: Time units the fast path can serve, mapped to the SAME truncation
#: ``sus_data_aggregate`` uses (D3).
#:
#: This used to hold its own expressions, and they produced a *label*
#: rather than a date: ``STRFTIME(__date, '%Y-%m')`` gave the string
#: ``"2023-01"`` where the staged path gave the DATE ``2023-01-01``. A
#: fourth difference between the two paths, and one M52 did not list —
#: measured after the other three were closed. It matters downstream:
#: date arithmetic, resampling and plotting all work on one and not the
#: other.
#:
#: The staged path is the one that matches R, whose ``agg_date`` comes
#: from ``lubridate::floor_date()`` and is a Date. Reusing
#: ``AGG_TIME_EXPRS`` rather than writing a third copy of the truncation
#: also inherits its Sunday-week fix (M95), which a copy would have
#: missed.
def _time_expr(time: str) -> str | None:
    """The truncation for *time*, in terms of the ``__date`` alias."""
    from .aggregate import AGG_TIME_EXPRS

    modelo = AGG_TIME_EXPRS.get(time)
    if modelo is None:
        return None
    return modelo.replace("{date}", "__date")


#: Which units the fast path handles. A unit ``sus_data_aggregate``
#: supports but that needs more than a truncation (``season``) is left to
#: the staged path.
_FAST_TIME_UNITS = ("year", "quarter", "month", "week", "day",
                    "5 days", "14 days")


def _can_fast_path(
    age_group: str | list[int] | None,
    epi_week: bool,
    time: str,
    geo: str,
) -> bool:
    """Check if fast path is usable (same constraints as R)."""
    if age_group is not None or epi_week:
        return False
    if time not in _FAST_TIME_UNITS:
        return False
    return geo in ("state", "municipality")


def _date_parse_sql(col: str) -> str:
    """Multi-format date parsing SQL (mirrors R _duckdb_try_date_expr)."""
    v = f'CAST("{col}" AS VARCHAR)'
    return (
        f"CASE"
        f"  WHEN LENGTH({v}) = 8 AND STRPOS({v}, '-') = 0 AND STRPOS({v}, '/') = 0"
        f"    THEN TRY_STRPTIME({v}, '%d%m%Y')"
        # ISO order is unambiguous, so cast instead of matching a fixed
        # pattern: DATE and TIMESTAMP columns stringify with a trailing
        # time ("2023-01-01 00:00:00") that '%Y-%m-%d' rejects, which
        # turned every already-typed date column into NULL.
        f"  WHEN STRPOS({v}, '-') = 5"
        f"    THEN TRY_CAST({v} AS TIMESTAMP)"
        f"  WHEN STRPOS({v}, '/') = 3"
        f"    THEN TRY_STRPTIME({v}, '%d/%m/%Y')"
        f"  ELSE TRY_CAST({v} AS DATE)"
        f" END"
    )


#: Key columns the fast path deduplicates on — the same candidates
#: ``sus_data_clean_encoding`` uses, so the two paths remove the same rows.
#:
#: Removes nothing on the data measured (SIM-DO SP 2023 has 334,303
#: distinct CONTADOR in 334,303 rows). Applied anyway because the staged
#: path applies it, and a total that depends on which path ran is the
#: defect being fixed — not because it is known to matter here.
_DEDUP_KEYS = ("CONTADOR", "NUMERODO", "NUMERODN", "N_AIH", "NU_NOTIFIC",
               "counter", "record_id")

#: The plausibility range ``sus_data_clean_encoding`` applies by default,
#: and therefore what the staged path applies whether or not the caller
#: asked for an age filter.
_AGE_PLAUSIBLE = (0, 120)


def _build_fast_sql(
    parquet_paths: list[Path],
    groups: list[str] | None,
    age_min: int | None,
    age_max: int | None,
    time: str,
    geo: str,
    system: str | None = None,
    lang: str = "en",
) -> str | None:
    """Build a single CTE query that does filter+aggregate in one shot.

    Emits the **same schema, the same geographic cut and the same totals**
    as the staged path since 21/09/2026 (D3). The three used to differ,
    all three measured on SIM-DO SP 2023:

    * schema — ``time_group``/``state``/``count`` against
      ``date``/``occurrence_municipality_code``/``n_deaths``, so anything
      downstream broke the moment the pipeline fell back;
    * geography — this path derived the state from the *residence*
      municipality while the staged path used *occurrence*;
    * totals — 334,303 against 333,968. M52 recorded that gap as what
      deduplication removes; it is not. ``CONTADOR`` has 334,303 distinct
      values in 334,303 rows on that data, so dedup removes nothing, and
      the 335 rows come entirely from ``sus_data_clean_encoding``'s
      default ``age_range=(0, 120)``. Both are applied here now, dedup
      because the staged path applies it and not because it is known to
      bite.

    The staged path is the one that matches R, which is why it is the
    target rather than the other way round. R's
    ``.data_aggregate_tibble_internal`` renames its date column to
    ``date``, names the count with
    ``get_smart_column_name(system, "count", lang)`` — ``n_obitos`` in pt,
    ``n_deaths`` in en, ``n_muertes`` in es — and picks the geographic
    column from a **per-system** priority: ``system_priority$SIM`` is
    ``c("ocorrencia", "residencia", ...)``, so a mortality series is
    aggregated by place of occurrence.

    Returns the SQL string, or None if required columns are missing.
    """
    from ..utils.cid import codes_for_groups
    from ..utils.data import (
        detect_age_column,
        detect_cause_column,
        detect_date_column,
    )
    from .aggregate import _agg_detect_geo_col, _agg_smart_name
    from .standardize import _load_column_dict

    conn = get_connection()

    # Read schema from first parquet to detect columns
    test_rel = conn.read_parquet(str(parquet_paths[0]))
    columns = test_rel.columns

    date_col = detect_date_column(columns)
    if not date_col:
        return None

    # The SAME per-system priority the staged path uses, so the two agree
    # on the epidemiological cut and not merely on a column name.
    muni_col = _agg_detect_geo_col(list(columns), system)

    if geo == "state":
        # No staged equivalent: `sus_data_aggregate` has no `geo`, matching
        # R, so a state level exists only here. Derived from whichever
        # municipality column the staged path would have used, so at least
        # the CUT agrees even though the level cannot.
        if not muni_col:
            return None
        geo_alias = "state"
        geo_sql = f'SUBSTR(CAST("{muni_col}" AS VARCHAR), 1, 2)'
    else:
        if not muni_col:
            return None
        # Named as the staged path names it: the translated name, because
        # the staged path standardises before aggregating and this path
        # reads raw parquet.
        traduzidos = _load_column_dict(lang, system=system)
        geo_alias = traduzidos.get(muni_col, muni_col)
        geo_sql = f'CAST("{muni_col}" AS VARCHAR)'

    # --- Build SELECT for base CTE (only needed columns) ---
    select_parts = [f'{_date_parse_sql(date_col)} AS __date']
    select_parts.append(f'{geo_sql} AS "{geo_alias}"')

    where_parts = ["__date IS NOT NULL"]

    # Disease filter — fast path returns ``None`` if the CID prefix list is too
    # large to embed inline; the caller then falls back to the staged pipeline.
    # Previously the list was silently truncated to the first 200 prefixes,
    # which produced results inconsistent with the staged pipeline.
    if groups:
        cause_col = detect_cause_column(columns)
        if cause_col:
            codes = codes_for_groups(groups)
            prefixes = sorted({c[:3] for c in codes})
            if len(prefixes) > 200:
                # Defer to the staged pipeline which uses a SEMI JOIN for
                # large code lists (see filter.py).
                return None
            select_parts.append(f'SUBSTR(CAST("{cause_col}" AS VARCHAR), 1, 3) AS __cid')
            codes_str = ", ".join(sql_string(c) for c in prefixes)
            where_parts.append(f"__cid IN ({codes_str})")

    # Age: the caller's filter, and the plausibility range the staged path
    # applies whether or not the caller asked for one.
    #
    # THIS is what made the totals differ, and not deduplication as M52
    # recorded. Measured on SIM-DO SP 2023: CONTADOR has 334,303 distinct
    # values in 334,303 rows, so dedup removes NOTHING there; the 335-row
    # gap comes entirely from `sus_data_clean_encoding`'s default
    # `age_range=(0, 120)` dropping records whose decoded age falls
    # outside it. Checked by running the clean step with dedup and the
    # range switched on one at a time.
    age_col = detect_age_column(columns)
    if age_col:
        from ..utils.data import decode_age_sql
        decoded = decode_age_sql(age_col)
        select_parts.append(f'({decoded}) AS __age')
        # Nulls survive: the staged path's range filter drops a row whose
        # age is out of range, not one whose age is unknown.
        where_parts.append(
            f"(__age IS NULL OR (__age >= {_AGE_PLAUSIBLE[0]} "
            f"AND __age <= {_AGE_PLAUSIBLE[1]}))"
        )
        if age_min is not None:
            where_parts.append(f"__age >= {int(age_min)}")
        if age_max is not None:
            where_parts.append(f"__age <= {int(age_max)}")

    # --- Assemble ---
    paths_sql = ", ".join(
        sql_string(str(p).replace("\\", "/")) for p in parquet_paths
    )
    source = f"read_parquet([{paths_sql}], union_by_name=True)"

    time_sql = _time_expr(time)
    if time_sql is None:
        return None
    conta = _agg_smart_name(system, "count", lang)

    # Deduplication, on the same keys `sus_data_clean_encoding` uses. The
    # staged path deduplicates because it runs that function; this path,
    # being one CTE over the parquet, did not — 335 rows of difference on
    # SIM-DO SP 2023. Costs a window function, and a total that depends on
    # which path happened to run costs more.
    chaves = [c for c in _DEDUP_KEYS if c in columns]
    if chaves:
        particao = ", ".join(f'"{c}"' for c in chaves)
        select_parts.append(
            f"ROW_NUMBER() OVER (PARTITION BY {particao} "
            f"ORDER BY {particao}) AS __rn"
        )
        where_parts.append("__rn = 1")

    sql = (
        f"WITH base AS ("
        f"  SELECT {', '.join(select_parts)}"
        f"  FROM {source}"
        f") "
        f'SELECT {time_sql} AS date, "{geo_alias}", COUNT(*) AS "{conta}" '
        f"FROM base "
        f"WHERE {' AND '.join(where_parts)} "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    )
    return sql


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def sus_pipeline(
    system: str,
    uf: str | list[str],
    year: int | list[int],
    *,
    lang: str = "en",
    groups: str | list[str] | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    age_group: str | list[int] | None = None,
    time: str = "month",
    geo: str = "state",
    epi_week: bool = False,
    output: str | Path | None = None,
    overwrite: bool = False,
    cache_dir: str | Path = Path("dados/cache"),
    verbose: bool = True,
    **kwargs: Any,
) -> duckdb.DuckDBPyRelation | pd.DataFrame:
    """Run the full SUS ETL pipeline: import → clean → standardise → filter → variables → aggregate.

    Main entry point for most users. **Python-only**: there is no
    ``sus_pipeline()`` in ``climasus4r`` — the R user chains the stage
    functions by hand. (This docstring used to claim it mirrored an R
    function of the same name; it does not exist there, and saying so sent
    anyone comparing the two packages looking for it.) It uses a
    single-CTE SQL fast path when the requested operations allow it, and
    falls back to the staged pipeline for complex operations such as
    custom age groups or epidemiological-week breakdowns.

    The two paths produce the same table:
        Which one runs is decided for you, from the arguments, so they
        had better agree. Until 21/09/2026 they did not, in four measured
        ways (M52, and the fourth was not in the record). The staged path
        was the one matching R, so it is what the fast path was brought
        onto — D3.

        * **Schema.** Both return ``date`` / the geographic column /
          the count named per system and language: ``n_obitos`` in pt,
          ``n_deaths`` in en, ``n_muertes`` in es, from R's
          ``get_smart_column_name``. The fast path used to return
          ``time_group`` / ``state`` / ``count``.
        * **Geography.** Both use the per-system priority
          ``sus_data_aggregate`` uses, which for SIM starts at
          *occurrence* — R's ``system_priority$SIM`` is
          ``c("ocorrencia", "residencia", ...)``. The fast path used to
          derive the state from the *residence* municipality: a different
          epidemiological cut, not a different spelling.
        * **Totals.** Both return 334,303 on SIM-DO SP 2023. The staged
          path used to return 333,968, and the 335-row gap was **not**
          what M52 recorded. ``CONTADOR`` has no duplicates in that
          file, so deduplication removes nothing; the rows were lost
          because ``clean.py`` carried its own copy of the age decoder,
          which read the *unknown-age* sentinel ``999`` as 999 years and
          dropped the record. See M119.
        * **Date type.** Both return a real ``DATE``. The fast path used
          to return the label ``"2023-01"``, which no date arithmetic
          accepts.

        Verified cell by cell for year, month, week and day. What did
        **not** converge is ``geo="state"``: ``sus_data_aggregate`` has no
        ``geo``, matching R, so that level exists only on the fast path
        and the function still warns when a fallback cannot honour it.
        The *cut* agrees even there — the state digits come from the same
        municipality column the staged path would have used.

    Args:
        system: SUS system identifier, e.g. ``"SIM-DO"`` or
            ``"SINASC"``.
        uf: State abbreviation(s), ``"all"``, or a region name.
        year: Year(s) to process, e.g. ``2022`` or
            ``[2020, 2021, 2022]``.
        lang: Output language for column names \u2014 ``"en"`` (default),
            ``"pt"``, or ``"es"``.
        groups: Disease group name(s) or ``None`` to include all causes.
        age_min: Minimum age in years to retain.
        age_max: Maximum age in years to retain.
        age_group: Age grouping scheme \u2014 ``"who"``, ``"decadal"``,
            a custom list of breakpoints, or ``None`` to skip.
        time: Temporal aggregation granularity \u2014 ``"year"``,
            ``"quarter"``, ``"month"`` (default), ``"week"``, or
            ``"day"``.
        geo: Geographic aggregation level \u2014 ``"state"`` (default) or
            ``"municipality"``.
        epi_week: If ``True``, add an ``epi_week`` column (disables fast
            path).
        output: Optional file path to export results
            (parquet / csv / xlsx).
        overwrite: Whether *output* may replace an existing file.
            Defaults to ``False``, so re-running with the same *output*
            raises ``FileExistsError`` rather than destroying the
            previous result. Forwarded to :func:`sus_export`, which used
            to default to replacing silently — see M17.
        cache_dir: Root directory for the Parquet cache.
        verbose: Print progress messages via Rich.
        **kwargs: Additional keyword arguments forwarded to
            :func:`~climasus.core.importer.sus_data_import`.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` with aggregated results, or a
        ``pandas.DataFrame`` when *output* forces materialisation.

    Example:
        >>> import climasus4py as cs
        >>> result = cs.sus_pipeline("SIM-DO", "SP", 2022,
        ...                          groups="respiratory", time="month")
        >>> result.df().head()
        >>> cs.sus_pipeline("SIM-DO", ["SP", "RJ"], [2020, 2021],
        ...                  output="output/mortality.parquet")
    """
    group_list = [groups] if isinstance(groups, str) else groups

    # Step 1: Import (always needed — resolves UFs, discovers/downloads parquets)
    rel = sus_data_import(system, uf, year, cache_dir=cache_dir, verbose=verbose, **kwargs)

    # ``sus_data_import`` documents returning None when nothing is available.
    # Nothing here used to check, so the None travelled on and failed deep in
    # the fast path or in sus_data_clean_encoding — an error that says nothing
    # about the actual problem, which is that this system/UF/year has no data.
    if rel is None:
        raise RuntimeError(
            f"No data imported for system={system!r}, uf={uf!r}, year={year!r}. "
            f"Check that the year is published for this system and that the "
            f"UF code is valid, or inspect the download with "
            f"sus_data_import(...) directly."
        )

    # --- Try fast path: single CTE query like R rc_a ---
    if _can_fast_path(age_group, epi_week, time, geo):
        # Resolve parquet paths from cache
        from ..utils.data import resolve_uf

        ufs = resolve_uf(uf)
        years = [year] if isinstance(year, int) else list(year)
        cache_path = Path(cache_dir)
        parquet_paths = [
            cache_path / system / f"{u}_{y}_all.parquet"
            for u in ufs
            for y in years
        ]
        parquet_paths = [p for p in parquet_paths if p.is_file()]

        if parquet_paths:
            sql = _build_fast_sql(
                parquet_paths, group_list, age_min, age_max, time, geo,
                system=system, lang=lang,
            )
            if sql:
                conn = get_connection()
                try:
                    result = conn.sql(sql)
                    if output:
                        sus_export(result, output, overwrite=overwrite)
                    return result
                except Exception as exc:
                    # Fast path failed — say what the fallback actually
                    # changes. This warning used to end with "Results should
                    # be equivalent but slower", which is false in three
                    # measured ways (M52). Promising equivalence here is
                    # worse than no warning: the caller stops checking.
                    warnings.warn(
                        f"sus_pipeline: fast path failed ({exc!r}); falling "
                        "back to the staged pipeline. The two paths now "
                        "return the SAME table — schema, geographic cut, "
                        "totals and the date column's type — verified "
                        "cell by cell on SIM-DO SP 2023 for year, month, "
                        "week and day (D3). The one thing the staged path "
                        "cannot do is geo='state': sus_data_aggregate has "
                        "no geo, matching R, so the fallback aggregates by "
                        "municipality and warns separately about that.",
                        UserWarning,
                        stacklevel=2,
                    )

    # --- Staged pipeline (fallback) ---
    rel = sus_data_clean_encoding(rel)
    rel = sus_data_standardize(rel, lang=lang, system=system)
    rel = sus_filter(rel, groups=group_list, age_min=age_min, age_max=age_max)
    # The staged stages take different argument names than the fast path.
    # ``sus_data_create_variables`` expects ``age_breaks`` (a list of cut
    # points) or a preset name resolved to one — there is no ``age_group``
    # argument — and the epidemiological week is emitted by
    # ``create_calendar_vars`` (on by default), so ``epi_week`` needs no flag.
    var_kwargs: dict[str, Any] = {"lang": lang, "verbose": verbose}
    if age_group is not None:
        var_kwargs["age_breaks"] = (
            _age_breaks_for_preset(age_group)
            if isinstance(age_group, str)
            else list(age_group)
        )
    rel = sus_data_create_variables(rel, **var_kwargs)

    # ``sus_data_aggregate`` takes ``time_unit``, not ``time``, and has no
    # ``geo`` argument at all: it detects the geographic column itself from the
    # data (helped by ``system``). The staged path therefore cannot force the
    # aggregation level the way the fast path does, so check afterwards and say
    # so instead of returning a differently-grouped table in silence.
    rel = sus_data_aggregate(
        rel, time_unit=time, system=system, lang=lang, verbose=verbose
    )
    if not detect_geo_column(list(rel.columns), level=geo):
        warnings.warn(
            f"sus_pipeline: the staged pipeline could not honour geo={geo!r}. "
            f"It aggregated by the geographic column detected in the data, and "
            f"the result carries no column at the requested level "
            f"(columns: {list(rel.columns)}). The fast path derives the state "
            f"code from the municipality code, but the staged path has no such "
            f"step. Aggregate the requested level yourself with "
            f"sus_data_aggregate(group_by=[...]) if it matters.",
            UserWarning,
            stacklevel=2,
        )

    if output:
        sus_export(rel, output, overwrite=overwrite)

    return rel
