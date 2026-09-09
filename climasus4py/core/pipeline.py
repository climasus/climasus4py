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

_TIME_EXPR = {
    "year": "EXTRACT(YEAR FROM __date)",
    "quarter": "EXTRACT(YEAR FROM __date) || '-Q' || EXTRACT(QUARTER FROM __date)",
    "month": "STRFTIME(__date, '%Y-%m')",
    "week": "STRFTIME(__date, '%Y-W%W')",
    "day": "CAST(__date AS VARCHAR)",
}


def _can_fast_path(
    age_group: str | list[int] | None,
    epi_week: bool,
    time: str,
    geo: str,
) -> bool:
    """Check if fast path is usable (same constraints as R)."""
    if age_group is not None or epi_week:
        return False
    if time not in _TIME_EXPR:
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


def _build_fast_sql(
    parquet_paths: list[Path],
    groups: list[str] | None,
    age_min: int | None,
    age_max: int | None,
    time: str,
    geo: str,
) -> str | None:
    """Build a single CTE query that does filter+aggregate in one shot.

    Returns the SQL string, or None if required columns are missing.
    """
    from ..utils.cid import codes_for_groups
    from ..utils.data import (
        detect_age_column,
        detect_cause_column,
        detect_date_column,
        detect_geo_column,
    )

    conn = get_connection()

    # Read schema from first parquet to detect columns
    test_rel = conn.read_parquet(str(parquet_paths[0]))
    columns = test_rel.columns

    date_col = detect_date_column(columns)
    geo_col = detect_geo_column(columns, level=geo)
    if not date_col:
        return None

    # Output column name is always ``geo`` ("state" / "municipality") so the
    # fast path's schema is stable regardless of which DATASUS column was
    # detected in the source — matches the staged pipeline's contract.
    geo_alias = geo
    if not geo_col and geo == "state":
        muni_col = detect_geo_column(columns, level="municipality")
        if muni_col:
            geo_sql = f'SUBSTR(CAST("{muni_col}" AS VARCHAR), 1, 2)'
        else:
            return None
    elif geo_col:
        geo_sql = f'CAST("{geo_col}" AS VARCHAR)'
    else:
        return None

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

    # Age filter
    if age_min is not None or age_max is not None:
        age_col = detect_age_column(columns)
        if age_col:
            from ..utils.data import decode_age_sql
            decoded = decode_age_sql(age_col)
            select_parts.append(f'({decoded}) AS __age')
            if age_min is not None:
                where_parts.append(f"__age >= {int(age_min)}")
            if age_max is not None:
                where_parts.append(f"__age <= {int(age_max)}")

    # --- Assemble ---
    paths_sql = ", ".join(
        sql_string(str(p).replace("\\", "/")) for p in parquet_paths
    )
    source = f"read_parquet([{paths_sql}], union_by_name=True)"

    time_sql = _TIME_EXPR.get(time, "STRFTIME(__date, '%Y-%m')")

    sql = (
        f"WITH base AS ("
        f"  SELECT {', '.join(select_parts)}"
        f"  FROM {source}"
        f") "
        f'SELECT {time_sql} AS time_group, "{geo_alias}", COUNT(*) AS count '
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
    cache_dir: str | Path = Path("dados/cache"),
    verbose: bool = True,
    **kwargs: Any,
) -> duckdb.DuckDBPyRelation | pd.DataFrame:
    """Run the full SUS ETL pipeline: import → clean → standardise → filter → variables → aggregate.

    Main entry point for most users. Mirrors ``sus_pipeline()`` from the
    R package and uses a single-CTE SQL fast path when the requested
    operations allow it (same optimisation as R ``rc_a``). Falls back to
    the staged pipeline for complex operations such as custom age groups
    or epidemiological-week breakdowns.

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
                parquet_paths, group_list, age_min, age_max, time, geo
            )
            if sql:
                conn = get_connection()
                try:
                    result = conn.sql(sql)
                    if output:
                        sus_export(result, output)
                    return result
                except Exception as exc:
                    # Fast path failed — warn the user before falling back so
                    # silent divergence between fast and staged results does
                    # not go unnoticed.
                    warnings.warn(
                        f"sus_pipeline: fast path failed ({exc!r}); "
                        "falling back to the staged pipeline. "
                        "Results should be equivalent but slower.",
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
        sus_export(rel, output)

    return rel
