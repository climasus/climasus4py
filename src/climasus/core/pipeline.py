"""Main pipeline — orchestrates the full ETL chain.

Mirrors R: pipeline.R + pipeline-fast.R
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from climasus.core.engine import get_connection, collect
from climasus.core.importer import sus_import
from climasus.core.clean import sus_clean
from climasus.core.standardize import sus_standardize
from climasus.core.filter import sus_filter
from climasus.core.variables import sus_variables
from climasus.core.aggregate import sus_aggregate
from climasus.io.export import sus_export


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
    if geo not in ("state", "municipality"):
        return False
    return True


def _date_parse_sql(col: str) -> str:
    """Multi-format date parsing SQL (mirrors R _duckdb_try_date_expr)."""
    v = f'CAST("{col}" AS VARCHAR)'
    return (
        f"CASE"
        f"  WHEN LENGTH({v}) = 8 AND STRPOS({v}, '-') = 0 AND STRPOS({v}, '/') = 0"
        f"    THEN TRY_STRPTIME({v}, '%d%m%Y')"
        f"  WHEN STRPOS({v}, '-') = 5"
        f"    THEN TRY_STRPTIME({v}, '%Y-%m-%d')"
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
    from climasus.utils.cid import codes_for_groups
    from climasus.utils.data import (
        detect_cause_column,
        detect_date_column,
        detect_geo_column,
        detect_age_column,
    )

    conn = get_connection()

    # Read schema from first parquet to detect columns
    test_rel = conn.read_parquet(str(parquet_paths[0]))
    columns = test_rel.columns

    date_col = detect_date_column(columns)
    geo_col = detect_geo_column(columns, level=geo)
    if not date_col:
        return None

    # If no direct geo column for "state", try deriving from municipality code
    geo_alias = geo  # output column name: "state" or "municipality"
    if not geo_col and geo == "state":
        muni_col = detect_geo_column(columns, level="municipality")
        if muni_col:
            geo_sql = f'SUBSTR(CAST("{muni_col}" AS VARCHAR), 1, 2)'
        else:
            return None
    elif geo_col:
        geo_sql = f'CAST("{geo_col}" AS VARCHAR)'
        geo_alias = geo_col
    else:
        return None

    # --- Build SELECT for base CTE (only needed columns) ---
    select_parts = [f'{_date_parse_sql(date_col)} AS __date']
    select_parts.append(f'{geo_sql} AS "{geo_alias}"')

    where_parts = ["__date IS NOT NULL"]

    # Disease filter
    if groups:
        cause_col = detect_cause_column(columns)
        if cause_col:
            codes = codes_for_groups(groups)
            prefixes = sorted(set(c[:3] for c in codes))
            select_parts.append(f'SUBSTR(CAST("{cause_col}" AS VARCHAR), 1, 3) AS __cid')
            codes_str = ", ".join(f"'{c}'" for c in prefixes[:200])
            where_parts.append(f"__cid IN ({codes_str})")

    # Age filter
    if age_min is not None or age_max is not None:
        age_col = detect_age_column(columns)
        if age_col:
            select_parts.append(f'TRY_CAST("{age_col}" AS DOUBLE) AS __age')
            if age_min is not None:
                where_parts.append(f"__age >= {age_min}")
            if age_max is not None:
                where_parts.append(f"__age <= {age_max}")

    # --- Assemble ---
    paths_sql = ", ".join(f"'{str(p).replace(chr(92), '/')}'" for p in parquet_paths)
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
    **kwargs,
) -> duckdb.DuckDBPyRelation | pd.DataFrame:
    """Full ETL pipeline: import → clean → standardize → filter → variables → aggregate.

    This is the main entry point for users. Mirrors R's sus_pipeline().
    Uses a single-SQL fast path when possible (like R rc_a).
    """
    group_list = [groups] if isinstance(groups, str) else groups

    # Step 1: Import (always needed — resolves UFs, discovers/downloads parquets)
    rel = sus_import(system, uf, year, cache_dir=cache_dir, verbose=verbose, **kwargs)

    # --- Try fast path: single CTE query like R rc_a ---
    if _can_fast_path(age_group, epi_week, time, geo):
        # Resolve parquet paths from cache
        from climasus.utils.data import resolve_uf

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
                except Exception:
                    pass  # Fall through to staged pipeline

    # --- Staged pipeline (fallback) ---
    rel = sus_clean(rel)
    rel = sus_standardize(rel, lang=lang, system=system)
    rel = sus_filter(rel, groups=group_list, age_min=age_min, age_max=age_max)
    rel = sus_variables(rel, age_group=age_group, epi_week=epi_week)
    rel = sus_aggregate(rel, time=time, geo=geo)

    if output:
        sus_export(rel, output)

    return rel
