"""Aggregation — group by time/geography and summarise.

Mirrors R: aggregate.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.data import detect_date_column, detect_geo_column


def sus_aggregate(
    rel: duckdb.DuckDBPyRelation,
    *,
    time: str = "month",
    geo: str = "state",
    extra_groups: list[str] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Aggregate SUS data by time period and geography.

    Groups the relation by a temporal expression and a geographic level,
    counting rows and summarising recognised numeric columns. The relation
    stays lazy until materialised.

    Args:
        rel: Lazy DuckDB relation produced by a previous pipeline step.
        time: Temporal granularity — one of ``"year"``, ``"quarter"``,
            ``"month"``, ``"week"``, or ``"day"``.
        geo: Geographic aggregation level — one of ``"state"``,
            ``"municipality"``, ``"region"``, or ``"country"``.
        extra_groups: Additional column names to include in the GROUP BY
            clause, e.g. ``["sex", "age_group"]``.

    Returns:
        Lazy DuckDB relation with columns: ``time_group``, the resolved
        geo column, any extra group columns, ``count``, and
        ``sum_*/mean_*`` for any recognised numeric columns present.

    Example:
        >>> import climasus as cs
        >>> agg = cs.sus_aggregate(rel, time="year", geo="state")
        >>> agg.df().head()
        >>> agg_sex = cs.sus_aggregate(rel, time="month",
        ...                            geo="municipality",
        ...                            extra_groups=["sex"])
    """
    columns = schema_columns(rel)
    conn = get_connection()

    group_cols: list[str] = []

    # --- Time grouping ---
    date_col = detect_date_column(columns)
    if date_col:
        date_cast = f'TRY_CAST("{date_col}" AS DATE)'
        time_expr = {
            "year": f"EXTRACT(YEAR FROM {date_cast})",
            "quarter": f"EXTRACT(YEAR FROM {date_cast}) || '-Q' || EXTRACT(QUARTER FROM {date_cast})",
            "month": f"STRFTIME({date_cast}, '%Y-%m')",
            "week": f"STRFTIME({date_cast}, '%Y-W%W')",
            "day": f"CAST({date_cast} AS VARCHAR)",
        }.get(time, f"STRFTIME({date_cast}, '%Y-%m')")

        time_alias = "time_group"
        group_cols.append(time_alias)
    else:
        time_expr = None
        time_alias = None

    # --- Geo grouping ---
    geo_col = detect_geo_column(columns, level=geo)
    if geo_col:
        group_cols.append(f'"{geo_col}"')

    # --- Extra groups ---
    if extra_groups:
        for eg in extra_groups:
            if eg in columns:
                group_cols.append(f'"{eg}"')

    if not group_cols:
        # No grouping possible — return count
        return conn.sql("SELECT COUNT(*) AS count FROM rel")

    # Build SELECT
    select_parts: list[str] = []
    if time_expr:
        select_parts.append(f"{time_expr} AS {time_alias}")
    if geo_col:
        select_parts.append(f'"{geo_col}"')
    if extra_groups:
        for eg in extra_groups:
            if eg in columns:
                select_parts.append(f'"{eg}"')

    select_parts.append("COUNT(*) AS count")

    # Numeric columns → SUM and MEAN
    numeric_candidates = ["count", "deaths", "admissions", "cases", "value"]
    for nc in numeric_candidates:
        if nc in columns and nc not in [g.strip('"') for g in group_cols]:
            select_parts.append(f'SUM(TRY_CAST("{nc}" AS DOUBLE)) AS sum_{nc}')
            select_parts.append(f'AVG(TRY_CAST("{nc}" AS DOUBLE)) AS mean_{nc}')

    group_by = ", ".join(group_cols)
    select = ", ".join(select_parts)

    return conn.sql(f"SELECT {select} FROM rel GROUP BY {group_by} ORDER BY {group_by}")
