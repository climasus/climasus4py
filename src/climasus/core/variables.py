"""Derived variable creation — age groups, epi weeks, seasons.

Mirrors R: variables.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.data import detect_age_column, detect_date_column, decode_age_sql


def sus_variables(
    rel: duckdb.DuckDBPyRelation,
    *,
    age_group: str | list[int] | None = None,
    epi_week: bool = False,
    season: bool = False,
    quarter: bool = False,
    month_name: bool = False,
    day_of_week: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Create derived variables from SUS data (stays lazy)."""
    columns = schema_columns(rel)
    conn = get_connection()
    projections = [f'"{c}"' for c in columns]

    # --- Age group ---
    age_col = detect_age_column(columns)
    if age_col and age_group:
        if age_group == "who":
            breaks = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 999]
        elif age_group == "decadal":
            breaks = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 999]
        elif isinstance(age_group, list):
            breaks = sorted(age_group + [999])
        else:
            breaks = [0, 18, 65, 999]

        case_parts = []
        decoded = decode_age_sql(age_col)
        for i in range(len(breaks) - 1):
            lo, hi = breaks[i], breaks[i + 1] - 1
            is_last = (i == len(breaks) - 2)
            label = f"{lo}+" if is_last else f"{lo}-{hi}"
            case_parts.append(
                f'WHEN ({decoded}) BETWEEN {lo} AND {hi} '
                f"THEN '{label}'"
            )
        case_sql = f'CASE {" ".join(case_parts)} ELSE \'unknown\' END AS "age_group"'
        projections.append(case_sql)

    # --- Temporal variables (require a date column) ---
    date_col = detect_date_column(columns)
    if date_col:
        date_cast = f'TRY_CAST("{date_col}" AS DATE)'

        if epi_week:
            projections.append(
                f"STRFTIME({date_cast}, '%Y-W%W') AS epi_week"
            )

        if season:
            projections.append(
                f"CASE "
                f"WHEN EXTRACT(MONTH FROM {date_cast}) IN (12, 1, 2) THEN 'Summer' "
                f"WHEN EXTRACT(MONTH FROM {date_cast}) IN (3, 4, 5) THEN 'Autumn' "
                f"WHEN EXTRACT(MONTH FROM {date_cast}) IN (6, 7, 8) THEN 'Winter' "
                f"ELSE 'Spring' END AS season"
            )

        if quarter:
            projections.append(
                f"'Q' || EXTRACT(QUARTER FROM {date_cast}) AS quarter"
            )

        if month_name:
            projections.append(
                f"STRFTIME({date_cast}, '%B') AS month_name"
            )

        if day_of_week:
            projections.append(
                f"STRFTIME({date_cast}, '%A') AS day_of_week"
            )

    rel = conn.sql(f"SELECT {', '.join(projections)} FROM rel")
    return rel
