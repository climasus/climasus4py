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
    """Create derived variables from SUS data (stays lazy).

    Appends computed columns to the relation without materialising it.
    All new columns are calculated via DuckDB SQL expressions and added
    to the existing SELECT projection.

    Args:
        rel: Lazy DuckDB relation from a previous pipeline step.
        age_group: Age grouping scheme to apply to the age column.
            Accepted values: ``"who"`` (WHO standard bands),
            ``"decadal"`` (0–9, 10–19, …), a custom list of integer
            breakpoints, or ``None`` to skip. Adds an ``age_group``
            column.
        epi_week: If ``True``, add an ``epi_week`` column formatted as
            ``YYYY-Www``.
        season: If ``True``, add a ``season`` column (Southern Hemisphere
            seasons: Summer/Autumn/Winter/Spring).
        quarter: If ``True``, add a ``quarter`` column
            (``"Q1"`` … ``"Q4"``).
        month_name: If ``True``, add a ``month_name`` column (English
            month names derived from the detected date column).
        day_of_week: If ``True``, add a ``day_of_week`` column (English
            day names).

    Returns:
        Lazy DuckDB relation with the requested derived columns appended
        after the original columns.

    Example:
        >>> with_vars = sus_variables(rel, age_group="who", season=True)
        >>> with_vars.columns
        [..., 'age_group', 'season']
        >>> sus_variables(rel, epi_week=True, quarter=True)
    """
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
