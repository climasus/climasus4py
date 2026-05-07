"""Derived variable creation - age groups, epi weeks, seasons.

Mirrors R: variables.R
"""

from __future__ import annotations

import duckdb

from ..utils.data import decode_age_sql, detect_age_column, detect_date_column, load_json
from .engine import get_connection, schema_columns

_DEFAULT_AGE_PRESETS: dict[str, dict[str, list[int | None]]] = {
    "who": {
        "breaks": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, None]
    },
    "decadal": {"breaks": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, None]},
    "epidemiological_default": {"breaks": [0, 5, 15, 60, None]},
}

_DEFAULT_SEASONAL_PATTERNS: dict[str, object] = {
    "default": "south",
    "patterns": {
        "south": {
            "summer": [12, 1, 2],
            "autumn": [3, 4, 5],
            "winter": [6, 7, 8],
            "spring": [9, 10, 11],
        },
        "north": {
            "winter": [12, 1, 2],
            "spring": [3, 4, 5],
            "summer": [6, 7, 8],
            "autumn": [9, 10, 11],
        },
    },
}


def _age_groups_config() -> dict[str, object]:
    """Load age-group presets from climasus-data with fallback defaults."""
    try:
        data = load_json("templates/age_groups.json")
        if isinstance(data, dict) and "presets" in data:
            return data
    except FileNotFoundError:
        pass
    return {"default": "epidemiological_default", "presets": _DEFAULT_AGE_PRESETS}


def _seasonal_patterns_config() -> dict[str, object]:
    """Load seasonal patterns from climasus-data with fallback defaults."""
    try:
        data = load_json("templates/seasonal_patterns.json")
        if isinstance(data, dict) and "patterns" in data:
            return data
    except FileNotFoundError:
        pass
    return _DEFAULT_SEASONAL_PATTERNS


def _age_breaks_for_preset(preset: str) -> list[int]:
    """Load age break points, converting null to 999."""
    cfg = _age_groups_config()
    presets = cfg["presets"]
    if preset not in presets:
        return [0, 18, 65, 999]
    raw = presets[preset]["breaks"]
    return [999 if v is None else int(v) for v in raw]


def _season_case_sql(date_cast: str, hemisphere: str = "south") -> str:
    """Build a DuckDB CASE expression for season."""
    seasonal_patterns = _seasonal_patterns_config()
    patterns = seasonal_patterns["patterns"]
    hemi = hemisphere if hemisphere in patterns else seasonal_patterns["default"]
    season_map = patterns[hemi]

    parts = []
    for season_name, months in season_map.items():
        month_list = ", ".join(str(m) for m in months)
        display_name = str(season_name).capitalize()
        parts.append(f"WHEN EXTRACT(MONTH FROM {date_cast}) IN ({month_list}) THEN '{display_name}'")
    return f"CASE {' '.join(parts)} END"


def sus_variables(
    rel: duckdb.DuckDBPyRelation,
    *,
    age_group: str | list[int] | None = None,
    epi_week: bool = False,
    season: bool = False,
    quarter: bool = False,
    month_name: bool = False,
    day_of_week: bool = False,
    hemisphere: str = "south",
) -> duckdb.DuckDBPyRelation:
    """Create derived variables from SUS data (stays lazy)."""
    columns = schema_columns(rel)
    conn = get_connection()
    projections = [f'"{c}"' for c in columns]

    age_col = detect_age_column(columns)
    date_col = detect_date_column(columns)

    # Guard: temporal variables requested but no date column present
    _temporal_requested = any([epi_week, season, quarter, month_name, day_of_week])
    if _temporal_requested and not date_col:
        raise ValueError(
            "No date column detected in the relation. "
            "Temporal variables (epi_week, season, quarter, month_name, day_of_week) "
            "require a date column."
        )

    if age_col and age_group:
        if isinstance(age_group, list):
            breaks = sorted(age_group + [999])
        else:
            breaks = _age_breaks_for_preset(age_group)

        case_parts = []
        decoded = decode_age_sql(age_col)
        for i in range(len(breaks) - 1):
            lo, hi = breaks[i], breaks[i + 1] - 1
            is_last = i == len(breaks) - 2
            label = f"{lo}+" if is_last else f"{lo}-{hi}"
            case_parts.append(
                f"WHEN ({decoded}) BETWEEN {lo} AND {hi} "
                f"THEN '{label}'"
            )
        case_sql = f'CASE {" ".join(case_parts)} ELSE \'unknown\' END AS "age_group"'
        projections.append(case_sql)

    if date_col:
        date_cast = f'TRY_CAST("{date_col}" AS DATE)'

        if epi_week:
            # SVS (Semana Epidemiológica) — domingo como 1.º dia da semana
            projections.append(
                f"LPAD(CAST(STRFTIME({date_cast}, '%U') AS VARCHAR), 2, '0')"
                f" || '/' || STRFTIME({date_cast}, '%Y') AS epi_week"
            )

        if season:
            projections.append(f"{_season_case_sql(date_cast, hemisphere)} AS season")

        if quarter:
            projections.append(f"'Q' || EXTRACT(QUARTER FROM {date_cast}) AS quarter")

        if month_name:
            projections.append(f"STRFTIME({date_cast}, '%B') AS month_name")

        if day_of_week:
            projections.append(f"STRFTIME({date_cast}, '%A') AS day_of_week")

    rel = conn.sql(f"SELECT {', '.join(projections)} FROM rel")
    return rel
