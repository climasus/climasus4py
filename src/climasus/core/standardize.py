"""Data standardization - column renaming, type conversion, translations.

Mirrors R: standardize.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.data import detect_system, load_json


def _load_column_dict(lang: str) -> dict[str, str]:
    """Load column translation dictionary for the requested language."""
    if lang == "pt":
        return {}

    json_path = f"dictionaries/pt-{lang}/columns.json"
    data = load_json(json_path)

    mapping: dict[str, str] = {}
    for _system, cols in data.items():
        if isinstance(cols, dict):
            mapping.update(cols)
    return mapping


def sus_standardize(
    rel: duckdb.DuckDBPyRelation,
    *,
    lang: str = "en",
    system: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Standardize column names, translate labels, and convert date columns."""
    columns = schema_columns(rel)

    if system is None:
        system = detect_system(columns)

    col_map = _load_column_dict(lang)
    renames = {col: col_map[col] for col in columns if col in col_map}

    if renames:
        projections = []
        for col in columns:
            if col in renames:
                projections.append(f'"{col}" AS "{renames[col]}"')
            else:
                projections.append(f'"{col}"')
        conn = get_connection()
        rel = conn.sql(f"SELECT {', '.join(projections)} FROM rel")

    # Date conversion (DDMMYYYY -> DATE), robust for both VARCHAR and DATE/TIMESTAMP.
    # We avoid TRY/EXCEPT to prevent leaving transaction context aborted in DuckDB.
    new_columns = schema_columns(rel)
    date_candidates = [
        "death_date",
        "date",
        "DTOBITO",
        "DTNASC",
        "admission_date",
        "birth_date",
        "case_conclusion_date",
    ]

    for dc in date_candidates:
        if dc in new_columns:
            rel = rel.project(
                ", ".join(
                    (
                        f"COALESCE("
                        f"TRY_STRPTIME(CAST(\"{dc}\" AS VARCHAR), '%d%m%Y')::DATE, "
                        f"TRY_CAST(\"{dc}\" AS DATE)"
                        f") AS \"{dc}\""
                    )
                    if c == dc
                    else f'"{c}"'
                    for c in new_columns
                )
            )

    rel = rel.set_alias(system or "unknown")
    return rel
