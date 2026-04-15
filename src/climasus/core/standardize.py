"""Data standardization — column renaming, type conversion, translations.

Mirrors R: standardize.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.data import detect_system, load_json


def _load_column_dict(lang: str) -> dict[str, str]:
    """Load column translation dictionary for the given language pair."""
    if lang == "pt":
        return {}  # No translation needed
    json_path = f"dictionaries/pt-{lang}/columns.json"
    data = load_json(json_path)
    # Flatten: system → {original: translated}
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
    """Standardize column names, translate labels, convert types.

    Parameters
    ----------
    rel : DuckDB relation (lazy)
    lang : Target language for column names ("en", "es", "pt")
    system : SUS system name. Auto-detected if None.
    """
    columns = schema_columns(rel)

    # Auto-detect system
    if system is None:
        system = detect_system(columns)

    # Translate column names
    col_map = _load_column_dict(lang)
    renames = {}
    for col in columns:
        if col in col_map:
            renames[col] = col_map[col]

    if renames:
        projections = []
        for col in columns:
            if col in renames:
                projections.append(f'"{col}" AS "{renames[col]}"')
            else:
                projections.append(f'"{col}"')
        conn = get_connection()
        rel = conn.sql(f"SELECT {', '.join(projections)} FROM rel")

    # Date conversion — try parsing common DATASUS date columns
    new_columns = schema_columns(rel)
    date_candidates = ["death_date", "date", "DTOBITO", "DTNASC", "admission_date"]
    for dc in date_candidates:
        if dc in new_columns:
            try:
                rel = rel.project(
                    ", ".join(
                        f'TRY_CAST("{dc}" AS DATE) AS "{dc}"'
                        if c == dc else f'"{c}"'
                        for c in new_columns
                    )
                )
            except Exception:
                pass  # Skip if conversion fails

    # Store system as relation description
    rel = rel.set_alias(system or "unknown")

    return rel
