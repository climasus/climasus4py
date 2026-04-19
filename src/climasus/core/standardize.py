"""Data standardization — column renaming, type conversion, translations.

Mirrors R: standardize.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.data import detect_system, load_json


def _load_column_dict(lang: str) -> dict[str, str]:
    """Carrega o dicionário de tradução de colunas para o idioma solicitado."""
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
    """Standardise column names, translate labels, and convert date columns.

    Renames columns according to the translation dictionary from
    climasus-data (``dictionaries/pt-{lang}/columns.json``). Attempts to
    parse common DATASUS date columns from the ``DDMMYYYY`` string format
    to proper ``DATE`` values. Sets the relation alias to the detected
    (or provided) system name.

    Args:
        rel: Lazy DuckDB relation whose columns will be renamed.
        lang: Target language for column names — ``"en"`` (default),
            ``"pt"`` (no-op, no rename), or ``"es"``.
        system: SUS system name (e.g. ``"SIM-DO"``). Auto-detected from
            column signatures when ``None``.

    Returns:
        Lazy DuckDB relation with standardised column names and parsed
        date columns.

    Example:
        >>> std = sus_standardize(rel, lang="en")
        >>> "death_date" in std.columns
        True
        >>> sus_standardize(rel, lang="pt", system="SINASC")
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
    # DATASUS uses DDMMYYYY format (e.g. "01012023" = 2023-01-01)
    # Use TRY_STRPTIME for proper format parsing, fallback to TRY_CAST
    new_columns = schema_columns(rel)
    date_candidates = ["death_date", "date", "DTOBITO", "DTNASC", "admission_date",
                       "birth_date", "case_conclusion_date"]
    for dc in date_candidates:
        if dc in new_columns:
            try:
                rel = rel.project(
                    ", ".join(
                        (
                            f"CASE WHEN typeof(\"{dc}\") = 'VARCHAR' "
                            f"THEN TRY_STRPTIME(\"{dc}\", '%d%m%Y')::DATE "
                            f"ELSE TRY_CAST(\"{dc}\" AS DATE) END AS \"{dc}\""
                        )
                        if c == dc else f'"{c}"'
                        for c in new_columns
                    )
                )
            except Exception:
                pass  # Skip if conversion fails

    # Store system as relation description
    rel = rel.set_alias(system or "unknown")

    return rel
