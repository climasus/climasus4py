"""Disease and demographic filtering.

Mirrors R: filter.R + utils-cid.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.cid import codes_for_groups, expand_cid_ranges
from climasus.utils.data import detect_cause_column, detect_age_column, detect_sex_column, decode_age_sql


def sus_filter(
    rel: duckdb.DuckDBPyRelation,
    *,
    groups: str | list[str] | None = None,
    codes: str | list[str] | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    sex: str | None = None,
    race: str | list[str] | None = None,
    uf: str | list[str] | None = None,
    municipality: str | list[str] | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Filter SUS data by disease groups, demographics, geography, and dates.

    All predicates are applied as DuckDB ``WHERE`` clauses; the relation
    stays lazy until materialised. CID-10 codes are resolved via
    ``codes_for_groups`` and ``expand_cid_ranges``; geographic and
    demographic columns are auto-detected from the relation schema.

    Args:
        rel: Lazy DuckDB relation to filter.
        groups: Named disease group(s) from climasus-data, e.g.
            ``"respiratory"`` or ``["cardiovascular", "dengue"]``.
        codes: Explicit ICD-10 code(s) or ranges, e.g.
            ``["J00-J99", "A90"]``.
        age_min: Minimum age in years (inclusive). Decoded from DATASUS
            coded age when the column is in SIM-DO format.
        age_max: Maximum age in years (inclusive).
        sex: Sex code to keep — ``"M"`` (male) or ``"F"`` (female).
        race: ``RACACOR`` code(s) to keep, e.g. ``["1", "4"]``.
        uf: One or more Brazilian state abbreviations, e.g.
            ``["SP", "RJ"]``.
        municipality: Municipality code(s) (IBGE 6-digit), e.g.
            ``["355030"]``.
        date_start: Earliest event date (inclusive), ISO format
            ``"YYYY-MM-DD"``.
        date_end: Latest event date (inclusive), ISO format
            ``"YYYY-MM-DD"``.

    Returns:
        Lazy DuckDB relation with all specified filters applied.

    Example:
        >>> filtered = sus_filter(rel, groups="respiratory",
        ...                       age_min=15, age_max=64, uf="SP")
        >>> sus_filter(rel, codes=["A90", "A91"], sex="F").count()
    """
    columns = schema_columns(rel)
    conn = get_connection()

    # --- Disease filtering (CID-10) ---
    icd_codes: list[str] = []
    if groups:
        group_list = [groups] if isinstance(groups, str) else groups
        icd_codes.extend(codes_for_groups(group_list))
    if codes:
        code_list = [codes] if isinstance(codes, str) else codes
        icd_codes.extend(expand_cid_ranges(code_list))

    if icd_codes:
        cause_col = detect_cause_column(columns)
        if cause_col:
            unique_codes = sorted(set(icd_codes))
            if len(unique_codes) <= 100:
                # Direct IN filter
                codes_str = ", ".join(f"'{c}'" for c in unique_codes)
                rel = rel.filter(f'"{cause_col}" IN ({codes_str})')
            else:
                # Semi-join via temporary table for large code lists
                codes_sql = ", ".join(f"('{c}')" for c in unique_codes)
                conn.execute(
                    f"CREATE OR REPLACE TEMP TABLE _icd_filter AS "
                    f"SELECT * FROM (VALUES {codes_sql}) AS t(code)"
                )
                rel = conn.sql(
                    f'SELECT r.* FROM rel r SEMI JOIN _icd_filter f '
                    f'ON SUBSTR(r."{cause_col}", 1, 3) = f.code'
                )

    # --- Age filtering ---
    age_col = detect_age_column(columns)
    if age_col and (age_min is not None or age_max is not None):
        decoded = decode_age_sql(age_col)
        conditions = []
        if age_min is not None:
            conditions.append(f'({decoded}) >= {age_min}')
        if age_max is not None:
            conditions.append(f'({decoded}) <= {age_max}')
        rel = rel.filter(" AND ".join(conditions))

    # --- Sex filtering ---
    sex_col = detect_sex_column(columns)
    if sex_col and sex:
        sex_val = sex.upper()
        rel = rel.filter(f'"{sex_col}" = \'{sex_val}\'')

    # --- Race filtering ---
    if race:
        race_list = [race] if isinstance(race, str) else race
        for candidate in ("RACACOR", "race"):
            if candidate in columns:
                vals = ", ".join(f"'{r}'" for r in race_list)
                rel = rel.filter(f'"{candidate}" IN ({vals})')
                break

    # --- UF filtering ---
    if uf:
        uf_list = [uf] if isinstance(uf, str) else uf
        for candidate in ("UF", "SG_UF", "state"):
            if candidate in columns:
                vals = ", ".join(f"'{u.upper()}'" for u in uf_list)
                rel = rel.filter(f'"{candidate}" IN ({vals})')
                break

    # --- Municipality filtering ---
    if municipality:
        muni_list = [municipality] if isinstance(municipality, str) else municipality
        for candidate in ("CODMUNRES", "municipality_code", "ID_MUNICIP"):
            if candidate in columns:
                vals = ", ".join(f"'{m}'" for m in muni_list)
                rel = rel.filter(f'"{candidate}" IN ({vals})')
                break

    # --- Date range filtering ---
    if date_start or date_end:
        for candidate in ("death_date", "date", "DTOBITO", "DTNASC"):
            if candidate in columns:
                if date_start:
                    rel = rel.filter(
                        f'TRY_CAST("{candidate}" AS DATE) >= \'{date_start}\''
                    )
                if date_end:
                    rel = rel.filter(
                        f'TRY_CAST("{candidate}" AS DATE) <= \'{date_end}\''
                    )
                break

    return rel
