"""Disease and demographic filtering.

Mirrors R: filter.R + utils-cid.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.cid import codes_for_groups, expand_cid_ranges
from climasus.utils.data import detect_cause_column, detect_age_column, detect_sex_column


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

    All filtering stays lazy (DuckDB relation).
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
        conditions = []
        if age_min is not None:
            conditions.append(f'TRY_CAST("{age_col}" AS INTEGER) >= {age_min}')
        if age_max is not None:
            conditions.append(f'TRY_CAST("{age_col}" AS INTEGER) <= {age_max}')
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
