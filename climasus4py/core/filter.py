"""Disease and demographic filtering.

Mirrors R: filter.R + utils-cid.R
"""

from __future__ import annotations

import duckdb

from ..utils.cid import codes_for_groups, expand_cid_ranges
from ..utils.data import (
    _IGNORABLE_DEMO_COLUMNS,
    _IGNORED_VALUES,
    decode_age_sql,
    detect_age_column,
    detect_cause_column,
    detect_education_column,
    detect_sex_column,
    expand_city_to_codes,
)
from ._sql import sql_string
from .engine import get_connection, schema_columns

# ---------------------------------------------------------------------------
# Sex synonym expansion
# ---------------------------------------------------------------------------

_SEX_SYNONYMS: dict[str, list[str]] = {
    "1": ["1"],
    "2": ["2"],
    "m": ["1"],
    "male": ["1"],
    "masculino": ["1"],
    "f": ["2"],
    "female": ["2"],
    "feminino": ["2"],
    "femenino": ["2"],
}


def expand_sex_synonyms(sex: str | list[str]) -> list[str]:
    """Expand sex label(s) to the canonical DATASUS codes (``"1"`` / ``"2"``).

    Args:
        sex: A sex value or list of sex values. Accepts DATASUS codes
            (``"1"``, ``"2"``), canonical letters (``"M"``, ``"F"``),
            and full names in English, Portuguese, and Spanish.

    Returns:
        Deduplicated list of DATASUS sex codes.

    Raises:
        ValueError: If any value is not recognised.
    """
    values = [sex] if isinstance(sex, str) else list(sex)
    result: list[str] = []
    for v in values:
        key = v.lower()
        if key not in _SEX_SYNONYMS:
            raise ValueError(
                f"Unrecognised sex value: {v!r}. "
                "Use '1'/'2', 'M'/'F', 'Male'/'Female', "
                "'Masculino'/'Feminino', or 'Femenino'."
            )
        result.extend(_SEX_SYNONYMS[key])
    return list(dict.fromkeys(result))  # deduplicate, preserve order


def sus_filter(
    rel: duckdb.DuckDBPyRelation,
    *,
    groups: str | list[str] | None = None,
    codes: str | list[str] | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    sex: str | list[str] | None = None,
    race: str | list[str] | None = None,
    uf: str | list[str] | None = None,
    municipality: str | list[str] | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    education: str | list[str] | None = None,
    city: str | list[str] | None = None,
    drop_ignored: bool = False,
    match_type: str = "starts_with",
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
        education: Education level code(s) to keep, e.g. ``["1", "2"]``.
            Auto-detects the column among ``education``,
            ``education_2010``, ``ESC``, ``ESC2010``.
            Mirrors ``climasus4r::sus_data_filter_demographics`` education=.
        city: City name(s) to filter by, e.g. ``"São Paulo"`` or
            ``["São Paulo", "Rio de Janeiro"]``. Resolved to IBGE codes
            via ``climasus-data/spatial/municipalities.parquet``.
            Mirrors ``climasus4r::sus_data_filter_demographics`` city=.
        drop_ignored: When ``True``, removes rows where any detectable
            demographic column (sex, race, education, age) contains a
            coded "ignored/unknown" value (9, 99, Ignorado, etc.).
            Mirrors ``climasus4r::sus_data_filter_demographics``
            drop_ignored=.  Default: ``False``.
        match_type: Controls how ICD-10 codes are matched against the
            cause column.  ``"starts_with"`` (default) uses a 3-character
            prefix match (e.g. ``"J18"`` matches ``"J189"``).
            ``"exact"`` requires the full code to match exactly.
            Mirrors ``climasus4r::sus_data_filter_cid`` match_type=.

    Returns:
        Lazy DuckDB relation with all specified filters applied.

    Raises:
        ValueError: If ``match_type`` is not ``"starts_with"`` or
            ``"exact"``.

    Example:
        >>> import climasus4py as cs
        >>> filtered = cs.sus_filter(rel, groups="respiratory",
        ...                          age_min=15, age_max=64, uf="SP")
        >>> cs.sus_filter(rel, codes=["A90", "A91"], sex="F").count()
        >>> cs.sus_filter(rel, groups="respiratory",
        ...               match_type="exact", codes=["J189"])
        >>> cs.sus_filter(rel, education=["1", "2"], drop_ignored=True)
        >>> cs.sus_filter(rel, city="São Paulo")
    """
    _valid_match_types = {"starts_with", "exact"}
    if match_type not in _valid_match_types:
        raise ValueError(
            f"Invalid match_type {match_type!r}. "
            f"Choose from: {sorted(_valid_match_types)}."
        )

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
        if not cause_col:
            raise ValueError(
                "No cause/CID column found in the relation. "
                "Expected one of: CAUSABAS, DIAG_PRINC, underlying_cause, cause."
            )
        if match_type == "exact":
            # Exact match: full code must equal one of the requested codes
            unique_codes = sorted(set(icd_codes))
            vals = ", ".join(f"'{c}'" for c in unique_codes)
            rel = rel.filter(f'"{cause_col}" IN ({vals})')
        else:
            # starts_with (default): normalise to 3-char prefix
            unique_codes = sorted(set(c[:3] for c in icd_codes))
            if len(unique_codes) <= 100:
                codes_str = ", ".join(f"'{c}'" for c in unique_codes)
                rel = rel.filter(f'SUBSTR("{cause_col}", 1, 3) IN ({codes_str})')
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
    if age_min is not None or age_max is not None:
        age_col = detect_age_column(columns)
        if not age_col:
            raise ValueError(
                "No age column found in the relation. "
                "Expected one of: age, age_years, age_code, IDADE, IDADEMAE."
            )
        decoded = decode_age_sql(age_col)
        conditions = []
        if age_min is not None:
            conditions.append(f'({decoded}) >= {age_min}')
        if age_max is not None:
            conditions.append(f'({decoded}) <= {age_max}')
        rel = rel.filter(" AND ".join(conditions))

    # --- Sex filtering ---
    if sex is not None:
        sex_codes = expand_sex_synonyms(sex)  # raises ValueError for unknown values
        sex_col = detect_sex_column(columns)
        if not sex_col:
            raise ValueError(
                "No sex column found in the relation. "
                "Expected one of: sex, SEXO, CS_SEXO."
            )
        if len(sex_codes) == 1:
            rel = rel.filter(f'"{sex_col}" = \'{sex_codes[0]}\'')
        else:
            vals = ", ".join(f"'{c}'" for c in sex_codes)
            rel = rel.filter(f'"{sex_col}" IN ({vals})')

    # --- Race filtering ---
    if race is not None:
        race_list = [race] if isinstance(race, str) else race
        race_col = next(
            (c for c in ("RACACOR", "race") if c in columns),
            None,
        )
        if not race_col:
            raise ValueError(
                "No race column found in the relation. "
                "Expected one of: RACACOR, race."
            )
        vals = ", ".join(sql_string(r) for r in race_list)
        rel = rel.filter(f'"{race_col}" IN ({vals})')

    # --- UF filtering ---
    if uf is not None:
        uf_list = [uf] if isinstance(uf, str) else uf
        uf_col = next(
            (c for c in ("UF", "SG_UF", "state") if c in columns),
            None,
        )
        if not uf_col:
            raise ValueError(
                "No UF/state column found in the relation. "
                "Expected one of: UF, SG_UF, state."
            )
        vals = ", ".join(sql_string(u.upper()) for u in uf_list)
        rel = rel.filter(f'"{uf_col}" IN ({vals})')

    # --- Municipality filtering ---
    if municipality is not None:
        muni_list = [municipality] if isinstance(municipality, str) else municipality
        muni_col = next(
            (c for c in ("CODMUNRES", "municipality_code", "ID_MUNICIP") if c in columns),
            None,
        )
        if not muni_col:
            raise ValueError(
                "No municipality column found in the relation. "
                "Expected one of: CODMUNRES, municipality_code, ID_MUNICIP."
            )
        vals = ", ".join(sql_string(m) for m in muni_list)
        rel = rel.filter(f'"{muni_col}" IN ({vals})')

    # --- Date range filtering ---
    if date_start is not None or date_end is not None:
        date_col = next(
            (c for c in ("death_date", "date", "DTOBITO", "DTNASC") if c in columns),
            None,
        )
        if not date_col:
            raise ValueError(
                "No date column found in the relation. "
                "Expected one of: death_date, date, DTOBITO, DTNASC."
            )
        if date_start:
            rel = rel.filter(
                f'TRY_CAST("{date_col}" AS DATE) >= \'{date_start}\''
            )
        if date_end:
            rel = rel.filter(
                f'TRY_CAST("{date_col}" AS DATE) <= \'{date_end}\''
            )

    # --- Education filtering ---
    if education is not None:
        edu_list = [education] if isinstance(education, str) else list(education)
        edu_col = detect_education_column(columns)
        if not edu_col:
            raise ValueError(
                "No education column found in the relation. "
                "Expected one of: education, education_2010, ESC, ESC2010."
            )
        vals = ", ".join(sql_string(e) for e in edu_list)
        rel = rel.filter(f'"{edu_col}" IN ({vals})')

    # --- City filtering (resolved to IBGE codes) ---
    if city is not None:
        city_codes = expand_city_to_codes(city)
        muni_col = next(
            (c for c in ("CODMUNRES", "municipality_code", "ID_MUNICIP") if c in columns),
            None,
        )
        if not muni_col:
            raise ValueError(
                "No municipality column found in the relation. "
                "Expected one of: CODMUNRES, municipality_code, ID_MUNICIP."
            )
        vals = ", ".join(sql_string(c) for c in city_codes)
        rel = rel.filter(f'"{muni_col}" IN ({vals})')

    # --- Drop ignored demographic values ---
    if drop_ignored:
        ignorable_present = [c for c in _IGNORABLE_DEMO_COLUMNS if c in columns]
        ignored_vals_sql = ", ".join(f"'{v}'" for v in _IGNORED_VALUES if v != "")
        for col in ignorable_present:
            rel = rel.filter(
                f'("{col}" IS NOT NULL AND '
                f'TRIM(CAST("{col}" AS VARCHAR)) NOT IN ({ignored_vals_sql}) AND '
                f"TRIM(CAST(\"{col}\" AS VARCHAR)) != '')"
            )

    return rel

