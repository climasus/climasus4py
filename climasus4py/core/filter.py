"""Disease and demographic filtering.

Mirrors R: filter.R + utils-cid.R
"""

from __future__ import annotations

import warnings
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
    load_json,
)
from ._stage import add_history, set_stage
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

_SEX_TRANSLATED: dict[str, list[str]] = {
    "1": ["Male", "Masculino"],
    "2": ["Female", "Feminino"],
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
    return list(dict.fromkeys(result))


# ---------------------------------------------------------------------------
# Region expansion
# ---------------------------------------------------------------------------

def expand_region_to_states(region: str | list[str]) -> list[str]:
    """Resolve region name(s) to list of state abbreviations."""
    regions_data = load_json("metadata/regions.json")
    region_list  = [region] if isinstance(region, str) else list(region)
    all_states: list[str] = []

    for reg in region_list:
        reg_norm = reg.lower().strip().replace("-", "_").replace(" ", "_")
        found    = False

        for category in regions_data["categories"].values():
            for key, data in category["regions"].items():
                aliases      = [key] + data.get("aliases", [])
                aliases_norm = [
                    a.lower().replace("-", "_").replace(" ", "_")
                    for a in aliases
                ]
                if reg_norm in aliases_norm:
                    all_states.extend(data["states"])
                    found = True
                    break
            if found:
                break

        if not found:
            raise ValueError(
                f"Region {reg!r} not found in regions.json. "
                f"Valid options: norte, nordeste, sudeste, sul, centro_oeste, "
                f"amazonia_legal, cerrado, caatinga, pantanal, pampa, "
                f"semi_arido, matopiba, sudene, fronteira_brasil, "
                f"dengue_hyperendemic."
            )

    return list(dict.fromkeys(all_states))


# ---------------------------------------------------------------------------
# UF column detection helper
# ---------------------------------------------------------------------------

_UF_COLUMN_CANDIDATES = (
    "abbrev_state", "state", "UF", "SG_UF",
    "notification_uf", "UF_ZI", "uf_gestor",
)


def _detect_uf_column(columns: list[str]) -> str | None:
    """Auto-detect UF/state column from schema."""
    return next((c for c in _UF_COLUMN_CANDIDATES if c in columns), None)


# ---------------------------------------------------------------------------
# Sex column translation detection
# ---------------------------------------------------------------------------

def _sex_col_is_translated(rel: duckdb.DuckDBPyRelation, sex_col: str) -> bool:
    """Check if the sex column already contains translated values (Male/Female)
    instead of raw DATASUS codes (1/2)."""
    try:
        sample = (
            rel.limit(200)
               .select(f'"{sex_col}"')
               .fetchdf()[sex_col]
               .dropna()
               .astype(str)
               .unique()
        )
        translated = {"Male", "Female", "Masculino", "Feminino",
                      "male", "female", "masculino", "feminino"}
        return any(v in translated for v in sample)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Main filter function
# ---------------------------------------------------------------------------

def sus_filter(
    rel: duckdb.DuckDBPyRelation,
    *,
    groups: str | list[str] | None = None,
    codes: str | list[str] | None = None,
    icd_column: str | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
    sex: str | list[str] | None = None,
    race: str | list[str] | None = None,
    uf: str | list[str] | None = None,
    region: str | list[str] | None = None,
    municipality: str | list[str] | None = None,
    date_start: str | None = None,
    date_end: str | None = None,
    education: str | list[str] | None = None,
    city: str | list[str] | None = None,
    drop_ignored: bool = False,
    match_type: str = "starts_with",
    verbose: bool = False,
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
        icd_column: Column name containing ICD-10 codes. If ``None``
            (default), auto-detects among ``CAUSABAS``, ``DIAG_PRINC``,
            ``underlying_cause``, ``cause``.
        age_min: Minimum age in years (inclusive).
        age_max: Maximum age in years (inclusive).
        sex: Sex value to keep — ``"M"`` / ``"Male"`` / ``"Masculino"``
            or ``"F"`` / ``"Female"`` / ``"Feminino"``.
            Works with both raw DATASUS codes (``"1"``/``"2"``) and
            translated values (``"Male"``/``"Female"``).
        race: ``RACACOR`` code(s) to keep, e.g. ``["1", "4"]``.
        uf: One or more Brazilian state abbreviations, e.g. ``["SP", "RJ"]``.
        region: Brazilian region name(s) in PT/EN/ES.
        municipality: Municipality code(s) (IBGE 6-digit).
        date_start: Earliest event date (inclusive), ISO ``"YYYY-MM-DD"``.
        date_end: Latest event date (inclusive), ISO ``"YYYY-MM-DD"``.
        education: Education level code(s) to keep.
        city: City name(s) resolved to IBGE codes.
        drop_ignored: Remove rows where demographic columns contain
            coded "ignored/unknown" values. Default: ``False``.
        match_type: ``"starts_with"`` (default) or ``"exact"`` for ICD matching.
        verbose: Print progress messages. Default: ``False``.

    Returns:
        Lazy DuckDB relation with all specified filters applied.
        Registers ``stage="filter"`` and timestamped history in sus_meta.

    Example:
        >>> filtered = cs.sus_filter(rel, groups="respiratory",
        ...                          age_min=15, age_max=64, sex="Male")
        >>> cs.sus_filter(rel, codes=["A90", "A91"], sex="Female")
        >>> cs.sus_filter(rel, region="nordeste", groups="dengue")
    """
    _valid_match_types = {"starts_with", "exact"}
    if match_type not in _valid_match_types:
        raise ValueError(
            f"Invalid match_type {match_type!r}. "
            f"Choose from: {sorted(_valid_match_types)}."
        )

    _original_rel = rel
    columns = schema_columns(rel)
    conn    = get_connection()

    if verbose:
        filters_requested = [k for k, v in {
            "groups": groups, "codes": codes, "icd_column": icd_column,
            "age": age_min or age_max, "sex": sex, "race": race,
            "uf": uf, "region": region, "municipality": municipality,
            "date": date_start or date_end, "education": education,
            "city": city,
        }.items() if v is not None]
        print(f"sus_filter — {len(filters_requested)} filtro(s): "
              f"{filters_requested}")

    # ------------------------------------------------------------------
    # Disease filtering (CID-10)
    # ------------------------------------------------------------------
    icd_codes: list[str] = []
    if groups:
        group_list = [groups] if isinstance(groups, str) else groups
        icd_codes.extend(codes_for_groups(group_list))
    if codes:
        code_list = [codes] if isinstance(codes, str) else codes
        icd_codes.extend(expand_cid_ranges(code_list))

    if icd_codes:
        if icd_column is not None:
            if icd_column not in columns:
                raise ValueError(
                    f"icd_column {icd_column!r} not found in relation. "
                    f"Available columns: {columns[:10]}..."
                )
            cause_col = icd_column
        else:
            cause_col = detect_cause_column(columns)

        if not cause_col:
            raise ValueError(
                "No cause/CID column found in the relation. "
                "Expected one of: CAUSABAS, DIAG_PRINC, underlying_cause, cause."
            )

        if verbose:
            print(f"  CID — coluna: {cause_col} | códigos: {len(icd_codes)}")

        if match_type == "exact":
            unique_codes = sorted(set(icd_codes))
            vals = ", ".join(sql_string(c) for c in unique_codes)
            rel = rel.filter(f'"{cause_col}" IN ({vals})')
        else:
            unique_codes = sorted({c[:3] for c in icd_codes})
            if len(unique_codes) <= 100:
                codes_str = ", ".join(sql_string(c) for c in unique_codes)
                rel = rel.filter(
                    f'SUBSTR("{cause_col}", 1, 3) IN ({codes_str})'
                )
            else:
                codes_sql = ", ".join(
                    f"({sql_string(c)})" for c in unique_codes
                )
                conn.execute(
                    f"CREATE OR REPLACE TEMP TABLE _icd_filter AS "
                    f"SELECT * FROM (VALUES {codes_sql}) AS t(code)"
                )
                rel = conn.sql(
                    f'SELECT r.* FROM rel r SEMI JOIN _icd_filter f '
                    f'ON SUBSTR(r."{cause_col}", 1, 3) = f.code'
                )

    # ------------------------------------------------------------------
    # Age filtering
    # ------------------------------------------------------------------
    if age_min is not None or age_max is not None:
        age_col = detect_age_column(columns)
        if not age_col:
            raise ValueError(
                "No age column found in the relation. "
                "Expected one of: age, age_years, age_code, IDADE, IDADEMAE."
            )

        # if age_years already exists (post create_variables), use it directly
        if "age_years" in columns:
            conditions = []
            if age_min is not None:
                conditions.append(f'"age_years" >= {age_min}')
            if age_max is not None:
                conditions.append(f'"age_years" <= {age_max}')
            # exclude sentinel value 999 (DATASUS undecodable age)
            conditions.append('"age_years" < 999')
            rel = rel.filter(" AND ".join(conditions))
        else:
            decoded    = decode_age_sql(age_col)
            conditions = []
            if age_min is not None:
                conditions.append(f'({decoded}) >= {age_min}')
            if age_max is not None:
                conditions.append(f'({decoded}) <= {age_max}')
            rel = rel.filter(" AND ".join(conditions))

        if verbose:
            print(f"  Idade — coluna: {age_col} | min={age_min} max={age_max}")

    # ------------------------------------------------------------------
    # Sex filtering — handles both raw codes ("1"/"2") and
    # translated values ("Male"/"Female") after sus_data_standardize()
    # ------------------------------------------------------------------
    if sex is not None:
        sex_codes = expand_sex_synonyms(sex)  # always resolves to ["1"] or ["2"]
        sex_col   = detect_sex_column(columns)
        if not sex_col:
            raise ValueError(
                "No sex column found in the relation. "
                "Expected one of: sex, SEXO, CS_SEXO."
            )

        if _sex_col_is_translated(rel, sex_col):
            # column already translated by sus_data_standardize() →
            # filter by translated labels instead of raw codes
            translated_vals: list[str] = []
            for code in sex_codes:
                translated_vals.extend(_SEX_TRANSLATED.get(code, [code]))
            vals = ", ".join(sql_string(v) for v in translated_vals)
            rel  = rel.filter(f'"{sex_col}" IN ({vals})')
            if verbose:
                print(f"  Sexo (traduzido) — coluna: {sex_col} | "
                      f"valores: {translated_vals}")
        else:
            # raw DATASUS codes
            if len(sex_codes) == 1:
                rel = rel.filter(f'"{sex_col}" = {sql_string(sex_codes[0])}')
            else:
                vals = ", ".join(sql_string(c) for c in sex_codes)
                rel  = rel.filter(f'"{sex_col}" IN ({vals})')
            if verbose:
                print(f"  Sexo (código) — coluna: {sex_col} | "
                      f"códigos: {sex_codes}")

    # ------------------------------------------------------------------
    # Race filtering
    # ------------------------------------------------------------------
    if race is not None:
        race_list = [race] if isinstance(race, str) else race
        race_col  = next(
            (c for c in ("RACACOR", "race") if c in columns), None
        )
        if not race_col:
            raise ValueError(
                "No race column found in the relation. "
                "Expected one of: RACACOR, race."
            )
        vals = ", ".join(sql_string(r) for r in race_list)
        rel  = rel.filter(f'"{race_col}" IN ({vals})')
        if verbose:
            print(f"  Raça — coluna: {race_col} | valores: {race_list}")

    # ------------------------------------------------------------------
    # UF filtering
    # ------------------------------------------------------------------
    if uf is not None:
        uf_list = [uf] if isinstance(uf, str) else uf
        uf_col  = _detect_uf_column(columns)
        if uf_col is None:
            warnings.warn(
                "No UF/state column found — uf filter skipped. "
                "Run sus_spatial_join() first, or use SINAN/SIH data "
                "which already contain a UF column.",
                UserWarning,
                stacklevel=2,
            )
        else:
            vals = ", ".join(sql_string(u.upper()) for u in uf_list)
            rel  = rel.filter(f'"{uf_col}" IN ({vals})')
            if verbose:
                print(f"  UF — coluna: {uf_col} | estados: {uf_list}")

    # ------------------------------------------------------------------
    # Region filtering
    # ------------------------------------------------------------------
    if region is not None:
        states = expand_region_to_states(region)
        uf_col  = _detect_uf_column(columns)
        if uf_col is None:
            warnings.warn(
                "No UF/state column found — region filter skipped. "
                "Run sus_spatial_join() first, or use SINAN/SIH data "
                "which already contain a UF column.",
                UserWarning,
                stacklevel=2,
            )
        else:
            vals = ", ".join(sql_string(u) for u in states)
            rel  = rel.filter(f'"{uf_col}" IN ({vals})')
            if verbose:
                print(f"  Região — {region!r} → {len(states)} estados")

    # ------------------------------------------------------------------
    # Municipality filtering
    # ------------------------------------------------------------------
    if municipality is not None:
        muni_list = [municipality] if isinstance(municipality, str) else municipality
        muni_col  = next(
            (c for c in (
                "CODMUNRES", "municipality_code", "ID_MUNICIP",
                "residence_municipality_code", "occurrence_municipality_code",
            ) if c in columns),
            None,
        )
        if not muni_col:
            raise ValueError(
                "No municipality column found in the relation. "
                "Expected one of: CODMUNRES, municipality_code, ID_MUNICIP, "
                "residence_municipality_code, occurrence_municipality_code."
            )
        vals = ", ".join(sql_string(m) for m in muni_list)
        rel  = rel.filter(f'"{muni_col}" IN ({vals})')
        if verbose:
            print(f"  Município — coluna: {muni_col} | códigos: {muni_list}")

    # ------------------------------------------------------------------
    # Date range filtering
    # ------------------------------------------------------------------
    if date_start is not None or date_end is not None:
        date_col = next(
            (c for c in (
                "death_date", "date", "DTOBITO", "DTNASC",
                "admission_date", "notification_date",
            ) if c in columns),
            None,
        )
        if not date_col:
            raise ValueError(
                "No date column found in the relation. "
                "Expected one of: death_date, date, DTOBITO, DTNASC, "
                "admission_date, notification_date."
            )
        if date_start:
            rel = rel.filter(
                f'TRY_CAST("{date_col}" AS DATE) >= {sql_string(date_start)}'
            )
        if date_end:
            rel = rel.filter(
                f'TRY_CAST("{date_col}" AS DATE) <= {sql_string(date_end)}'
            )
        if verbose:
            print(f"  Data — coluna: {date_col} | {date_start} → {date_end}")

    # ------------------------------------------------------------------
    # Education filtering
    # ------------------------------------------------------------------
    if education is not None:
        edu_list = [education] if isinstance(education, str) else list(education)
        edu_col  = detect_education_column(columns)
        if not edu_col:
            raise ValueError(
                "No education column found in the relation. "
                "Expected one of: education, education_2010, ESC, ESC2010."
            )
        vals = ", ".join(sql_string(e) for e in edu_list)
        rel  = rel.filter(f'"{edu_col}" IN ({vals})')
        if verbose:
            print(f"  Educação — coluna: {edu_col} | valores: {edu_list}")

    # ------------------------------------------------------------------
    # City filtering (resolved to IBGE codes)
    # ------------------------------------------------------------------
    if city is not None:
        city_codes = expand_city_to_codes(city)
        muni_col   = next(
            (c for c in (
                "CODMUNRES", "municipality_code", "ID_MUNICIP",
                "residence_municipality_code", "occurrence_municipality_code",
            ) if c in columns),
            None,
        )
        if not muni_col:
            raise ValueError(
                "No municipality column found in the relation. "
                "Expected one of: CODMUNRES, municipality_code, ID_MUNICIP."
            )
        vals = ", ".join(sql_string(c) for c in city_codes)
        rel  = rel.filter(f'"{muni_col}" IN ({vals})')
        if verbose:
            print(f"  Cidade — {city!r} → códigos: {city_codes}")

    # ------------------------------------------------------------------
    # Drop ignored demographic values
    # ------------------------------------------------------------------
    if drop_ignored:
        ignorable_present = [c for c in _IGNORABLE_DEMO_COLUMNS if c in columns]
        ignored_vals_sql  = ", ".join(
            sql_string(v) for v in _IGNORED_VALUES if v != ""
        )
        for col in ignorable_present:
            rel = rel.filter(
                f'("{col}" IS NOT NULL AND '
                f'TRIM(CAST("{col}" AS VARCHAR)) NOT IN ({ignored_vals_sql}) AND '
                f"TRIM(CAST(\"{col}\" AS VARCHAR)) != '')"
            )
        if verbose:
            print(f"  Drop ignored — {len(ignorable_present)} colunas")

    # ------------------------------------------------------------------
    # sus_meta
    # ------------------------------------------------------------------
    filters_applied = []
    if groups:           filters_applied.append(f"groups={groups!r}")
    if codes:            filters_applied.append(f"codes={codes!r}")
    if icd_column:       filters_applied.append(f"icd_column={icd_column!r}")
    if age_min is not None or age_max is not None:
                         filters_applied.append(f"age=[{age_min},{age_max}]")
    if sex is not None:        filters_applied.append(f"sex={sex!r}")
    if race is not None:       filters_applied.append(f"race={race!r}")
    if uf is not None:         filters_applied.append(f"uf={uf!r}")
    if region is not None:     filters_applied.append(f"region={region!r}")
    if municipality is not None: filters_applied.append(f"municipality={municipality!r}")
    if date_start or date_end: filters_applied.append(f"date={date_start}→{date_end}")
    if education is not None:  filters_applied.append(f"education={education!r}")
    if city is not None:       filters_applied.append(f"city={city!r}")
    if drop_ignored:           filters_applied.append("drop_ignored=True")

    history_msg = (
        f"Filtered: {'; '.join(filters_applied)}"
        if filters_applied else "Filter applied (no criteria)"
    )

    rel = rel.set_alias("filter")
    rel = set_stage(rel, "filter", _inherit_from=_original_rel)
    rel = add_history(rel, history_msg)

    if verbose:
        print("✓ sus_filter concluído")

    return rel
