"""Derived variable creation — age groups, calendar vars, seasons.

Mirrors R: variables.R
Pipeline stage: variables (optional, applied after standardize).
"""

from __future__ import annotations

from typing import Any, cast

import duckdb

from ..utils.data import decode_age_sql, detect_age_column, detect_date_column, load_json
from ._stage import add_history, set_stage
from .engine import get_connection, schema_columns

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AGE_BREAKS_DEFAULT = [0, 5, 15, 60, 999]
AGE_BREAKS_IBGE = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55,
                   60, 65, 70, 75, 80, 999]
AGE_LABELS_IBGE = [
    "0-4", "5-9", "10-14", "15-19", "20-24", "25-29",
    "30-34", "35-39", "40-44", "45-49", "50-54", "55-59",
    "60-64", "65-69", "70-74", "75-79", "80+"
]

DATE_COLUMN_CANDIDATES: dict[str, list[str]] = {
    "SIM":    ["death_date", "DTOBITO"],
    "SINASC": ["birth_date", "DTNASC"],
    "SIH":    ["admission_date", "DT_INTER"],
    "SINAN":  ["notification_date", "DT_NOTIFIC", "first_symptom_date", "DT_SIN_PRI"],
    "CNES":   ["update_date", "DT_COMPET"],
    "common": ["death_date", "birth_date", "admission_date", "notification_date",
               "date", "DTOBITO", "DT_NOTIFIC", "DT_INTER", "DTNASC"],
}

# ---------------------------------------------------------------------------
# JSON loaders (with fallback defaults)
# ---------------------------------------------------------------------------

def _age_groups_config() -> dict[str, Any]:
    try:
        data = load_json("templates/age_groups.json")
        if isinstance(data, dict) and "presets" in data:
            return data
    except FileNotFoundError:
        pass
    return {
        "default": "epidemiological_default",
        "presets": {
            "epidemiological_default": {"breaks": [0, 5, 15, 60, None]},
            "who": {"breaks": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45,
                               50, 55, 60, 65, 70, 75, 80, None]},
            "decadal": {"breaks": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, None]},
            "ibge": {"breaks": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45,
                                50, 55, 60, 65, 70, 75, 80, None]},
            "climate_risk": {"breaks": [0, 5, 65, None]},
        },
        "variable_names": {
            "age_years":          {"en": "age_years",          "pt": "idade_anos",          "es": "edad_anios"},
            "age_group":          {"en": "age_group",          "pt": "faixa_etaria",         "es": "grupo_edad"},
            "ibge_age_group":     {"en": "ibge_age_group",     "pt": "faixa_etaria_ibge",    "es": "grupo_edad_ibge"},
            "climate_risk_group": {"en": "climate_risk_group", "pt": "grupo_risco_climatico","es": "grupo_riesgo_climatico"},
        },
        "climate_risk_labels": {
            "en": ["High Risk (0-4)", "Standard Risk (5-64)", "High Risk (65+)"],
            "pt": ["Alto Risco (0-4)", "Risco Padrao (5-64)", "Alto Risco (65+)"],
            "es": ["Alto Riesgo (0-4)", "Riesgo Estandar (5-64)", "Alto Riesgo (65+)"],
        }
    }


def _calendar_config() -> dict[str, Any]:
    try:
        data = load_json("templates/calendar_labels.json")
        if isinstance(data, dict) and "variable_names" in data:
            return data
    except FileNotFoundError:
        pass
    return {
        "month_names": {
            "en": ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"],
            "pt": ["Janeiro","Fevereiro","Marco","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"],
            "es": ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                   "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"],
        },
        "day_names": {
            "en": ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
            "pt": ["Segunda-feira","Terca-feira","Quarta-feira","Quinta-feira",
                   "Sexta-feira","Sabado","Domingo"],
            "es": ["Lunes","Martes","Miercoles","Jueves","Viernes","Sabado","Domingo"],
        },
        "variable_names": {
            "en": {"year":"year","month":"month","quarter":"quarter",
                   "day_of_year":"day_of_year","day_of_week":"day_of_week",
                   "is_weekend":"is_weekend","semester":"semester",
                   "epi_week":"epidemiological_week","month_name":"month_name",
                   "dow_name":"day_of_week_name"},
            "pt": {"year":"ano","month":"mes","quarter":"trimestre",
                   "day_of_year":"dia_ano","day_of_week":"dia_semana",
                   "is_weekend":"fim_semana","semester":"semestre",
                   "epi_week":"semana_epidemiologica","month_name":"nome_mes",
                   "dow_name":"nome_dia_semana"},
            "es": {"year":"anio","month":"mes","quarter":"trimestre",
                   "day_of_year":"dia_anio","day_of_week":"dia_semana",
                   "is_weekend":"fin_semana","semester":"semestre",
                   "epi_week":"semana_epidemiologica","month_name":"nombre_mes",
                   "dow_name":"nombre_dia_semana"},
        }
    }


def _seasonal_config() -> dict[str, Any]:
    try:
        data = load_json("templates/seasonal_patterns.json")
        if isinstance(data, dict) and "astronomical" in data:
            return data
    except FileNotFoundError:
        pass
    return {
        "astronomical": {
            "patterns": {
                "south": {"summer":[12,1,2],"autumn":[3,4,5],"winter":[6,7,8],"spring":[9,10,11]},
                "north":  {"winter":[12,1,2],"spring":[3,4,5],"summer":[6,7,8],"autumn":[9,10,11]},
            },
            "labels": {
                "south": {"en":["Summer","Autumn","Winter","Spring"],
                          "pt":["Verao","Outono","Inverno","Primavera"],
                          "es":["Verano","Otono","Invierno","Primavera"]},
                "north": {"en":["Winter","Spring","Summer","Autumn"],
                          "pt":["Inverno","Primavera","Verao","Outono"],
                          "es":["Invierno","Primavera","Verano","Otono"]},
            },
            "variable_names": {"en":"astronomical_season","pt":"estacao_astronomica","es":"estacion_astronomica"}
        },
        "climatic": {
            "regions": {
                "norte":        {"rainy_months":[1,2,3,4,5],"dry_months":[6,7,8,9,10,11],"transition_months":[12]},
                "nordeste":     {"rainy_months":[2,3,4,5,6,7],"dry_months":[8,9,10,11,12,1]},
                "centro-oeste": {"rainy_months":[10,11,12,1,2,3],"dry_months":[4,5,6,7,8,9]},
                "sudeste":      {"rainy_months":[10,11,12,1,2,3],"dry_months":[4,5,6,7,8,9]},
                "sul":          {"note":"No defined dry season"},
            },
            "labels": {
                "en": {"rainy":"Rainy","transition":"Transition","dry":"Dry","no_dry":"No defined dry season"},
                "pt": {"rainy":"Chuvosa","transition":"Transicao","dry":"Seca","no_dry":"Sem estacao seca definida"},
                "es": {"rainy":"Lluviosa","transition":"Transicion","dry":"Seca","no_dry":"Sin estacion seca definida"},
            },
            "variable_names": {
                "climatic_season": {"en":"climatic_season","pt":"estacao_climatica","es":"estacion_climatica"},
                "dry_rainy_season": {"en":"dry_rainy_season","pt":"estacao_seca_chuvosa","es":"estacion_seca_lluviosa"},
            }
        }
    }

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _age_breaks_for_preset(preset: str) -> list[int]:
    cfg = _age_groups_config()
    presets = cast(dict[str, Any], cfg["presets"])
    if preset not in presets:
        return AGE_BREAKS_DEFAULT
    raw = presets[preset]["breaks"]
    return [999 if v is None else int(v) for v in raw]


def _age_group_sql(age_expr: str, breaks: list[int], labels: list[str]) -> str:
    cases = []
    for i, label in enumerate(labels):
        lo = breaks[i]
        hi = breaks[i + 1]
        if hi == 999:
            cases.append(f"WHEN ({age_expr}) >= {lo} THEN '{label}'")
        else:
            cases.append(f"WHEN ({age_expr}) >= {lo} AND ({age_expr}) < {hi} THEN '{label}'")
    return f"CASE {' '.join(cases)} ELSE NULL END"


def _climate_risk_sql(age_expr: str, lang: str = "en") -> str:
    cfg = _age_groups_config()
    labels = cfg["climate_risk_labels"][lang]
    return (
        f"CASE "
        f"WHEN ({age_expr}) IS NULL THEN NULL "
        f"WHEN ({age_expr}) <= 4  THEN '{labels[0]}' "
        f"WHEN ({age_expr}) <= 64 THEN '{labels[1]}' "
        f"WHEN ({age_expr}) >= 65 THEN '{labels[2]}' "
        f"ELSE NULL END"
    )


def _month_name_sql(month_expr: str, lang: str = "en") -> str:
    names = _calendar_config()["month_names"][lang]
    cases = " ".join(f"WHEN {month_expr} = {i+1} THEN '{n}'" for i, n in enumerate(names))
    return f"CASE {cases} ELSE NULL END"


def _day_name_sql(date_expr: str, lang: str = "en") -> str:
    names = _calendar_config()["day_names"][lang]
    cases = " ".join(
        f"WHEN ((DAYOFWEEK({date_expr}) + 6) % 7) + 1 = {i+1} THEN '{n}'"
        for i, n in enumerate(names)
    )
    return f"CASE {cases} ELSE NULL END"


def _astronomical_season_sql(month_expr: str, lang: str = "en", hemisphere: str = "south") -> str:
    cfg = _seasonal_config()["astronomical"]
    patterns = cfg["patterns"][hemisphere]
    labels_list = cfg["labels"][hemisphere][lang]
    season_keys = list(patterns.keys())
    cases = " ".join(
        f"WHEN {month_expr} IN ({','.join(str(m) for m in patterns[k])}) THEN '{labels_list[i]}'"
        for i, k in enumerate(season_keys)
    )
    return f"CASE {cases} ELSE NULL END"


def _climatic_season_sql(month_expr: str, region: str, lang: str = "en") -> str:
    cfg = _seasonal_config()["climatic"]
    labels = cfg["labels"][lang]
    r = region.lower().strip()
    reg = cfg["regions"].get(r, {})
    if "note" in reg:
        return f"'{labels['no_dry']}'"
    rainy = reg.get("rainy_months", [])
    trans = reg.get("transition_months", [])
    dry   = reg.get("dry_months", [])
    parts = []
    if rainy:
        parts.append(f"WHEN {month_expr} IN ({','.join(str(m) for m in rainy)}) THEN '{labels['rainy']}'")
    if trans:
        parts.append(f"WHEN {month_expr} IN ({','.join(str(m) for m in trans)}) THEN '{labels['transition']}'")
    if dry:
        parts.append(f"WHEN {month_expr} IN ({','.join(str(m) for m in dry)}) THEN '{labels['dry']}'")
    return f"CASE {' '.join(parts)} ELSE NULL END" if parts else "NULL"


def _dry_rainy_sql(month_expr: str, region: str, lang: str = "en") -> str:
    cfg = _seasonal_config()["climatic"]
    labels = cfg["labels"][lang]
    r = region.lower().strip()
    reg = cfg["regions"].get(r, {})
    if "note" in reg:
        return f"'{labels['no_dry']}'"
    rainy = reg.get("rainy_months", [])
    dry   = reg.get("dry_months", [])
    parts = []
    if rainy:
        parts.append(f"WHEN {month_expr} IN ({','.join(str(m) for m in rainy)}) THEN '{labels['rainy']}'")
    if dry:
        parts.append(f"WHEN {month_expr} IN ({','.join(str(m) for m in dry)}) THEN '{labels['dry']}'")
    return f"CASE {' '.join(parts)} ELSE NULL END" if parts else "NULL"


def _detect_date_col(columns: list[str], system: str | None = None) -> str | None:
    system_base = (system or "").split("-")[0].upper()
    candidates  = DATE_COLUMN_CANDIDATES.get(system_base, [])
    candidates  = candidates + DATE_COLUMN_CANDIDATES["common"]
    return next((c for c in candidates if c in columns), None)


def _epi_week_sql(date_expr: str) -> str:
    return (
        f"LPAD(CAST(STRFTIME({date_expr}, '%U') AS VARCHAR), 2, '0')"
        f" || '/' || STRFTIME({date_expr}, '%Y')"
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_create_variables(
    rel: duckdb.DuckDBPyRelation,
    *,
    create_age_groups:    bool           = True,
    age_breaks:           list[int]      = AGE_BREAKS_DEFAULT,
    age_labels:           list[str] | None = None,
    create_calendar_vars: bool           = True,
    create_climate_vars:  bool           = True,
    climate_region:       str | None     = None,
    hemisphere:           str            = "south",
    lang:                 str            = "en",
    verbose:              bool           = True,
) -> duckdb.DuckDBPyRelation:
    """Create derived variables from SUS data (stays lazy).

    Mirrors R: sus_data_create_variables().

    Three variable blocks — all optional:

    1. **Age variables** (``create_age_groups=True``):
       ``age_years``, ``age_group``, ``climate_risk_group``, ``ibge_age_group``

    2. **Calendar variables** (``create_calendar_vars=True``):
       ``year``, ``month``, ``quarter``, ``semester``, ``day_of_year``,
       ``day_of_week``, ``is_weekend``, ``epidemiological_week``,
       ``month_name``, ``day_of_week_name``

    3. **Season variables** (``create_climate_vars=True``):
       ``astronomical_season`` — or ``climatic_season`` + ``dry_rainy_season``
       when ``climate_region`` is provided.

    All variable names are translated to ``lang`` (``"en"``, ``"pt"``, ``"es"``).

    Args:
        rel: Lazy DuckDB relation (after ``sus_data_standardize``).
        create_age_groups: Create age-derived variables. Default ``True``.
        age_breaks: Custom age break points. Default epidemiological (0,5,15,60,999).
        age_labels: Custom labels. Auto-generated if ``None``.
        create_calendar_vars: Create calendar variables. Default ``True``.
        create_climate_vars: Create season variables. Requires ``create_calendar_vars``.
        climate_region: Brazilian region for climatic seasons
            (``"norte"``, ``"nordeste"``, ``"centro-oeste"``, ``"sudeste"``, ``"sul"``).
            If ``None``, uses astronomical seasons.
        hemisphere: ``"south"`` (default) or ``"north"``.
        lang: Output language — ``"en"``, ``"pt"`` or ``"es"``.
        verbose: Print progress messages.

    Returns:
        Lazy DuckDB relation with new derived columns appended.

    Example:
        >>> rel   = cs.sus_data_import("SIM-DO", "SE", 2023)
        >>> clean = cs.sus_data_clean_encoding(rel)
        >>> stand = cs.sus_data_standardize(clean, lang="en")
        >>> final = cs.sus_data_create_variables(stand,
        ...     age_breaks=[0, 5, 15, 60, 999],
        ...     create_calendar_vars=True,
        ...     climate_region="nordeste",
        ...     lang="en")
    """
    _original_rel = rel

    if create_climate_vars and not create_calendar_vars:
        raise ValueError("create_climate_vars=True requires create_calendar_vars=True")
    if hemisphere not in ("south", "north"):
        raise ValueError("hemisphere must be 'south' or 'north'")
    if lang not in ("en", "pt", "es"):
        raise ValueError("lang must be 'en', 'pt' or 'es'")

    conn    = get_connection()
    columns = schema_columns(rel)
    cfg_age = _age_groups_config()
    cfg_cal = _calendar_config()

    # ------------------------------------------------------------------
    # BLOCK 1 — Age variables
    # ------------------------------------------------------------------
    if create_age_groups:
        age_col = next(
            (c for c in ("age_code", "age_years", "IDADE", "NU_IDADE_N") if c in columns),
            None
        )
        if age_col is None:
            raise ValueError(
                "Age column not found. Run sus_data_standardize() before this function."
            )
        if verbose:
            print(f"Age column: {age_col}")

        # generate labels automatically
        labels = age_labels if age_labels is not None else [
            f"{age_breaks[i]}+" if age_breaks[i + 1] == 999
            else f"{age_breaks[i]}-{age_breaks[i+1]-1}"
            for i in range(len(age_breaks) - 1)
        ]

        vn = cfg_age["variable_names"]
        decoded = decode_age_sql(age_col)

        # age_years
        all_cols = ", ".join(f'"{c}"' for c in columns)
        rel = conn.sql(
            f"SELECT {all_cols}, "
            f"({decoded}) AS {vn['age_years'][lang]} "
            f"FROM rel"
        )

        # age_group
        columns  = schema_columns(rel)
        all_cols = ", ".join(f'"{c}"' for c in columns)
        age_yr   = vn['age_years'][lang]
        rel = conn.sql(
            f"SELECT {all_cols}, "
            f"({_age_group_sql(age_yr, age_breaks, labels)}) AS {vn['age_group'][lang]} "
            f"FROM rel"
        )

        # climate_risk_group
        columns  = schema_columns(rel)
        all_cols = ", ".join(f'"{c}"' for c in columns)
        rel = conn.sql(
            f"SELECT {all_cols}, "
            f"({_climate_risk_sql(age_yr, lang)}) AS {vn['climate_risk_group'][lang]} "
            f"FROM rel"
        )

        # ibge_age_group
        columns  = schema_columns(rel)
        all_cols = ", ".join(f'"{c}"' for c in columns)
        rel = conn.sql(
            f"SELECT {all_cols}, "
            f"({_age_group_sql(age_yr, AGE_BREAKS_IBGE, AGE_LABELS_IBGE)}) AS {vn['ibge_age_group'][lang]} "
            f"FROM rel"
        )

        if verbose:
            print(f"✓ Age variables created: {[vn[k][lang] for k in vn]}")

    # ------------------------------------------------------------------
    # BLOCK 2 — Calendar variables
    # ------------------------------------------------------------------
    date_col = None
    if create_calendar_vars:
        date_col = _detect_date_col(schema_columns(rel))

        if date_col is None:
            if verbose:
                print("⚠ Date column not found — calendar variables skipped")
        else:
            if verbose:
                print(f"Date column: {date_col}")

            date_expr = f'TRY_CAST("{date_col}" AS DATE)'
            vn        = cfg_cal["variable_names"][lang]
            columns   = schema_columns(rel)
            all_cols  = ", ".join(f'"{c}"' for c in columns)
            month_expr = f"EXTRACT(MONTH FROM {date_expr})"

            rel = conn.sql(f"""
                SELECT {all_cols},
                    EXTRACT(YEAR    FROM {date_expr})                       AS "{vn['year']}",
                    EXTRACT(MONTH   FROM {date_expr})                       AS "{vn['month']}",
                    EXTRACT(QUARTER FROM {date_expr})                       AS "{vn['quarter']}",
                    EXTRACT(DOY     FROM {date_expr})                       AS "{vn['day_of_year']}",
                    ((DAYOFWEEK({date_expr}) + 6) % 7) + 1                 AS "{vn['day_of_week']}",
                    CASE WHEN ((DAYOFWEEK({date_expr}) + 6) % 7) + 1 >= 6
                         THEN TRUE ELSE FALSE END                           AS "{vn['is_weekend']}",
                    CASE WHEN EXTRACT(MONTH FROM {date_expr}) <= 6
                         THEN 1 ELSE 2 END                                  AS "{vn['semester']}",
                    ({_epi_week_sql(date_expr)})                            AS "{vn['epi_week']}",
                    ({_month_name_sql(month_expr, lang)})                   AS "{vn['month_name']}",
                    ({_day_name_sql(date_expr, lang)})                      AS "{vn['dow_name']}"
                FROM rel
            """)

            if verbose:
                print(f"✓ Calendar variables created: {list(vn.values())}")

    # ------------------------------------------------------------------
    # BLOCK 3 — Season variables
    # ------------------------------------------------------------------
    if create_climate_vars and date_col is not None:
        cfg_sea   = _seasonal_config()
        cal_vn    = cfg_cal["variable_names"][lang]
        month_col = f'"{cal_vn["month"]}"'

        if climate_region is None:
            # astronomical season
            season_vn = cfg_sea["astronomical"]["variable_names"][lang]
            columns   = schema_columns(rel)
            all_cols  = ", ".join(f'"{c}"' for c in columns)
            rel = conn.sql(
                f"SELECT {all_cols}, "
                f"({_astronomical_season_sql(month_col, lang, hemisphere)}) AS \"{season_vn}\" "
                f"FROM rel"
            )
            if verbose:
                print(f"✓ Season variable created: {season_vn}")
        else:
            # climatic season + dry/rainy
            cs_vn  = cfg_sea["climatic"]["variable_names"]["climatic_season"][lang]
            dry_vn = cfg_sea["climatic"]["variable_names"]["dry_rainy_season"][lang]
            columns  = schema_columns(rel)
            all_cols = ", ".join(f'"{c}"' for c in columns)
            rel = conn.sql(
                f"SELECT {all_cols}, "
                f"({_climatic_season_sql(month_col, climate_region, lang)}) AS \"{cs_vn}\", "
                f"({_dry_rainy_sql(month_col, climate_region, lang)}) AS \"{dry_vn}\" "
                f"FROM rel"
            )
            if verbose:
                print(f"✓ Climate variables created: {cs_vn}, {dry_vn}")

    # ------------------------------------------------------------------
    # sus_meta — registra stage e history
    # ------------------------------------------------------------------
    created = []
    if create_age_groups:
        cfg_vn = _age_groups_config()["variable_names"]
        created.extend([cfg_vn[k][lang] for k in cfg_vn])
    if create_calendar_vars and date_col:
        created.extend(list(_calendar_config()["variable_names"][lang].values()))
    if create_climate_vars and date_col:
        cfg_sea = _seasonal_config()
        if climate_region is None:
            created.append(cfg_sea["astronomical"]["variable_names"][lang])
        else:
            created.append(cfg_sea["climatic"]["variable_names"]["climatic_season"][lang])
            created.append(cfg_sea["climatic"]["variable_names"]["dry_rainy_season"][lang])

    history_msg = (
        f"Created derived variables ({lang.upper()}): {', '.join(created)}"
        if created else "No variables created (no age/date columns found)"
    )

    rel = rel.set_alias("variables")
    rel = set_stage(rel, "variables", _inherit_from=_original_rel)
    rel = add_history(rel, history_msg)

    return rel
