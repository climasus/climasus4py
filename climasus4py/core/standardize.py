"""Column standardization, value translation and date conversion.

Mirrors R: standardize.R
Pipeline stage: "standardize" (registered in sus_meta after processing).
"""

from __future__ import annotations

import duckdb

from ..utils.data import detect_system, load_json
from ._guards import _assert_lazy
from ._stage import add_history, set_stage
from .engine import get_connection, schema_columns


# ---------------------------------------------------------------------------
# Dictionary loaders
# ---------------------------------------------------------------------------

def _load_column_dict(lang: str, system: str | None = None) -> dict[str, str]:
    """Load column translation dictionary for the requested language.

    Loads COMMON first (lowest priority), then the system-specific section
    (highest priority) to avoid conflicts between systems that share column
    names with different meanings (e.g. PESO → birth_weight in SIM vs
    birth_weight_grams in SINASC).

    Args:
        lang: Language code — ``"pt"``, ``"en"`` or ``"es"``.
        system: SUS system identifier (e.g. ``"SIM-DO"``). Used to select
            the correct section from the JSON. If ``None``, all sections
            are merged (may cause conflicts for shared column names).

    Returns:
        Flat dict mapping original DATASUS column name to translated name.
        Empty dict if language file not found.
    """
    json_path = f"dictionaries/pt-{lang}/columns.json"
    try:
        data = load_json(json_path)
    except FileNotFoundError:
        return {}

    mapping: dict[str, str] = {}

    # COMMON always loaded first (lowest priority)
    common = data.get("COMMON", {})
    if isinstance(common, dict):
        mapping.update(common)

    if system is not None:
        # extract family: "SIM-DO" → "SIM", "SIH-RD" → "SIH", "SIA-PA" → "SIA"
        family = system.split("-")[0]
        specific = data.get(system, data.get(family, {}))
        if isinstance(specific, dict):
            mapping.update(specific)  # overrides COMMON
    else:
        # no system — merge all sections (may have conflicts)
        for section, cols in data.items():
            if not section.startswith("_") and section != "COMMON":
                if isinstance(cols, dict):
                    mapping.update(cols)

    return mapping


def _load_category_dict(lang: str) -> dict[str, dict[str, str]]:
    """Load category value translation dictionary for the requested language.

    Mirrors R: get_translation_dict_en()$values

    The keys are **translated** column names (applied after column rename),
    e.g. ``"sex"`` → ``{"1": "Male", "2": "Female"}``.

    Args:
        lang: Language code — ``"pt"``, ``"en"`` or ``"es"``.

    Returns:
        Dict mapping translated_column_name → {original_value → label}.
        Empty dict if language file not found.
    """
    json_path = f"dictionaries/pt-{lang}/categories.json"
    try:
        data = load_json(json_path)
    except FileNotFoundError:
        return {}

    return {k: v for k, v in data.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_standardize(
    rel: duckdb.DuckDBPyRelation,
    *,
    lang: str = "en",
    system: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Standardize column names, translate categorical values and convert dates.

    Mirrors ``climasus4r::sus_data_standardize()``. Three operations in order:

    1. **Column rename** — maps DATASUS uppercase names to readable names in
       the requested language using ``dictionaries/pt-{lang}/columns.json``.
       Uses system-specific section to avoid conflicts between systems sharing
       column names (e.g. PESO means different things in SIM vs SINASC).
    2. **Value translation** — maps coded values (e.g. SEXO ``"1"`` → ``"Male"``)
       using ``dictionaries/pt-{lang}/categories.json`` (applied after rename).
    3. **Date conversion** — converts DATASUS DDMMYYYY strings to DATE type
       for known date columns.

    Registers ``stage="standardize"`` and a timestamped history entry in
    ``sus_meta``, propagating ``system`` and ``type`` from the previous stage.

    Args:
        rel: Lazy DuckDB relation (must be at least at stage ``"import"``).
        lang: Target language — ``"en"`` (default), ``"pt"`` or ``"es"``.
            ``"pt"`` renames to Portuguese snake_case (e.g. ``data_obito``).
            ``"en"`` renames to English (e.g. ``death_date``).
        system: Override system detection (e.g. ``"SIM-DO"``). If ``None``,
            auto-detected from column signatures.

    Returns:
        Lazy DuckDB relation with standardized column names, translated values
        and converted dates, registered at stage ``"standardize"`` in sus_meta.

    Example:
        >>> import climasus4py as cs
        >>> rel    = cs.sus_data_import("SIM-DO", "SE", 2023)
        >>> clean  = cs.sus_data_clean_encoding(rel)
        >>> stand  = cs.sus_data_standardize(clean, lang="en")
        >>> cs.sus_meta(stand, "stage")
        'standardize'
    """
    _assert_lazy(rel)

    # capture original rel before any transformation
    _original_rel = rel

    columns = schema_columns(rel)

    if system is None:
        system = detect_system(columns)
        if system is None:
            alias = getattr(rel, 'alias', None)
            if alias and alias != 'unknown':
                system = alias

    # ------------------------------------------------------------------
    # 1. Column rename — system-specific to avoid cross-system conflicts
    # ------------------------------------------------------------------
    col_map = _load_column_dict(lang, system=system)
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

    # ------------------------------------------------------------------
    # 2. Date conversion (DDMMYYYY → DATE)
    # ------------------------------------------------------------------
    new_columns = schema_columns(rel)
    date_candidates = [
        # EN
        "death_date", "birth_date", "admission_date", "discharge_date",
        "notification_date", "first_symptom_date", "investigation_date",
        "case_conclusion_date", "registration_date", "receipt_date",
        "reception_date", "investigation_conclusion_date",
        "investigation_registration_date", "certificate_date",
        "digitization_date",
        # PT snake_case
        "data_obito", "data_nascimento", "data_internacao", "data_saida",
        "data_notificacao", "data_primeiro_sintoma", "data_investigacao",
        "data_conclusao_caso", "data_cadastro", "data_recebimento",
        # ES
        "fecha_muerte", "fecha_nacimiento", "fecha_ingreso", "fecha_alta",
        "fecha_notificacion", "fecha_primeros_sintomas",
        # DATASUS original (if not renamed)
        "DTOBITO", "DTNASC", "DT_INTER", "DT_SAIDA",
        "DT_NOTIFIC", "DT_SIN_PRI", "DTINVESTIG", "DTCONCASO",
        "DTCADASTRO", "DTRECEBIM",
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
            new_columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # 3. Value translation (categorical labels)
    # Applied AFTER rename — keys in categories.json are translated names
    # ------------------------------------------------------------------
    cat_map = _load_category_dict(lang)

    if cat_map:
        new_columns = schema_columns(rel)
        cat_cols = [c for c in new_columns if c in cat_map]

        if cat_cols:
            cat_projections = []
            for col in new_columns:
                if col in cat_map:
                    cases = " ".join([
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = '{k}' THEN '{v}' "
                        f"WHEN CAST(TRY_CAST(\"{col}\" AS INTEGER) AS VARCHAR) = '{k}' THEN '{v}'"
                        for k, v in cat_map[col].items()
                    ])
                    cat_projections.append(
                        f"CASE "
                        f"WHEN \"{col}\" IS NULL THEN NULL "
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = 'None' THEN NULL "
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = 'nan' THEN NULL "
                        f"{cases} "
                        f"ELSE CAST(\"{col}\" AS VARCHAR) "
                        f"END AS \"{col}\""
                    )
                else:
                    cat_projections.append(f'"{col}"')

            conn = get_connection()
            rel = conn.sql(
                f"SELECT {', '.join(cat_projections)} FROM rel"
            )

    # ------------------------------------------------------------------
    # 4. Register stage and history — mirrors climasus4r
    # set_alias before set_stage to avoid losing meta on new DuckDB object
    # ------------------------------------------------------------------
    n_renamed  = len(renames)
    n_cat_cols = len([c for c in schema_columns(rel) if c in cat_map]) if cat_map else 0
    history_msg = (
        f"Standardized column names and types to {lang.upper()}; "
        f"{n_renamed} columns renamed; "
        f"{n_cat_cols} categorical columns translated"
    )

    rel = rel.set_alias(system or "unknown")
    rel = set_stage(rel, "standardize", _inherit_from=_original_rel)
    rel = add_history(rel, history_msg)

    return rel
