"""Socioeconomic and epidemiological indicator formulas.

Mirrors R: sus_socio_compute_indicators.R

Not lazy-in-SQL: several indicators need Poisson (Garwood exact) or
Wilson-score confidence intervals via ``scipy.stats`` (gamma/normal
quantile functions), which DuckDB has no SQL builtin for — exactly the
same reason ``climate_spei.py`` / ``climate_spi.py`` materialise instead
of staying lazy. A ``DuckDBPyRelation`` input is therefore materialised
to ``pandas`` (with a ``UserWarning``, same as those siblings) and the
result is returned as a ``pandas.DataFrame`` with pipeline metadata in
``.attrs["sus_meta"]``.

The indicator catalogue below (formula, required columns, category,
unit, uncertainty method) is domain reference data. It does **not**
exist yet in ``climasus-data`` (checked 2026-08-13: no socio/indicator
catalogue file there) — this hardcodes the exact same static table the
R source itself hardcodes (``.sus_indicators_catalog``), as a stopgap.
Flagged in IDEIAS.md as a candidate to migrate to ``climasus-data`` so
R and Python share one source of truth instead of two independently
maintained copies.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import duckdb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Internal indicators catalog (mirrors R's .sus_indicators_catalog exactly)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _IndicatorSpec:
    name_pt: str
    name_en: str
    name_es: str
    category: str
    formula: str
    required_cols: tuple[str, ...]
    multiplier: float
    unit: str
    uncertainty_method: Literal["none", "poisson", "binomial"]
    numerator_col: str | None
    denominator_col: str | None
    source: str


_CATALOG: dict[str, _IndicatorSpec] = {
    # -- Demographic ---------------------------------------------------
    "dependency_ratio": _IndicatorSpec(
        name_pt="Razão de Dependência",
        name_en="Dependency Ratio",
        name_es="Razón de Dependencia",
        category="demographic",
        formula="(pop_young + pop_elderly) / pop_working",
        required_cols=("pop_young", "pop_elderly", "pop_working"),
        multiplier=100,
        unit="%",
        uncertainty_method="none",
        numerator_col=None,
        denominator_col=None,
        source="census",
    ),
    "aging_index": _IndicatorSpec(
        name_pt="Índice de Envelhecimento",
        name_en="Aging Index",
        name_es="Índice de Envejecimiento",
        category="demographic",
        formula="pop_elderly / pop_young",
        required_cols=("pop_elderly", "pop_young"),
        multiplier=100,
        unit="%",
        uncertainty_method="none",
        numerator_col=None,
        denominator_col=None,
        source="census",
    ),
    "urbanization_rate": _IndicatorSpec(
        name_pt="Taxa de Urbanização",
        name_en="Urbanization Rate",
        name_es="Tasa de Urbanización",
        category="demographic",
        formula="pop_urban / pop_total",
        required_cols=("pop_urban", "pop_total"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="pop_urban",
        denominator_col="pop_total",
        source="census",
    ),
    # -- Socioeconomic --------------------------------------------------
    "illiteracy_rate": _IndicatorSpec(
        name_pt="Taxa de Analfabetismo",
        name_en="Illiteracy Rate",
        name_es="Tasa de Analfabetismo",
        category="socioeconomic",
        formula="pop_illiterate / pop_15_plus",
        required_cols=("pop_illiterate", "pop_15_plus"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="pop_illiterate",
        denominator_col="pop_15_plus",
        source="census",
    ),
    "water_connection_rate": _IndicatorSpec(
        name_pt="Cobertura de Água Encanada",
        name_en="Water Connection Rate",
        name_es="Cobertura de Agua Entubada",
        category="socioeconomic",
        formula="hh_water / total_hh",
        required_cols=("hh_water", "total_hh"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="hh_water",
        denominator_col="total_hh",
        source="census",
    ),
    "sewage_connection_rate": _IndicatorSpec(
        name_pt="Cobertura de Esgoto",
        name_en="Sewage Connection Rate",
        name_es="Cobertura de Alcantarillado",
        category="socioeconomic",
        formula="hh_sewage / total_hh",
        required_cols=("hh_sewage", "total_hh"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="hh_sewage",
        denominator_col="total_hh",
        source="census",
    ),
    "gini_index": _IndicatorSpec(
        name_pt="Índice de Gini",
        name_en="Gini Index",
        name_es="Índice de Gini",
        category="socioeconomic",
        formula="gini_value",
        required_cols=("gini_value",),
        multiplier=1,
        unit="index (0-1)",
        uncertainty_method="none",
        numerator_col=None,
        denominator_col=None,
        source="census",
    ),
    # -- Mortality --------------------------------------------------
    "infant_mortality_rate": _IndicatorSpec(
        name_pt="Taxa de Mortalidade Infantil",
        name_en="Infant Mortality Rate",
        name_es="Tasa de Mortalidad Infantil",
        category="mortality",
        formula="deaths_infant / live_births",
        required_cols=("deaths_infant", "live_births"),
        multiplier=1000,
        unit="per 1,000 live births",
        uncertainty_method="poisson",
        numerator_col="deaths_infant",
        denominator_col="live_births",
        source="sim_sinasc",
    ),
    "maternal_mortality_ratio": _IndicatorSpec(
        name_pt="Razão de Mortalidade Materna",
        name_en="Maternal Mortality Ratio",
        name_es="Razón de Mortalidad Materna",
        category="mortality",
        formula="deaths_maternal / live_births",
        required_cols=("deaths_maternal", "live_births"),
        multiplier=100_000,
        unit="per 100,000 live births",
        uncertainty_method="poisson",
        numerator_col="deaths_maternal",
        denominator_col="live_births",
        source="sim_sinasc",
    ),
    "premature_dcn_mortality": _IndicatorSpec(
        name_pt="Mortalidade Prematura por DCNT",
        name_en="Premature NCD Mortality Rate",
        name_es="Mortalidad Prematura por ECNT",
        category="mortality",
        formula="deaths_dcn_30_69 / pop_30_69",
        required_cols=("deaths_dcn_30_69", "pop_30_69"),
        multiplier=100_000,
        unit="per 100,000 (30-69 years)",
        uncertainty_method="poisson",
        numerator_col="deaths_dcn_30_69",
        denominator_col="pop_30_69",
        source="sim",
    ),
    "homicide_rate": _IndicatorSpec(
        name_pt="Taxa de Homicídios",
        name_en="Homicide Rate",
        name_es="Tasa de Homicidios",
        category="mortality",
        formula="deaths_homicide / pop_total",
        required_cols=("deaths_homicide", "pop_total"),
        multiplier=100_000,
        unit="per 100,000",
        uncertainty_method="poisson",
        numerator_col="deaths_homicide",
        denominator_col="pop_total",
        source="sim",
    ),
    "traffic_mortality_rate": _IndicatorSpec(
        name_pt="Taxa de Mortalidade por Trânsito",
        name_en="Traffic Mortality Rate",
        name_es="Tasa de Mortalidad por Tránsito",
        category="mortality",
        formula="deaths_traffic / pop_total",
        required_cols=("deaths_traffic", "pop_total"),
        multiplier=100_000,
        unit="per 100,000",
        uncertainty_method="poisson",
        numerator_col="deaths_traffic",
        denominator_col="pop_total",
        source="sim",
    ),
    # -- Morbidity --------------------------------------------------
    "arbovirus_incidence_rate": _IndicatorSpec(
        name_pt="Taxa de Incidência de Arboviroses",
        name_en="Arbovirus Incidence Rate",
        name_es="Tasa de Incidencia de Arbovirus",
        category="morbidity",
        formula="cases_arbovirus / pop_total",
        required_cols=("cases_arbovirus", "pop_total"),
        multiplier=100_000,
        unit="per 100,000",
        uncertainty_method="poisson",
        numerator_col="cases_arbovirus",
        denominator_col="pop_total",
        source="sinan",
    ),
    "tb_incidence_rate": _IndicatorSpec(
        name_pt="Taxa de Incidência de Tuberculose",
        name_en="TB Incidence Rate",
        name_es="Tasa de Incidencia de Tuberculosis",
        category="morbidity",
        formula="cases_tb_new / pop_total",
        required_cols=("cases_tb_new", "pop_total"),
        multiplier=100_000,
        unit="per 100,000",
        uncertainty_method="poisson",
        numerator_col="cases_tb_new",
        denominator_col="pop_total",
        source="sinan",
    ),
    "icsap_hospitalization_rate": _IndicatorSpec(
        name_pt="Taxa de Internações por ICSAP",
        name_en="ACSC Hospitalization Rate",
        name_es="Tasa de Hospitalizaciones por ICSAP",
        category="morbidity",
        formula="hosp_icsap / pop_total",
        required_cols=("hosp_icsap", "pop_total"),
        multiplier=10_000,
        unit="per 10,000",
        uncertainty_method="poisson",
        numerator_col="hosp_icsap",
        denominator_col="pop_total",
        source="sih",
    ),
    "respiratory_hospitalization_rate": _IndicatorSpec(
        name_pt="Taxa de Internações Respiratórias",
        name_en="Respiratory Hospitalization Rate",
        name_es="Tasa de Hospitalizaciones Respiratorias",
        category="morbidity",
        formula="hosp_resp / pop_total",
        required_cols=("hosp_resp", "pop_total"),
        multiplier=10_000,
        unit="per 10,000",
        uncertainty_method="poisson",
        numerator_col="hosp_resp",
        denominator_col="pop_total",
        source="sih",
    ),
    # -- Maternal-child health -------------------------------------
    "low_birth_weight_proportion": _IndicatorSpec(
        name_pt="Proporção de Baixo Peso ao Nascer",
        name_en="Low Birth Weight Proportion",
        name_es="Proporción de Bajo Peso al Nacer",
        category="maternal_child",
        formula="births_low_weight / live_births",
        required_cols=("births_low_weight", "live_births"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="births_low_weight",
        denominator_col="live_births",
        source="sinasc",
    ),
    "general_fertility_rate": _IndicatorSpec(
        name_pt="Taxa de Fecundidade Geral",
        name_en="General Fertility Rate",
        name_es="Tasa de Fecundidad General",
        category="maternal_child",
        formula="live_births / pop_women_15_49",
        required_cols=("live_births", "pop_women_15_49"),
        multiplier=1000,
        unit="per 1,000 women (15-49 years)",
        uncertainty_method="poisson",
        numerator_col="live_births",
        denominator_col="pop_women_15_49",
        source="sinasc",
    ),
    "cesarean_proportion": _IndicatorSpec(
        name_pt="Proporção de Partos Cesáreos",
        name_en="Cesarean Delivery Proportion",
        name_es="Proporción de Partos por Cesárea",
        category="maternal_child",
        formula="deliveries_cesarean / total_deliveries",
        required_cols=("deliveries_cesarean", "total_deliveries"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="deliveries_cesarean",
        denominator_col="total_deliveries",
        source="sinasc",
    ),
    "prenatal_early_coverage": _IndicatorSpec(
        name_pt="Cobertura de Pré-natal Precoce",
        name_en="Early Prenatal Care Coverage",
        name_es="Cobertura de Control Prenatal Precoz",
        category="maternal_child",
        formula="prenatal_early / live_births",
        required_cols=("prenatal_early", "live_births"),
        multiplier=100,
        unit="%",
        uncertainty_method="binomial",
        numerator_col="prenatal_early",
        denominator_col="live_births",
        source="sinasc",
    ),
    # -- Health resources --------------------------------------------
    "beds_per_capita": _IndicatorSpec(
        name_pt="Leitos por Mil Habitantes",
        name_en="Hospital Beds per 1,000",
        name_es="Camas por 1.000 Habitantes",
        category="health_resources",
        formula="cnes_beds / pop_total",
        required_cols=("cnes_beds", "pop_total"),
        multiplier=1000,
        unit="per 1,000",
        uncertainty_method="none",
        numerator_col=None,
        denominator_col=None,
        source="cnes",
    ),
    "doctors_per_capita": _IndicatorSpec(
        name_pt="Médicos por Mil Habitantes",
        name_en="Doctors per 1,000",
        name_es="Médicos por 1.000 Habitantes",
        category="health_resources",
        formula="cnes_doctors / pop_total",
        required_cols=("cnes_doctors", "pop_total"),
        multiplier=1000,
        unit="per 1,000",
        uncertainty_method="none",
        numerator_col=None,
        denominator_col=None,
        source="cnes",
    ),
}

_LANG_ATTR = {"pt": "name_pt", "en": "name_en", "es": "name_es"}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "unsupported_lang": "lang deve ser 'pt', 'en' ou 'es'. Usando 'pt'.",
        "no_indicators": (
            "Nenhum indicador disponível com as colunas presentes. "
            "Use col_mapping para mapear suas colunas."
        ),
        "no_valid_indicators": (
            "Nenhum dos indicadores solicitados é válido. "
            "Use sus_socio_list_indicators() para ver os IDs disponíveis."
        ),
        "unknown_indicators": "IDs desconhecidos ignorados: {ids}",
        "skipping_missing": (
            "Pulando '{id}': colunas ausentes: {cols}. "
            "Use col_mapping para mapear suas colunas."
        ),
        "formula_error": "Erro ao avaliar fórmula do indicador '{id}': {err}",
        "nothing_computed": "Nenhum indicador foi calculado com sucesso.",
        "materialize_warning": (
            "sus_socio_compute_indicators: a DuckDBPyRelation de entrada está "
            "sendo materializada para o cálculo com pandas/scipy — este cálculo "
            "não é expressável em SQL lazy (intervalos de confiança usam "
            "scipy.stats.gamma/norm)."
        ),
    },
    "en": {
        "unsupported_lang": "lang must be 'pt', 'en', or 'es'. Defaulting to 'pt'.",
        "no_indicators": (
            "No indicators available with the current columns. "
            "Use col_mapping to map your column names."
        ),
        "no_valid_indicators": (
            "None of the requested indicators are valid. "
            "Use sus_socio_list_indicators() to see available IDs."
        ),
        "unknown_indicators": "Unknown indicator IDs skipped: {ids}",
        "skipping_missing": (
            "Skipping '{id}': missing columns: {cols}. "
            "Use col_mapping to map your columns."
        ),
        "formula_error": "Error evaluating formula for indicator '{id}': {err}",
        "nothing_computed": "No indicators were successfully computed.",
        "materialize_warning": (
            "sus_socio_compute_indicators: the input DuckDBPyRelation is being "
            "materialised for the pandas/scipy computation — this cannot be "
            "expressed as lazy SQL (confidence intervals use "
            "scipy.stats.gamma/norm)."
        ),
    },
    "es": {
        "unsupported_lang": "lang debe ser 'pt', 'en' o 'es'. Usando 'pt'.",
        "no_indicators": (
            "Ningún indicador disponible con las columnas presentes. "
            "Use col_mapping para mapear sus columnas."
        ),
        "no_valid_indicators": (
            "Ninguno de los indicadores solicitados es válido. "
            "Use sus_socio_list_indicators() para ver los IDs disponibles."
        ),
        "unknown_indicators": "IDs desconocidos ignorados: {ids}",
        "skipping_missing": (
            "Omitiendo '{id}': columnas faltantes: {cols}. "
            "Use col_mapping para mapear sus columnas."
        ),
        "formula_error": "Error al evaluar la fórmula del indicador '{id}': {err}",
        "nothing_computed": "Ningún indicador fue calculado con éxito.",
        "materialize_warning": (
            "sus_socio_compute_indicators: la DuckDBPyRelation de entrada se "
            "está materializando para el cálculo con pandas/scipy — este "
            "cálculo no es expresable en SQL lazy (los intervalos de "
            "confianza usan scipy.stats.gamma/norm)."
        ),
    },
}


# ---------------------------------------------------------------------------
# Internal helpers (mirror the R dot-prefixed helpers 1:1)
# ---------------------------------------------------------------------------


def _resolve_indicator_cols(
    required_cols: tuple[str, ...],
    col_mapping: dict[str, str],
    df_cols: set[str],
) -> tuple[dict[str, str], list[str]]:
    """Resolve formula-space names to actual DataFrame columns.

    Mirrors R's ``.resolve_indicator_cols``.

    Returns:
        Tuple of (resolved mapping formula-name -> actual column,
        list of actual column names that are missing from *df_cols*).
    """
    resolved = {fn: col_mapping.get(fn, fn) for fn in required_cols}
    missing = [actual for actual in resolved.values() if actual not in df_cols]
    return resolved, missing


def _available_indicators(df_cols: set[str], col_mapping: dict[str, str]) -> list[str]:
    """Return indicator IDs computable given available columns (R's ``.available_indicators``)."""
    out = []
    for ind_id, spec in _CATALOG.items():
        _, missing = _resolve_indicator_cols(spec.required_cols, col_mapping, df_cols)
        if not missing:
            out.append(ind_id)
    return out


def _poisson_ci(
    k: pd.Series, n: pd.Series, conf_level: float, multiplier: float
) -> tuple[pd.Series, pd.Series]:
    """Poisson Garwood exact CI, vectorised (mirrors R's ``.poisson_ci``)."""
    from scipy import stats

    alpha = 1 - conf_level
    k = pd.to_numeric(k, errors="coerce")
    n = pd.to_numeric(n, errors="coerce")
    valid = k.notna() & n.notna() & (n > 0)

    low = pd.Series(np.nan, index=k.index, dtype="float64")
    high = pd.Series(np.nan, index=k.index, dtype="float64")

    k_v = k[valid]
    n_v = n[valid]
    low_v = np.where(
        k_v == 0, 0.0, stats.gamma.ppf(alpha / 2, a=k_v, scale=1.0)
    ) / n_v * multiplier
    high_v = stats.gamma.ppf(1 - alpha / 2, a=k_v + 1, scale=1.0) / n_v * multiplier

    low.loc[valid] = low_v
    high.loc[valid] = high_v
    return low, high


def _binomial_ci(
    n: pd.Series, p: pd.Series, conf_level: float, multiplier: float
) -> tuple[pd.Series, pd.Series]:
    """Binomial Wilson score CI, vectorised (mirrors R's ``.binomial_ci``).

    *p* is the raw (0-1) proportion, not yet scaled by *multiplier*.
    """
    from scipy import stats

    z = stats.norm.ppf(1 - (1 - conf_level) / 2)
    n = pd.to_numeric(n, errors="coerce")
    p = pd.to_numeric(p, errors="coerce")
    valid = n.notna() & p.notna() & (n > 0) & (p >= 0) & (p <= 1)

    low = pd.Series(np.nan, index=n.index, dtype="float64")
    high = pd.Series(np.nan, index=n.index, dtype="float64")

    n_v = n[valid]
    p_v = p[valid]
    z2n = z**2 / n_v
    denom = 1 + z2n
    ctr = (p_v + z2n / 2) / denom
    marg = z * np.sqrt(p_v * (1 - p_v) / n_v + z2n**2 / 4) / denom

    low.loc[valid] = np.maximum(0, ctr - marg) * multiplier
    high.loc[valid] = np.minimum(1, ctr + marg) * multiplier
    return low, high


def _eval_formula(formula: str, env: dict[str, pd.Series]) -> pd.Series:
    """Evaluate a trusted catalogue formula string against resolved columns.

    Mirrors R's ``rlang::eval_tidy(rlang::parse_expr(spec$formula), data =
    env_list)``. *formula* always comes from the internal, developer-owned
    ``_CATALOG`` (never from user input), so a restricted ``eval()`` with no
    builtins is the direct Python equivalent of R's tidy-eval here — not a
    general-purpose expression parser exposed to callers.
    """
    return eval(formula, {"__builtins__": {}}, dict(env))  # noqa: S307


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def sus_socio_compute_indicators(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    indicators: list[str] | None = None,
    col_mapping: dict[str, str] | None = None,
    confidence_level: float = 0.95,
    add_ci: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Compute socioeconomic and epidemiological indicators.

    Computes a set of standardised indicators (demographic, socioeconomic
    vulnerability, mortality, morbidity, maternal-child health, and
    health-resource availability) from columns already present in *df* —
    typically the output of ``sus_census()`` / ``sus_spatial_join()``.
    Column names produced by upstream aggregation steps (e.g.
    ``V003_sum``) can be mapped to the formula-space names each indicator
    expects via *col_mapping*.

    Mirrors ``climasus4r::sus_socio_compute_indicators``.

    Formula evaluation: each catalogue formula (e.g.
    ``"(pop_young + pop_elderly) / pop_working"``) is evaluated against the
    resolved column values — the Python equivalent of R's
    ``rlang::eval_tidy(rlang::parse_expr(...))``.

    Missing columns never abort the function: a requested indicator whose
    required columns (after *col_mapping*) are not found in *df* is
    skipped with a warning, exactly like the R source.

    Confidence intervals:
        - Poisson (Garwood exact): event-count / population-at-risk rates
          (mortality, incidence, hospitalisation).
        - Binomial (Wilson score): proportions (%, coverage indices).
        - None: ratios with a compound numerator or direct index values.

    Args:
        df: Input health/socioeconomic data — lazy ``DuckDBPyRelation``
            (materialised internally, with a ``UserWarning``) or
            ``pandas.DataFrame``.
        indicators: Indicator IDs to compute. When ``None`` (default),
            every indicator whose required columns are present in *df*
            is computed automatically. Use
            ``sus_socio_list_indicators()`` to inspect the catalogue.
        col_mapping: Maps formula-space variable names to actual column
            names in *df*, e.g.
            ``{"pop_young": "pop_0_14_sum", "total_hh": "V003_sum"}``.
            Only overrides need to be specified.
        confidence_level: Confidence level for Poisson/Binomial
            intervals. Default ``0.95``.
        add_ci: If ``True`` (default), adds ``*_low``/``*_high`` columns
            for indicators with ``uncertainty_method != "none"``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        *df* (materialised) with additional ``ind_<id>`` columns and, for
        Poisson/Binomial indicators when *add_ci* is ``True``,
        ``ind_<id>_low`` / ``ind_<id>_high``. Pipeline metadata is
        recorded in ``.attrs["sus_meta"]`` (``stage="census"``,
        ``type="indicators"``).

    Raises:
        TypeError: If *df* is not a ``DuckDBPyRelation`` or
            ``pandas.DataFrame``.
        ValueError: If *col_mapping* is not a dict, *confidence_level* is
            outside ``(0, 1)``, no indicators are available/valid, or no
            indicator was successfully computed.

    Examples::

        import climasus4py as cs

        df_ind = cs.sus_socio_compute_indicators(
            df_census,
            indicators=["dependency_ratio", "water_connection_rate"],
            col_mapping={
                "pop_young": "pop_0_14_sum",
                "pop_elderly": "pop_65_plus_sum",
                "pop_working": "pop_15_64_sum",
                "hh_water": "V111_sum",
                "total_hh": "V003_sum",
            },
        )
    """
    if lang not in ("pt", "en", "es"):
        warnings.warn(_MESSAGES["pt"]["unsupported_lang"], UserWarning, stacklevel=2)
        lang = "pt"
    msg = _MESSAGES[lang]

    col_mapping = {} if col_mapping is None else col_mapping
    if not isinstance(col_mapping, dict):
        raise ValueError("col_mapping must be a dict.")

    if (
        not isinstance(confidence_level, (int, float))
        or confidence_level <= 0
        or confidence_level >= 1
    ):
        raise ValueError("confidence_level must be a number between 0 and 1.")

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    elif isinstance(df, pd.DataFrame):
        data = df.copy()
    else:
        raise TypeError(
            f"Expected DuckDBPyRelation or pandas.DataFrame, got {type(df).__name__}."
        )

    df_cols = set(data.columns)

    # -----------------------------------------------------------------
    # Resolve which indicators to compute
    # -----------------------------------------------------------------
    if indicators is None:
        indicators_to_compute = _available_indicators(df_cols, col_mapping)
        if not indicators_to_compute:
            raise ValueError(msg["no_indicators"])
    else:
        unknown = sorted(set(indicators) - set(_CATALOG))
        if unknown:
            warnings.warn(
                msg["unknown_indicators"].format(ids=", ".join(unknown)),
                UserWarning,
                stacklevel=2,
            )
        indicators_to_compute = [i for i in indicators if i in _CATALOG]
        if not indicators_to_compute:
            raise ValueError(msg["no_valid_indicators"])

    # -----------------------------------------------------------------
    # Compute each indicator
    # -----------------------------------------------------------------
    n_computed = 0
    n_ci = 0

    for ind_id in indicators_to_compute:
        spec = _CATALOG[ind_id]

        resolved, missing = _resolve_indicator_cols(
            spec.required_cols, col_mapping, df_cols
        )
        if missing:
            warnings.warn(
                msg["skipping_missing"].format(id=ind_id, cols=", ".join(missing)),
                UserWarning,
                stacklevel=2,
            )
            continue

        env = {fn: data[actual] for fn, actual in resolved.items()}

        try:
            value = _eval_formula(spec.formula, env)
        except Exception as exc:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
            warnings.warn(
                msg["formula_error"].format(id=ind_id, err=str(exc)),
                UserWarning,
                stacklevel=2,
            )
            continue

        data[f"ind_{ind_id}"] = value * spec.multiplier
        n_computed += 1

        if add_ci and spec.uncertainty_method != "none":
            num_vals = env[spec.numerator_col]
            denom_vals = env[spec.denominator_col]

            if spec.uncertainty_method == "poisson":
                low, high = _poisson_ci(
                    num_vals, denom_vals, confidence_level, spec.multiplier
                )
            else:  # "binomial" — p is the raw (pre-multiplier) proportion
                low, high = _binomial_ci(
                    denom_vals, value, confidence_level, spec.multiplier
                )
            data[f"ind_{ind_id}_low"] = low
            data[f"ind_{ind_id}_high"] = high
            n_ci += 1

    if n_computed == 0:
        raise ValueError(msg["nothing_computed"])

    # -----------------------------------------------------------------
    # Metadata
    # -----------------------------------------------------------------
    # NOTE (preserved R bug — see IDEIAS.md): the R source builds the history
    # *step* string with `glue::glue("[%s] Computed ...")`. `glue()` only
    # substitutes `{...}` placeholders, not `%s`, so the literal text
    # "[%s]" survives untouched inside the step string. That step string is
    # then handed to `sus_meta(..., add_history = step)`, whose generic
    # internal helper (`add_climasus_history_internal`, utils-S3.R) always
    # prepends a REAL timestamp via
    # `sprintf("[%s] %s", format(Sys.time(), ...), step)` — so the final
    # entry is double-bracketed: a genuine timestamp from the generic
    # wrapper, followed by the never-substituted literal "[%s]" baked into
    # the step text itself. It also slices
    # `indicators_to_compute[seq_len(n_computed)]` (the first n_computed
    # *requested* ids), not the ids that actually succeeded, so the listed
    # names can be wrong whenever an earlier indicator in the list was
    # skipped. Both quirks are replicated as-is.
    computed_ids = indicators_to_compute[:n_computed]
    step = f"[%s] Computed {n_computed} indicator(s): {', '.join(computed_ids)}"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_entry = f"[{now_str}] {step}"
    existing_meta: dict = data.attrs.get("sus_meta", {})
    data.attrs["sus_meta"] = {
        **existing_meta,
        "stage": "census",
        "type": "indicators",
        "history": [*existing_meta.get("history", []), history_entry],
    }

    if verbose:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] sus_socio_compute_indicators: {n_computed} indicator(s), {n_ci} with CI.")

    return data


def sus_socio_list_indicators(
    lang: Literal["pt", "en", "es"] = "pt",
    category: str | list[str] | None = None,
) -> pd.DataFrame:
    """List the indicator catalogue used by ``sus_socio_compute_indicators``.

    Mirrors ``climasus4r::sus_socio_list_indicators``.

    Args:
        lang: Language for the ``name`` column: ``"pt"`` (default),
            ``"en"``, ``"es"``.
        category: Optional category (or list of categories) to filter
            by. ``None`` (default) returns every category.

    Returns:
        DataFrame with columns ``id``, ``name``, ``category``,
        ``required_cols``, ``formula``, ``multiplier``, ``unit``,
        ``uncertainty_method``, ``source`` — one row per indicator.

    Examples::

        import climasus4py as cs

        cs.sus_socio_list_indicators(lang="pt")
        cs.sus_socio_list_indicators(lang="en", category="mortality")
    """
    if lang not in _LANG_ATTR:
        lang = "pt"
    name_attr = _LANG_ATTR[lang]

    rows = [
        {
            "id": ind_id,
            "name": getattr(spec, name_attr),
            "category": spec.category,
            "required_cols": ", ".join(spec.required_cols),
            "formula": spec.formula,
            "multiplier": spec.multiplier,
            "unit": spec.unit,
            "uncertainty_method": spec.uncertainty_method,
            "source": spec.source,
        }
        for ind_id, spec in _CATALOG.items()
    ]
    out = pd.DataFrame(rows)

    if category is not None:
        categories = [category] if isinstance(category, str) else list(category)
        out = out[out["category"].isin(categories)].reset_index(drop=True)

    return out
