"""Distributed Lag Non-linear Model (DLNM) for climate-health analyses.

Mirrors R: sus_mod_dlnm.R

Theory: Gasparrini et al. (2010, 2011, 2014); Armstrong (2006);
Bhaskaran et al. (2013).

Not lazy — operates on an in-memory ``pandas.DataFrame`` (the Python analog
of R's ``climasus_df`` at stage ``"climate"``, type ``"distributed_lag"``,
produced by ``sus_climate_aggregate(temporal_strategy="distributed_lag")``
in R). ``climasus4py``'s own ``sus_climate_aggregate`` does not yet emit
this shape (see IDEIAS.md) — callers must supply a ``date`` column plus
``{climate_col}_lag0..lag{L}`` columns themselves.

R's ``dlnm`` package (``crossbasis``/``crosspred``) has no Python binding.
This module re-implements, from first principles and validated numerically
against the real R package, the exact pieces ``sus_mod_dlnm`` needs:
  - a natural cubic spline basis matching ``splines::ns()``'s knot
    placement and boundary behaviour (``_ns_basis``),
  - the bidimensional (exposure x lag) cross-basis construction
    (``_crossbasis``), matching ``dlnm::crossbasis``,
  - the centred prediction grid with cumulative-over-lag and per-lag
    relative risks and Wald confidence intervals (``_crosspred``),
    matching ``dlnm::crosspred``.

The GLM coefficients Python fits will *not* numerically equal R's (a
different, but equivalent, basis parametrisation of the same natural
cubic spline space is used) — but because a GLM's fitted values are
invariant to a linear reparametrisation of a group of predictor columns,
the exposure-response / lag-response outputs (RR, CI) match R's
``sus_mod_dlnm()`` output to floating-point precision on a synthetic
validation dataset (see ``IDEIAS.md``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)


def _require_statsmodels():
    try:
        import statsmodels.api as sm
    except ImportError as exc:
        raise ImportError(
            "statsmodels is required for sus_mod_dlnm() (quasi-Poisson GLM "
            "fitting). Install it with: pip install statsmodels"
        ) from exc
    return sm


def _require_scipy_stats():
    try:
        from scipy import stats as sp_stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required for sus_mod_dlnm() (Wald confidence "
            "intervals). Install it with: pip install scipy"
        ) from exc
    return sp_stats

_KNOWN_CLIMATE_VARS: tuple[str, ...] = (
    "tair_dry_bulb_c",
    "tair_max_c",
    "tair_min_c",
    "rainfall_mm",
    "rh_mean_porc",
    "patm_mb",
    "sr_kj_m2",
    "wd_degrees",
    "ws_2_m_s",
    "utci_c",
    "wbgt_c",
    "hi_c",
    "pet_c",
    "diurnal_range_c",
    "vapor_pressure_kpa",
)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_validate": "Validando entradas...",
        "step_aggregate": "Agregando dados por data ({n} obs)...",
        "step_crossbasis": "Construindo crossbasis (lag 0-{lag_max})...",
        "step_fit": "Ajustando GLM {fam}...",
        "step_crosspred": "Calculando crosspred (grade de {n_grid} pontos)...",
        "step_diagnostics": "Executando diagnosticos do modelo...",
        "done": (
            "DLNM ajustado. RR cumulativo (p75 vs mediana): {rr} [{lo}, {hi}] "
            "-- lag pico: {lag_pk} dias"
        ),
        "err_outcome": "Coluna de desfecho '{outcome_col}' nao encontrada.",
        "err_no_lag_cols": (
            "Nenhuma coluna '{climate_col}_lagN' encontrada. Forneca um "
            "DataFrame com colunas de lag distribuido."
        ),
        "err_missing_lags": "Colunas de lag ausentes: {missing}. Verifique 'lag_max'.",
        "err_bad_cov": "Covariavel(is) nao encontrada(s): {bad_cov}.",
        "err_insufficient_obs": (
            "Observacoes insuficientes apos remover NA: {n_obs} "
            "(minimo necessario: {min_need})."
        ),
        "err_unsupported_fun": "Funcao de base '{fun}' nao suportada. Use uma de: {valid}.",
        "warn_negbin": (
            "Binomial negativa nao e suportada com crossbasis. Usando "
            "'quasipoisson' (alternativa robusta recomendada por Gasparrini, 2014)."
        ),
        "warn_autocorr": (
            "Autocorrelacao residual detectada (Ljung-Box p = {p_lb}). Aumente "
            "'ns_df' ou 'dof_per_year' para melhor controle temporal."
        ),
        "warn_overdispersion": (
            "Razao de dispersao = {phi}. Superdispersao detectada; "
            "'quasipoisson' e adequado."
        ),
        "warn_short_series": (
            "Serie curta ({n_days} dias / {n_yrs} ano(s)). Resultados podem ser "
            "pouco robustos com bases de alta complexidade."
        ),
        "warn_lang": "Idioma '{lang}' nao suportado. Usando 'pt'.",
        "info_var_detected": "Variavel climatica detectada: '{climate_col}'",
        "info_lagmax_detected": "lag_max detectado automaticamente: {lag_max}",
        "info_ns_df_auto": "ns_df calculado automaticamente: {ns_df}",
        "info_ref_value": "Valor de referencia (RR = 1): mediana = {ref_val}",
    },
    "en": {
        "step_validate": "Validating inputs...",
        "step_aggregate": "Aggregating data by date ({n} obs)...",
        "step_crossbasis": "Building crossbasis (lag 0-{lag_max})...",
        "step_fit": "Fitting {fam} GLM...",
        "step_crosspred": "Computing crosspred ({n_grid}-point grid)...",
        "step_diagnostics": "Running model diagnostics...",
        "done": (
            "DLNM fitted. Cumulative RR (p75 vs median): {rr} [{lo}, {hi}] "
            "-- peak lag: {lag_pk} days"
        ),
        "err_outcome": "Outcome column '{outcome_col}' not found.",
        "err_no_lag_cols": (
            "No '{climate_col}_lagN' column found. Supply a DataFrame with "
            "distributed-lag columns."
        ),
        "err_missing_lags": "Missing lag columns: {missing}. Check 'lag_max'.",
        "err_bad_cov": "Covariate(s) not found: {bad_cov}.",
        "err_insufficient_obs": (
            "Insufficient observations after dropping NA: {n_obs} "
            "(minimum needed: {min_need})."
        ),
        "err_unsupported_fun": "Unsupported basis function '{fun}'. Use one of: {valid}.",
        "warn_negbin": (
            "Negative binomial is not supported with crossbasis. Using "
            "'quasipoisson' (robust alternative recommended by Gasparrini, 2014)."
        ),
        "warn_autocorr": (
            "Residual autocorrelation detected (Ljung-Box p = {p_lb}). Increase "
            "'ns_df' or 'dof_per_year' for better temporal control."
        ),
        "warn_overdispersion": (
            "Dispersion ratio = {phi}. Overdispersion detected; "
            "'quasipoisson' is appropriate."
        ),
        "warn_short_series": (
            "Short series ({n_days} days / {n_yrs} year(s)). Results may not "
            "be robust with high-complexity bases."
        ),
        "warn_lang": "Unsupported language '{lang}'. Falling back to 'pt'.",
        "info_var_detected": "Climate variable detected: '{climate_col}'",
        "info_lagmax_detected": "lag_max auto-detected: {lag_max}",
        "info_ns_df_auto": "ns_df auto-computed: {ns_df}",
        "info_ref_value": "Reference value (RR = 1): median = {ref_val}",
    },
    "es": {
        "step_validate": "Validando entradas...",
        "step_aggregate": "Agregando datos por fecha ({n} obs)...",
        "step_crossbasis": "Construyendo crossbasis (lag 0-{lag_max})...",
        "step_fit": "Ajustando GLM {fam}...",
        "step_crosspred": "Calculando crosspred (grilla de {n_grid} puntos)...",
        "step_diagnostics": "Ejecutando diagnosticos del modelo...",
        "done": (
            "DLNM ajustado. RR acumulado (p75 vs mediana): {rr} [{lo}, {hi}] "
            "-- lag pico: {lag_pk} dias"
        ),
        "err_outcome": "Columna de resultado '{outcome_col}' no encontrada.",
        "err_no_lag_cols": (
            "No se encontro la columna '{climate_col}_lagN'. Proporcione un "
            "DataFrame con columnas de lag distribuido."
        ),
        "err_missing_lags": "Columnas de lag faltantes: {missing}. Verifique 'lag_max'.",
        "err_bad_cov": "Covariable(s) no encontrada(s): {bad_cov}.",
        "err_insufficient_obs": (
            "Observaciones insuficientes tras quitar NA: {n_obs} "
            "(minimo necesario: {min_need})."
        ),
        "err_unsupported_fun": "Funcion de base '{fun}' no soportada. Use una de: {valid}.",
        "warn_negbin": (
            "Binomial negativa no es soportada con crossbasis. Usando "
            "'quasipoisson' (alternativa robusta recomendada por Gasparrini, 2014)."
        ),
        "warn_autocorr": (
            "Autocorrelacion residual detectada (Ljung-Box p = {p_lb}). Aumente "
            "'ns_df' o 'dof_per_year' para mejor control temporal."
        ),
        "warn_overdispersion": (
            "Razon de dispersion = {phi}. Superdispersion detectada; "
            "'quasipoisson' es adecuado."
        ),
        "warn_short_series": (
            "Serie corta ({n_days} dias / {n_yrs} ano(s)). Los resultados "
            "pueden no ser robustos con bases de alta complejidad."
        ),
        "warn_lang": "Idioma '{lang}' no soportado. Usando 'pt'.",
        "info_var_detected": "Variable climatica detectada: '{climate_col}'",
        "info_lagmax_detected": "lag_max detectado automaticamente: {lag_max}",
        "info_ns_df_auto": "ns_df calculado automaticamente: {ns_df}",
        "info_ref_value": "Valor de referencia (RR = 1): mediana = {ref_val}",
    },
}

_VALID_FUNS: tuple[str, ...] = ("ns", "lin", "poly")


# ---------------------------------------------------------------------------
# Basis construction — matches splines::ns() / dlnm::onebasis() knot rules
# ---------------------------------------------------------------------------


def _ns_interior_knots(x: np.ndarray, df: int, intercept: bool) -> np.ndarray:
    """Interior knot quantiles, matching ``splines::ns(x, df, intercept)``."""
    n_iknots = df - 1 - int(intercept)
    if n_iknots <= 0:
        return np.array([])
    probs = np.linspace(0.0, 1.0, n_iknots + 2)[1:-1]
    return np.quantile(x, probs)


def _ns_basis(
    x: np.ndarray,
    knots: np.ndarray,
    boundary_knots: tuple[float, float],
    intercept: bool,
) -> np.ndarray:
    """Natural cubic spline basis (Hastie/Tibshirani ESL eq. 5.4-5.5).

    Spans the same natural-cubic-spline function space as R's
    ``splines::ns()`` for the same knots (interior + boundary), including
    linear extrapolation beyond the boundary knots. The column
    parametrisation differs from R's QR-based one, but since this basis
    always feeds a GLM as a block of predictor columns, fitted
    values/predictions are unaffected by the choice of parametrisation
    (any linear reparametrisation of the same space) — see module
    docstring.
    """
    all_knots = np.concatenate(([boundary_knots[0]], knots, [boundary_knots[1]]))
    k = len(all_knots)

    def d(idx: int, values: np.ndarray) -> np.ndarray:
        xk = all_knots[idx]
        x_last = all_knots[-1]
        term1 = np.clip(values - xk, 0.0, None) ** 3
        term2 = np.clip(values - x_last, 0.0, None) ** 3
        return (term1 - term2) / (x_last - xk)

    cols = []
    if intercept:
        cols.append(np.ones_like(x))
    cols.append(x)
    if k > 2:
        d_last = d(k - 2, x)
        for idx in range(k - 2):
            cols.append(d(idx, x) - d_last)
    return np.column_stack(cols)


def _onebasis(
    x: np.ndarray,
    fun: str,
    df: int | None,
    intercept: bool,
    lang: str,
    *,
    knots: np.ndarray | None = None,
    boundary_knots: tuple[float, float] | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Build a univariate basis, returning (matrix, meta-for-reuse).

    When ``knots``/``boundary_knots`` are given (prediction time), reuses
    them instead of recomputing from ``x`` — matching ``dlnm``'s behaviour
    of storing the training basis attributes on the crossbasis and reusing
    them unchanged at ``crosspred()`` time.
    """
    if fun not in _VALID_FUNS:
        raise ValueError(
            _MESSAGES[lang]["err_unsupported_fun"].format(fun=fun, valid=_VALID_FUNS)
        )
    if fun == "lin":
        basis = x.reshape(-1, 1)
        meta = {"fun": "lin", "df": 1, "intercept": False}
        return basis, meta
    if fun == "poly":
        degree = int(df or 1)
        basis = np.column_stack([x**p for p in range(1, degree + 1)])
        meta = {"fun": "poly", "df": degree, "intercept": False}
        return basis, meta

    # fun == "ns"
    df_eff = int(df or 4)
    if boundary_knots is None:
        boundary_knots = (float(np.min(x)), float(np.max(x)))
    if knots is None:
        knots = _ns_interior_knots(x, df_eff, intercept)
    basis = _ns_basis(x, knots, boundary_knots, intercept)
    meta = {
        "fun": "ns",
        "df": df_eff,
        "intercept": intercept,
        "knots": knots,
        "boundary_knots": boundary_knots,
    }
    return basis, meta


# ---------------------------------------------------------------------------
# Crossbasis — matches dlnm::crossbasis()
# ---------------------------------------------------------------------------


def _crossbasis(
    expo_matrix: np.ndarray,
    lag_max: int,
    argvar: dict[str, Any],
    arglag: dict[str, Any],
    lang: str,
) -> tuple[np.ndarray, dict[str, Any], dict[str, Any]]:
    """Bidimensional (exposure x lag) cross-basis, matching dlnm::crossbasis.

    ``expo_matrix`` has shape (n, lag_max + 1): column l is the exposure at
    lag l (0-indexed). Flattened column-major (Fortran order) to match R's
    ``as.numeric()`` on a matrix.
    """
    n = expo_matrix.shape[0]
    x_flat = expo_matrix.flatten(order="F")

    basisvar, var_meta = _onebasis(
        x_flat,
        fun=argvar.get("fun", "ns"),
        df=argvar.get("df"),
        intercept=False,
        lang=lang,
    )
    lag_seq = np.arange(0, lag_max + 1, dtype=float)
    basislag, lag_meta = _onebasis(
        lag_seq,
        fun=arglag.get("fun", "ns"),
        df=arglag.get("df"),
        intercept=True,
        lang=lang,
    )

    df_var = basisvar.shape[1]
    df_lag = basislag.shape[1]
    crossbasis = np.zeros((n, df_var * df_lag))
    for v in range(df_var):
        mat = basisvar[:, v].reshape(n, lag_max + 1, order="F")
        for lag_col in range(df_lag):
            crossbasis[:, df_lag * v + lag_col] = mat @ basislag[:, lag_col]

    col_names = [f"v{v + 1}.l{lag_col + 1}" for v in range(df_var) for lag_col in range(df_lag)]
    return crossbasis, var_meta, {**lag_meta, "col_names": col_names}


def _predict_basisvar(x: np.ndarray, var_meta: dict[str, Any], lang: str) -> np.ndarray:
    basis, _ = _onebasis(
        x,
        fun=var_meta["fun"],
        df=var_meta["df"],
        intercept=var_meta["intercept"],
        lang=lang,
        knots=var_meta.get("knots"),
        boundary_knots=var_meta.get("boundary_knots"),
    )
    return basis


def _predict_basislag(x: np.ndarray, lag_meta: dict[str, Any], lang: str) -> np.ndarray:
    basis, _ = _onebasis(
        x,
        fun=lag_meta["fun"],
        df=lag_meta["df"],
        intercept=lag_meta["intercept"],
        lang=lang,
        knots=lag_meta.get("knots"),
        boundary_knots=lag_meta.get("boundary_knots"),
    )
    return basis


# ---------------------------------------------------------------------------
# Crosspred — matches dlnm::crosspred() for a crossbasis input
# ---------------------------------------------------------------------------


def _xpred_accum(
    var_meta: dict[str, Any],
    lag_meta: dict[str, Any],
    at_grid: np.ndarray,
    lag_max: int,
    cen: float,
    lang: str,
) -> np.ndarray:
    """Cumulative-over-lag prediction design matrix at ``at_grid`` values.

    Matches the ``Xpredall`` accumulator inside ``dlnm::crosspred`` — shared
    by ``_crosspred`` (full grid + CI) and ``_allrr_fit`` (point estimate
    only, used inside a Monte Carlo loop where CI isn't needed per draw).
    """
    predlag = np.arange(0, lag_max + 1)
    basisvar_grid = _predict_basisvar(at_grid, var_meta, lang)
    basiscen = _predict_basisvar(np.array([cen]), var_meta, lang)
    basisvar_centered = basisvar_grid - basiscen
    basislag_grid = _predict_basislag(predlag.astype(float), lag_meta, lang)

    n_var = len(at_grid)
    df_var = basisvar_centered.shape[1]
    df_lag = basislag_grid.shape[1]
    xpred_accum = np.zeros((n_var, df_var * df_lag))
    for lag_idx in range(len(predlag)):
        for v in range(df_var):
            xpred_accum[:, df_lag * v : df_lag * (v + 1)] += (
                basisvar_centered[:, v : v + 1] * basislag_grid[lag_idx, :]
            )
    return xpred_accum


def _allrr_fit(
    var_meta: dict[str, Any],
    lag_meta: dict[str, Any],
    coef: np.ndarray,
    at_grid: np.ndarray,
    lag_max: int,
    cen: float,
    lang: str,
) -> np.ndarray:
    """Cumulative-over-lag RR point estimate only (no CI) at ``at_grid``.

    Matches ``dlnm::crosspred(cb, model, at=at_grid, cen=cen)$allRRfit``.
    """
    xpred_accum = _xpred_accum(var_meta, lag_meta, at_grid, lag_max, cen, lang)
    return np.exp(xpred_accum @ coef)


def _crosspred(
    var_meta: dict[str, Any],
    lag_meta: dict[str, Any],
    coef: np.ndarray,
    vcov: np.ndarray,
    at_grid: np.ndarray,
    lag_max: int,
    cen: float,
    alpha: float,
    lang: str,
) -> dict[str, Any]:
    """Centred prediction grid: per-lag and cumulative-over-lag RR + Wald CI."""
    sp_stats = _require_scipy_stats()
    predlag = np.arange(0, lag_max + 1)
    basisvar_grid = _predict_basisvar(at_grid, var_meta, lang)
    basiscen = _predict_basisvar(np.array([cen]), var_meta, lang)
    basisvar_centered = basisvar_grid - basiscen  # broadcast (1, df_var)
    basislag_grid = _predict_basislag(predlag.astype(float), lag_meta, lang)

    n_var = len(at_grid)
    n_lag = len(predlag)
    df_var = basisvar_centered.shape[1]
    df_lag = basislag_grid.shape[1]

    # Xpred[v, l, :] = kron(basisvar_centered[v], basislag_grid[l]) — matches
    # crossbasis column order "v{v}.l{l}" (v-major, l-minor).
    matfit = np.zeros((n_var, n_lag))
    matse = np.zeros((n_var, n_lag))
    cumfit = np.zeros((n_var, n_lag))
    cumse = np.zeros((n_var, n_lag))
    xpred_accum = np.zeros((n_var, df_var * df_lag))

    for lag_idx in range(n_lag):
        xpred_lag = np.zeros((n_var, df_var * df_lag))
        for v in range(df_var):
            xpred_lag[:, df_lag * v : df_lag * (v + 1)] = (
                basisvar_centered[:, v : v + 1] * basislag_grid[lag_idx, :]
            )
        matfit[:, lag_idx] = xpred_lag @ coef
        matse[:, lag_idx] = np.sqrt(np.maximum(0.0, np.sum((xpred_lag @ vcov) * xpred_lag, axis=1)))
        xpred_accum += xpred_lag
        cumfit[:, lag_idx] = xpred_accum @ coef
        cumse[:, lag_idx] = np.sqrt(
            np.maximum(0.0, np.sum((xpred_accum @ vcov) * xpred_accum, axis=1))
        )

    allfit = xpred_accum @ coef
    allse = np.sqrt(np.maximum(0.0, np.sum((xpred_accum @ vcov) * xpred_accum, axis=1)))

    z = float(sp_stats.norm.ppf(1 - alpha / 2))

    return {
        "predvar": at_grid,
        "lag": np.array([0, lag_max]),
        "cen": cen,
        "matRRfit": np.exp(matfit),
        "matRRlow": np.exp(matfit - z * matse),
        "matRRhigh": np.exp(matfit + z * matse),
        "allRRfit": np.exp(allfit),
        "allRRlow": np.exp(allfit - z * allse),
        "allRRhigh": np.exp(allfit + z * allse),
        "cumRRfit": np.exp(cumfit),
        "cumRRlow": np.exp(cumfit - z * cumse),
        "cumRRhigh": np.exp(cumfit + z * cumse),
    }


# ---------------------------------------------------------------------------
# Other helpers
# ---------------------------------------------------------------------------


def _detect_base_var(columns: list[str], lang: str) -> str:
    lag_cols = [c for c in columns if pd.Series([c]).str.contains(r"_lag\d+$", regex=True).iloc[0]]
    if not lag_cols:
        raise ValueError(
            _MESSAGES[lang]["err_no_lag_cols"].format(climate_col="<auto>")
        )
    base_vars = sorted({c.rsplit("_lag", 1)[0] for c in lag_cols})
    for known in _KNOWN_CLIMATE_VARS:
        if known in base_vars:
            return known
    return base_vars[0]


def _compute_ns_df(dates: pd.Series, dof_per_year: int) -> int:
    n_years = (dates.max() - dates.min()).days / 365.25
    return max(dof_per_year, round(dof_per_year * n_years))


def _diagnostics(
    result: Any,
    resid_deviance: np.ndarray,
    aic_poisson: float,
    alpha: float,
) -> dict[str, Any]:
    from statsmodels.stats.diagnostic import acorr_ljungbox

    disp_ratio = result.deviance / result.df_resid
    if disp_ratio < 1.5:
        disp_category = "adequate"
    elif disp_ratio < 3.0:
        disp_category = "moderate"
    else:
        disp_category = "high"

    try:
        lb = acorr_ljungbox(resid_deviance, lags=[10], return_df=True)
        autocorr_pval = float(lb["lb_pvalue"].iloc[0])
    except Exception:
        autocorr_pval = float("nan")

    return {
        "disp_ratio": disp_ratio,
        "disp_category": disp_category,
        "autocorr_pval": autocorr_pval,
        "has_autocorr": bool(autocorr_pval < alpha) if not np.isnan(autocorr_pval) else None,
        "aic_poisson": aic_poisson,
        "deviance": result.deviance,
        "null_deviance": result.null_deviance,
        "df_residual": result.df_resid,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def sus_mod_dlnm(
    df: pd.DataFrame,
    outcome_col: str = "n_obitos",
    climate_col: str | None = None,
    lag_max: int | None = None,
    covariates: list[str] | None = None,
    argvar: dict[str, Any] | None = None,
    arglag: dict[str, Any] | None = None,
    family: Literal["quasipoisson", "poisson", "negbin"] = "quasipoisson",
    ns_df: int | None = None,
    dof_per_year: int = 4,
    ref_value: float | None = None,
    pred_at: tuple[float, ...] = (0.25, 0.50, 0.75, 0.90, 0.95, 0.99),
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Fit a Distributed Lag Non-linear Model (DLNM) to a climate-health series.

    Quantifies the association between a climate exposure history and a
    daily health-outcome count using a bidimensional cross-basis (exposure
    x lag) and a quasi-Poisson GLM, following Gasparrini et al. (2010,
    2011, 2014). See the module docstring for how the underlying
    ``dlnm::crossbasis``/``dlnm::crosspred`` machinery was re-implemented
    and numerically validated against the real R package.

    Args:
        df: DataFrame with a ``date`` column, the outcome column, and lag
            columns ``{climate_col}_lag0`` .. ``{climate_col}_lag{L}``
            (as produced by R's
            ``sus_climate_aggregate(temporal_strategy="distributed_lag")``
            — see IDEIAS.md for the current Python-side contract gap).
        outcome_col: Name of the daily health count column.
        climate_col: Base name of the climate exposure (without the
            ``_lagN`` suffix). ``None`` auto-detects.
        lag_max: Maximum lag to include. ``None`` auto-detects from the
            highest ``N`` present in ``{climate_col}_lagN`` columns.
        covariates: Additional column names to include as linear
            confounders (averaged by date before fitting).
        argvar: Basis spec for the exposure dimension, e.g.
            ``{"fun": "ns", "df": 4}`` (default). ``fun`` in
            ``{"ns", "lin", "poly"}``.
        arglag: Basis spec for the lag dimension, e.g.
            ``{"fun": "ns", "df": 3}`` (default).
        family: ``"quasipoisson"`` (default), ``"poisson"``, or
            ``"negbin"`` (redirected to ``"quasipoisson"`` with a warning
            — negative binomial is incompatible with the cross-basis GLM).
        ns_df: Degrees of freedom for the ``date`` natural-spline time
            trend. ``None`` auto-computes from ``dof_per_year``. ``0``
            suppresses time control.
        dof_per_year: DF per year of data for automatic ``ns_df``.
        ref_value: Exposure value where RR = 1. ``None`` uses the sample
            median of lag-0 exposure.
        pred_at: Quantile probabilities for ``exposure_response``.
        alpha: Significance level for confidence intervals.
        lang: Message language.
        verbose: Print progress steps and warnings.

    Returns:
        A dict (Python analog of R's ``climasus_dlnm`` S3 object):
        ``model`` (the fitted ``statsmodels`` GLM results object),
        ``crossbasis`` (the raw cross-basis ``numpy.ndarray``),
        ``pred`` (dict with ``predvar``/``matRRfit``/``allRRfit``/etc.,
        analog of ``dlnm::crosspred``), ``exposure_response`` and
        ``lag_response`` (``pandas.DataFrame``), ``models`` (one-row
        summary DataFrame), ``data_daily``, ``diagnostics``, ``meta``.

    Raises:
        ValueError: On missing columns, an unsupported basis ``fun``, or
            insufficient observations after dropping NA rows.

    Examples::

        import climasus4py as cs

        fit = cs.sus_mod_dlnm(
            df, outcome_col="n_obitos", lag_max=14,
            argvar={"fun": "ns", "df": 4}, arglag={"fun": "ns", "df": 3},
        )
        fit["exposure_response"]
        fit["lag_response"]
        fit["diagnostics"]["disp_ratio"]
    """
    sm = _require_statsmodels()

    if lang not in ("pt", "en", "es"):
        console.print(f"[yellow]{_MESSAGES['pt']['warn_lang'].format(lang=lang)}[/yellow]")
        lang = "pt"
    msgs = _MESSAGES[lang]

    if family == "negbin":
        if verbose:
            console.print(f"[yellow]{msgs['warn_negbin']}[/yellow]")
        family = "quasipoisson"
    argvar = argvar or {"fun": "ns", "df": 4}
    arglag = arglag or {"fun": "ns", "df": 3}

    if verbose:
        console.print(msgs["step_validate"])

    if outcome_col not in df.columns:
        raise ValueError(msgs["err_outcome"].format(outcome_col=outcome_col))

    if climate_col is None:
        climate_col = _detect_base_var(list(df.columns), lang)
        if verbose:
            console.print(msgs["info_var_detected"].format(climate_col=climate_col))

    lag_cols = [c for c in df.columns if c.startswith(f"{climate_col}_lag")]
    if not lag_cols:
        raise ValueError(msgs["err_no_lag_cols"].format(climate_col=climate_col))

    if lag_max is None:
        lag_nums = [int(c.replace(f"{climate_col}_lag", "")) for c in lag_cols]
        lag_max = max(lag_nums)
        if verbose:
            console.print(msgs["info_lagmax_detected"].format(lag_max=lag_max))

    lag_col_names = [f"{climate_col}_lag{i}" for i in range(lag_max + 1)]
    missing_lags = [c for c in lag_col_names if c not in df.columns]
    if missing_lags:
        raise ValueError(msgs["err_missing_lags"].format(missing=", ".join(missing_lags)))

    if covariates:
        bad_cov = [c for c in covariates if c not in df.columns]
        if bad_cov:
            raise ValueError(msgs["err_bad_cov"].format(bad_cov=", ".join(bad_cov)))

    if verbose:
        console.print(msgs["step_aggregate"].format(n=len(df)))

    agg_cols = ["date", outcome_col, *lag_col_names, *(covariates or [])]
    df_agg = (
        df[agg_cols]
        .rename(columns={outcome_col: "y"})
        .groupby("date", as_index=False)
        .agg({"y": "sum", **{c: "mean" for c in [*lag_col_names, *(covariates or [])]}})
        .sort_values("date")
        .dropna(subset=["y", *lag_col_names])
        .reset_index(drop=True)
    )

    n_obs = len(df_agg)
    min_need = lag_max + 10
    if n_obs < min_need:
        raise ValueError(msgs["err_insufficient_obs"].format(n_obs=n_obs, min_need=min_need))

    n_days = (df_agg["date"].max() - df_agg["date"].min()).days
    n_yrs = round(n_days / 365.25, 1)
    if n_days < 365 and verbose:
        warn_short = msgs["warn_short_series"].format(n_days=n_days, n_yrs=n_yrs)
        console.print(f"[yellow]{warn_short}[/yellow]")

    expo_matrix = df_agg[lag_col_names].to_numpy(dtype=float)

    if verbose:
        console.print(msgs["step_crossbasis"].format(lag_max=lag_max))
    crossbasis, var_meta, lag_meta = _crossbasis(expo_matrix, lag_max, argvar, arglag, lang)

    if ns_df is None and dof_per_year and dof_per_year > 0:
        ns_df = _compute_ns_df(df_agg["date"], dof_per_year)
        if verbose:
            console.print(msgs["info_ns_df_auto"].format(ns_df=ns_df))

    if ref_value is None:
        ref_value = float(np.median(expo_matrix[:, 0]))
        if verbose:
            console.print(msgs["info_ref_value"].format(ref_val=round(ref_value, 2)))

    date_int = (df_agg["date"] - pd.Timestamp("1970-01-01")).dt.days.to_numpy(dtype=float)
    design_blocks = [np.ones((n_obs, 1)), crossbasis]
    n_cb_cols = crossbasis.shape[1]
    if ns_df and ns_df > 0:
        date_basis, _ = _onebasis(date_int, fun="ns", df=ns_df, intercept=False, lang=lang)
        design_blocks.append(date_basis)
    if covariates:
        design_blocks.append(df_agg[covariates].to_numpy(dtype=float))
    x_design = np.column_stack(design_blocks)
    y = df_agg["y"].to_numpy(dtype=float)

    if verbose:
        console.print(msgs["step_fit"].format(fam=family))
    glm_model = sm.GLM(y, x_design, family=sm.families.Poisson())
    result = glm_model.fit(scale="X2" if family == "quasipoisson" else 1.0)

    pois_result = sm.GLM(y, x_design, family=sm.families.Poisson()).fit(scale=1.0)
    aic_poisson = float(pois_result.aic)

    expo_range = np.quantile(expo_matrix[:, 0], [0.01, 0.99])
    expo_grid = np.linspace(expo_range[0], expo_range[1], 100)

    if verbose:
        console.print(msgs["step_crosspred"].format(n_grid=len(expo_grid)))

    coef = result.params[1 : 1 + n_cb_cols]
    vcov = np.asarray(result.cov_params())[1 : 1 + n_cb_cols, 1 : 1 + n_cb_cols]
    pred = _crosspred(var_meta, lag_meta, coef, vcov, expo_grid, lag_max, ref_value, alpha, lang)

    if verbose:
        console.print(msgs["step_diagnostics"])
    resid_deviance = np.asarray(result.resid_deviance)
    diagnostics = _diagnostics(result, resid_deviance, aic_poisson, alpha)

    if verbose:
        if diagnostics["has_autocorr"]:
            p_lb = round(diagnostics["autocorr_pval"], 4)
            console.print(f"[yellow]{msgs['warn_autocorr'].format(p_lb=p_lb)}[/yellow]")
        if diagnostics["disp_ratio"] > 1.5:
            phi = round(diagnostics["disp_ratio"], 2)
            console.print(f"[cyan]{msgs['warn_overdispersion'].format(phi=phi)}[/cyan]")

    expo_pcts = np.quantile(expo_matrix[:, 0], pred_at)
    expo_resp_rows = []
    for pct, val in zip(pred_at, expo_pcts, strict=True):
        idx = int(np.argmin(np.abs(pred["predvar"] - val)))
        expo_resp_rows.append(
            {
                "pct": pct,
                "exposure": round(float(val), 3),
                "rr": float(pred["allRRfit"][idx]),
                "lo": float(pred["allRRlow"][idx]),
                "hi": float(pred["allRRhigh"][idx]),
            }
        )
    exposure_response = pd.DataFrame(expo_resp_rows)

    p75_val = float(np.quantile(expo_matrix[:, 0], 0.75))
    p75_idx = int(np.argmin(np.abs(pred["predvar"] - p75_val)))
    lag_seq = np.arange(0, lag_max + 1)
    rr_lag = pred["matRRfit"][p75_idx, :]
    lag_response = pd.DataFrame(
        {
            "lag": lag_seq,
            "rr": rr_lag,
            "lo": pred["matRRlow"][p75_idx, :],
            "hi": pred["matRRhigh"][p75_idx, :],
            "rr_cum": np.cumprod(rr_lag),
        }
    )

    lag_abs_effect = np.abs(np.log(rr_lag))
    lag_peak = int(lag_seq[int(np.argmax(lag_abs_effect))])
    rr_p75 = float(pred["allRRfit"][p75_idx])
    lo_p75 = float(pred["allRRlow"][p75_idx])
    hi_p75 = float(pred["allRRhigh"][p75_idx])

    models_tbl = pd.DataFrame(
        [
            {
                "variable": climate_col,
                "n": n_obs,
                "family": family,
                "lag_max": lag_max,
                "ref_value": round(ref_value, 3),
                "exposure_p75": round(p75_val, 3),
                "rr": rr_p75,
                "lo": lo_p75,
                "hi": hi_p75,
                "lag_peak": lag_peak,
                "disp_ratio": round(diagnostics["disp_ratio"], 3),
                "aic_poisson": round(aic_poisson, 1),
            }
        ]
    )

    if verbose:
        console.print(
            msgs["done"].format(
                rr=round(rr_p75, 4), lo=round(lo_p75, 4), hi=round(hi_p75, 4), lag_pk=lag_peak
            )
        )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    input_meta = dict(df.attrs.get("sus_meta", {}))
    input_meta["history"] = [
        *input_meta.get("history", []),
        (
            f"[{ts}] sus_mod_dlnm(): var={climate_col}; lag_max={lag_max}; "
            f"family={family}; ns_df={ns_df}; n={n_obs}; "
            f"RR_p75={rr_p75:.4f} [{lo_p75:.4f},{hi_p75:.4f}]; lag_peak={lag_peak}"
        ),
    ]

    return {
        "model": result,
        "crossbasis": crossbasis,
        "pred": pred,
        "exposure_response": exposure_response,
        "lag_response": lag_response,
        "models": models_tbl,
        "data_daily": df_agg,
        "diagnostics": diagnostics,
        "meta": {
            "climate_col": climate_col,
            "outcome_col": outcome_col,
            "lag_max": lag_max,
            "ref_value": ref_value,
            "argvar": argvar,
            "arglag": arglag,
            "family": family,
            "ns_df": ns_df,
            "dof_per_year": dof_per_year,
            "n": n_obs,
            "n_days": n_days,
            "alpha": alpha,
            "pred_at": pred_at,
            "var_meta": var_meta,
            "lag_meta": lag_meta,
            "input_meta": input_meta,
        },
    }
