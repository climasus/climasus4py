"""Spatial regression models (SAR / SEM / SDM) for climate-health associations.

Mirrors R: sus_mod_spatial_reg.R

Not lazy — this operates on an in-memory ``pandas.DataFrame`` (or
``geopandas.GeoDataFrame``, the Python analog of R's ``sf``/``data.frame``)
plus the ``dict`` returned by ``sus_mod_spatial_weights``, not a
``DuckDBPyRelation``. Like R's function, it sits outside the
``import -> clean -> standardize -> filter -> variables -> aggregate``
pipeline as a standalone spatial-modelling helper.

Theory:
  Anselin (1988) - Spatial Econometrics: Methods and Models
  LeSage & Pace (2009) - Introduction to Spatial Econometrics
  Bivand, Pebesma & Gomez-Rubio (2013) - Applied Spatial Data Analysis with R
  Elhorst (2014) - Spatial Econometrics: From Cross-Sectional Data to Panels
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

VALID_MODELS: tuple[str, ...] = ("lag", "error", "durbin", "sac")
VALID_METHODS: tuple[str, ...] = ("eigen", "LU", "Chebyshev", "MC")

_MODEL_LABELS: dict[str, str] = {
    "lag": "SAR (Spatial Lag)",
    "error": "SEM (Spatial Error)",
    "durbin": "SDM (Spatial Durbin)",
    "sac": "SAC (Spatial Autocorrelation)",
}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_check": "Verificando entradas...",
        "step_ols": "Ajustando baseline OLS para comparacao de AIC...",
        "step_fit": "Ajustando modelo {model_label} (metodo = {method})...",
        "step_impacts": (
            "Calculando impactos diretos/indiretos/totais "
            "(decomposicao de LeSage & Pace, estimativas pontuais)..."
        ),
        "step_moran": "Testando autocorrelacao espacial nos residuos (Moran)...",
        "warn_impacts": (
            "Nao foi possivel calcular impactos: {msg}. "
            "O elemento 'impacts' sera None."
        ),
        "warn_sf_drop": (
            "'df' e um geopandas.GeoDataFrame. Geometria descartada."
        ),
        "done": (
            "Concluido. AIC modelo = {aic_m} | AIC OLS = {aic_ols} | "
            "Moran (residuos) p = {p_moran}"
        ),
        "err_not_weights": (
            "'W' deve ser um dict produzido por sus_mod_spatial_weights()."
        ),
        "err_no_listw": (
            "'W' nao contem a chave 'listw'. Reconstrua com "
            "sus_mod_spatial_weights()."
        ),
        "err_not_formula": (
            "'formula' deve ser uma string no formato 'y ~ x1 + x2' "
            "(ex.: 'deaths ~ mean_temp + precip')."
        ),
        "err_not_df": "'df' deve ser um pandas.DataFrame.",
        "err_model": "'model' invalido: '{model}'. Use um de: {valid}.",
        "err_method": "'method' invalido: '{method}'. Use um de: {valid}.",
        "err_missing_cols": "Coluna(s) nao encontrada(s) em 'df': {cols}.",
        "err_non_numeric": "Coluna '{col}' nao pode ser convertida para numerico: {err}.",
        "err_row_mismatch": (
            "'df' tem {n_df} linha(s), mas W['listw'] espera {n_w} "
            "regiao(oes). As dimensoes devem coincidir."
        ),
        "err_id_mismatch": (
            "Os valores de 'code_muni' em 'df' nao coincidem com os ids de "
            "W['listw'].id_order. Verifique se 'df' e a malha usada em "
            "sus_mod_spatial_weights() correspondem."
        ),
        "err_islands_strict": (
            "{n_islands} ilha(s) espacial(is) em W e zero_policy=False. "
            "Defina zero_policy=True ou remova as ilhas ao construir W."
        ),
        "err_sac_not_implemented": (
            "model='sac' (SAC, lag + error combinados via maxima "
            "verossimilhanca, spatialreg::sacsarlm no R) nao tem equivalente "
            "fiel em 'spreg': o pacote so oferece uma versao GMM "
            "(spreg.GM_Combo), um estimador diferente do ML usado pelo R. "
            "Nao aproximamos silenciosamente com GMM: use model='lag', "
            "'error' ou 'durbin', ou ajuste GM_Combo diretamente."
        ),
        "err_method_not_implemented": (
            "method='{method}' nao tem equivalente em 'spreg' para "
            "model='{model}'. spreg.ML_Lag aceita apenas 'full'/'ord'; "
            "spreg.ML_Error aceita 'full'/'ord'/'LU'. Nao ha aproximacao "
            "por Chebyshev ou Monte Carlo (metodos 'Chebyshev'/'MC' do R) "
            "em spreg. Use method='eigen' (mapeado para 'ord'), ou "
            "method='LU' apenas com model='error'."
        ),
    },
    "en": {
        "step_check": "Checking inputs...",
        "step_ols": "Fitting OLS baseline for AIC comparison...",
        "step_fit": "Fitting {model_label} model (method = {method})...",
        "step_impacts": (
            "Computing direct/indirect/total impacts "
            "(LeSage & Pace decomposition, point estimates)..."
        ),
        "step_moran": "Testing spatial autocorrelation in residuals (Moran)...",
        "warn_impacts": (
            "Could not compute impacts: {msg}. The 'impacts' element will "
            "be None."
        ),
        "warn_sf_drop": "'df' is a geopandas.GeoDataFrame. Geometry dropped.",
        "done": (
            "Done. Model AIC = {aic_m} | OLS AIC = {aic_ols} | "
            "Moran (residuals) p = {p_moran}"
        ),
        "err_not_weights": "'W' must be a dict produced by sus_mod_spatial_weights().",
        "err_no_listw": (
            "'W' does not contain key 'listw'. Rebuild with "
            "sus_mod_spatial_weights()."
        ),
        "err_not_formula": (
            "'formula' must be a string of the form 'y ~ x1 + x2' "
            "(e.g. 'deaths ~ mean_temp + precip')."
        ),
        "err_not_df": "'df' must be a pandas.DataFrame.",
        "err_model": "Invalid 'model': '{model}'. Use one of: {valid}.",
        "err_method": "Invalid 'method': '{method}'. Use one of: {valid}.",
        "err_missing_cols": "Column(s) not found in 'df': {cols}.",
        "err_non_numeric": "Column '{col}' could not be converted to numeric: {err}.",
        "err_row_mismatch": (
            "'df' has {n_df} row(s) but W['listw'] expects {n_w} region(s). "
            "Dimensions must match."
        ),
        "err_id_mismatch": (
            "'code_muni' values in 'df' do not match the ids in "
            "W['listw'].id_order. Check that 'df' and the mesh used in "
            "sus_mod_spatial_weights() correspond."
        ),
        "err_islands_strict": (
            "{n_islands} spatial island(s) in W and zero_policy=False. "
            "Set zero_policy=True or remove islands when building W."
        ),
        "err_sac_not_implemented": (
            "model='sac' (SAC, combined lag + error via maximum likelihood, "
            "spatialreg::sacsarlm in R) has no faithful equivalent in "
            "'spreg': the package only offers a GMM version "
            "(spreg.GM_Combo), a different estimator than R's ML. We do "
            "not silently approximate with GMM: use model='lag', 'error' "
            "or 'durbin', or fit GM_Combo directly."
        ),
        "err_method_not_implemented": (
            "method='{method}' has no equivalent in 'spreg' for "
            "model='{model}'. spreg.ML_Lag only accepts 'full'/'ord'; "
            "spreg.ML_Error accepts 'full'/'ord'/'LU'. There is no "
            "Chebyshev or Monte Carlo approximation (R's "
            "'Chebyshev'/'MC' methods) in spreg. Use method='eigen' "
            "(mapped to 'ord'), or method='LU' only with model='error'."
        ),
    },
    "es": {
        "step_check": "Verificando entradas...",
        "step_ols": "Ajustando OLS de referencia para comparacion de AIC...",
        "step_fit": "Ajustando modelo {model_label} (metodo = {method})...",
        "step_impacts": (
            "Calculando impactos directos/indirectos/totales "
            "(descomposicion de LeSage & Pace, estimaciones puntuales)..."
        ),
        "step_moran": "Probando autocorrelacion espacial en residuos (Moran)...",
        "warn_impacts": (
            "No se pudieron calcular los impactos: {msg}. El elemento "
            "'impacts' sera None."
        ),
        "warn_sf_drop": "'df' es un geopandas.GeoDataFrame. Geometria descartada.",
        "done": (
            "Listo. AIC modelo = {aic_m} | AIC OLS = {aic_ols} | "
            "Moran (residuos) p = {p_moran}"
        ),
        "err_not_weights": "'W' debe ser un dict producido por sus_mod_spatial_weights().",
        "err_no_listw": (
            "'W' no contiene la clave 'listw'. Reconstruya con "
            "sus_mod_spatial_weights()."
        ),
        "err_not_formula": (
            "'formula' debe ser una cadena con el formato 'y ~ x1 + x2' "
            "(p.ej.: 'deaths ~ mean_temp + precip')."
        ),
        "err_not_df": "'df' debe ser un pandas.DataFrame.",
        "err_model": "'model' invalido: '{model}'. Use uno de: {valid}.",
        "err_method": "'method' invalido: '{method}'. Use uno de: {valid}.",
        "err_missing_cols": "Columna(s) no encontrada(s) en 'df': {cols}.",
        "err_non_numeric": "La columna '{col}' no pudo convertirse a numerico: {err}.",
        "err_row_mismatch": (
            "'df' tiene {n_df} fila(s) pero W['listw'] espera {n_w} "
            "region(es). Las dimensiones deben coincidir."
        ),
        "err_id_mismatch": (
            "Los valores de 'code_muni' en 'df' no coinciden con los ids de "
            "W['listw'].id_order. Verifique que 'df' y la malla usada en "
            "sus_mod_spatial_weights() correspondan."
        ),
        "err_islands_strict": (
            "{n_islands} isla(s) espacial(es) en W y zero_policy=False. "
            "Defina zero_policy=True o elimine las islas al construir W."
        ),
        "err_sac_not_implemented": (
            "model='sac' (SAC, lag + error combinados por maxima "
            "verosimilitud, spatialreg::sacsarlm en R) no tiene "
            "equivalente fiel en 'spreg': el paquete solo ofrece una "
            "version GMM (spreg.GM_Combo), un estimador distinto al ML "
            "de R. No aproximamos en silencio con GMM: use model='lag', "
            "'error' o 'durbin', o ajuste GM_Combo directamente."
        ),
        "err_method_not_implemented": (
            "method='{method}' no tiene equivalente en 'spreg' para "
            "model='{model}'. spreg.ML_Lag solo acepta 'full'/'ord'; "
            "spreg.ML_Error acepta 'full'/'ord'/'LU'. No hay aproximacion "
            "por Chebyshev o Monte Carlo (metodos 'Chebyshev'/'MC' de R) "
            "en spreg. Use method='eigen' (mapeado a 'ord'), o "
            "method='LU' solo con model='error'."
        ),
    },
}


def _parse_formula(formula: str, msg: dict[str, str]) -> tuple[str, list[str]]:
    """Parse a minimal R-style additive formula string ``"y ~ x1 + x2"``.

    Only additive main-effect terms are supported (no interactions,
    transformations, or intercept removal) — this covers every example
    documented for ``sus_mod_spatial_reg()`` in R. There is no ``patsy``/
    ``formulaic`` dependency in climasus4py, so this is a small stdlib
    parser rather than a new hard dependency; see IDEIAS.md.
    """
    if not isinstance(formula, str) or "~" not in formula:
        raise ValueError(msg["err_not_formula"])
    lhs, _, rhs = formula.partition("~")
    lhs = lhs.strip()
    rhs_terms = [t.strip() for t in rhs.split("+")]
    rhs_terms = [t for t in rhs_terms if t and t != "1"]
    if not lhs or not rhs_terms:
        raise ValueError(msg["err_not_formula"])
    return lhs, rhs_terms


def _resolve_spreg_method(model: str, method: str, msg: dict[str, str]) -> str:
    """Map R/spdep Jacobian-computation method names onto spreg's.

    spdep/spatialreg offer four methods for the log-Jacobian term of the
    spatial ML likelihood: ``"eigen"`` (exact, via eigenvalues of W),
    ``"LU"`` (sparse LU decomposition), ``"Chebyshev"`` (polynomial
    approximation), and ``"MC"`` (Monte Carlo trace estimation). spreg's
    ``ML_Lag``/``ML_Error`` only implement two of these concepts:
    ``"full"`` (brute-force full-matrix determinant) and ``"ord"``
    (Ord's (1975) eigenvalue method) — plus ``"LU"`` for ``ML_Error``
    only. ``"eigen"`` maps onto spreg's ``"ord"`` (both are exact,
    eigenvalue-based). ``"LU"`` maps onto spreg's ``"LU"`` for the
    ``error`` model only (spreg.ML_Lag has no LU option).
    ``"Chebyshev"`` and ``"MC"`` have no spreg equivalent at all —
    raises ``NotImplementedError`` rather than silently substituting a
    different (and numerically different) method.
    """
    if method == "eigen":
        return "ord"
    if method == "LU" and model == "error":
        return "LU"
    raise NotImplementedError(
        msg["err_method_not_implemented"].format(method=method, model=model)
    )


def sus_mod_spatial_reg(
    df: Any,
    formula: str,
    W: dict[str, Any],
    model: Literal["lag", "error", "durbin", "sac"] = "lag",
    method: Literal["eigen", "LU", "Chebyshev", "MC"] = "eigen",
    zero_policy: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Fit a maximum-likelihood spatial regression model.

    Fits spatial regression models that account for spatial spillovers
    between geographic units (e.g. municipalities) in climate-health
    studies: the Spatial Autoregressive Model (SAR / spatial lag), the
    Spatial Error Model (SEM), and the Spatial Durbin Model (SDM). This
    is the Python analog of R's ``sus_mod_spatial_reg()``, which also
    supports a fourth family (SAC, combined lag + error); see **Model
    families** below for why ``model="sac"`` raises here.

    Model families:
        * ``"lag"`` (SAR, default): the outcome in unit *i* depends on a
          weighted average of outcomes in neighbouring units. Estimated
          via ``spreg.ML_Lag``. Contains a spatial lag parameter ``rho``.
        * ``"error"`` (SEM): spatial dependence enters through a
          spatially autocorrelated error term. Estimated via
          ``spreg.ML_Error``. Contains a spatial error parameter
          ``lambda``.
        * ``"durbin"`` (SDM): extends the SAR model with spatially
          lagged covariates. Estimated via ``spreg.ML_Lag(slx_lags=1)``
          — the same call as R's ``spatialreg::lagsarlm(Durbin = TRUE)``,
          which is likewise a spatial-lag fit with lagged-X terms added.
          Contains both ``rho`` and lagged-covariate coefficients
          (named ``W_<covariate>`` in ``$coefficients``).
        * ``"sac"`` (SAC): **not implemented**. R fits this via
          ``spatialreg::sacsarlm()``, a maximum-likelihood estimator for
          the combined lag + error model. ``spreg`` has no ML estimator
          for this specification — only a GMM one (``spreg.GM_Combo``),
          which is numerically a different estimator, not a drop-in
          match for R's ML fit. Rather than silently approximating with
          GMM, this raises ``NotImplementedError``.

    Impacts:
        For models with a spatial lag (``"lag"``, ``"durbin"``), the
        total effect of a covariate is decomposed into *direct*
        (own-unit) and *indirect* (spillover) effects using the LeSage
        & Pace (2009) "full" multiplier — ``ati = 1/(1-rho)`` and
        ``adi = trace((I - rho*W)^-1) / n`` (a dense trace over *n*
        regions, computed via ``spreg``'s own ``spat_impacts="full"``),
        the same trace-based decomposition R's
        ``spatialreg::impacts()`` uses for its point estimates. Unlike
        R's ``spatialreg::impacts(fit, R = 200)``, no simulation-based
        standard errors are computed here — only point estimates. For
        the ``"error"`` model, impacts equal OLS slopes and no
        decomposition is meaningful; ``$impacts`` is ``None``.

    Residual diagnostics:
        After fitting, ``esda.Moran`` is applied to the model residuals
        (using randomisation-based inference, matching R's
        ``spdep::moran.test(randomisation = TRUE)`` default) to check
        whether spatial autocorrelation has been adequately absorbed.

    Row alignment:
        *df* is realigned to ``W["listw"].id_order`` before fitting: if
        *df* has a ``code_muni`` column, rows are reordered to match
        ``id_order`` by that key (raising if the id sets don't match).
        Otherwise *df* is assumed to already be in ``id_order`` — the
        same assumption R's ``sus_mod_spatial_reg()`` makes (it never
        checks alignment either), but with an explicit row-count guard.

    Args:
        df: A ``pandas.DataFrame`` (or ``geopandas.GeoDataFrame``, whose
            geometry is dropped with a warning) containing all variables
            referenced in *formula*.
        formula: A string of the form ``"y ~ x1 + x2 + x3"`` (e.g.
            ``"deaths ~ mean_temp + precip + pib_pc"``). Only additive
            main-effect terms are supported — see ``_parse_formula``.
        W: A ``dict`` produced by ``sus_mod_spatial_weights()``. Must
            contain key ``"listw"`` (a ``libpysal.weights.W`` object)
            whose ``id_order`` length matches ``len(df)``.
        model: Spatial model family. One of ``"lag"`` (SAR, default),
            ``"error"`` (SEM), ``"durbin"`` (SDM), or ``"sac"`` (SAC —
            raises ``NotImplementedError``, see **Model families**).
        method: Jacobian-computation method. One of ``"eigen"``
            (default, mapped to spreg's ``"ord"``), ``"LU"`` (only
            valid with ``model="error"``), ``"Chebyshev"``, or ``"MC"``
            (the latter two raise ``NotImplementedError`` — no spreg
            equivalent). See ``_resolve_spreg_method``.
        zero_policy: If ``True`` (default), spatial islands in *W* are
            allowed. If ``False`` and *W* contains islands
            (``W["n_islands"] > 0``), raises ``ValueError``.
        lang: Output language for messages: ``"pt"`` (default), ``"en"``,
            or ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A ``dict`` (the Python analog of R's ``climasus_spatial_reg``
        S3 list — see ``sus_mod_spatial_weights`` for why this package
        uses plain dicts rather than a printer class) with keys:

        - ``"model"``: the *model* argument value.
        - ``"model_label"``: human-readable model name.
        - ``"coefficients"``: ``pandas.DataFrame`` with columns
          ``term``, ``estimate``, ``std_error``, ``z_value``,
          ``p_value`` for all covariates (excluding ``rho``/``lambda``).
        - ``"rho"``: spatial lag parameter, or ``None`` for the
          ``error`` model.
        - ``"lambda"``: spatial error parameter, or ``None`` for
          ``lag``/``durbin``.
        - ``"impacts"``: ``pandas.DataFrame`` with columns ``term``,
          ``direct``, ``indirect``, ``total``, or ``None`` for the
          ``error`` model or if impact computation fails.
        - ``"aic"``: AIC of the spatial model.
        - ``"lm_aic"``: AIC of the OLS baseline (same formula, same
          data).
        - ``"moran_residuals"``: ``dict`` with keys ``"I"``,
          ``"p_value"``, ``"z"``.
        - ``"fitted"``: ``numpy.ndarray`` of fitted values.
        - ``"residuals"``: ``numpy.ndarray`` of model residuals.
        - ``"meta"``: ``dict`` with ``stage="mod"``,
          ``type="spatial_reg"``, and a ``history`` entry.

    Raises:
        TypeError: If *df* is not a ``pandas.DataFrame``, or *W* is not
            a ``dict``.
        ValueError: If *formula* is malformed, *W* lacks ``"listw"``,
            *model*/*method* is invalid, referenced columns are
            missing/non-numeric, row counts don't match, or islands are
            found while *zero_policy* is ``False``.
        NotImplementedError: If *model* is ``"sac"``, or *method* has
            no spreg equivalent for the chosen *model* (see **Model
            families** and *method* above).
        ImportError: If ``spreg`` or ``esda`` is not installed.

    Examples::

        import climasus4py as cs

        # W = cs.sus_mod_spatial_weights(muni, style="W")
        result = cs.sus_mod_spatial_reg(
            df=my_df,
            formula="deaths ~ mean_temp + precip + pib_pc",
            W=W,
            model="lag",
        )
        result["coefficients"]
        result["rho"]

        # SDM with spillovers
        result_sdm = cs.sus_mod_spatial_reg(
            df=my_df, formula="deaths ~ mean_temp + precip", W=W, model="durbin",
        )
        result_sdm["impacts"]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if model not in VALID_MODELS:
        raise ValueError(msg["err_model"].format(model=model, valid=", ".join(VALID_MODELS)))
    if method not in VALID_METHODS:
        raise ValueError(msg["err_method"].format(method=method, valid=", ".join(VALID_METHODS)))
    model_label = _MODEL_LABELS[model]

    if model == "sac":
        raise NotImplementedError(msg["err_sac_not_implemented"])

    try:
        import spreg
    except ImportError as exc:
        raise ImportError(
            "spreg is required for sus_mod_spatial_reg(). "
            "Install it with: pip install spreg"
        ) from exc
    try:
        import esda
    except ImportError as exc:
        raise ImportError(
            "esda is required for sus_mod_spatial_reg() (Moran test on "
            "residuals). Install it with: pip install esda"
        ) from exc

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_check"])

    if not isinstance(df, pd.DataFrame):
        raise TypeError(msg["err_not_df"])

    lhs, rhs_terms = _parse_formula(formula, msg)

    if not isinstance(W, dict):
        raise TypeError(msg["err_not_weights"])
    if "listw" not in W:
        raise ValueError(msg["err_no_listw"])
    listw = W["listw"]

    n_islands = int(W.get("n_islands", 0))
    if n_islands > 0 and not zero_policy:
        raise ValueError(msg["err_islands_strict"].format(n_islands=n_islands))

    # -- drop sf/geopandas geometry if present ------------------------------
    try:
        import geopandas as gpd

        is_gdf = isinstance(df, gpd.GeoDataFrame)
    except ImportError:
        is_gdf = False
    if is_gdf:
        if verbose:
            console.print("[yellow]WARNING[/]  " + msg["warn_sf_drop"])
        df = pd.DataFrame(df.drop(columns=[df.geometry.name]))

    # -- align df rows to W["listw"].id_order --------------------------------
    ids = list(listw.id_order)
    n_w = len(ids)
    if len(df) != n_w:
        raise ValueError(msg["err_row_mismatch"].format(n_df=len(df), n_w=n_w))
    if "code_muni" in df.columns:
        key = df["code_muni"].astype(str)
        if set(key) != set(ids):
            raise ValueError(msg["err_id_mismatch"])
        df = df.set_index(key).loc[ids].reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    # -- validate/extract formula columns -------------------------------------
    needed_cols = [lhs, *rhs_terms]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise ValueError(msg["err_missing_cols"].format(cols=", ".join(missing)))

    try:
        y = pd.to_numeric(df[lhs]).to_numpy(dtype=float).reshape(-1, 1)
    except (TypeError, ValueError) as exc:
        raise ValueError(msg["err_non_numeric"].format(col=lhs, err=exc)) from exc
    x_cols = []
    for col in rhs_terms:
        try:
            x_cols.append(pd.to_numeric(df[col]).to_numpy(dtype=float))
        except (TypeError, ValueError) as exc:
            raise ValueError(msg["err_non_numeric"].format(col=col, err=exc)) from exc
    x = np.column_stack(x_cols) if x_cols else np.empty((len(df), 0))

    n = len(y)

    # -- OLS baseline (for AIC comparison) -------------------------------------
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_ols"])
    x_ols = np.hstack([np.ones((n, 1)), x])
    beta_ols, *_ = np.linalg.lstsq(x_ols, y, rcond=None)
    resid_ols = y - x_ols @ beta_ols
    rss_ols = float(np.sum(resid_ols**2))
    p_ols = x_ols.shape[1]
    loglik_ols = -n / 2 * (np.log(2 * np.pi) + np.log(rss_ols / n) + 1)
    lm_aic = float(-2 * loglik_ols + 2 * (p_ols + 1))

    # -- fit spatial model ------------------------------------------------------
    spreg_method = _resolve_spreg_method(model, method, msg)
    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_fit"].format(model_label=model_label, method=method)
        )

    if model in ("lag", "durbin"):
        slx_lags = 1 if model == "durbin" else 0
        fit = spreg.ML_Lag(
            y,
            x,
            w=listw,
            slx_lags=slx_lags,
            method=spreg_method,
            name_x=rhs_terms,
            spat_impacts="full",
        )
    else:  # model == "error"
        fit = spreg.ML_Error(
            y,
            x,
            w=listw,
            method=spreg_method,
            name_x=rhs_terms,
        )

    # -- extract coefficients (excluding rho/lambda) -----------------------------
    out = fit.output
    spatial_rows = out["var_names"].isin(["W_dep_var", "lambda"])
    coef_df = pd.DataFrame(
        {
            "term": out.loc[~spatial_rows, "var_names"].tolist(),
            "estimate": out.loc[~spatial_rows, "coefficients"].astype(float).tolist(),
            "std_error": out.loc[~spatial_rows, "std_err"].astype(float).tolist(),
            "z_value": out.loc[~spatial_rows, "zt_stat"].astype(float).tolist(),
            "p_value": out.loc[~spatial_rows, "prob"].astype(float).tolist(),
        }
    )

    rho = float(fit.rho) if model in ("lag", "durbin") else None
    lam = float(fit.lam) if model == "error" else None

    # -- impacts (lag/durbin only) ------------------------------------------------
    impacts_df = None
    if model in ("lag", "durbin"):
        if verbose:
            console.print("[cyan]INFO[/]  " + msg["step_impacts"])
        try:
            adi, aii, ati = fit.sp_multipliers["full"]
            betas_map = dict(
                zip(out["var_names"], out["coefficients"].astype(float), strict=False)
            )
            own_terms = [
                v
                for v in out["var_names"]
                if v not in ("CONSTANT", "W_dep_var") and not str(v).startswith("W_")
            ]
            rows = []
            for term in own_terms:
                beta = betas_map[term]
                theta = betas_map.get(f"W_{term}", 0.0)
                total = ati * (beta + theta)
                direct = adi * beta
                indirect = total - direct
                rows.append((term, direct, indirect, total))
            impacts_df = pd.DataFrame(rows, columns=["term", "direct", "indirect", "total"])
        except Exception as exc:  # noqa: BLE001 - mirrors R's tryCatch(..., error=...)
            if verbose:
                console.print(
                    "[yellow]WARNING[/]  " + msg["warn_impacts"].format(msg=str(exc))
                )
            impacts_df = None

    fitted = np.asarray(fit.predy).flatten()
    residuals = np.asarray(fit.u).flatten()

    # fit.aic is NOT used directly: spreg's own parameter count (reg.k) is
    # inconsistent between ML_Lag (includes rho) and ML_Error (excludes
    # lambda), and neither counts sigma2 -- see spreg.diagnostics.akaike().
    # Recomputed here to match R's stats::AIC.sarlm convention, where the
    # parameter count is len(coefficients incl. rho/lambda) + 1 (sigma2),
    # so aic and lm_aic are on a comparable scale.
    n_params = int(np.asarray(fit.betas).flatten().shape[0]) + 1
    mod_aic = float(-2 * fit.logll + 2 * n_params)

    # -- Moran test on residuals -----------------------------------------------
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_moran"])
    # R hardcodes zero.policy = TRUE for this specific spdep::moran.test()
    # call regardless of the user's zero_policy argument (R source lines
    # 429-434) -- a preserved quirk, see IDEIAS.md. esda has no equivalent
    # knob to mirror either way, so this is behaviourally moot here.
    try:
        mt = esda.Moran(residuals, listw)
        moran_out = {"I": float(mt.I), "p_value": float(mt.p_rand), "z": float(mt.z_rand)}
    except Exception:  # noqa: BLE001 - mirrors R's tryCatch(..., error=...)
        moran_out = {"I": float("nan"), "p_value": float("nan"), "z": float("nan")}

    if verbose:
        p_moran = (
            f"{moran_out['p_value']:.4f}"
            if not np.isnan(moran_out["p_value"])
            else "NA"
        )
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(aic_m=f"{mod_aic:.2f}", aic_ols=f"{lm_aic:.2f}", p_moran=p_moran)
        )

    now = datetime.now()
    meta = {
        "stage": "mod",
        "type": "spatial_reg",
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_mod_spatial_reg(): model={model}; method={method}; n={n}"
        ],
    }

    return {
        "model": model,
        "model_label": model_label,
        "coefficients": coef_df,
        "rho": rho,
        "lambda": lam,
        "impacts": impacts_df,
        "aic": mod_aic,
        "lm_aic": lm_aic,
        "moran_residuals": moran_out,
        "fitted": fitted,
        "residuals": residuals,
        "meta": meta,
    }
