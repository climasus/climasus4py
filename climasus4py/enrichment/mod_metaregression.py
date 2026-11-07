"""Meta-regression of pooled DLNM estimates with city-level covariates.

Mirrors R: sus_mod_metaregression.R

Extends :func:`climasus4py.enrichment.mod_pool.sus_mod_pool` by letting
city-level predictors (mean temperature, a poverty index, ...) explain the
between-city heterogeneity that plain pooling can only measure.

**This port deliberately diverges from R**, which slices ``mvmeta``'s
coefficient vector as if one moderator's effects sat in a contiguous
leading block. They do not: the vector is outcome-major and interleaved
(``y1.(Intercept)``, ``y1.x``, ``y2.(Intercept)``, ...), so R's slice is
half intercepts and half covariate slopes, covering only the first half of
the outcomes. That corrupts both the pooled curve and every covariate
Wald test. Recorded as M67; see :meth:`_mvmeta.MvmetaFit.block`, which is
how this module reads the blocks instead.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

from climasus4py.enrichment._mvmeta import MvmetaFit, mvmeta_fit, qtest
from climasus4py.enrichment._mvmeta import blup as _mvmeta_blup
from climasus4py.enrichment.mod_dlnm import _crosspred
from climasus4py.enrichment.mod_pool import (
    _REQUIRED_KEYS,
    _check_shared,
    _city_coefficients,
    _city_exposure,
    _msgs as _pool_msgs,
    _same_basis,
)

console = Console(stderr=True)


_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "header": "climasus4py — Meta-regressao ({n} cidades)",
        "step_align": "Alinhando covariaveis e padronizando...",
        "step_metareg": "Ajustando o modelo nulo e o completo ({method})...",
        "step_pred": "Predizendo a curva no valor medio das covariaveis...",
        "step_tests": "Testes de Wald por covariavel...",
        "done": "RR no p75: {rr} [{lo}, {hi}]; I2 = {i2}%; R2 = {r2}",
        "err_covariates": "'covariates' deve ser um DataFrame com ao menos uma coluna numerica.",
        "err_city_col": "Coluna '{col}' nao existe em 'covariates'.",
        "err_no_match": (
            "Nenhuma cidade de 'fits' foi encontrada em 'covariates'. "
            "Em 'fits': {fits}. Em 'covariates': {cov}."
        ),
        "warn_unmatched": (
            "{n} cidade(s) de 'fits' ausente(s) em 'covariates': {cities}. "
            "Excluida(s) da meta-regressao."
        ),
        "warn_constant": (
            "A(s) covariavel(is) {cols} tem desvio-padrao zero entre as "
            "cidades e nao explica(m) heterogeneidade nenhuma."
        ),
        "warn_few_cities": (
            "{m} cidades para {q} termos por resultado. A meta-regressao "
            "precisa de mais cidades que termos; com esta razao os "
            "coeficientes das covariaveis nao sao identificaveis."
        ),
    },
    "en": {
        "header": "climasus4py — Meta-regression ({n} cities)",
        "step_align": "Aligning covariates and standardising...",
        "step_metareg": "Fitting the null and full models ({method})...",
        "step_pred": "Predicting the curve at the covariate means...",
        "step_tests": "Per-covariate Wald tests...",
        "done": "RR at p75: {rr} [{lo}, {hi}]; I2 = {i2}%; R2 = {r2}",
        "err_covariates": "'covariates' must be a DataFrame with at least one numeric column.",
        "err_city_col": "Column '{col}' not found in 'covariates'.",
        "err_no_match": (
            "No city from 'fits' was found in 'covariates'. "
            "In 'fits': {fits}. In 'covariates': {cov}."
        ),
        "warn_unmatched": (
            "{n} city/cities in 'fits' absent from 'covariates': {cities}. "
            "Excluded from the meta-regression."
        ),
        "warn_constant": (
            "Covariate(s) {cols} have zero standard deviation across cities "
            "and cannot explain any heterogeneity."
        ),
        "warn_few_cities": (
            "{m} cities for {q} terms per outcome. Meta-regression needs more "
            "cities than terms; at this ratio the covariate coefficients are "
            "not identifiable."
        ),
    },
    "es": {
        "header": "climasus4py — Metarregresion ({n} ciudades)",
        "step_align": "Alineando covariables y estandarizando...",
        "step_metareg": "Ajustando el modelo nulo y el completo ({method})...",
        "step_pred": "Prediciendo la curva en la media de las covariables...",
        "step_tests": "Pruebas de Wald por covariable...",
        "done": "RR en p75: {rr} [{lo}, {hi}]; I2 = {i2}%; R2 = {r2}",
        "err_covariates": "'covariates' debe ser un DataFrame con al menos una columna numerica.",
        "err_city_col": "La columna '{col}' no existe en 'covariates'.",
        "err_no_match": (
            "Ninguna ciudad de 'fits' fue encontrada en 'covariates'. "
            "En 'fits': {fits}. En 'covariates': {cov}."
        ),
        "warn_unmatched": (
            "{n} ciudad(es) de 'fits' ausente(s) en 'covariates': {cities}. "
            "Excluida(s) de la metarregresion."
        ),
        "warn_constant": (
            "La(s) covariable(s) {cols} tiene(n) desviacion tipica cero entre "
            "ciudades y no explica(n) heterogeneidad alguna."
        ),
        "warn_few_cities": (
            "{m} ciudades para {q} terminos por resultado. La metarregresion "
            "necesita mas ciudades que terminos; con esta razon los "
            "coeficientes de las covariables no son identificables."
        ),
    },
}


def _msgs(lang: str) -> dict[str, str]:
    """Meta-regression messages, layered over the pooling ones."""
    base = _pool_msgs(lang)
    key = lang if lang in _MESSAGES else "pt"
    return {**base, **_MESSAGES[key]}


def _align_covariates(
    covariates: pd.DataFrame,
    city_names: list[str],
    covariate_cols: list[str] | None,
    city_col: str | None,
    msgs: dict[str, str],
) -> tuple[pd.DataFrame, list[str]]:
    """Match the covariate table to the fitted cities, in fit order."""
    if not isinstance(covariates, pd.DataFrame):
        raise ValueError(msgs["err_covariates"])

    table = covariates
    if city_col is not None:
        if city_col not in table.columns:
            raise ValueError(msgs["err_city_col"].format(col=city_col))
        table = table.set_index(table[city_col].astype(str)).drop(columns=[city_col])
    else:
        table = table.set_index(table.index.astype(str))

    if covariate_cols is None:
        covariate_cols = [
            c for c in table.columns if pd.api.types.is_numeric_dtype(table[c])
        ]
    covariate_cols = [c for c in covariate_cols if c in table.columns]
    if not covariate_cols:
        raise ValueError(msgs["err_covariates"])

    matched = [c for c in city_names if c in table.index]
    if not matched:
        raise ValueError(
            msgs["err_no_match"].format(fits=city_names, cov=list(table.index)[:10])
        )
    unmatched = [c for c in city_names if c not in table.index]
    if unmatched:
        console.print(f"[yellow]{msgs['warn_unmatched'].format(
            n=len(unmatched), cities=unmatched)}[/yellow]")

    return table.loc[matched, covariate_cols].astype(float), covariate_cols


def sus_mod_metaregression(
    fits: dict[str, Any],
    covariates: pd.DataFrame,
    covariate_cols: list[str] | None = None,
    city_col: str | None = None,
    pred_at: tuple[float, ...] = (0.75, 0.90, 0.95, 0.99),
    blup: bool = True,  # noqa: A002 - name kept for parity with R
    method: Literal["reml", "ml", "fixed"] = "reml",
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Meta-regression of pooled DLNM estimates with city-level covariates.

    Fits a null (intercept-only) and a full model, reports a Wald test per
    covariate, and predicts the exposure-response curve at the covariate
    means. Covariates are standardised, so the intercept block *is* the
    curve for an average city.

    Args:
        fits: ``{city_name: fit}``, each fit as returned by
            ``sus_mod_dlnm()``. All must share ``climate_col`` and
            ``lag_max``.
        covariates: One row per city, holding city-level predictors.
        covariate_cols: Columns to use, or ``None`` for every numeric one.
        city_col: Column holding city identifiers, or ``None`` to use the
            index.
        pred_at: Quantile probabilities for the summary table.
        blup: Whether to compute per-city BLUP curves. Default ``True``.
        method: ``"reml"`` (default), ``"ml"``, or ``"fixed"``.
        alpha: Significance level for the confidence intervals.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress. Default ``True``.

    Returns:
        A dict mirroring R's ``climasus_metaregression`` object, with keys
        ``mvmeta_fit``, ``null_fit``, ``pooled_pred``, ``pooled_curve``,
        ``exposure_response``, ``blup_preds``, ``city_table``,
        ``covariate_tests``, ``heterogeneity``, ``cov_scales`` and
        ``meta``.

    Raises:
        ValueError: If ``fits`` is empty or malformed, if the fits
            disagree on ``climate_col``/``lag_max``, if ``covariates`` has
            no usable numeric column, or if no city matches.
    """
    msgs = _msgs(lang)

    if not isinstance(fits, dict):
        raise ValueError(msgs["err_not_dict"])
    if len(fits) == 0:
        raise ValueError(msgs["err_empty"])
    bad = {
        name: [k for k in _REQUIRED_KEYS if not isinstance(fit, dict) or k not in fit]
        for name, fit in fits.items()
    }
    bad = {k: v for k, v in bad.items() if v}
    if bad:
        raise ValueError(msgs["err_not_dlnm"].format(
            bad=sorted(bad), missing=sorted({k for v in bad.values() for k in v})))

    if verbose:
        console.print(f"[bold cyan]{msgs['header'].format(n=len(fits))}[/bold cyan]")
        console.print(msgs["step_align"])

    cov_table, covariate_cols = _align_covariates(
        covariates, list(fits.keys()), covariate_cols, city_col, msgs
    )
    fits = {name: fits[name] for name in cov_table.index}
    city_names = list(fits.keys())
    n_cities = len(city_names)

    lag_max = int(_check_shared(fits, "lag_max", msgs))
    climate_col = _check_shared(fits, "climate_col", msgs)

    ref_name = city_names[0]
    ref_meta = fits[ref_name]["meta"]
    divergent = [
        n for n in city_names[1:]
        if not _same_basis(fits[n]["meta"]["var_meta"], ref_meta["var_meta"])
    ]
    if divergent:
        console.print(f"[yellow]{msgs['warn_basis_differs'].format(
            cities=divergent, ref=ref_name)}[/yellow]")

    # ---- standardise the covariates --------------------------------
    sds = cov_table.std(ddof=1)
    # A covariate that never varies is collinear with the intercept, so
    # keeping it would make the design singular. R scales it by 1 and
    # carries it into the fit; dropping it is both what the caller meant
    # and the only way the rest of the model stays identifiable.
    constant = [c for c in covariate_cols if not np.isfinite(sds[c]) or sds[c] == 0]
    if constant:
        console.print(f"[yellow]{msgs['warn_constant'].format(cols=constant)}[/yellow]")
        covariate_cols = [c for c in covariate_cols if c not in constant]
        if not covariate_cols:
            raise ValueError(msgs["err_covariates"])
        cov_table = cov_table[covariate_cols]
        sds = cov_table.std(ddof=1)
    means = cov_table.mean()
    cov_scaled = (cov_table - means) / sds
    cov_scales = pd.DataFrame({
        "covariate": covariate_cols,
        "mean": means[covariate_cols].to_numpy(),
        "sd": sds[covariate_cols].to_numpy(),
    })

    n_terms = len(covariate_cols) + 1
    if n_cities <= n_terms:
        console.print(f"[yellow]{msgs['warn_few_cities'].format(
            m=n_cities, q=n_terms)}[/yellow]")

    # ---- stage 1 ----------------------------------------------------
    if verbose:
        console.print(msgs["step_extract"])
    coefs, vcovs = [], []
    for name in city_names:
        c_i, v_i = _city_coefficients(fits[name], name, msgs)
        coefs.append(c_i)
        vcovs.append(v_i)
    if len({len(c) for c in coefs}) > 1:
        raise ValueError(msgs["err_incompatible"].format(
            field="n_coef", values=sorted({len(c) for c in coefs})))
    coef_mat = np.vstack(coefs)

    # ---- stage 2: null and full ------------------------------------
    if verbose:
        console.print(msgs["step_metareg"].format(method=method))
    null_fit: MvmetaFit = mvmeta_fit(coef_mat, vcovs, method=method)
    design = np.column_stack([np.ones(n_cities), cov_scaled.to_numpy()])
    full_fit: MvmetaFit = mvmeta_fit(coef_mat, vcovs, X=design, method=method)
    if full_fit.overparametrised:
        console.print(f"[yellow]{msgs['warn_overparam'].format(
            n_par=full_fit.n_par_psi, m=n_cities, p=full_fit.n_outcomes,
            n_obs=n_cities * full_fit.n_outcomes)}[/yellow]")

    # ---- prediction at the covariate means -------------------------
    if verbose:
        console.print(msgs["step_pred"])
    all_expo = np.concatenate([_city_exposure(fits[n]) for n in city_names])
    expo_grid = np.linspace(float(np.min(all_expo)), float(np.max(all_expo)), 100)
    ref_value = float(np.median(all_expo))

    # Covariates are centred, so moderator 0 — the intercept — is exactly
    # the curve for a city at the average of every covariate. R takes the
    # first n_coef entries of the flattened coefficient vector here, which
    # is a different (and wrong) set of numbers; see M67.
    coef_intercept, vcov_intercept = full_fit.block(0)
    pooled_pred = _crosspred(
        ref_meta["var_meta"], ref_meta["lag_meta"],
        coef_intercept, vcov_intercept,
        expo_grid, lag_max, ref_value, alpha, lang,
    )

    quantis = np.quantile(all_expo, pred_at)
    exposure_response = pd.DataFrame([
        {
            "pct": pct,
            "exposure": round(float(val), 3),
            "rr": float(pooled_pred["allRRfit"][idx]),
            "rr_lo": float(pooled_pred["allRRlow"][idx]),
            "rr_hi": float(pooled_pred["allRRhigh"][idx]),
        }
        for pct, val, idx in (
            (p, v, int(np.argmin(np.abs(expo_grid - v))))
            for p, v in zip(pred_at, quantis, strict=True)
        )
    ])
    pooled_curve = pd.DataFrame({
        "exposure": pooled_pred["predvar"],
        "rr": pooled_pred["allRRfit"],
        "rr_lo": pooled_pred["allRRlow"],
        "rr_hi": pooled_pred["allRRhigh"],
    })

    # ---- Wald tests, one per covariate -----------------------------
    if verbose:
        console.print(msgs["step_tests"])
    covariate_tests = _wald_tests(full_fit, covariate_cols)

    heterogeneity = _heterogeneity(null_fit, full_fit)

    # ---- per-city table and BLUPs ----------------------------------
    p75_val = float(np.quantile(all_expo, 0.75))
    p75_idx = int(np.argmin(np.abs(expo_grid - p75_val)))
    rows = []
    for name in city_names:
        models = fits[name].get("models")
        first = models.iloc[0] if models is not None and len(models) else {}
        rows.append({
            "city": name,
            "n_obs": fits[name]["meta"].get("n"),
            "raw_rr": float(first["rr"]) if "rr" in first else np.nan,
            "raw_rr_lo": float(first["lo"]) if "lo" in first else np.nan,
            "raw_rr_hi": float(first["hi"]) if "hi" in first else np.nan,
            "blup_rr": np.nan, "blup_rr_lo": np.nan, "blup_rr_hi": np.nan,
        })
    city_table = pd.DataFrame(rows)

    blup_preds: dict[str, dict[str, Any]] | None = None
    if blup and n_cities >= 2:
        if verbose:
            console.print(msgs["step_blup"])
        blup_preds = {}
        for i, (name, b) in enumerate(zip(city_names, _mvmeta_blup(full_fit),
                                          strict=True)):
            pred_b = _crosspred(
                ref_meta["var_meta"], ref_meta["lag_meta"],
                b["blup"], b["vcov"], expo_grid, lag_max, ref_value, alpha, lang,
            )
            blup_preds[name] = pred_b
            city_table.loc[i, "blup_rr"] = float(pred_b["allRRfit"][p75_idx])
            city_table.loc[i, "blup_rr_lo"] = float(pred_b["allRRlow"][p75_idx])
            city_table.loc[i, "blup_rr_hi"] = float(pred_b["allRRhigh"][p75_idx])

    if verbose:
        row = exposure_response.iloc[
            int(np.argmin(np.abs(exposure_response["pct"] - 0.75)))
        ]
        cheia = heterogeneity[heterogeneity["model"] == "full"].iloc[0]
        console.print(msgs["done"].format(
            rr=round(float(row["rr"]), 4), lo=round(float(row["rr_lo"]), 4),
            hi=round(float(row["rr_hi"]), 4), i2=cheia["i2"], r2=cheia["r2_het"],
        ))

    return {
        "mvmeta_fit": full_fit,
        "null_fit": null_fit,
        "pooled_pred": pooled_pred,
        "pooled_curve": pooled_curve,
        "exposure_response": exposure_response,
        "blup_preds": blup_preds,
        "city_table": city_table,
        "covariate_tests": covariate_tests,
        "heterogeneity": heterogeneity,
        "cov_scales": cov_scales,
        "meta": {
            "climate_col": climate_col,
            "outcome_col": ref_meta.get("outcome_col"),
            "lag_max": lag_max,
            "n_cities": n_cities,
            "city_names": city_names,
            "covariate_cols": covariate_cols,
            "method": method,
            "alpha": alpha,
            "ref_value": ref_value,
            "expo_grid": expo_grid,
            "pred_at": pred_at,
            "shared_basis": not divergent,
            "call_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }


def _wald_tests(fit: MvmetaFit, covariate_cols: list[str]) -> pd.DataFrame:
    """Joint Wald test that a covariate has no effect on any coefficient.

    For covariate *j*, tests ``H0: beta_j = 0`` across all ``p``
    cross-basis coefficients at once — a single chi-square on ``p``
    degrees of freedom, rather than ``p`` separate tests.
    """
    from scipy import stats  # noqa: PLC0415

    rows = []
    for j, name in enumerate(covariate_cols, start=1):
        coef_j, vcov_j = fit.block(j)
        try:
            stat = float(coef_j @ np.linalg.solve(vcov_j, coef_j))
        except np.linalg.LinAlgError:
            stat = float("nan")
        df = fit.n_outcomes
        rows.append({
            "covariate": name,
            "n_df": df,
            "wald_stat": round(stat, 3) if np.isfinite(stat) else np.nan,
            "p_value": (round(float(stats.chi2.sf(stat, df)), 4)
                        if np.isfinite(stat) else np.nan),
        })
    return pd.DataFrame(rows, columns=["covariate", "n_df", "wald_stat", "p_value"])


def _heterogeneity(null_fit: MvmetaFit, full_fit: MvmetaFit) -> pd.DataFrame:
    """Residual heterogeneity before and after the covariates.

    ``r2_het`` is the share of the null model's I-squared that the
    covariates account for — how much of the between-city variation they
    explain.
    """
    rows = []
    for label, fit in (("null", null_fit), ("full", full_fit)):
        het = qtest(fit)
        rows.append({
            "model": label,
            "Q": round(het["Q"], 2),
            "df_het": het["df"],
            "p_het": round(het["p_value"], 4) if np.isfinite(het["p_value"]) else np.nan,
            "i2": round(het["i2"], 1) if np.isfinite(het["i2"]) else np.nan,
            "r2_het": np.nan,
        })
    tabela = pd.DataFrame(rows)
    i2_null, i2_full = tabela.loc[0, "i2"], tabela.loc[1, "i2"]
    if np.isfinite(i2_null) and np.isfinite(i2_full) and i2_null > 0:
        tabela.loc[1, "r2_het"] = round(max(0.0, (i2_null - i2_full) / i2_null), 3)
    return tabela
