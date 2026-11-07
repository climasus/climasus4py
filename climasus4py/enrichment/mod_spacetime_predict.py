"""Predictions from a fitted spatiotemporal Bayesian disease-mapping model.

Mirrors R: sus_mod_spacetime_predict.R

Consumes the object from :func:`~climasus4py.enrichment.mod_spacetime_bayes
.sus_mod_spacetime_bayes`. R composes the prediction from the fit's
**summary statistics** rather than its posterior draws — means add,
variances add — which is why its own documentation calls the result
approximate. That arithmetic is followed here.

Five broken producer/consumer contracts in R, and four of them silent (M110)
----------------------------------------------------------------------------
R's predict reads slots and columns that R's own fitter never writes. The
mismatches, all verified by calling the two functions against each other:

1. ``temporal_re[["gamma_mean"]]`` / ``[["gamma_sd"]]`` — the fitter writes
   ``psi_mean`` / ``psi_sd``; ``gamma_*`` are the *interaction* table's
   names. And the guard is::

       if (!is.null(temporal_re) && "time_idx" %in% names(temporal_re)
           && "gamma_mean" %in% names(temporal_re))

   so the whole temporal block is **skipped**, ``eta_temporal`` stays zero,
   and no error or warning is raised. **The temporal random effect is
   dropped from every prediction.** Measured on the reference fit: the
   first cell predicts 1.0103 without it and 0.9114 with it — an 11% error
   that looks like a perfectly ordinary number.
2. ``fit[["data_last_obs"]]`` — never written, so for ``horizon > 0`` the
   covariates fall to the ``NA_real_`` branch and every forecast comes back
   ``NA``. Reproduced: ``horizon = 2`` returned 84 rows of ``NA``.
3. ``fit[["temporal_model"]]`` — the fitter puts it in ``model_spec``, so
   the random-walk order always falls back to 1, and an RW2 fit is
   extrapolated with no drift.
4. ``fit[["family"]]`` — also in ``model_spec``, so the family always
   defaults to Poisson.
5. ``fit[["fitted"]]`` is read as a data frame with ``code_muni`` and
   ``time_idx`` columns; the fitter returns a plain numeric vector, so
   those fallbacks never fire either.

Only ``fixed``, ``spatial_re`` and ``temporal_re``'s ``time_idx`` actually
connect. This port reads the names the producer writes, and
``sus_mod_spacetime_bayes`` here also stores ``data_last_obs`` so that
forecasting with covariates works at all.

Note the contrast with M88: there a regex mistake dropped the intercept.
Here the same function's intercept regex is written correctly,
``^(Intercept|\\(Intercept\\))$``, so that defect is specific to
``sus_mod_spatial_bayes``.
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd
from scipy import stats

__all__ = ["sus_mod_spacetime_predict"]

_Z95 = 1.959963984540054

#: Drop the temporal random effect from predictions, as R does (M110).
#:
#: Set to ``True`` to reproduce R's numbers for a parity check. Default
#: ``False``: the temporal effect is part of the model that was fitted, and
#: silently omitting it returns a plausible-looking number that is simply
#: wrong — measured at 11% on the reference fit's first cell.
R_DROPS_TEMPORAL_EFFECT: bool = False

_MESSAGES = {
    "pt": {
        "pred": "[sus_mod_spacetime_predict] {n} pontos"
                "{extra}",
        "extrap": ", extrapolando {h} periodo(s) com {mo}",
    },
    "en": {
        "pred": "[sus_mod_spacetime_predict] {n} points{extra}",
        "extrap": ", extrapolating {h} period(s) with {mo}",
    },
    "es": {
        "pred": "[sus_mod_spacetime_predict] {n} puntos{extra}",
        "extrap": ", extrapolando {h} periodo(s) con {mo}",
    },
}


def _msg(key: str, lang: str, **kw: Any) -> str:
    return _MESSAGES.get(lang, _MESSAGES["pt"])[key].format(**kw)


def _spec(fit: dict, chave: str, default: Any) -> Any:
    """Read from ``model_spec``, falling back to a top-level slot.

    R reads ``fit[["family"]]`` and ``fit[["temporal_model"]]`` directly and
    its fitter writes neither — both live in ``model_spec``. Both places
    are accepted here so a hand-built fit still works.
    """
    spec = fit.get("model_spec") or {}
    if chave in spec and spec[chave] is not None:
        return spec[chave]
    return fit.get(chave, default)


def sus_mod_spacetime_predict(
    fit: Any,
    newdata: Any = None,
    horizon: int = 0,
    covariates_new: dict[str, Any] | None = None,
    include_ci: bool = True,
    return_samples: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Predict relative risk at new or future space-time points.

    The linear predictor is assembled from the fit's summaries, as R does::

        eta   = intercept + x' beta + phi_i + psi_t
        var   = sd_intercept^2 + sum (x sd_beta)^2 + phi_sd^2 + psi_sd^2
        pred  = exp(eta)                       (Poisson / negative binomial)
        CI    = exp(eta +/- 1.96 sqrt(var))

    Adding variances treats the terms as independent, which they are not —
    they come from one joint posterior. The interval is therefore
    approximate, and R says so too. When the fit carries its draws
    (``"idata"``), ``return_samples=True`` gives the exact alternative.

    **Beyond the last observed period**, ``psi`` is extrapolated as
    ``psi_last + drift * steps`` with the variance growing linearly,
    ``psi_sd_last^2 * steps`` — the random walk's own variance growth.
    ``drift`` is zero for RW1 and the last observed increment for RW2,
    following R. Covariates are carried forward from the last observed
    period unless *covariates_new* overrides them.

    Args:
        fit: The dict returned by ``sus_mod_spacetime_bayes()``.
        newdata: Table with ``code_muni`` and ``time_idx`` (plus any
            covariate the fit used) for arbitrary prediction points, or
            ``None`` to use *horizon*.
        horizon: Future periods to extrapolate when *newdata* is ``None``.
            Must be ``0`` if *newdata* is given.
        covariates_new: Covariate overrides for counterfactual scenarios,
            as ``{name: value}``. A scalar applies to every row.
        include_ci: Whether to add ``pred_lower95`` / ``pred_upper95``.
        return_samples: Draw predictions from the fit's posterior instead of
            composing summaries. Requires ``fit["idata"]``. Where R needs
            ``INLA::inla.posterior.sample()`` and errors without the stored
            INLA object, this uses the draws already in the fit.
        lang: Message language.
        verbose: Whether to print progress.

    Returns:
        A dict mirroring R's ``climasus_spacetime_pred`` object:

        - ``"predictions"``: ``DataFrame`` with ``code_muni``,
          ``time_idx``, ``pred_mean`` and, with *include_ci*,
          ``pred_lower95`` / ``pred_upper95``. Also ``in_sample``, flagging
          which rows are extrapolations — R does not carry it, and without
          it a forecast is indistinguishable from a fitted value.
        - ``"n_predicted"``, ``"horizon"``.
        - ``"samples"``: ``(draws, n)`` array when *return_samples*, else
          ``None``.

    Raises:
        TypeError: If *fit* is not a mapping with a ``"fixed"`` table.
        ValueError: If *horizon* is negative, if both *newdata* and a
            positive *horizon* are given, if *newdata* lacks
            ``code_muni``/``time_idx``, or if *return_samples* is asked of a
            fit with no draws.
    """
    if not isinstance(fit, dict) or "fixed" not in fit:
        raise TypeError(
            "fit must be the dict returned by sus_mod_spacetime_bayes() "
            "(or sus_mod_spatial_bayes()); it needs at least a 'fixed' "
            "table."
        )
    horizon = int(horizon)
    if horizon < 0:
        raise ValueError(f"horizon must be >= 0; got {horizon}.")
    if newdata is not None and horizon > 0:
        raise ValueError(
            "pass either newdata or horizon > 0, not both: newdata already "
            "says which points to predict."
        )

    fixed = pd.DataFrame(fit["fixed"])
    termos = fixed["term"].astype(str).tolist()
    e_intercepto = [t.strip("()").lower() == "intercept" for t in termos]
    media = dict(zip(termos, fixed["mean"].to_numpy(dtype=float)))
    desvio = (dict(zip(termos, fixed["sd"].to_numpy(dtype=float)))
              if "sd" in fixed.columns
              else {t: 0.0 for t in termos})
    desvio = {k: (0.0 if pd.isna(v) else float(v)) for k, v in desvio.items()}

    b0 = sum(media[t] for t, e in zip(termos, e_intercepto) if e)
    var0 = sum(desvio[t] ** 2 for t, e in zip(termos, e_intercepto) if e)
    covariaveis = [t for t, e in zip(termos, e_intercepto) if not e]

    espacial = fit.get("spatial_re")
    if espacial is None:
        espacial = fit.get("random")
    espacial = pd.DataFrame(espacial) if espacial is not None else None
    temporal = (pd.DataFrame(fit["temporal_re"])
                if fit.get("temporal_re") is not None else None)
    familia = _spec(fit, "family", "poisson")
    modelo_t = str(_spec(fit, "temporal_model", "rw1"))

    # --- the prediction grid ---------------------------------------------
    if newdata is not None:
        grade = pd.DataFrame(newdata).copy()
        for col in ("code_muni", "time_idx"):
            if col not in grade.columns:
                raise ValueError(f"newdata must have a {col!r} column.")
    else:
        if horizon == 0:
            vazio = pd.DataFrame({"code_muni": [], "time_idx": [],
                                  "pred_mean": [], "in_sample": []})
            if include_ci:
                vazio["pred_lower95"] = []
                vazio["pred_upper95"] = []
            return {"predictions": vazio, "n_predicted": 0, "horizon": 0,
                    "samples": None}
        ultimo_t = (int(temporal["time_idx"].max()) if temporal is not None
                    else int(fit.get("n_times", 0)))
        municipios = (espacial["code_muni"].to_numpy()
                      if espacial is not None else np.array([]))
        grade = pd.DataFrame(
            [(m, ultimo_t + h) for m in municipios
             for h in range(1, horizon + 1)],
            columns=["code_muni", "time_idx"])
        # carry the last observed covariates forward
        ultimo = fit.get("data_last_obs")
        if covariaveis and ultimo is not None and len(ultimo):
            ultimo = pd.DataFrame(ultimo)
            cols = ["code_muni", *(c for c in covariaveis
                                   if c in ultimo.columns)]
            grade = grade.merge(ultimo[cols].drop_duplicates("code_muni"),
                                on="code_muni", how="left")
        elif covariaveis:
            for c in covariaveis:
                grade[c] = np.nan

    if covariates_new:
        for nome, valor in covariates_new.items():
            grade[nome] = valor

    n_pred = len(grade)
    if verbose:
        extra = ("" if newdata is not None else
                 _msg("extrap", lang, h=horizon, mo=modelo_t))
        print(_msg("pred", lang, n=n_pred, extra=extra))

    # Refuse rather than predict NaN. R fills the missing covariates with
    # NA_real_ and hands back a table of NA without a word; the check has to
    # be for USABLE values, not merely for a column of the right name -
    # filling with NaN and then testing for presence is the same mistake,
    # and it is the one this test caught here.
    inutilizaveis = [
        c for c in covariaveis
        if c not in grade.columns
        or pd.to_numeric(grade[c], errors="coerce").isna().all()
    ]
    if inutilizaveis and n_pred:
        raise ValueError(
            f"the fit used covariate(s) {inutilizaveis} and the prediction "
            f"grid has no usable values for them. Supply them in newdata or "
            f"covariates_new, or fit with sus_mod_spacetime_bayes() so the "
            f"last observed period travels with the fit."
        )
    parciais = [
        c for c in covariaveis if c in grade.columns
        and pd.to_numeric(grade[c], errors="coerce").isna().any()
    ]
    if parciais:
        warnings.warn(
            f"covariate(s) {parciais} are missing on some prediction rows; "
            f"those rows come back as NaN.",
            UserWarning, stacklevel=2,
        )

    eta = np.full(n_pred, float(b0))
    var = np.full(n_pred, float(var0))
    for c in covariaveis:
        x = pd.to_numeric(grade[c], errors="coerce").to_numpy(dtype=float)
        eta = eta + x * media[c]
        var = var + (x * desvio[c]) ** 2

    # --- spatial ---------------------------------------------------------
    if espacial is not None and "phi_mean" in espacial.columns:
        mapa = espacial.set_index("code_muni")
        achou = grade["code_muni"].isin(mapa.index).to_numpy()
        if not achou.all():
            novos = sorted(pd.unique(grade["code_muni"][~achou]))
            warnings.warn(
                f"{len(novos)} municipality/ies are not in the fit, so they "
                f"get a spatial effect of zero: {novos[:5]}"
                f"{'...' if len(novos) > 5 else ''}",
                UserWarning, stacklevel=2,
            )
        alinhado = mapa.reindex(grade["code_muni"])
        eta = eta + alinhado["phi_mean"].fillna(0.0).to_numpy()
        if "phi_sd" in alinhado.columns:
            var = var + alinhado["phi_sd"].fillna(0.0).to_numpy() ** 2

    # --- temporal --------------------------------------------------------
    em_amostra = np.ones(n_pred, dtype=bool)
    if temporal is not None and not R_DROPS_TEMPORAL_EFFECT:
        col_m = "psi_mean" if "psi_mean" in temporal.columns else "gamma_mean"
        col_s = "psi_sd" if "psi_sd" in temporal.columns else "gamma_sd"
        if col_m in temporal.columns:
            t_obs = temporal["time_idx"].to_numpy().astype(int)
            t_max = int(t_obs.max())
            tv = grade["time_idx"].to_numpy().astype(int)
            em_amostra = tv <= t_max

            mapa_t = temporal.set_index("time_idx")
            dentro = mapa_t.reindex(tv[em_amostra])
            eta[em_amostra] += dentro[col_m].fillna(0.0).to_numpy()
            if col_s in mapa_t.columns:
                var[em_amostra] += dentro[col_s].fillna(0.0).to_numpy() ** 2

            if (~em_amostra).any():
                ordenado = temporal.sort_values("time_idx")
                psi_ult = float(ordenado[col_m].iloc[-1])
                sd_ult = (float(ordenado[col_s].iloc[-1])
                          if col_s in ordenado.columns else 0.0)
                # RW2 keeps the last increment as drift; RW1 has none
                ordem = 2 if "2" in modelo_t else 1
                deriva = (float(ordenado[col_m].iloc[-1]
                                - ordenado[col_m].iloc[-2])
                          if ordem == 2 and len(ordenado) >= 2 else 0.0)
                passos = tv[~em_amostra] - t_max
                eta[~em_amostra] += psi_ult + deriva * passos
                # a random walk's variance grows linearly in the horizon
                var[~em_amostra] += sd_ult ** 2 * passos

    dp = np.sqrt(np.maximum(var, 0.0))
    if familia in ("poisson", "nbinomial"):
        media_pred = np.exp(eta)
        baixo, alto = np.exp(eta - _Z95 * dp), np.exp(eta + _Z95 * dp)
    elif familia == "binomial":
        media_pred = stats.logistic.cdf(eta)
        baixo = stats.logistic.cdf(eta - _Z95 * dp)
        alto = stats.logistic.cdf(eta + _Z95 * dp)
    else:
        media_pred = eta
        baixo, alto = eta - _Z95 * dp, eta + _Z95 * dp

    pred = pd.DataFrame({
        "code_muni": grade["code_muni"].to_numpy(),
        "time_idx": grade["time_idx"].to_numpy().astype(int),
        "pred_mean": media_pred,
        "in_sample": em_amostra,
    })
    if include_ci:
        pred["pred_lower95"] = baixo
        pred["pred_upper95"] = alto

    amostras = None
    if return_samples:
        amostras = _amostras(fit, grade, covariaveis, familia, espacial,
                             temporal, modelo_t, em_amostra)

    return {
        "predictions": pred,
        "n_predicted": int(n_pred),
        "horizon": int(horizon),
        "samples": amostras,
    }


def _amostras(fit: dict, grade: pd.DataFrame, covariaveis: list[str],
              familia: str, espacial: Any, temporal: Any,
              modelo_t: str, em_amostra: np.ndarray) -> np.ndarray:
    """Posterior predictive draws, using the fit's own samples.

    R needs ``fit$inla_fit`` for this and its fitter never stores it, so
    ``return_samples=TRUE`` always errors there. Here the draws travel with
    the fit, and this path is exact where the summary arithmetic is not:
    it keeps the joint posterior instead of adding variances as if the
    terms were independent.
    """
    idata = fit.get("idata")
    if idata is None:
        raise ValueError(
            "return_samples=True needs the posterior draws, which live in "
            "fit['idata']. Refit with sus_mod_spacetime_bayes(), or use the "
            "summary-based prediction (return_samples=False)."
        )
    post = idata.posterior

    def pega(nome: str) -> np.ndarray:
        return post[nome].stack(amostra=("chain", "draw")).to_numpy()

    b0 = pega("intercept")
    n_draws = b0.shape[-1]
    eta = np.tile(b0, (len(grade), 1))
    if covariaveis and "beta" in post:
        beta = pega("beta")
        for i, c in enumerate(covariaveis):
            x = pd.to_numeric(grade[c], errors="coerce").to_numpy(dtype=float)
            eta = eta + np.outer(x, beta[i])

    if "phi" in post and espacial is not None:
        phi = pega("phi")
        pos = {m: i for i, m in enumerate(espacial["code_muni"].to_numpy())}
        linhas = [pos.get(m) for m in grade["code_muni"].to_numpy()]
        for k, i in enumerate(linhas):
            if i is not None:
                eta[k] += phi[i]

    if "psi" in post and temporal is not None:
        psi = pega("psi")
        t_max = psi.shape[0]
        tv = grade["time_idx"].to_numpy().astype(int)
        ordem = 2 if "2" in modelo_t else 1
        for k, t in enumerate(tv):
            if t <= t_max:
                eta[k] += psi[t - 1]
            else:
                passos = t - t_max
                deriva = (psi[-1] - psi[-2] if ordem == 2 and t_max >= 2
                          else 0.0)
                eta[k] += psi[-1] + deriva * passos

    if familia in ("poisson", "nbinomial"):
        return np.exp(eta).T
    if familia == "binomial":
        return stats.logistic.cdf(eta).T
    return eta.T
