"""Bayesian CAR/BYM disease mapping with climate covariates.

Mirrors R: sus_mod_spatial_bayes.R

R fits these with CARBayes's Gibbs/Metropolis samplers
(``S.CARbym``, ``S.CARleroux``, ``S.glm``) and, for ``model="bym2"``, with
INLA's Laplace approximation. This port uses **PyMC** (NUTS), decided on
2026-09-17.

What that costs, stated plainly: **exact numerical parity is not available
and never will be.** A different sampler exploring the same posterior gives
different draws, and INLA does not sample at all. The acceptance criterion
here is therefore statistical agreement within Monte Carlo error, measured
against frozen R output in ``tests/fixtures/spatial_bayes/`` — not equality.

The model specifications follow CARBayes:

- ``bym``: ``phi ~ ICAR(tau2)`` plus ``theta ~ N(0, sigma2)`` iid, and the
  reported random effect is their **sum**, matching R reading
  ``fit$samples[["psi"]]``.
- ``leroux``: ``phi ~ N(0, Q^-1)`` with precision
  ``Q = (rho (D - W) + (1 - rho) I) / tau2`` and ``rho ~ U(0, 1)``. PyMC's
  ``pm.CAR`` is a *different* parameterisation, so the precision is built
  explicitly.
- ``independent``: no spatial term at all — a plain GLM.
- ``bym2``: the reparameterisation of Riebler et al. (2016), which R fits
  with INLA rather than CARBayes —
  ``b = sigma (sqrt(1-rho) v + sqrt(rho/s) u)`` with ``s`` the ICAR's
  generalised geometric variance, so ``rho`` is an honest variance split.
  Two approximations are declared here: the PC prior on ``rho`` has no
  closed form in PyMC and is replaced by a Beta calibrated to the *same*
  probability statement (``P(rho < 0.5) = 2/3``), and WAIC is computed
  directly because ArviZ 1.x dropped it. Measured against INLA: 1.0000 on
  ``rr_mean``, 0.9998 on the random effect, DIC 339.27 against 340.09 and
  WAIC 331.90 against 332.77 — two different inference machineries, NUTS
  against a Laplace approximation, agreeing to three or four decimals.

Which sampler actually runs, and why it matters
-----------------------------------------------
``pm.sample`` is called with ``nuts_sampler=None``, PyMC's default, which
**prefers nutpie when it is installed** and falls back to PyMC's own NUTS
otherwise. That is deliberate: measured on the reference BYM fit on a
machine with no C compiler, nutpie took 28s against 157s — 5.6x, for no
code change — and agreed with R slightly better (0.9999 against 0.9994 on
the random effect). ``climasus4py[bayes]`` therefore installs it.

The consequence to be aware of: **the same ``seed`` gives different draws
under different samplers.** A fit is reproducible on a given install, not
across installs that differ in which samplers are present. Since exact
parity with R is already out of reach, this costs nothing against R — but
it does mean two Python runs are only comparable when the environment is.
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

__all__ = ["sus_mod_spatial_bayes"]

# CARBayes defaults, from its documentation
_PRIOR_VAR_BETA = 100_000.0
_PRIOR_SIGMA2 = (1.0, 0.01)
# INLA's PC priors for bym2, exactly as R spells them:
#   prec = list(prior='pc.prec', param=c(1, 0.01))  ->  P(sigma > 1) = 0.01
_PC_PREC_PARAM = (1.0, 0.01)
#   phi  = list(prior='pc',      param=c(0.5, 2/3)) ->  P(rho < 0.5) = 2/3
# The PC prior on the mixing parameter has no closed form in PyMC, so a Beta
# calibrated to the SAME probability statement is used: Beta(1, b) with
# 0.5**b = 1/3, i.e. b = log(1/3)/log(0.5) = 1.58496. The calibration point
# matches; the shape between the tails does not. Recorded as an approximation
# rather than a replication - and measured: it moves the covariate by 0.003.
_PC_PHI_BETA = (1.0, 1.5849625007211562)
# NUTS warm-up bounds; see the note where pm.sample is called
_TUNE_MIN, _TUNE_MAX = 500, 2000

#: Reproduce R's fixed-effects table verbatim, defect included (M88).
#:
#: R picks the fixed-effect rows out of CARBayes's ``summary.results`` by
#: regex. ``^(Intercept|...)`` treats the parentheses as a group, so the row
#: actually named ``(Intercept)`` never matches and the intercept is
#: dropped; and the exclusion pattern writes ``Sigma`` with a capital S, so
#: ``sigma2`` — a variance, not a fixed effect — survives into the table.
#: The two mistakes cancel in the row count, which is why it went unnoticed.
#: CARBayes's summary has no ``SD`` column either, so R's ``sd`` is always
#: missing.
#:
#: Default ``False``: this port builds the table itself, so it reports the
#: intercept, a real posterior ``sd``, and no variance among the fixed
#: effects. Set to ``True`` to get R's shape for a parity check.
R_FIXED_TABLE_DROPS_INTERCEPT: bool = False

_MESSAGES = {
    "pt": {
        "fitting": "[sus_mod_spatial_bayes] ajustando {model} ({family}), "
                   "{n} areas, {draws} amostras por cadeia",
        "done": "[sus_mod_spatial_bayes] pronto: DIC {dic:.2f}",
    },
    "en": {
        "fitting": "[sus_mod_spatial_bayes] fitting {model} ({family}), "
                   "{n} areas, {draws} draws per chain",
        "done": "[sus_mod_spatial_bayes] done: DIC {dic:.2f}",
    },
    "es": {
        "fitting": "[sus_mod_spatial_bayes] ajustando {model} ({family}), "
                   "{n} areas, {draws} muestras por cadena",
        "done": "[sus_mod_spatial_bayes] listo: DIC {dic:.2f}",
    },
}


def _msg(key: str, lang: str, **kw: Any) -> str:
    entry = _MESSAGES.get(lang, _MESSAGES["pt"])
    return entry[key].format(**kw)


def _weights_matrix(W: Any, n: int) -> np.ndarray:
    """Binary adjacency matrix, the way R derives ``W_mat``.

    R accepts the object from ``sus_mod_spatial_weights()``: it takes
    ``W$W`` when present, otherwise builds one from ``W$nb``, then
    binarises with ``(W_mat != 0) * 1``. Anything array-like is accepted
    here, plus a mapping with a ``"W"`` key, plus an object exposing ``.W``
    or a libpysal ``full()``.
    """
    mat = None
    if isinstance(W, dict) and "W" in W:
        mat = W["W"]
    elif hasattr(W, "W") and not isinstance(W, np.ndarray):
        mat = W.W
    elif hasattr(W, "full"):
        mat = W.full()[0]
    else:
        mat = W
    mat = np.asarray(mat, dtype=float)
    if mat.shape != (n, n):
        raise ValueError(
            f"W must be {n}x{n} to match df ({n} rows); got {mat.shape}. "
            "Pass the object returned by sus_mod_spatial_weights(), a dict "
            "with a 'W' key, or a square adjacency matrix."
        )
    mat = (mat != 0).astype(float)
    np.fill_diagonal(mat, 0.0)
    if not np.allclose(mat, mat.T):
        raise ValueError("W must be symmetric after binarisation.")
    if mat.sum() == 0:
        raise ValueError("W has no edges: every area is isolated.")
    return mat


def _icar_scale(W: np.ndarray) -> float:
    """BYM2's scaling factor: the ICAR's generalised geometric variance.

    The reparameterisation of Riebler et al. (2016) needs the intrinsic CAR
    scaled to unit generalised variance, so that ``rho`` really splits the
    variance between the structured and the unstructured part. The factor
    is ``exp(mean(log(diag(Q^-))))`` with ``Q = D - W`` and ``Q^-`` its
    generalised inverse under the sum-to-zero constraint — the same recipe
    INLA applies internally when ``scale.model`` is in force for bym2.
    """
    Q = np.diag(W.sum(axis=1)) - W
    n = Q.shape[0]
    # Q is singular by construction; perturb, invert, then project onto the
    # sum-to-zero subspace
    Q_p = Q + np.eye(n) * (np.max(np.diag(Q)) * np.sqrt(np.finfo(float).eps))
    Q_inv = np.linalg.inv(Q_p)
    um = np.ones((n, 1))
    Q_inv = Q_inv - (Q_inv @ um @ um.T @ Q_inv) / (um.T @ Q_inv @ um).item()
    return float(np.exp(np.mean(np.log(np.diag(Q_inv)))))


def _pc_prec_rate(u: float, alpha: float) -> float:
    """Rate of the PC prior on a standard deviation: ``P(sigma > u) = alpha``.

    INLA's ``pc.prec`` puts an exponential prior on ``sigma`` with rate
    ``-log(alpha) / u``. With R's ``param=c(1, 0.01)`` that is 4.605.
    """
    return -np.log(alpha) / u


def _pointwise_loglik(y: np.ndarray, mu: np.ndarray,
                      family: str) -> np.ndarray:
    """``(draws, n)`` log-likelihood, one entry per draw and observation.

    Needed for WAIC, which is a per-observation quantity: summing first
    would throw away exactly the variance the penalty term measures.
    """
    from scipy import stats

    if family == "poisson":
        return np.asarray(stats.poisson.logpmf(y, mu))
    if family == "binomial":
        return np.asarray(
            stats.bernoulli.logpmf(y, np.clip(mu, 1e-12, 1 - 1e-12)))
    resid = y - mu
    var = np.var(resid, axis=-1, keepdims=True)
    return np.asarray(stats.norm.logpdf(y, mu, np.sqrt(np.maximum(var, 1e-12))))


def _deviance(y: np.ndarray, mu: np.ndarray, family: str) -> np.ndarray:
    """-2 log-likelihood, summed over areas, for one or many draws.

    ``mu`` may be ``(n,)`` or ``(draws, n)``; the result is scalar or
    ``(draws,)``. Constant terms are kept so the value is comparable with
    CARBayes's DIC rather than merely proportional to it.
    """
    from scipy import stats

    if family == "poisson":
        ll = stats.poisson.logpmf(y, mu)
    elif family == "binomial":
        ll = stats.bernoulli.logpmf(y, np.clip(mu, 1e-12, 1 - 1e-12))
    else:
        resid = y - mu
        var = np.var(resid, axis=-1, keepdims=True)
        ll = stats.norm.logpdf(y, mu, np.sqrt(np.maximum(var, 1e-12)))
    return -2.0 * np.asarray(ll).sum(axis=-1)


def sus_mod_spatial_bayes(
    df: Any,
    outcome: str,
    W: Any,
    covariates: list[str] | None = None,
    offset: str | None = None,
    family: Literal["poisson", "binomial", "gaussian"] = "poisson",
    model: Literal["bym", "leroux", "independent", "bym2"] = "bym",
    n_iter: int = 10000,
    burnin: int = 2000,
    thin: int = 10,
    prior_tau2: tuple[float, float] = (1.0, 0.01),
    seed: int = 42,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
    chains: int = 2,
) -> dict[str, Any]:
    """Fit a Bayesian CAR/BYM spatial disease-mapping model with PyMC.

    Smoothed relative risk is ``fitted / expected``, taken from the
    posterior draws exactly as R does: ``rr_samples`` is the matrix of
    fitted draws divided column-wise by the expected counts, and the
    reported mean and 95% interval are its mean and 2.5/97.5 percentiles.

    **Without an offset the relative risk is not relative to 1.** R falls
    back to ``expected = mean(fitted)``, so the ratio is against the average
    fitted value. Replicated.

    Args:
        df: Table with one row per municipality and a ``code_muni``
            column, containing *outcome*, *covariates* and (optionally)
            *offset*. Sorted by ``code_muni`` before fitting, as R does.
        outcome: Name of the outcome (count) column in *df*.
        W: Spatial weights — the object from
            ``sus_mod_spatial_weights()``, a dict with a ``"W"`` key, or a
            square adjacency matrix. Binarised, as R does.
        covariates: Names of covariate columns in *df*, or ``None``.
        offset: Name of the offset (e.g. expected counts) column, or
            ``None``. Enters the linear predictor as ``log(offset)``.
        family: ``"poisson"`` (default), ``"binomial"`` or ``"gaussian"``.
        model: ``"bym"`` (default), ``"leroux"`` or ``"independent"``.
            ``"bym2"`` is not yet available here — see *Raises*.
        n_iter: Total iterations, counted as R does: the kept draws per
            chain are ``(n_iter - burnin) // thin``. Defaults to ``10000``.
        burnin: Warm-up iterations. Defaults to ``2000``.
        thin: Thinning interval. Defaults to ``10``.
        prior_tau2: Inverse-gamma shape/scale for the CAR variance,
            matching CARBayes's ``prior.tau2``. Defaults to ``(1.0, 0.01)``.
        seed: Random seed. Defaults to ``42``. It makes this implementation
            reproducible **on a given install**: it cannot make it agree
            with R's stream, and it does not carry across installs that
            differ in which NUTS sampler is available - see the module
            docstring.
        lang: Message language.
        verbose: Whether to print progress.
        chains: Number of chains. **Not in R's signature** — CARBayes runs
            a single chain. Defaults to ``2`` because two chains are what
            make convergence diagnosable at all; pass ``1`` for R's shape.

    Returns:
        A dict mirroring R's ``climasus_spatial_bayes`` object:

        - ``"fixed"``: ``DataFrame`` with ``term``, ``mean``, ``sd``,
          ``lower95``, ``upper95``. Includes the intercept and excludes
          variance parameters, unlike R — see
          :data:`R_FIXED_TABLE_DROPS_INTERCEPT`.
        - ``"random"``: ``DataFrame`` with ``code_muni``, ``phi_mean``,
          ``phi_sd``. For ``model="bym"`` these describe the **combined**
          random effect, replicating R, whose column name says ``phi`` but
          whose values come from ``psi``. Identically zero for
          ``model="independent"``, which has no random effect.
        - ``"rr"``: ``DataFrame`` with ``code_muni``, ``rr_mean``,
          ``rr_lower95``, ``rr_upper95``.
        - ``"fitted"``: ``ndarray`` of posterior mean fitted values.
        - ``"dic"``, ``"model"``, ``"family"``, ``"n_iter_effective"``.
        - ``"idata"``: the ArviZ ``InferenceData``. Not in R's object; kept
          because discarding the draws would make the fit unauditable.

        R's ``$call`` slot has no meaningful Python analog and is not
        reproduced, matching the other ported model functions.

    Raises:
        ImportError: If PyMC is not installed.
        NotImplementedError: If ``model="bym2"``, which in R is fitted by
            INLA rather than CARBayes and is not ported yet.
        ValueError: If *df* lacks ``code_muni``, *outcome*, a named
            covariate or the *offset* column; if *W* does not match *df*;
            or if the iteration counts leave no draws.
    """
    try:
        import pymc as pm
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(
            "sus_mod_spatial_bayes() needs PyMC. Install it with "
            "`pip install climasus4py[bayes]`."
        ) from exc

    if model not in ("bym", "leroux", "independent", "bym2"):
        raise ValueError(
            f"model must be 'bym', 'leroux', 'independent' or 'bym2'; "
            f"got {model!r}."
        )
    if family not in ("poisson", "binomial", "gaussian"):
        raise ValueError(
            f"family must be 'poisson', 'binomial' or 'gaussian'; "
            f"got {family!r}."
        )

    tabela = pd.DataFrame(df).copy()
    if "code_muni" not in tabela.columns:
        raise ValueError("df must have a 'code_muni' column.")
    faltando = [c for c in [outcome, *(covariates or []),
                            *( [offset] if offset else [] )]
                if c not in tabela.columns]
    if faltando:
        raise ValueError(f"df is missing column(s): {faltando}")

    # R sorts by code_muni before fitting, and every output table is in
    # that order, so W must be in it too.
    tabela = tabela.sort_values("code_muni").reset_index(drop=True)
    n_areas = len(tabela)

    faltantes = tabela[outcome].isna().sum()
    if faltantes:
        if verbose:
            warnings.warn(
                f"{faltantes} row(s) dropped: '{outcome}' is missing there.",
                UserWarning, stacklevel=2,
            )
        manter = tabela[outcome].notna().to_numpy()
        tabela = tabela[manter].reset_index(drop=True)
    else:
        manter = np.ones(n_areas, dtype=bool)

    W_mat = _weights_matrix(W, n_areas)
    if not manter.all():
        W_mat = W_mat[np.ix_(manter, manter)]
    n = len(tabela)

    draws = (int(n_iter) - int(burnin)) // int(thin)
    if draws < 1:
        raise ValueError(
            f"(n_iter - burnin) // thin = {draws}; no draws would be kept. "
            f"Got n_iter={n_iter}, burnin={burnin}, thin={thin}."
        )

    y = tabela[outcome].to_numpy()
    y = y.astype(int) if family in ("poisson", "binomial") else y.astype(float)
    X = (tabela[list(covariates)].to_numpy(dtype=float)
         if covariates else np.empty((n, 0)))
    log_E = (np.log(tabela[offset].to_numpy(dtype=float))
             if offset else np.zeros(n))

    if verbose:
        print(_msg("fitting", lang, model=model, family=family, n=n,
                   draws=draws))

    grau = W_mat.sum(axis=1)
    sd_beta = float(np.sqrt(_PRIOR_VAR_BETA))

    with pm.Model() as modelo:
        beta0 = pm.Normal("intercept", 0.0, sd_beta)
        eta = beta0 + log_E
        if X.shape[1]:
            beta = pm.Normal("beta", 0.0, sd_beta, shape=X.shape[1])
            eta = eta + pm.math.dot(X, beta)

        if model == "bym":
            tau2 = pm.InverseGamma("tau2", alpha=prior_tau2[0],
                                   beta=prior_tau2[1])
            sigma2 = pm.InverseGamma("sigma2", alpha=_PRIOR_SIGMA2[0],
                                     beta=_PRIOR_SIGMA2[1])
            # NON-CENTRED, and the difference is not cosmetic. Putting the
            # prior directly on ICAR's `sigma` makes a funnel that NUTS
            # climbs badly: measured against R on the reference fit, the
            # centred version gave tau2 = 103 (against 0.053 here), the
            # covariate 0.159 (against R's 0.134) and a correlation of only
            # 0.89 on the random effect, while reporting ZERO divergences -
            # so no convergence check would have caught it. Non-centred
            # brings the same quantities to 0.9995 and 0.130.
            phi_raw = pm.ICAR("phi_raw", W=W_mat)
            phi = pm.Deterministic("phi", phi_raw * pm.math.sqrt(tau2))
            theta_raw = pm.Normal("theta_raw", 0.0, 1.0, shape=n)
            theta = pm.Deterministic("theta",
                                     theta_raw * pm.math.sqrt(sigma2))
            # R reads fit$samples[["psi"]] here: the COMBINED effect
            re = pm.Deterministic("re", phi + theta)
        elif model == "leroux":
            tau2 = pm.InverseGamma("tau2", alpha=prior_tau2[0],
                                   beta=prior_tau2[1])
            rho = pm.Uniform("rho", 0.0, 1.0)
            # Leroux precision, built explicitly: pm.CAR uses another
            # parameterisation and would not be the same model.
            Q = (rho * (np.diag(grau) - W_mat)
                 + (1.0 - rho) * np.eye(n)) / tau2
            re = pm.MvNormal("re", mu=np.zeros(n), tau=Q)
        elif model == "bym2":
            # Riebler et al. (2016): one variance and one mixing parameter,
            #   b = sigma * ( sqrt(1 - rho) v + sqrt(rho / s) u )
            # with v iid standard normal, u the ICAR, and s the scaling
            # factor that makes rho an honest variance split.
            escala = _icar_scale(W_mat)
            sigma = pm.Exponential(
                "sigma", _pc_prec_rate(*_PC_PREC_PARAM))
            rho = pm.Beta("rho", *_PC_PHI_BETA)
            u = pm.ICAR("u", W=W_mat)
            v = pm.Normal("v", 0.0, 1.0, shape=n)
            re = pm.Deterministic(
                "re",
                sigma * (pm.math.sqrt(1.0 - rho) * v
                         + pm.math.sqrt(rho / escala) * u),
            )
        else:  # independent: a plain GLM, no random effect
            re = pm.Deterministic("re", pm.math.zeros(n))

        eta = eta + re
        if family == "poisson":
            mu = pm.Deterministic("mu", pm.math.exp(eta))
            pm.Poisson("y_obs", mu=mu, observed=y)
        elif family == "binomial":
            mu = pm.Deterministic("mu", pm.math.sigmoid(eta))
            pm.Bernoulli("y_obs", p=mu, observed=y)
        else:
            sigma_y = pm.HalfNormal("sigma_y", 10.0)
            mu = pm.Deterministic("mu", eta)
            pm.Normal("y_obs", mu=mu, sigma=sigma_y, observed=y)

        # `burnin` steers the warm-up, but NOT literally. R's burnin is
        # CARBayes discarding cheap Gibbs sweeps; NUTS spends its warm-up
        # adapting a step size and mass matrix, which is expensive and
        # converges in far fewer iterations. Passing burnin=10000 straight
        # through made a 42-area fit run for over ten minutes with no
        # statistical gain. Clamped to [500, 2000] — a deliberate
        # divergence, and the reason `tune` is reported back in the result.
        tune = int(min(max(int(burnin), _TUNE_MIN), _TUNE_MAX))
        idata = pm.sample(
            draws=draws, tune=tune, chains=chains,
            cores=1, random_seed=seed, progressbar=False,
            compute_convergence_checks=False,
        )

    post = idata.posterior
    mu_s = post["mu"].stack(amostra=("chain", "draw")).to_numpy()   # (n, S)
    re_s = post["re"].stack(amostra=("chain", "draw")).to_numpy()
    n_kept = mu_s.shape[1]

    fitted = mu_s.mean(axis=1)
    # R: expected = df[[offset]] when given, else rep(mean(fitted), n)
    esperado = (tabela[offset].to_numpy(dtype=float) if offset
                else np.full(n, fitted.mean()))
    rr_s = mu_s / esperado[:, None]

    codigos = tabela["code_muni"].to_numpy()
    rr_df = pd.DataFrame({
        "code_muni": codigos,
        "rr_mean": rr_s.mean(axis=1),
        "rr_lower95": np.quantile(rr_s, 0.025, axis=1),
        "rr_upper95": np.quantile(rr_s, 0.975, axis=1),
    })
    random_df = pd.DataFrame({
        "code_muni": codigos,
        "phi_mean": re_s.mean(axis=1),
        "phi_sd": re_s.std(axis=1, ddof=1),
    })

    termos = ["(Intercept)", *(covariates or [])]
    empilhado = [post["intercept"].stack(amostra=("chain", "draw")).to_numpy()]
    if covariates:
        b = post["beta"].stack(amostra=("chain", "draw")).to_numpy()
        empilhado.extend(b[i] for i in range(b.shape[0]))
    amostras_fixas = np.vstack(empilhado)

    # M88 is a defect of the CARBayes path only. R's bym2 branch builds
    # its table from INLA's summary.fixed, which carries (Intercept) and a
    # real sd - so applying the switch there would diverge FROM R, not
    # towards it.
    if R_FIXED_TABLE_DROPS_INTERCEPT and model != "bym2":
        linhas, valores = [], []
        for nome, col in zip(termos[1:], amostras_fixas[1:]):
            linhas.append(nome)
            valores.append(col)
        if model in ("bym", "leroux") and "sigma2" in post:
            linhas.append("sigma2")
            valores.append(
                post["sigma2"].stack(amostra=("chain", "draw")).to_numpy())
        amostras_fixas = np.vstack(valores) if valores else np.empty((0, n_kept))
        termos = linhas
        sd_col = np.full(len(termos), np.nan)   # CARBayes has no SD column
    else:
        sd_col = amostras_fixas.std(axis=1, ddof=1)

    fixed_df = pd.DataFrame({
        "term": termos,
        "mean": amostras_fixas.mean(axis=1) if len(termos) else [],
        "sd": sd_col,
        "lower95": (np.quantile(amostras_fixas, 0.025, axis=1)
                    if len(termos) else []),
        "upper95": (np.quantile(amostras_fixas, 0.975, axis=1)
                    if len(termos) else []),
    })

    # Spiegelhalter DIC: D(theta_bar) + 2 pD, with pD = D_bar - D(theta_bar)
    d_barra = float(_deviance(y, mu_s.T, family).mean())
    d_media = float(_deviance(y, fitted, family))
    dic = d_media + 2.0 * (d_barra - d_media)

    # R's `dic` slot is a bare scalar for the CARBayes models and a named
    # vector c(DIC=, WAIC=) for bym2 - the same field, two types. Rather
    # than propagate that, `dic` is always a float here and `waic` is its
    # own key, so nothing is lost and the type never depends on `model`.
    # WAIC is computed here rather than through ArviZ because ArviZ 1.x
    # dropped `waic` entirely in favour of PSIS-LOO. R reports WAIC, so WAIC
    # is what belongs in the object; the formula is short and removes a
    # dependency on which ArviZ generation is installed.
    #   lppd   = sum_i log mean_s p(y_i | theta_s)
    #   p_waic = sum_i var_s log p(y_i | theta_s)
    #   WAIC   = -2 (lppd - p_waic)
    ll = _pointwise_loglik(y, mu_s.T, family)
    maximo = ll.max(axis=0)
    lppd = float(np.sum(maximo + np.log(np.mean(np.exp(ll - maximo), axis=0))))
    p_waic = float(np.sum(np.var(ll, axis=0, ddof=1)))
    waic = -2.0 * (lppd - p_waic)

    if verbose:
        print(_msg("done", lang, dic=dic))

    return {
        "fixed": fixed_df,
        "random": random_df,
        "rr": rr_df,
        "fitted": fitted,
        "dic": dic,
        "model": model,
        "family": family,
        "waic": waic,
        "n_iter_effective": int(n_kept),
        "tune": tune,
        "idata": idata,
    }
