"""Bayesian spatiotemporal disease mapping: space + time + interaction.

Mirrors R: sus_mod_spacetime_bayes.R

R fits this with INLA's nested Laplace approximation. This port uses
**PyMC** (NUTS), decided on 2026-09-17. Exact numerical parity is not
available: INLA does not sample at all, so there is no stream to match. The
criterion is statistical agreement against the frozen INLA output in
``tests/fixtures/spacetime/``.

The decomposition, following R term by term::

    log(RR_it) = beta0 + x_it' beta + phi_i + psi_t + gamma_it

with ``phi`` spatial (bym2/bym/besag/iid), ``psi`` temporal
(rw1/rw2/ar1/iid_time) and ``gamma`` the optional Knorr-Held (2000)
interaction (types I-IV). Expected counts enter as ``E``, not as a
covariate, so the linear predictor **is** ``log(RR)`` — which is why R
reads the relative risk straight off it.

Every structured term is a singular Gaussian prior; they are all built
through one mechanism in ``_latent_gaussian``, which drops the null space
(making the identifiability constraint exact rather than penalised) and is
non-centred by construction.

Three findings meet here, and the port takes a different decision on each
--------------------------------------------------------------------------
**M89 — R does not run on Windows.** ``sus_mod_spacetime_bayes`` writes the
neighbourhood graph to a temp file and interpolates *the path* into a
formula string that then goes through ``as.formula()``, i.e. through R code
parse. On Windows the backslashes become escapes: either a hard error
(``'\\U' used without hex digits``) or, worse, silent corruption — a
19-character path parsing to 17 characters with a TAB and a BEL in it. Not
applicable here; recorded because it is why the reference fixtures had to
be generated with a one-line-patched copy of the R function.

**M90 — ``time_idx`` is a 1-based index.** This function is the *producer*
of that column, and it writes the index rather than the original temporal
label, which is what makes ``sus_mod_spacetime_exceedance`` read
``"0001"`` as a year. Andrey decided on 2026-09-15 to replicate R, so the
column is written the same way here, and the docstring says so where a
caller will see it.

**M91 — R's interaction is over-parameterised, not just mislabelled.**
The original finding described a labelling defect. It is worse than that,
and the cause is one line::

    df[["area.time"]] <- (area_idx_v - 1L) * n_times + time_idx_v

``area.time`` is the **cell** index, 1..336. For type I, used as
``f(area.time, model='iid')``, that is exactly right — an unstructured
interaction lives on cells. But types II, III and IV use it as
``f(area.time, model=..., group=.time_idx_st, ...)``, and in INLA the main
index of a grouped term must range over the **graph's nodes** — the 42
areas — with ``group`` supplying the second dimension. Passing the cell
index there multiplies the latent dimension instead of factorising it:
336 x 8 = **2688 latent interaction values for 336 observations**, eight
per data point.

The reference fit shows the consequence. ``gamma_sd`` runs from 6.34 to
7.22 while ``|gamma_mean|`` averages 0.139 — a posterior spread some fifty
times the effect size, i.e. a term the data cannot identify. On the log
scale ``sd = 6.6`` admits risk multipliers around 700. Type III is worse
again: ``model='besag'`` applies a spatial neighbourhood to an index that
is not spatial, so the graph structure is meaningless there.

Only then does the labelling defect land on top: ``pmin(ai_seq, n_areas)``
clamps every index past 42 onto the last municipality, putting **2360 of
the 2688 rows** on one of them.

This port uses the area index with a Kronecker structure, so the
interaction has one latent value per cell — 336, not 2688. That means the
type II-IV fits here **do not and should not agree** with the reference:
the reference is an over-parameterised model, and matching it would mean
reproducing the over-parameterisation. The no-interaction fit, where no
such defect exists, agrees closely (see the function docstring).
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

from ._latent_gaussian import (
    basis,
    scale_structure,
    structure_icar,
    structure_rw,
)

__all__ = ["sus_mod_spacetime_bayes"]

# INLA's pc.prec on the temporal and interaction terms is hard-coded in R as
# param=c(0.5, 0.01); only the spatial term takes it from the arguments.
_PC_TIME = (0.5, 0.01)
_PC_INTERACTION = (0.5, 0.01)
# bym2's mixing parameter here is pc(0.5, 0.5) - note this differs from
# sus_mod_spatial_bayes, which uses pc(0.5, 2/3). Beta(1, b) calibrated to
# the same statement P(rho < 0.5) = 0.5 gives b = 1.
_PC_PHI_BETA = (1.0, 1.0)
# R's `bym` branch uses loggamma(1, 0.01) on both precisions, which is the
# gamma(shape=1, rate=0.01) prior on precision, i.e. inverse-gamma(1, 0.01)
# on variance.
_PRIOR_BYM_IG = (1.0, 0.01)
_TUNE_MIN, _TUNE_MAX = 500, 2000

#: Reproduce R's mislabelled interaction table (M91).
#:
#: R builds it as::
#:
#:     n_inter <- nrow(inter_sum)
#:     ai_seq  <- ((seq_len(n_inter) - 1) %/% n_times) + 1
#:     code_muni = area_keys[pmin(ai_seq, n_areas)]
#:
#: Two mistakes compound. ``area.time`` is already a cell-level index, so
#: grouping it by time again multiplies the rows: 2688 entries for 336
#: cells. Then ``pmin(..., n_areas)`` clamps every index past 42 onto the
#: last municipality, so **2360 of the 2688 rows are attributed to one
#: municipality**, each with a different value.
#:
#: Default ``False``: the table here has one row per cell, labelled with the
#: cell's own municipality and period. Replicating would ship correct
#: numbers under wrong labels, which is the case this port corrects rather
#: than mirrors — the same call as M92, M94 and M107. Set to ``True`` to
#: reproduce R's shape for a parity check.
R_INTERACTION_TABLE_CLAMPS_AREAS: bool = False

_MESSAGES = {
    "pt": {
        "fit": "[sus_mod_spacetime_bayes] {n_areas} areas x {n_times} "
               "periodos, espacial={sp} temporal={tp} interacao={ia}",
        "done": "[sus_mod_spacetime_bayes] pronto: WAIC {waic:.2f}",
    },
    "en": {
        "fit": "[sus_mod_spacetime_bayes] {n_areas} areas x {n_times} "
               "periods, spatial={sp} temporal={tp} interaction={ia}",
        "done": "[sus_mod_spacetime_bayes] done: WAIC {waic:.2f}",
    },
    "es": {
        "fit": "[sus_mod_spacetime_bayes] {n_areas} areas x {n_times} "
               "periodos, espacial={sp} temporal={tp} interaccion={ia}",
        "done": "[sus_mod_spacetime_bayes] listo: WAIC {waic:.2f}",
    },
}


def _msg(key: str, lang: str, **kw: Any) -> str:
    return _MESSAGES.get(lang, _MESSAGES["pt"])[key].format(**kw)


def _pc_rate(u: float, alpha: float) -> float:
    """Exponential rate of INLA's ``pc.prec``: ``P(sigma > u) = alpha``."""
    return -float(np.log(alpha)) / float(u)


def _time_index(serie: pd.Series, unit: str) -> tuple[np.ndarray, np.ndarray]:
    """Map the time column to a 1-based index, returning ``(idx, labels)``.

    R derives the index by ranking the distinct period values, and writes
    only the index into the output — see M90 in the module docstring. The
    original labels are returned here as well so callers are not forced to
    re-derive what was thrown away.
    """
    if unit == "auto":
        unit = "year"
    valores = serie
    if not pd.api.types.is_numeric_dtype(valores):
        datas = pd.to_datetime(valores, errors="coerce")
        if datas.notna().any():
            if unit == "year":
                valores = datas.dt.year
            elif unit == "month":
                valores = datas.dt.to_period("M").astype(str)
            else:
                valores = datas.dt.to_period("W").astype(str)
    rotulos = np.array(sorted(pd.unique(valores.dropna())))
    mapa = {v: i + 1 for i, v in enumerate(rotulos)}
    return valores.map(mapa).to_numpy(), rotulos


def _weights_matrix(W: Any, n: int) -> np.ndarray:
    """Binary, symmetric adjacency over the AREAS (not the cells)."""
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
            f"W must be {n}x{n} to match the {n} distinct areas in df; got "
            f"{mat.shape}."
        )
    mat = (mat != 0).astype(float)
    np.fill_diagonal(mat, 0.0)
    if not np.allclose(mat, mat.T):
        raise ValueError("W must be symmetric after binarisation.")
    if mat.sum() == 0:
        raise ValueError("W has no edges: every area is isolated.")
    return mat


def _structured(pm: Any, nome: str, R: np.ndarray, sigma: Any) -> Any:
    """A singular Gaussian prior with precision ``R``, non-centred.

    ``sigma * V (z / sqrt(lambda))`` over the non-null eigenpairs. The null
    space is dropped, so the constraint is exact.
    """
    V, lam = basis(scale_structure(R))
    z = pm.Normal(f"{nome}_z", 0.0, 1.0, shape=V.shape[1])
    return sigma * (V @ (z / np.sqrt(lam)))


def sus_mod_spacetime_bayes(
    df: Any,
    outcome: str,
    W: Any,
    time_col: str = "date",
    time_unit: Literal["year", "month", "week", "auto"] = "year",
    covariates: list[str] | None = None,
    offset: str | None = None,
    family: Literal["poisson", "nbinomial", "binomial", "gaussian"] = "poisson",
    spatial_model: Literal["bym2", "bym", "besag", "iid"] = "bym2",
    temporal_model: Literal["rw1", "rw2", "ar1", "iid_time"] = "rw1",
    interaction_type: Literal["none", "I", "II", "III", "IV"] = "none",
    pc_prior_u: float = 0.5,
    pc_prior_alpha: float = 0.01,
    compute_waic: bool = True,
    compute_cpo: bool = False,
    exceedance_threshold: float = 1.0,
    n_samples: int = 1000,
    seed: int = 42,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
    chains: int = 2,
) -> dict[str, Any]:
    """Fit a Bayesian spatiotemporal disease-mapping model with PyMC.

    Relative risk comes off the linear predictor, as R takes it from
    INLA's: with expected counts entering as ``E``, the predictor is
    ``log(RR)`` directly, so ``rr_mean = exp(lp)`` and the interval is the
    exponentiated interval of ``lp``.

    ``p_exceed`` follows R, and that was not the first plan. INLA has only
    the marginal mean and sd of the predictor, so R computes
    ``pnorm(log(threshold), lp_mean, lp_sd, lower.tail = FALSE)`` — a normal
    approximation on the log scale. Having the draws in hand, counting the
    probability directly looked strictly better, so that is what this
    function did first. Measured against the INLA reference over the 43
    cells where the probability is below 0.01 — which is exactly where an
    exceedance probability is read — the counted version was **worse**: mean
    error 0.00065 against 0.00031, and **15 of the 43 came out as exact
    zero**, because a count over 2000 draws cannot resolve below 1/2000 and
    reports absence instead of smallness. So ``p_exceed`` is R's normal
    approximation and ``p_exceed_empirical`` carries the counted one beside
    it. The approximation is not a concession here; it is the better
    estimator in the tail.

    Args:
        df: One row per area x period, with ``code_muni``, *outcome*,
            *time_col*, *covariates* and optionally *offset*. Sorted by
            ``(code_muni, period)`` before fitting.
        outcome: Outcome (count) column.
        W: Spatial weights over the **distinct areas** — the object from
            ``sus_mod_spatial_weights()``, a dict with ``"W"``, or a square
            adjacency matrix, ordered by sorted ``code_muni``.
        time_col: Time column. Defaults to ``"date"``.
        time_unit: ``"year"`` (default), ``"month"``, ``"week"`` or
            ``"auto"`` (treated as ``"year"``, as in R).
        covariates: Covariate columns, or ``None``.
        offset: Expected-counts column, or ``None``. Enters as ``E``; with
            no offset the predictor is a log rate against 1.
        family: ``"poisson"`` (default), ``"nbinomial"``, ``"binomial"`` or
            ``"gaussian"``.
        spatial_model: ``"bym2"`` (default), ``"bym"``, ``"besag"`` or
            ``"iid"``.
        temporal_model: ``"rw1"`` (default), ``"rw2"``, ``"ar1"`` or
            ``"iid_time"``.
        interaction_type: ``"none"`` (default) or Knorr-Held ``"I"``,
            ``"II"``, ``"III"``, ``"IV"``. Type I is unstructured; II is
            structured in time within each area; III in space within each
            period; IV in both.
        pc_prior_u: PC-prior scale for the **spatial** term's precision.
            Defaults to ``0.5``. R hard-codes ``(0.5, 0.01)`` for the
            temporal and interaction terms regardless of this argument;
            replicated.
        pc_prior_alpha: PC-prior tail probability for the spatial term.
        compute_waic: Whether to compute WAIC. Defaults to ``True``.
        compute_cpo: Whether to compute a leave-one-out diagnostic. Where
            R asks INLA for CPO, this reports PSIS-LOO via ArviZ — a
            different estimator of the same quantity, so the numbers are
            not comparable to R's. Defaults to ``False``.
        exceedance_threshold: RR threshold for exceedance. Defaults to
            ``1.0``.
        n_samples: Kept posterior draws per chain. Defaults to ``1000``.
        seed: Random seed. Reproducible on a given install; see
            ``sus_mod_spatial_bayes`` on why it does not carry across
            installs.
        lang: Message language.
        verbose: Whether to print progress.
        chains: Chains. Not in R's signature (INLA does not sample);
            defaults to ``2`` so convergence is diagnosable.

    Returns:
        A dict mirroring R's ``climasus_spacetime_bayes`` object:

        - ``"fixed"``: ``term``, ``mean``, ``sd``, ``lower95``, ``upper95``,
          ``mode``. Includes the intercept — R's INLA path does too, so
          M88 does not apply here.
        - ``"rr"``: ``code_muni``, ``time_idx``, ``rr_mean``,
          ``rr_lower95``, ``rr_upper95``, ``p_exceed``, plus
          ``p_exceed_empirical`` and ``time_label``, which R does not
          carry.
        - ``"spatial_re"``: ``code_muni``, ``phi_mean``, ``phi_sd``.
        - ``"temporal_re"``: ``time_idx``, ``psi_mean``, ``psi_sd``.
        - ``"interaction_re"``: ``code_muni``, ``time_idx``,
          ``gamma_mean``, ``gamma_sd`` — **one row per cell**, unlike R;
          see :data:`R_INTERACTION_TABLE_CLAMPS_AREAS`. ``None`` when
          ``interaction_type="none"``.
        - ``"fitted"``, ``"waic"``, ``"dic"``, ``"loo"``, ``"model_spec"``,
          ``"n_areas"``, ``"n_times"``, ``"time_labels"``, ``"idata"``.
        - ``"data_last_obs"``: the last observed period's rows. R's
          ``sus_mod_spacetime_predict`` reads a slot by this name and R's
          fitter never writes it, so forecasting with a covariate returns
          all ``NA`` there (M110). Written here so it works.

        **``time_idx`` is a 1-based index, not a year** (M90). R writes it
        that way and this replicates it, which is why
        ``sus_mod_spacetime_exceedance`` labels periods ``"0001"``. The
        original labels are in ``"time_labels"`` and in the ``rr`` table's
        ``time_label`` column — added here precisely so the information is
        not lost, without changing what ``time_idx`` means.

    Raises:
        ImportError: If PyMC is not installed.
        ValueError: On a missing column, a mismatched *W*, an unknown
            option, or a panel that is not one row per area-period.
    """
    try:
        import pymc as pm
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "sus_mod_spacetime_bayes() needs PyMC. Install it with "
            "`pip install climasus4py[bayes]`."
        ) from exc

    for nome, valor, validos in (
        ("family", family, ("poisson", "nbinomial", "binomial", "gaussian")),
        ("spatial_model", spatial_model, ("bym2", "bym", "besag", "iid")),
        ("temporal_model", temporal_model,
         ("rw1", "rw2", "ar1", "iid_time")),
        ("interaction_type", interaction_type,
         ("none", "I", "II", "III", "IV")),
        ("time_unit", time_unit, ("year", "month", "week", "auto")),
    ):
        if valor not in validos:
            raise ValueError(f"{nome} must be one of {validos}; got {valor!r}.")

    tabela = pd.DataFrame(df).copy()
    exigidas = ["code_muni", outcome, time_col, *(covariates or [])]
    if offset:
        exigidas.append(offset)
    faltando = [c for c in exigidas if c not in tabela.columns]
    if faltando:
        raise ValueError(f"df is missing column(s): {faltando}")

    idx_t, rotulos_t = _time_index(tabela[time_col], time_unit)
    tabela["__time_idx"] = idx_t
    if tabela["__time_idx"].isna().any():
        raise ValueError(f"{time_col!r} has values that could not be ordered.")
    tabela = (tabela.sort_values(["code_muni", "__time_idx"])
                    .reset_index(drop=True))

    areas = np.array(sorted(pd.unique(tabela["code_muni"])))
    n_areas, n_times = len(areas), len(rotulos_t)
    if len(tabela) != n_areas * n_times:
        raise ValueError(
            f"df must hold one row per area-period: {n_areas} areas x "
            f"{n_times} periods = {n_areas * n_times}, but df has "
            f"{len(tabela)} rows. Fill or drop the gaps first."
        )
    pos_area = pd.Series(np.arange(n_areas), index=areas)
    ia = pos_area[tabela["code_muni"]].to_numpy()
    it = tabela["__time_idx"].to_numpy().astype(int) - 1

    W_mat = _weights_matrix(W, n_areas)
    n = len(tabela)
    y = tabela[outcome].to_numpy()
    y = y.astype(int) if family in ("poisson", "nbinomial", "binomial") \
        else y.astype(float)
    X = (tabela[list(covariates)].to_numpy(dtype=float)
         if covariates else np.empty((n, 0)))
    E = (tabela[offset].to_numpy(dtype=float) if offset else np.ones(n))

    if verbose:
        print(_msg("fit", lang, n_areas=n_areas, n_times=n_times,
                   sp=spatial_model, tp=temporal_model, ia=interaction_type))

    R_esp = structure_icar(W_mat)
    with pm.Model() as modelo:
        beta0 = pm.Normal("intercept", 0.0, 5.0)
        eta = beta0
        if X.shape[1]:
            beta = pm.Normal("beta", 0.0, 5.0, shape=X.shape[1])
            eta = eta + pm.math.dot(X, beta)

        # --- spatial -----------------------------------------------------
        if spatial_model == "bym2":
            sigma_s = pm.Exponential(
                "sigma_spatial", _pc_rate(pc_prior_u, pc_prior_alpha))
            rho = pm.Beta("rho_spatial", *_PC_PHI_BETA)
            u_esp = _structured(pm, "u_spatial", R_esp, 1.0)
            v_esp = pm.Normal("v_spatial", 0.0, 1.0, shape=n_areas)
            phi = pm.Deterministic(
                "phi",
                sigma_s * (pm.math.sqrt(rho) * u_esp
                           + pm.math.sqrt(1.0 - rho) * v_esp))
        elif spatial_model == "bym":
            tau_u = pm.InverseGamma("tau2_spatial", *_PRIOR_BYM_IG)
            tau_v = pm.InverseGamma("sigma2_spatial", *_PRIOR_BYM_IG)
            u_esp = _structured(pm, "u_spatial", R_esp, pm.math.sqrt(tau_u))
            v_esp = pm.Normal("v_spatial", 0.0, pm.math.sqrt(tau_v),
                              shape=n_areas)
            phi = pm.Deterministic("phi", u_esp + v_esp)
        elif spatial_model == "besag":
            sigma_s = pm.Exponential(
                "sigma_spatial", _pc_rate(pc_prior_u, pc_prior_alpha))
            phi = pm.Deterministic(
                "phi", _structured(pm, "u_spatial", R_esp, sigma_s))
        else:
            sigma_s = pm.Exponential(
                "sigma_spatial", _pc_rate(pc_prior_u, pc_prior_alpha))
            phi = pm.Deterministic(
                "phi", sigma_s * pm.Normal("u_spatial", 0.0, 1.0,
                                           shape=n_areas))

        # --- temporal ----------------------------------------------------
        sigma_t = pm.Exponential("sigma_time", _pc_rate(*_PC_TIME))
        if temporal_model in ("rw1", "rw2"):
            ordem = 1 if temporal_model == "rw1" else 2
            psi = pm.Deterministic(
                "psi",
                _structured(pm, "u_time", structure_rw(n_times, ordem),
                            sigma_t))
        elif temporal_model == "ar1":
            # AR1's precision is non-singular and depends on rho, so it is
            # not a fixed structure matrix: PyMC's AR walk is used instead.
            rho_t = pm.Uniform("rho_time", -1.0, 1.0)
            psi = pm.Deterministic(
                "psi",
                pm.AR("u_time", rho=rho_t, sigma=sigma_t, shape=n_times,
                      init_dist=pm.Normal.dist(0.0, sigma_t)))
        else:
            psi = pm.Deterministic(
                "psi",
                sigma_t * pm.Normal("u_time", 0.0, 1.0, shape=n_times))

        eta = eta + phi[ia] + psi[it]

        # --- Knorr-Held interaction --------------------------------------
        gamma = None
        if interaction_type != "none":
            sigma_g = pm.Exponential("sigma_interaction",
                                     _pc_rate(*_PC_INTERACTION))
            if interaction_type == "I":
                gamma = pm.Deterministic(
                    "gamma",
                    sigma_g * pm.Normal("u_interaction", 0.0, 1.0, shape=n))
            else:
                # Kronecker over (area, time), in the row order the panel
                # was sorted into: area-major, time-minor.
                R_t1 = structure_rw(n_times, 1)
                if interaction_type == "II":
                    R_ia = np.kron(np.eye(n_areas), R_t1)
                elif interaction_type == "III":
                    R_ia = np.kron(R_esp, np.eye(n_times))
                else:
                    R_ia = np.kron(R_esp, R_t1)
                gamma = pm.Deterministic(
                    "gamma", _structured(pm, "u_interaction", R_ia, sigma_g))
            eta = eta + gamma

        lp = pm.Deterministic("lp", eta)
        if family in ("poisson", "nbinomial"):
            mu = pm.Deterministic("mu", E * pm.math.exp(lp))
            if family == "poisson":
                pm.Poisson("y_obs", mu=mu, observed=y)
            else:
                alfa = pm.HalfNormal("alpha_nb", 10.0)
                pm.NegativeBinomial("y_obs", mu=mu, alpha=alfa, observed=y)
        elif family == "binomial":
            pm.Bernoulli("y_obs", p=pm.math.sigmoid(lp), observed=y)
        else:
            sigma_y = pm.HalfNormal("sigma_y", 10.0)
            pm.Normal("y_obs", mu=lp, sigma=sigma_y, observed=y)

        idata = pm.sample(
            draws=int(n_samples),
            tune=int(min(max(int(n_samples), _TUNE_MIN), _TUNE_MAX)),
            chains=chains, cores=1, random_seed=seed, progressbar=False,
            compute_convergence_checks=False,
        )

    post = idata.posterior

    def empilha(nome: str) -> np.ndarray:
        return post[nome].stack(amostra=("chain", "draw")).to_numpy()

    lp_s = empilha("lp")
    phi_s = empilha("phi")
    psi_s = empilha("psi")

    lp_mean = lp_s.mean(axis=1)
    lp_sd = lp_s.std(axis=1, ddof=1)
    limiar = float(np.log(exceedance_threshold))

    from scipy import stats

    rr_df = pd.DataFrame({
        "code_muni": tabela["code_muni"].to_numpy(),
        "time_idx": tabela["__time_idx"].to_numpy().astype(int),
        "time_label": rotulos_t[it],
        "rr_mean": np.exp(lp_mean),
        "rr_lower95": np.exp(np.quantile(lp_s, 0.025, axis=1)),
        "rr_upper95": np.exp(np.quantile(lp_s, 0.975, axis=1)),
        # R's normal approximation on the log scale, and it is the DEFAULT
        # here because measurement says it is the better estimator where it
        # matters. See the note on p_exceed in the docstring.
        "p_exceed": stats.norm.sf(limiar, loc=lp_mean, scale=lp_sd),
        # counted from the draws, kept so the difference stays visible
        "p_exceed_empirical": (lp_s > limiar).mean(axis=1),
    })

    fixos = [("(Intercept)", empilha("intercept"))]
    if covariates:
        b = empilha("beta")
        fixos += [(c, b[i]) for i, c in enumerate(covariates)]
    fixed_df = pd.DataFrame({
        "term": [t for t, _ in fixos],
        "mean": [v.mean() for _, v in fixos],
        "sd": [v.std(ddof=1) for _, v in fixos],
        "lower95": [np.quantile(v, 0.025) for _, v in fixos],
        "upper95": [np.quantile(v, 0.975) for _, v in fixos],
        # INLA reports the marginal's mode; with draws the closest honest
        # analogue is the posterior median, and it is labelled as such in
        # the docstring rather than pretending to be a mode.
        "mode": [np.median(v) for _, v in fixos],
    })

    spatial_df = pd.DataFrame({
        "code_muni": areas,
        "phi_mean": phi_s.mean(axis=1),
        "phi_sd": phi_s.std(axis=1, ddof=1),
    })
    temporal_df = pd.DataFrame({
        "time_idx": np.arange(1, n_times + 1),
        "psi_mean": psi_s.mean(axis=1),
        "psi_sd": psi_s.std(axis=1, ddof=1),
    })

    interaction_df = None
    if interaction_type != "none":
        g = empilha("gamma")
        interaction_df = pd.DataFrame({
            "code_muni": tabela["code_muni"].to_numpy(),
            "time_idx": tabela["__time_idx"].to_numpy().astype(int),
            "gamma_mean": g.mean(axis=1),
            "gamma_sd": g.std(axis=1, ddof=1),
        })
        if R_INTERACTION_TABLE_CLAMPS_AREAS:
            interaction_df = _replica_tabela_r(interaction_df, areas,
                                               n_areas, n_times)

    mu_s = (empilha("mu") if family in ("poisson", "nbinomial")
            else lp_s)
    fitted = mu_s.mean(axis=1)

    waic_out = dic_out = loo_out = None
    ll = _pointwise_loglik(pm, idata, modelo)
    if ll is not None:
        maximo = ll.max(axis=0)
        lppd = float(np.sum(maximo + np.log(np.mean(np.exp(ll - maximo),
                                                    axis=0))))
        p_eff = float(np.sum(np.var(ll, axis=0, ddof=1)))
        if compute_waic:
            waic_out = {"waic": -2.0 * (lppd - p_eff), "p_eff": p_eff}
        dev = -2.0 * ll.sum(axis=1)
        d_barra = float(dev.mean())
        d_media = float(-2.0 * _loglik_at(y, fitted, family, mu_s).sum())
        dic_out = {"dic": d_media + 2.0 * (d_barra - d_media),
                   "p_d": d_barra - d_media}
        if compute_cpo:
            loo_out = _loo(idata)

    if verbose and waic_out:
        print(_msg("done", lang, waic=waic_out["waic"]))

    return {
        "fixed": fixed_df,
        "rr": rr_df,
        "spatial_re": spatial_df,
        "temporal_re": temporal_df,
        "interaction_re": interaction_df,
        "fitted": fitted,
        "waic": waic_out,
        "dic": dic_out,
        "loo": loo_out,
        "model_spec": {
            "spatial_model": spatial_model,
            "temporal_model": temporal_model,
            "interaction_type": interaction_type,
            "family": family,
        },
        "n_areas": n_areas,
        "n_times": n_times,
        "time_labels": rotulos_t,
        # The rows of the last observed period, kept so
        # sus_mod_spacetime_predict can carry covariates forward for
        # horizon > 0. R's predict reads a slot with this exact name and
        # R's fitter never writes it, which is why forecasting with a
        # covariate returns all NA there (M110).
        "data_last_obs": tabela[tabela["__time_idx"] == n_times]
                         .drop(columns="__time_idx").reset_index(drop=True),
        "idata": idata,
    }


def _replica_tabela_r(cells: pd.DataFrame, areas: np.ndarray,
                      n_areas: int, n_times: int) -> pd.DataFrame:
    """R's mislabelled interaction table, for a parity check (M91).

    Rebuilds the two mistakes: ``area.time`` grouped by time again, giving
    ``n_cells * n_times`` rows, and then ``pmin(area_index, n_areas)``
    clamping the overflow onto the last municipality.
    """
    n_inter = len(cells) * n_times
    seq = np.arange(n_inter)
    ai = seq // n_times
    ti = seq % n_times + 1
    valores = np.resize(cells["gamma_mean"].to_numpy(), n_inter)
    desvios = np.resize(cells["gamma_sd"].to_numpy(), n_inter)
    return pd.DataFrame({
        "code_muni": areas[np.minimum(ai, n_areas - 1)],
        "time_idx": ti,
        "gamma_mean": valores,
        "gamma_sd": desvios,
    })


def _pointwise_loglik(pm: Any, idata: Any, modelo: Any) -> np.ndarray | None:
    """``(draws, n)`` pointwise log-likelihood, or ``None`` if unavailable."""
    try:
        pm.compute_log_likelihood(idata, model=modelo, progressbar=False)
        ll = idata.log_likelihood["y_obs"]
        return ll.stack(amostra=("chain", "draw")).to_numpy().T
    except Exception as exc:  # pragma: no cover
        warnings.warn(
            f"pointwise log-likelihood unavailable ({type(exc).__name__}: "
            f"{exc}); WAIC, DIC and LOO are omitted from the result.",
            UserWarning, stacklevel=3,
        )
        return None


def _loglik_at(y: np.ndarray, fitted: np.ndarray, family: str,
               mu_s: np.ndarray) -> np.ndarray:
    """Log-likelihood at the posterior-mean fit, for DIC's plug-in term."""
    from scipy import stats

    if family in ("poisson", "nbinomial"):
        return np.asarray(stats.poisson.logpmf(y, np.maximum(fitted, 1e-12)))
    if family == "binomial":
        p = np.clip(fitted, 1e-12, 1 - 1e-12)
        return np.asarray(stats.bernoulli.logpmf(y, p))
    resid = y - fitted
    sd = max(float(np.std(resid)), 1e-12)
    return np.asarray(stats.norm.logpdf(y, fitted, sd))


def _loo(idata: Any) -> dict[str, float] | None:
    """PSIS-LOO via ArviZ — NOT comparable to INLA's CPO.

    R asks INLA for CPO, a cross-validated predictive density computed
    inside the Laplace approximation. PSIS-LOO estimates the same quantity
    by importance sampling from the posterior. Same target, different
    estimator, so the numbers should not be compared across the two.
    """
    try:
        import arviz as az

        r = az.loo(idata)
        return {"elpd_loo": float(r.elpd_loo), "p_loo": float(r.p_loo)}
    except Exception as exc:  # pragma: no cover
        warnings.warn(
            f"PSIS-LOO unavailable ({type(exc).__name__}: {exc}).",
            UserWarning, stacklevel=3,
        )
        return None
