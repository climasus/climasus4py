"""Bayesian spatiotemporal hierarchical disease-mapping model (BYM2/BYM/Besag + AR1/RW1/RW2, INLA).

Mirrors R: sus_mod_spacetime_bayes.R

NOT YET IMPLEMENTED: INLA's Laplace-approximation Bayesian inference (the
engine this model is fit with) has no Python equivalent.
"""

from __future__ import annotations

from typing import Any, Literal


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
) -> dict[str, Any]:
    """Fit a Bayesian spatiotemporal hierarchical model via INLA.

    R's ``sus_mod_spacetime_bayes()`` decomposes disease risk into a
    structured spatial effect (BYM2/BYM/Besag/IID), a structured temporal
    effect (RW1/RW2/AR1/IID), and an optional Knorr-Held (2000) space-time
    interaction term (types I-IV), all fit via ``INLA::inla()`` using
    penalized-complexity (PC) priors. There is no faithful Python port of
    this model — see module docstring and ``Raises`` below.

    Args:
        df: Table with one row per municipality x time period, containing
            *outcome*, *time_col*, *covariates* and (optionally) *offset*.
        outcome: Name of the outcome (count) column in *df*.
        W: Spatial weights object as returned by ``sus_mod_spatial_weights()``.
        time_col: Name of the date/time column in *df*. Defaults to ``"date"``.
        time_unit: Temporal aggregation unit: ``"year"`` (default), ``"month"``,
            ``"week"``, or ``"auto"``.
        covariates: Names of covariate columns in *df*, or ``None``.
        offset: Name of the offset (e.g. expected counts/population) column,
            or ``None``.
        family: Outcome distribution: ``"poisson"`` (default), ``"nbinomial"``,
            ``"binomial"``, or ``"gaussian"``.
        spatial_model: Spatial random-effect prior: ``"bym2"`` (default),
            ``"bym"``, ``"besag"``, or ``"iid"``.
        temporal_model: Temporal random-effect prior: ``"rw1"`` (default),
            ``"rw2"``, ``"ar1"``, or ``"iid_time"``.
        interaction_type: Knorr-Held space-time interaction type: ``"none"``
            (default), ``"I"``, ``"II"``, ``"III"``, or ``"IV"``.
        pc_prior_u: PC-prior scale parameter *u*. Defaults to ``0.5``.
        pc_prior_alpha: PC-prior tail probability *alpha*. Defaults to ``0.01``.
        compute_waic: Whether to compute WAIC. Defaults to ``True``.
        compute_cpo: Whether to compute CPO/PIT diagnostics. Defaults to
            ``False``.
        exceedance_threshold: Relative-risk threshold for posterior exceedance
            probabilities. Defaults to ``1.0``.
        n_samples: Number of posterior samples for derived quantities.
            Defaults to ``1000``.
        seed: Random seed. Defaults to ``42``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A dict mirroring R's ``climasus_spacetime_bayes`` object (never
        reached — this function always raises).

    Raises:
        NotImplementedError: Always. INLA has no Python equivalent.
    """
    raise NotImplementedError(
        "sus_mod_spacetime_bayes() has no faithful Python port: INLA's "
        "Laplace-approximation Bayesian inference has no Python equivalent "
        "(INLA's nested Laplace approximation for latent Gaussian models is "
        "not replicated by any Python MCMC/VI library — a PyMC reimplementation "
        "would use a different inference algorithm, not a port). See "
        "IDEIAS.md for details. Flagged for coordinator review before any "
        "approximate implementation is attempted."
    )
