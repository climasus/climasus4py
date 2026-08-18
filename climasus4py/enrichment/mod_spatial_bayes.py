"""Bayesian CAR/BYM disease mapping with climate covariates (CARBayes / INLA BYM2).

Mirrors R: sus_mod_spatial_bayes.R

NOT YET IMPLEMENTED: CARBayes's CAR-prior MCMC sampler (and, for
``model="bym2"``, INLA's Laplace approximation) have no Python equivalent.
"""

from __future__ import annotations

from typing import Any, Literal


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
) -> dict[str, Any]:
    """Fit a Bayesian CAR/BYM spatial disease-mapping model.

    R's ``sus_mod_spatial_bayes()`` fits a Bayesian hierarchical spatial
    disease-mapping model with a conditional autoregressive (CAR) random
    effect. For ``model in ("bym", "leroux", "independent")`` it uses
    CARBayes's MCMC samplers (``CARBayes::S.CARbym()``,
    ``CARBayes::S.CARleroux()``, ``CARBayes::S.glm()``); for
    ``model="bym2"`` it uses the reparameterized BYM2 prior fit via
    ``INLA::inla()``. Smoothed relative risk = fitted / expected. Neither
    engine has a faithful Python port — see module docstring and
    ``Raises`` below.

    Args:
        df: Table with one row per municipality, containing *outcome*,
            *covariates* and (optionally) *offset*.
        outcome: Name of the outcome (count) column in *df*.
        W: Spatial weights object as returned by ``sus_mod_spatial_weights()``.
        covariates: Names of covariate columns in *df*, or ``None``.
        offset: Name of the offset (e.g. expected counts) column, or ``None``.
        family: Outcome distribution: ``"poisson"`` (default), ``"binomial"``,
            or ``"gaussian"``.
        model: Spatial prior specification: ``"bym"`` (default), ``"leroux"``,
            ``"independent"`` (all via CARBayes MCMC), or ``"bym2"`` (via INLA).
        n_iter: Total MCMC iterations (CARBayes models only). Defaults to
            ``10000``.
        burnin: MCMC burn-in iterations (CARBayes models only). Defaults to
            ``2000``.
        thin: MCMC thinning interval (CARBayes models only). Defaults to
            ``10``.
        prior_tau2: Inverse-gamma prior shape/scale for the CAR variance
            (CARBayes models only). Defaults to ``(1.0, 0.01)``.
        seed: Random seed. Defaults to ``42``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A dict mirroring R's ``climasus_spatial_bayes`` object (never
        reached — this function always raises).

    Raises:
        NotImplementedError: Always. CARBayes (and, for ``model="bym2"``,
            INLA) have no Python equivalent.
    """
    raise NotImplementedError(
        "sus_mod_spatial_bayes() has no faithful Python port: CARBayes's "
        "CAR-prior MCMC sampler has no Python equivalent (its Gibbs/Metropolis "
        "samplers for the CAR/leroux/BYM priors are not replicated by any "
        "Python library; model=\"bym2\" additionally depends on INLA's "
        "Laplace-approximation inference, likewise unavailable in Python). "
        "See IDEIAS.md for details. Flagged for coordinator review before "
        "any approximate implementation is attempted."
    )
