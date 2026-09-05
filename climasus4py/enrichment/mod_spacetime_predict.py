"""Predictions from a fitted spatiotemporal Bayesian disease-mapping model.

Mirrors R: sus_mod_spacetime_predict.R

NOT YET IMPLEMENTED: consumes the ``climasus_spacetime_bayes`` fit object
produced by ``sus_mod_spacetime_bayes()``, which itself has no faithful
Python port (INLA dependency) — there is no valid *fit* this function
could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


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
    """Generate predictions from a fitted space-time Bayesian model.

    R's ``sus_mod_spacetime_predict()`` produces approximate predictions
    at new or future space-time points from a ``climasus_spacetime_bayes``
    fit, for disease-surveillance projections and counterfactual scenario
    analysis (via *covariates_new*), reusing the fit's posterior fixed
    effects, spatial random effects, and (extrapolated, for ``horizon >
    0``) temporal random effects. ``return_samples=True`` additionally
    draws raw posterior samples via ``INLA::inla.posterior.sample()``. Its
    only valid input is the object returned by ``sus_mod_spacetime_bayes()``,
    which has no faithful Python port (see module docstring) — so this
    function can never receive a usable *fit* and is stubbed alongside it.

    Args:
        fit: A ``climasus_spacetime_bayes`` fit object (from
            ``sus_mod_spacetime_bayes()``).
        newdata: Table with ``code_muni``/``time_idx`` (plus any needed
            covariate columns) for in-sample or out-of-sample prediction
            points, or ``None``.
        horizon: Number of future time steps to extrapolate, when *newdata*
            is ``None``. Must be ``0`` (default) if *newdata* is supplied.
        covariates_new: Named overrides of covariate values for
            counterfactual scenario analysis, or ``None``.
        include_ci: Whether to include 95% prediction intervals. Defaults
            to ``True``.
        return_samples: Whether to return raw posterior samples (requires
            the fit's stored INLA object). Defaults to ``False``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A dict mirroring R's ``climasus_spacetime_pred`` object (never
        reached — this function always raises).

    Raises:
        NotImplementedError: Always. Depends on the INLA-fitted
            ``climasus_spacetime_bayes`` object.
    """
    raise NotImplementedError(
        "sus_mod_spacetime_predict() has no faithful Python port: it "
        "consumes the fit object produced by sus_mod_spacetime_bayes(), "
        "which depends on INLA's Laplace-approximation Bayesian inference "
        "— unavailable in Python — so no valid input can ever reach this "
        "function. See IDEIAS.md for details. Flagged for coordinator "
        "review before any approximate implementation is attempted."
    )
