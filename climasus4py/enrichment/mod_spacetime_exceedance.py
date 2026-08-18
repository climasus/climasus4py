"""Posterior exceedance probabilities from a spatiotemporal Bayesian model fit.

Mirrors R: sus_mod_spacetime_exceedance.R

NOT YET IMPLEMENTED: consumes the ``climasus_spacetime_bayes`` fit object
produced by ``sus_mod_spacetime_bayes()``, which itself has no faithful
Python port (INLA dependency) — there is no valid *fit* this function
could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_spacetime_exceedance(
    fit: Any,
    thresholds: tuple[float, ...] = (1.0, 1.5, 2.0),
    aggregate_time: Literal["year", "month"] | None = None,
    municipalities: Any = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Compute posterior exceedance probabilities P(RR > threshold).

    R's ``sus_mod_spacetime_exceedance()`` takes a fitted
    ``climasus_spacetime_bayes`` object and computes, per municipality x
    time cell, the posterior probability that the relative risk exceeds
    one or more *thresholds*, via a log-normal approximation of the
    posterior RR (``sigma_log = (log(upper95) - log(lower95)) / (2*1.96)``).
    Its only valid input is the object returned by
    ``sus_mod_spacetime_bayes()``, which has no faithful Python port (see
    module docstring) — so this function can never receive a usable *fit*
    and is stubbed alongside it.

    Args:
        fit: A ``climasus_spacetime_bayes`` fit object (from
            ``sus_mod_spacetime_bayes()``).
        thresholds: Relative-risk thresholds to evaluate. Defaults to
            ``(1.0, 1.5, 2.0)``.
        aggregate_time: Optional temporal aggregation before computing
            exceedance: ``"year"``, ``"month"``, or ``None`` (default, no
            aggregation).
        municipalities: Optional ``geopandas.GeoDataFrame`` (the Python
            analog of R's ``sf``) with a ``code_muni`` column, joined onto
            the result.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A dict mirroring R's ``climasus_spacetime_exceedance`` object
        (never reached — this function always raises).

    Raises:
        NotImplementedError: Always. Depends on the INLA-fitted
            ``climasus_spacetime_bayes`` object.
    """
    raise NotImplementedError(
        "sus_mod_spacetime_exceedance() has no faithful Python port: it "
        "consumes the fit object produced by sus_mod_spacetime_bayes(), "
        "which depends on INLA's Laplace-approximation Bayesian inference "
        "— unavailable in Python — so no valid input can ever reach this "
        "function. See IDEIAS.md for details. Flagged for coordinator "
        "review before any approximate implementation is attempted."
    )
