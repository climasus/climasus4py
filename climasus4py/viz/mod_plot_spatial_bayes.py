"""Visualizations for Bayesian spatial disease-mapping results.

Mirrors R: sus_mod_plot_spatial_bayes.R

NOT YET IMPLEMENTED: consumes the ``climasus_spatial_bayes`` object
produced by ``sus_mod_spatial_bayes()``, which itself has no faithful
Python port (CARBayes / INLA dependency) — there is no valid *x* this
function could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_plot_spatial_bayes(
    x: Any,
    municipalities: Any = None,
    type: Literal["rr", "uncertainty", "coef", "both"] = "rr",
    title: str | None = None,
    base_size: float = 12,
    lang: Literal["pt", "en", "es"] = "pt",
    **kwargs: Any,
) -> Any:
    """Plot Bayesian spatial disease-mapping results.

    R's ``sus_mod_plot_spatial_bayes()`` renders a relative-risk
    choropleth map (``"rr"``), a credible-interval-width uncertainty
    choropleth (``"uncertainty"``), a fixed-effects forest plot
    (``"coef"``), or both maps side by side (``"both"``) for a
    ``climasus_spatial_bayes`` object; returns a ``ggplot`` object (or a
    ``patchwork``-equivalent panel / dict of two plots for ``"both"``).
    Its only valid input is the object returned by
    ``sus_mod_spatial_bayes()``, which has no faithful Python port (see
    module docstring) — so this function can never receive a usable *x*
    and is stubbed alongside it.

    Args:
        x: A ``climasus_spatial_bayes`` object (from
            ``sus_mod_spatial_bayes()``).
        municipalities: ``geopandas.GeoDataFrame`` with a ``code_muni``
            column (the Python analog of R's ``sf``). Required for
            ``type in ("rr", "uncertainty", "both")``.
        type: Plot type: ``"rr"`` (default), ``"uncertainty"``, ``"coef"``,
            or ``"both"``.
        title: Custom plot title, or ``None`` for a multilingual default.
        base_size: Base font size for plot themes. Defaults to ``12``.
        lang: Label language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        **kwargs: Currently unused; reserved for future arguments (mirrors
            R's ``...``).

    Returns:
        A ``ggplot``-equivalent plot object, or a dict with ``"rr"``/
        ``"uncertainty"`` keys for ``type="both"`` (never reached — this
        function always raises).

    Raises:
        NotImplementedError: Always. Depends on the CARBayes/INLA-fitted
            ``climasus_spatial_bayes`` object.
    """
    raise NotImplementedError(
        "sus_mod_plot_spatial_bayes() has no faithful Python port: it "
        "consumes the fit object produced by sus_mod_spatial_bayes(), "
        "which depends on CARBayes's CAR-prior MCMC sampler (and, for "
        "model=\"bym2\", INLA's Laplace-approximation inference) — neither "
        "available in Python — so no valid input can ever reach this "
        "function. See IDEIAS.md for details. Flagged for coordinator "
        "review before any approximate implementation is attempted."
    )
