"""Visualizations for space-time Bayesian disease-mapping results.

Mirrors R: sus_mod_plot_spacetime.R

NOT YET IMPLEMENTED: consumes ``climasus_spacetime_bayes`` /
``climasus_spacetime_exceedance`` objects, produced by
``sus_mod_spacetime_bayes()`` / ``sus_mod_spacetime_exceedance()``, neither
of which has a faithful Python port (INLA dependency) — there is no valid
*x* this function could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_plot_spacetime(
    x: Any,
    type: Literal["rr_map", "temporal", "interaction", "exceedance", "coef"] = "rr_map",
    municipalities: Any = None,
    time_point: int | None = None,
    time_range: tuple[int, int] | None = None,
    threshold: float = 1.0,
    facet_time: bool = False,
    palette: str = "RdYlBu",
    title: str | None = None,
    base_size: float = 11,
    interactive: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    **kwargs: Any,
) -> Any:
    """Plot space-time Bayesian disease-mapping results.

    R's ``sus_mod_plot_spacetime()`` renders five plot types for a
    ``climasus_spacetime_bayes`` or ``climasus_spacetime_exceedance``
    object: a relative-risk choropleth map (``"rr_map"``), a temporal
    trend chart (``"temporal"``), a space-time interaction heatmap
    (``"interaction"``), an exceedance-probability choropleth
    (``"exceedance"``), or a fixed-effects forest plot (``"coef"``);
    returns a ``ggplot`` object, or a ``plotly`` object when
    ``interactive=True``. Its only valid inputs are the objects returned by
    ``sus_mod_spacetime_bayes()`` / ``sus_mod_spacetime_exceedance()``,
    neither of which has a faithful Python port (see module docstring) —
    so this function can never receive a usable *x* and is stubbed
    alongside them.

    Args:
        x: A ``climasus_spacetime_bayes`` or ``climasus_spacetime_exceedance``
            object.
        type: Plot type: ``"rr_map"`` (default), ``"temporal"``,
            ``"interaction"``, ``"exceedance"``, or ``"coef"``.
        municipalities: ``geopandas.GeoDataFrame`` with a ``code_muni``
            column (the Python analog of R's ``sf``). Required for
            ``type in ("rr_map", "exceedance")``.
        time_point: Single time index to plot, for ``type="rr_map"``, or
            ``None``.
        time_range: ``(start, end)`` time-index range to plot, for
            ``type in ("temporal", "interaction")``, or ``None``.
        threshold: Relative-risk threshold for ``type="exceedance"``.
            Defaults to ``1.0``.
        facet_time: Whether to facet the RR map by time period instead of
            averaging across periods. Defaults to ``False``.
        palette: Diverging color-palette name (RColorBrewer-style).
            Defaults to ``"RdYlBu"``.
        title: Custom plot title, or ``None`` for a multilingual default.
        base_size: Base font size for plot themes. Defaults to ``11``.
        interactive: Whether to return a ``plotly`` object instead of
            ``ggplot``. Defaults to ``False``.
        lang: Label language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        **kwargs: Currently unused; reserved for future arguments (mirrors
            R's ``...``).

    Returns:
        A ``ggplot``/``plotly``-equivalent plot object (never reached —
        this function always raises).

    Raises:
        NotImplementedError: Always. Depends on the INLA-fitted
            ``climasus_spacetime_bayes``/``climasus_spacetime_exceedance``
            objects.
    """
    raise NotImplementedError(
        "sus_mod_plot_spacetime() has no faithful Python port: it consumes "
        "objects produced by sus_mod_spacetime_bayes() / "
        "sus_mod_spacetime_exceedance(), which depend on INLA's "
        "Laplace-approximation Bayesian inference — unavailable in Python "
        "— so no valid input can ever reach this function. See IDEIAS.md "
        "for details. Flagged for coordinator review before any "
        "approximate implementation is attempted."
    )
