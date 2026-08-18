"""Visualisations for pooled multi-city DLNM meta-analysis results.

Mirrors R: sus_mod_plot_pool.R

NOT YET IMPLEMENTED: consumes a ``climasus_pool`` object, produced by
``sus_mod_pool()``, which has no faithful Python port (``dlnm``/``mvmeta``
dependency) — there is no valid *x* this function could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_plot_pool(
    x: Any,
    type: Literal["overall", "forest", "spaghetti"] = "overall",
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:
    """Plot pooled multi-city DLNM meta-analysis results.

    R's ``sus_mod_plot_pool()`` renders three plot types for a
    ``climasus_pool`` object: the pooled exposure-response curve with CI
    ribbon (``"overall"``), a city-specific RR forest plot
    (``"forest"``), or city BLUP curves overlaid on the pooled curve
    (``"spaghetti"``). Its only valid input is the object returned by
    ``sus_mod_pool()``, which has no faithful Python port (see module
    docstring) — so this function can never receive a usable *x* and is
    stubbed alongside it.

    Args:
        x: A ``climasus_pool`` object.
        type: Plot type: ``"overall"`` (default), ``"forest"``, or
            ``"spaghetti"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``.
        interactive: Whether to return a ``plotly`` object instead of
            ``ggplot``. Defaults to ``False``.
        base_size: Base font size for plot themes. Defaults to ``12``.
        save_plot: File path to save the output, or ``None``.
        lang: Label language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to
            ``False``.

    Returns:
        A ``ggplot``/``plotly``-equivalent plot object (never reached —
        this function always raises).

    Raises:
        NotImplementedError: Always. Depends on the ``dlnm``/``mvmeta``
            -fitted ``climasus_pool`` object.
    """
    raise NotImplementedError(
        "sus_mod_plot_pool() has no faithful Python port: it consumes objects "
        "produced by sus_mod_pool(), which depends on R's dlnm/mvmeta packages "
        "— unavailable in Python — so no valid input can ever reach this "
        "function. See IDEIAS.md for details. Flagged for coordinator review "
        "before any approximate implementation is attempted."
    )
