"""Choropleth map of Kulldorff spatial scan cluster results.

Mirrors R: sus_mod_plot_spatial_scan.R

NOT YET IMPLEMENTED: consumes a ``climasus_spatial_scan`` object,
produced by ``sus_mod_spatial_scan()``, which has no faithful Python
port (``SpatialEpi`` dependency) — there is no valid *x* this function
could ever receive.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_plot_spatial_scan(
    x: Any,
    municipalities: Any,
    show_rr: bool = True,
    alpha: float = 0.05,
    title: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    **kwargs: Any,
) -> Any:
    """Plot a choropleth of Kulldorff spatial scan cluster results.

    R's ``sus_mod_plot_spatial_scan()`` renders a choropleth in which
    municipalities belonging to significant clusters are highlighted —
    filled by relative risk (``show_rr=True``) or by a categorical
    cluster label (``show_rr=False``). Its only valid input is the
    object returned by ``sus_mod_spatial_scan()``, which has no faithful
    Python port (see module docstring) — so this function can never
    receive a usable *x* and is stubbed alongside it.

    Args:
        x: A ``climasus_spatial_scan`` object.
        municipalities: A ``geopandas.GeoDataFrame`` with POLYGON/
            MULTIPOLYGON geometry and a ``code_muni`` column.
        show_rr: If ``True``, fill encodes relative risk on a diverging
            colour scale; if ``False``, fill encodes a categorical
            cluster label. Defaults to ``True``.
        alpha: Significance level used to flag clusters. Defaults to
            ``0.05``.
        title: Custom plot title, or ``None`` for a default.
        lang: Label language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        **kwargs: Currently unused; reserved for future arguments
            (mirrors R's ``...``).

    Returns:
        A plot object (never reached — this function always raises).

    Raises:
        NotImplementedError: Always. Depends on the ``SpatialEpi``
            -fitted ``climasus_spatial_scan`` object.
    """
    raise NotImplementedError(
        "sus_mod_plot_spatial_scan() has no faithful Python port: it consumes "
        "objects produced by sus_mod_spatial_scan(), which depends on R's "
        "SpatialEpi package — unavailable in Python — so no valid input can "
        "ever reach this function. See IDEIAS.md for details. Flagged for "
        "coordinator review before any approximate implementation is "
        "attempted."
    )
