"""Two-panel LISA / Moran's I visualisation — ggplot-style via plotnine.

Mirrors R: sus_mod_plot_spatial_moran (sus_mod_plot_spatial_moran.R)

Consumes the ``climasus_spatial_moran``-shaped output of
``sus_mod_spatial_moran()`` (a ``dict`` in the Python port, mirroring the
``dict``-based ``climasus_weights`` analog already used by
``sus_mod_spatial_weights``). Produces:

- ``type="map"`` — a choropleth of the LISA quadrant classification
  (HH/LL/HL/LH/NS), merged onto municipality polygons.
- ``type="scatter"`` — a Moran scatter plot (standardised value vs.
  spatial lag), coloured by LISA quadrant.
- ``type="both"`` — both panels. R falls back to a named list when
  **patchwork** is not installed; there is no maintained plotnine
  equivalent of patchwork's ``wrap_plots()``, so the Python port always
  returns the named-list form (see the "both" branch below).

Requires the optional [plot] extra (and, for ``type="map"``/``"both"``,
[geo]):
    pip install climasus4py[plot]
    pip install geopandas

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_mod_plot_spatial_moran(moran_res, type="scatter", lang="en")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings
# ---------------------------------------------------------------------------

_I18N = {
    "pt": {
        "title_map": "Mapa de Clusters LISA",
        "subtitle_map": "Classificação por quadrante de autocorrelação local",
        "title_scatter": "Diagrama de Dispersão de Moran",
        "x_scatter": "Valor padronizado (z)",
        "y_scatter": "Defasagem espacial (Wz)",
        "legend": "Quadrante LISA",
        "quad_HH": "HH (ponto quente)",
        "quad_LL": "LL (ponto frio)",
        "quad_HL": "HL (outlier espacial)",
        "quad_LH": "LH (outlier espacial)",
        "quad_NS": "Não significativo",
        "warn_unmatched": (
            "{n} município(s) em x['local'] sem correspondência em "
            "municipalities. Serão omitidos do mapa."
        ),
        "warn_lang": "Idioma '{lang}' não suportado. Usando 'pt'.",
    },
    "en": {
        "title_map": "LISA Cluster Map",
        "subtitle_map": "Local spatial autocorrelation quadrant classification",
        "title_scatter": "Moran Scatter Plot",
        "x_scatter": "Standardised value (z)",
        "y_scatter": "Spatial lag (Wz)",
        "legend": "LISA Quadrant",
        "quad_HH": "HH (hotspot)",
        "quad_LL": "LL (coldspot)",
        "quad_HL": "HL (spatial outlier)",
        "quad_LH": "LH (spatial outlier)",
        "quad_NS": "Not significant",
        "warn_unmatched": (
            "{n} municipality/ies in x['local'] with no match in "
            "municipalities. They will be omitted from the map."
        ),
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
    },
    "es": {
        "title_map": "Mapa de Clústeres LISA",
        "subtitle_map": "Clasificación por cuadrante de autocorrelación local",
        "title_scatter": "Diagrama de Dispersión de Moran",
        "x_scatter": "Valor estandarizado (z)",
        "y_scatter": "Rezago espacial (Wz)",
        "legend": "Cuadrante LISA",
        "quad_HH": "HH (punto caliente)",
        "quad_LL": "LL (punto frío)",
        "quad_HL": "HL (valor atípico espacial)",
        "quad_LH": "LH (valor atípico espacial)",
        "quad_NS": "No significativo",
        "warn_unmatched": (
            "{n} municipio(s) en x['local'] sin coincidencia en "
            "municipalities. Se omitirán del mapa."
        ),
        "warn_lang": "Idioma '{lang}' no admitido. Usando 'pt'.",
    },
}

# Fixed conventional LISA palette. R uses base-R colour names
# (red3/blue3/darkorange/steelblue/grey80, na.value="grey90"); these are
# their exact hex equivalents so the rendered colours match R, since
# plotnine's colour backend (mizani) does not recognise R's numbered
# colour names ("red3", "blue3", ...).
_QUAD_COLORS = {
    "HH": "#CD0000",  # red3
    "LL": "#0000CD",  # blue3
    "HL": "#FF8C00",  # darkorange
    "LH": "#4682B4",  # steelblue
    "NS": "#CCCCCC",  # grey80
}
_NA_COLOR = "#E5E5E5"  # grey90
_QUAD_LEVELS = ("HH", "LL", "HL", "LH", "NS")


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_spatial_moran requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _resolve_lang(lang: str) -> str:
    if lang not in _I18N:
        warnings.warn(
            _I18N["pt"]["warn_lang"].format(lang=lang), UserWarning, stacklevel=3
        )
        return "pt"
    return lang


def sus_mod_plot_spatial_moran(
    x: dict[str, Any],
    municipalities: Any = None,
    type: Literal["map", "scatter", "both"] = "map",
    alpha: float = 0.05,
    title: str | None = None,
    lang: str = "pt",
    **kwargs: Any,
) -> Any:
    """Two-panel LISA visualisation: cluster map and Moran scatter plot.

    Produces a choropleth LISA cluster map and/or a Moran scatter plot
    from the ``climasus_spatial_moran``-shaped ``dict`` returned by
    ``sus_mod_spatial_moran()``.

    Panel types (``type``):
        - ``"map"``: choropleth map coloured by LISA quadrant
          (HH/LL/HL/LH/NS). Requires *municipalities*.
        - ``"scatter"``: Moran scatter plot of standardised values vs.
          spatial lag.
        - ``"both"``: both panels. R combines them side-by-side via
          **patchwork** when installed, else falls back to a named
          ``list(map=..., scatter=...)``. There is no maintained
          plotnine equivalent of ``patchwork::wrap_plots()``, so the
          Python port always returns the named-``dict`` fallback form
          (a documented library gap — see module docstring).

    Colour scheme:
        Quadrant colours follow the conventional LISA palette: HH =
        red3, LL = blue3, HL = darkorange, LH = steelblue, NS = grey80
        (hex equivalents are used internally — see module source —
        because plotnine's colour backend does not resolve R's base
        colour names).

    Args:
        x: A ``dict`` shaped like R's ``climasus_spatial_moran`` object
            (the output of ``sus_mod_spatial_moran()``), with at least
            keys ``"global"`` (mapping/DataFrame with keys/columns
            ``"I"`` and ``"p_simulated"``) and ``"local"`` (a
            ``pandas.DataFrame`` with columns ``"code_muni"``, ``"Ii"``,
            and ``"quadrant"`` — plus, optionally, ``"z_std"``/``"lag_z"``
            if already computed; they are derived from ``"Ii"`` and
            ``"quadrant"`` when absent, exactly mirroring R's
            approximation — see ``_moran_scatter_panel``).
        municipalities: A ``geopandas.GeoDataFrame`` with at least the
            column ``code_muni`` (7-digit IBGE code) and polygon
            geometry. Required when *type* is ``"map"`` or ``"both"``.
            Ignored for ``type="scatter"``.
        type: Which panel(s) to produce. One of ``"map"`` (default),
            ``"scatter"``, or ``"both"``.
        alpha: Significance level, documented in R as used for the
            subtitle annotation. **Preserved R quirk:** neither R's
            ``.moran_map_panel``/``.moran_scatter_panel`` nor this port
            actually reference *alpha* anywhere in the plot body — it
            is accepted for signature parity but has no effect.
        title: Custom title overriding the default translated title.
            Applies to the first / only panel only; when *type* is
            ``"both"``, the scatter panel always uses its default
            translated title (mirrors R, which hard-codes ``NULL`` for
            the scatter panel's title in the "both" branch).
        lang: Output language: ``"pt"`` (default), ``"en"``, or
            ``"es"``. Falls back to ``"pt"`` with a warning if invalid.
        **kwargs: Accepted for signature parity with R's ``...``
            (passed to ``ggplot2::ggsave()`` there); ignored here.

    Returns:
        ``type="map"`` or ``type="scatter"``: a ``plotnine.ggplot``
        object. ``type="both"``: a ``dict`` with keys ``"map"`` and
        ``"scatter"``, each a ``plotnine.ggplot`` object.

    Raises:
        ImportError: If ``plotnine`` (or, for ``type in ("map", "both")``,
            ``geopandas``) is not installed.
        TypeError: If *x* is not a ``dict`` with the expected
            ``"global"``/``"local"`` keys.
        ValueError: If *type* requires *municipalities* and it is
            ``None``, or *municipalities* lacks a ``code_muni`` column.

    Examples::

        import climasus4py as cs

        # moran_res = cs.sus_mod_spatial_moran(df, "deaths", W)
        # shp = geopandas.read_file("municipios.gpkg")

        cs.sus_mod_plot_spatial_moran(moran_res, municipalities=shp, type="both")
        cs.sus_mod_plot_spatial_moran(
            moran_res, municipalities=shp, type="map", lang="en"
        )
        cs.sus_mod_plot_spatial_moran(moran_res, type="scatter", lang="es")
    """
    _require_plotnine()

    lang = _resolve_lang(lang)

    if not isinstance(x, dict) or "global" not in x or "local" not in x:
        raise TypeError(
            "x must be a dict shaped like the output of sus_mod_spatial_moran() "
            "(a mapping with 'global' and 'local' keys)."
        )

    if type in ("map", "both"):
        try:
            import geopandas as gpd  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "sus_mod_plot_spatial_moran requires geopandas to merge LISA "
                "results with municipality geometry. Install with: "
                "pip install geopandas"
            ) from exc
        if municipalities is None:
            raise ValueError(
                "municipalities must be provided when type is 'map' or 'both'."
            )
        if "code_muni" not in municipalities.columns:
            raise ValueError(
                "municipalities must contain column 'code_muni' for the "
                "spatial merge."
            )

    if type == "map":
        return _moran_map_panel(x, municipalities, alpha, title, lang)

    if type == "scatter":
        return _moran_scatter_panel(x, alpha, title, lang)

    # type == "both" -- R falls back to a named list when patchwork is
    # unavailable; plotnine has no maintained patchwork equivalent, so the
    # Python port always uses that fallback shape (documented gap).
    p_map = _moran_map_panel(x, municipalities, alpha, title, lang)
    p_scatter = _moran_scatter_panel(x, alpha, None, lang)
    return {"map": p_map, "scatter": p_scatter}


# ---------------------------------------------------------------------------
# Internal panel builders
# ---------------------------------------------------------------------------


def _moran_map_panel(
    x: dict[str, Any],
    municipalities: Any,
    alpha: float,  # noqa: ARG001 -- accepted for parity, unused (preserved R quirk)
    title: str | None,
    lang: str,
) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_map,
        ggplot,
        labs,
        scale_fill_manual,
        theme,
        theme_void,
    )

    strings = _I18N[lang]
    local_df = x["local"]

    municipalities = municipalities.copy()
    municipalities["code_muni"] = municipalities["code_muni"].astype(str)
    local_df = local_df.copy()
    local_df["code_muni"] = local_df["code_muni"].astype(str)

    n_unmatched = int((~local_df["code_muni"].isin(municipalities["code_muni"])).sum())
    if n_unmatched > 0:
        warnings.warn(
            strings["warn_unmatched"].format(n=n_unmatched), UserWarning, stacklevel=2
        )

    merged = municipalities.merge(
        local_df[["code_muni", "quadrant", "Ii"]], on="code_muni", how="left"
    )
    merged["quadrant"] = pd.Categorical(
        merged["quadrant"].astype(str), categories=_QUAD_LEVELS
    )

    present_levels = [
        lvl for lvl in _QUAD_LEVELS if lvl in merged["quadrant"].dropna().unique()
    ]
    quad_colors_use = {lvl: _QUAD_COLORS[lvl] for lvl in present_levels}
    quad_labels_use = {lvl: strings[f"quad_{lvl}"] for lvl in present_levels}

    p_map = (
        ggplot(merged)
        + geom_map(aes(fill="quadrant"), color="white", size=0.1)
        + scale_fill_manual(
            values=quad_colors_use,
            labels=quad_labels_use,
            name=strings["legend"],
            na_value=_NA_COLOR,
            drop=False,
        )
        + theme_void()
        + labs(
            title=title if title is not None else strings["title_map"],
            subtitle=strings["subtitle_map"],
        )
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="#666666"),
            legend_position="bottom",
        )
    )
    return p_map


def _moran_scatter_panel(
    x: dict[str, Any],
    alpha: float,  # noqa: ARG001 -- accepted for parity, unused (preserved R quirk)
    title: str | None,
    lang: str,
) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_hline,
        geom_point,
        geom_smooth,
        geom_vline,
        ggplot,
        labs,
        scale_color_manual,
        theme,
        theme_bw,
    )

    strings = _I18N[lang]
    local_df = x["local"].copy()

    # Attach z_std and lag_z if not already present.
    # sus_mod_spatial_moran does not store z_std/lag_z in "local" by
    # default, so they are derived from Ii and the quadrant as visual
    # approximations -- this mirrors R's .moran_scatter_panel exactly
    # (a documented approximation, not something this port improves on).
    if "z_std" not in local_df.columns:
        sign_z = np.where(
            local_df["quadrant"].isin(["HH", "HL"]),
            1,
            np.where(local_df["quadrant"].isin(["LL", "LH"]), -1, np.sign(local_df["Ii"])),
        )
        local_df["z_std"] = sign_z * np.sqrt(np.abs(local_df["Ii"]))

    if "lag_z" not in local_df.columns:
        safe_z = np.where(np.abs(local_df["z_std"]) < 1e-10, np.nan, local_df["z_std"])
        sign_wz = np.where(
            local_df["quadrant"].isin(["HH", "LH"]),
            1,
            np.where(local_df["quadrant"].isin(["LL", "HL"]), -1, np.sign(local_df["Ii"])),
        )
        with np.errstate(invalid="ignore", divide="ignore"):
            raw_lag = local_df["Ii"] / safe_z
        local_df["lag_z"] = np.where(
            np.isnan(raw_lag), sign_wz * np.sqrt(np.abs(local_df["Ii"])), raw_lag
        )

    local_df["quadrant"] = pd.Categorical(
        local_df["quadrant"].astype(str), categories=_QUAD_LEVELS
    )

    g = x["global"]
    i_val = g["I"] if isinstance(g, dict) else g["I"].iloc[0]
    p_val = g["p_simulated"] if isinstance(g, dict) else g["p_simulated"].iloc[0]
    subtitle_txt = f"I = {round(float(i_val), 3)}  p = {round(float(p_val), 3)}"

    present_levels = [
        lvl for lvl in _QUAD_LEVELS if lvl in local_df["quadrant"].dropna().unique()
    ]
    quad_colors_use = {lvl: _QUAD_COLORS[lvl] for lvl in present_levels}

    p_scatter = (
        ggplot(local_df, aes(x="z_std", y="lag_z"))
        + geom_point(aes(color="quadrant"), alpha=0.7, size=1.8)
        + geom_vline(xintercept=0, linetype="dashed", color="#808080")
        + geom_hline(yintercept=0, linetype="dashed", color="#808080")
        + geom_smooth(method="lm", se=False, color="#666666", size=0.8)
        + scale_color_manual(values=quad_colors_use, name=strings["legend"], drop=False)
        + labs(
            title=title if title is not None else strings["title_scatter"],
            subtitle=subtitle_txt,
            x=strings["x_scatter"],
            y=strings["y_scatter"],
        )
        + theme_bw()
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="#666666"),
            legend_position="bottom",
        )
    )
    return p_scatter
