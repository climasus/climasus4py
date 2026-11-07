"""Choropleth of Kulldorff spatial scan clusters - ggplot-style via plotnine.

Mirrors R: sus_mod_plot_spatial_scan.R

Consumes the ``climasus_spatial_scan``-shaped output of
``sus_mod_spatial_scan()`` (a ``dict`` in the Python port, mirroring the
convention already used by ``sus_mod_spatial_weights`` and
``sus_mod_spatial_moran``) and merges it onto municipality polygons.

Two fills, selected by *show_rr*:

- ``show_rr=True`` (default) - continuous fill by relative risk on a
  diverging scale centred at 1, with significant clusters outlined.
- ``show_rr=False`` - categorical fill by cluster membership.

Requires the optional [plot] extra and geopandas::

    pip install climasus4py[plot]
    pip install geopandas

Usage::

    >>> import climasus4py as cs
    >>> scan = cs.sus_mod_spatial_scan(df, "obitos", "pop", malha)
    >>> p = cs.sus_mod_plot_spatial_scan(scan, malha)
    >>> p.draw()
    >>> p.save("clusters.png")
"""

from __future__ import annotations

import itertools
import warnings
from typing import Any

import numpy as np
import pandas as pd

# R base-R colour names resolved to hex: plotnine's colour backend
# (mizani) does not recognise them, so they are spelled out here to keep
# the rendered colours identical.
_LOW = "#000080"  # navy
_MID = "#FFFFFF"  # white
_HIGH = "#FF0000"  # red
_BACKGROUND = "#CCCCCC"  # grey80
_MLC_FILL = "#B22222"  # firebrick
_OUTLINE = "#000000"  # black
_CAPTION = "#666666"  # gray40

#: Okabe-Ito plus two, exactly as R lists them.
_PALETTE_POOL: tuple[str, ...] = (
    "#E69F00",
    "#56B4E9",
    "#009E73",
    "#F0E442",
    "#0072B2",
    "#D55E00",
    "#CC79A7",
    "#44AA99",
    "#882255",
)

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Aglomerados Espaciais de Kulldorff",
        "caption_rr": (
            "Preenchimento: Risco Relativo (RR). "
            "Contorno espesso = aglomerado significativo."
        ),
        "caption_cat": "Preenchimento: categoria de aglomerado.",
        "legend_rr": "Risco Relativo",
        "legend_cluster": "Aglomerado",
        "label_mlc": "Mais provavel",
        "label_secondary": "Aglomerado {i}",
        "label_background": "Plano de fundo",
        "err_not_scan": (
            "'x' deve ser a saida de sus_mod_spatial_scan()."
        ),
        "err_not_sf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "err_no_code_muni": "'municipalities' deve conter a coluna 'code_muni'.",
        "warn_no_sig": (
            "Nenhum aglomerado significativo (alpha = {alpha}); o mapa mostra "
            "apenas o mais provavel."
        ),
        "warn_lang": "Idioma '{lang}' nao suportado. Usando 'pt'.",
    },
    "en": {
        "title": "Kulldorff Spatial Clusters",
        "caption_rr": (
            "Fill: Relative Risk (RR). Thick border = significant cluster."
        ),
        "caption_cat": "Fill: cluster category.",
        "legend_rr": "Relative Risk",
        "legend_cluster": "Cluster",
        "label_mlc": "Most likely",
        "label_secondary": "Cluster {i}",
        "label_background": "Background",
        "err_not_scan": "'x' must be the output of sus_mod_spatial_scan().",
        "err_not_sf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "err_no_code_muni": "'municipalities' must contain the column 'code_muni'.",
        "warn_no_sig": (
            "No significant clusters (alpha = {alpha}); map shows only the "
            "most-likely cluster."
        ),
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
    },
    "es": {
        "title": "Aglomerados Espaciales de Kulldorff",
        "caption_rr": (
            "Relleno: Riesgo Relativo (RR). "
            "Borde grueso = aglomerado significativo."
        ),
        "caption_cat": "Relleno: categoria de aglomerado.",
        "legend_rr": "Riesgo Relativo",
        "legend_cluster": "Aglomerado",
        "label_mlc": "Mas probable",
        "label_secondary": "Aglomerado {i}",
        "label_background": "Segundo plano",
        "err_not_scan": "'x' debe ser la salida de sus_mod_spatial_scan().",
        "err_not_sf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "err_no_code_muni": "'municipalities' debe contener la columna 'code_muni'.",
        "warn_no_sig": (
            "No hay aglomerados significativos (alpha = {alpha}); el mapa "
            "muestra solo el mas probable."
        ),
        "warn_lang": "Idioma '{lang}' no admitido. Usando 'pt'.",
    },
}


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_spatial_scan requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _cluster_colours(n_secondary: int) -> list[str]:
    """Fill colours for the secondary clusters.

    R slices the nine-colour pool with ``palette_pool[seq_len(min(n, 9))]``
    and then names the result against one level per cluster, so from the
    tenth secondary cluster on it hands ``setNames()`` more names than
    values and the call fails with *'names' attribute [n] must be the
    same length as the vector* -- the plot never renders. Confirmed
    end to end against the installed package: nine clusters draw, ten
    abort. Recorded as **M86**.

    This port cycles the pool instead, so a tenth cluster reuses the
    first colour rather than killing the figure. That is a deliberate
    departure from parity, on the grounds that there is no output to be
    faithful to -- R produces none. Flagged for the coordinator.
    """
    return [c for c, _ in zip(itertools.cycle(_PALETTE_POOL), range(n_secondary))]


def sus_mod_plot_spatial_scan(
    x: dict[str, Any],
    municipalities: Any,
    show_rr: bool = True,
    alpha: float = 0.05,
    title: str | None = None,
    lang: str = "pt",
    **kwargs: Any,
) -> Any:
    """Plot a choropleth of Kulldorff spatial scan cluster results.

    Renders the clusters found by ``sus_mod_spatial_scan()`` onto
    municipality polygons. Municipalities outside every cluster form the
    background.

    Fill (``show_rr``):
        - ``True`` (default): continuous fill by relative risk on a
          navy-white-red diverging scale centred at ``1``, so that
          municipalities at the null risk stay white. Municipalities in
          a cluster significant at *alpha* additionally get a thick
          black outline. Background municipalities have no relative
          risk and take the NA colour.
        - ``False``: categorical fill by cluster membership -- firebrick
          for the most-likely cluster, one palette colour per secondary
          cluster, grey for the background.

    Significance:
        *alpha* is applied here, to the p-values already stored on *x*;
        it is not re-estimated. Passing a stricter *alpha* than the one
        used for the scan therefore narrows what is drawn as
        significant, which is what R does too. When nothing clears the
        threshold, a warning is issued and the map still draws the
        most-likely cluster.

    Args:
        x: A ``dict`` shaped like R's ``climasus_spatial_scan`` object
            (the output of ``sus_mod_spatial_scan()``), with keys
            ``"most_likely_cluster"`` and ``"secondary_clusters"``.
        municipalities: A ``geopandas.GeoDataFrame`` with polygon
            geometry and a ``code_muni`` column.
        show_rr: If ``True`` (default), fill encodes relative risk; if
            ``False``, fill encodes cluster membership.
        alpha: Significance level used to flag clusters. Defaults to
            ``0.05``.
        title: Custom plot title, or ``None`` for the default.
        lang: Label language: ``"pt"`` (default), ``"en"`` or ``"es"``.
            An unrecognised value warns and falls back to ``"pt"``,
            as in R.
        **kwargs: Accepted and ignored, mirroring R's ``...``.

    Returns:
        A ``plotnine.ggplot`` object. Call ``.draw()`` to display or
        ``.save(path)`` to write it out.

    Raises:
        TypeError: If *x* is not a scan-shaped ``dict`` or
            *municipalities* is not a ``geopandas.GeoDataFrame``.
        ValueError: If *municipalities* has no ``code_muni`` column.
        ImportError: If ``plotnine`` or ``geopandas`` is not installed.

    Examples::

        import climasus4py as cs

        scan = cs.sus_mod_spatial_scan(df, "obitos", "pop", malha)
        cs.sus_mod_plot_spatial_scan(scan, malha, show_rr=False).draw()
    """
    if lang not in _I18N:
        warnings.warn(
            _I18N["pt"]["warn_lang"].format(lang=lang), UserWarning, stacklevel=2
        )
        lang = "pt"
    strings = _I18N[lang]

    _require_plotnine()
    try:
        import geopandas as gpd
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "sus_mod_plot_spatial_scan requires geopandas. "
            "Install with: pip install geopandas"
        ) from exc

    from plotnine import (
        aes,
        element_text,
        geom_map,
        ggplot,
        labs,
        scale_fill_gradient2,
        scale_fill_manual,
        theme,
        theme_void,
    )

    if not isinstance(x, dict) or "most_likely_cluster" not in x:
        raise TypeError(strings["err_not_scan"])
    if not isinstance(municipalities, gpd.GeoDataFrame):
        raise TypeError(strings["err_not_sf"])
    if "code_muni" not in municipalities.columns:
        raise ValueError(strings["err_no_code_muni"])

    municipalities = municipalities.copy()
    municipalities["code_muni"] = municipalities["code_muni"].astype(str)

    mlc = x["most_likely_cluster"]
    sec = x.get("secondary_clusters") or []

    def _significant(cluster: dict[str, Any]) -> bool:
        p = cluster.get("p_value")
        return p is not None and not pd.isna(p) and p < alpha

    mlc_sig = _significant(mlc)
    sec_sig = [_significant(c) for c in sec]
    if not mlc_sig and not any(sec_sig):
        warnings.warn(
            strings["warn_no_sig"].format(alpha=alpha), UserWarning, stacklevel=2
        )

    # Cluster 0 is the background; 1 is the most-likely cluster; secondary
    # cluster i carries id i + 1, matching R's numbering in the labels.
    lookup = pd.DataFrame(
        {
            "code_muni": municipalities["code_muni"].to_numpy(),
            "cluster_id": 0,
            "RR": np.nan,
            "is_significant": False,
        }
    )

    def _stamp(ids: Any, cid: int, rr: float, sig: bool) -> None:
        hit = lookup["code_muni"].isin([str(i) for i in ids])
        lookup.loc[hit, "cluster_id"] = cid
        lookup.loc[hit, "RR"] = rr
        lookup.loc[hit, "is_significant"] = sig

    _stamp(mlc["location_ids"], 1, mlc["RR"], mlc_sig)
    for i, cluster in enumerate(sec):
        _stamp(cluster["location_ids"], i + 2, cluster["RR"], sec_sig[i])

    lbl_bg = strings["label_background"]
    lbl_mlc = strings["label_mlc"]
    levels = (
        [lbl_mlc]
        + [strings["label_secondary"].format(i=i + 2) for i in range(len(sec))]
        + [lbl_bg]
    )
    lookup["cluster_label"] = pd.Categorical(
        [
            lbl_bg
            if cid == 0
            else lbl_mlc
            if cid == 1
            else strings["label_secondary"].format(i=cid)
            for cid in lookup["cluster_id"]
        ],
        categories=levels,
    )

    merged = municipalities.merge(lookup, on="code_muni", how="left")
    plot_title = title if title is not None else strings["title"]
    caption = strings["caption_rr"] if show_rr else strings["caption_cat"]

    base_theme = theme(
        plot_title=element_text(face="bold", ha="center"),
        plot_caption=element_text(color=_CAPTION, ha="center", size=8),
    )

    if show_rr:
        significant = merged[merged["is_significant"].fillna(False)]
        p = ggplot(merged) + geom_map(
            aes(fill="RR"), color="white", size=0.1
        )
        if len(significant) > 0:
            p = p + geom_map(
                significant, fill=None, color=_OUTLINE, size=1.5
            )
        p = (
            p
            + scale_fill_gradient2(
                low=_LOW,
                mid=_MID,
                high=_HIGH,
                midpoint=1,
                na_value=_BACKGROUND,
                name=strings["legend_rr"],
            )
            + theme_void()
            + labs(title=plot_title, caption=caption)
            + base_theme
        )
        return p

    fill_values = dict(
        zip(
            levels,
            [_MLC_FILL] + _cluster_colours(len(sec)) + [_BACKGROUND],
        )
    )
    return (
        ggplot(merged)
        + geom_map(aes(fill="cluster_label"), color="white", size=0.1)
        + scale_fill_manual(
            values=fill_values,
            na_value=_BACKGROUND,
            name=strings["legend_cluster"],
            drop=False,
        )
        + theme_void()
        + labs(title=plot_title, caption=caption)
        + base_theme
        + theme(legend_position="bottom")
    )
