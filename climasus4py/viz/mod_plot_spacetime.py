"""Five views of a spatio-temporal Bayesian fit - ggplot-style via plotnine.

Mirrors R: sus_mod_plot_spacetime.R

Consumes a ``climasus_spacetime_bayes``- or
``climasus_spacetime_exceedance``-shaped ``dict`` and draws one of:

- ``"rr_map"`` - choropleth of the relative risk
- ``"temporal"`` - the risk trend over time, with a credible band
- ``"interaction"`` - heatmap of the space-time interaction component
- ``"exceedance"`` - choropleth of the exceedance probability
- ``"coef"`` - the fixed-effect coefficients

This function does **not** depend on INLA - it reads tables off an
already fitted object - so it is portable today even though
``sus_mod_spacetime_bayes()`` is still a stub here.

Porting this module turned up four defects in the R original. Two are
replicated and two are not, on the principle that output carrying the
information in a poor form is worth reproducing, while output carrying
no information at all is not:

- **M92** (fixed here): every label on every panel comes out as the
  untranslated internal key - ``"rr_map_title"``, ``"coef_x"``. The
  translation table exists in all three languages and nothing reads it.
  See :data:`USE_INTENDED_LABELS`.
- **M94** (fixed here): ``type="exceedance"`` always aborts, because it
  reads a column no function in the package produces. See
  :data:`EXCEEDANCE_COLUMN_FROM_THRESHOLD`.
- **M93** (replicated): on ``type="rr_map"`` the colour scale is
  inverted - high risk renders blue, low risk red. See
  :data:`INVERT_RR_PALETTE`.
- **M91** (replicated, and not this function's doing): the interaction
  table handed over by the fitter is mislabelled, so the heatmap is
  drawn on bad keys. Documented under ``type="interaction"``.
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Behaviour switches for the replicated / corrected R defects
# ---------------------------------------------------------------------------

#: Use the label table the R author wrote but the R code never reads (M92).
#:
#: R resolves plot labels through a helper that looks them up in the
#: *wrong* table, so every title and axis label renders as its own key:
#: ``"rr_map_title"``, ``"temporal_title"``, ``"coef_x"``. The intended
#: table - ``.st_plot_labels``, 32 keys in pt/en/es - sits in the package
#: and no function reads it. ``lang`` therefore has no effect in R.
#:
#: ``True`` (the default) uses that table, copied verbatim. Set it to
#: ``False`` to reproduce R's actual output, keys and all.
USE_INTENDED_LABELS: bool = True

#: Read the exceedance column the pipeline actually produces (M94).
#:
#: R's exceedance panel reads a column named ``exceedance_prob``. That
#: name appears in exactly one place in the whole package - the panel
#: itself - and no function ever writes it, so ``type="exceedance"``
#: aborts on every input. ``sus_mod_spacetime_exceedance()`` produces
#: ``p_gt_1``, ``p_gt_1_5``, ... instead.
#:
#: ``True`` (the default) selects ``p_gt_<threshold>`` from the table,
#: which is what the panel's own legend (``P(RR > {thr})``) describes.
#: Set it to ``False`` to insist on ``exceedance_prob`` as R does.
EXCEEDANCE_COLUMN_FROM_THRESHOLD: bool = True

#: Reproduce R's inverted relative-risk colour scale (M93).
#:
#: ``.st_plot_rr_map`` passes ``brewer.pal(11, palette)[1]`` as the
#: scale's *low* end and ``[11]`` as its *high* end. For the default
#: ``"RdYlBu"`` that is ``#A50026`` (dark red) for low risk and
#: ``#313695`` (dark blue) for high risk - so a high-risk municipality
#: renders blue and a low-risk one red. Both sibling panels in the same
#: package run the other way: the exceedance map goes white to dark red
#: as probability rises, and ``sus_mod_plot_spatial_bayes`` goes navy to
#: red3 as risk rises.
#:
#: ``True`` (the default) keeps parity with R. Set it to ``False`` to
#: swap the ends so that high risk is warm.
INVERT_RR_PALETTE: bool = True

VALID_TYPES: tuple[str, ...] = (
    "rr_map", "temporal", "interaction", "exceedance", "coef",
)

# Ends of the 11-class ColorBrewer diverging palettes, which is all R
# uses (it indexes [1] and [11]). Values are ColorBrewer's own, so the
# rendered ends match RColorBrewer exactly.
_BREWER_ENDS: dict[str, tuple[str, str]] = {
    "RdYlBu": ("#A50026", "#313695"),
    "RdBu": ("#67001F", "#053061"),
    "RdYlGn": ("#A50026", "#006837"),
    "Spectral": ("#9E0142", "#5E4FA2"),
    "BrBG": ("#543005", "#003C30"),
    "PiYG": ("#8E0152", "#276419"),
    "PRGn": ("#40004B", "#00441B"),
    "PuOr": ("#7F3B08", "#2D004B"),
}

_EXC_COLORS: tuple[str, ...] = ("#FFFFFF", "#fee08b", "#d73027", "#a50026")
_TILE_LOW, _TILE_HIGH = "#313695", "#a50026"
_LINE = "#2166ac"
_NA_FILL = "#E5E5E5"  # grey90
_BORDER = "#CCCCCC"  # grey80
_OUTLINE = "#000000"
_REF_LINE = "#999999"  # grey60
_SUBTITLE = "#666666"  # grey40
_CAPTION = "#7F7F7F"  # grey50

# The label table R writes and never reads (see USE_INTENDED_LABELS).
# Copied verbatim, including the "Excedaencia" typo in the Portuguese
# exceedance title, so that turning the switch off and on only changes
# which table is consulted and never the wording.
_LABELS: dict[str, dict[str, str]] = {
    "rr_map_title": {
        "pt": "Risco Relativo Espaco-Temporal (media posterior)",
        "en": "Space-Time Relative Risk (posterior mean)",
        "es": "Riesgo Relativo Espacio-Temporal (media posterior)",
    },
    "rr_map_title_t": {
        "pt": "Risco Relativo Espaco-Temporal -- periodo {t}",
        "en": "Space-Time Relative Risk -- period {t}",
        "es": "Riesgo Relativo Espacio-Temporal -- periodo {t}",
    },
    "rr_fill": {"pt": "RR", "en": "RR", "es": "RR"},
    "rr_sig_note": {
        "pt": "Contorno preto: IC95% inferior > 1 (risco significativamente elevado)",
        "en": "Black outline: lower 95% CI > 1 (significantly elevated risk)",
        "es": "Contorno negro: IC95% inferior > 1 (riesgo significativamente elevado)",
    },
    "facet_time_label": {"pt": "Periodo", "en": "Period", "es": "Periodo"},
    "temporal_title": {
        "pt": "Evolucao Temporal do Risco Relativo",
        "en": "Temporal Trend of Relative Risk",
        "es": "Evolucion Temporal del Riesgo Relativo",
    },
    "temporal_sub": {
        "pt": "Media posterior com intervalo de credibilidade de 95%",
        "en": "Posterior mean with 95% credible interval",
        "es": "Media posterior con intervalo de credibilidad del 95%",
    },
    "x_time": {"pt": "Indice de Tempo", "en": "Time Index", "es": "Indice de Tiempo"},
    "y_rr": {
        "pt": "Risco Relativo (IC 95%)",
        "en": "Relative Risk (95% CI)",
        "es": "Riesgo Relativo (IC 95%)",
    },
    "interaction_title": {
        "pt": "Interacao Espaco-Tempo (efeito diferencial)",
        "en": "Space-Time Interaction (differential effect)",
        "es": "Interaccion Espacio-Tiempo (efecto diferencial)",
    },
    "interaction_sub": {
        "pt": "Media posterior do componente de interacao (gamma)",
        "en": "Posterior mean of the interaction component (gamma)",
        "es": "Media posterior del componente de interaccion (gamma)",
    },
    "x_time_idx": {"pt": "Periodo", "en": "Period", "es": "Periodo"},
    "y_muni": {"pt": "Municipio", "en": "Municipality", "es": "Municipio"},
    "gamma_fill": {
        "pt": "gamma (interacao)",
        "en": "gamma (interaction)",
        "es": "gamma (interaccion)",
    },
    "exc_title": {
        "pt": "Probabilidade de Excedaencia P(RR > {thr})",
        "en": "Exceedance Probability P(RR > {thr})",
        "es": "Probabilidad de Excedencia P(RR > {thr})",
    },
    "exc_fill": {"pt": "P(RR > {thr})", "en": "P(RR > {thr})", "es": "P(RR > {thr})"},
    "coef_title": {
        "pt": "Efeitos Fixos: Media Posterior (IC 95%)",
        "en": "Fixed Effects: Posterior Mean (95% CI)",
        "es": "Efectos Fijos: Media Posterior (IC 95%)",
    },
    "coef_x": {
        "pt": "Media Posterior (IC 95%)",
        "en": "Posterior Mean (95% CI)",
        "es": "Media Posterior (IC 95%)",
    },
    "coef_y": {"pt": "Covariavel", "en": "Covariate", "es": "Covariable"},
    "err_not_spacetime": {
        "pt": (
            "'x' deve ser a saida de sus_mod_spacetime_bayes() ou de "
            "sus_mod_spacetime_exceedance()."
        ),
        "en": (
            "'x' must be the output of sus_mod_spacetime_bayes() or "
            "sus_mod_spacetime_exceedance()."
        ),
        "es": (
            "'x' debe ser la salida de sus_mod_spacetime_bayes() o de "
            "sus_mod_spacetime_exceedance()."
        ),
    },
    "err_exceedance_type": {
        "pt": (
            "type='exceedance' requer a saida de sus_mod_spacetime_exceedance()."
        ),
        "en": (
            "type='exceedance' requires the output of sus_mod_spacetime_exceedance()."
        ),
        "es": (
            "type='exceedance' requiere la salida de sus_mod_spacetime_exceedance()."
        ),
    },
    "err_not_sf": {
        "pt": (
            "'municipalities' deve ser um geopandas.GeoDataFrame com a coluna "
            "'code_muni'."
        ),
        "en": (
            "'municipalities' must be a geopandas.GeoDataFrame with a "
            "'code_muni' column."
        ),
        "es": (
            "'municipalities' debe ser un geopandas.GeoDataFrame con la columna "
            "'code_muni'."
        ),
    },
    "err_type_needs_sf": {
        "pt": "type='{type}' requer o argumento 'municipalities' (GeoDataFrame).",
        "en": "type='{type}' requires the 'municipalities' argument (GeoDataFrame).",
        "es": "type='{type}' requiere el argumento 'municipalities' (GeoDataFrame).",
    },
    "err_no_fixed": {
        "pt": "Nenhum efeito fixo encontrado em x['fixed'].",
        "en": "No fixed effects found in x['fixed'].",
        "es": "No se encontraron efectos fijos en x['fixed'].",
    },
    "err_no_rr": {
        "pt": "O objeto 'x' nao contem 'rr'.",
        "en": "Object 'x' does not contain 'rr'.",
        "es": "El objeto 'x' no contiene 'rr'.",
    },
    "err_no_interaction": {
        "pt": (
            "O objeto 'x' nao contem 'interaction_re'. Ajuste o modelo com "
            "interacao espaco-tempo."
        ),
        "en": (
            "Object 'x' does not contain 'interaction_re'. Fit the model with "
            "space-time interaction."
        ),
        "es": (
            "El objeto 'x' no contiene 'interaction_re'. Ajuste el modelo con "
            "interaccion espacio-tiempo."
        ),
    },
    "err_no_exceedance": {
        "pt": "O objeto 'x' nao contem 'exceedance'.",
        "en": "Object 'x' does not contain 'exceedance'.",
        "es": "El objeto 'x' no contiene 'exceedance'.",
    },
    "err_threshold_not_found": {
        "pt": (
            "Limiar {thr} nao encontrado em x['exceedance']. Colunas "
            "disponiveis: {avail}."
        ),
        "en": (
            "Threshold {thr} not found in x['exceedance']. Available columns: "
            "{avail}."
        ),
        "es": (
            "Umbral {thr} no encontrado en x['exceedance']. Columnas "
            "disponibles: {avail}."
        ),
    },
    "err_bad_type": {
        "pt": "type='{type}' invalido. Use um de: {valid}.",
        "en": "type='{type}' is invalid. Use one of: {valid}.",
        "es": "type='{type}' no es valido. Use uno de: {valid}.",
    },
    "err_bad_palette": {
        "pt": "Paleta '{palette}' nao reconhecida. Use uma de: {valid}.",
        "en": "Palette '{palette}' not recognised. Use one of: {valid}.",
        "es": "Paleta '{palette}' no reconocida. Use una de: {valid}.",
    },
    "warn_lang": {
        "pt": "Idioma '{lang}' nao suportado. Usando 'pt'.",
        "en": "Language '{lang}' not supported. Using 'pt'.",
        "es": "Idioma '{lang}' no admitido. Usando 'pt'.",
    },
    "warn_time_range": {
        "pt": (
            "'time_range' ignorado para type='{type}'. Somente 'temporal' e "
            "'interaction' filtram por tempo."
        ),
        "en": (
            "'time_range' ignored for type='{type}'. Only 'temporal' and "
            "'interaction' support time filtering."
        ),
        "es": (
            "'time_range' ignorado para type='{type}'. Solo 'temporal' e "
            "'interaction' admiten filtrado por tiempo."
        ),
    },
    "warn_plotly": {
        "pt": (
            "plotnine nao tem equivalente ao plotly::ggplotly. Retornando o "
            "objeto estatico."
        ),
        "en": (
            "plotnine has no plotly::ggplotly equivalent. Returning the static "
            "object."
        ),
        "es": (
            "plotnine no tiene equivalente a plotly::ggplotly. Retornando el "
            "objeto estatico."
        ),
    },
}


def _lbl(key: str, lang: str, **kwargs: Any) -> str:
    """Resolve a label, honouring :data:`USE_INTENDED_LABELS` (M92)."""
    if not USE_INTENDED_LABELS and not key.startswith(("err_", "warn_")):
        return key
    entry = _LABELS.get(key)
    if entry is None:
        return key
    return (entry.get(lang) or entry["pt"]).format(**kwargs)


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_spacetime requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _map_theme(base_size: float) -> Any:
    """R's ``.st_map_theme()``."""
    from plotnine import element_text, theme, theme_void

    return theme_void(base_size=base_size) + theme(
        legend_position="right",
        plot_title=element_text(ha="center", face="bold", size=base_size + 1),
        plot_subtitle=element_text(ha="center", size=base_size - 1, color=_SUBTITLE),
        plot_caption=element_text(ha="left", size=base_size - 2, color=_CAPTION),
        strip_text=element_text(size=base_size - 1, face="bold"),
    )


def _panel_theme(base_size: float) -> Any:
    from plotnine import element_text, theme, theme_bw

    return theme_bw(base_size=base_size) + theme(
        plot_title=element_text(ha="center", face="bold"),
        plot_subtitle=element_text(ha="center", color=_SUBTITLE),
    )


def _as_str_codes(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["code_muni"] = out["code_muni"].astype(str)
    return out


def _plot_rr_map(
    x: dict[str, Any], municipalities: Any, time_point: Any, time_range: Any,
    facet_time: bool, palette: str, title: str | None, base_size: float, lang: str,
) -> Any:
    from plotnine import (
        aes, facet_wrap, geom_map, ggplot, labs, scale_fill_gradient2,
    )

    rr = x.get("rr")
    if rr is None:
        raise ValueError(_lbl("err_no_rr", lang))
    if palette not in _BREWER_ENDS:
        raise ValueError(
            _lbl("err_bad_palette", lang, palette=palette,
                 valid=", ".join(sorted(_BREWER_ENDS)))
        )

    rr = _as_str_codes(pd.DataFrame(rr))
    munis = _as_str_codes(municipalities)
    has_time = "time_idx" in rr.columns

    if has_time and not facet_time:
        if time_point is not None:
            rr = rr[rr["time_idx"] == time_point]
        if time_range is not None:
            rr = rr[
                (rr["time_idx"] >= time_range[0]) & (rr["time_idx"] <= time_range[1])
            ]
        if time_point is None:
            # R averages each municipality over the periods that survived.
            rr = (
                rr.groupby("code_muni", as_index=False)[
                    ["rr_mean", "rr_lower95", "rr_upper95"]
                ].mean()
            )

    merged = munis.merge(rr, on="code_muni", how="left")
    merged["sig_elevated"] = merged["rr_lower95"].notna() & (
        merged["rr_lower95"] > 1
    )

    low, high = _BREWER_ENDS[palette]
    if not INVERT_RR_PALETTE:
        low, high = high, low

    default_title = (
        _lbl("rr_map_title_t", lang, t=time_point)
        if time_point is not None and not facet_time
        else _lbl("rr_map_title", lang)
    )

    p = ggplot(merged) + geom_map(aes(fill="rr_mean"), color=_BORDER, size=0.15)
    flagged = merged[merged["sig_elevated"]]
    if len(flagged) > 0:
        p = p + geom_map(flagged, fill=None, color=_OUTLINE, size=0.55)
    p = (
        p
        + scale_fill_gradient2(
            low=low, mid="#FFFFFF", high=high, midpoint=1,
            na_value=_NA_FILL, name=_lbl("rr_fill", lang),
        )
        + labs(
            title=title if title is not None else default_title,
            caption=_lbl("rr_sig_note", lang),
        )
        + _map_theme(base_size)
    )
    if has_time and facet_time:
        p = p + facet_wrap("~time_idx")
    return p


def _plot_temporal(
    x: dict[str, Any], time_range: Any, title: str | None, base_size: float,
    lang: str,
) -> Any:
    from plotnine import (
        aes, geom_hline, geom_line, geom_point, geom_ribbon, ggplot, labs,
    )

    rr = x.get("rr")
    if rr is None:
        raise ValueError(_lbl("err_no_rr", lang))
    rr = pd.DataFrame(rr).copy()
    if "time_idx" not in rr.columns:
        rr["time_idx"] = 1
    if time_range is not None:
        rr = rr[
            (rr["time_idx"] >= time_range[0]) & (rr["time_idx"] <= time_range[1])
        ]
    trend = rr.groupby("time_idx", as_index=False)[
        ["rr_mean", "rr_lower95", "rr_upper95"]
    ].mean()

    return (
        ggplot(trend, aes(x="time_idx", y="rr_mean"))
        + geom_hline(yintercept=1, linetype="dashed", color=_REF_LINE, size=0.5)
        + geom_ribbon(
            aes(ymin="rr_lower95", ymax="rr_upper95"), fill=_LINE, alpha=0.2
        )
        + geom_line(color=_LINE, size=0.8)
        + geom_point(color=_LINE, size=1.8)
        + labs(
            title=title if title is not None else _lbl("temporal_title", lang),
            subtitle=_lbl("temporal_sub", lang),
            x=_lbl("x_time", lang),
            y=_lbl("y_rr", lang),
        )
        + _panel_theme(base_size)
    )


def _plot_interaction(
    x: dict[str, Any], time_range: Any, title: str | None, base_size: float,
    lang: str,
) -> Any:
    """Heatmap of the space-time interaction component.

    R reaches this table as ``x$interaction``, which resolves to the
    fitter's ``interaction_re`` slot only through R's partial matching of
    ``$`` on lists. This port reads ``interaction_re`` by its real name
    and also accepts ``interaction``.

    What the fitter puts in that table is not trustworthy (**M91**):
    measured on a real type-II fit of 42 areas over 8 periods, it came
    back with 2688 rows for 336 municipality-period cells - 328 cells
    appearing once and 8 cells, all of one municipality, appearing 295
    times each, with 295 *different* values apiece. The labels are
    misaligned against a longer vector of interaction nodes. The heatmap
    below is therefore drawn on bad keys, and the row ordering - R
    averages gamma per municipality to order the rows - is skewed by the
    duplicates. Replicated as-is; the defect is upstream.
    """
    from plotnine import (
        aes, element_blank, element_text, geom_tile, ggplot, labs,
        scale_fill_gradient2, theme, theme_bw,
    )

    ia = x.get("interaction_re")
    if ia is None:
        ia = x.get("interaction")
    if ia is None or len(ia) == 0:
        raise ValueError(_lbl("err_no_interaction", lang))

    ia = _as_str_codes(pd.DataFrame(ia))
    if time_range is not None and "time_idx" in ia.columns:
        ia = ia[
            (ia["time_idx"] >= time_range[0]) & (ia["time_idx"] <= time_range[1])
        ]

    order = (
        ia.groupby("code_muni", as_index=False)["gamma_mean"].mean()
        .sort_values("gamma_mean", kind="stable")["code_muni"].tolist()
    )
    ia["code_muni"] = pd.Categorical(ia["code_muni"], categories=order)

    return (
        ggplot(ia, aes(x="time_idx", y="code_muni", fill="gamma_mean"))
        + geom_tile(color="white", size=0.05)
        + scale_fill_gradient2(
            low=_TILE_LOW, mid="#FFFFFF", high=_TILE_HIGH, midpoint=0,
            na_value=_NA_FILL, name=_lbl("gamma_fill", lang),
        )
        + labs(
            title=title if title is not None else _lbl("interaction_title", lang),
            subtitle=_lbl("interaction_sub", lang),
            x=_lbl("x_time_idx", lang),
            y=_lbl("y_muni", lang),
        )
        + theme_bw(base_size=base_size)
        + theme(
            axis_text_y=element_text(size=max(base_size - 4, 5)),
            plot_title=element_text(ha="center", face="bold"),
            plot_subtitle=element_text(ha="center", color=_SUBTITLE),
            panel_grid=element_blank(),
        )
    )


def _exceedance_column(
    table: pd.DataFrame, threshold: float, lang: str
) -> str:
    """Pick the probability column for *threshold* (M94)."""
    if not EXCEEDANCE_COLUMN_FROM_THRESHOLD:
        return "exceedance_prob"
    suffix = f"{threshold:.15g}".replace(".", "_")
    col = f"p_gt_{suffix}"
    if col not in table.columns:
        avail = ", ".join(c for c in table.columns if c.startswith("p_gt_"))
        raise ValueError(
            _lbl("err_threshold_not_found", lang, thr=threshold, avail=avail)
        )
    return col


def _plot_exceedance(
    x: dict[str, Any], municipalities: Any, threshold: float, title: str | None,
    base_size: float, lang: str,
) -> Any:
    from plotnine import aes, geom_map, ggplot, labs, scale_fill_gradientn

    table = x.get("exceedance")
    if table is None:
        raise ValueError(_lbl("err_no_exceedance", lang))
    table = _as_str_codes(pd.DataFrame(table).drop(columns="geometry", errors="ignore"))
    col = _exceedance_column(table, threshold, lang)
    if col not in table.columns:
        raise ValueError(_lbl("err_no_exceedance", lang))

    # R averages the probability over periods, one value per municipality.
    if "time_idx" in table.columns or "time_period" in table.columns:
        table = table.groupby("code_muni", as_index=False)[col].mean()

    merged = _as_str_codes(municipalities).merge(
        table[["code_muni", col]], on="code_muni", how="left"
    )

    return (
        ggplot(merged)
        + geom_map(aes(fill=col), color=_BORDER, size=0.15)
        + scale_fill_gradientn(
            colors=list(_EXC_COLORS), limits=(0, 1), na_value=_NA_FILL,
            name=_lbl("exc_fill", lang, thr=threshold),
        )
        + labs(
            title=title if title is not None
            else _lbl("exc_title", lang, thr=threshold)
        )
        + _map_theme(base_size)
    )


def _plot_coef(
    x: dict[str, Any], title: str | None, base_size: float, lang: str
) -> Any:
    from plotnine import (
        aes, element_blank, element_text, geom_errorbarh, geom_point,
        geom_vline, ggplot, labs, theme, theme_bw,
    )

    fixed = x.get("fixed")
    if fixed is None or len(fixed) == 0:
        raise ValueError(_lbl("err_no_fixed", lang))
    fixed = pd.DataFrame(fixed).copy()

    order = fixed.sort_values("mean", kind="stable")["term"].tolist()
    fixed["term"] = pd.Categorical(fixed["term"], categories=order, ordered=True)

    # plotnine's geom_pointrange is vertical-only; the horizontal figure
    # is built from errorbarh + point, as in mod_plot_pool's forest panel.
    return (
        ggplot(fixed, aes(x="mean", y="term"))
        + geom_vline(xintercept=0, linetype="dashed", color=_REF_LINE)
        + geom_errorbarh(
            aes(xmin="lower95", xmax="upper95"), height=0, size=0.7, color=_LINE
        )
        + geom_point(color=_LINE, size=0.55 * 4)
        + labs(
            title=title if title is not None else _lbl("coef_title", lang),
            x=_lbl("coef_x", lang),
            y=_lbl("coef_y", lang),
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(ha="center", face="bold"),
            panel_grid_major_y=element_blank(),
        )
    )


def sus_mod_plot_spacetime(
    x: dict[str, Any],
    type: Literal["rr_map", "temporal", "interaction", "exceedance", "coef"] = "rr_map",
    municipalities: Any = None,
    time_point: Any = None,
    time_range: Any = None,
    threshold: float = 1,
    facet_time: bool = False,
    palette: str = "RdYlBu",
    title: str | None = None,
    base_size: float = 11,
    interactive: bool = False,
    lang: str = "pt",
    **kwargs: Any,
) -> Any:
    """Five views of a spatio-temporal Bayesian fit.

    Panel types (``type``):
        - ``"rr_map"`` (default): choropleth of the relative risk.
          Municipalities whose lower 95% credible bound exceeds 1 are
          outlined in black. With *time_point* the map is that single
          period; with *facet_time* one small map per period; otherwise
          each municipality is averaged over the periods kept.

          Note the colour direction: R maps **low** risk to the warm end
          of *palette* and **high** risk to the cool end, so on the
          default ``"RdYlBu"`` a high-risk municipality renders blue.
          Replicated - see :data:`INVERT_RR_PALETTE` to swap it.
        - ``"temporal"``: the risk averaged across municipalities, per
          period, with a 95% credible band and a reference at 1.
        - ``"interaction"``: heatmap of the interaction component, rows
          ordered by each municipality's mean. Requires a fit made with
          an interaction term. **The table this reads is mislabelled by
          the fitter (M91)** - see :func:`_plot_interaction`.
        - ``"exceedance"``: choropleth of the exceedance probability for
          *threshold*, averaged over periods. Requires the output of
          ``sus_mod_spacetime_exceedance()``.
        - ``"coef"``: the fixed effects, ordered by posterior mean, with
          a dashed reference at zero.

    Args:
        x: A ``dict`` shaped like R's ``climasus_spacetime_bayes`` (for
            every type but ``"exceedance"``) or
            ``climasus_spacetime_exceedance`` (for that one). The slots
            read, by type: ``"rr"`` for ``"rr_map"`` and ``"temporal"``,
            ``"interaction_re"`` for ``"interaction"``, ``"fixed"`` for
            ``"coef"``, ``"exceedance"`` for ``"exceedance"``.
        type: Which view to draw. Defaults to ``"rr_map"``.
        municipalities: A ``geopandas.GeoDataFrame`` with polygon
            geometry and a ``code_muni`` column. Required for
            ``"rr_map"`` and ``"exceedance"``.
        time_point: Single period to map, or ``None`` (default) to
            average over periods. Only used by ``"rr_map"``.
        time_range: ``(start, end)`` period bounds, inclusive. Honoured
            by ``"temporal"`` and ``"interaction"``; for any other type
            a warning is issued -- though ``"rr_map"`` does in fact
            apply it, which is what R does too.
        threshold: Relative-risk threshold for ``"exceedance"``.
            Defaults to ``1``.
        facet_time: If ``True``, ``"rr_map"`` draws one panel per
            period instead of averaging. Defaults to ``False``.
        palette: Name of an 11-class ColorBrewer diverging palette for
            ``"rr_map"``. One of ``"RdYlBu"`` (default), ``"RdBu"``,
            ``"RdYlGn"``, ``"Spectral"``, ``"BrBG"``, ``"PiYG"``,
            ``"PRGn"``, ``"PuOr"``.
        title: Custom title, or ``None`` for the default.
        base_size: Base font size for the theme. Defaults to ``11``.
        interactive: R wraps the figure with ``plotly::ggglotly()``.
            plotnine has no equivalent, so this warns and returns the
            static figure -- which is also R's own fallback when plotly
            is not installed.
        lang: Label language: ``"pt"`` (default), ``"en"`` or ``"es"``.
            An unrecognised value warns and falls back to ``"pt"``.

            In R this argument has **no effect** on this function: the
            labels are looked up in the wrong table and every one comes
            out as its own key. This port uses the intended table -- see
            :data:`USE_INTENDED_LABELS`.
        **kwargs: Accepted and ignored, mirroring R's ``...``.

    Returns:
        A ``plotnine.ggplot``.

    Raises:
        TypeError: If *x* is not a dict, or *municipalities* is not a
            ``geopandas.GeoDataFrame``.
        ValueError: If *type* or *palette* is invalid, if a required
            slot is missing, if *municipalities* is absent for a map
            type, or if *threshold* has no matching column.
        ImportError: If ``plotnine`` or ``geopandas`` is not installed.

    Examples::

        import climasus4py as cs

        cs.sus_mod_plot_spacetime(fit, type="temporal").draw()
        cs.sus_mod_plot_spacetime(fit, "rr_map", malha, facet_time=True).draw()
    """
    if lang not in ("pt", "en", "es"):
        warnings.warn(
            _LABELS["warn_lang"]["pt"].format(lang=lang), UserWarning, stacklevel=2
        )
        lang = "pt"

    if type not in VALID_TYPES:
        raise ValueError(
            _lbl("err_bad_type", lang, type=type, valid=", ".join(VALID_TYPES))
        )
    if not isinstance(x, dict):
        raise TypeError(_lbl("err_not_spacetime", lang))
    if type == "exceedance" and x.get("exceedance") is None:
        raise ValueError(_lbl("err_exceedance_type", lang))

    _require_plotnine()

    if type in ("rr_map", "exceedance"):
        import geopandas as gpd

        if municipalities is None:
            raise ValueError(_lbl("err_type_needs_sf", lang, type=type))
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise TypeError(_lbl("err_not_sf", lang))
        if "code_muni" not in municipalities.columns:
            raise ValueError(_lbl("err_not_sf", lang))

    if time_range is not None and type not in ("temporal", "interaction"):
        warnings.warn(
            _lbl("warn_time_range", lang, type=type), UserWarning, stacklevel=2
        )

    if type == "rr_map":
        p = _plot_rr_map(
            x, municipalities, time_point, time_range, facet_time, palette,
            title, base_size, lang,
        )
    elif type == "temporal":
        p = _plot_temporal(x, time_range, title, base_size, lang)
    elif type == "interaction":
        p = _plot_interaction(x, time_range, title, base_size, lang)
    elif type == "exceedance":
        p = _plot_exceedance(x, municipalities, threshold, title, base_size, lang)
    else:
        p = _plot_coef(x, title, base_size, lang)

    if interactive:
        warnings.warn(_lbl("warn_plotly", lang), UserWarning, stacklevel=2)
    return p
