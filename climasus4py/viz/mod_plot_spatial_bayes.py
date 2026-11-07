"""Maps and coefficient plot for Bayesian CAR/BYM disease mapping - via plotnine.

Mirrors R: sus_mod_plot_spatial_bayes.R

Consumes a ``climasus_spatial_bayes``-shaped ``dict`` and renders three
views of it: smoothed relative risk, posterior uncertainty, and the
fixed-effect coefficients.

This function does **not** depend on CARBayes. It takes an already
fitted object, so it is portable today even though
``sus_mod_spatial_bayes()`` is still a stub in this package (CARBayes's
MCMC sampler has no Python equivalent - see its module docstring). Until
a Python fitter exists, the input is produced in R and carried over;
the shape that has to be reproduced is spelled out under *Args* below
and is the reason this docstring is as specific as it is.

Requires the optional [plot] extra and, for the map types, geopandas::

    pip install climasus4py[plot]
    pip install geopandas
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

# R base-R colour names resolved to hex - mizani does not recognise
# them. Same convention as mod_plot_spatial_moran / mod_plot_spatial_scan.
_LOW = "#000080"  # navy
_MID = "#FFFFFF"  # white
_HIGH = "#CD0000"  # red3
_NA_FILL = "#E5E5E5"  # grey90
_BORDER = "#CCCCCC"  # grey80
_OUTLINE = "#000000"  # black
_ZERO_LINE = "#999999"  # grey60
_CAPTION = "#7F7F7F"  # grey50
_POINTRANGE = "#2166ac"

VALID_TYPES: tuple[str, ...] = ("rr", "uncertainty", "coef", "both")

#: Columns each slot of the fitted object must carry.
_REQUIRED_RR = ("code_muni", "rr_mean", "rr_lower95", "rr_upper95")
_REQUIRED_FIXED = ("term", "mean", "lower95", "upper95")

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "rr_title": "Risco Relativo Suavizado (BYM/CAR)",
        "rr_fill": "RR",
        "unc_title": "Incerteza: Largura do Intervalo de Credibilidade (95%)",
        "unc_fill": "Largura IC",
        "coef_title": "Efeitos Fixos: Media Posterior (IC 95%)",
        "coef_x": "Media Posterior (IC 95%)",
        "coef_y": "Covariavel",
        "both_title": "Mapeamento Bayesiano Espacial de Doencas",
        "sig_note": (
            "Contorno preto: IC95% inferior > 1 (risco significativamente elevado)"
        ),
        "err_not_bayes": (
            "'x' deve ser a saida de sus_mod_spatial_bayes() (chaves 'rr' e 'fixed')."
        ),
        "err_rr_cols": "'x[\"rr\"]' deve conter as colunas: {cols}.",
        "err_not_sf": (
            "'municipalities' deve ser um geopandas.GeoDataFrame com a coluna "
            "'code_muni'."
        ),
        "err_no_fixed": (
            "Nenhum efeito fixo encontrado em x['fixed']. Ajuste o modelo com "
            "covariaveis."
        ),
        "err_type_needs_sf": (
            "type='{type}' requer o argumento 'municipalities' (GeoDataFrame)."
        ),
        "err_bad_type": "type='{type}' invalido. Use um de: {valid}.",
        "warn_lang": "Idioma '{lang}' nao suportado. Usando 'pt'.",
        "warn_patchwork": (
            "plotnine nao tem equivalente ao patchwork. Retornando dict com "
            "'rr' e 'uncertainty'."
        ),
    },
    "en": {
        "rr_title": "Smoothed Relative Risk (BYM/CAR)",
        "rr_fill": "RR",
        "unc_title": "Uncertainty: 95% Credible Interval Width",
        "unc_fill": "CI Width",
        "coef_title": "Fixed Effects: Posterior Mean (95% CI)",
        "coef_x": "Posterior Mean (95% CI)",
        "coef_y": "Covariate",
        "both_title": "Bayesian Spatial Disease Mapping",
        "sig_note": "Black outline: lower 95% CI > 1 (significantly elevated risk)",
        "err_not_bayes": (
            "'x' must be the output of sus_mod_spatial_bayes() (keys 'rr' and 'fixed')."
        ),
        "err_rr_cols": "'x[\"rr\"]' must contain the columns: {cols}.",
        "err_not_sf": (
            "'municipalities' must be a geopandas.GeoDataFrame with a "
            "'code_muni' column."
        ),
        "err_no_fixed": (
            "No fixed effects found in x['fixed']. Fit the model with covariates."
        ),
        "err_type_needs_sf": (
            "type='{type}' requires the 'municipalities' argument (GeoDataFrame)."
        ),
        "err_bad_type": "type='{type}' is invalid. Use one of: {valid}.",
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
        "warn_patchwork": (
            "plotnine has no patchwork equivalent. Returning a dict with "
            "'rr' and 'uncertainty'."
        ),
    },
    "es": {
        "rr_title": "Riesgo Relativo Suavizado (BYM/CAR)",
        "rr_fill": "RR",
        "unc_title": "Incertidumbre: Amplitud del Intervalo de Credibilidad (95%)",
        "unc_fill": "Amplitud IC",
        "coef_title": "Efectos Fijos: Media Posterior (IC 95%)",
        "coef_x": "Media Posterior (IC 95%)",
        "coef_y": "Covariable",
        "both_title": "Mapeo Bayesiano Espacial de Enfermedades",
        "sig_note": (
            "Contorno negro: IC95% inferior > 1 (riesgo significativamente elevado)"
        ),
        "err_not_bayes": (
            "'x' debe ser la salida de sus_mod_spatial_bayes() (claves 'rr' y 'fixed')."
        ),
        "err_rr_cols": "'x[\"rr\"]' debe contener las columnas: {cols}.",
        "err_not_sf": (
            "'municipalities' debe ser un geopandas.GeoDataFrame con la columna "
            "'code_muni'."
        ),
        "err_no_fixed": (
            "No se encontraron efectos fijos en x['fixed']. Ajuste el modelo con "
            "covariables."
        ),
        "err_type_needs_sf": (
            "type='{type}' requiere el argumento 'municipalities' (GeoDataFrame)."
        ),
        "err_bad_type": "type='{type}' no es valido. Use uno de: {valid}.",
        "warn_lang": "Idioma '{lang}' no admitido. Usando 'pt'.",
        "warn_patchwork": (
            "plotnine no tiene equivalente a patchwork. Retornando dict con "
            "'rr' y 'uncertainty'."
        ),
    },
}


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_spatial_bayes requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _map_theme(base_size: float) -> Any:
    """R's ``.bayes_map_theme()``."""
    from plotnine import element_text, theme, theme_void

    return theme_void(base_size=base_size) + theme(
        legend_position="right",
        plot_title=element_text(ha="center", face="bold", size=base_size + 1),
        plot_caption=element_text(ha="left", size=base_size - 2, color=_CAPTION),
    )


def _merge_rr(x: dict[str, Any], municipalities: Any) -> Any:
    rr = pd.DataFrame(x["rr"]).copy()
    rr["code_muni"] = rr["code_muni"].astype(str)
    munis = municipalities.copy()
    munis["code_muni"] = munis["code_muni"].astype(str)
    return munis.merge(rr, on="code_muni", how="left")


def _plot_rr(
    x: dict[str, Any], municipalities: Any, title: str | None, lang: str,
    base_size: float,
) -> Any:
    from plotnine import aes, geom_map, ggplot, labs, scale_fill_gradient2

    strings = _I18N[lang]
    merged = _merge_rr(x, municipalities)
    # R flags only ELEVATED risk: lower bound above 1. Municipalities whose
    # whole interval sits below 1 - significantly *reduced* risk - get no
    # outline. The caption states the rule, so this is by design, not an
    # oversight; it is noted here because the map reads as "significant"
    # when it means "significantly high".
    merged["sig_elevated"] = merged["rr_lower95"] > 1

    p = ggplot(merged) + geom_map(aes(fill="rr_mean"), color=_BORDER, size=0.15)
    flagged = merged[merged["sig_elevated"].fillna(False)]
    if len(flagged) > 0:
        p = p + geom_map(flagged, fill=None, color=_OUTLINE, size=0.6)
    return (
        p
        + scale_fill_gradient2(
            low=_LOW, mid=_MID, high=_HIGH, midpoint=1,
            na_value=_NA_FILL, name=strings["rr_fill"],
        )
        + labs(
            title=title if title is not None else strings["rr_title"],
            caption=strings["sig_note"],
        )
        + _map_theme(base_size)
    )


def _plot_uncertainty(
    x: dict[str, Any], municipalities: Any, title: str | None, lang: str,
    base_size: float,
) -> Any:
    from plotnine import aes, geom_map, ggplot, labs, scale_fill_cmap

    strings = _I18N[lang]
    merged = _merge_rr(x, municipalities)
    merged["ci_width"] = merged["rr_upper95"] - merged["rr_lower95"]

    return (
        ggplot(merged)
        + geom_map(aes(fill="ci_width"), color=_BORDER, size=0.15)
        + scale_fill_cmap(
            cmap_name="plasma", na_value=_NA_FILL, name=strings["unc_fill"]
        )
        + labs(title=title if title is not None else strings["unc_title"])
        + _map_theme(base_size)
    )


def _plot_coef(
    x: dict[str, Any], title: str | None, lang: str, base_size: float
) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_errorbarh,
        geom_point,
        geom_vline,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    strings = _I18N[lang]
    fixed = x.get("fixed")
    if fixed is None or len(fixed) == 0:
        raise ValueError(strings["err_no_fixed"])
    fixed = pd.DataFrame(fixed).copy()
    missing = [c for c in _REQUIRED_FIXED if c not in fixed.columns]
    if missing:
        raise ValueError(strings["err_no_fixed"])

    # R: reorder(term, mean) - terms ordered by posterior mean, ascending.
    order = fixed.sort_values("mean", kind="stable")["term"].tolist()
    fixed["term"] = pd.Categorical(fixed["term"], categories=order, ordered=True)

    # R draws a horizontal geom_pointrange. plotnine's geom_pointrange is
    # vertical only (it requires ymin/ymax), so the same figure is built
    # from geom_errorbarh + geom_point -- the convention already used by
    # mod_plot_pool's forest panel. height=0 drops the end caps, which a
    # pointrange does not have. R's linewidth=0.7 maps to the bar size;
    # its size=0.55 is scaled by ggplot2's fatten=4 for the point.
    return (
        ggplot(fixed, aes(x="mean", y="term"))
        + geom_vline(xintercept=0, linetype="dashed", color=_ZERO_LINE)
        + geom_errorbarh(
            aes(xmin="lower95", xmax="upper95"),
            height=0, size=0.7, color=_POINTRANGE,
        )
        + geom_point(color=_POINTRANGE, size=0.55 * 4)
        + labs(
            title=title if title is not None else strings["coef_title"],
            x=strings["coef_x"],
            y=strings["coef_y"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(ha="center", face="bold"),
            panel_grid_major_y=element_blank(),
        )
    )


def sus_mod_plot_spatial_bayes(
    x: dict[str, Any],
    municipalities: Any = None,
    type: Literal["rr", "uncertainty", "coef", "both"] = "rr",
    title: str | None = None,
    base_size: float = 12,
    lang: str = "pt",
    **kwargs: Any,
) -> Any:
    """Maps and coefficient plot for a Bayesian CAR/BYM disease-mapping fit.

    Panel types (``type``):
        - ``"rr"`` (default): choropleth of the smoothed relative risk on
          a navy-white-red diverging scale centred at ``1``, so that
          municipalities at the null risk stay white. Municipalities
          whose lower 95% credible bound exceeds 1 are outlined in
          black.
        - ``"uncertainty"``: choropleth of the width of the 95% credible
          interval (``rr_upper95 - rr_lower95``) on the plasma scale -
          where the posterior is least certain.
        - ``"coef"``: point-and-interval plot of the fixed effects,
          ordered by posterior mean, with a dashed reference at zero.

          Be aware of what R puts in that table. ``sus_mod_spatial_bayes()``
          selects the fixed effects by pattern-matching CARBayes's row
          names, and on a real fit the match misses at both ends: the
          intercept is dropped (the pattern demands a leading letter and
          CARBayes writes ``(Intercept)``) while ``sigma2`` is kept (the
          exclusion list spells ``Sigma`` and the match is
          case-sensitive). So the panel tends to show a variance
          component -- necessarily positive, so it can never cross the
          zero reference -- next to a log rate ratio, and no intercept.
          Recorded as **M88**; this function plots what it is given.
        - ``"both"``: the two maps. R combines them side by side with
          **patchwork** when available; plotnine has no maintained
          patchwork equivalent, so this port always returns the named
          ``dict`` that R falls back to - the same documented gap
          already carried by ``sus_mod_plot_spatial_moran``.

    One-sided significance:
        The outline marks *elevated* risk only (lower bound above 1).
        A municipality whose whole interval sits below 1 - significantly
        **reduced** risk - is drawn plain. R does the same and its
        caption says so; the note is repeated here because "significant"
        on this map means "significantly high", not "significantly
        different".

    Args:
        x: A ``dict`` shaped like R's ``climasus_spatial_bayes`` object.
            ``sus_mod_spatial_bayes()`` is still a stub in this package,
            so until a Python fitter exists this is produced in R and
            carried over. The slots this function reads:

            - ``"rr"``: ``pandas.DataFrame`` with columns
              ``"code_muni"``, ``"rr_mean"``, ``"rr_lower95"``,
              ``"rr_upper95"`` - one row per municipality. Required for
              ``type`` in ``("rr", "uncertainty", "both")``.
            - ``"fixed"``: ``pandas.DataFrame`` with columns ``"term"``,
              ``"mean"``, ``"lower95"``, ``"upper95"`` (and optionally
              ``"sd"``) - one row per fixed effect. Required for
              ``type="coef"``.

            The full R object also carries ``"random"``, ``"fitted"``,
            ``"dic"``, ``"model"``, ``"family"`` and
            ``"n_iter_effective"``; none is read here, and R's ``$call``
            has no Python analog.
        municipalities: A ``geopandas.GeoDataFrame`` with polygon
            geometry and a ``code_muni`` column. Required for every
            *type* except ``"coef"``.
        type: Which view to draw. One of ``"rr"`` (default),
            ``"uncertainty"``, ``"coef"``, ``"both"``.
        title: Custom title, or ``None`` for the default. Ignored for
            ``type="both"``, where R passes it to patchwork's annotation
            and this port has no combined figure to annotate.
        base_size: Base font size passed to the theme. Defaults to ``12``.
        lang: Label language: ``"pt"`` (default), ``"en"`` or ``"es"``.
            An unrecognised value warns and falls back to ``"pt"``.
        **kwargs: Accepted and ignored, mirroring R's ``...``.

    Returns:
        A ``plotnine.ggplot`` for ``type`` in
        ``("rr", "uncertainty", "coef")``; for ``type="both"``, a
        ``dict`` with keys ``"rr"`` and ``"uncertainty"``.

    Raises:
        TypeError: If *x* is not a fit-shaped ``dict``, or
            *municipalities* is not a ``geopandas.GeoDataFrame``.
        ValueError: If *type* is invalid, *municipalities* is missing
            for a map type, required columns are absent, or
            ``type="coef"`` is asked of a fit with no fixed effects.
        ImportError: If ``plotnine`` or ``geopandas`` is not installed.

    Examples::

        import climasus4py as cs

        cs.sus_mod_plot_spatial_bayes(fit, malha, type="rr").draw()
        cs.sus_mod_plot_spatial_bayes(fit, type="coef").draw()
    """
    if lang not in _I18N:
        warnings.warn(
            _I18N["pt"]["warn_lang"].format(lang=lang), UserWarning, stacklevel=2
        )
        lang = "pt"
    strings = _I18N[lang]

    if type not in VALID_TYPES:
        raise ValueError(
            strings["err_bad_type"].format(type=type, valid=", ".join(VALID_TYPES))
        )

    if not isinstance(x, dict) or ("rr" not in x and "fixed" not in x):
        raise TypeError(strings["err_not_bayes"])

    _require_plotnine()

    needs_geo = type in ("rr", "uncertainty", "both")
    if needs_geo:
        try:
            import geopandas as gpd
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise ImportError(
                "sus_mod_plot_spatial_bayes requires geopandas for map types. "
                "Install with: pip install geopandas"
            ) from exc

        if municipalities is None:
            raise ValueError(strings["err_type_needs_sf"].format(type=type))
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise TypeError(strings["err_not_sf"])
        if "code_muni" not in municipalities.columns:
            raise ValueError(strings["err_not_sf"])

        rr = x.get("rr")
        if rr is None:
            raise TypeError(strings["err_not_bayes"])
        missing = [c for c in _REQUIRED_RR if c not in pd.DataFrame(rr).columns]
        if missing:
            raise ValueError(
                strings["err_rr_cols"].format(cols=", ".join(_REQUIRED_RR))
            )

    if type == "rr":
        return _plot_rr(x, municipalities, title, lang, base_size)
    if type == "uncertainty":
        return _plot_uncertainty(x, municipalities, title, lang, base_size)
    if type == "coef":
        return _plot_coef(x, title, lang, base_size)

    warnings.warn(strings["warn_patchwork"], UserWarning, stacklevel=2)
    return {
        "rr": _plot_rr(x, municipalities, None, lang, base_size),
        "uncertainty": _plot_uncertainty(x, municipalities, None, lang, base_size),
    }
