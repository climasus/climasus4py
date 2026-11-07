"""Scientific visualisations and statistical tables for a DLNM fit.

Mirrors R: sus_mod_plot_dlnm.R

Theory: Gasparrini et al. (2010, 2011, 2014); Armstrong (2006); Bhaskaran
et al. (2013).

Visualises the dict returned by ``sus_mod_dlnm()`` (the Python analogue
of R's ``climasus_dlnm`` S3 object — see ``enrichment/mod_dlnm.py`` for
its exact shape). Seven plot types, mirroring the R helpers:
``"overall"``, ``"lag"``, ``"surface"``, ``"contour"``, ``"slice"``,
``"distribution"``, ``"series"``.

Not lazy — operates on the arrays/tables already computed by
``sus_mod_dlnm()``; there is no DuckDB relation involved.

Deliberately narrower than the R source:
  - ``interactive=True`` (plotly / 3-D surface) is **not supported** —
    ``plotly`` is not bundled with climasus4py, same precedent as
    ``sus_mod_plot_burden()``; raises ``ImportError``. The static
    ``"surface"`` request falls back to the ``"contour"`` rendering,
    matching R's own static fallback for this type.
  - ``"overall"`` renders the exposure-response curve only (no
    ``patchwork``-stacked exposure histogram sub-panel below it —
    ``plotnine`` has no direct ``patchwork`` equivalent). See
    IDEIAS.md.
  - ``color_palette`` accepts a small built-in palette name (default
    ``"default"``) instead of a ``ggsci`` palette name — ``ggsci`` is
    an R-only dependency.
  - ``output_type="table"``/``"all"`` returns a dict of plain
    ``pandas.DataFrame`` (mirroring R's own gt-unavailable tibble
    fallback path), not a ``gt`` table — ``gt`` has no Python
    equivalent bundled here.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> fit = cs.sus_mod_dlnm(df, outcome_col="n_obitos")
    >>> p = cs.sus_mod_plot_dlnm(fit, type="overall", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

from typing import Any, Literal

import numpy as np
import pandas as pd

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "overall_title": "Curva Exposição-Resposta Cumulativa",
        "overall_sub": "Efeito cumulativo da exposição climática sobre o desfecho diário",
        "x_exposure": "Exposição",
        "y_rr": "Risco Relativo (IC 95%)",
        "y_count": "Contagem",
        "lag_title": "Curva Lag-Resposta",
        "lag_sub": "Efeito por tempo de lag (exposição no percentil 75)",
        "x_lag": "Dias de Atraso (lag)",
        "rr_specific": "RR Específico por Lag",
        "rr_cumulative": "RR Cumulativo",
        "surface_title": "Superfície de Resposta DLNM",
        "surface_sub": "Efeito bidimensional exposição × lag",
        "contour_title": "Mapa de Contorno DLNM",
        "z_rr_log": "log(RR)",
        "slice_title": "Curvas de Resposta por Lag Específico",
        "slice_sub": "Perfil dose-resposta em cada tempo de lag",
        "lag_group": "Lag (dias)",
        "dist_title": "Distribuição da Exposição Climática",
        "dist_sub": "Histograma com percentis-chave",
        "series_title": "Série Temporal: Desfecho e Exposição",
        "series_sub": "Contagem diária de eventos e exposição climática (normalizada)",
        "outcome_label": "Desfecho (contagem diária)",
        "exposure_label": "Exposição (lag 0, normalizada)",
        "err_not_dlnm": "'fit' deve ser o dict retornado por sus_mod_dlnm().",
        "err_interactive": (
            "interactive=True requer a dependência opcional 'plotly', que "
            "climasus4py não empacota atualmente. Instale plotly manualmente "
            "se necessário; ver IDEIAS.md."
        ),
    },
    "en": {
        "overall_title": "Cumulative Exposure-Response Curve",
        "overall_sub": "Cumulative effect of climate exposure on daily health outcome",
        "x_exposure": "Exposure",
        "y_rr": "Relative Risk (95% CI)",
        "y_count": "Count",
        "lag_title": "Lag-Response Curve",
        "lag_sub": "Effect by lag time (exposure at 75th percentile)",
        "x_lag": "Lag (days)",
        "rr_specific": "Lag-Specific RR",
        "rr_cumulative": "Cumulative RR",
        "surface_title": "DLNM Response Surface",
        "surface_sub": "Bidimensional exposure x lag effect",
        "contour_title": "DLNM Contour Map",
        "z_rr_log": "log(RR)",
        "slice_title": "Lag-Specific Exposure-Response Curves",
        "slice_sub": "Dose-response profile at each lag time",
        "lag_group": "Lag (days)",
        "dist_title": "Climate Exposure Distribution",
        "dist_sub": "Histogram with key percentiles",
        "series_title": "Time Series: Outcome and Exposure",
        "series_sub": "Daily event count and climate exposure (normalized)",
        "outcome_label": "Outcome (daily count)",
        "exposure_label": "Exposure (lag 0, normalized)",
        "err_not_dlnm": "'fit' must be the dict returned by sus_mod_dlnm().",
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle. Install plotly manually if "
            "needed; see IDEIAS.md."
        ),
    },
    "es": {
        "overall_title": "Curva Acumulada Exposición-Respuesta",
        "overall_sub": "Efecto acumulado de la exposición climática sobre el resultado diario",
        "x_exposure": "Exposición",
        "y_rr": "Riesgo Relativo (IC 95%)",
        "y_count": "Recuento",
        "lag_title": "Curva Lag-Respuesta",
        "lag_sub": "Efecto por tiempo de lag (exposición en percentil 75)",
        "x_lag": "Días de Retraso (lag)",
        "rr_specific": "RR Específico por Lag",
        "rr_cumulative": "RR Acumulado",
        "surface_title": "Superficie de Respuesta DLNM",
        "surface_sub": "Efecto bidimensional exposición x lag",
        "contour_title": "Mapa de Contorno DLNM",
        "z_rr_log": "log(RR)",
        "slice_title": "Curvas de Respuesta por Lag Específico",
        "slice_sub": "Perfil dosis-respuesta en cada tiempo de lag",
        "lag_group": "Lag (días)",
        "dist_title": "Distribución de la Exposición Climática",
        "dist_sub": "Histograma con percentiles clave",
        "series_title": "Serie Temporal: Resultado y Exposición",
        "series_sub": "Recuento diario de eventos y exposición climática (normalizada)",
        "outcome_label": "Resultado (recuento diario)",
        "exposure_label": "Exposición (lag 0, normalizada)",
        "err_not_dlnm": "'fit' debe ser el dict retornado por sus_mod_dlnm().",
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye actualmente. Instale plotly manualmente "
            "si es necesario; ver IDEIAS.md."
        ),
    },
}

_PALETTES: dict[str, dict[str, str]] = {
    "default": {"main": "#E64B35", "second": "#4DBBD5", "ref": "#E74C3C", "cold": "#3C5488", "hot": "#E64B35"},
}


def _palette(name: str) -> dict[str, str]:
    return _PALETTES.get(name, _PALETTES["default"])


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_dlnm requires plotnine. Install with: pip install climasus4py[plot]"
        ) from exc


def _plot_overall(fit: dict[str, Any], pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_hline,
        geom_line,
        geom_point,
        geom_ribbon,
        geom_vline,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    pred = fit["pred"]
    df_curve = pd.DataFrame(
        {
            "exposure": np.asarray(pred["predvar"], dtype=float),
            "rr": np.asarray(pred["allRRfit"], dtype=float),
            "lo": np.asarray(pred["allRRlow"], dtype=float),
            "hi": np.asarray(pred["allRRhigh"], dtype=float),
        }
    )
    er = fit["exposure_response"]
    ref_val = fit["meta"]["ref_value"]
    var_lbl = fit["meta"]["climate_col"]

    p = (
        ggplot(df_curve, aes(x="exposure"))
        + geom_ribbon(aes(ymin="lo", ymax="hi"), fill=pal["main"], alpha=0.15)
        + geom_line(aes(y="rr"), color=pal["main"], size=1)
        + geom_hline(yintercept=1, linetype="dashed", color=pal["ref"])
        + geom_vline(xintercept=ref_val, linetype="dotted", color="#555555")
        + geom_point(data=er, mapping=aes(x="exposure", y="rr"), color=pal["second"], size=2.5, alpha=0.9)
        + labs(
            title=strings["overall_title"],
            subtitle=strings["overall_sub"],
            x=var_lbl,
            y=strings["y_rr"],
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"))
    )
    return p


def _plot_lag(fit: dict[str, Any], exposure_at: float | None, pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_hline,
        geom_line,
        ggplot,
        labs,
        scale_color_manual,
        theme,
        theme_bw,
    )

    pred = fit["pred"]
    lag_seq = np.arange(int(pred["lag"][0]), int(pred["lag"][1]) + 1)
    lag0_col = f"{fit['meta']['climate_col']}_lag0"
    expo = fit["data_daily"][lag0_col].to_numpy(dtype=float)
    p75_val = float(np.quantile(expo, 0.75))
    focal_exp = p75_val if exposure_at is None else float(exposure_at)
    predvar = np.asarray(pred["predvar"], dtype=float)
    focal_idx = int(np.argmin(np.abs(predvar - focal_exp)))

    lbl_spec, lbl_cum = strings["rr_specific"], strings["rr_cumulative"]
    df_lag = pd.DataFrame(
        {
            "lag": lag_seq,
            "rr": np.asarray(pred["matRRfit"])[focal_idx, :],
            "series": lbl_spec,
        }
    )
    df_cum = fit["lag_response"][["lag", "rr_cum"]].rename(columns={"rr_cum": "rr"})
    df_cum["series"] = lbl_cum
    df_all = pd.concat([df_lag, df_cum], ignore_index=True)

    p = (
        ggplot(df_all, aes(x="lag", y="rr", color="series"))
        + geom_line(size=1)
        + geom_hline(yintercept=1, linetype="dotted", color=pal["ref"])
        + scale_color_manual(values={lbl_spec: pal["main"], lbl_cum: pal["second"]}, name="")
        + labs(
            title=strings["lag_title"],
            subtitle=f"{strings['lag_sub']} = {round(focal_exp, 1)}",
            x=strings["x_lag"],
            y=strings["y_rr"],
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"), legend_position="bottom")
    )
    return p


def _plot_contour(fit: dict[str, Any], pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import aes, element_text, geom_tile, ggplot, labs, scale_fill_gradient2, theme, theme_bw

    pred = fit["pred"]
    lag_seq = np.arange(int(pred["lag"][0]), int(pred["lag"][1]) + 1)
    exp_vec = np.asarray(pred["predvar"], dtype=float)
    rr_mat = np.asarray(pred["matRRfit"], dtype=float)
    n_exp, n_lag = len(exp_vec), len(lag_seq)

    df_cont = pd.DataFrame(
        {
            "lag_val": np.repeat(lag_seq, n_exp),
            "exp_val": np.tile(exp_vec, n_lag),
            "log_rr": np.log(rr_mat).T.reshape(-1),
        }
    )

    p = (
        ggplot(df_cont, aes(x="lag_val", y="exp_val", fill="log_rr"))
        + geom_tile()
        + scale_fill_gradient2(low=pal["cold"], mid="white", high=pal["hot"], midpoint=0, name=strings["z_rr_log"])
        + labs(
            title=strings["contour_title"],
            subtitle=strings["surface_sub"],
            x=strings["x_lag"],
            y=fit["meta"]["climate_col"],
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"))
    )
    return p


def _plot_slice(fit: dict[str, Any], lags_at: list[int], pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_hline,
        geom_line,
        geom_ribbon,
        geom_vline,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    pred = fit["pred"]
    lag_seq = np.arange(int(pred["lag"][0]), int(pred["lag"][1]) + 1)
    selected = sorted(set(int(x) for x in lags_at) & set(lag_seq.tolist()))
    if not selected:
        selected = np.linspace(lag_seq[0], lag_seq[-1], 5, dtype=int).tolist()

    exp_vec = np.asarray(pred["predvar"], dtype=float)
    rows = []
    for lag_val in selected:
        lag_idx = int(np.where(lag_seq == lag_val)[0][0])
        rows.append(
            pd.DataFrame(
                {
                    "exposure": exp_vec,
                    "lag_label": f"Lag {lag_val}",
                    "rr": np.asarray(pred["matRRfit"])[:, lag_idx],
                    "lo": np.asarray(pred["matRRlow"])[:, lag_idx],
                    "hi": np.asarray(pred["matRRhigh"])[:, lag_idx],
                }
            )
        )
    df_slice = pd.concat(rows, ignore_index=True)
    order = [f"Lag {v}" for v in selected]
    df_slice["lag_label"] = pd.Categorical(df_slice["lag_label"], categories=order, ordered=True)

    p = (
        ggplot(df_slice, aes(x="exposure", color="lag_label", fill="lag_label"))
        + geom_ribbon(aes(ymin="lo", ymax="hi"), alpha=0.10)
        + geom_line(aes(y="rr"), size=0.85)
        + geom_hline(yintercept=1, linetype="dashed", color=pal["ref"])
        + geom_vline(xintercept=fit["meta"]["ref_value"], linetype="dotted", color="#555555")
        + labs(
            title=strings["slice_title"],
            subtitle=strings["slice_sub"],
            x=fit["meta"]["climate_col"],
            y=strings["y_rr"],
            color=strings["lag_group"],
            fill=strings["lag_group"],
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"))
    )
    return p


def _plot_distribution(fit: dict[str, Any], pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_histogram,
        geom_vline,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    lag0_col = f"{fit['meta']['climate_col']}_lag0"
    df_dist = pd.DataFrame({"exposure": fit["data_daily"][lag0_col].to_numpy(dtype=float)})
    ref_val = fit["meta"]["ref_value"]

    p = (
        ggplot(df_dist, aes(x="exposure"))
        + geom_histogram(bins=35, fill=pal["main"], alpha=0.45, color="white")
        + geom_vline(xintercept=ref_val, linetype="dashed", color=pal["ref"])
        + labs(
            title=strings["dist_title"],
            subtitle=strings["dist_sub"],
            x=fit["meta"]["climate_col"],
            y=strings["y_count"],
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"))
    )
    return p


def _plot_series(fit: dict[str, Any], pal: dict[str, str], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_col,
        geom_line,
        ggplot,
        labs,
        scale_color_manual,
        theme,
        theme_bw,
    )

    lag0_col = f"{fit['meta']['climate_col']}_lag0"
    df_ts = fit["data_daily"][["date", "y", lag0_col]].sort_values("date").reset_index(drop=True)

    y_vals = df_ts["y"].to_numpy(dtype=float)
    exp_vals = df_ts[lag0_col].to_numpy(dtype=float)
    y_min, y_max = float(np.min(y_vals)), float(np.max(y_vals))
    e_min, e_max = float(np.min(exp_vals)), float(np.max(exp_vals))
    e_range = e_max - e_min if e_max > e_min else 1.0
    df_ts["exposure_scaled"] = (exp_vals - e_min) / e_range * (y_max - y_min) + y_min

    lbl_out, lbl_exp = strings["outcome_label"], strings["exposure_label"]
    df_long = pd.concat(
        [
            pd.DataFrame({"date": df_ts["date"], "value": df_ts["y"], "series": lbl_out}),
            pd.DataFrame({"date": df_ts["date"], "value": df_ts["exposure_scaled"], "series": lbl_exp}),
        ],
        ignore_index=True,
    )

    p = (
        ggplot(df_long, aes(x="date", y="value", color="series"))
        + geom_col(
            data=df_long[df_long["series"] == lbl_out],
            mapping=aes(x="date", y="value"),
            fill=pal["main"],
            alpha=0.55,
            inherit_aes=False,
        )
        + geom_line(size=0.6)
        + scale_color_manual(values={lbl_out: pal["main"], lbl_exp: pal["second"]}, name="")
        + labs(
            title=strings["series_title"],
            subtitle=strings["series_sub"],
            x="",
            y=f"{lbl_out} / {lbl_exp}",
        )
        + theme_bw(base_size=base_size)
        + theme(plot_title=element_text(face="bold"), plot_subtitle=element_text(color="gray"), legend_position="bottom")
    )
    return p


def _table_summary(fit: dict[str, Any], pred_at: tuple[float, ...]) -> dict[str, pd.DataFrame]:
    meta = fit["meta"]
    diag = fit["diagnostics"]
    pred = fit["pred"]
    lag0_col = f"{meta['climate_col']}_lag0"
    expo = fit["data_daily"][lag0_col].to_numpy(dtype=float)
    predvar = np.asarray(pred["predvar"], dtype=float)

    expo_vals = np.quantile(expo, pred_at)
    rows = []
    for pct, val in zip(pred_at, expo_vals, strict=True):
        idx = int(np.argmin(np.abs(predvar - val)))
        rows.append(
            {
                "percentile": f"P{round(pct * 100)}",
                "exposure": round(float(val), 2),
                "rr": round(float(pred["allRRfit"][idx]), 4),
                "lo": round(float(pred["allRRlow"][idx]), 4),
                "hi": round(float(pred["allRRhigh"][idx]), 4),
            }
        )
    er_tbl = pd.DataFrame(rows)

    lag_seq = np.arange(int(pred["lag"][0]), int(pred["lag"][1]) + 1)
    p75_idx = int(np.argmin(np.abs(predvar - np.quantile(expo, 0.75))))
    lag_abs_rr = np.abs(np.log(np.asarray(pred["matRRfit"])[p75_idx, :]))
    lag_peak = int(lag_seq[int(np.argmax(lag_abs_rr))])

    model_spec = pd.DataFrame(
        {
            "parameter": [
                "climate_col", "outcome_col", "family", "lag_max",
                "ref_value", "ns_df", "n",
            ],
            "value": [
                meta["climate_col"], meta["outcome_col"], meta["family"],
                str(meta["lag_max"]), str(round(meta["ref_value"], 3)),
                str(meta.get("ns_df", "auto")), str(meta["n"]),
            ],
        }
    )

    diagnostics = pd.DataFrame(
        {
            "indicator": ["disp_ratio", "disp_category", "autocorr_pval", "has_autocorr", "aic_poisson", "lag_peak"],
            "value": [
                str(round(diag["disp_ratio"], 3)),
                diag["disp_category"],
                str(round(diag["autocorr_pval"], 4)),
                str(bool(diag["has_autocorr"])),
                "NA" if diag["aic_poisson"] is None or np.isnan(diag["aic_poisson"]) else str(round(diag["aic_poisson"], 1)),
                str(lag_peak),
            ],
        }
    )

    return {"model_spec": model_spec, "exposure_response": er_tbl, "diagnostics": diagnostics}


def sus_mod_plot_dlnm(
    fit: dict[str, Any],
    type: Literal["overall", "lag", "surface", "contour", "slice", "distribution", "series"] = "overall",
    output_type: Literal["plot", "table", "all"] = "plot",
    exposure_at: float | None = None,
    lags_at: tuple[int, ...] = (0, 3, 7, 14, 21),
    pred_at: tuple[float, ...] = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99),
    interactive: bool = False,
    color_palette: str = "default",
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, dict[str, pd.DataFrame], or dict, depending on output_type
    """Scientific plots and tables from a DLNM fit.

    Produces publication-style visualisations and statistical summary
    tables from the dict returned by ``sus_mod_dlnm()`` — the Python
    analogue of R's ``climasus_dlnm`` object. Mirrors
    ``climasus4r::sus_mod_plot_dlnm()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Plot types (*type*):
        * ``"overall"``: Cumulative exposure-response curve with CI
          (default).
        * ``"lag"``: Lag-specific and cumulative lag-response at a
          given exposure.
        * ``"surface"``: Static fallback renders the same as
          ``"contour"`` (no bundled 3-D/plotly backend — see module
          docstring).
        * ``"contour"``: 2-D heat map of exposure x lag, log(RR) scale.
        * ``"slice"``: Exposure-response curves at specific lag times.
        * ``"distribution"``: Exposure histogram with the reference
          value marked.
        * ``"series"``: Daily outcome count + normalized exposure time
          series.

    Args:
        fit: Dict returned by ``sus_mod_dlnm()``.
        type: Plot type (see **Plot types** above). Default ``"overall"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``.
        exposure_at: Exposure value used as the focal point for the
            ``"lag"`` plot. ``None`` (default) uses the 75th percentile
            of lag-0 exposure.
        lags_at: Lag times shown in the ``"slice"`` plot, clipped to
            the fit's ``lag_max``.
        pred_at: Quantile probabilities for the ``"table"`` summary.
        interactive: **Not currently supported** — raises
            ``ImportError`` because ``plotly`` is not a climasus4py
            dependency. See module docstring / IDEIAS.md.
        color_palette: Built-in palette name. Only ``"default"`` is
            currently defined (``ggsci`` palettes have no Python
            equivalent bundled here).
        base_size: ``plotnine`` base font size. Default ``12``.
        save_plot: File path to save the plot, or ``None`` (default).
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``False``.

    Returns:
        Depending on *output_type*:

        - ``"plot"``: a ``plotnine.ggplot`` object.
        - ``"table"``: a dict of ``pandas.DataFrame`` with keys
          ``"model_spec"``, ``"exposure_response"``, ``"diagnostics"``.
        - ``"all"``: a dict with ``"plot"``, ``"table"``, ``"data"``
          keys (``"data"`` holds ``exposure_response``, ``lag_response``,
          ``data_daily``, ``diagnostics`` from *fit*).

    Raises:
        ImportError: If ``plotnine`` is not installed, or if
            ``interactive=True``.
        TypeError: If *fit* is not a dict with the expected
            ``sus_mod_dlnm()`` keys.

    Examples::

        import climasus4py as cs

        fit = cs.sus_mod_dlnm(df, outcome_col="n_obitos", lag_max=14)

        cs.sus_mod_plot_dlnm(fit, type="overall", lang="pt")
        cs.sus_mod_plot_dlnm(fit, type="lag", lang="en")
        cs.sus_mod_plot_dlnm(fit, type="contour", lang="es")
        out = cs.sus_mod_plot_dlnm(fit, output_type="all")
        out["table"]["exposure_response"]
    """
    if lang not in _I18N:
        lang = "pt"
    strings = _I18N[lang]

    valid_types = ("overall", "lag", "surface", "contour", "slice", "distribution", "series")
    if type not in valid_types:
        raise ValueError(f"type must be one of {valid_types}; got {type!r}.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}.")

    if not isinstance(fit, dict) or not {"pred", "meta", "data_daily", "exposure_response", "lag_response", "diagnostics"}.issubset(fit.keys()):
        raise TypeError(strings["err_not_dlnm"])

    if interactive:
        raise ImportError(strings["err_interactive"])

    _require_plotnine()

    if verbose:
        print(f"climasus4py — DLNM Visualisation (type: {type})")

    pal = _palette(color_palette)

    p = None
    if output_type in ("plot", "all"):
        if type == "overall":
            p = _plot_overall(fit, pal, strings, base_size)
        elif type == "lag":
            p = _plot_lag(fit, exposure_at, pal, strings, base_size)
        elif type in ("surface", "contour"):
            p = _plot_contour(fit, pal, strings, base_size)
        elif type == "slice":
            p = _plot_slice(fit, list(lags_at), pal, strings, base_size)
        elif type == "distribution":
            p = _plot_distribution(fit, pal, strings, base_size)
        else:  # series
            p = _plot_series(fit, pal, strings, base_size)

    tbl = None
    if output_type in ("table", "all"):
        tbl = _table_summary(fit, pred_at)

    if save_plot is not None and p is not None:
        p.save(save_plot, width=10, height=7, dpi=300)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {
        "plot": p,
        "table": tbl,
        "data": {
            "exposure_response": fit["exposure_response"],
            "lag_response": fit["lag_response"],
            "data_daily": fit["data_daily"],
            "diagnostics": fit["diagnostics"],
        },
    }
