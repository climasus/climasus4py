"""Plots and tables from a multi-stratum sensitivity analysis.

Mirrors R: sus_mod_plot_sensitivity.R

Visualises the ``dict`` returned by ``sus_mod_sensitivity()`` (the Python
analogue of the R function's ``climasus_sensitivity`` S3 object — see
``enrichment/mod_sensitivity.py`` for its exact shape). Three plot types
are supported, mirroring the R helpers one-to-one:
  - ``"curves"``  (``_sns_plot_curves``)  — overlay of all strata
    exposure-response curves from ``x["stratum_curves"]``.
  - ``"scatter"`` (``_sns_plot_scatter``) — hot_rr vs cold_rr per stratum
    from ``x["comparison"]`` (point size encodes sensitivity index).
  - ``"bar"``     (``_sns_plot_bar``)     — horizontal forest of hot/cold
    RR per stratum from ``x["rr_table"]``.

Not lazy — operates on the ``pd.DataFrame`` tables already materialised by
``sus_mod_sensitivity()``; there is no DuckDB relation involved.

Deliberately narrower than the R source: ``interactive=True`` would return
a ``plotly`` interactive version (mirroring R's ``plotly::ggplotly()``
path). **Not currently supported** — ``plotly`` is not bundled with
climasus4py; raises ``ImportError``, matching the precedent set by
``sus_mod_plot_burden()``. ``save_plot`` *is* implemented (via
``plotnine``'s own ``.save()``), since it needs no new dependency.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> sens = cs.sus_mod_sensitivity(fits, lang="pt")
    >>> p = cs.sus_mod_plot_sensitivity(sens, type="curves", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .sns_plot_labels)
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "curves_title": "Curvas Exposição-Resposta por Estrato",
        "scatter_title": "RR Calor vs. RR Frio por Estrato",
        "bar_title": "Estimativas de RR por Estrato (Calor e Frio)",
        "x_exposure": "Exposição",
        "y_rr": "RR (IC 95%)",
        "x_cold_rr": "RR Frio",
        "y_hot_rr": "RR Calor",
        "x_rr": "RR (IC 95%)",
        "y_stratum": "Estrato",
        "strata": "estratos",
        "comp_hot": "Calor",
        "comp_cold": "Frio",
        "size_si": "Índice de Sensibilidade",
        "err_not_sens": "'x' deve ser o dict retornado por sus_mod_sensitivity().",
        "err_interactive": (
            "interactive=True requer a dependência opcional 'plotly', que "
            "climasus4py não empacota atualmente (assim como o caminho "
            "plotly do sus_mod_plot_burden()). Instale plotly manualmente "
            "se necessário; ver IDEIAS.md."
        ),
    },
    "en": {
        "curves_title": "Exposure-Response Curves by Stratum",
        "scatter_title": "Hot RR vs. Cold RR by Stratum",
        "bar_title": "RR Estimates by Stratum (Hot and Cold)",
        "x_exposure": "Exposure",
        "y_rr": "RR (95% CI)",
        "x_cold_rr": "Cold RR",
        "y_hot_rr": "Hot RR",
        "x_rr": "RR (95% CI)",
        "y_stratum": "Stratum",
        "strata": "strata",
        "comp_hot": "Hot",
        "comp_cold": "Cold",
        "size_si": "Sensitivity Index",
        "err_not_sens": "'x' must be the dict returned by sus_mod_sensitivity().",
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike sus_mod_plot_burden()'s "
            "plotly path). Install plotly manually if needed; see IDEIAS.md."
        ),
    },
    "es": {
        "curves_title": "Curvas Exposición-Respuesta por Estrato",
        "scatter_title": "RR Calor vs. RR Frío por Estrato",
        "bar_title": "Estimaciones de RR por Estrato (Calor y Frío)",
        "x_exposure": "Exposición",
        "y_rr": "RR (IC 95%)",
        "x_cold_rr": "RR Frío",
        "y_hot_rr": "RR Calor",
        "x_rr": "RR (IC 95%)",
        "y_stratum": "Estrato",
        "strata": "estratos",
        "comp_hot": "Calor",
        "comp_cold": "Frío",
        "size_si": "Índice de Sensibilidad",
        "err_not_sens": "'x' debe ser el dict retornado por sus_mod_sensitivity().",
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye actualmente (a diferencia del camino "
            "plotly de sus_mod_plot_burden()). Instale plotly manualmente "
            "si es necesario; ver IDEIAS.md."
        ),
    },
}

_FILL_HOT = "#E05C5C"
_FILL_COLD = "#4472C4"


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_sensitivity requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _sns_plot_curves(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_hline,
        geom_line,
        geom_ribbon,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    sc = x["stratum_curves"].copy()
    meta = x["meta"]

    si_order = x["comparison"]["label"].tolist()
    sc["label"] = pd.Categorical(sc["label"], categories=si_order, ordered=True)

    p = (
        ggplot(sc, aes(x="exposure", y="rr", color="label", fill="label", group="stratum"))
        + geom_ribbon(aes(ymin="rr_lo", ymax="rr_hi"), alpha=0.08, color=None)
        + geom_line(size=0.9)
        + geom_hline(yintercept=1, linetype="dashed", color="gray")
        + labs(
            title=strings["curves_title"],
            subtitle=f"{meta['climate_col']} | {meta['n_strata']} {strings['strata']}",
            x=strings["x_exposure"],
            y=strings["y_rr"],
            color="",
            fill="",
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            legend_position="bottom",
        )
    )
    return p


def _sns_plot_scatter(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_abline,
        geom_hline,
        geom_point,
        geom_text,
        geom_vline,
        ggplot,
        labs,
        scale_size_continuous,
        theme,
        theme_bw,
    )

    comp = x["comparison"]
    meta = x["meta"]

    p = (
        ggplot(comp, aes(x="cold_rr", y="hot_rr"))
        + geom_hline(yintercept=1, linetype="dashed", color="gray")
        + geom_vline(xintercept=1, linetype="dashed", color="gray")
        + geom_abline(slope=1, intercept=0, linetype="dotted", color="gray")
        + geom_point(aes(size="sensitivity_index"), color=_FILL_COLD, alpha=0.85)
        + geom_text(aes(label="label"), size=base_size * 0.7, va="bottom", nudge_y=0.02, color="gray")
        + scale_size_continuous(name=strings["size_si"], range=(3, 9))
        + labs(
            title=strings["scatter_title"],
            subtitle=f"{meta['climate_col']} | {meta['n_strata']} {strings['strata']}",
            x=strings["x_cold_rr"],
            y=strings["y_hot_rr"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
        )
    )
    return p


def _sns_plot_bar(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_errorbarh,
        geom_point,
        geom_vline,
        ggplot,
        labs,
        position_dodge,
        scale_color_manual,
        theme,
        theme_bw,
    )

    rr_tbl = x["rr_table"].copy()
    meta = x["meta"]

    si_order = x["comparison"]["label"].tolist()

    lbl_hot, lbl_cold = strings["comp_hot"], strings["comp_cold"]
    comp_map = {"hot": lbl_hot, "cold": lbl_cold}

    rr_tbl["component_label"] = pd.Categorical(
        rr_tbl["component"].map(comp_map), categories=[lbl_hot, lbl_cold], ordered=True
    )
    rr_tbl["label"] = pd.Categorical(rr_tbl["label"], categories=list(reversed(si_order)), ordered=True)

    fill_vals = {lbl_hot: _FILL_HOT, lbl_cold: _FILL_COLD}
    dodge = position_dodge(width=0.6)

    p = (
        ggplot(rr_tbl, aes(y="label", x="rr", color="component_label"))
        + geom_vline(xintercept=1, linetype="dashed", color="gray")
        + geom_errorbarh(
            aes(xmin="rr_lo", xmax="rr_hi", y="label"),
            height=0.25,
            size=0.8,
            na_rm=True,
            position=dodge,
        )
        + geom_point(size=3.5, position=dodge)
        + scale_color_manual(values=fill_vals, name="")
        + labs(
            title=strings["bar_title"],
            subtitle=f"{meta['climate_col']} | {meta['n_strata']} {strings['strata']}",
            x=strings["x_rr"],
            y=strings["y_stratum"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            panel_grid_major_y=element_blank(),
            legend_position="top",
        )
    )
    return p


def sus_mod_plot_sensitivity(
    x: dict[str, Any],
    type: Literal["curves", "scatter", "bar"] = "curves",
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, pd.DataFrame, or dict, depending on output_type
    """Plots and tables from a multi-stratum sensitivity analysis.

    Produces exposure-response curve overlays, hot-vs-cold RR scatter
    plots, and grouped forest plots from the ``dict`` returned by
    ``sus_mod_sensitivity()`` — the Python analogue of R's
    ``climasus_sensitivity`` object. Mirrors
    ``climasus4r::sus_mod_plot_sensitivity()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Plot types (*type*):
        * ``"curves"``: Overlay of each stratum's exposure-response curve
          with CI ribbon (default).
        * ``"scatter"``: Hot RR vs Cold RR per stratum; point size encodes
          sensitivity index.
        * ``"bar"``: Horizontal forest plot of hot and cold RR per
          stratum ordered by SI.

    Args:
        x: Dict returned by ``sus_mod_sensitivity()``, with keys
            ``"rr_table"``, ``"comparison"``, ``"stratum_curves"``,
            ``"meta"``.
        type: Plot type: ``"curves"`` (default), ``"scatter"``, or
            ``"bar"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``
            (dict with ``"plot"``, ``"table"``, ``"data"`` keys).
        interactive: ``True`` would convert the ``plotnine`` output to an
            interactive ``plotly`` widget. **Not currently supported** —
            raises ``ImportError`` because ``plotly`` is not a
            climasus4py dependency. See IDEIAS.md.
        base_size: ``plotnine`` base font size. Default ``12``.
        save_plot: File path to save the plot, or ``None`` (default).
            Uses ``plotnine``'s ``.save()`` (9x5 inches).
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``False``.

    Returns:
        Depending on *output_type*:

        - ``"plot"``: a ``plotnine.ggplot`` object.
        - ``"table"``: a ``pandas.DataFrame`` of the plotted data.
        - ``"all"``: a dict with ``"plot"``, ``"table"``, ``"data"`` keys.

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if ``interactive=True``.
        TypeError: If *x* is not a dict with the expected
            ``sus_mod_sensitivity()`` keys.

    Examples::

        import climasus4py as cs

        sens = cs.sus_mod_sensitivity(fits, lang="pt")

        cs.sus_mod_plot_sensitivity(sens, type="curves", lang="pt")
        cs.sus_mod_plot_sensitivity(sens, type="scatter", lang="en")
        cs.sus_mod_plot_sensitivity(sens, type="bar", lang="es")
        out = cs.sus_mod_plot_sensitivity(sens, output_type="all")
        out["table"]
    """
    if lang not in _I18N:
        lang = "pt"
    strings = _I18N[lang]

    if type not in ("curves", "scatter", "bar"):
        raise ValueError(f"type must be one of 'curves', 'scatter', 'bar'; got {type!r}.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}.")

    if not isinstance(x, dict) or not {
        "rr_table",
        "comparison",
        "stratum_curves",
        "meta",
    }.issubset(x.keys()):
        raise TypeError(strings["err_not_sens"])

    _require_plotnine()

    if verbose:
        print("climasus4py — Sensitivity Plot")

    if type == "curves":
        p = _sns_plot_curves(x, strings, base_size)
        tbl = x["stratum_curves"]
    elif type == "scatter":
        p = _sns_plot_scatter(x, strings, base_size)
        tbl = x["comparison"]
    else:  # bar
        p = _sns_plot_bar(x, strings, base_size)
        tbl = x["rr_table"]

    if interactive:
        raise ImportError(strings["err_interactive"])

    if save_plot is not None:
        p.save(save_plot, width=9, height=5)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {"plot": p, "table": tbl, "data": tbl}
