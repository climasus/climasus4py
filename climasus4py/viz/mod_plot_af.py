"""Plots and tables from an Attributable Fraction analysis.

Mirrors R: sus_mod_plot_af.R

Visualises the ``dict`` returned by ``sus_mod_af()`` (the Python analogue
of the R function's ``climasus_af`` S3 object — see
``enrichment/mod_af.py`` for its exact shape). Three plot types are
supported, mirroring the R helpers one-to-one:
  - ``"bar"``      (``_afplot_bar``)      — grouped bar chart of AF% by
    component (total/heat/cold), with CI error bars.
  - ``"forest"``   (``_afplot_forest``)   — horizontal forest plot of AN
    +/- CI by component.
  - ``"quantile"`` (``_afplot_quantile``) — AF% by exposure quantile band
    from ``x["by_quantile"]``.

Not lazy — operates on the ``pd.DataFrame`` tables already materialised by
``sus_mod_af()``; there is no DuckDB relation involved.

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
    >>> af = cs.sus_mod_af(fit, lang="pt")
    >>> p = cs.sus_mod_plot_af(af, type="bar", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .af_plot_labels)
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "bar_title": "Fração Atribuível por Componente",
        "forest_title": "Número Atribuível (± IC) por Componente",
        "quantile_title": "FA por Faixa de Percentil de Exposição",
        "x_component": "Componente",
        "y_af_pct": "FA (%)",
        "y_an": "NA (casos)",
        "x_quantile": "Faixa de Percentil",
        "comp_total": "Total",
        "comp_heat": "Calor",
        "comp_cold": "Frio",
        "err_not_af": "'x' deve ser o dict retornado por sus_mod_af().",
        "err_no_quantile": (
            "Sem dados em x['by_quantile']. Execute sus_mod_af() com 'pred_at' padrão."
        ),
        "err_interactive": (
            "interactive=True requer a dependência opcional 'plotly', que "
            "climasus4py não empacota atualmente (assim como o caminho "
            "plotly do sus_mod_plot_burden()). Instale plotly manualmente "
            "se necessário; ver IDEIAS.md."
        ),
    },
    "en": {
        "bar_title": "Attributable Fraction by Component",
        "forest_title": "Attributable Number (± CI) by Component",
        "quantile_title": "AF by Exposure Quantile Range",
        "x_component": "Component",
        "y_af_pct": "AF (%)",
        "y_an": "AN (cases)",
        "x_quantile": "Quantile Range",
        "comp_total": "Total",
        "comp_heat": "Heat",
        "comp_cold": "Cold",
        "err_not_af": "'x' must be the dict returned by sus_mod_af().",
        "err_no_quantile": (
            "No data in x['by_quantile']. Run sus_mod_af() with default 'pred_at'."
        ),
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike sus_mod_plot_burden()'s "
            "plotly path). Install plotly manually if needed; see IDEIAS.md."
        ),
    },
    "es": {
        "bar_title": "Fracción Atribuible por Componente",
        "forest_title": "Número Atribuible (± IC) por Componente",
        "quantile_title": "FA por Rango de Percentil de Exposición",
        "x_component": "Componente",
        "y_af_pct": "FA (%)",
        "y_an": "NA (casos)",
        "x_quantile": "Rango de Percentil",
        "comp_total": "Total",
        "comp_heat": "Calor",
        "comp_cold": "Frío",
        "err_not_af": "'x' debe ser el dict retornado por sus_mod_af().",
        "err_no_quantile": (
            "Sin datos en x['by_quantile']. Ejecute sus_mod_af() con 'pred_at' predeterminado."
        ),
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye actualmente (a diferencia del camino "
            "plotly de sus_mod_plot_burden()). Instale plotly manualmente "
            "si es necesario; ver IDEIAS.md."
        ),
    },
}

_FILL_TOTAL = "#808080"
_FILL_HEAT = "#E05C5C"
_FILL_COLD = "#4472C4"


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_af requires plotnine. Install with: pip install climasus4py[plot]"
        ) from exc


def _afplot_bar(dat: pd.DataFrame, fill_cols: dict[str, str], meta: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_col,
        geom_errorbar,
        geom_hline,
        ggplot,
        labs,
        scale_fill_manual,
        theme,
        theme_bw,
    )

    p = (
        ggplot(dat, aes(x="component_label", y="af_pct", fill="component_label"))
        + geom_col(width=0.6, alpha=0.85)
        + geom_errorbar(
            aes(ymin="af_pct_lo", ymax="af_pct_hi"), width=0.2, size=0.8, na_rm=True
        )
        + geom_hline(yintercept=0, linetype="dashed", color="gray")
        + scale_fill_manual(values=fill_cols)
        + labs(
            title=strings["bar_title"],
            subtitle=f"{meta['outcome_col']} — {meta['climate_col']}",
            x=strings["x_component"],
            y=strings["y_af_pct"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            panel_grid_major_x=element_blank(),
            legend_position="none",
        )
    )
    return p


def _afplot_forest(dat: pd.DataFrame, fill_cols: dict[str, str], meta: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_errorbarh,
        geom_point,
        geom_vline,
        ggplot,
        labs,
        scale_color_manual,
        theme,
        theme_bw,
    )

    p = (
        ggplot(dat, aes(y="component_label", color="component_label"))
        + geom_vline(xintercept=0, linetype="dashed", color="gray")
        + geom_errorbarh(
            aes(xmin="an_lo", xmax="an_hi", y="component_label"), height=0.25, size=0.9, na_rm=True
        )
        + geom_point(aes(x="an"), size=3.5)
        + scale_color_manual(values=fill_cols)
        + labs(
            title=strings["forest_title"],
            subtitle=f"{meta['outcome_col']} — {meta['climate_col']}",
            x=strings["y_an"],
            y=strings["x_component"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            panel_grid_major_y=element_blank(),
            legend_position="none",
        )
    )
    return p


def _afplot_quantile(qtl: pd.DataFrame, lbl_heat: str, lbl_cold: str, meta: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_col,
        geom_hline,
        ggplot,
        labs,
        scale_fill_manual,
        theme,
        theme_bw,
    )

    qtl = qtl.copy()
    if "quantile_label" in qtl.columns:
        qtl["x_val"] = pd.Categorical(
            qtl["quantile_label"], categories=list(dict.fromkeys(qtl["quantile_label"])), ordered=True
        )
    else:
        qtl["x_val"] = qtl["quantile_prob"].astype(str)

    if "component" in qtl.columns:
        comp_map_q = {"hot": lbl_heat, "cold": lbl_cold}
        qtl["comp_label"] = pd.Categorical(
            qtl["component"].map(comp_map_q), categories=[lbl_heat, lbl_cold], ordered=True
        )
        p = ggplot(qtl, aes(x="x_val", y="af_pct", fill="comp_label")) + geom_col(
            alpha=0.85
        ) + scale_fill_manual(values={lbl_heat: _FILL_HEAT, lbl_cold: _FILL_COLD}, name=strings["x_component"])
    else:
        p = ggplot(qtl, aes(x="x_val", y="af_pct")) + geom_col(fill=_FILL_COLD, alpha=0.85)

    p = (
        p
        + geom_hline(yintercept=0, linetype="dashed", color="gray")
        + labs(
            title=strings["quantile_title"],
            subtitle=f"{meta['outcome_col']} — {meta['climate_col']}",
            x=strings["x_quantile"],
            y=strings["y_af_pct"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            axis_text_x=element_text(angle=30, hjust=1),
        )
    )
    return p


def sus_mod_plot_af(
    fit: dict[str, Any],
    type: Literal["bar", "forest", "quantile"] = "bar",
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, pd.DataFrame, or dict, depending on output_type
    """Plots and tables from an Attributable Fraction analysis.

    Produces bar charts, forest plots, and quantile summaries from the
    ``dict`` returned by ``sus_mod_af()`` — the Python analogue of R's
    ``climasus_af`` object. Mirrors ``climasus4r::sus_mod_plot_af()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Plot types (*type*):
        * ``"bar"``: Grouped bar chart of AF% with CI by component
          (heat/cold/total) (default).
        * ``"forest"``: Horizontal forest plot of AN +/- CI by component.
        * ``"quantile"``: Bar chart of AF% across exposure quantile bands
          (requires ``fit["by_quantile"]``).

    Args:
        fit: Dict returned by ``sus_mod_af()``.
        type: Plot type: ``"bar"`` (default), ``"forest"``, or
            ``"quantile"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``
            (dict with ``"plot"``, ``"table"``, ``"data"`` keys).
        interactive: ``True`` would convert the ``plotnine`` output to an
            interactive ``plotly`` widget. **Not currently supported** —
            raises ``ImportError`` because ``plotly`` is not a
            climasus4py dependency. See IDEIAS.md.
        base_size: ``plotnine`` base font size. Default ``12``.
        save_plot: File path to save the plot, or ``None`` (default).
            Uses ``plotnine``'s ``.save()`` (8x5 inches).
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
        TypeError: If *fit* is not a dict with the expected
            ``sus_mod_af()`` keys.
        ValueError: If *type* is ``"quantile"`` but ``fit["by_quantile"]``
            is empty.

    Examples::

        import climasus4py as cs

        fit = cs.sus_mod_dlnm(df, outcome_col="n_obitos")
        af = cs.sus_mod_af(fit)

        cs.sus_mod_plot_af(af, type="bar", lang="pt")
        cs.sus_mod_plot_af(af, type="forest", lang="en")
        cs.sus_mod_plot_af(af, type="quantile", lang="es")
        out = cs.sus_mod_plot_af(af, output_type="all")
        out["table"]
    """
    if lang not in _I18N:
        lang = "pt"
    strings = _I18N[lang]

    if type not in ("bar", "forest", "quantile"):
        raise ValueError(f"type must be one of 'bar', 'forest', 'quantile'; got {type!r}.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}.")

    if not isinstance(fit, dict) or not {"total", "by_quantile", "meta"}.issubset(fit.keys()):
        raise TypeError(strings["err_not_af"])

    _require_plotnine()

    if verbose:
        print("climasus4py — Attributable Fraction Plot")

    meta = fit["meta"]
    lbl_total, lbl_heat, lbl_cold = (
        strings["comp_total"],
        strings["comp_heat"],
        strings["comp_cold"],
    )
    comp_map = {"total": lbl_total, "heat": lbl_heat, "cold": lbl_cold}
    fill_cols = {lbl_total: _FILL_TOTAL, lbl_heat: _FILL_HEAT, lbl_cold: _FILL_COLD}

    if type == "quantile":
        qtl_data = fit["by_quantile"]
        if qtl_data is None or len(qtl_data) == 0:
            raise ValueError(strings["err_no_quantile"])
        p = _afplot_quantile(qtl_data, lbl_heat, lbl_cold, meta, strings, base_size)
        tbl = qtl_data
    else:
        dat = fit["total"].copy()
        dat["component_label"] = pd.Categorical(
            dat["component"].map(comp_map), categories=[lbl_total, lbl_heat, lbl_cold], ordered=True
        )
        p = (
            _afplot_bar(dat, fill_cols, meta, strings, base_size)
            if type == "bar"
            else _afplot_forest(dat, fill_cols, meta, strings, base_size)
        )
        tbl = fit["total"]

    if interactive:
        raise ImportError(strings["err_interactive"])

    if save_plot is not None:
        p.save(save_plot, width=8, height=5)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {"plot": p, "table": tbl, "data": tbl}
