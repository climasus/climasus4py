"""Plots and tables from a city-level disease burden analysis.

Mirrors R: sus_mod_plot_burden.R

Visualises the ``dict`` returned by ``sus_mod_burden()`` (the Python
analogue of the R function's ``climasus_burden`` S3 object — see
``enrichment/mod_burden.py`` for its exact shape). Three plot types are
supported, mirroring the R helpers one-to-one:
  - ``"lollipop"`` (``.bplot_lollipop``) — ranked lollipop chart of city
    burden (AN or excess), with confidence-interval error bars when the
    ``burden_table`` carries lo/hi columns.
  - ``"lorenz"``   (``.bplot_lorenz``)   — Lorenz concentration curve
    from ``concentration``, with a trapezoidal Gini coefficient annotation.
  - ``"stacked"``  (``.bplot_stacked``)  — stacked heat/cold bar by city
    (AF input only, requires ``component="all"`` on the original
    ``sus_mod_burden()`` call).

Not lazy — operates on the ``pd.DataFrame`` tables already materialised by
``sus_mod_burden()``; there is no DuckDB relation involved.

Deliberately narrower than the R source: ``interactive=True`` would return
a ``plotly`` interactive version (mirroring R's ``plotly::ggplotly()``
path). **Not currently supported** — ``plotly`` is not bundled with
climasus4py; raises ``ImportError``, matching the precedent already set by
``sus_climate_plot_aggregate()``. ``save_plot`` *is* implemented (via
``plotnine``'s own ``.save()``), since it needs no new dependency. See
IDEIAS.md.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> burden = cs.sus_mod_burden(af_list, lang="pt")
    >>> p = cs.sus_mod_plot_burden(burden, type="lollipop", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

from typing import Any, Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .burd_plot_labels)
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "lollipop_title": "Carga de Doença por Cidade",
        "lorenz_title": "Curva de Concentração de Lorenz",
        "lorenz_equality": "Igualdade perfeita",
        "stacked_title": "Decomposição Calor/Frio por Cidade",
        "x_city": "Cidade",
        "y_an": "NA (casos)",
        "y_excess": "Excesso (casos)",
        "x_cities_pct": "Cidades acumuladas (%)",
        "y_burden_pct": "Carga acumulada (%)",
        "comp_heat": "Calor",
        "comp_cold": "Frio",
        "err_not_burden": (
            "'x' deve ser o dict retornado por sus_mod_burden()."
        ),
        "err_stacked_excess": (
            "O tipo 'stacked' requer entrada AF (climasus_af) com component='all'."
        ),
        "err_interactive": (
            "interactive=True requer a dependência opcional 'plotly', que "
            "climasus4py não empacota atualmente (assim como o caminho "
            "plotly do sus_climate_plot_aggregate()). Instale plotly "
            "manualmente se necessário; ver IDEIAS.md."
        ),
        "warn_lang": "Idioma '{lang}' não suportado. Usando 'pt'.",
    },
    "en": {
        "lollipop_title": "Disease Burden by City",
        "lorenz_title": "Lorenz Concentration Curve",
        "lorenz_equality": "Perfect equality",
        "stacked_title": "Heat/Cold Decomposition by City",
        "x_city": "City",
        "y_an": "AN (cases)",
        "y_excess": "Excess (cases)",
        "x_cities_pct": "Cumulative cities (%)",
        "y_burden_pct": "Cumulative burden (%)",
        "comp_heat": "Heat",
        "comp_cold": "Cold",
        "err_not_burden": "'x' must be the dict returned by sus_mod_burden().",
        "err_stacked_excess": (
            "The 'stacked' type requires AF (climasus_af) input with component='all'."
        ),
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike climasus4r's plotly "
            "path). Install plotly manually if needed; see IDEIAS.md."
        ),
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
    },
    "es": {
        "lollipop_title": "Carga de Enfermedad por Ciudad",
        "lorenz_title": "Curva de Concentración de Lorenz",
        "lorenz_equality": "Igualdad perfecta",
        "stacked_title": "Descomposición Calor/Frío por Ciudad",
        "x_city": "Ciudad",
        "y_an": "NA (casos)",
        "y_excess": "Exceso (casos)",
        "x_cities_pct": "Ciudades acumuladas (%)",
        "y_burden_pct": "Carga acumulada (%)",
        "comp_heat": "Calor",
        "comp_cold": "Frío",
        "err_not_burden": "'x' debe ser el dict retornado por sus_mod_burden().",
        "err_stacked_excess": (
            "El tipo 'stacked' requiere entrada AF (climasus_af) con component='all'."
        ),
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye actualmente (a diferencia del camino "
            "plotly de climasus4r). Instale plotly manualmente si es "
            "necesario; ver IDEIAS.md."
        ),
        "warn_lang": "Idioma '{lang}' no admitido. Usando 'pt'.",
    },
}

_LOLLIPOP_COLOR = "#4472C4"
_STACKED_COLORS = {"heat": "#E05C5C", "cold": "#4472C4"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_burden requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _bplot_lollipop(x: dict[str, Any], is_af: bool, strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_errorbarh,
        geom_point,
        geom_segment,
        geom_vline,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    bt = x["burden_table"].copy()
    meta = x["meta"]

    if is_af and "component" in bt.columns:
        bt = bt[bt["component"] == "total"].copy()

    metric_col = "an" if "an" in bt.columns else "excess"
    y_label = strings["y_an"] if metric_col == "an" else strings["y_excess"]
    lo_col = "an_lo" if metric_col == "an" else "excess_lo"
    hi_col = "an_hi" if metric_col == "an" else "excess_hi"
    has_ci = lo_col in bt.columns and not bt[lo_col].isna().all()

    # Sort so the highest-burden city ends up at the top of the horizontal plot.
    bt = bt.sort_values("rank", ascending=False, kind="stable").reset_index(drop=True)
    bt["city"] = pd.Categorical(bt["city"], categories=bt["city"].tolist(), ordered=True)
    bt["metric_val"] = bt[metric_col]
    bt["ci_lo"] = bt[lo_col] if has_ci else np.nan
    bt["ci_hi"] = bt[hi_col] if has_ci else np.nan

    p = (
        ggplot(bt, aes(y="city", x="metric_val"))
        + geom_segment(
            aes(y="city", yend="city", x=0, xend="metric_val"),
            color=_LOLLIPOP_COLOR,
            size=0.8,
        )
        + geom_point(color=_LOLLIPOP_COLOR, size=3.2)
        + geom_vline(xintercept=0, color="#4D4D4D", size=0.5)
    )

    if has_ci:
        p = p + geom_errorbarh(
            aes(xmin="ci_lo", xmax="ci_hi"), height=0.2, color=_LOLLIPOP_COLOR, na_rm=True
        )

    p = (
        p
        + labs(
            title=strings["lollipop_title"],
            subtitle=f"Top {len(bt)} — {meta['rank_by']}",
            x=y_label,
            y=strings["x_city"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="#666666"),
            panel_grid_major_y=element_blank(),
        )
    )
    return p


def _bplot_lorenz(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        annotate,
        element_text,
        geom_area,
        geom_line,
        geom_point,
        ggplot,
        labs,
        theme,
        theme_bw,
    )

    conc = x["concentration"].copy()
    n = len(conc)
    meta = x["meta"]

    conc["rank_pct"] = conc["rank"] / n * 100
    eq_df = pd.DataFrame({"rank_pct": [0, 100], "cumulative_pct": [0, 100]})

    # Trapezoidal Gini from the concentration data.
    if n >= 2:
        rank_pct = conc["rank_pct"].to_numpy()
        cum_pct = conc["cumulative_pct"].to_numpy()
        area_curve = np.sum(np.diff(rank_pct) * (cum_pct[1:] + cum_pct[:-1]) / 2) / 100
        gini = round((50 - area_curve) / 50, 3)
    else:
        gini = np.nan
    gini_txt = f"Gini = {gini}" if not np.isnan(gini) else ""

    p = (
        ggplot(conc, aes(x="rank_pct", y="cumulative_pct"))
        + geom_line(
            data=eq_df,
            mapping=aes(x="rank_pct", y="cumulative_pct"),
            linetype="dashed",
            color="#808080",
            size=0.8,
        )
        + geom_area(fill=_LOLLIPOP_COLOR, alpha=0.12)
        + geom_line(color=_LOLLIPOP_COLOR, size=1.1)
        + geom_point(color=_LOLLIPOP_COLOR, size=2.5)
        + annotate(
            "text",
            x=65,
            y=12,
            label=gini_txt,
            color=_LOLLIPOP_COLOR,
            size=base_size * 0.3,
            ha="left",
        )
        + labs(
            title=strings["lorenz_title"],
            subtitle=f"{meta['n_cities']} {strings['x_city']} — {meta['rank_by']}",
            caption=f"-- {strings['lorenz_equality']}",
            x=strings["x_cities_pct"],
            y=strings["y_burden_pct"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="#666666"),
        )
    )
    return p


def _bplot_stacked(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        geom_col,
        ggplot,
        labs,
        scale_fill_manual,
        theme,
        theme_bw,
    )

    bt = x["burden_table"]
    meta = x["meta"]

    # Derive city display order from total-component rank.
    tot_order = bt.loc[bt["component"] == "total", ["city", "rank"]]
    tot_order = tot_order.sort_values("rank", ascending=False, kind="stable")

    dat = bt[bt["component"].isin(["heat", "cold"])].copy()

    lbl_heat = strings["comp_heat"]
    lbl_cold = strings["comp_cold"]
    comp_map = {"heat": lbl_heat, "cold": lbl_cold}
    dat["component_label"] = pd.Categorical(
        dat["component"].map(comp_map), categories=[lbl_heat, lbl_cold], ordered=True
    )
    dat["city"] = pd.Categorical(dat["city"], categories=tot_order["city"].tolist(), ordered=True)

    fill_vals = {lbl_heat: _STACKED_COLORS["heat"], lbl_cold: _STACKED_COLORS["cold"]}

    p = (
        ggplot(dat, aes(x="city", y="an", fill="component_label"))
        + geom_col(alpha=0.85)
        + scale_fill_manual(values=fill_vals, name="")
        + labs(
            title=strings["stacked_title"],
            subtitle=f"{meta['n_cities']} {strings['x_city']}",
            x=strings["x_city"],
            y=strings["y_an"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="#666666"),
            axis_text_x=element_text(angle=30, hjust=1),
            panel_grid_major_x=element_blank(),
            legend_position="top",
        )
    )
    return p


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_mod_plot_burden(
    x: dict[str, Any],
    type: Literal["lollipop", "lorenz", "stacked"] = "lollipop",
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, pd.DataFrame, or dict, depending on output_type
    """Plots and tables from a city-level disease burden analysis.

    Produces lollipop charts, Lorenz concentration curves, and stacked bar
    charts from the ``dict`` returned by ``sus_mod_burden()`` — the Python
    analogue of R's ``climasus_burden`` object. Mirrors
    ``climasus4r::sus_mod_plot_burden()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Plot types (*type*):
        * ``"lollipop"``: Ranked lollipop chart of city AN or excess
          burden (default).
        * ``"lorenz"``: Lorenz concentration curve showing burden
          inequality across cities.
        * ``"stacked"``: Stacked heat/cold bar by city (only for AF input
          with ``component="all"`` on the original ``sus_mod_burden()``
          call).

    Args:
        x: Dict returned by ``sus_mod_burden()``, with keys
            ``"burden_table"``, ``"concentration"``, ``"total_burden"``,
            ``"meta"``.
        type: Plot type: ``"lollipop"`` (default), ``"lorenz"``, or
            ``"stacked"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``
            (dict with ``"plot"``, ``"table"``, ``"data"`` keys).
        interactive: ``True`` would convert the ``plotnine`` output to an
            interactive ``plotly`` widget (mirroring R's
            ``plotly::ggplotly()``). **Not currently supported** —
            raises ``ImportError`` because ``plotly`` is not a
            climasus4py dependency. See IDEIAS.md.
        base_size: ``plotnine`` base font size. Default ``12``.
        save_plot: File path to save the plot, or ``None`` (default).
            Uses ``plotnine``'s ``.save()`` (9x5 inches), the static
            equivalent of the R source's ``ggplot2::ggsave()`` branch.
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``False``.

    Returns:
        Depending on *output_type*:

        - ``"plot"``: a ``plotnine.ggplot`` object.
        - ``"table"``: a ``pandas.DataFrame`` of the plotted data.
        - ``"all"``: a dict with ``"plot"``, ``"table"``, ``"data"`` keys
          (``"table"`` and ``"data"`` are the same DataFrame).

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if ``interactive=True``.
        TypeError: If *x* is not a dict with the expected
            ``sus_mod_burden()`` keys.
        ValueError: If *type* is ``"stacked"`` but *x* was not built from
            AF input with ``component="all"``.

    Examples::

        import climasus4py as cs

        burden = cs.sus_mod_burden(af_list, lang="pt")
        cs.sus_mod_plot_burden(burden, type="lollipop", lang="pt")
        cs.sus_mod_plot_burden(burden, type="lorenz", lang="en")
        cs.sus_mod_plot_burden(burden, type="stacked", lang="es")
        out = cs.sus_mod_plot_burden(burden, output_type="all")
        out["table"]
    """
    if lang not in _I18N:
        lang = "pt"
    strings = _I18N[lang]

    if type not in ("lollipop", "lorenz", "stacked"):
        raise ValueError(f"type must be one of 'lollipop', 'lorenz', 'stacked'; got {type!r}.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}.")

    if not isinstance(x, dict) or not {
        "burden_table",
        "concentration",
        "total_burden",
        "meta",
    }.issubset(x.keys()):
        raise TypeError(strings["err_not_burden"])

    _require_plotnine()

    if verbose:
        print("climasus4py — Disease Burden Plot")

    meta = x["meta"]
    is_af = meta.get("input_type") == "climasus_af"
    comp_all = meta.get("component") == "all"

    if type == "lollipop":
        p = _bplot_lollipop(x, is_af, strings, base_size)
        bt = x["burden_table"]
        tbl = bt[bt["component"] == "total"] if is_af and "component" in bt.columns else bt
    elif type == "lorenz":
        p = _bplot_lorenz(x, strings, base_size)
        tbl = x["concentration"]
    else:  # stacked
        if not is_af or not comp_all:
            raise ValueError(strings["err_stacked_excess"])
        p = _bplot_stacked(x, strings, base_size)
        bt = x["burden_table"]
        tbl = bt[bt["component"].isin(["heat", "cold"])]

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
