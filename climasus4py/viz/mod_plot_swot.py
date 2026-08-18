"""Plots and tables from a climate-health SWOT analysis.

Mirrors R: sus_mod_plot_swot.R

Visualises the dict returned by ``sus_mod_swot()`` (the Python analogue
of R's ``climasus_swot`` S3 object — see ``enrichment/mod_swot.py`` for
its exact shape). Three plot types, mirroring the R helpers one-to-one:
  - ``"radar"``  (``_swot_plot_radar``)  — spider/radar chart of the four
    quadrant scores (0-100) per entity.
  - ``"matrix"`` (``_swot_plot_matrix``) — classic 2x2 SWOT board with
    score, category, and top indicators per quadrant (single entity).
  - ``"bar"``    (``_swot_plot_bar``)    — grouped horizontal bars of
    quadrant scores per entity.

Not lazy — operates on the ``pd.DataFrame`` tables already materialised by
``sus_mod_swot()``; there is no DuckDB relation involved.

Deliberately narrower than the R source: ``interactive=True`` would return
a ``plotly`` interactive version. **Not currently supported** — ``plotly``
is not bundled with climasus4py; raises ``ImportError``, matching the
precedent set by ``sus_mod_plot_burden()``.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> swot = cs.sus_mod_swot(vulnerability=vi, af=af_res, lang="pt")
    >>> p = cs.sus_mod_plot_swot(swot, type="matrix", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "radar_title": "Perfil SWOT Climático-Saúde",
        "matrix_title": "Análise SWOT Climático-Saúde",
        "bar_title": "Pontuações SWOT por Entidade",
        "quadrant_S": "Forças",
        "quadrant_W": "Fraquezas",
        "quadrant_O": "Oportunidades",
        "quadrant_T": "Ameaças",
        "x_score": "Pontuação (0-100)",
        "y_entity": "Entidade",
        "no_data": "Sem dados",
        "err_not_swot": "'x' deve ser o dict retornado por sus_mod_swot().",
        "warn_matrix_multi": (
            "type='matrix' mostra apenas uma entidade. Exibindo: {ent}. "
            "Use type='bar' para múltiplas."
        ),
        "err_interactive": (
            "interactive=True requer a dependência opcional 'plotly', que "
            "climasus4py não empacota atualmente (assim como o caminho "
            "plotly do sus_mod_plot_burden()). Instale plotly manualmente "
            "se necessário; ver IDEIAS.md."
        ),
    },
    "en": {
        "radar_title": "Climate-Health SWOT Profile",
        "matrix_title": "Climate-Health SWOT Analysis",
        "bar_title": "SWOT Scores by Entity",
        "quadrant_S": "Strengths",
        "quadrant_W": "Weaknesses",
        "quadrant_O": "Opportunities",
        "quadrant_T": "Threats",
        "x_score": "Score (0-100)",
        "y_entity": "Entity",
        "no_data": "No data",
        "err_not_swot": "'x' must be the dict returned by sus_mod_swot().",
        "warn_matrix_multi": (
            "type='matrix' shows one entity only. Showing: {ent}. Use "
            "type='bar' for multiple."
        ),
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike sus_mod_plot_burden()'s "
            "plotly path). Install plotly manually if needed; see IDEIAS.md."
        ),
    },
    "es": {
        "radar_title": "Perfil SWOT Clima-Salud",
        "matrix_title": "Análisis SWOT Clima-Salud",
        "bar_title": "Puntuaciones SWOT por Entidad",
        "quadrant_S": "Fortalezas",
        "quadrant_W": "Debilidades",
        "quadrant_O": "Oportunidades",
        "quadrant_T": "Amenazas",
        "x_score": "Puntuación (0-100)",
        "y_entity": "Entidad",
        "no_data": "Sin datos",
        "err_not_swot": "'x' debe ser el dict retornado por sus_mod_swot().",
        "warn_matrix_multi": (
            "type='matrix' muestra solo una entidad. Mostrando: {ent}. Use "
            "type='bar' para múltiples."
        ),
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye actualmente (a diferencia del camino "
            "plotly de sus_mod_plot_burden()). Instale plotly manualmente "
            "si es necesario; ver IDEIAS.md."
        ),
    },
}

_QUAD_COLORS = {"S": "#22c55e", "W": "#f97316", "O": "#3b82f6", "T": "#ef4444"}


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_swot requires plotnine. Install with: pip install climasus4py[plot]"
        ) from exc


def _long_scores(x: dict[str, Any], strings: dict[str, str], entities_sel: list[str] | None) -> pd.DataFrame:
    scores = x["scores"]
    if entities_sel is not None:
        scores = scores[scores["entity"].isin(entities_sel)]

    quad_labels = {q: strings[f"quadrant_{q}"] for q in ("S", "W", "O", "T")}
    rows = []
    for q in ("S", "W", "O", "T"):
        rows.append(
            pd.DataFrame(
                {
                    "entity": scores["entity"].to_numpy(),
                    "quadrant": q,
                    "quadrant_label": quad_labels[q],
                    "score": scores[f"{q}_score"].to_numpy(dtype=float),
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def _plot_radar(x: dict[str, Any], strings: dict[str, str], base_size: int, entities_sel: list[str] | None) -> Any:
    from plotnine import (
        aes,
        annotate,
        coord_fixed,
        element_text,
        geom_line,
        geom_path,
        geom_point,
        geom_polygon,
        geom_text,
        ggplot,
        labs,
        scale_color_manual,
        scale_fill_manual,
        theme,
        theme_void,
    )

    long = _long_scores(x, strings, entities_sel)
    long["score"] = long["score"].fillna(0)

    quad_order = ["S", "O", "T", "W"]
    quad_angles = {"S": 90.0, "O": 0.0, "T": 270.0, "W": 180.0}
    quad_angles = {k: np.deg2rad(v) for k, v in quad_angles.items()}
    quad_labels_vec = {q: strings[f"quadrant_{q}"] for q in quad_order}

    all_entities = long["entity"].unique().tolist()

    poly_rows = []
    for ent in all_entities:
        ent_data = long[long["entity"] == ent]
        pts = []
        for q in quad_order:
            row = ent_data[ent_data["quadrant"] == q]
            r = float(row["score"].iloc[0]) / 100 if len(row) else 0.0
            theta = quad_angles[q]
            pts.append({"entity": ent, "quadrant": q, "r": r, "x": r * np.cos(theta), "y": r * np.sin(theta)})
        pts.append(pts[0])
        poly_rows.append(pd.DataFrame(pts))
    poly_df = pd.concat(poly_rows, ignore_index=True)

    ring_vals = [0.33, 0.66, 1.0]
    theta_seq = np.linspace(0, 2 * np.pi, 200)
    grid_rows = [
        pd.DataFrame({"r": rv, "x": rv * np.cos(theta_seq), "y": rv * np.sin(theta_seq)}) for rv in ring_vals
    ]
    grid_df = pd.concat(grid_rows, ignore_index=True)

    axis_rows = [
        pd.DataFrame({"quadrant": q, "x": [0, np.cos(quad_angles[q])], "y": [0, np.sin(quad_angles[q])]})
        for q in quad_order
    ]
    axis_df = pd.concat(axis_rows, ignore_index=True)

    label_scale = 1.22
    axis_labels = pd.DataFrame(
        {
            "label": [quad_labels_vec[q] for q in quad_order],
            "x": [label_scale * np.cos(quad_angles[q]) for q in quad_order],
            "y": [label_scale * np.sin(quad_angles[q]) for q in quad_order],
        }
    )

    n_ents = len(all_entities)
    if n_ents == 1:
        fill_colors = {all_entities[0]: _QUAD_COLORS["S"]}
    else:
        ramp = ["#10b981", "#3b82f6", "#f97316", "#8b5cf6", "#ef4444"]
        fill_colors = {ent: ramp[i % len(ramp)] for i, ent in enumerate(all_entities)}

    first_pts = poly_df.drop_duplicates(subset=["entity", "quadrant"])

    p = (
        ggplot()
        + geom_path(data=grid_df, mapping=aes(x="x", y="y", group="r"), color="gray", linetype="dashed", size=0.35)
        + geom_line(data=axis_df, mapping=aes(x="x", y="y", group="quadrant"), color="gray", size=0.5)
        + geom_polygon(data=poly_df, mapping=aes(x="x", y="y", group="entity", fill="entity"), alpha=0.20)
        + geom_path(data=poly_df, mapping=aes(x="x", y="y", group="entity", color="entity"), size=0.9)
        + geom_point(data=first_pts, mapping=aes(x="x", y="y", color="entity"), size=2.5)
        + geom_text(
            data=axis_labels, mapping=aes(x="x", y="y", label="label"),
            size=base_size * 0.9, fontweight="bold", ha="center", va="center",
        )
        + annotate(
            "text", x=[0.33, 0.66, 1.0], y=[0.0, 0.0, 0.0],
            label=["33", "66", "100"], size=base_size * 0.7, color="gray", ha="left",
        )
        + scale_fill_manual(values=fill_colors, name="")
        + scale_color_manual(values=fill_colors, name="")
        + coord_fixed(xlim=(-1.35, 1.35), ylim=(-1.35, 1.35))
        + theme_void(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold", ha="center"),
            legend_position="bottom" if n_ents > 1 else "none",
        )
        + labs(title=strings["radar_title"])
    )
    return p


def _plot_matrix(
    x: dict[str, Any], strings: dict[str, str], base_size: int, entities_sel: list[str] | None, top_n: int
) -> Any:
    from plotnine import (
        annotate,
        element_text,
        geom_rect,
        ggplot,
        labs,
        lims,
        theme,
        theme_void,
    )

    scores = x["scores"]
    if entities_sel is not None:
        scores = scores[scores["entity"].isin(entities_sel)]

    if len(scores) > 1:
        ent_show = scores["entity"].iloc[0]
        warnings.warn(strings["warn_matrix_multi"].format(ent=ent_show), UserWarning, stacklevel=2)
        scores = scores.iloc[[0]]

    ent = scores["entity"].iloc[0]

    quads = {
        "S": {"xmn": 0, "xmx": 0.5, "ymn": 0.5, "ymx": 1, "color": _QUAD_COLORS["S"]},
        "W": {"xmn": 0.5, "xmx": 1, "ymn": 0.5, "ymx": 1, "color": _QUAD_COLORS["W"]},
        "O": {"xmn": 0, "xmx": 0.5, "ymn": 0, "ymx": 0.5, "color": _QUAD_COLORS["O"]},
        "T": {"xmn": 0.5, "xmx": 1, "ymn": 0, "ymx": 0.5, "color": _QUAD_COLORS["T"]},
    }

    p = (
        ggplot()
        + lims(x=(0, 1), y=(0, 1))
        + theme_void(base_size=base_size)
        + labs(title=strings["matrix_title"], subtitle=ent)
        + theme(
            plot_title=element_text(face="bold", ha="center", size=base_size * 1.1),
            plot_subtitle=element_text(color="gray", ha="center"),
        )
        + annotate("segment", x=0, xend=1, y=0.5, yend=0.5, color="gray", size=1.2)
        + annotate("segment", x=0.5, xend=0.5, y=0, yend=1, color="gray", size=1.2)
    )

    pad = 0.025
    for q, qd in quads.items():
        q_lbl = strings[f"quadrant_{q}"]
        sc_val = scores[f"{q}_score"].iloc[0]
        sc_cat = scores[f"{q}_cat"].iloc[0] if f"{q}_cat" in scores.columns else None

        score_txt = f"{sc_val:.0f}" if pd.notna(sc_val) else strings["no_data"]
        cat_txt = f" ({sc_cat})" if sc_cat is not None and pd.notna(sc_cat) else ""

        x_mid = (qd["xmn"] + qd["xmx"]) / 2
        y_top = qd["ymx"] - pad

        rect_df = pd.DataFrame(
            {"xmin": [qd["xmn"] + pad], "xmax": [qd["xmx"] - pad], "ymin": [qd["ymn"] + pad], "ymax": [qd["ymx"] - pad]}
        )
        from plotnine import aes

        p = p + geom_rect(
            data=rect_df, mapping=aes(xmin="xmin", xmax="xmax", ymin="ymin", ymax="ymax"),
            fill=qd["color"], alpha=0.10, color=qd["color"], size=0.8,
        )

        header_lbl = f"{q_lbl}  {score_txt}{cat_txt}"
        p = p + annotate(
            "text", x=x_mid, y=y_top - 0.01, label=header_lbl,
            ha="center", va="top", size=base_size * 1.0, fontweight="bold", color=qd["color"],
        )

        ent_inds = x["indicators"]
        ent_inds = ent_inds[(ent_inds["entity"] == ent) & (ent_inds["quadrant"] == q)]
        ent_inds = ent_inds.sort_values("norm_score", ascending=False).head(top_n)

        n_ind = len(ent_inds)
        y_start = y_top - 0.09
        line_h = (qd["ymx"] - qd["ymn"] - 0.15) / max(1, top_n)

        if n_ind > 0:
            for k, (_, ind_row) in enumerate(ent_inds.iterrows()):
                lbl = f"• {ind_row['indicator']} – {round(ind_row['norm_score'])}"
                p = p + annotate(
                    "text", x=qd["xmn"] + pad * 1.5, y=y_start - k * line_h, label=lbl,
                    ha="left", va="top", size=base_size * 0.7, color="gray",
                )
        else:
            p = p + annotate(
                "text", x=x_mid, y=y_start, label=strings["no_data"],
                ha="center", va="top", size=base_size * 0.8, color="gray", fontstyle="italic",
            )

    return p


def _plot_bar(x: dict[str, Any], strings: dict[str, str], base_size: int, entities_sel: list[str] | None) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        expand_limits,
        geom_col,
        ggplot,
        labs,
        position_dodge,
        scale_fill_manual,
        scale_x_continuous,
        theme,
        theme_bw,
    )

    long = _long_scores(x, strings, entities_sel)
    quad_order = ["S", "W", "O", "T"]
    quad_labels = [strings[f"quadrant_{q}"] for q in quad_order]
    fill_vec = dict(zip(quad_labels, [_QUAD_COLORS[q] for q in quad_order], strict=True))

    long["quadrant_label"] = pd.Categorical(long["quadrant_label"], categories=quad_labels, ordered=True)

    p = (
        ggplot(long, aes(x="score", y="entity", fill="quadrant_label"))
        + geom_col(position=position_dodge(width=0.75), width=0.65, alpha=0.88, na_rm=True)
        + scale_fill_manual(values=fill_vec, name="")
        + scale_x_continuous(limits=(0, 100))
        + expand_limits(x=0)
        + labs(title=strings["bar_title"], x=strings["x_score"], y=strings["y_entity"])
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            panel_grid_major_y=element_blank(),
            legend_position="top",
        )
    )
    return p


def sus_mod_plot_swot(
    x: dict[str, Any],
    type: Literal["radar", "matrix", "bar"] = "radar",
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    entities: list[str] | None = None,
    top_n: int = 3,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, pd.DataFrame, or dict, depending on output_type
    """Plots and tables from a climate-health SWOT analysis.

    Produces radar (spider), matrix (2x2 SWOT board), and bar charts from
    the dict returned by ``sus_mod_swot()`` — the Python analogue of R's
    ``climasus_swot`` object. Mirrors ``climasus4r::sus_mod_plot_swot()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Plot types (*type*):
        * ``"radar"``: Spider/radar chart of the four quadrant scores
          (0-100) per entity (default).
        * ``"matrix"``: Classic 2x2 SWOT board with score, category
          label, and top indicators per quadrant. Best for a single
          entity — a multi-entity selection warns and keeps only the
          first.
        * ``"bar"``: Grouped horizontal bars comparing all four
          quadrant scores per entity.

    Args:
        x: Dict returned by ``sus_mod_swot()``, with keys ``"scores"``,
            ``"indicators"``, ``"meta"``.
        type: Plot type: ``"radar"`` (default), ``"matrix"``, or
            ``"bar"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``
            (dict with ``"plot"``, ``"table"`` keys).
        interactive: **Not currently supported** — raises
            ``ImportError`` because ``plotly`` is not a climasus4py
            dependency. See IDEIAS.md.
        entities: Subset of entity names to plot, or ``None`` (default,
            all entities). For ``type="matrix"``, only the first entity
            in the selection is shown.
        top_n: For ``type="matrix"``, maximum number of indicator
            bullets per quadrant. Default ``3``.
        base_size: ``plotnine`` base font size. Default ``12``.
        save_plot: File path to save the plot, or ``None`` (default).
            Uses ``plotnine``'s ``.save()`` (9x6 inches).
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``False``.

    Returns:
        Depending on *output_type*:

        - ``"plot"``: a ``plotnine.ggplot`` object.
        - ``"table"``: a ``pandas.DataFrame`` of the plotted data.
        - ``"all"``: a dict with ``"plot"``, ``"table"`` keys.

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if ``interactive=True``.
        TypeError: If *x* is not a dict with the expected
            ``sus_mod_swot()`` keys.

    Examples::

        import climasus4py as cs

        swot = cs.sus_mod_swot(vulnerability=vi, af=af_res, lang="pt")

        cs.sus_mod_plot_swot(swot, type="matrix", lang="pt")
        cs.sus_mod_plot_swot(swot, type="radar", lang="en")
        cs.sus_mod_plot_swot(swot, type="bar", lang="es")
        out = cs.sus_mod_plot_swot(swot, output_type="all")
        out["table"]
    """
    if lang not in _I18N:
        lang = "pt"
    strings = _I18N[lang]

    if type not in ("radar", "matrix", "bar"):
        raise ValueError(f"type must be one of 'radar', 'matrix', 'bar'; got {type!r}.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}.")

    if not isinstance(x, dict) or not {"scores", "indicators", "meta"}.issubset(x.keys()):
        raise TypeError(strings["err_not_swot"])

    if interactive:
        raise ImportError(strings["err_interactive"])

    _require_plotnine()

    if verbose:
        print("climasus4py — SWOT Plot")

    if type == "radar":
        p = _plot_radar(x, strings, base_size, entities)
        tbl = _long_scores(x, strings, entities)
    elif type == "matrix":
        p = _plot_matrix(x, strings, base_size, entities, top_n)
        tbl = x["scores"]
        if entities is not None:
            tbl = tbl[tbl["entity"].isin(entities)]
    else:  # bar
        p = _plot_bar(x, strings, base_size, entities)
        tbl = _long_scores(x, strings, entities)

    if save_plot is not None:
        p.save(save_plot, width=9, height=6)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {"plot": p, "table": tbl}
