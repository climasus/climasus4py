"""Visualisations for pooled multi-city DLNM meta-analysis results.

Mirrors R: sus_mod_plot_pool.R

Consumes the dict returned by
:func:`climasus4py.enrichment.mod_pool.sus_mod_pool` (the Python analogue
of R's ``climasus_pool`` S3 object). Three plot types, matching the R
helpers:

``"overall"``
    The pooled exposure-response curve with its confidence ribbon.
``"forest"``
    One row per city: the city's own estimate and, when available, its
    BLUP, both at the 75th percentile of exposure.
``"spaghetti"``
    Every city's BLUP curve in grey behind the pooled curve, which is the
    picture that shows how much the pooling shrank each city.

Not lazy — reads the tables ``sus_mod_pool()`` already computed.

Deliberately narrower than the R source: ``interactive=True`` is **not
supported**, because ``plotly`` is not bundled with climasus4py — same
precedent as ``sus_mod_plot_dlnm()`` and ``sus_mod_plot_burden()``. It
raises ``ImportError`` rather than silently returning a static plot.

Requires the optional [plot] extra::

    pip install climasus4py[plot]

Usage::

    >>> import climasus4py as cs
    >>> pool = cs.sus_mod_pool({"sp": fit_sp, "rj": fit_rj})
    >>> p = cs.sus_mod_plot_pool(pool, type="forest", lang="pt")
    >>> p.save("forest.png")
"""

from __future__ import annotations

from typing import Any, Literal

import numpy as np
import pandas as pd

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "overall_title": "Curva Exposição-Resposta Agrupada",
        "forest_title": "Estimativas por Cidade (± IC) — p75 da Exposição",
        "spaghetti_title": "Curvas BLUP por Cidade + Estimativa Agrupada",
        "x_exposure": "Exposição",
        "y_rr": "RR (IC 95%)",
        "y_city": "Cidade",
        "forest_cap_blup": "◆ estimativa original    ● BLUP",
        "cities": "cidades",
        "err_not_pool": "'x' deve ser o dict retornado por sus_mod_pool().",
        "err_no_curve": (
            "Sem curva agrupada em x['exposure_curve']. O agrupamento falhou."
        ),
        "err_no_cities": "Sem estimativas por cidade em x['city_table'].",
        "warn_no_blup": (
            "Sem BLUPs em x['blup_preds']. Mostrando apenas a curva agrupada."
        ),
        "warn_lang": "Idioma '{lang}' nao suportado. Usando 'pt'.",
        "warn_basis": (
            "Este agrupamento foi feito com bases de exposicao DIFERENTES entre "
            "as cidades (x['meta']['shared_basis'] e False). A curva abaixo "
            "agrega coeficientes que nao sao comparaveis -- ver M65."
        ),
        "err_interactive": (
            "interactive=True requer a dependencia opcional 'plotly', que "
            "climasus4py nao empacota. Instale plotly manualmente se "
            "necessario; ver IDEIAS.md."
        ),
    },
    "en": {
        "overall_title": "Pooled Exposure-Response Curve",
        "forest_title": "City-Specific Estimates (± CI) — Exposure p75",
        "spaghetti_title": "City BLUP Curves + Pooled Estimate",
        "x_exposure": "Exposure",
        "y_rr": "RR (95% CI)",
        "y_city": "City",
        "forest_cap_blup": "◆ original estimate    ● BLUP",
        "cities": "cities",
        "err_not_pool": "'x' must be the dict returned by sus_mod_pool().",
        "err_no_curve": (
            "No pooled curve in x['exposure_curve']. The pooling failed."
        ),
        "err_no_cities": "No city-specific estimates in x['city_table'].",
        "warn_no_blup": (
            "No BLUPs in x['blup_preds']. Showing the pooled curve only."
        ),
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
        "warn_basis": (
            "This pooling used DIFFERENT exposure bases across cities "
            "(x['meta']['shared_basis'] is False). The curve below aggregates "
            "coefficients that are not comparable — see M65."
        ),
        "err_interactive": (
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not bundle. Install plotly manually if needed; "
            "see IDEIAS.md."
        ),
    },
    "es": {
        "overall_title": "Curva Exposición-Respuesta Agrupada",
        "forest_title": "Estimativas por Ciudad (± IC) — p75 de Exposición",
        "spaghetti_title": "Curvas BLUP por Ciudad + Estimativa Agrupada",
        "x_exposure": "Exposición",
        "y_rr": "RR (IC 95%)",
        "y_city": "Ciudad",
        "forest_cap_blup": "◆ estimativa original    ● BLUP",
        "cities": "ciudades",
        "err_not_pool": "'x' debe ser el dict retornado por sus_mod_pool().",
        "err_no_curve": (
            "Sin curva agrupada en x['exposure_curve']. El agrupamiento fallo."
        ),
        "err_no_cities": "Sin estimativas por ciudad en x['city_table'].",
        "warn_no_blup": (
            "Sin BLUPs en x['blup_preds']. Mostrando solo la curva agrupada."
        ),
        "warn_lang": "Idioma '{lang}' no soportado. Usando 'pt'.",
        "warn_basis": (
            "Este agrupamiento uso bases de exposicion DIFERENTES entre las "
            "ciudades (x['meta']['shared_basis'] es False). La curva agrega "
            "coeficientes que no son comparables — ver M65."
        ),
        "err_interactive": (
            "interactive=True requiere la dependencia opcional 'plotly', que "
            "climasus4py no incluye. Instale plotly manualmente si es "
            "necesario; ver IDEIAS.md."
        ),
    },
}

#: The R helpers hardcode these two colours; kept identical so the two
#: packages produce visually comparable figures.
_POOLED = "#4472C4"
_CITY = "#B3B3B3"
_CITY_POINT = "#737373"

_REQUIRED_KEYS = ("exposure_curve", "city_table", "meta")


def _strings(lang: str) -> dict[str, str]:
    if lang not in _I18N:
        print(_I18N["pt"]["warn_lang"].format(lang=lang))
        return _I18N["pt"]
    return _I18N[lang]


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401, PLC0415
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_pool requires plotnine. Install with: "
            "pip install climasus4py[plot]"
        ) from exc


def _subtitle(meta: dict[str, Any], strings: dict[str, str]) -> str:
    return (
        f"{meta.get('outcome_col')} — {meta.get('climate_col')} | "
        f"{meta.get('n_cities')} {strings['cities']}"
    )


def _plot_overall(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (  # noqa: PLC0415
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

    curve, meta = x["exposure_curve"], x["meta"]
    return (
        ggplot(curve, aes(x="exposure"))
        + geom_ribbon(aes(ymin="lo", ymax="hi"), fill=_POOLED, alpha=0.15)
        + geom_line(aes(y="rr"), color=_POOLED, size=1.1)
        + geom_hline(yintercept=1, linetype="dashed", color="#4D4D4D")
        + geom_vline(xintercept=meta["ref_value"], linetype="dotted", color="#808080")
        + labs(
            title=strings["overall_title"],
            subtitle=_subtitle(meta, strings),
            x=strings["x_exposure"],
            y=strings["y_rr"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
        )
    )


def _plot_forest(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (  # noqa: PLC0415
        aes,
        element_blank,
        element_text,
        geom_errorbarh,
        geom_point,
        geom_vline,
        ggplot,
        labs,
        position_nudge,
        theme,
        theme_bw,
    )

    table, meta = x["city_table"], x["meta"]
    # Ordered by effect size, so the reader sees the gradient rather than
    # whatever order the fits happened to arrive in.
    ordered = table.sort_values("rr").reset_index(drop=True)
    ordered["city"] = pd.Categorical(
        ordered["city"], categories=list(ordered["city"]), ordered=True
    )
    has_blup = bool(ordered["blup_rr"].notna().any())

    p = (
        ggplot(ordered, aes(y="city"))
        + geom_vline(xintercept=1, linetype="dashed", color="#666666")
        + geom_errorbarh(aes(xmin="lo", xmax="hi"), height=0.2, size=0.8, color="#8C8C8C")
        + geom_point(aes(x="rr"), shape="D", size=3.0, color=_CITY_POINT)
    )
    if has_blup:
        # Nudged below the raw estimate so the shrinkage is readable as a
        # vertical pair rather than two overlapping points.
        nudge = position_nudge(y=-0.28)
        com_blup = ordered[ordered["blup_rr"].notna()]
        p = (
            p
            + geom_errorbarh(
                data=com_blup,
                mapping=aes(xmin="blup_lo", xmax="blup_hi"),
                height=0.2, size=0.8, color=_POOLED, position=nudge,
            )
            + geom_point(
                data=com_blup,
                mapping=aes(x="blup_rr"),
                shape="o", size=2.6, color=_POOLED, position=nudge,
            )
        )

    return (
        p
        + labs(
            title=strings["forest_title"],
            subtitle=_subtitle(meta, strings),
            caption=strings["forest_cap_blup"] if has_blup else "",
            x=strings["y_rr"],
            y=strings["y_city"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
            panel_grid_major_y=element_blank(),
        )
    )


def _blup_curves(x: dict[str, Any]) -> pd.DataFrame:
    """Long table of every city's BLUP curve, for the spaghetti plot."""
    blocos = [
        pd.DataFrame({
            "city": name,
            "exposure": np.asarray(pred["predvar"], dtype=float),
            "rr": np.asarray(pred["allRRfit"], dtype=float),
        })
        for name, pred in (x.get("blup_preds") or {}).items()
        if pred is not None
    ]
    return pd.concat(blocos, ignore_index=True) if blocos else pd.DataFrame()


def _plot_spaghetti(x: dict[str, Any], strings: dict[str, str], base_size: int) -> Any:
    from plotnine import (  # noqa: PLC0415
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

    curves = _blup_curves(x)
    pooled, meta = x["exposure_curve"], x["meta"]
    return (
        ggplot()
        + geom_line(
            data=curves, mapping=aes(x="exposure", y="rr", group="city"),
            color=_CITY, size=0.5, alpha=0.8,
        )
        + geom_ribbon(
            data=pooled, mapping=aes(x="exposure", ymin="lo", ymax="hi"),
            fill=_POOLED, alpha=0.15,
        )
        + geom_line(
            data=pooled, mapping=aes(x="exposure", y="rr"),
            color=_POOLED, size=1.3,
        )
        + geom_hline(yintercept=1, linetype="dashed", color="#4D4D4D")
        + geom_vline(xintercept=meta["ref_value"], linetype="dotted", color="#808080")
        + labs(
            title=strings["spaghetti_title"],
            subtitle=_subtitle(meta, strings),
            x=strings["x_exposure"],
            y=strings["y_rr"],
        )
        + theme_bw(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold"),
            plot_subtitle=element_text(color="gray"),
        )
    )


def sus_mod_plot_pool(
    x: dict[str, Any],
    type: Literal["overall", "forest", "spaghetti"] = "overall",  # noqa: A002
    output_type: Literal["plot", "table", "all"] = "plot",
    interactive: bool = False,
    base_size: int = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:
    """Plot pooled multi-city DLNM meta-analysis results.

    Args:
        x: The dict returned by ``sus_mod_pool()``.
        type: ``"overall"`` (default) for the pooled exposure-response
            curve, ``"forest"`` for per-city estimates, or
            ``"spaghetti"`` for city BLUP curves behind the pooled one.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``.
        interactive: Not supported — raises ``ImportError``. ``plotly``
            is not bundled with climasus4py.
        base_size: Base font size passed to the theme. Default ``12``.
        save_plot: Path to write the figure to, or ``None``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Whether to print progress. Default ``False``.

    Returns:
        A ``plotnine.ggplot`` for ``output_type="plot"``; the backing
        ``pandas.DataFrame`` for ``"table"``; a dict with ``plot``,
        ``table`` and ``data`` for ``"all"``.

    Raises:
        TypeError: If *x* is not a ``sus_mod_pool()`` result.
        ValueError: If *type* or *output_type* is unknown, or the table
            the requested plot needs is missing or empty.
        ImportError: If ``interactive=True``, or ``plotnine`` is absent.

    Warns:
        Prints a warning when ``"spaghetti"`` is requested but no BLUPs
        are present (falls back to ``"overall"``, as R does), and when
        the pooling used different exposure bases across cities.
    """
    strings = _strings(lang)

    if type not in ("overall", "forest", "spaghetti"):
        raise ValueError(
            f"type must be one of 'overall', 'forest', 'spaghetti'; got {type!r}."
        )
    if output_type not in ("plot", "table", "all"):
        raise ValueError(
            f"output_type must be one of 'plot', 'table', 'all'; got {output_type!r}."
        )
    if not isinstance(x, dict) or not set(_REQUIRED_KEYS).issubset(x):
        raise TypeError(strings["err_not_pool"])
    if interactive:
        raise ImportError(strings["err_interactive"])

    _require_plotnine()

    if verbose:
        print(f"climasus4py — Pooled DLNM Plot (type: {type})")
    # The pooled numbers are only interpretable when every city shared the
    # exposure basis; say so at the point where someone is about to read
    # the curve, not only when it was computed.
    if x["meta"].get("shared_basis") is False:
        print(strings["warn_basis"])

    curve = x.get("exposure_curve")
    table = x.get("city_table")
    tem_curva = curve is not None and len(curve) > 0
    tem_cidades = table is not None and len(table) > 0

    efetivo = type
    if type == "spaghetti" and _blup_curves(x).empty:
        print(strings["warn_no_blup"])
        efetivo = "overall"

    if efetivo == "forest":
        if not tem_cidades:
            raise ValueError(strings["err_no_cities"])
    elif not tem_curva:
        raise ValueError(strings["err_no_curve"])

    p = None
    if output_type in ("plot", "all"):
        if efetivo == "overall":
            p = _plot_overall(x, strings, base_size)
        elif efetivo == "forest":
            p = _plot_forest(x, strings, base_size)
        else:
            p = _plot_spaghetti(x, strings, base_size)

    # R returns exposure_curve for "overall" and city_table for the other
    # two; the fallback above changes the plot but not which table the
    # caller asked about, so it keys off `type`, not `efetivo`.
    tbl = curve if type == "overall" else table

    if save_plot is not None and p is not None:
        p.save(save_plot, width=9, height=5, dpi=300, verbose=False)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {"plot": p, "table": tbl, "data": tbl}
