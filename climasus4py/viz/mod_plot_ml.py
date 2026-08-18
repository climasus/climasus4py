"""Plots and tables from an XGBoost machine learning model.

Mirrors R: sus_mod_plot_ml.R

Not lazy — this operates on the ``dict`` returned by
:func:`climasus4py.enrichment.mod_ml.sus_mod_ml` (the Python equivalent of
R's ``climasus_ml`` S3 object), which is already fully materialised
in-memory. There is no ``duckdb.DuckDBPyRelation`` input here.

Requires the optional ``[plot]`` extra::

    pip install climasus4py[plot]
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "importance_title": "Importância das Variáveis (Gain)",
        "fit_title": "Observado vs. Predito (CV fora da amostra)",
        "cv_log_title": "Convergência do Modelo por Rodada",
        "x_gain": "Ganho (Gain)",
        "y_feature": "Variável",
        "x_observed": "Observado",
        "y_cv_pred": "Predito (CV)",
        "x_round": "Rodada",
        "lbl_train": "treino",
        "lbl_test": "validação (CV)",
        "lbl_best": "melhor rodada",
        "no_importance": "Sem dados de importância",
        "no_cv_log": "Sem colunas de métrica de CV",
    },
    "en": {
        "importance_title": "Feature Importance (Gain)",
        "fit_title": "Observed vs. CV Predicted (Out-of-Fold)",
        "cv_log_title": "Model Loss by Boosting Round",
        "x_gain": "Gain",
        "y_feature": "Feature",
        "x_observed": "Observed",
        "y_cv_pred": "CV Predicted",
        "x_round": "Round",
        "lbl_train": "train",
        "lbl_test": "CV test",
        "lbl_best": "best round",
        "no_importance": "No importance data",
        "no_cv_log": "No CV metric columns found",
    },
    "es": {
        "importance_title": "Importancia de las Variables (Gain)",
        "fit_title": "Observado vs. Predicho (CV fuera de muestra)",
        "cv_log_title": "Convergencia del Modelo por Ronda",
        "x_gain": "Ganancia",
        "y_feature": "Variable",
        "x_observed": "Observado",
        "y_cv_pred": "Predicho (CV)",
        "x_round": "Ronda",
        "lbl_train": "entrenamiento",
        "lbl_test": "validación (CV)",
        "lbl_best": "mejor ronda",
        "no_importance": "Sin datos de importancia",
        "no_cv_log": "Sin columnas de métrica de CV",
    },
}

_REQUIRED_KEYS = ("predictions", "importance", "performance", "model", "cv_log", "meta")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_mod_plot_ml requires plotnine. Install with: pip install climasus4py[plot]"
        ) from exc


def _empty_plot(label: str, base_size: float) -> Any:
    import plotnine as p9

    return (
        p9.ggplot()
        + p9.annotate("text", x=0.5, y=0.5, label=label)
        + p9.theme_bw(base_size=base_size)
    )


def _plot_importance(
    ml: dict[str, Any], n_top: int, strings: dict[str, str], base_size: float
) -> Any:
    import plotnine as p9

    imp = ml["importance"]
    if len(imp) == 0:
        return _empty_plot(strings["no_importance"], base_size)

    imp_top = imp.iloc[: min(n_top, len(imp))].copy()
    # Highest Gain at the top of the horizontal bar chart (mirrors R's
    # `factor(Feature, levels = rev(imp_top$Feature))`).
    imp_top["Feature"] = pd.Categorical(
        imp_top["Feature"], categories=list(reversed(imp_top["Feature"])), ordered=True
    )
    imp_top["_gain_label"] = imp_top["Gain"].round(3)

    subtitle = (
        f"{ml['meta']['outcome_col']} | top {len(imp_top)} / {len(imp)} {strings['y_feature']}"
    )

    return (
        p9.ggplot(imp_top, p9.aes(y="Feature", x="Gain"))
        + p9.geom_col(fill="#4472C4", alpha=0.85)
        + p9.geom_text(
            p9.aes(label="_gain_label"),
            ha="left",
            nudge_x=0.005,
            size=base_size * 0.7,
            color="#333333",
        )
        + p9.scale_x_continuous(expand=(0, 0, 0.15, 0))
        + p9.labs(
            title=strings["importance_title"],
            subtitle=subtitle,
            x=strings["x_gain"],
            y=strings["y_feature"],
        )
        + p9.theme_bw(base_size=base_size)
        + p9.theme(
            plot_title=p9.element_text(face="bold"),
            plot_subtitle=p9.element_text(color="#666666"),
            panel_grid_major_y=p9.element_blank(),
        )
    )


def _plot_fit(ml: dict[str, Any], strings: dict[str, str], base_size: float) -> Any:
    import plotnine as p9

    pred = ml["predictions"]
    perf = ml["performance"]
    meta = ml["meta"]

    r2_cv = perf["R2_cv"]
    r2_txt = f"R² (CV) = {round(r2_cv, 3)}" if pd.notna(r2_cv) else "R² (CV) = NA"
    rmse_txt = f"RMSE (CV) = {round(perf['RMSE_cv'], 3)}"
    annot = f"{r2_txt}\n{rmse_txt}"

    all_vals = pd.concat([pred["observed"], pred["cv_predicted"]]).dropna()
    ax_min = float(all_vals.min())
    ax_max = float(all_vals.max())

    subtitle = f"{meta['outcome_col']} | {len(pred)} obs | nrounds = {perf['best_nrounds']}"

    return (
        p9.ggplot(pred, p9.aes(x="observed", y="cv_predicted"))
        + p9.geom_abline(slope=1, intercept=0, linetype="dashed", color="#666666")
        + p9.geom_point(alpha=0.45, color="#4472C4", size=1.8)
        + p9.annotate(
            "text",
            x=ax_min + 0.05 * (ax_max - ax_min),
            y=ax_max - 0.05 * (ax_max - ax_min),
            label=annot,
            ha="left",
            va="top",
            size=base_size * 0.9,
            color="#4472C4",
        )
        + p9.labs(
            title=strings["fit_title"],
            subtitle=subtitle,
            x=strings["x_observed"],
            y=strings["y_cv_pred"],
        )
        + p9.theme_bw(base_size=base_size)
        + p9.theme(
            plot_title=p9.element_text(face="bold"),
            plot_subtitle=p9.element_text(color="#666666"),
        )
    )


def _plot_cv_log(ml: dict[str, Any], strings: dict[str, str], base_size: float) -> Any:
    import plotnine as p9

    cv_df = ml["cv_log"]
    meta = ml["meta"]

    # Detect train/test *-mean columns. Note: Python's xgboost.cv() names
    # these columns with hyphens ("train-rmse-mean"), unlike R xgboost's
    # underscore convention ("train_rmse_mean") that the R source greps for
    # — this is a real difference between the R and Python xgboost bindings,
    # not an R bug, so the Python port adapts the pattern rather than
    # replicating R's regex verbatim. See IDEIAS.md.
    mean_cols = [c for c in cv_df.columns if c.endswith("-mean")]
    train_cols = [c for c in mean_cols if c.startswith("train-")]
    test_cols = [c for c in mean_cols if c.startswith("test-")]

    if not train_cols or not test_cols:
        return _empty_plot(strings["no_cv_log"], base_size)

    train_col = train_cols[0]
    test_col = test_cols[0]
    metric_nm = train_col.removeprefix("train-").removesuffix("-mean")

    lbl_train = strings["lbl_train"]
    lbl_test = strings["lbl_test"]

    # xgboost.cv()'s result has no "iter" column — the boosting round number
    # is just the 0-based row index. R's xgb.cv()$evaluation_log$iter is
    # 1-based, so +1 here to match that convention.
    iter_col = pd.Series(cv_df.index, dtype="int64") + 1

    cv_long = pd.DataFrame(
        {
            "iter": pd.concat([iter_col, iter_col], ignore_index=True),
            "metric_val": pd.concat(
                [cv_df[train_col], cv_df[test_col]], ignore_index=True
            ),
            "split_type": pd.Categorical(
                [lbl_train] * len(cv_df) + [lbl_test] * len(cv_df),
                categories=[lbl_train, lbl_test],
                ordered=True,
            ),
        }
    )

    fill_vals = {lbl_train: "#999999", lbl_test: "#4472C4"}
    best_nr = meta["best_nrounds"]

    subtitle = f"{meta['outcome_col']} | {meta['objective']} | eta = {meta['eta']}"

    return (
        p9.ggplot(cv_long, p9.aes(x="iter", y="metric_val", color="split_type"))
        + p9.geom_line(size=0.8)
        + p9.geom_vline(xintercept=best_nr, linetype="dashed", color="#595959", size=0.7)
        + p9.annotate(
            "text",
            x=best_nr,
            y=float("inf"),
            label=f"{strings['lbl_best']}: {best_nr}",
            ha="right",
            va="top",
            size=base_size * 0.85,
            color="#595959",
        )
        + p9.scale_color_manual(values=fill_vals, name=None)
        + p9.labs(
            title=strings["cv_log_title"],
            subtitle=subtitle,
            x=strings["x_round"],
            y=metric_nm.upper(),
        )
        + p9.theme_bw(base_size=base_size)
        + p9.theme(
            plot_title=p9.element_text(face="bold"),
            plot_subtitle=p9.element_text(color="#666666"),
            legend_position="top",
        )
    )


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_mod_plot_ml(
    x: dict[str, Any],
    type: Literal["importance", "fit", "cv_log"] = "importance",
    output_type: Literal["plot", "table", "all"] = "plot",
    n_top: int = 20,
    interactive: bool = False,
    base_size: float = 12,
    save_plot: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot, pd.DataFrame, or dict[str, Any]
    """Plot and table an XGBoost machine learning model result.

    Produces feature importance charts, observed-vs-predicted scatter
    plots, and cross-validation loss curves from the ``dict`` returned by
    :func:`climasus4py.enrichment.mod_ml.sus_mod_ml` (the Python
    equivalent of R's ``climasus_ml`` S3 object).

    Plot types (*type*):

    - ``"importance"``: Horizontal bar chart of XGBoost feature Gain (top
      *n_top* features).
    - ``"fit"``: Observed vs. out-of-fold CV-predicted scatter with R²
      and RMSE annotation.
    - ``"cv_log"``: Train/test loss per boosting round from
      ``xgboost.cv()``, with a best-round marker.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        x: The ``dict`` returned by ``sus_mod_ml()`` — must contain the
            keys ``predictions``, ``importance``, ``performance``,
            ``model``, ``cv_log``, ``meta``.
        type: Plot type: ``"importance"`` (default), ``"fit"``, or
            ``"cv_log"``.
        output_type: ``"plot"`` (default), ``"table"``, or ``"all"``
            (dict with keys ``"plot"``, ``"table"``, ``"data"``).
        n_top: Maximum number of features to show in the importance
            plot. Default ``20``.
        interactive: Kept for signature parity with the R function, which
            returns a Plotly widget when ``True``. plotnine has no
            faithful equivalent, so this implementation always returns a
            static ``ggplot`` and emits a warning when ``interactive=True``
            — see IDEIAS.md.
        base_size: Base font size for the plot theme. Default ``12``.
        save_plot: Optional file path to save the plot to (via
            ``ggplot.save``), e.g. ``"plot.png"``.
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
            Falls back to ``"pt"`` for unsupported values, mirroring
            ``sus_mod_ml``'s own silent fallback.
        verbose: Print progress messages. Default ``False``.

    Returns:
        Depending on *output_type*: a ``plotnine.ggplot`` object
        (``"plot"``), a ``pd.DataFrame`` of the plotted data (``"table"``),
        or a dict with keys ``"plot"``, ``"table"``, ``"data"``
        (``"all"``).

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``).
        TypeError: If *x* is not a dict with the expected ``sus_mod_ml``
            keys.
        ValueError: If *type*, *output_type*, or *lang* are invalid.

    Examples::

        import climasus4py as cs

        ml = cs.sus_mod_ml(df, outcome_col="n_obitos", feature_cols=["tmax", "pop"])

        cs.sus_mod_plot_ml(ml, type="importance", lang="pt")
        cs.sus_mod_plot_ml(ml, type="fit", lang="en")
        cs.sus_mod_plot_ml(ml, type="cv_log", lang="es")
        out = cs.sus_mod_plot_ml(ml, output_type="all")
        out["table"]
    """
    if type not in ("importance", "fit", "cv_log"):
        raise ValueError(f"Invalid type '{type}'. Expected one of: 'importance', 'fit', 'cv_log'.")
    if output_type not in ("plot", "table", "all"):
        raise ValueError(
            f"Invalid output_type '{output_type}'. Expected one of: 'plot', 'table', 'all'."
        )
    n_top = max(1, int(n_top))

    if lang not in ("pt", "en", "es"):
        lang = "pt"
    strings = _I18N[lang]

    if not isinstance(x, dict) or not all(k in x for k in _REQUIRED_KEYS):
        got = sorted(x.keys()) if isinstance(x, dict) else x.__class__.__name__
        raise TypeError(
            f"'x' must be the dict returned by sus_mod_ml(), "
            f"with keys {_REQUIRED_KEYS}. Got: {got}."
        )

    _require_plotnine()

    if verbose:
        print("climasus4py — ML Model Plot")

    if type == "importance":
        p = _plot_importance(x, n_top, strings, base_size)
        imp = x["importance"]
        tbl = imp.iloc[: min(n_top, len(imp))] if len(imp) > 0 else imp
    elif type == "fit":
        p = _plot_fit(x, strings, base_size)
        tbl = x["predictions"]
    else:  # cv_log
        p = _plot_cv_log(x, strings, base_size)
        tbl = x["cv_log"]

    if interactive:
        warnings.warn(
            "sus_mod_plot_ml: interactive=True has no plotnine equivalent "
            "(R returns a Plotly widget here); returning a static ggplot "
            "instead. See IDEIAS.md.",
            UserWarning,
            stacklevel=2,
        )

    if save_plot is not None:
        p.save(filename=save_plot, width=9, height=5, dpi=300)
        if verbose:
            print(f"Plot saved to {save_plot}")

    if output_type == "plot":
        return p
    if output_type == "table":
        return tbl
    return {"plot": p, "table": tbl, "data": tbl}
