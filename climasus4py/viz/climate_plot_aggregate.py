"""Exploratory climate-health aggregate visualisation — ggplot-style via plotnine.

Mirrors R: sus_climate_plot_aggregate (sus_climate_plot_aggregate.R)

``sus_climate_plot_aggregate`` produces six complementary exploratory plot
types for a daily climate-health table (time-series overlay, scatter with
smooth, cross-correlation, distribution, correlation matrix, seasonal
boxplots). Modelling-specific plots (DLNM surfaces, residual diagnostics, RR
tables) are deliberately excluded, mirroring the R docstring's explicit scope
note (those live in the ``sus_mod_plot_*`` family, not yet ported).

This function operates on an already-materialised table (a ``date`` column,
a health-outcome column, and one or more climate columns) — it is a plotting
utility, not a pipeline stage, so a ``duckdb.DuckDBPyRelation`` input is
materialised via ``.df()`` at the top and the function otherwise never
touches the lazy pipeline.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_climate_plot_aggregate(df_agg, plot_type="timeseries", lang="en")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import re
import warnings
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import pandas as pd

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

# ---------------------------------------------------------------------------
# Palette (mirrors R's .CPA_PAL)
# ---------------------------------------------------------------------------

_CPA_PAL = {
    "primary": "#185FA5",
    "secondary": "#D85A30",
    "tertiary": "#1D9E75",
    "light": "#C8D9EF",
    "neutral": "#888780",
}

_VALID_PLOT_TYPES = (
    "timeseries",
    "scatter",
    "ccf",
    "distribution",
    "corr_matrix",
    "seasonal",
)

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .cpa_labels)
# ---------------------------------------------------------------------------

_I18N = {
    "pt": {
        "title": "climasus4r — Visualização do Agregado Climato-Saúde",
        "n_rows": "Linhas",
        "outcome_lbl": "Desfecho",
        "climate_lbl": "Clima",
        "date_lbl": "Data",
        "lag_lbl": "Defasagem (dias)",
        "corr_lbl": "Correlação",
        "sig_lbl": "Significativo",
        "density_lbl": "Densidade",
        "variable_lbl": "Variável",
        "month_lbl": "Mês",
        "spearman_lbl": "Spearman",
        "ts_title": "Série Temporal: Clima e Saúde",
        "sc_title": "Dispersão: Clima vs Desfecho",
        "ccf_title": "Função de Correlação Cruzada",
        "dist_title": "Distribuição das Variáveis Climáticas",
        "corr_title": "Matriz de Correlação de Spearman",
        "corr_subtitle": "Correlação entre defasagens climáticas e desfecho",
        "seas_title": "Padrão Sazonal",
        "done": "Plot gerado com sucesso.",
        "months": [
            "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
            "Jul", "Ago", "Set", "Out", "Nov", "Dez",
        ],
        "type_label": {
            "timeseries": "Série Temporal",
            "scatter": "Dispersão",
            "ccf": "Correlação Cruzada",
            "distribution": "Distribuição",
            "corr_matrix": "Matriz de Correlação",
            "seasonal": "Padrão Sazonal",
        },
    },
    "en": {
        "title": "climasus4r — Climate-Health Aggregate Visualisation",
        "n_rows": "Rows",
        "outcome_lbl": "Outcome",
        "climate_lbl": "Climate",
        "date_lbl": "Date",
        "lag_lbl": "Lag (days)",
        "corr_lbl": "Correlation",
        "sig_lbl": "Significant",
        "density_lbl": "Density",
        "variable_lbl": "Variable",
        "month_lbl": "Month",
        "spearman_lbl": "Spearman",
        "ts_title": "Time Series: Climate and Health",
        "sc_title": "Scatter: Climate vs Outcome",
        "ccf_title": "Cross-Correlation Function",
        "dist_title": "Distribution of Climate Variables",
        "corr_title": "Spearman Correlation Matrix",
        "corr_subtitle": "Correlations between climate lags and outcome",
        "seas_title": "Seasonal Pattern",
        "done": "Plot generated successfully.",
        "months": [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ],
        "type_label": {
            "timeseries": "Time Series",
            "scatter": "Scatter",
            "ccf": "Cross-Correlation",
            "distribution": "Distribution",
            "corr_matrix": "Correlation Matrix",
            "seasonal": "Seasonal Pattern",
        },
    },
    "es": {
        "title": "climasus4r — Visualización del Agregado Clima-Salud",
        "n_rows": "Filas",
        "outcome_lbl": "Desenlace",
        "climate_lbl": "Clima",
        "date_lbl": "Fecha",
        "lag_lbl": "Rezago (días)",
        "corr_lbl": "Correlación",
        "sig_lbl": "Significativo",
        "density_lbl": "Densidad",
        "variable_lbl": "Variable",
        "month_lbl": "Mes",
        "spearman_lbl": "Spearman",
        "ts_title": "Serie Temporal: Clima y Salud",
        "sc_title": "Dispersión: Clima vs Desenlace",
        "ccf_title": "Función de Correlación Cruzada",
        "dist_title": "Distribución de Variables Climáticas",
        "corr_title": "Matriz de Correlación de Spearman",
        "corr_subtitle": "Correlaciones entre rezagos climáticos y desenlace",
        "seas_title": "Patrón Estacional",
        "done": "Gráfico generado correctamente.",
        "months": [
            "Ene", "Feb", "Mar", "Abr", "May", "Jun",
            "Jul", "Ago", "Sep", "Oct", "Nov", "Dic",
        ],
        "type_label": {
            "timeseries": "Serie Temporal",
            "scatter": "Dispersión",
            "ccf": "Correlación Cruzada",
            "distribution": "Distribución",
            "corr_matrix": "Matriz de Correlación",
            "seasonal": "Patrón Estacional",
        },
    },
}

# Climate-column detection regexes (mirrors R's .cpa_detect_climate_cols)
_CLIMATE_COL_PATTERNS = (
    r"^lag\d+_",
    r"^mvwin\d+_",
    r"^off\d+to\d+_",
    r"^gdd\d+",
    r"_lag\d+$",
    r"^(tair|patm|rh_|dew_|rainfall|ws_|wd_|sr_|cdd|hdd|wbgt|hi_|utci|pet|thi|diurnal|vapor)",
    r"^(spi_|spei_|pdsi|smvi|chirps|era5|cams|ghap|merra2|fires|prodes)",
)

_META_COLS_CLIMATE_DETECT = frozenset(
    {
        "date", "code_muni", "code_muni_7", "name_muni",
        "code_state", "abbrev_state", "geom", "geometry",
        "region", "UF", "latitude", "longitude", "altitude",
    }
)

# NB: intentionally narrower than _META_COLS_CLIMATE_DETECT — this mirrors
# the R source's .cpa_detect_outcome_col(), which uses a *different*,
# shorter meta_cols list (missing region/UF/latitude/longitude/altitude).
# Preserved as-is rather than "fixed"; see IDEIAS.md.
_META_COLS_OUTCOME_DETECT = frozenset(
    {
        "date", "code_muni", "code_muni_7", "name_muni",
        "code_state", "abbrev_state", "geom", "geometry",
    }
)

_OUTCOME_PREFERRED_PATTERNS = (
    r"^n_", r"^count", r"obito", r"internac", r"caso", r"morte",
    r"hospitaliz", r"death", r"case", r"admission",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_climate_plot_aggregate requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _detect_climate_cols(df: pd.DataFrame) -> list[str]:
    candidate = [c for c in df.columns if c not in _META_COLS_CLIMATE_DETECT]
    return [
        c
        for c in candidate
        if any(re.search(rx, c) for rx in _CLIMATE_COL_PATTERNS)
    ]


def _detect_outcome_col(df: pd.DataFrame, climate_cols: list[str]) -> str | None:
    exclude = _META_COLS_OUTCOME_DETECT | set(climate_cols)
    candidate = [c for c in df.columns if c not in exclude]
    int_cols = [c for c in candidate if pd.api.types.is_numeric_dtype(df[c])]
    named = [
        c
        for c in int_cols
        if any(re.search(rx, c, re.IGNORECASE) for rx in _OUTCOME_PREFERRED_PATTERNS)
    ]
    if named:
        return named[0]
    if int_cols:
        return int_cols[0]
    return None


def _unit_label(col: str) -> str:
    """Return a unit suffix for a climate column, inferred from its name suffix."""
    if col.endswith("_c"):
        return " (°C)"
    if col.endswith("_mb"):
        return " (mb)"
    if col.endswith("_mm"):
        return " (mm)"
    if col.endswith("_porc"):
        return " (%)"
    if col.endswith("_m_s"):
        return " (m/s)"
    if col.endswith("_kj_m2"):
        return " (kJ/m²)"
    if col.endswith("_degrees"):
        return " (°)"
    return ""


def _caption(base_caption: str, source: str | None) -> str:
    if source:
        return f"Data: {source} • {base_caption}"
    return base_caption


def _color_ramp(colors: list[str], n: int) -> list[str]:
    """Linearly interpolate n hex colors along the given control-point ramp.

    Approximates R's grDevices::colorRampPalette without adding a new
    dependency (matplotlib is already pulled in transitively by plotnine).
    """
    from matplotlib.colors import LinearSegmentedColormap, to_hex

    if n <= 0:
        return []
    if n == 1:
        return [colors[0]]
    cmap = LinearSegmentedColormap.from_list("_cpa_ramp", colors)
    return [to_hex(cmap(i / (n - 1))) for i in range(n)]


def _cpa_theme() -> Any:
    import plotnine as p9

    return p9.theme_classic(base_size=12) + p9.theme(
        panel_grid_minor=p9.element_blank(),
        panel_grid_major_x=p9.element_blank(),
        panel_grid_major_y=p9.element_line(color="#EBEBEB", size=0.3),
        axis_line=p9.element_line(color="#333333", size=0.5),
        plot_title=p9.element_text(weight="bold", size=13, ha="left"),
        plot_subtitle=p9.element_text(color="#4A4A4A", size=10, ha="left"),
        plot_caption=p9.element_text(color="#777777", size=8, ha="right"),
        axis_title=p9.element_text(size=10, color="#444441"),
        axis_text=p9.element_text(size=9, color="#5F5E5A"),
        legend_position="bottom",
        strip_text=p9.element_text(weight="bold", size=10),
        figure_size=(9, 5),
    )


# ---------------------------------------------------------------------------
# Internal: single time-series panel
# ---------------------------------------------------------------------------


def _cpa_ts_single(
    dts: pd.DataFrame, col: str, outcome_col: str, lbl: dict, title_str: str, caption_str: str
) -> Any:
    import plotnine as p9

    keep_ok = dts["outcome_val"].notna() & dts[col].notna()
    dts = dts.loc[keep_ok].copy()

    y = dts["outcome_val"].to_numpy(dtype=float)
    x = dts[col].to_numpy(dtype=float)

    y_lo, y_hi = float(np.nanmin(y)), float(np.nanmax(y))
    c_lo, c_hi = float(np.nanmin(x)), float(np.nanmax(x))

    if not np.isfinite(c_hi - c_lo) or (c_hi - c_lo) < 1e-12:
        c_lo, c_hi = c_lo - 1, c_hi + 1
    if not np.isfinite(y_hi - y_lo) or (y_hi - y_lo) < 1e-12:
        y_lo, y_hi = y_lo - 1, y_hi + 1

    # Rescale climate into the outcome's y-range (mirrors scales::rescale).
    dts["climate_sc"] = y_lo + (x - c_lo) * (y_hi - y_lo) / (c_hi - c_lo)
    dts["y_floor"] = y_lo

    unit_lbl = _unit_label(col)
    sec_name = f"{col}{unit_lbl}"

    # NOTE: plotnine has no ggplot2::sec_axis() equivalent, so unlike the R
    # source this cannot draw a true secondary axis with climate-native tick
    # labels. The climate series is rescaled into the outcome's range (same
    # technique as R) and its original range is surfaced in the subtitle
    # instead. See IDEIAS.md.
    subtitle = (
        f"{lbl['outcome_lbl']}: {outcome_col}   |   {lbl['climate_lbl']}: {col}  "
        f"({sec_name} rescaled from [{c_lo:.1f}, {c_hi:.1f}])"
    )

    return (
        p9.ggplot(dts, p9.aes(x="date"))
        + p9.geom_ribbon(
            p9.aes(ymin="y_floor", ymax="climate_sc"),
            fill=_CPA_PAL["light"],
            color=_CPA_PAL["primary"],
            alpha=0.55,
            size=0.5,
        )
        + p9.geom_line(p9.aes(y="climate_sc"), color=_CPA_PAL["primary"], size=0.5, alpha=0.8)
        + p9.geom_line(p9.aes(y="outcome_val"), color=_CPA_PAL["secondary"], size=0.9)
        + p9.geom_point(p9.aes(y="outcome_val"), color=_CPA_PAL["secondary"], size=1.2, alpha=0.7)
        + p9.scale_x_date(date_breaks="2 months", date_labels="%b/%y")
        + p9.scale_y_continuous(name=lbl["outcome_lbl"])
        + p9.labs(title=title_str, subtitle=subtitle, x=lbl["date_lbl"], caption=caption_str)
        + _cpa_theme()
    )


def _cpa_timeseries(
    df: pd.DataFrame,
    climate_cols: list[str],
    outcome_col: str,
    lbl: dict,
    title: str | None,
    source: str | None,
) -> Any:
    y_vec = pd.to_numeric(df[outcome_col], errors="coerce")
    n_muni = df["code_muni"].nunique() if "code_muni" in df.columns else None

    min_date = df["date"].min()
    max_date = df["date"].max()
    subtitle_sfx = f"Period: {min_date:%Y-%m-%d} – {max_date:%Y-%m-%d}"
    if n_muni is not None:
        subtitle_sfx += f" | n = {n_muni} municipalities"

    base_cap = "DATASUS • INMET • climasus4py • sus_climate_plot_aggregate()"
    caption_str = f"{subtitle_sfx}\n{_caption(base_cap, source)}"
    title_str = title if title is not None else lbl["ts_title"]

    panels = []
    for col in climate_cols:
        dts = pd.DataFrame(
            {"date": df["date"], "outcome_val": y_vec, "y_floor": 0.0, col: df[col]}
        )
        panels.append(_cpa_ts_single(dts, col, outcome_col, lbl, title_str, caption_str))

    p = panels[0]
    for extra in panels[1:]:
        p = p / extra
    return p


# ---------------------------------------------------------------------------
# Internal: scatter
# ---------------------------------------------------------------------------

_SMOOTH_METHOD_MAP = {"loess": "loess", "gam": "loess"}


def _resolve_smooth_method(smooth_method: str) -> str:
    if smooth_method not in _SMOOTH_METHOD_MAP:
        raise ValueError(
            f"Invalid smooth_method '{smooth_method}'. Choose from: "
            f"{sorted(_SMOOTH_METHOD_MAP)}."
        )
    if smooth_method == "gam":
        warnings.warn(
            "sus_climate_plot_aggregate: plotnine has no GAM smoother "
            "backend (unlike R's mgcv::gam). Falling back to 'loess', "
            "which approximates but does not reproduce the R output.",
            UserWarning,
            stacklevel=3,
        )
    return _SMOOTH_METHOD_MAP[smooth_method]


def _cpa_scatter_panel(
    df_plot: pd.DataFrame,
    x_lbl: str,
    y_lbl: str,
    method: str,
    alpha: float,
    title_str: str,
    caption_str: str | None,
    subtitle: str | None,
) -> Any:
    import plotnine as p9

    p = (
        p9.ggplot(df_plot, p9.aes(x="x_val", y="y_val"))
        + p9.geom_point(color=_CPA_PAL["primary"], alpha=0.35, size=1.6)
        + p9.geom_smooth(
            method=method,
            se=True,
            color=_CPA_PAL["secondary"],
            fill=_CPA_PAL["light"],
            size=1.1,
            level=1 - alpha,
        )
        + p9.labs(title=title_str, subtitle=subtitle, x=x_lbl, y=y_lbl, caption=caption_str)
        + _cpa_theme()
    )
    return p


def _cpa_scatter(
    df: pd.DataFrame,
    climate_cols: list[str],
    outcome_col: str,
    smooth_method: str,
    alpha: float,
    lbl: dict,
    title: str | None,
    source: str | None,
) -> Any:
    method = _resolve_smooth_method(smooth_method)
    base_cap = f"smooth = {smooth_method} • climasus4py • sus_climate_plot_aggregate()"
    caption_str = _caption(base_cap, source)
    title_str = title if title is not None else lbl["sc_title"]

    y_num = pd.to_numeric(df[outcome_col], errors="coerce")

    panels = []
    for col in climate_cols:
        unit_lbl = _unit_label(col)
        df_plot = pd.DataFrame({"x_val": df[col], "y_val": y_num}).dropna()
        subtitle = f"{lbl['climate_lbl']}: {col}  →  {lbl['outcome_lbl']}: {outcome_col}"
        panels.append(
            _cpa_scatter_panel(
                df_plot,
                f"{col}{unit_lbl}",
                outcome_col,
                method,
                alpha,
                title_str,
                caption_str,
                subtitle,
            )
        )

    p = panels[0]
    for extra in panels[1:]:
        p = p / extra
    return p


# ---------------------------------------------------------------------------
# Internal: ccf
# ---------------------------------------------------------------------------


def _cpa_ccf(
    df: pd.DataFrame,
    climate_cols: list[str],
    outcome_col: str,
    max_lag: int,
    alpha: float,
    lbl: dict,
    title: str | None,
    source: str | None,
) -> Any:
    import plotnine as p9
    from statsmodels.tsa.stattools import ccf as _sm_ccf

    _ = alpha  # confidence bounds are the fixed +-2/sqrt(n) rule, as in R

    col = climate_cols[0]
    x_ts = pd.to_numeric(df[col], errors="coerce")
    y_ts = pd.to_numeric(df[outcome_col], errors="coerce")
    ok = x_ts.notna() & y_ts.notna()
    x_ts = x_ts[ok].to_numpy(dtype=float)
    y_ts = y_ts[ok].to_numpy(dtype=float)
    n = len(x_ts)

    # Mirrors stats::ccf(x, y, lag.max): corr(x[t+k], y[t]) for k in
    # [-max_lag, max_lag]. statsmodels.ccf(a, b)[k] = corr(a[t+k], b[t])
    # for k >= 0 only, so negative lags are obtained by swapping a and b.
    pos = _sm_ccf(x_ts, y_ts, adjusted=False)[: max_lag + 1]
    neg = _sm_ccf(y_ts, x_ts, adjusted=False)[1 : max_lag + 1]
    acf_val = np.concatenate([neg[::-1], pos])
    lag_val = np.arange(-max_lag, max_lag + 1)

    ci = 2 / np.sqrt(n) if n > 0 else np.nan
    df_ccf = pd.DataFrame(
        {
            "lag_val": lag_val,
            "acf_val": acf_val,
            "sig": np.where(np.abs(acf_val) > ci, "True", "False"),
        }
    )

    base_cap = (
        f"±2/√n = ±{ci:.3f} | n = {n} • climasus4py • "
        "sus_climate_plot_aggregate()"
    )
    caption_str = _caption(base_cap, source)
    title_str = title if title is not None else lbl["ccf_title"]

    step = 7
    breaks = list(range(-max_lag, max_lag + 1, step))

    return (
        p9.ggplot(df_ccf, p9.aes(x="lag_val", y="acf_val", fill="sig"))
        + p9.geom_col(width=0.75, color="white", size=0.2)
        + p9.geom_hline(yintercept=ci, linetype="dashed", color=_CPA_PAL["secondary"], size=0.7)
        + p9.geom_hline(yintercept=-ci, linetype="dashed", color=_CPA_PAL["secondary"], size=0.7)
        + p9.geom_hline(yintercept=0, color=_CPA_PAL["neutral"], size=0.4)
        + p9.scale_fill_manual(
            values={"False": _CPA_PAL["light"], "True": _CPA_PAL["primary"]},
            name=lbl["sig_lbl"],
        )
        + p9.scale_x_continuous(breaks=breaks)
        + p9.labs(
            title=title_str,
            subtitle=f"{col}  →  {outcome_col}  |  n = {n}",
            x=lbl["lag_lbl"],
            y=lbl["corr_lbl"],
            caption=caption_str,
        )
        + _cpa_theme()
    )


# ---------------------------------------------------------------------------
# Internal: distribution
# ---------------------------------------------------------------------------


def _cpa_distribution(
    df: pd.DataFrame, climate_cols: list[str], lbl: dict, title: str | None, source: str | None
) -> Any:
    import plotnine as p9

    long = pd.concat(
        [pd.DataFrame({"value": df[cc], "variable": cc}) for cc in climate_cols],
        ignore_index=True,
    )
    long = long.dropna(subset=["value"])

    base_cap = "climasus4py • sus_climate_plot_aggregate()"
    caption_str = _caption(base_cap, source)
    title_str = title if title is not None else lbl["dist_title"]

    ramp = _color_ramp(
        [_CPA_PAL["primary"], _CPA_PAL["tertiary"], _CPA_PAL["secondary"]], len(climate_cols)
    )
    color_map = dict(zip(climate_cols, ramp, strict=True))

    p = (
        p9.ggplot(long, p9.aes(x="value", fill="variable", color="variable"))
        + p9.geom_histogram(
            p9.aes(y=p9.after_stat("density")),
            alpha=0.35,
            bins=40,
            position="identity",
            size=0.3,
        )
        + p9.geom_density(alpha=0, size=0.8)
        + p9.scale_fill_manual(values=color_map, name=lbl["variable_lbl"])
        + p9.scale_color_manual(values=color_map, guide=None)
        + p9.labs(title=title_str, x=lbl["climate_lbl"], y=lbl["density_lbl"], caption=caption_str)
        + _cpa_theme()
    )

    if len(climate_cols) > 3:
        p = p + p9.facet_wrap("variable", scales="free", ncol=2)
    return p


# ---------------------------------------------------------------------------
# Internal: corr_matrix
# ---------------------------------------------------------------------------


def _cpa_corr_matrix(
    df: pd.DataFrame,
    climate_cols: list[str],
    outcome_col: str,
    lbl: dict,
    title: str | None,
    source: str | None,
) -> Any:
    import plotnine as p9

    cols_all = [*climate_cols, outcome_col]
    mat_data = df[cols_all].apply(pd.to_numeric, errors="coerce").dropna()
    n_cases = len(mat_data)

    cor_mat = mat_data.corr(method="spearman")

    records = []
    for i, v1 in enumerate(cols_all):
        for j, v2 in enumerate(cols_all):
            if i >= j:
                records.append({"var1": v1, "var2": v2, "corr": cor_mat.loc[v1, v2]})
    cor_df = pd.DataFrame(records)
    cor_df["var1"] = pd.Categorical(cor_df["var1"], categories=cols_all, ordered=True)
    cor_df["var2"] = pd.Categorical(
        cor_df["var2"], categories=list(reversed(cols_all)), ordered=True
    )
    cor_df["label"] = cor_df["corr"].round(2)

    base_cap = (
        f"Spearman | n = {n_cases} complete cases • climasus4py • "
        "sus_climate_plot_aggregate()"
    )
    caption_str = _caption(base_cap, source)
    title_str = title if title is not None else lbl["corr_title"]

    return (
        p9.ggplot(cor_df, p9.aes(x="var1", y="var2", fill="corr"))
        + p9.geom_tile(color="white", size=0.5)
        + p9.geom_text(p9.aes(label="label"), size=8, color="white", fontweight="bold")
        + p9.scale_fill_gradient2(
            low=_CPA_PAL["secondary"],
            mid="white",
            high=_CPA_PAL["primary"],
            midpoint=0,
            limits=(-1, 1),
            name=lbl["spearman_lbl"],
        )
        + p9.labs(
            title=title_str, subtitle=lbl["corr_subtitle"], x=None, y=None, caption=caption_str
        )
        + p9.theme(
            axis_text_x=p9.element_text(angle=35, ha="right", size=8),
            axis_text_y=p9.element_text(size=8),
        )
        + _cpa_theme()
    )


# ---------------------------------------------------------------------------
# Internal: seasonal
# ---------------------------------------------------------------------------


def _cpa_seasonal(
    df: pd.DataFrame,
    climate_cols: list[str],
    outcome_col: str,
    lbl: dict,
    title: str | None,
    source: str | None,
) -> Any:
    import plotnine as p9

    col = climate_cols[0]
    mo_lbl = lbl["months"]

    df_s = pd.DataFrame(
        {
            "month_num": df["date"].dt.month,
            "climate_val": pd.to_numeric(df[col], errors="coerce"),
            "outcome_val": pd.to_numeric(df[outcome_col], errors="coerce"),
        }
    ).dropna(subset=["climate_val", "outcome_val"])
    df_s["month_lbl"] = pd.Categorical(
        [mo_lbl[m - 1] for m in df_s["month_num"]], categories=mo_lbl, ordered=True
    )

    base_cap = "climasus4py • sus_climate_plot_aggregate()"
    caption_str = _caption(base_cap, source)
    title_str = title if title is not None else lbl["seas_title"]
    unit_lbl = _unit_label(col)

    p_clim = (
        p9.ggplot(df_s, p9.aes(x="month_lbl", y="climate_val"))
        + p9.geom_boxplot(
            fill=_CPA_PAL["light"], color=_CPA_PAL["primary"], outlier_size=1, outlier_alpha=0.5
        )
        + p9.labs(title=title_str, subtitle=f"{col}{unit_lbl}", x=None, y=f"{col}{unit_lbl}")
        + _cpa_theme()
    )
    p_out = (
        p9.ggplot(df_s, p9.aes(x="month_lbl", y="outcome_val"))
        + p9.geom_boxplot(
            fill="#F5D5C8", color=_CPA_PAL["secondary"], outlier_size=1, outlier_alpha=0.5
        )
        + p9.labs(x=lbl["month_lbl"], y=outcome_col, caption=caption_str)
        + _cpa_theme()
    )

    # NOTE: R falls back to p_clim alone when the optional `patchwork`
    # package is unavailable. plotnine ships composition (`/`, `|`)
    # natively (no optional dependency), so this port always stacks the two
    # panels -- a strict improvement over the R fallback path, not a
    # behaviour change under normal (patchwork-installed) R usage.
    return p_clim / p_out


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_plot_aggregate(
    df: pd.DataFrame | duckdb.DuckDBPyRelation,
    outcome_col: str | None = None,
    climate_cols: list[str] | None = None,
    plot_type: Literal[
        "timeseries", "scatter", "ccf", "distribution", "corr_matrix", "seasonal"
    ] = "timeseries",
    smooth_method: Literal["loess", "gam"] = "loess",
    max_lag: int = 30,
    alpha: float = 0.05,
    interactive: bool = False,
    lang: str = "pt",
    verbose: bool = True,
    title: str | None = None,
    source: str | None = None,
) -> Any:
    """Visualise climate-health aggregate data (exploratory plots).

    Produces exploratory ``plotnine.ggplot`` visualisations for a daily
    climate-health table: time-series overlay, scatter with smooth,
    cross-correlation (CCF), distribution, correlation matrix, or seasonal
    boxplots. Mirrors ``climasus4r::sus_climate_plot_aggregate()``.

    Modelling-specific plots (DLNM surfaces, residual diagnostics, RR
    tables) are deliberately excluded, matching the R scope note — those
    belong to the (not yet ported) ``sus_mod_plot_*`` family.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        df: Table with at least a ``date`` column, a health-outcome column,
            and one or more climate exposure columns. Accepts a
            ``pandas.DataFrame`` or a ``duckdb.DuckDBPyRelation`` (the
            latter is materialised via ``.df()`` immediately — this is a
            plotting utility, not a lazy pipeline stage). A ``geometry``
            column (e.g. from a GeoDataFrame) is dropped if present.
        outcome_col: Name of the health-outcome column (e.g. ``"n_obitos"``).
            If ``None`` (default), the first numeric column that is not a
            climate variable is auto-detected.
        climate_cols: Climate column names to visualise. If ``None``
            (default), columns are auto-detected from naming conventions
            (``lag*_``, ``mvwin*_``, ``off*to*_``, ``gdd*``, ``*_lag*``, or
            known INMET/climate-index prefixes).
        plot_type: One of ``"timeseries"``, ``"scatter"``, ``"ccf"``,
            ``"distribution"``, ``"corr_matrix"``, ``"seasonal"``.
            Default ``"timeseries"``.
        smooth_method: Smoothing method for the ``"scatter"`` plot:
            ``"loess"`` (default) or ``"gam"``. plotnine has no GAM
            smoother backend (unlike R's ``mgcv::gam``); ``"gam"`` falls
            back to ``"loess"`` with a warning.
        max_lag: Maximum lag (days) shown in the ``"ccf"`` plot. Default 30.
        alpha: Confidence level for CCF significance bounds (default
            ``0.05``; bounds drawn at +-2/sqrt(n)) and for the scatter-plot
            smoother's confidence band.
        interactive: If ``True``, would return a ``plotly`` interactive
            version (mirroring R's native dual-axis ``plotly`` path).
            **Not currently supported** — ``plotly`` is not bundled with
            climasus4py; raises ``ImportError``. See IDEIAS.md.
        lang: Language for axis labels and titles: ``"pt"`` (default),
            ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.
        title: Override the auto-generated plot title. Default ``None``
            uses the built-in multilingual title.
        source: Data source attribution prepended to the plot caption.
            Default ``None``.

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``).
        When multiple ``climate_cols`` are supplied for ``"timeseries"`` or
        ``"scatter"``, or always for ``"seasonal"``, the result is a
        plotnine composed plot (stacked via ``/``) — the native plotnine
        equivalent of R's optional ``patchwork`` stacking.

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if ``interactive=True``.
        ValueError: If *lang*, *plot_type*, or *smooth_method* is invalid,
            *df* has no ``date`` column, or *outcome_col*/*climate_cols*
            cannot be found/auto-detected.

    Example:
        >>> import climasus4py as cs
        >>> p = cs.sus_climate_plot_aggregate(df_agg, plot_type="timeseries", lang="pt")
        >>> p.draw()
        >>> p2 = cs.sus_climate_plot_aggregate(df_lag, plot_type="corr_matrix", lang="en")
    """
    if lang not in _I18N:
        raise ValueError(f"lang must be one of {sorted(_I18N)!r}, got {lang!r}.")
    lbl = _I18N[lang]

    if plot_type not in _VALID_PLOT_TYPES:
        raise ValueError(
            f"plot_type must be one of {list(_VALID_PLOT_TYPES)!r}, got {plot_type!r}. "
            "Modelling plots (DLNM, residuals, RR) belong to sus_mod_plot_dlnm "
            "(not yet ported)."
        )

    if interactive:
        raise ImportError(
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike climasus4r's plotly "
            "path). Install plotly manually if needed; see IDEIAS.md for the "
            "open decision on adding it as a first-class extra."
        )

    _require_plotnine()

    df = df.df() if not isinstance(df, pd.DataFrame) else df.copy()

    if hasattr(df, "geometry") and "geometry" in df.columns:
        df = pd.DataFrame(df.drop(columns=["geometry"]))

    if "date" not in df.columns:
        raise ValueError("df must contain a 'date' column.")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    if climate_cols is None:
        climate_cols = _detect_climate_cols(df)
        if not climate_cols:
            raise ValueError(
                "No climate columns detected in df. Pass column names explicitly "
                "via climate_cols."
            )
    else:
        missing_c = [c for c in climate_cols if c not in df.columns]
        if missing_c:
            raise ValueError(f"Climate column(s) not found: {missing_c}.")

    if outcome_col is None:
        outcome_col = _detect_outcome_col(df, climate_cols)
        if outcome_col is None:
            raise ValueError(
                "No health-outcome column detected in df. Pass the column name "
                "via outcome_col."
            )
    elif outcome_col not in df.columns:
        raise ValueError(f"Outcome column '{outcome_col}' not found in df.")

    if verbose:
        type_lbl = lbl["type_label"].get(plot_type, plot_type)
        print(f"== {lbl['title']} - {type_lbl} ==")
        print(
            f"{lbl['n_rows']}: {len(df)} | {lbl['outcome_lbl']}: {outcome_col} | "
            f"{lbl['climate_lbl']}: {', '.join(climate_cols)}"
        )

    if plot_type == "timeseries":
        p = _cpa_timeseries(df, climate_cols, outcome_col, lbl, title, source)
    elif plot_type == "scatter":
        p = _cpa_scatter(df, climate_cols, outcome_col, smooth_method, alpha, lbl, title, source)
    elif plot_type == "ccf":
        p = _cpa_ccf(df, climate_cols, outcome_col, max_lag, alpha, lbl, title, source)
    elif plot_type == "distribution":
        p = _cpa_distribution(df, climate_cols, lbl, title, source)
    elif plot_type == "corr_matrix":
        p = _cpa_corr_matrix(df, climate_cols, outcome_col, lbl, title, source)
    else:  # "seasonal"
        p = _cpa_seasonal(df, climate_cols, outcome_col, lbl, title, source)

    if verbose:
        print(lbl["done"])

    return p
