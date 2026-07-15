"""Time series visualization of aggregated SUS health data.

Mirrors R: sus_data_plot_aggregate_ts.R
"""

from __future__ import annotations

import os
import tempfile
import warnings

import numpy as np
import pandas as pd

from ..utils.data import load_json

# ---------------------------------------------------------------------------
# Column candidates (stay in code — tied to aggregate output names)
# ---------------------------------------------------------------------------

_TS_OUTCOME_CANDIDATES = [
    "n_obitos", "n_internacoes", "n_nascimentos", "n_casos",
    "n_procedimentos", "n_estabelecimentos",
    "n_deaths", "n_hospitalizations", "n_births", "n_cases",
    "n_procedures", "n_establishments",
    "n_muertes", "n_hospitalizaciones", "n_nacimientos",
    "n_procedimientos", "n_establecimientos",
    "count", "n", "total",
]

_TS_DATE_CANDIDATES = [
    "date", "data", "DT_NOTIFIC", "DTOBITO", "DT_INTER",
    "DTNASC", "DT_COMPET",
]

_TS_PLOT_TYPES = {"epidemic", "seasonal", "heatmap", "trend"}

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_viz_labels() -> dict:
    try:
        return load_json("viz/viz_labels.json")
    except FileNotFoundError:
        return {}


def _load_viz_config() -> dict:
    try:
        return load_json("viz/viz_config.json")
    except FileNotFoundError:
        return {}


def _load_calendar() -> dict:
    try:
        return load_json("templates/calendar_labels.json")
    except FileNotFoundError:
        return {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tsm(key: str, lang: str = "pt", **kwargs) -> str:
    labels = _load_viz_labels()
    row = labels.get(key, {})
    txt = row.get(lang, row.get("pt", key))
    return txt.format(**kwargs) if kwargs else txt


def _ts_palette(name: str = "lancet") -> list:
    cfg = _load_viz_config()
    palettes = cfg.get("palettes", {})
    return palettes.get(name, palettes.get("lancet", [
        "#00468B", "#ED0000", "#42B540", "#0099B4", "#925E9F",
        "#FDAF91", "#AD002A", "#ADB6B6", "#1B1919",
    ]))


def _ts_month_labels(lang: str = "pt") -> list:
    cal = _load_calendar()
    abbr = cal.get("month_names_abbr", {})
    return abbr.get(lang, abbr.get("en",
        ["Jan","Feb","Mar","Apr","May","Jun",
         "Jul","Aug","Sep","Oct","Nov","Dec"]))


def _ts_detect_value_col(columns: list, value_col: str | None) -> str:
    if value_col is not None:
        if value_col not in columns:
            raise ValueError(f"value_col {value_col!r} not found.")
        return value_col
    for c in _TS_OUTCOME_CANDIDATES:
        if c in columns:
            return c
    raise ValueError(
        "Outcome column not detected. "
        "Specify value_col= or run sus_data_aggregate() first."
    )


def _ts_detect_date_col(columns: list) -> str:
    for c in _TS_DATE_CANDIDATES:
        if c in columns:
            return c
    raise ValueError("Date column not found.")


def _ts_prepare(df: pd.DataFrame, value_col: str, date_col: str) -> pd.DataFrame:
    out = df.copy()
    out["_date"]  = pd.to_datetime(out[date_col], errors="coerce")
    out = out[out["_date"].notna()].copy()
    out["_year"]  = out["_date"].dt.year
    out["_month"] = out["_date"].dt.month
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    return out


def _ts_theme(base_size: int = 11):
    from plotnine import (theme_classic, theme, element_text,
                          element_line, element_blank)
    return (
        theme_classic(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold", size=base_size + 1, ha="left"),
            plot_subtitle=element_text(color="grey", size=base_size - 1, ha="left"),
            plot_caption=element_text(color="grey", size=base_size - 2, ha="left"),
            panel_grid_major_y=element_line(color="#eeeeee", size=0.3),
            panel_grid_minor=element_blank(),
            legend_position="top",
            legend_key=element_blank(),
        )
    )

# ---------------------------------------------------------------------------
# Internal plot functions
# ---------------------------------------------------------------------------

def _ts_epidemic(df, value_col, group_col, facet_col, facet_ncol,
                 smooth_method, log_transform, free_scales,
                 pal, title, subtitle, caption, base_size, lang):
    from plotnine import (ggplot, aes, geom_line, geom_point, geom_smooth,
                          facet_wrap, scale_color_manual, scale_x_datetime,
                          labs, theme, element_text)
    keys = ["_date"]
    if group_col and group_col in df.columns:
        keys.append(group_col)

    agg = df.groupby(keys)[value_col].sum().reset_index()
    if log_transform:
        agg[value_col] = np.log1p(agg[value_col])

    if group_col and group_col in df.columns:
        agg[group_col] = agg[group_col].astype(str)
        n_grp  = agg[group_col].nunique()
        colors = (pal * ((n_grp // len(pal)) + 1))[:n_grp]
        col_map = dict(zip(agg[group_col].unique(), colors))
        p = (
            ggplot(agg, aes(x="_date", y=value_col,
                            color=group_col, group=group_col))
            + geom_line(size=0.7)
            + geom_point(size=1.3)
            + scale_color_manual(values=col_map, name=None)
        )
    else:
        p = (
            ggplot(agg, aes(x="_date", y=value_col))
            + geom_line(size=0.7, color=pal[0])
            + geom_point(size=1.3, color=pal[0])
        )

    n_pts = len(agg["_date"].unique())
    if smooth_method and smooth_method != "none" and n_pts >= 6:
        try:
            use_method = "lm" if smooth_method in ("loess", "gam") else smooth_method
            p = p + geom_smooth(
                method=use_method, se=False,
                color="#333333", linetype="dashed", size=0.6
            )
        except Exception:
            pass

    if facet_col and facet_col in df.columns:
        scales = "free_y" if free_scales else "fixed"
        p = p + facet_wrap(f"~{facet_col}", ncol=facet_ncol, scales=scales)

    y_lab = _tsm("ts_axis_count", lang)
    if log_transform:
        y_lab += " (log1p)"

    return (
        p
        + scale_x_datetime(date_labels="%Y-%m")
        + labs(title=title or _tsm("ts_epidemic_title", lang),
               subtitle=subtitle, x=_tsm("ts_axis_date", lang),
               y=y_lab, caption=caption)
        + _ts_theme(base_size)
        + theme(axis_text_x=element_text(angle=45, ha="right"))
    )


def _ts_seasonal(df, value_col, group_col, facet_col, facet_ncol,
                 log_transform, free_scales, pal, title, subtitle,
                 caption, base_size, lang):
    from plotnine import (ggplot, aes, geom_col, geom_boxplot,
                          facet_wrap, scale_fill_manual, labs)
    month_labels = _ts_month_labels(lang)

    keys = ["_year", "_month"]
    if group_col and group_col in df.columns:
        keys.append(group_col)

    monthly = df.groupby(keys)[value_col].sum().reset_index()
    monthly["month_lbl"] = monthly["_month"].map(
        lambda m: month_labels[int(m) - 1]
    )
    monthly["month_lbl"] = pd.Categorical(
        monthly["month_lbl"], categories=month_labels, ordered=True
    )

    if log_transform:
        monthly[value_col] = np.log1p(monthly[value_col])

    if group_col and group_col in df.columns:
        monthly[group_col] = monthly[group_col].astype(str)
        n_grp  = monthly[group_col].nunique()
        colors = (pal * ((n_grp // len(pal)) + 1))[:n_grp]
        col_map = dict(zip(monthly[group_col].unique(), colors))
        p = (
            ggplot(monthly, aes(x="month_lbl", y=value_col, fill=group_col))
            + geom_boxplot(alpha=0.7)
            + scale_fill_manual(values=col_map, name=None)
        )
    else:
        mean_month = (monthly.groupby("month_lbl", observed=True)[value_col]
                      .mean().reset_index())
        p = (
            ggplot(mean_month, aes(x="month_lbl", y=value_col))
            + geom_col(fill=pal[0], width=0.7)
        )

    if facet_col and facet_col in df.columns:
        scales = "free_y" if free_scales else "fixed"
        p = p + facet_wrap(f"~{facet_col}", ncol=facet_ncol, scales=scales)

    y_lab = _tsm("ts_axis_count", lang)
    if log_transform:
        y_lab += " (log1p)"

    return (
        p
        + labs(title=title or _tsm("ts_seasonal_title", lang),
               subtitle=subtitle, x=_tsm("ts_axis_month", lang),
               y=y_lab, caption=caption)
        + _ts_theme(base_size)
    )


def _ts_heatmap(df, value_col, facet_col, facet_ncol, log_transform,
                free_scales, pal, title, subtitle, caption, base_size, lang):
    from plotnine import (ggplot, aes, geom_tile, scale_fill_gradientn,
                          labs, theme_classic, theme, element_text,
                          element_blank)
    month_labels = _ts_month_labels(lang)

    heat = df.groupby(["_year", "_month"])[value_col].sum().reset_index()
    heat["month_lbl"] = heat["_month"].map(lambda m: month_labels[int(m) - 1])
    heat["month_lbl"] = pd.Categorical(
        heat["month_lbl"], categories=month_labels, ordered=True
    )
    heat["_year"] = heat["_year"].astype(int).astype(str)

    if log_transform:
        heat[value_col] = np.log1p(heat[value_col])

    fill_lab = _tsm("ts_axis_count", lang)
    if log_transform:
        fill_lab += " (log1p)"

    return (
        ggplot(heat, aes(x="month_lbl", y="_year", fill=value_col))
        + geom_tile(color="white", size=0.3)
        + scale_fill_gradientn(
            colors=["#FFF5F0", "#FCBBA1", "#FC6D4C", "#D32020", "#67000D"],
            name=fill_lab,
        )
        + labs(title=title or _tsm("ts_heatmap_title", lang),
               subtitle=subtitle, x=_tsm("ts_axis_month", lang),
               y=_tsm("ts_axis_year", lang), caption=caption)
        + theme_classic(base_size=base_size)
        + theme(
            axis_ticks=element_blank(),
            axis_line=element_blank(),
            panel_grid=element_blank(),
            legend_position="right",
            plot_title=element_text(face="bold", size=base_size + 1),
            plot_subtitle=element_text(color="grey", size=base_size - 1),
        )
    )


def _ts_trend(df, value_col, facet_col, facet_ncol, free_scales,
              pal, title, subtitle, caption, base_size, lang):
    from plotnine import (ggplot, aes, geom_line, geom_point, geom_smooth,
                          facet_wrap, scale_x_datetime, labs, theme,
                          element_text)
    agg = df.groupby("_date")[value_col].sum().reset_index()

    p = (
        ggplot(agg, aes(x="_date", y=value_col))
        + geom_line(size=0.5, color="#AAAAAA")
        + geom_point(size=1.0, color="#AAAAAA", alpha=0.5)
    )

    if len(agg) >= 3:
        try:
            p = p + geom_smooth(
                method="lm", se=True,
                color=pal[1], fill=pal[1], alpha=0.15, size=0.9
            )
        except Exception:
            pass

    if facet_col and facet_col in df.columns:
        scales = "free_y" if free_scales else "fixed"
        p = p + facet_wrap(f"~{facet_col}", ncol=facet_ncol, scales=scales)

    return (
        p
        + scale_x_datetime(date_labels="%Y-%m")
        + labs(title=title or _tsm("ts_trend_title", lang),
               subtitle=subtitle, x=_tsm("ts_axis_date", lang),
               y=_tsm("ts_axis_count", lang), caption=caption)
        + _ts_theme(base_size)
        + theme(axis_text_x=element_text(angle=45, ha="right"))
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_plot_aggregate_ts(
    df,
    *,
    value_col: str | None = None,
    group_col: str | None = None,
    facet_col: str | None = None,
    facet_ncol: int = 2,
    plot_type: str | list = "epidemic",
    smooth_method: str = "loess",
    log_transform: bool = False,
    free_scales: bool = False,
    palette: str = "lancet",
    title: str | None = None,
    subtitle: str | None = None,
    caption: str | None = None,
    base_size: int = 11,
    save_path: str | None = None,
    width: float = 10,
    height: float = 5,
    dpi: int = 150,
    lang: str = "pt",
    verbose: bool = True,
):
    """Time series visualization of aggregated SUS health data.

    Mirrors ``climasus4r::sus_data_plot_aggregate_ts()``.

    Args:
        df: Output of ``sus_data_aggregate()`` — DataFrame or DuckDBPyRelation.
        value_col: Outcome column (auto-detected from n_deaths, n_cases...).
        group_col: Colour stratification column (e.g. ``"sex"``).
        facet_col: Faceting variable.
        facet_ncol: Number of facet columns.
        plot_type: ``"epidemic"``, ``"seasonal"``, ``"heatmap"``,
            ``"trend"`` — or a list for multiple combined plots.
        smooth_method: ``"loess"``, ``"lm"``, ``"gam"``, ``"none"``.
        log_transform: Apply log1p to Y axis.
        free_scales: Free Y scales in facets.
        palette: Colour palette — ``"lancet"``, ``"nature"``, ``"nejm"``,
            ``"jco"``, ``"aaas"``, ``"sus"``, ``"colorblind"``.
        title / subtitle / caption: Plot text (auto-generated if None).
        base_size: Base font size.
        save_path: File path to save (None = no save).
        width / height: Dimensions in inches.
        dpi: Resolution.
        lang: Language — ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages.

    Returns:
        ``ggplot`` (single type) or ``PIL.Image`` (multiple types combined).

    Example:
        >>> agg = cs.sus_data_aggregate(stand, time_unit="month", system="SIM-DO")
        >>> cs.sus_data_plot_aggregate_ts(agg, plot_type="epidemic", lang="pt")
        >>> cs.sus_data_plot_aggregate_ts(agg,
        ...     plot_type=["epidemic", "seasonal"], lang="en")
    """
    if hasattr(df, "df") and not isinstance(df, pd.DataFrame):
        df = df.df()
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a DataFrame or DuckDBPyRelation.")
    if len(df) == 0:
        raise ValueError("df is empty.")

    types = [plot_type] if isinstance(plot_type, str) else list(plot_type)
    bad   = set(types) - _TS_PLOT_TYPES
    if bad:
        raise ValueError(f"Invalid plot_type: {bad}. Options: {_TS_PLOT_TYPES}")

    columns   = list(df.columns)
    value_col = _ts_detect_value_col(columns, value_col)
    date_col  = _ts_detect_date_col(columns)

    if verbose:
        print(f"[ts] value_col={value_col!r} | date_col={date_col!r} | types={types}")

    pal  = _ts_palette(palette)
    work = _ts_prepare(df, value_col, date_col)

    n_obs     = len(work)
    start     = work["_date"].min().strftime("%Y-%m")
    end       = work["_date"].max().strftime("%Y-%m")
    plot_sub  = subtitle or _tsm("ts_subtitle_fmt", lang,
                                  n=f"{n_obs:,}", col=value_col,
                                  start=start, end=end)
    plot_cap  = caption or f"{_tsm('source', lang)} | climasus4py"

    plots = []
    for tp in types:
        if tp == "epidemic":
            p = _ts_epidemic(work, value_col, group_col, facet_col, facet_ncol,
                             smooth_method, log_transform, free_scales,
                             pal, title, plot_sub, plot_cap, base_size, lang)
        elif tp == "seasonal":
            p = _ts_seasonal(work, value_col, group_col, facet_col, facet_ncol,
                             log_transform, free_scales, pal, title, plot_sub,
                             plot_cap, base_size, lang)
        elif tp == "heatmap":
            p = _ts_heatmap(work, value_col, facet_col, facet_ncol,
                            log_transform, free_scales, pal, title, plot_sub,
                            plot_cap, base_size, lang)
        elif tp == "trend":
            p = _ts_trend(work, value_col, facet_col, facet_ncol, free_scales,
                          pal, title, plot_sub, plot_cap, base_size, lang)
        plots.append(p)

    if len(plots) == 1:
        result = plots[0]
        if save_path:
            result.save(save_path, dpi=dpi, width=width, height=height, verbose=False)
            if verbose:
                print(f"[ts] Saved: {save_path}")
        return result

    # multiple types — combine vertically with PIL
    from PIL import Image
    tmp_paths = []
    for p in plots:
        tmp = tempfile.mktemp(suffix=".png")
        p.save(tmp, dpi=dpi, width=width, height=height, verbose=False)
        tmp_paths.append(tmp)

    imgs     = [Image.open(t) for t in tmp_paths]
    total_w  = max(im.width for im in imgs)
    total_h  = sum(im.height for im in imgs)
    combined = Image.new("RGB", (total_w, total_h), "white")
    y = 0
    for im in imgs:
        combined.paste(im, (0, y))
        y += im.height

    for t in tmp_paths:
        try: os.remove(t)
        except Exception: pass

    if save_path:
        combined.save(save_path)
        if verbose:
            print(f"[ts] Saved: {save_path}")

    return combined
