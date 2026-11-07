"""Demographic profile visualization.

Mirrors R: sus_data_plot_demographics.R
"""

from __future__ import annotations

import os
import tempfile
import warnings

import numpy as np
import pandas as pd

from ..utils.data import load_json

# ---------------------------------------------------------------------------
# Config loaders
# ---------------------------------------------------------------------------

def _load_labels() -> dict:
    try:
        return load_json("viz/viz_labels.json")
    except FileNotFoundError:
        return {}


def _load_config() -> dict:
    try:
        return load_json("viz/viz_config.json")
    except FileNotFoundError:
        return {}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEMO_TYPES = {
    "table", "bar", "pyramid", "heatmap",
    "temporal", "climate", "race_equity", "dashboard",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vl(key: str, lang: str = "pt") -> str:
    """Get multilingual label from viz_labels.json."""
    labels = _load_labels()
    row = labels.get(key)
    if row is None:
        return key
    return row.get(lang, row.get("pt", key))


def _palette(name: str = "lancet") -> list:
    """Get colour palette from viz_config.json."""
    cfg = _load_config()
    palettes = cfg.get("palettes", {})
    if name not in palettes:
        warnings.warn(f"Palette {name!r} not found; using 'lancet'.")
        name = "lancet"
    return palettes.get(name, [
        "#00468B", "#ED0000", "#42B540", "#0099B4", "#925E9F",
        "#FDAF91", "#AD002A", "#ADB6B6", "#1B1919",
    ])


def _find_col(df: pd.DataFrame, patterns: list) -> str | None:
    for p in patterns:
        if p in df.columns:
            return p
    return None


def _detect_demo_cols(df: pd.DataFrame) -> dict:
    cfg = _load_config().get("demo_column_patterns", {})
    return {k: _find_col(df, v) for k, v in cfg.items()}


def _to_df(data) -> pd.DataFrame:
    try:
        import duckdb
        if isinstance(data, duckdb.DuckDBPyRelation):
            return data.df()
    except ImportError:
        pass
    if isinstance(data, pd.DataFrame):
        return data.copy()
    if hasattr(data, "df"):
        return data.df()
    raise TypeError(
        f"Expected DuckDBPyRelation or DataFrame, got {type(data)!r}."
    )


def _lancet_theme(base_size: int = 11):
    from plotnine import (theme_classic, theme, element_text,
                          element_line, element_blank)
    return (
        theme_classic(base_size=base_size)
        + theme(
            plot_title=element_text(face="bold", size=base_size + 1,
                                    ha="left", margin={"b": 4}),
            plot_subtitle=element_text(color="grey", size=base_size - 1, ha="left"),
            plot_caption=element_text(color="grey", size=base_size - 2, ha="left"),
            axis_title=element_text(size=base_size - 0.5, color="#333333"),
            axis_text=element_text(size=base_size - 1, color="#333333"),
            panel_grid_major_y=element_line(color="#eeeeee", size=0.3),
            panel_grid_major_x=element_blank(),
            panel_grid_minor=element_blank(),
            legend_background=element_blank(),
            legend_key=element_blank(),
            legend_text=element_text(size=base_size - 1),
        )
    )


def _age_group_order(values: list) -> list:
    import re
    def _lead(s):
        m = re.match(r"^\d+", str(s))
        return int(m.group()) if m else 9999
    return sorted(set(values), key=_lead)


# ---------------------------------------------------------------------------
# Internal plot functions
# ---------------------------------------------------------------------------

def _vd_table(df, var, lang):
    demo_cols = _detect_demo_cols(df)
    if var is None:
        rows = []
        for dim, col in demo_cols.items():
            if col is None or col not in df.columns:
                continue
            tab = df[df[col].notna()].groupby(col).size().reset_index(name="n")
            tab["category"]  = tab[col].astype(str)
            tab["dimension"] = _vl(dim, lang)
            tab["pct"]       = (100 * tab["n"] / tab["n"].sum()).round(1)
            rows.append(tab[["dimension", "category", "n", "pct"]]
                        .sort_values("n", ascending=False))
        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    col = demo_cols.get(var)
    if col is None or col not in df.columns:
        raise ValueError(f"Column for {var!r} not found.")
    tab = df[df[col].notna()].groupby(col).size().reset_index(name="n")
    tab["category"] = tab[col].astype(str)
    tab["pct"]      = (100 * tab["n"] / tab["n"].sum()).round(1)
    return tab[["category", "n", "pct"]].sort_values("n", ascending=False).reset_index(drop=True)


def _vd_bar(df, var, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_col, geom_text, coord_flip,
                          scale_fill_manual, scale_y_continuous, labs, theme,
                          element_blank)
    if var is None:
        raise ValueError("var is required for type='bar'.")
    demo_cols = _detect_demo_cols(df)
    col = demo_cols.get(var)
    if col is None or col not in df.columns:
        raise ValueError(f"Column for {var!r} not found.")
    pal = _palette(palette)
    bar = (df[df[col].notna()].groupby(col).size()
           .reset_index(name="n").rename(columns={col: "category"}))
    bar["category"] = bar["category"].astype(str)
    bar["pct"]      = (100 * bar["n"] / bar["n"].sum()).round(1)
    bar["pct_lbl"]  = bar["pct"].astype(str) + "%"
    bar = bar.sort_values("n")
    bar["category"] = pd.Categorical(bar["category"], categories=bar["category"].tolist())
    n_cats = len(bar)
    colors = (pal * ((n_cats // len(pal)) + 1))[:n_cats]
    title  = _vl(f"bar_title_{var}", lang)
    return (
        ggplot(bar, aes(x="category", y="n", fill="category"))
        + geom_col(width=0.72, show_legend=False)
        + geom_text(aes(label="pct_lbl"), ha="left", size=base_size * 0.8,
                    color="grey", nudge_y=bar["n"].max() * 0.02)
        + coord_flip()
        + scale_fill_manual(values=colors)
        + scale_y_continuous(expand=(0, 0, 0.18, 0),
                             labels=lambda lst: [f"{int(x):,}" for x in lst])
        + labs(title=title, x=None, y=_vl("count", lang), caption=caption)
        + _lancet_theme(base_size)
        + theme(axis_line_y=element_blank(), panel_grid_major_y=element_blank())
    )


def _vd_pyramid(df, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_col, geom_hline, coord_flip,
                          scale_y_continuous, scale_fill_manual, labs, theme,
                          element_blank)
    cfg = _load_config()
    male_vals   = set(cfg.get("male_values",   ["Male","Masculino","M","1","male","masculino"]))
    female_vals = set(cfg.get("female_values", ["Female","Feminino","F","2","female","feminino"]))

    age_col = _find_col(df, ["ibge_age_group", "faixa_etaria_ibge",
                              "grupo_edad_ibge", "age_group", "faixa_etaria"])
    sex_col = _find_col(df, ["sex", "sexo", "SEXO"])
    if age_col is None or sex_col is None:
        raise ValueError("Pyramid requires age group AND sex columns.")

    lab_male   = _vl("male",   lang)
    lab_female = _vl("female", lang)

    pyr = df[df[age_col].notna() & df[sex_col].notna()].copy()
    pyr["sex_label"] = pyr[sex_col].astype(str).map(
        lambda v: lab_male if v in male_vals
        else (lab_female if v in female_vals else None)
    )
    pyr = pyr[pyr["sex_label"].notna()]
    agg = (pyr.groupby([age_col, "sex_label"]).size()
           .reset_index(name="n").rename(columns={age_col: "age_group"}))
    agg["n"]      = agg["n"].astype(float)
    agg["n_plot"] = agg.apply(
        lambda r: -r["n"] if r["sex_label"] == lab_male else r["n"], axis=1
    )
    ordered = _age_group_order(agg["age_group"].unique().tolist())
    agg["age_group"] = pd.Categorical(agg["age_group"], categories=ordered)
    tot_m  = agg.loc[agg["sex_label"] == lab_male,   "n"].sum()
    tot_f  = agg.loc[agg["sex_label"] == lab_female, "n"].sum()
    ratio  = round(tot_m / tot_f, 2) if tot_f > 0 else float("nan")
    max_n  = agg["n"].abs().max()
    col_map = {lab_male: "#2166AC", lab_female: "#D6604D"}
    return (
        ggplot(agg, aes(x="age_group", y="n_plot", fill="sex_label"))
        + geom_col(width=0.82)
        + geom_hline(yintercept=0, color="white", size=0.4)
        + coord_flip()
        + scale_y_continuous(
            breaks=[-max_n*0.75, -max_n*0.5, -max_n*0.25, 0,
                     max_n*0.25,  max_n*0.5,  max_n*0.75],
            labels=lambda lst: [f"{abs(int(x)):,}" for x in lst],
            expand=(0.04, 0),
        )
        + scale_fill_manual(values=col_map, name=None)
        + labs(
            title    = _vl("pyramid_title", lang),
            subtitle = f"{_vl('sex_ratio', lang)}: {ratio}",
            x        = _vl("age_group",    lang),
            y        = _vl("count",        lang),
            caption  = caption,
        )
        + _lancet_theme(base_size)
        + theme(legend_position="top")
    )


def _vd_temporal(df, time_unit, fill_var, show_ci, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_line, geom_point, geom_ribbon,
                          scale_color_manual, scale_fill_manual,
                          scale_y_continuous, labs, theme, element_text)
    cfg      = _load_config()
    patterns = cfg.get("time_patterns", {})
    time_col = _find_col(df, patterns.get(time_unit, [time_unit]))
    if time_col is None:
        raise ValueError(f"Column for time_unit={time_unit!r} not found.")
    pal      = _palette(palette)
    fill_col = None
    if fill_var:
        fill_col = _find_col(df, [fill_var, f"{fill_var}_group"])
        if fill_col is None:
            warnings.warn(f"fill_var {fill_var!r} not found; ignoring.")

    if fill_col:
        agg = (df[df[time_col].notna() & df[fill_col].notna()]
               .groupby([time_col, fill_col]).size().reset_index(name="n")
               .rename(columns={time_col: "time_x", fill_col: "group"}))
    else:
        agg = (df[df[time_col].notna()].groupby(time_col).size()
               .reset_index(name="n").rename(columns={time_col: "time_x"}))
        agg["group"] = _vl("count", lang)

    agg["n"] = agg["n"].astype(float)
    try:
        agg["_sort_key"] = pd.to_numeric(agg["time_x"], errors="coerce")
        agg = agg.sort_values("_sort_key").drop(columns="_sort_key")
    except Exception:
        agg = agg.sort_values("time_x")

    agg["time_x"] = agg["time_x"].astype(str)
    agg["time_x"] = pd.Categorical(
        agg["time_x"], categories=agg["time_x"].unique().tolist(), ordered=True
    )

    if show_ci:
        agg["ci_lo"] = agg["n"].apply(lambda x: max(0, x - 1.96 * np.sqrt(x)))
        agg["ci_hi"] = agg["n"].apply(lambda x: x + 1.96 * np.sqrt(x))

    n_groups = agg["group"].nunique()
    colors   = (pal * ((n_groups // len(pal)) + 1))[:n_groups]
    col_map  = dict(zip(agg["group"].unique().tolist(), colors))

    p = (
        ggplot(agg, aes(x="time_x", y="n", color="group", group="group"))
        + geom_line(size=0.8)
        + geom_point(size=1.8, shape="o")
        + scale_color_manual(values=col_map, name=None)
        + scale_y_continuous(expand=(0, 0, 0.08, 0),
                             labels=lambda lst: [f"{int(x):,}" for x in lst])
        + labs(title=_vl("temporal_title", lang),
               x=_vl(time_unit, lang), y=_vl("count", lang), caption=caption)
        + _lancet_theme(base_size)
        + theme(legend_position="top")
    )

    if show_ci:
        p = (p + geom_ribbon(aes(ymin="ci_lo", ymax="ci_hi", fill="group"),
                             alpha=0.12, color=None, show_legend=False)
               + scale_fill_manual(values=col_map))

    if agg["time_x"].nunique() > 12:
        p = p + theme(axis_text_x=element_text(angle=45, ha="right"))

    return p


def _vd_heatmap(df, row_var, col_var, fill_metric, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_tile, geom_text,
                          scale_fill_gradientn, scale_color_manual,
                          scale_y_discrete, labs, theme_classic, theme,
                          element_text, element_blank)
    demo_cols = _detect_demo_cols(df)
    avail     = [k for k, v in demo_cols.items() if v is not None]
    if row_var is None:
        row_var = "ibge_age_group" if "ibge_age_group" in avail \
                  else ("age_group" if "age_group" in avail else avail[0])
    if col_var is None:
        col_var = next((k for k in ["race", "sex"] if k in avail and k != row_var),
                       next((k for k in avail if k != row_var), None))
    if col_var is None or row_var == col_var:
        raise ValueError("heatmap_row and heatmap_col must be different variables.")

    row_col = demo_cols.get(row_var) or _find_col(df, [row_var])
    col_col = demo_cols.get(col_var) or _find_col(df, [col_var])
    if row_col is None or row_col not in df.columns:
        raise ValueError(f"Row variable {row_var!r} not found.")
    if col_col is None or col_col not in df.columns:
        raise ValueError(f"Column variable {col_var!r} not found.")

    valid_metrics = {"pct_row", "pct_col", "pct_total", "count"}
    if fill_metric not in valid_metrics:
        fill_metric = "pct_row"

    heat = (df[df[row_col].notna() & df[col_col].notna()]
            .groupby([row_col, col_col]).size().reset_index(name="n")
            .rename(columns={row_col: "row_val", col_col: "col_val"}))
    heat["row_val"] = heat["row_val"].astype(str)
    heat["col_val"] = heat["col_val"].astype(str)
    heat["n"]       = heat["n"].astype(float)

    if fill_metric == "pct_row":
        heat["fill_val"] = heat.groupby("row_val")["n"].transform(
            lambda x: (100 * x / x.sum()).round(1))
    elif fill_metric == "pct_col":
        heat["fill_val"] = heat.groupby("col_val")["n"].transform(
            lambda x: (100 * x / x.sum()).round(1))
    elif fill_metric == "pct_total":
        heat["fill_val"] = (100 * heat["n"] / heat["n"].sum()).round(1)
    else:
        heat["fill_val"] = heat["n"]

    row_order = _age_group_order(heat["row_val"].unique().tolist())
    heat["row_val"] = pd.Categorical(heat["row_val"], categories=row_order)
    heat["col_val"] = pd.Categorical(
        heat["col_val"], categories=sorted(heat["col_val"].unique().tolist()))

    fill_label = {
        "pct_row":   f"% {_vl('within_row',   lang)}",
        "pct_col":   f"% {_vl('within_col',   lang)}",
        "pct_total": f"% {_vl('of_total',     lang)}",
        "count":     _vl("legend_count", lang),
    }[fill_metric]

    heat["label"]     = heat["fill_val"].apply(
        lambda x: f"{int(x):,}" if fill_metric == "count" else f"{x:.1f}%")
    threshold         = heat["fill_val"].median()
    heat["txt_color"] = heat["fill_val"].apply(
        lambda x: "white" if x > threshold else "#333333")

    return (
        ggplot(heat, aes(x="col_val", y="row_val", fill="fill_val"))
        + geom_tile(color="white", size=0.3)
        + geom_text(aes(label="label", color="txt_color"),
                    size=base_size * 0.8, show_legend=False)
        + scale_color_manual(values={"white": "white", "#333333": "#333333"})
        + scale_fill_gradientn(
            colors=["#F7FBFF", "#DEEBF7", "#9ECAE1", "#3182BD", "#08519C"],
            name=fill_label)
        + scale_y_discrete(limits=list(reversed(row_order)))
        + labs(
            title    = f"{_vl('heatmap_title', lang)}: "
                       f"{_vl(row_var, lang)} × {_vl(col_var, lang)}",
            subtitle = f"{_vl('metric', lang)}: {fill_label}",
            x        = _vl(col_var, lang),
            y        = _vl(row_var, lang),
            caption  = caption,
        )
        + theme_classic(base_size=base_size)
        + theme(
            axis_text_x=element_text(angle=30, ha="right", size=base_size * 0.85),
            axis_text_y=element_text(size=base_size * 0.85),
            axis_ticks=element_blank(),
            axis_line=element_blank(),
            panel_grid=element_blank(),
            legend_position="right",
            plot_title=element_text(face="bold", size=base_size + 1),
            plot_subtitle=element_text(color="grey", size=base_size - 1),
        )
    )


def _vd_climate(df, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_col, geom_text, geom_tile,
                          coord_flip, scale_fill_manual, scale_fill_gradientn,
                          scale_y_continuous, labs, theme_classic, theme,
                          element_text, element_blank)
    from ..utils.data import load_json as _lj
    try:
        cal = _lj("templates/calendar_labels.json")
        month_labels_raw = cal.get("month_names_abbr", {}).get(lang, {})
        month_labels = {i+1: v for i, v in enumerate(month_labels_raw)} \
                       if isinstance(month_labels_raw, list) else month_labels_raw
        season_names = _lj("templates/seasonal_patterns.json")
        season_order = season_names["astronomical"]["labels"]["south"][lang]
    except Exception:
        month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                        7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        season_order = ["Summer","Autumn","Winter","Spring"]

    clim_col   = _find_col(df, ["climate_risk_group", "grupo_risco_climatico"])
    season_col = _find_col(df, ["astronomical_season", "estacao_astronomica", "season"])
    month_col  = _find_col(df, ["month", "mes"])
    pal        = _palette(palette)

    if clim_col is None:
        raise ValueError("climate_risk_group column not found.")

    bar = (df[df[clim_col].notna()].groupby(clim_col).size()
           .reset_index(name="n").rename(columns={clim_col: "group"}))
    bar["group"]   = bar["group"].astype(str)
    bar["pct"]     = (100 * bar["n"] / bar["n"].sum()).round(1)
    bar["pct_lbl"] = bar["pct"].astype(str) + "%"
    bar = bar.sort_values("n", ascending=False)
    bar["group"] = pd.Categorical(bar["group"], categories=bar["group"].tolist())
    n_cats   = len(bar)
    clim_pal = (pal * ((n_cats // len(pal)) + 1))[:n_cats]

    p_bar = (
        ggplot(bar, aes(x="group", y="n", fill="group"))
        + geom_col(width=0.7, show_legend=False)
        + geom_text(aes(label="pct_lbl"), ha="left", size=base_size * 0.8,
                    color="grey", nudge_y=bar["n"].max() * 0.02)
        + coord_flip()
        + scale_fill_manual(values=clim_pal)
        + scale_y_continuous(expand=(0, 0, 0.2, 0),
                             labels=lambda lst: [f"{int(x):,}" for x in lst])
        + labs(title=_vl("climate_bar_title", lang), x=None, y=_vl("count", lang))
        + _lancet_theme(base_size)
    )

    if season_col is None or month_col is None:
        return p_bar

    heat = (
        df[df[month_col].notna() & df[season_col].notna()]
        .groupby([month_col, season_col]).size()
        .reset_index(name="n")
        .rename(columns={month_col: "month", season_col: "season"})
    )
    heat["month"]   = heat["month"].astype(int)
    heat["n"]       = heat["n"].astype(float)
    heat["lbl"]     = heat["n"].astype(int).astype(str)
    heat["mon_lbl"] = heat["month"].map(month_labels)
    heat = heat.sort_values("month")
    heat["mon_lbl"] = pd.Categorical(
        heat["mon_lbl"],
        categories=[month_labels[i] for i in range(1, 13)],
        ordered=True
    )
    available = [s for s in season_order if s in heat["season"].unique()]
    heat["season"] = pd.Categorical(heat["season"], categories=available, ordered=True)

    p_heat = (
        ggplot(heat, aes(x="mon_lbl", y="season", fill="n"))
        + geom_tile(color="white", size=0.4)
        + geom_text(aes(label="lbl"), size=base_size * 0.8, color="#333333")
        + scale_fill_gradientn(
            colors=["#FFF5F0", "#FCBBA1", "#FC6D4C", "#D32020"],
            name=_vl("count", lang),
        )
        + labs(
            title=_vl("climate_heat_title", lang),
            x=_vl("month", lang), y=_vl("season", lang),
        )
        + theme_classic(base_size=base_size)
        + theme(
            axis_text_x=element_text(size=base_size - 1),
            axis_ticks=element_blank(),
            axis_line=element_blank(),
            panel_grid=element_blank(),
            legend_position="right",
            plot_title=element_text(face="bold", size=base_size + 1),
        )
    )

    from PIL import Image
    tmp_bar  = tempfile.mktemp(suffix=".png")
    tmp_heat = tempfile.mktemp(suffix=".png")
    p_bar.save(tmp_bar,   dpi=150, width=9, height=3.5, verbose=False)
    p_heat.save(tmp_heat, dpi=150, width=9, height=3.5, verbose=False)
    img_bar  = Image.open(tmp_bar)
    img_heat = Image.open(tmp_heat)
    combined = Image.new("RGB", (max(img_bar.width, img_heat.width),
                                  img_bar.height + img_heat.height), "white")
    combined.paste(img_bar,  (0, 0))
    combined.paste(img_heat, (0, img_bar.height))
    for tmp in [tmp_bar, tmp_heat]:
        try: os.remove(tmp)
        except Exception: pass
    return combined


def _vd_race_equity(df, benchmark, lang, palette, caption, base_size):
    from plotnine import (ggplot, aes, geom_col, geom_hline, coord_flip,
                          scale_fill_manual, scale_y_continuous, labs, theme)
    cfg  = _load_config()
    ref  = benchmark if benchmark is not None else cfg.get("ibge_2022", {})
    race_col = _find_col(df, ["race", "raca", "raza", "RACACOR", "RACA_COR"])
    if race_col is None:
        raise ValueError("Race column not found.")
    obs = (df[df[race_col].notna()].groupby(race_col).size()
           .reset_index(name="n").rename(columns={race_col: "race"}))
    obs["race"]    = obs["race"].astype(str)
    obs["obs_pct"] = 100 * obs["n"] / obs["n"].sum()
    obs["ref_pct"] = obs["race"].map(ref)
    obs = obs[obs["ref_pct"].notna()].copy()
    obs["diff"] = (obs["obs_pct"] - obs["ref_pct"]).round(2)
    obs["dir"]  = obs["diff"].apply(
        lambda x: _vl("overrep", lang) if x >= 0 else _vl("underrep", lang))
    obs = obs.sort_values("diff")
    obs["race"] = pd.Categorical(obs["race"], categories=obs["race"].tolist())
    col_map = {_vl("overrep", lang): "#B22222", _vl("underrep", lang): "#1B6CA8"}
    return (
        ggplot(obs, aes(x="race", y="diff", fill="dir"))
        + geom_col(width=0.68)
        + geom_hline(yintercept=0, color="#333333", size=0.6)
        + coord_flip()
        + scale_fill_manual(values=col_map, name=None)
        + scale_y_continuous(
            labels=lambda lst: [f"{'+' if x > 0 else ''}{x:.1f} pp" for x in lst],
            expand=(0.12, 0))
        + labs(
            title    = _vl("equity_title",    lang),
            subtitle = _vl("equity_subtitle", lang),
            x        = _vl("race",            lang),
            y        = _vl("equity_axis",     lang),
            caption  = caption,
        )
        + _lancet_theme(base_size)
        + theme(legend_position="top")
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_plot_demographics(
    data,
    type: str = "table",
    var: str | None = None,
    *,
    time_unit: str = "month",
    fill_var: str | None = None,
    heatmap_row: str | None = None,
    heatmap_col: str | None = None,
    fill_metric: str = "pct_row",
    show_ci: bool = False,
    benchmark: dict | None = None,
    palette: str = "lancet",
    lang: str = "pt",
    subtitle: str | None = None,
    caption: str | None = None,
    caption_suffix: str | None = None,
    base_size: int = 11,
    save_path: str | None = None,
    width: float = 7,
    height: float = 5,
    dpi: int = 150,
    verbose: bool = True,
):
    """Visualize demographic profiles of SUS data.

    Mirrors ``climasus4r::sus_data_plot_demographics()``.

    Args:
        data: DuckDBPyRelation or DataFrame.
        type: Plot type — ``"table"``, ``"bar"``, ``"pyramid"``,
            ``"heatmap"``, ``"temporal"``, ``"climate"``,
            ``"race_equity"``, ``"dashboard"``.
        var: Variable for ``type="bar"`` — ``"sex"``, ``"race"``,
            ``"age_group"``, ``"education"``, ``"climate_risk"``,
            ``"region"``.
        time_unit: For ``type="temporal"`` — ``"month"``, ``"epi_week"``,
            ``"year"``, ``"quarter"``, ``"semester"``.
        fill_var: Stratification variable for temporal plot.
        heatmap_row: Row variable for heatmap.
        heatmap_col: Column variable for heatmap.
        fill_metric: Heatmap fill — ``"pct_row"``, ``"pct_col"``,
            ``"pct_total"``, ``"count"``.
        show_ci: Show confidence intervals in temporal plot.
        benchmark: Reference proportions for race_equity
            (default: IBGE 2022 Census).
        palette: Colour palette — ``"lancet"``, ``"nature"``, ``"nejm"``,
            ``"jco"``, ``"aaas"``, ``"sus"``, ``"colorblind"``.
        lang: Language — ``"pt"`` (default), ``"en"``, ``"es"``.
        subtitle: Optional subtitle.
        caption: Custom caption (auto-generated if None).
        caption_suffix: Extra text appended to auto caption.
        base_size: Base font size.
        save_path: File path to save the plot (None = no save).
        width: Plot width in inches.
        height: Plot height in inches.
        dpi: Resolution.
        verbose: Print progress messages.

    Returns:
        ``pd.DataFrame`` (type='table'), ``ggplot``, ``PIL.Image``,
        or ``matplotlib.Figure`` (dashboard).
    """
    import matplotlib.pyplot as plt

    if type not in _DEMO_TYPES:
        raise ValueError(f"type must be one of {sorted(_DEMO_TYPES)}")

    df = _to_df(data)

    if verbose:
        print(f"[sus_data_plot_demographics] type={type!r} | n={len(df):,} | lang={lang}")

    cap_base = _vl("source", lang)
    if caption_suffix:
        cap_base = f"{cap_base} | {caption_suffix}"
    resolved_caption = caption or f"{cap_base} | climasus4py"

    # table
    if type == "table":
        return _vd_table(df, var, lang)

    # dashboard
    if type == "dashboard":
        def _try(fn):
            try:
                return fn()
            except Exception as e:
                if verbose:
                    print(f"  [panel skipped] {e}")
                return None

        plots = {
            "pyramid":  _try(lambda: _vd_pyramid(df, lang, palette, "", base_size)),
            "temporal": _try(lambda: _vd_temporal(df, "month", None, False, lang,
                                                   palette, "", base_size)),
            "sex":      _try(lambda: _vd_bar(df, "sex",       lang, palette, "", base_size)),
            "race":     _try(lambda: _vd_bar(df, "race",      lang, palette, "", base_size)),
            "age":      _try(lambda: _vd_bar(df, "age_group", lang, palette, "", base_size)),
            "heatmap":  _try(lambda: _vd_heatmap(df, None, None, "pct_row",
                                                  lang, palette, "", base_size)),
        }
        valid = {k: v for k, v in plots.items() if v is not None}
        if not valid:
            raise ValueError("No panel could be generated for dashboard.")

        from PIL import Image
        tmp_paths = {}
        for k, pp in valid.items():
            tmp = tempfile.mktemp(suffix=".png")
            pp.save(tmp, dpi=dpi, width=5, height=4, verbose=False)
            tmp_paths[k] = tmp

        n_panels  = len(tmp_paths)
        ncols     = 2
        nrows     = (n_panels + 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(width or 12, height or 9))
        axes_flat = axes.flatten() if n_panels > 1 else [axes]

        for ax, (k, path) in zip(axes_flat, tmp_paths.items()):
            img = Image.open(path)
            ax.imshow(img)
            ax.axis("off")
        for ax in axes_flat[n_panels:]:
            ax.axis("off")

        fig.suptitle(_vl("dashboard_title", lang),
                     fontsize=base_size + 4, fontweight="bold", y=1.01)
        fig.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=dpi, bbox_inches="tight", facecolor="white")
            if verbose:
                print(f"[sus_data_plot_demographics] Saved: {save_path}")

        for path in tmp_paths.values():
            try: os.remove(path)
            except Exception: pass

        return fig

    # all other types
    if type == "bar":
        p = _vd_bar(df, var, lang, palette, resolved_caption, base_size)
    elif type == "pyramid":
        p = _vd_pyramid(df, lang, palette, resolved_caption, base_size)
    elif type == "temporal":
        p = _vd_temporal(df, time_unit, fill_var, show_ci, lang,
                         palette, resolved_caption, base_size)
    elif type == "heatmap":
        p = _vd_heatmap(df, heatmap_row, heatmap_col, fill_metric,
                        lang, palette, resolved_caption, base_size)
    elif type == "climate":
        p = _vd_climate(df, lang, palette, resolved_caption, base_size)
    elif type == "race_equity":
        p = _vd_race_equity(df, benchmark, lang, palette, resolved_caption, base_size)

    # subtitle (ggplot only)
    if subtitle and hasattr(p, "__add__"):
        from plotnine import labs
        p = p + labs(subtitle=subtitle)

    # save
    if save_path:
        from PIL import Image as _PIL_Image
        if isinstance(p, _PIL_Image.Image):
            p.save(save_path)
        elif hasattr(p, "savefig"):
            p.savefig(save_path, dpi=dpi)
        else:
            p.save(save_path, dpi=dpi, width=width, height=height, verbose=False)
        if verbose:
            print(f"[sus_data_plot_demographics] Saved: {save_path}")

    return p
