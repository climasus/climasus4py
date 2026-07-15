"""Choropleth and bubble map visualization of aggregated SUS health data.

Mirrors R: sus_data_plot_aggregate_map.R
Uses matplotlib + geopandas (not plotnine).
"""

from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from ..utils.data import load_json

# ---------------------------------------------------------------------------
# Column candidates
# ---------------------------------------------------------------------------

_MAP_OUTCOME_CANDIDATES = [
    "n_obitos", "n_internacoes", "n_nascimentos", "n_casos",
    "n_procedimentos", "n_estabelecimentos",
    "n_deaths", "n_hospitalizations", "n_births", "n_cases",
    "n_procedures", "n_establishments",
    "n_muertes", "n_hospitalizaciones", "n_nacimientos",
    "n_procedimientos", "n_establecimientos",
    "count", "n", "total",
]

_MAP_MUNI_CANDIDATES = [
    "residence_municipality_code", "occurrence_municipality_code",
    "notification_municipality_code", "municipality_code",
    "CODMUNRES", "CODMUNOCOR", "ID_MUNICIP", "code_muni",
]

_MAP_TYPES = {"bubble", "choropleth", "quantile_choropleth"}

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mapm(key: str, lang: str = "pt", **kwargs) -> str:
    labels = _load_viz_labels()
    row = labels.get(key, {})
    txt = row.get(lang, row.get("pt", key))
    return txt.format(**kwargs) if kwargs else txt


def _map_palette(name: str = "lancet", n: int = 256):
    from matplotlib.colors import LinearSegmentedColormap
    seq_maps = {
        "lancet":     ["#F7FBFF", "#08468B"],
        "nature":     ["#FEF0E5", "#E64B35"],
        "nejm":       ["#EAF1F6", "#0072B5"],
        "jco":        ["#E5F0F8", "#0073C2"],
        "aaas":       ["#EAF0FB", "#3B4992"],
        "sus":        ["#E8F1F8", "#1B6CA8"],
        "colorblind": ["#E5F0F8", "#0072B2"],
    }
    colors = seq_maps.get(name, seq_maps["lancet"])
    return LinearSegmentedColormap.from_list("map_cmap", colors, N=n)


def _map_detect_outcome_col(columns: list, value_col: str | None) -> str:
    if value_col is not None:
        if value_col not in columns:
            raise ValueError(f"value_col {value_col!r} not found.")
        return value_col
    for c in _MAP_OUTCOME_CANDIDATES:
        if c in columns:
            return c
    raise ValueError(
        "Outcome column not detected. "
        "Specify value_col= or run sus_data_aggregate() first."
    )


def _map_detect_muni_col(columns: list) -> str:
    for c in _MAP_MUNI_CANDIDATES:
        if c in columns:
            return c
    raise ValueError(
        "Municipality column not detected. "
        "Expected one of: residence_municipality_code, CODMUNRES, ..."
    )


def _map_load_meta() -> pd.DataFrame:
    import climasus_data
    path = Path(climasus_data.__file__).parent / "assets" / "spatial" / "municipio_meta.parquet"
    if not path.exists():
        raise FileNotFoundError(f"municipio_meta.parquet not found: {path}")
    meta = pd.read_parquet(path)
    meta["_code6"] = meta["municipio"].astype(str).str[:6]
    return meta


def _map_load_geo():
    import climasus_data
    import geopandas as gpd
    from shapely import wkt

    path = Path(climasus_data.__file__).parent / "assets" / "spatial" / "municipalities.parquet"
    if not path.exists():
        raise FileNotFoundError(f"municipalities.parquet not found: {path}")

    geo = pd.read_parquet(path)
    geo["geometry"] = geo["geometry_wkt"].apply(
        lambda w: wkt.loads(w) if isinstance(w, str) else None
    )
    gdf = gpd.GeoDataFrame(geo, geometry="geometry", crs="EPSG:4326")
    gdf["_code6"] = gdf["code_muni"].astype(str).str[:6]
    return gdf


def _map_load_states():
    try:
        import geobr
        gdf = geobr.read_state(year=2020)
        return gdf.to_crs("EPSG:4326")
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_plot_aggregate_map(
    df,
    *,
    value_col: str | None = None,
    map_type: str = "choropleth",
    rate_per_100k: bool = False,
    period: list | None = None,
    city: list | None = None,
    top_n: int | None = None,
    state_borders: bool = True,
    show_labels: bool = False,
    palette: str = "lancet",
    log_scale: bool = False,
    title: str | None = None,
    subtitle: str | None = None,
    caption: str | None = None,
    base_size: int = 11,
    save_path: str | None = None,
    width: float = 10,
    height: float = 9,
    dpi: int = 150,
    lang: str = "pt",
    verbose: bool = True,
):
    """Choropleth or bubble map of aggregated SUS health data by municipality.

    Mirrors ``climasus4r::sus_data_plot_aggregate_map()``.
    Uses ``matplotlib`` + ``geopandas`` (not plotnine).
    Requires ``municipio_meta.parquet`` in climasus-data for coordinates.

    Args:
        df: Output of ``sus_data_aggregate()`` — DataFrame or DuckDBPyRelation.
        value_col: Outcome column (auto-detected from n_deaths, n_cases...).
        map_type: ``"bubble"``, ``"choropleth"``, ``"quantile_choropleth"``.
        rate_per_100k: Calculate rate per 100k inhabitants (needs pop_25).
        period: ``[start_date, end_date]`` to filter by date column.
        city: List of city names to filter.
        top_n: Label top N municipalities (bubble only).
        state_borders: Draw state borders.
        show_labels: Label capital cities (bubble only).
        palette: Colour palette — ``"lancet"``, ``"nature"``, ``"nejm"``,
            ``"jco"``, ``"aaas"``, ``"sus"``, ``"colorblind"``.
        log_scale: Logarithmic colour scale.
        title / subtitle / caption: Plot text (auto-generated if None).
        base_size: Base font size.
        save_path: File path to save (None = no save).
        width / height: Dimensions in inches.
        dpi: Resolution.
        lang: Language — ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages.

    Returns:
        ``matplotlib.Figure``.

    Example:
        >>> agg = cs.sus_data_aggregate(stand, time_unit="month", system="SIM-DO")
        >>> cs.sus_data_plot_aggregate_map(agg, map_type="choropleth", lang="pt")
        >>> cs.sus_data_plot_aggregate_map(agg, map_type="bubble",
        ...     rate_per_100k=True, top_n=5, lang="en")
    """
    import matplotlib.pyplot as plt

    if map_type not in _MAP_TYPES:
        raise ValueError(f"map_type must be one of {_MAP_TYPES}.")

    if hasattr(df, "df") and not isinstance(df, pd.DataFrame):
        df = df.df()
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a DataFrame or DuckDBPyRelation.")

    columns   = list(df.columns)
    value_col = _map_detect_outcome_col(columns, value_col)
    muni_col  = _map_detect_muni_col(columns)

    if verbose:
        print(f"[map] value_col={value_col!r} | muni_col={muni_col!r} | type={map_type}")

    work = df.copy()

    # period filter
    if period is not None and "date" in work.columns:
        work["date"] = pd.to_datetime(work["date"], errors="coerce")
        p_start = pd.to_datetime(period[0])
        p_end   = pd.to_datetime(period[1]) if len(period) > 1 else pd.Timestamp.now()
        work = work[(work["date"] >= p_start) & (work["date"] <= p_end)]

    # aggregate by municipality (6-digit code)
    work["_code6"] = work[muni_col].astype(str).str[:6]
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    muni_agg = (work.groupby("_code6")[value_col]
                .sum().reset_index()
                .rename(columns={value_col: "total_cases"}))

    # merge with metadata (lon/lat/pop)
    meta = _map_load_meta()
    muni_data = muni_agg.merge(
        meta[["_code6", "name", "uf", "lon", "lat", "pop_25", "is_capital"]],
        on="_code6", how="left"
    )

    # city filter
    if city is not None and len(city) > 0:
        city_lower = [c.lower() for c in city]
        mask = muni_data["name"].str.lower().isin(city_lower)
        muni_data = muni_data[mask]
        if len(muni_data) == 0:
            raise ValueError(f"No municipalities found: {city}")

    n_matched = muni_data["lon"].notna().sum()
    if verbose:
        print(f"[map] {_mapm('map_n_muni', lang, n=n_matched)}")

    # rate per 100k
    if rate_per_100k:
        if "pop_25" not in muni_data.columns or muni_data["pop_25"].isna().all():
            warnings.warn(_mapm("map_no_pop", lang))
            rate_per_100k = False
        else:
            muni_data["rate"] = (
                muni_data["total_cases"] / muni_data["pop_25"] * 1e5
            )

    muni_data["fill_var"] = (
        muni_data["rate"] if rate_per_100k else muni_data["total_cases"]
    )

    cmap      = _map_palette(palette)
    fill_lbl  = _mapm("map_rate_label" if rate_per_100k else "map_fill_label", lang)
    if log_scale:
        fill_lbl += " (log1p)"

    default_title_key = {
        "bubble":             "map_title_bubble",
        "choropleth":         "map_title_choropleth",
        "quantile_choropleth":"map_title_quantile",
    }
    map_title = title or _mapm(default_title_key[map_type], lang)
    n_sub     = muni_data["lon"].notna().sum()
    map_sub   = subtitle or f"{n_sub} {_mapm('map_municipalities', lang)} | {fill_lbl}"
    cap_lbl   = caption or f"{_mapm('source', lang)} | climasus4py"

    fig, ax = plt.subplots(figsize=(width, height))

    # state borders
    state_gdf = _map_load_states() if state_borders else None

    # ------------------------------------------------------------------
    # BUBBLE
    # ------------------------------------------------------------------
    if map_type == "bubble":
        plot_data = muni_data[
            muni_data["lon"].notna() & muni_data["lat"].notna()
        ].copy().sort_values("total_cases", ascending=False)

        if state_gdf is not None:
            state_gdf.plot(ax=ax, color="#F8F8F8",
                           edgecolor="#AAAAAA", linewidth=0.3)

        fill_vals = np.log1p(plot_data["fill_var"]) if log_scale else plot_data["fill_var"]
        sizes     = np.log1p(plot_data["total_cases"]) * 30 + 5

        scatter = ax.scatter(
            plot_data["lon"], plot_data["lat"],
            s=sizes, c=fill_vals, cmap=cmap,
            edgecolors="white", linewidths=0.3, alpha=0.8, zorder=3
        )
        cbar = fig.colorbar(scatter, ax=ax, orientation="horizontal",
                            fraction=0.04, pad=0.02, aspect=40)
        cbar.set_label(fill_lbl, size=base_size - 1)

        # labels
        label_data = None
        if top_n and top_n > 0:
            label_data = plot_data.head(top_n)
        elif show_labels and "is_capital" in plot_data.columns:
            label_data = plot_data[
                plot_data["is_capital"].isin([True, 1, "TRUE", "1"])
            ]

        if label_data is not None and len(label_data) > 0:
            for _, row in label_data.iterrows():
                if pd.notna(row.get("name")):
                    ax.annotate(row["name"], (row["lon"], row["lat"]),
                               fontsize=base_size - 4, fontweight="bold",
                               ha="center", va="bottom",
                               color="#1A252F", zorder=4)

    # ------------------------------------------------------------------
    # CHOROPLETH / QUANTILE
    # ------------------------------------------------------------------
    else:
        gdf    = _map_load_geo()
        merged = gdf.merge(
            muni_data[["_code6", "total_cases", "fill_var"]],
            on="_code6", how="left"
        )

        if map_type == "choropleth":
            norm = None
            if log_scale:
                from matplotlib.colors import LogNorm
                vmin = max(merged["fill_var"].dropna().min(), 0.1)
                vmax = merged["fill_var"].dropna().max()
                norm = LogNorm(vmin=vmin, vmax=vmax)

            merged.plot(
                ax=ax, column="fill_var", cmap=cmap, norm=norm,
                edgecolor="#C8C8C8", linewidth=0.05,
                missing_kwds={"color": "#EBEBEB"},
                legend=True,
                legend_kwds={
                    "label": fill_lbl, "orientation": "horizontal",
                    "fraction": 0.04, "pad": 0.02, "aspect": 40,
                }
            )
        else:  # quantile_choropleth
            merged.plot(
                ax=ax, column="fill_var", cmap=cmap,
                scheme="quantiles", k=5,
                edgecolor="#C8C8C8", linewidth=0.05,
                missing_kwds={"color": "#EBEBEB"},
                legend=True,
                legend_kwds={
                    "loc": "lower right",
                    "fontsize": base_size - 3,
                    "title": fill_lbl,
                }
            )

        if state_gdf is not None:
            state_gdf.boundary.plot(ax=ax, color="#4A4A4A", linewidth=0.35)

    # zoom to data
    valid = muni_data[muni_data["lon"].notna()]
    if len(valid) > 0:
        pad_x = max((valid["lon"].max() - valid["lon"].min()) * 0.1, 0.5)
        pad_y = max((valid["lat"].max() - valid["lat"].min()) * 0.1, 0.5)
        ax.set_xlim(valid["lon"].min() - pad_x, valid["lon"].max() + pad_x)
        ax.set_ylim(valid["lat"].min() - pad_y, valid["lat"].max() + pad_y)

    # labels
    ax.set_title(map_title, fontsize=base_size + 1, fontweight="bold", loc="left")
    ax.text(0, 1.02, map_sub, transform=ax.transAxes,
            fontsize=base_size * 0.88, color="grey", ha="left")
    ax.text(0, -0.05, cap_lbl, transform=ax.transAxes,
            fontsize=base_size - 2, color="#888888", ha="left")
    ax.axis("off")
    ax.set_aspect("equal")
    fig.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=dpi, bbox_inches="tight", facecolor="white")
        if verbose:
            print(f"[map] Saved: {save_path}")
        plt.close(fig)

    return fig
