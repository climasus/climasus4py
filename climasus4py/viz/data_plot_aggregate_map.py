"""Municipal choropleth / bubble map of aggregated health data — via plotnine.

Mirrors R: sus_data_plot_aggregate_map (sus_data_plot_aggregate_map.R)

**Input-contract divergence (flagged, see IDEIAS.md).** R's docstring assumes
*df* is a per-record ``climasus_df`` (one row per health event, columns like
``codigo_municipio_residencia`` and ``n_obitos``) and the R function performs
its own municipality-level aggregation internally. The *real* Python
``sus_data_aggregate()`` (``core/aggregate.py``) already aggregates by
``time_group`` + a geo column (``municipality_code``/``CODMUNRES``/
``ID_MUNICIP`` for ``geo="municipality"``) and produces ``count`` plus
``sum_*``/``mean_*`` columns for whichever of
``["count", "deaths", "admissions", "cases", "value"]`` were present
pre-aggregation — there is no ``n_obitos``-style column and no raw ``date``
column (it is collapsed into the string ``time_group``). This port targets
the *real* aggregated shape, not the R docstring's assumption.

**Geometry-source divergence (flagged, see IDEIAS.md).** R downloads state
and municipality polygons via ``geobr`` (with population/capital flags from
a bundled ``municipio_meta``). climasus4py has no ``geobr`` port — per
``dependency-map.md`` municipality metadata comes from ``climasus-data``,
which currently only ships ``geo/municipios.json`` (code, name, lat/lon) —
no polygons, no population, no capital flag. Following the precedent already
established by ``sus_mod_plot_spatial_moran`` (which also lacks a ``geobr``
equivalent), this port adds a **new** ``municipalities`` parameter: the
caller supplies a ``pandas.DataFrame``/``geopandas.GeoDataFrame`` with a
municipality-code column, plus ``lon``/``lat`` (bubble) or ``geometry``
(choropleth), and optionally ``pop``, ``is_capital``, ``name``, and a
state-code column. State borders (when requested) are derived by dissolving
municipality polygons on the state-code column — there is no separate
``geobr::read_state()`` download to replicate.

This is a plotting utility, not a pipeline stage: a ``duckdb.DuckDBPyRelation``
input is materialised via ``.df()`` at the top; the function never touches
the lazy pipeline.

Requires the optional [plot] extra (and, for ``map_type in
{"choropleth", "quantile_choropleth"}``, [spatial]):
    pip install climasus4py[plot]
    pip install climasus4py[spatial]

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_data_plot_aggregate_map(df_agg, municipalities=munis, lang="en")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .msgs)
# ---------------------------------------------------------------------------

_I18N = {
    "pt": {
        "title_bubble": "Distribuição espacial de eventos de saúde notificados",
        "title_choropleth": "Incidência municipal de eventos de saúde notificados",
        "title_quantile": "Incidência municipal classificada por quintis",
        "loading_meta": "Carregando metadados municipais...",
        "computing_rate": "Calculando taxa por 100.000 habitantes...",
        "n_muni": "{n} municípios mapeados",
        "fallback_bubble": "Coroplético requer geometria de polígono em 'municipalities'. Usando mapa de bolhas.",  # noqa: E501
        "done": "{n} municípios, tipo '{type}'.",
        "no_outcome": "Nenhuma coluna de desfecho detectada em df. Informe value_col.",
        "no_muni_col": "Nenhuma coluna de código municipal detectada em df.",
        "no_join": "Nenhum município foi pareado com municipalities. Verifique os códigos.",
        "col_used": "Coluna de desfecho usada: {col}",
        "muni_col_used": "Coluna municipal usada: {col}",
        "no_city_match": "Nenhum município informado em city foi encontrado nos dados.",
        "fill_label": "Eventos notificados",
        "rate_label": "Taxa por 100.000 habitantes",
        "fill_label_log": "Eventos notificados (log1p)",
        "rate_label_log": "Taxa por 100.000 hab. (log1p)",
        "size_label": "Total de eventos",
        "caption": "Fonte: DATASUS / Ministério da Saúde",
        "municipalities": "municípios",
        "sep": " | ",
    },
    "en": {
        "title_bubble": "Spatial distribution of reported health events",
        "title_choropleth": "Municipal incidence of reported health events",
        "title_quantile": "Classified municipal incidence (quintiles)",
        "loading_meta": "Loading municipality metadata...",
        "computing_rate": "Computing rate per 100,000 pop.",
        "n_muni": "{n} municipalities mapped",
        "fallback_bubble": "Choropleth requires polygon geometry in 'municipalities'. Using bubble map.",  # noqa: E501
        "done": "{n} municipalities, type '{type}'.",
        "no_outcome": "No health-outcome column detected in df. Supply value_col.",
        "no_muni_col": "No municipality-code column detected in df.",
        "no_join": "No municipality matched municipalities. Check your codes.",
        "col_used": "Outcome column used: {col}",
        "muni_col_used": "Municipality column used: {col}",
        "no_city_match": "No municipality supplied in city was found in the data.",
        "fill_label": "Reported events",
        "rate_label": "Rate per 100,000 population",
        "fill_label_log": "Reported events (log1p)",
        "rate_label_log": "Rate per 100,000 pop. (log1p)",
        "size_label": "Total events",
        "caption": "Source: DATASUS / Brazilian Ministry of Health",
        "municipalities": "municipalities",
        "sep": " | ",
    },
    "es": {
        "title_bubble": "Distribución espacial de eventos de salud notificados",
        "title_choropleth": "Incidencia municipal de eventos de salud notificados",
        "title_quantile": "Incidencia municipal clasificada (quintiles)",
        "loading_meta": "Cargando metadatos municipales...",
        "computing_rate": "Calculando tasa por 100.000 hab.",
        "n_muni": "{n} municipios mapeados",
        "fallback_bubble": "Coroplético requiere geometría de polígono en 'municipalities'. Usando burbujas.",  # noqa: E501
        "done": "{n} municipios, tipo '{type}'.",
        "no_outcome": "Ninguna columna de desenlace detectada en df. Indique value_col.",
        "no_muni_col": "Ninguna columna de código municipal detectada en df.",
        "no_join": "Ningún municipio se emparejó con municipalities. Verifique los códigos.",
        "col_used": "Columna de desenlace usada: {col}",
        "muni_col_used": "Columna municipal usada: {col}",
        "no_city_match": "Ningún municipio indicado en city fue encontrado en los datos.",
        "fill_label": "Eventos notificados",
        "rate_label": "Tasa por 100.000 habitantes",
        "fill_label_log": "Eventos notificados (log1p)",
        "rate_label_log": "Tasa por 100.000 hab. (log1p)",
        "size_label": "Total de eventos",
        "caption": "Fuente: DATASUS / Ministerio de Salud de Brasil",
        "municipalities": "municipios",
        "sep": " | ",
    },
}

_VALID_MAP_TYPES = ("bubble", "choropleth", "quantile_choropleth")

# Built-in perceptual-blue fallback (colour-blind safe), mirrors R's fallback
# when RColorBrewer/viridisLite are unavailable.
_FALLBACK_COLORS = ("#F7FBFF", "#D0E1F2", "#8AB6D6", "#3B7EA1", "#0B3C5D")

# ggsci aliases: R falls back to fixed brand palettes when ggsci is
# unavailable; matplotlib has no equivalent named colormap, so these literal
# hex constants (not DATASUS metadata) are kept inline, mirroring the same
# "fixed colour constant" precedent already used in mod_plot_spatial_moran.
_GGSCI_ALIASES = {
    "lancet": ("#00468B", "#ED0000", "#42B540", "#0099B4", "#925E9F", "#FDAF91"),
    "nejm": ("#BC3C29", "#0072B5", "#E18727", "#20854E", "#7876B1", "#6F99AD"),
}

# Non-outcome column names to exclude when auto-detecting value_col, mirrors
# R's .map_detect_outcome_col meta_cols list (adapted: no R-specific column
# names are present in the real Python sus_data_aggregate() output).
_META_COLS = {"time_group", "n_periods", "._muni6", "geometry", "geom"}

# Priority regex fragments for auto-detecting the outcome column, adapted
# from R's preferred_rx to the actual column names sus_data_aggregate()
# produces (count / sum_* / mean_* rather than n_obitos-style names).
_OUTCOME_PRIORITY = (
    "^sum_deaths$", "^sum_cases$", "^sum_admissions$", "^sum_value$",
    "^count$", "^sum_", "^mean_", "death", "case", "admission",
    "obito", "internac", "caso",
)

# Candidate municipality-code column names, mirrors R's .map_detect_muni_col
# priority list, adapted to the columns detect_geo_column() actually emits
# in climasus4py (utils/data.py).
_MUNI_COL_PRIORITY = (
    "municipality_code", "CODMUNRES", "ID_MUNICIP", "code_muni", "codigo_municipio",
)


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_data_plot_aggregate_map requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _require_geopandas() -> None:
    """Raise a clear ImportError if geopandas is not installed."""
    try:
        import geopandas  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_data_plot_aggregate_map requires geopandas for "
            "map_type='choropleth'/'quantile_choropleth'. "
            "Install with: pip install climasus4py[spatial]"
        ) from exc


def _detect_outcome_col(df: pd.DataFrame, exclude: set[str]) -> str | None:
    """Auto-detect the health-outcome column.

    Mirrors R's ``.map_detect_outcome_col``: only numeric columns are
    considered (R filters on ``is.numeric``/``is.integer`` before applying
    its regex priority list and its ``num_cols[1]`` fallback) and the
    municipality-code column is excluded, so the fallback can never pick a
    non-numeric identifier column.
    """
    import re

    numeric_cols = df.select_dtypes(include="number").columns
    candidates = [c for c in numeric_cols if c not in _META_COLS and c not in exclude]
    for rx in _OUTCOME_PRIORITY:
        matched = [c for c in candidates if re.search(rx, c, re.IGNORECASE)]
        if matched:
            return matched[0]
    return candidates[0] if candidates else None


def _detect_muni_col(columns: list[str]) -> str | None:
    import re

    for cand in _MUNI_COL_PRIORITY:
        if cand in columns:
            return cand
    fallback = [
        c for c in columns if re.search("muni|municipio|municipality|ibge", c, re.IGNORECASE)
    ]
    return fallback[0] if fallback else None


def _palette_colors(palette: str, n: int = 9) -> list[str]:
    """Resolve a palette name to a list of hex colours.

    Mirrors R's ``.map_palette_colors``: RColorBrewer/viridisLite names map
    to matplotlib colormaps of the same name (matplotlib ships colormaps
    named identically to the ColorBrewer/viridis families this function
    accepts), the ``lancet``/``nejm`` ggsci aliases map to fixed brand hex
    constants, and anything unresolved falls back to a built-in perceptual
    blue ramp.
    """
    if palette in _GGSCI_ALIASES:
        base = _GGSCI_ALIASES[palette]
        if n <= len(base):
            return list(base[:n])
        return _interpolate(list(base), n)

    try:
        import matplotlib

        cmap = matplotlib.colormaps.get(palette)
        if cmap is not None:
            return [
                matplotlib.colors.to_hex(cmap(i / max(n - 1, 1))) for i in range(n)
            ]
    except ImportError:
        pass

    return _interpolate(list(_FALLBACK_COLORS), n)


def _interpolate(colors: list[str], n: int) -> list[str]:
    import matplotlib

    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("_cs4py", colors)
    return [matplotlib.colors.to_hex(cmap(i / max(n - 1, 1))) for i in range(n)]


def sus_data_plot_aggregate_map(
    df: Any,
    value_col: str | None = None,
    map_type: Literal["bubble", "choropleth", "quantile_choropleth"] = "bubble",
    rate_per_100k: bool = False,
    period: Any = None,
    city: str | list[str] | None = None,
    top_n: int | None = None,
    state_borders: bool = True,
    show_labels: bool = True,
    palette: str = "YlOrRd",
    log_scale: bool = True,
    title: str | None = None,
    subtitle: str | None = None,
    caption: str | None = None,
    theme_style: str = "publication",
    base_size: float = 11,
    interactive: bool = False,
    use_cache: bool = True,
    cache_dir: str = "~/.climasus4r_cache/spatial",
    lang: str = "pt",
    verbose: bool = True,
    municipalities: Any = None,
) -> Any:
    """Plot a municipal map of aggregated health data.

    Renders a bubble or choropleth map showing the spatial distribution of
    health events (counts or incidence rates) at the Brazilian municipal
    level. Designed for the output of ``sus_data_aggregate()``.

    **Bubble map** (``map_type="bubble"``) places proportionally sized,
    colour-encoded circles at each municipality's coordinates (from
    *municipalities*). **Choropleth map** (``map_type="choropleth"``) fills
    municipality polygons supplied via *municipalities*; if geopandas is
    unavailable, or *municipalities* has no ``geometry`` column, the
    function falls back to a bubble map with a warning, mirroring R's
    ``geobr``-unavailable fallback.
    ``map_type="quantile_choropleth"`` classifies the same fill values into
    5 quantile classes instead of a continuous gradient.

    Requires the optional ``[plot]`` extra (and, for choropleth map types,
    ``[spatial]``)::

        pip install climasus4py[plot]
        pip install climasus4py[spatial]

    Args:
        df: Output of ``sus_data_aggregate()`` — a ``duckdb.DuckDBPyRelation``
            (materialised here via ``.df()``) or an already-materialised
            ``pandas.DataFrame`` with a municipality-code column (one of
            ``municipality_code``/``CODMUNRES``/``ID_MUNICIP``/``code_muni``)
            and a numeric outcome column (``count``, or ``sum_*``/``mean_*``).
        value_col: Name of the health-count column to map. If ``None``
            (default), auto-detected among ``count``/``sum_*``/``mean_*``
            columns, preferring ``sum_deaths``/``sum_cases``/``sum_admissions``.
        map_type: ``"bubble"`` (default), ``"choropleth"`` or
            ``"quantile_choropleth"``.
        rate_per_100k: If ``True``, divides the summed outcome by a ``pop``
            column in *municipalities* and multiplies by 1e5. Default
            ``False``. Falls back to raw counts with a warning if
            *municipalities* has no ``pop`` column.
        period: Not used to filter by calendar time in this port — the real
            ``sus_data_aggregate()`` output has no raw ``date`` column (only
            the aggregated string ``time_group``), unlike the R docstring's
            assumption. Accepted for signature parity; a warning is emitted
            if supplied. See IDEIAS.md.
        city: Municipality name(s) or 6-/7-digit IBGE code(s) to restrict the
            map to. Names are matched against a ``name`` column in
            *municipalities* (case-insensitive); codes are matched against
            the municipality-code column.
        top_n: If supplied, only the top N municipalities by summed outcome
            are labelled on the map (bubble map only). ``None`` disables.
        state_borders: Overlay state boundary outlines, dissolved from
            *municipalities* polygons on a state-code column
            (``code_state``/``uf_code``/``SG_UF``/``UF``), when present and
            geopandas is available. Default ``True``.
        show_labels: Annotate capital cities with text labels (bubble map
            only); requires ``is_capital`` and ``name`` columns in
            *municipalities*. Default ``True``.
        palette: Colour palette name for the fill/bubble scale. Any
            matplotlib colormap name (the ColorBrewer sequential/diverging
            names this accepts, e.g. ``"YlOrRd"``, ``"Blues"``, ``"RdYlBu"``,
            and the viridis family, are also valid matplotlib colormap
            names) or one of the fixed aliases ``"lancet"``/``"nejm"``.
            Default ``"YlOrRd"``.
        log_scale: Apply a ``log1p`` transform to the colour scale. Default
            ``True``.
        title: Map title. ``None`` uses a built-in multilingual default.
        subtitle: Map subtitle. ``None`` auto-generates one from the number
            of municipalities mapped and the metric.
        caption: Figure caption. ``None`` uses the DATASUS source string.
        theme_style: Reserved for future theme variants; only
            ``"publication"`` (default) is implemented, mirroring R.
        base_size: Base font size for the void theme. Default ``11``.
        interactive: If ``True``, would return a ``plotly`` interactive
            version (mirroring R's ``plotly::ggplotly()`` wrapper).
            **Not currently supported** — raises ``ImportError``.
        use_cache: Accepted for signature parity with R. This port has no
            ``geobr``-equivalent boundary download to cache (the caller
            supplies *municipalities* directly), so this is currently a
            no-op. See IDEIAS.md.
        cache_dir: Accepted for signature parity with R; currently unused
            (see *use_cache*).
        lang: Language for messages and labels: ``"pt"`` (default), ``"en"``,
            ``"es"``.
        verbose: Print progress messages. Default ``True``.
        municipalities: **New parameter, not present in the R signature**
            (flagged — see module docstring and IDEIAS.md). A
            ``pandas.DataFrame`` (bubble map) or ``geopandas.GeoDataFrame``
            (choropleth map types) with a municipality-code column plus
            ``lon``/``lat`` and/or ``geometry``, and optionally ``pop``,
            ``is_capital``, ``name``, and a state-code column. Required for
            all map types (there is no bundled boundary/centroid source with
            population/capital data in climasus-data yet).

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``).
        Does not modify *df*.

    Raises:
        ImportError: If ``plotnine`` is not installed, if geopandas is
            required and missing, or if ``interactive=True``.
        ValueError: If *lang* or *map_type* is invalid, *value_col* is
            supplied but absent, no outcome/municipality-code column can be
            auto-detected, *municipalities* is ``None``, or no municipality
            in *df* matched *municipalities*.

    Example:
        >>> import climasus4py as cs
        >>> agg = cs.sus_data_aggregate(rel, time="month", geo="municipality")
        >>> p = cs.sus_data_plot_aggregate_map(
        ...     agg, municipalities=munis, map_type="choropleth", lang="en"
        ... )
        >>> p.draw()
    """
    _require_plotnine()

    if lang not in _I18N:
        raise ValueError(f"lang must be one of {sorted(_I18N)!r}, got {lang!r}.")
    if map_type not in _VALID_MAP_TYPES:
        raise ValueError(f"map_type must be one of {list(_VALID_MAP_TYPES)!r}, got {map_type!r}.")
    msg = _I18N[lang]

    if interactive:
        raise ImportError(
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike climasus4r's plotly "
            "path). Install plotly manually if needed; see IDEIAS.md."
        )

    if period is not None:
        warnings.warn(
            "period is accepted for signature parity but has no effect: the "
            "real sus_data_aggregate() output has no raw 'date' column to "
            "filter by (only the aggregated 'time_group' string). See "
            "IDEIAS.md.",
            UserWarning,
            stacklevel=2,
        )

    if municipalities is None:
        raise ValueError(
            "municipalities must be supplied: climasus4py has no geobr-"
            "equivalent boundary/centroid download (see module docstring). "
            "Pass a DataFrame/GeoDataFrame with a municipality-code column "
            "plus lon/lat and/or geometry."
        )

    # -- materialise lazy relation -------------------------------------
    plot_df = df.df() if not isinstance(df, pd.DataFrame) else df.copy()

    columns = list(plot_df.columns)

    muni_col = _detect_muni_col(columns)
    if muni_col is None:
        raise ValueError(msg["no_muni_col"])

    if value_col is None:
        value_col = _detect_outcome_col(plot_df, exclude={muni_col})
        if value_col is None:
            raise ValueError(msg["no_outcome"])
        if verbose:
            print(msg["col_used"].format(col=value_col))
    elif value_col not in columns:
        raise ValueError(f"Column {value_col!r} not found in df. Available: {columns}")
    if verbose:
        print(msg["muni_col_used"].format(col=muni_col))

    # -- aggregate to muni level (sum outcome, count distinct time_group) --
    plot_df = plot_df.copy()
    plot_df["_muni6"] = plot_df[muni_col].astype(str).str[:6]
    if "time_group" in plot_df.columns:
        muni_agg = plot_df.groupby("_muni6", as_index=False).agg(
            total_cases=(value_col, "sum"),
            n_periods=("time_group", "nunique"),
        )
    else:
        muni_agg = plot_df.groupby("_muni6", as_index=False).agg(
            total_cases=(value_col, "sum"),
        )
        muni_agg["n_periods"] = plot_df.groupby("_muni6").size().to_numpy()

    # -- join municipality metadata ------------------------------------
    if verbose:
        print(msg["loading_meta"])

    munis = municipalities.copy()
    muni_geo_col = _detect_muni_col(list(munis.columns)) or "code_muni"
    if muni_geo_col not in munis.columns:
        raise ValueError(
            f"municipalities must contain a municipality-code column "
            f"(tried {list(_MUNI_COL_PRIORITY)}); found {list(munis.columns)}."
        )
    munis["_code6"] = munis[muni_geo_col].astype(str).str[:6]

    muni_data = muni_agg.merge(munis, left_on="_muni6", right_on="_code6", how="left")

    if city is not None:
        city_list = [city] if isinstance(city, str) else list(city)
        code_matches = {c[:6] for c in (str(x) for x in city_list)}
        name_matches: set[str] = set()
        if "name" in munis.columns:
            lowered = {str(x).lower() for x in city_list}
            name_matches = set(
                muni_data.loc[
                    muni_data["name"].astype(str).str.lower().isin(lowered), "_muni6"
                ]
            )
        keep = muni_data["_muni6"].isin(code_matches | name_matches)
        muni_data = muni_data[keep]
        if muni_data.empty:
            raise ValueError(msg["no_city_match"])

    has_lonlat = "lon" in muni_data.columns and "lat" in muni_data.columns
    n_matched = int(muni_data["lon"].notna().sum()) if has_lonlat else int(
        muni_data["_code6"].notna().sum()
    )
    if n_matched == 0:
        raise ValueError(msg["no_join"])
    if verbose:
        print(msg["n_muni"].format(n=n_matched))

    # -- incidence rate --------------------------------------------------
    if rate_per_100k:
        if verbose:
            print(msg["computing_rate"])
        if "pop" not in muni_data.columns:
            warnings.warn(
                "Column 'pop' not found in municipalities. Falling back to "
                "raw counts.",
                UserWarning,
                stacklevel=2,
            )
            rate_per_100k = False
        else:
            pop = muni_data["pop"]
            muni_data["rate"] = np.where(
                pop.notna() & (pop > 0), (muni_data["total_cases"] / pop) * 1e5, np.nan
            )

    muni_data["fill_var"] = muni_data["rate"] if rate_per_100k else muni_data["total_cases"]

    # -- labels -----------------------------------------------------------
    fill_lbl_base = msg["rate_label"] if rate_per_100k else msg["fill_label"]
    fill_lbl_log = msg["rate_label_log"] if rate_per_100k else msg["fill_label_log"]
    fill_lbl = fill_lbl_log if log_scale else fill_lbl_base
    size_lbl = msg["size_label"]

    cap_lbl = caption if caption is not None else f"{msg['caption']} | climasus4py"

    default_title = {
        "bubble": msg["title_bubble"],
        "choropleth": msg["title_choropleth"],
        "quantile_choropleth": msg["title_quantile"],
    }[map_type]
    map_title = title if title is not None else default_title

    color_vec = _palette_colors(palette, n=9)

    p = _build_map(
        muni_data=muni_data,
        map_type=map_type,
        top_n=top_n,
        state_borders=state_borders,
        show_labels=show_labels,
        color_vec=color_vec,
        log_scale=log_scale,
        fill_lbl=fill_lbl,
        size_lbl=size_lbl,
        map_title=map_title,
        subtitle=subtitle,
        cap_lbl=cap_lbl,
        base_size=base_size,
        msg=msg,
    )

    if verbose:
        print(msg["done"].format(n=n_matched, type=map_type))

    return p


# ---------------------------------------------------------------------------
# Internal map builder
# ---------------------------------------------------------------------------


def _dissolve_state_borders(muni_data: pd.DataFrame) -> Any | None:
    """Dissolve municipality polygons on a state-code column, if present."""
    state_col = next(
        (c for c in ("code_state", "uf_code", "SG_UF", "UF") if c in muni_data.columns),
        None,
    )
    if state_col is None or "geometry" not in muni_data.columns:
        return None
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(muni_data[[state_col, "geometry"]], geometry="geometry")
    return gdf.dissolve(by=state_col, as_index=False)


def _build_map(
    *,
    muni_data: pd.DataFrame,
    map_type: str,
    top_n: int | None,
    state_borders: bool,
    show_labels: bool,
    color_vec: list[str],
    log_scale: bool,
    fill_lbl: str,
    size_lbl: str,
    map_title: str,
    subtitle: str | None,
    cap_lbl: str,
    base_size: float,
    msg: dict[str, str],
) -> Any:
    from plotnine import (
        aes,
        element_text,
        geom_point,
        ggplot,
        guide_colorbar,
        guide_legend,
        guides,
        labs,
        scale_fill_gradientn,
        scale_size_continuous,
        theme,
        theme_void,
    )

    has_geometry = "geometry" in muni_data.columns and muni_data["geometry"].notna().any()

    if map_type in ("choropleth", "quantile_choropleth") and not has_geometry:
        warnings.warn(msg["fallback_bubble"], UserWarning, stacklevel=3)
        map_type = "bubble"
    elif map_type in ("choropleth", "quantile_choropleth"):
        _require_geopandas()

    if map_type == "bubble" and not {"lon", "lat"}.issubset(muni_data.columns):
        if has_geometry:
            # No lon/lat supplied but polygon geometry is present -- derive
            # bubble coordinates from polygon centroids rather than failing.
            import geopandas as gpd

            centroids = gpd.GeoSeries(muni_data["geometry"]).centroid
            muni_data = muni_data.copy()
            muni_data["lon"] = centroids.x
            muni_data["lat"] = centroids.y
        else:
            raise ValueError(
                "municipalities must contain 'lon'/'lat' columns (or a "
                "'geometry' column to derive centroids from) for "
                "map_type='bubble'."
            )

    n_muni_sub = int(muni_data["fill_var"].notna().sum())
    auto_subtitle = f"{n_muni_sub} {msg['municipalities']}{msg['sep']}{fill_lbl}"
    map_subtitle = subtitle if subtitle is not None else auto_subtitle

    sc_trans = "log1p" if log_scale else "identity"
    fill_scale_base = scale_fill_gradientn(
        colors=color_vec,
        trans=sc_trans,
        name=fill_lbl,
        na_value="#EBEBEB",
    )

    state_sf = None
    if state_borders and has_geometry:
        state_sf = _dissolve_state_borders(muni_data)
    elif state_borders and "geometry" not in muni_data.columns:
        state_sf = None

    if map_type == "bubble":
        muni_plot = muni_data.dropna(subset=["lon", "lat"]).copy()
        muni_plot = muni_plot.sort_values("total_cases", ascending=False)

        p = ggplot()

        if state_sf is not None:
            from plotnine import geom_map

            p = p + geom_map(
                data=state_sf, fill="#F8F8F8", color="#AAAAAA", size=0.30
            )

        p = p + geom_point(
            data=muni_plot,
            mapping=aes(x="lon", y="lat", size="total_cases", fill="fill_var"),
            shape="o",
            color="white",
            stroke=0.3,
            alpha=0.80,
        )
        p = p + scale_size_continuous(range=(1, 8), trans="log1p", name=size_lbl)
        p = p + fill_scale_base
        p = p + guides(
            size=guide_legend(nrow=1, order=1),
            fill=guide_colorbar(order=2),
        )

        label_data = None
        if show_labels and "is_capital" in muni_plot.columns:
            caps = muni_plot[
                muni_plot["is_capital"].isin([True, 1, "TRUE", "1"])
            ]
            if not caps.empty:
                label_data = caps
        if top_n is not None and top_n > 0:
            label_data = muni_plot.nlargest(top_n, "total_cases")

        if label_data is not None and not label_data.empty and "name" in label_data.columns:
            from plotnine import geom_text

            p = p + geom_text(
                data=label_data,
                mapping=aes(x="lon", y="lat", label="name"),
                size=7,
                color="#3A3A3A",
                fontweight="bold",
            )

    else:
        import geopandas as gpd
        from plotnine import geom_map

        poly = gpd.GeoDataFrame(muni_data, geometry="geometry")

        p = ggplot()

        if map_type == "choropleth":
            p = p + geom_map(
                data=poly, mapping=aes(fill="fill_var"), color="#C8C8C8", size=0.05
            )
            p = p + fill_scale_base
            p = p + guides(fill=guide_colorbar(order=1))
        else:
            n_cls = 5
            q_probs = np.linspace(0, 1, n_cls + 1)
            q_breaks = np.unique(poly["fill_var"].quantile(q_probs).dropna().to_numpy())
            if len(q_breaks) < 2:
                q_breaks = np.array([0, poly["fill_var"].max()])
            fill_class = pd.cut(poly["fill_var"], bins=q_breaks, include_lowest=True)
            # Keep the ordered Categorical (not .astype(str)) so 'levels'
            # below reflects bin order, not first-appearance order in the
            # data -- otherwise the fill scale can hand the lightest colour
            # to the highest bin.
            levels = [str(lv) for lv in fill_class.cat.categories]
            poly["fill_class"] = fill_class.astype(str)
            cls_cols = _interpolate(list(color_vec), max(len(levels), 2))
            from plotnine import scale_fill_manual

            p = p + geom_map(
                data=poly, mapping=aes(fill="fill_class"), color="#C8C8C8", size=0.05
            )
            p = p + scale_fill_manual(
                values=dict(zip(levels, cls_cols, strict=False)),
                name=fill_lbl,
                na_value="#EBEBEB",
            )

        if state_sf is not None:
            p = p + geom_map(data=state_sf, fill=None, color="#4A4A4A", size=0.35)

    p = p + labs(
        title=map_title,
        subtitle=map_subtitle,
        caption=cap_lbl,
        fill=fill_lbl,
        size=size_lbl,
    )
    p = p + theme_void(base_size=base_size)
    p = p + theme(
        legend_position="bottom",
        plot_title=element_text(fontweight="bold", size=base_size + 1),
        # "grey45" (R base colour name) has no equivalent in matplotlib's
        # named-colour table; use its exact hex value instead (mirrors the
        # same fix already applied in mod_plot_spatial_moran).
        plot_subtitle=element_text(color="#737373", size=base_size * 0.88),
        plot_caption=element_text(size=base_size - 2, color="#888888"),
    )

    return p
