"""Heatwave-event visualisation — ggplot-style via plotnine.

Mirrors R: sus_climate_plot_heatwaves (sus_climate_plot_heatwaves.R)

Visualises the ``dict`` returned by ``sus_climate_compute_heatwaves``
(keys ``"events"``, ``"daily"``, ``"summary"``) as one of four plot types:
a Gantt-style event timeline, a calendar heatmap of heatwave days, an
intensity-vs-duration scatter, or an annual trend bar chart.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> hw = cs.sus_climate_compute_heatwaves(df_indicators)
    >>> p = cs.sus_climate_plot_heatwaves(hw, type="timeline")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's per-plot `labels <- switch(lang, ...)`)
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, dict[str, str]]] = {
    "en": {
        "timeline": {
            "title": "Heatwave Events Timeline",
            "subtitle": "Duration and intensity of events across stations",
            "x": "Date",
            "y": "Station",
            "dur": "Duration (days)",
            "int": "Intensity",
        },
        "calendar": {
            "title": "Heatwave Calendar",
            "subtitle": "Daily occurrence of heatwaves",
            "x": "Month",
            "y": "Day",
            "fill": "Heatwave",
            "yes": "Yes",
            "no": "No",
        },
        "intensity": {
            "title": "Heatwave Intensity vs Duration",
            "subtitle": "Relationship between event length and peak temperature",
            "x": "Duration (days)",
            "y": "Peak Temperature (degC)",
            "color": "Intensity",
        },
        "trend": {
            "title": "Annual Heatwave Trend",
            "subtitle": "Number of heatwave events per year by method",
            "x": "Year",
            "y": "Number of Events",
            "fill": "Method",
        },
        "classes": {
            "unknown": "Unknown",
            "low": "Low Intensity (LIHW)",
            "severe": "Severe (SHW)",
            "extreme": "Extreme (EHW)",
        },
    },
    "pt": {
        "timeline": {
            "title": "Linha do Tempo de Ondas de Calor",
            "subtitle": "Duracao e intensidade dos eventos por estacao",
            "x": "Data",
            "y": "Estacao",
            "dur": "Duracao (dias)",
            "int": "Intensidade",
        },
        "calendar": {
            "title": "Calendario de Ondas de Calor",
            "subtitle": "Ocorrencia diaria de ondas de calor",
            "x": "Mes",
            "y": "Dia",
            "fill": "Onda de Calor",
            "yes": "Sim",
            "no": "Nao",
        },
        "intensity": {
            "title": "Intensidade vs Duracao da Onda de Calor",
            "subtitle": "Relacao entre a duracao do evento e a temperatura maxima",
            "x": "Duracao (dias)",
            "y": "Temperatura Maxima (degC)",
            "color": "Intensidade",
        },
        "trend": {
            "title": "Tendencia Anual de Ondas de Calor",
            "subtitle": "Numero de eventos de onda de calor por ano e metodo",
            "x": "Ano",
            "y": "Numero de Eventos",
            "fill": "Metodo",
        },
        "classes": {
            "unknown": "Desconhecida",
            "low": "Baixa Intensidade (LIHW)",
            "severe": "Severa (SHW)",
            "extreme": "Extrema (EHW)",
        },
    },
    "es": {
        "timeline": {
            "title": "Linea de Tiempo de Olas de Calor",
            "subtitle": "Duracion e intensidad de los eventos por estacion",
            "x": "Fecha",
            "y": "Estacion",
            "dur": "Duracion (dias)",
            "int": "Intensidad",
        },
        "calendar": {
            "title": "Calendario de Olas de Calor",
            "subtitle": "Ocurrencia diaria de olas de calor",
            "x": "Mes",
            "y": "Dia",
            "fill": "Ola de Calor",
            "yes": "Si",
            "no": "No",
        },
        "intensity": {
            "title": "Intensidad vs Duracion de la Ola de Calor",
            "subtitle": "Relacion entre la duracion del evento y la temperatura maxima",
            "x": "Duracion (dias)",
            "y": "Temperatura Maxima (degC)",
            "color": "Intensidad",
        },
        "trend": {
            "title": "Tendencia Anual de Olas de Calor",
            "subtitle": "Numero de eventos de ola de calor por ano y metodo",
            "x": "Ano",
            "y": "Numero de Eventos",
            "fill": "Metodo",
        },
        "classes": {
            "unknown": "Desconocida",
            "low": "Baja Intensidad (LIHW)",
            "severe": "Severa (SHW)",
            "extreme": "Extrema (EHW)",
        },
    },
}

_MONTH_ABBR = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)

# ggsci "npg" (Nature Publishing Group) palette, first five colors — the
# only ggsci palette the R source's default/fallback path actually reaches
# (`.get_hw_palette()` tries `pal_<name>`, falling back to `pal_npg` with a
# warning on lookup failure). Reproducing all ~26 ggsci palettes here would
# be scope creep beyond what this function needs; see IDEIAS.md.
_NPG_PALETTE = (
    "#E64B35FF", "#4DBBD5FF", "#00A087FF", "#3C5488FF", "#F39B7FFF",
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
            "sus_climate_plot_heatwaves requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _get_hw_palette(color_palette: str) -> dict[str, str]:
    """Resolve the color roles used by the plots.

    Only the ggsci "npg" palette is reproduced (see module-level comment);
    any other name falls back to "npg" with a warning, mirroring the R
    source's ``tryCatch`` fallback in ``.get_hw_palette()``. Note the
    ``severe`` role is index 2 of the npg palette (light blue, not
    orange/yellow as the R source's code comment claims) — the color
    itself is preserved as-is per index, only the misleading R comment is
    not carried over.
    """
    if color_palette != "npg":
        warnings.warn(
            f"Palette '{color_palette}' not found. Using 'npg' instead.",
            UserWarning,
            stacklevel=3,
        )
    cols = _NPG_PALETTE
    return {
        "low": cols[3],
        "severe": cols[1],
        "extreme": cols[0],
        "neutral": "#E0E0E0",
    }


def _translate_intensity_class(
    series: pd.Series, labels: dict[str, str]
) -> pd.Series:
    """Rewrite English intensity-class labels into the target language."""
    mapping = {
        "Low Intensity (LIHW)": labels["low"],
        "Severe (SHW)": labels["severe"],
        "Extreme (EHW)": labels["extreme"],
        "Unknown": labels["unknown"],
    }
    return series.replace(mapping)


def _hw_theme() -> Any:
    """Shared plotnine theme (mirrors R's ``.hw_theme()``)."""
    import plotnine as p9

    return p9.theme_minimal() + p9.theme(
        plot_title=p9.element_text(
            face="bold", size=16, color="#2C3E50", margin={"b": 10}
        ),
        plot_subtitle=p9.element_text(size=12, color="#7F8C8D", margin={"b": 15}),
        axis_title=p9.element_text(face="bold", size=12, color="#34495E"),
        axis_text=p9.element_text(size=10, color="#7F8C8D"),
        legend_title=p9.element_text(face="bold", size=11, color="#2C3E50"),
        legend_text=p9.element_text(size=10, color="#34495E"),
        legend_position="bottom",
        panel_grid_minor=p9.element_blank(),
        panel_grid_major=p9.element_line(color="#ECF0F1", size=0.5),
        plot_background=p9.element_rect(fill="white", color=None),
        panel_background=p9.element_rect(fill="white", color=None),
        strip_text=p9.element_text(face="bold", size=11, color="#2C3E50"),
        strip_background=p9.element_rect(fill="#ECF0F1", color=None),
    )


def _prep_intensity_class(events: pd.DataFrame, labels: dict[str, str], lang: str) -> pd.DataFrame:
    """Ensure ``intensity_class`` exists as an object column and is translated."""
    events = events.copy()
    if "intensity_class" not in events.columns:
        events["intensity_class"] = "Unknown"
    events["intensity_class"] = events["intensity_class"].astype(object)
    if lang in ("pt", "es"):
        events["intensity_class"] = _translate_intensity_class(
            events["intensity_class"], labels
        )
    return events


def _intensity_color_scale(colors: dict[str, str], labels: dict[str, str]) -> Any:
    """``scale_color_manual`` shared by the timeline and intensity plots.

    Mirrors R's ``setNames(c(low, severe, extreme, severe), c(low, severe,
    extreme, unknown))`` — note "unknown" is deliberately mapped to the
    *severe* color (a preserved R quirk, not a typo fixed here).
    """
    import plotnine as p9

    values = {
        labels["low"]: colors["low"],
        labels["severe"]: colors["severe"],
        labels["extreme"]: colors["extreme"],
        labels["unknown"]: colors["severe"],
    }
    return p9.scale_color_manual(values=values, na_value=colors["severe"])


# ---------------------------------------------------------------------------
# Per-type plot builders
# ---------------------------------------------------------------------------


def _plot_hw_timeline(events: pd.DataFrame, colors: dict[str, str], lang: str) -> Any:
    import plotnine as p9

    strings = _I18N[lang]
    labels = {**strings["timeline"], **strings["classes"]}
    events = _prep_intensity_class(events, labels, lang)

    p = (
        p9.ggplot(events)
        + p9.geom_segment(
            p9.aes(
                x="start_date",
                xend="end_date",
                y="station_code",
                yend="station_code",
                color="intensity_class",
                size="duration_days",
            ),
            alpha=0.8,
        )
        + _intensity_color_scale(colors, labels)
        + p9.scale_size_continuous(range=(2, 8), name=labels["dur"])
        + p9.scale_x_datetime(date_labels="%b %Y", date_breaks="3 months")
        + p9.labs(
            title=labels["title"],
            subtitle=labels["subtitle"],
            x=labels["x"],
            y=labels["y"],
            color=labels["int"],
        )
        + _hw_theme()
    )
    return p


def _plot_hw_calendar(
    daily: pd.DataFrame, method: str | list[str] | None, colors: dict[str, str], lang: str
) -> Any:
    import plotnine as p9

    strings = _I18N[lang]
    labels = strings["calendar"]

    # R takes `method[1]` (the first element) when a vector is passed; the
    # `year` argument accepted by R's `.plot_hw_calendar()` is unused dead
    # code there (facetting is derived from the data itself) and is not
    # replicated as a parameter here.
    if method is None:
        hw_col = "hw_any"
    else:
        first_method = method if isinstance(method, str) else method[0]
        hw_col = f"hw_{first_method.lower()}"

    if hw_col not in daily.columns:
        raise ValueError(f"Column {hw_col} not found in daily data.")

    cal_data = daily.copy()
    cal_data["year"] = cal_data["date_day"].dt.year
    cal_data["month"] = pd.Categorical(
        cal_data["date_day"].dt.strftime("%b"), categories=_MONTH_ABBR, ordered=True
    )
    cal_data["day"] = cal_data["date_day"].dt.day
    cal_data["is_hw"] = cal_data[hw_col].astype(object)

    p = (
        p9.ggplot(cal_data, p9.aes(x="month", y="day", fill="is_hw"))
        + p9.geom_tile(color="white", size=0.5)
        + p9.scale_fill_manual(
            values={True: colors["extreme"], False: colors["neutral"]},
            labels={True: labels["yes"], False: labels["no"]},
            name=labels["fill"],
        )
        + p9.scale_y_reverse(breaks=list(range(1, 32)))
        + p9.labs(title=labels["title"], subtitle=labels["subtitle"], x=labels["x"], y=labels["y"])
        + _hw_theme()
        + p9.theme(
            panel_grid_major=p9.element_blank(),
            axis_text_y=p9.element_text(size=7),
        )
    )
    if cal_data["year"].nunique() > 1:
        p = p + p9.facet_wrap("year", ncol=3)
    return p


def _plot_hw_intensity(events: pd.DataFrame, colors: dict[str, str], lang: str) -> Any:
    import plotnine as p9

    strings = _I18N[lang]
    labels = {**strings["intensity"], **strings["classes"]}
    events = _prep_intensity_class(events, labels, lang)

    p = (
        p9.ggplot(
            events,
            p9.aes(
                x="duration_days",
                y="temp_peak",
                color="intensity_class",
                size="duration_days",
            ),
        )
        + p9.geom_point(alpha=0.7)
        + _intensity_color_scale(colors, labels)
        + p9.labs(
            title=labels["title"],
            subtitle=labels["subtitle"],
            x=labels["x"],
            y=labels["y"],
            color=labels["color"],
        )
        + _hw_theme()
        + p9.guides(size="none")
    )
    return p


def _plot_hw_trend(summary_df: pd.DataFrame, colors: dict[str, str], lang: str) -> Any:
    import plotnine as p9

    strings = _I18N[lang]
    labels = strings["trend"]

    trend_data = (
        summary_df.groupby(["year", "method"], dropna=False)["n_events"]
        .sum(min_count=1)
        .reset_index()
        .rename(columns={"n_events": "total_events"})
    )

    method_colors = {
        "EHF": colors["extreme"],
        "INMET": colors["severe"],
        "WHO": colors["low"],
        "WMO": "#4DBBD5FF",
        "UTCI": "#00A087FF",
        "WBGT": "#3C5488FF",
        "HI": "#F39B7FFF",
    }

    p = (
        p9.ggplot(trend_data, p9.aes(x="year", y="total_events", fill="method"))
        + p9.geom_col(position="dodge", alpha=0.9)
        + p9.scale_fill_manual(values=method_colors, name=labels["fill"])
        + p9.labs(title=labels["title"], subtitle=labels["subtitle"], x=labels["x"], y=labels["y"])
        + _hw_theme()
    )
    return p


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_plot_heatwaves(
    hw_result: dict[str, pd.DataFrame],
    type: Literal["timeline", "calendar", "intensity", "trend"] = "timeline",
    station_code: str | list[str] | None = None,
    method: str | list[str] | None = None,
    year: int | list[int] | None = None,
    interactive: bool = True,
    color_palette: str = "npg",
    lang: Literal["en", "pt", "es"] = "en",
    save_plot: str | None = None,
) -> Any:  # returns plotnine.ggplot, or None if all events are filtered out
    """Plot and analyze heatwave events.

    Visualises the result of ``sus_climate_compute_heatwaves`` as one of
    four chart types: a Gantt-style timeline of events, a calendar heatmap
    of heatwave days, an intensity-vs-duration scatter, or an annual trend
    bar chart.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        hw_result: The ``dict`` returned by ``sus_climate_compute_heatwaves``
            (must contain the keys ``"events"``, ``"daily"``, ``"summary"``).
        type: Plot type — ``"timeline"``, ``"calendar"``, ``"intensity"``,
            or ``"trend"``.
        station_code: Optional station code or list of codes to filter by.
        method: Optional method name (or list) to filter by (e.g. ``"EHF"``,
            ``"INMET"``). For ``type="calendar"`` only, this also selects
            which ``hw_<method>`` daily flag column is plotted (defaults to
            ``hw_any``); when several methods are passed only the first is
            used for that selection, mirroring the R source's ``method[1]``.
        year: Optional year (or list of years) to filter by.
        interactive: Kept for signature parity with the R function, which
            returns a Plotly figure when ``True``. plotnine has no faithful
            equivalent, so this implementation always returns a static
            ``ggplot`` and emits a warning when ``interactive=True`` — see
            IDEIAS.md.
        color_palette: Name of the color palette to use. Only ``"npg"``
            (the ggsci default) is implemented; any other value falls back
            to ``"npg"`` with a warning, mirroring the R source's
            ``tryCatch`` fallback.
        lang: Language for labels and titles: ``"en"`` (default), ``"pt"``,
            or ``"es"``.
        save_plot: Optional file path to save the plot to (via
            ``ggplot.save``), e.g. ``"plot.png"``.

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``),
        or ``None`` if no heatwave events remain after filtering (a
        ``UserWarning`` is emitted in that case) — mirrors the R source's
        ``cli_warn`` + ``return(NULL)`` behavior.

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``).
        ValueError: If *hw_result* is not a dict with ``events``/``daily``/
            ``summary`` keys, or if *type* is invalid, or (for
            ``type="calendar"``) if the resolved ``hw_<method>`` column is
            not present in the daily table.

    Examples::

        import climasus4py as cs

        hw = cs.sus_climate_compute_heatwaves(df_indicators, method=["EHF", "INMET"])
        p = cs.sus_climate_plot_heatwaves(hw, type="timeline", lang="pt")
        p.draw()
    """
    if not (
        isinstance(hw_result, dict)
        and all(k in hw_result for k in ("events", "daily", "summary"))
    ):
        raise ValueError(
            "hw_result must be the dict output of sus_climate_compute_heatwaves()."
        )

    if type not in ("timeline", "calendar", "intensity", "trend"):
        raise ValueError(
            f"Invalid type '{type}'. Expected one of: "
            "'timeline', 'calendar', 'intensity', 'trend'."
        )

    if lang not in ("en", "pt", "es"):
        raise ValueError(f"Invalid lang '{lang}'. Expected one of: 'en', 'pt', 'es'.")

    events_df = hw_result["events"].copy()
    daily_df = hw_result["daily"].copy()
    summary_df = hw_result["summary"].copy()

    if station_code is not None:
        codes = [station_code] if isinstance(station_code, str) else list(station_code)
        events_df = events_df[events_df["station_code"].isin(codes)]
        daily_df = daily_df[daily_df["station_code"].isin(codes)]
        summary_df = summary_df[summary_df["station_code"].isin(codes)]

    if method is not None:
        methods = [method] if isinstance(method, str) else list(method)
        events_df = events_df[events_df["method"].isin(methods)]
        summary_df = summary_df[summary_df["method"].isin(methods)]
        # Note: daily_df is intentionally NOT filtered by method — it has
        # no `method` column (one row per station/day, with a `hw_<method>`
        # flag column per method), matching R's `.plot_hw_calendar()`.

    if year is not None:
        years = [year] if isinstance(year, int) else list(year)
        events_df = events_df[events_df["start_date"].dt.year.isin(years)]
        daily_df = daily_df[daily_df["date_day"].dt.year.isin(years)]
        summary_df = summary_df[summary_df["year"].isin(years)]

    if len(events_df) == 0:
        warnings.warn(
            "No heatwave events found after applying filters.",
            UserWarning,
            stacklevel=2,
        )
        return None

    _require_plotnine()

    if interactive:
        warnings.warn(
            "sus_climate_plot_heatwaves: interactive=True has no plotnine "
            "equivalent (R returns a Plotly figure here); returning a "
            "static ggplot instead. See IDEIAS.md.",
            UserWarning,
            stacklevel=2,
        )

    colors = _get_hw_palette(color_palette)

    if type == "timeline":
        p = _plot_hw_timeline(events_df, colors, lang)
    elif type == "calendar":
        p = _plot_hw_calendar(daily_df, method, colors, lang)
    elif type == "intensity":
        p = _plot_hw_intensity(events_df, colors, lang)
    else:  # "trend"
        p = _plot_hw_trend(summary_df, colors, lang)

    if save_plot is not None:
        p.save(filename=save_plot, width=10, height=6, dpi=300)

    return p
