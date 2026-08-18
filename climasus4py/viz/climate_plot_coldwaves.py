"""Coldwave event visualisation — ggplot-style via plotnine.

Mirrors R: sus_climate_plot_coldwaves (sus_climate_plot_coldwaves.R)

Visualises the ``dict`` returned by ``sus_climate_compute_coldwaves()``
(keys ``"events"``, ``"daily"``, ``"summary"`` — the Python analogue of
the R function's attributed ``climasus_cw`` list). Four plot types are
supported, mirroring the R helpers one-to-one:
  - ``"timeline"``  (``.plot_cw_timeline``)  — Gantt-style event timeline.
  - ``"calendar"``  (``.plot_cw_calendar``)  — daily calendar heatmap.
  - ``"intensity"`` (``.plot_cw_intensity``) — duration vs. coldest-temp scatter.
  - ``"trend"``     (``.plot_cw_trend``)     — annual event count bar chart.

Not lazy — operates on the ``pd.DataFrame`` tables already materialised by
``sus_climate_compute_coldwaves()``; there is no DuckDB relation involved.

Deliberately narrower than the R source: the R function also accepts
``interactive`` (plotly output via ``plotly::ggplotly``) and ``save_plot``
(``ggsave``/``htmlwidgets::saveWidget``). Neither is ported — matching the
precedent set by ``sus_climate_plot_fill`` in this same module family —
because ``plotly`` is not a climasus4py dependency (nor an existing
``[plot]``-extra one) and the returned ``plotnine.ggplot`` already exposes
``.draw()``/``.save()`` directly. See IDEIAS.md.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> cw = cs.sus_climate_compute_coldwaves(df_indicators, method=["WHO", "EHF"])
    >>> p = cs.sus_climate_plot_coldwaves(cw, type="timeline", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import warnings
from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings (ASCII, no diacritics — matches the R source's
# own pt/es label strings verbatim, a deliberate choice preserved as-is)
# ---------------------------------------------------------------------------

_INTENSITY_LABELS: dict[str, dict[str, str]] = {
    "en": {
        "unknown": "Unknown",
        "low": "Low Intensity (LICW)",
        "severe": "Severe (SCW)",
        "extreme": "Extreme (ECW)",
    },
    "pt": {
        "unknown": "Desconhecida",
        "low": "Baixa Intensidade (LICW)",
        "severe": "Severa (SCW)",
        "extreme": "Extrema (ECW)",
    },
    "es": {
        "unknown": "Desconocida",
        "low": "Baja Intensidad (LICW)",
        "severe": "Severa (SCW)",
        "extreme": "Extrema (ECW)",
    },
}

# Raw class strings as written by sus_climate_compute_coldwaves()'s
# `_extract_events` classifier (English literals, only ever produced for
# EHF events — see IDEIAS.md).
_RAW_LOW = "Low Intensity (LICW)"
_RAW_SEVERE = "Severe (SCW)"
_RAW_EXTREME = "Extreme (ECW)"
_RAW_UNKNOWN = "Unknown"

_I18N: dict[str, dict[str, str]] = {
    "en": {
        "err_bad_result": (
            "cw_result must be the dict returned by sus_climate_compute_coldwaves()."
        ),
        "warn_no_events": "No coldwave events found after applying filters.",
        "err_calendar_col": "Column {col} not found in daily data.",
        "timeline_title": "Coldwave Events Timeline",
        "timeline_subtitle": "Duration and intensity of events across stations",
        "timeline_x": "Date", "timeline_y": "Station",
        "timeline_dur": "Duration (days)", "timeline_int": "Intensity",
        "calendar_title": "Coldwave Calendar",
        "calendar_subtitle": "Daily occurrence of coldwaves",
        "calendar_x": "Month", "calendar_y": "Day", "calendar_fill": "Coldwave",
        "calendar_yes": "Yes", "calendar_no": "No",
        "intensity_title": "Coldwave Intensity vs Duration",
        "intensity_subtitle": "Relationship between event length and coldest temperature",
        "intensity_x": "Duration (days)", "intensity_y": "Coldest Temperature (°C)",
        "intensity_color": "Intensity",
        "trend_title": "Annual Coldwave Trend",
        "trend_subtitle": "Number of coldwave events per year by method",
        "trend_x": "Year", "trend_y": "Number of Events", "trend_fill": "Method",
    },
    "pt": {
        "err_bad_result": (
            "cw_result deve ser o dict retornado por sus_climate_compute_coldwaves()."
        ),
        "warn_no_events": "Nenhum evento de onda de frio encontrado apos aplicar os filtros.",
        "err_calendar_col": "Coluna {col} nao encontrada nos dados diarios.",
        "timeline_title": "Linha do Tempo de Ondas de Frio",
        "timeline_subtitle": "Duracao e intensidade dos eventos por estacao",
        "timeline_x": "Data", "timeline_y": "Estacao",
        "timeline_dur": "Duracao (dias)", "timeline_int": "Intensidade",
        "calendar_title": "Calendario de Ondas de Frio",
        "calendar_subtitle": "Ocorrencia diaria de ondas de frio",
        "calendar_x": "Mes", "calendar_y": "Dia", "calendar_fill": "Onda de Frio",
        "calendar_yes": "Sim", "calendar_no": "Nao",
        "intensity_title": "Intensidade vs Duracao da Onda de Frio",
        "intensity_subtitle": "Relacao entre a duracao do evento e a temperatura minima",
        "intensity_x": "Duracao (dias)", "intensity_y": "Temperatura Minima (°C)",
        "intensity_color": "Intensidade",
        "trend_title": "Tendencia Anual de Ondas de Frio",
        "trend_subtitle": "Numero de eventos de onda de frio por ano e metodo",
        "trend_x": "Ano", "trend_y": "Numero de Eventos", "trend_fill": "Metodo",
    },
    "es": {
        "err_bad_result": (
            "cw_result debe ser el dict retornado por sus_climate_compute_coldwaves()."
        ),
        "warn_no_events": "No se encontraron eventos de ola de frio tras aplicar los filtros.",
        "err_calendar_col": "Columna {col} no encontrada en los datos diarios.",
        "timeline_title": "Linea de Tiempo de Olas de Frio",
        "timeline_subtitle": "Duracion e intensidad de los eventos por estacion",
        "timeline_x": "Fecha", "timeline_y": "Estacion",
        "timeline_dur": "Duracion (dias)", "timeline_int": "Intensidad",
        "calendar_title": "Calendario de Olas de Frio",
        "calendar_subtitle": "Ocurrencia diaria de olas de frio",
        "calendar_x": "Mes", "calendar_y": "Dia", "calendar_fill": "Ola de Frio",
        "calendar_yes": "Si", "calendar_no": "No",
        "intensity_title": "Intensidad vs Duracion de la Ola de Frio",
        "intensity_subtitle": "Relacion entre la duracion del evento y la temperatura minima",
        "intensity_x": "Duracion (dias)", "intensity_y": "Temperatura Minima (°C)",
        "intensity_color": "Intensidad",
        "trend_title": "Tendencia Anual de Olas de Frio",
        "trend_subtitle": "Numero de eventos de ola de frio por ano y metodo",
        "trend_x": "Ano", "trend_y": "Numero de Eventos", "trend_fill": "Metodo",
    },
}

# Fixed palette. R quirk preserved: `.get_cw_palette()` looks up a ggsci
# palette function by `palette_name` (falling back to "npg" with a warning
# if the name is unknown) but never actually uses the looked-up colours —
# it always returns this same hardcoded dict regardless of `color_palette`.
# See IDEIAS.md.
_CW_COLORS = {
    "low": "#4DBBD5FF",
    "severe": "#3C5488FF",
    "extreme": "#8B1A4AFF",
    "neutral": "#E0E0E0",
    "text": "#333333",
    "bg": "#FFFFFF",
}

# Per-method fixed fill colours for the trend plot (matches R's .plot_cw_trend).
_METHOD_COLORS = {
    "EHF": "#8B1A4AFF",
    "INMET": "#3C5488FF",
    "WHO": "#4DBBD5FF",
    "WMO": "#00A087FF",
    "UTCI": "#4DBBD5FF",
    "WBGT": "#8B1A4AFF",
    "HI": "#F39B7FFF",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_climate_plot_coldwaves requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _as_list(x: Any) -> list[Any] | None:
    if x is None:
        return None
    if isinstance(x, (list, tuple, set)):
        return list(x)
    return [x]


def _translate_intensity(series: pd.Series, labels: dict[str, str]) -> pd.Series:
    """Map raw English intensity_class literals to localized labels.

    Non-matching / NaN values pass through unchanged, mirroring the R
    helper's `gsub()`-based translation.
    """
    mapping = {
        _RAW_LOW: labels["low"],
        _RAW_SEVERE: labels["severe"],
        _RAW_EXTREME: labels["extreme"],
        _RAW_UNKNOWN: labels["unknown"],
    }
    return series.map(lambda v: mapping.get(v, v))


def _cw_theme() -> Any:
    from plotnine import (
        element_blank,
        element_line,
        element_rect,
        element_text,
        theme,
        theme_minimal,
    )

    return theme_minimal() + theme(
        plot_title=element_text(size=16, color="#2C3E50"),
        plot_subtitle=element_text(size=12, color="#7F8C8D"),
        axis_title=element_text(size=12, color="#34495E"),
        axis_text=element_text(size=10, color="#7F8C8D"),
        legend_title=element_text(size=11, color="#2C3E50"),
        legend_text=element_text(size=10, color="#34495E"),
        legend_position="bottom",
        panel_grid_minor=element_blank(),
        panel_grid_major=element_line(color="#ECF0F1", size=0.5),
        plot_background=element_rect(fill="white"),
        panel_background=element_rect(fill="white"),
        strip_text=element_text(size=11, color="#2C3E50"),
        strip_background=element_rect(fill="#ECF0F1"),
    )


def _plot_cw_timeline(events: pd.DataFrame, strings: dict[str, str], labels: dict[str, str]) -> Any:
    from plotnine import (
        aes,
        geom_segment,
        ggplot,
        labs,
        scale_color_manual,
        scale_size_continuous,
        scale_x_datetime,
    )

    events = events.copy()
    if "intensity_class" not in events.columns:
        events["intensity_class"] = _RAW_UNKNOWN
    events["intensity_class"] = _translate_intensity(events["intensity_class"], labels)

    color_values = {
        labels["low"]: _CW_COLORS["low"],
        labels["severe"]: _CW_COLORS["severe"],
        labels["extreme"]: _CW_COLORS["extreme"],
        labels["unknown"]: _CW_COLORS["severe"],
    }

    p = (
        ggplot(
            events,
            aes(
                x="start_date",
                xend="end_date",
                y="station_code",
                yend="station_code",
                color="intensity_class",
                size="duration_days",
            ),
        )
        + geom_segment(alpha=0.8)
        + scale_color_manual(
            values=color_values, name=strings["timeline_int"], na_value=_CW_COLORS["neutral"]
        )
        + scale_size_continuous(range=(2, 8), name=strings["timeline_dur"])
        + scale_x_datetime(date_labels="%b %Y", date_breaks="3 months")
        + labs(
            title=strings["timeline_title"],
            subtitle=strings["timeline_subtitle"],
            x=strings["timeline_x"],
            y=strings["timeline_y"],
        )
        + _cw_theme()
    )
    return p


def _plot_cw_calendar(
    daily: pd.DataFrame,
    method: list[str] | None,
    strings: dict[str, str],
) -> Any:
    from plotnine import (
        aes,
        element_blank,
        element_text,
        facet_wrap,
        geom_tile,
        ggplot,
        labs,
        scale_fill_manual,
        scale_y_reverse,
        theme,
    )

    cw_col = "cw_any" if method is None else f"cw_{str(method[0]).lower()}"
    if cw_col not in daily.columns:
        raise ValueError(strings["err_calendar_col"].format(col=cw_col))

    cal_data = daily.copy()
    cal_data["date_day"] = pd.to_datetime(cal_data["date_day"])
    cal_data["year"] = cal_data["date_day"].dt.year
    month_cats = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    cal_data["month"] = pd.Categorical(
        cal_data["date_day"].dt.strftime("%b"), categories=month_cats, ordered=True
    )
    cal_data["day"] = cal_data["date_day"].dt.day
    cal_data["is_cw"] = cal_data[cw_col].astype("boolean")

    p = (
        ggplot(cal_data, aes(x="month", y="day", fill="is_cw"))
        + geom_tile(color="white", size=0.5)
        + scale_fill_manual(
            values={True: _CW_COLORS["extreme"], False: _CW_COLORS["neutral"]},
            labels={True: strings["calendar_yes"], False: strings["calendar_no"]},
            name=strings["calendar_fill"],
        )
        + scale_y_reverse(breaks=list(range(1, 32)))
        + labs(
            title=strings["calendar_title"],
            subtitle=strings["calendar_subtitle"],
            x=strings["calendar_x"],
            y=strings["calendar_y"],
        )
        + _cw_theme()
        + theme(panel_grid_major=element_blank(), axis_text_y=element_text(size=7))
    )

    if cal_data["year"].nunique() > 1:
        p = p + facet_wrap("year", ncol=3)

    return p


def _plot_cw_intensity(
    events: pd.DataFrame, strings: dict[str, str], labels: dict[str, str]
) -> Any:
    from plotnine import (
        aes,
        geom_point,
        ggplot,
        guides,
        labs,
        scale_color_manual,
        scale_y_reverse,
    )

    events = events.copy()
    if "intensity_class" not in events.columns:
        events["intensity_class"] = _RAW_UNKNOWN
    events["intensity_class"] = _translate_intensity(events["intensity_class"], labels)

    color_values = {
        labels["low"]: _CW_COLORS["low"],
        labels["severe"]: _CW_COLORS["severe"],
        labels["extreme"]: _CW_COLORS["extreme"],
        labels["unknown"]: _CW_COLORS["severe"],
    }

    p = (
        ggplot(
            events,
            aes(
                x="duration_days",
                y="temp_peak",
                color="intensity_class",
                size="duration_days",
            ),
        )
        + geom_point(alpha=0.7)
        + scale_color_manual(
            values=color_values, name=strings["intensity_color"], na_value=_CW_COLORS["neutral"]
        )
        # Lower temp_peak = more extreme cold; reverse so visually higher = colder.
        + scale_y_reverse()
        + labs(
            title=strings["intensity_title"],
            subtitle=strings["intensity_subtitle"],
            x=strings["intensity_x"],
            y=strings["intensity_y"],
        )
        + _cw_theme()
        + guides(size="none")
    )
    return p


def _plot_cw_trend(summary_df: pd.DataFrame, strings: dict[str, str]) -> Any:
    from plotnine import aes, geom_col, ggplot, labs, scale_fill_manual

    trend_data = (
        summary_df.groupby(["year", "method"], as_index=False)["n_events"]
        .sum()  # matches R's sum(n_events, na.rm = TRUE): all-NA group -> 0
        .rename(columns={"n_events": "total_events"})
    )

    p = (
        ggplot(trend_data, aes(x="year", y="total_events", fill="method"))
        + geom_col(position="dodge", alpha=0.9)
        + scale_fill_manual(values=_METHOD_COLORS, name=strings["trend_fill"])
        + labs(
            title=strings["trend_title"],
            subtitle=strings["trend_subtitle"],
            x=strings["trend_x"],
            y=strings["trend_y"],
        )
        + _cw_theme()
    )
    return p


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_plot_coldwaves(
    cw_result: dict[str, pd.DataFrame],
    type: Literal["timeline", "calendar", "intensity", "trend"] = "timeline",
    station_code: str | list[str] | None = None,
    method: str | list[str] | None = None,
    year: int | list[int] | None = None,
    color_palette: str = "npg",
    lang: str = "pt",
) -> Any:  # returns plotnine.ggplot, or None if no events survive the filters
    """Plot and analyze coldwave events from ``sus_climate_compute_coldwaves()``.

    Visualises the ``dict`` returned by ``sus_climate_compute_coldwaves()``.
    Provides four plot types: a Gantt-style timeline of events, a calendar
    heatmap, an intensity-vs-duration scatter plot, and an annual trend bar
    chart — mirroring ``climasus4r::sus_climate_plot_coldwaves()``'s four
    ``ggplot2``-backed static views.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        cw_result: Dict returned by ``sus_climate_compute_coldwaves()``,
            with keys ``"events"``, ``"daily"``, ``"summary"``.
        type: Type of plot to generate:
            * ``"timeline"``: Gantt-style timeline of coldwave events.
            * ``"calendar"``: Calendar heatmap showing days with coldwaves.
            * ``"intensity"``: Scatter plot of duration vs. coldest temperature.
            * ``"trend"``: Bar chart of number of events per year.
        station_code: Optional station code (or list of codes) to filter by.
        method: Optional method code (or list of codes, e.g. ``"EHF"``,
            ``"INMET"``) to filter by. For ``type="calendar"`` this instead
            selects which ``cw_<method>`` column of the daily table to draw
            (only the first method is used, and only ``station_code``/
            ``year`` filter the daily table itself — matches the R source;
            see IDEIAS.md).
        year: Optional year (or list of years) to filter by — useful for
            calendar plots.
        color_palette: Accepted for signature parity with the R function
            but has no effect — the R source computes it and never uses
            it either (see IDEIAS.md); colours are always the fixed
            blue/violet coldwave palette.
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``),
        or ``None`` if no coldwave events remain after filtering (a
        ``UserWarning`` is emitted in that case, mirroring the R source's
        ``cli::cli_warn()`` + ``return(NULL)``).

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``).
        ValueError: If *cw_result* is not a dict with ``"events"``,
            ``"daily"``, ``"summary"`` keys, *type* is invalid, or (for
            ``type="calendar"``) the selected ``cw_<method>`` column is not
            present in the daily table.

    Examples::

        import climasus4py as cs

        cw = cs.sus_climate_compute_coldwaves(df_indicators, method=["WHO", "EHF"])
        p = cs.sus_climate_plot_coldwaves(cw, type="timeline", lang="pt")
        p.draw()
        p2 = cs.sus_climate_plot_coldwaves(cw, type="trend")
    """
    if lang not in _I18N:
        lang = "en"
    strings = _I18N[lang]
    labels = _INTENSITY_LABELS[lang]

    # Argument validation runs before the plotnine dependency check, so
    # obviously-bad calls fail fast (and are testable) without plotnine
    # installed.
    if type not in ("timeline", "calendar", "intensity", "trend"):
        raise ValueError(
            f"type must be one of 'timeline', 'calendar', 'intensity', 'trend'; got {type!r}."
        )

    if (
        not isinstance(cw_result, dict)
        or not {"events", "daily", "summary"}.issubset(cw_result.keys())
    ):
        raise ValueError(strings["err_bad_result"])

    _require_plotnine()

    events_df = cw_result["events"].copy()
    daily_df = cw_result["daily"].copy()
    summary_df = cw_result["summary"].copy()

    station_codes = _as_list(station_code)
    methods = _as_list(method)
    years = _as_list(year)

    # Filters — matches the R source exactly: `method` never filters
    # `daily_df` (only `station_code`/`year` do); see IDEIAS.md.
    if station_codes is not None:
        events_df = events_df[events_df["station_code"].isin(station_codes)]
        daily_df = daily_df[daily_df["station_code"].isin(station_codes)]
        summary_df = summary_df[summary_df["station_code"].isin(station_codes)]
    if methods is not None:
        events_df = events_df[events_df["method"].isin(methods)]
        summary_df = summary_df[summary_df["method"].isin(methods)]
    if years is not None:
        events_df = events_df[pd.to_datetime(events_df["start_date"]).dt.year.isin(years)]
        daily_df = daily_df[pd.to_datetime(daily_df["date_day"]).dt.year.isin(years)]
        summary_df = summary_df[summary_df["year"].isin(years)]

    if len(events_df) == 0:
        warnings.warn(strings["warn_no_events"], UserWarning, stacklevel=2)
        return None

    if type == "timeline":
        return _plot_cw_timeline(events_df, strings, labels)
    if type == "calendar":
        return _plot_cw_calendar(daily_df, methods, strings)
    if type == "intensity":
        return _plot_cw_intensity(events_df, strings, labels)
    return _plot_cw_trend(summary_df, strings)
