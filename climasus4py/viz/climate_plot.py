"""Climate gap-fill visualisation — ggplot-style via plotnine.

Mirrors R: sus_climate_plot_fill (sus_climate_plot_fill.R)

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_climate_plot_fill(
    ...     df_filled, df_original, target_var="tair_dry_bulb_c"
    ... )
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

# ---------------------------------------------------------------------------
# Internationalisation strings
# ---------------------------------------------------------------------------

_I18N = {
    "pt": {
        "title": "Preenchimento de Lacunas — {var}",
        "observed": "Observado",
        "imputed": "Imputado",
        "x": "Data",
        "y": "Valor",
        "station": "Estação: {station}",
    },
    "en": {
        "title": "Gap Filling — {var}",
        "observed": "Observed",
        "imputed": "Imputed",
        "x": "Date",
        "y": "Value",
        "station": "Station: {station}",
    },
    "es": {
        "title": "Relleno de Brechas — {var}",
        "observed": "Observado",
        "imputed": "Imputado",
        "x": "Fecha",
        "y": "Valor",
        "station": "Estación: {station}",
    },
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
            "sus_climate_plot_fill requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _detect_datetime_col(df: pd.DataFrame) -> str:
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            return col
    raise ValueError(
        "Could not auto-detect a date/time column. "
        "Ensure the DataFrame has a column whose name contains 'date' or 'time'."
    )


def _detect_station_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if "station" in col.lower() or "estacao" in col.lower():
            return col
    return None


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_plot_fill(
    df_filled: pd.DataFrame,
    df_original: pd.DataFrame | None = None,
    *,
    target_var: str,
    station: str | None = None,
    datetime_col: str | None = None,
    station_col: str | None = None,
    output_type: Literal["plot", "all"] = "plot",
    lang: str = "pt",
    color_palette: tuple[str, str] = ("#2196F3", "#F44336"),
    verbose: bool = False,
) -> Any:  # returns plotnine.ggplot or dict[str, Any]
    """Visualise before/after of ``sus_climate_fill_inmet`` output.

    Produces a ``plotnine.ggplot`` (Grammar of Graphics) time-series plot
    showing observed vs imputed data points, mirroring the static backend
    of ``climasus4r::sus_climate_plot_fill``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        df_filled: DataFrame output of ``sus_climate_fill_inmet`` — must
            contain an ``is_imputed_<target_var>`` flag column.
        df_original: Optional raw DataFrame before filling (output of
            ``sus_climate_inmet``). When provided, original observed values
            are overlaid for comparison.
        target_var: Name of the variable to visualise (e.g.
            ``"tair_dry_bulb_c"``).
        station: Station identifier to filter. If ``None``, uses the first
            station found in *df_filled*.
        datetime_col: Name of the date/datetime column (auto-detected if
            ``None``).
        station_col: Name of the station column (auto-detected if ``None``).
        output_type: ``"plot"`` returns a ``ggplot`` object.
            ``"all"`` returns a dict ``{"plot": ggplot, "data": df}``.
        lang: Label language — ``"pt"``, ``"en"``, ``"es"``.
        color_palette: Tuple of two hex colours:
            ``(observed_color, imputed_color)``.
        verbose: Print diagnostic messages.

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``).
        When ``output_type="all"``, returns a ``dict`` with keys
        ``"plot"`` and ``"data"``.

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``).
        ValueError: If *target_var* is missing, or if no date column is found.

    Example:
        >>> import climasus4py as cs
        >>> df_raw = cs.sus_climate_inmet(years=2023, uf="AM").df()
        >>> df_filled = cs.sus_climate_fill_inmet(df_raw, target_var="tair_dry_bulb_c")
        >>> p = cs.sus_climate_plot_fill(
        ...     df_filled, df_raw,
        ...     target_var="tair_dry_bulb_c",
        ...     lang="en",
        ... )
        >>> p.draw()
    """
    _require_plotnine()

    from plotnine import (  # noqa: I001
        aes,
        geom_line,
        geom_point,
        ggplot,
        labs,
        scale_color_manual,
        theme,
        theme_minimal,
    )

    strings = _I18N.get(lang, _I18N["en"])

    # Validate target_var present
    if target_var not in df_filled.columns:
        raise ValueError(
            f"Column '{target_var}' not found in df_filled. "
            f"Available: {list(df_filled.columns)}"
        )

    _dt_col = datetime_col or _detect_datetime_col(df_filled)
    _station_col = station_col or _detect_station_col(df_filled)

    # Filter to a single station
    plot_df = df_filled.copy()
    available_stations = (
        plot_df[_station_col].unique().tolist()
        if _station_col and _station_col in plot_df.columns
        else []
    )
    if available_stations:
        _station = station if station else available_stations[0]
        plot_df = plot_df[plot_df[_station_col] == _station].copy()
        if verbose:
            print(f"[sus_climate_plot_fill] station={_station}")
    else:
        # An empty frame has a station column but no stations in it, and
        # ``available_stations[0]`` then raised IndexError — a message that
        # says nothing about the data being empty. Nothing to pick, so plot
        # what there is (nothing) instead of crashing.
        _station = "all"

    # Ensure datetime
    plot_df[_dt_col] = pd.to_datetime(plot_df[_dt_col], errors="coerce")
    plot_df = plot_df.sort_values(_dt_col).reset_index(drop=True)

    # Build status column (observed / imputed)
    flag_col = f"is_imputed_{target_var}"
    if flag_col in plot_df.columns:
        plot_df["_status"] = plot_df[flag_col].map(
            {True: strings["imputed"], False: strings["observed"]}
        )
    else:
        plot_df["_status"] = strings["observed"]

    # Rename for plotnine aesthetics
    plot_df = plot_df.rename(columns={_dt_col: "_date", target_var: "_value"})

    # Drop rows where value is NaN (can't plot)
    plot_df = plot_df.dropna(subset=["_value"])

    if plot_df.empty:
        import warnings
        warnings.warn(
            "sus_climate_plot_fill: DataFrame is empty after filtering. "
            "Returning an empty plot.",
            UserWarning,
            stacklevel=2,
        )

    title = strings["title"].format(var=target_var)
    if _station and _station != "all":
        title = f"{title} — {strings['station'].format(station=_station)}"

    p = (
        ggplot(plot_df, aes(x="_date", y="_value", color="_status"))
        + geom_line(data=plot_df[plot_df["_status"] == strings["observed"]], size=0.7)
        + geom_point(
            data=plot_df[plot_df["_status"] == strings["imputed"]],
            size=2.0,
            shape="o",
        )
        + scale_color_manual(
            values={
                strings["observed"]: color_palette[0],
                strings["imputed"]: color_palette[1],
            }
        )
        + labs(
            title=title,
            x=strings["x"],
            y=strings["y"],
            color="",
        )
        + theme_minimal()
        + theme(figure_size=(12, 4))
    )

    if output_type == "all":
        return {"plot": p, "data": plot_df}

    return p
