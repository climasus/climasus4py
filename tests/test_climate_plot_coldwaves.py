"""Tests for sus_climate_plot_coldwaves — plotnine visualisation."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_coldwaves import sus_climate_compute_coldwaves
from climasus4py.viz.climate_plot_coldwaves import sus_climate_plot_coldwaves

# ---------------------------------------------------------------------------
# Fixture — reuses the coldwave-injection pattern from test_climate_coldwaves.py
# ---------------------------------------------------------------------------


def _make_daily_hourly_df(
    n_years: int = 3,
    coldwave_start: str | None = "2020-06-10",
    coldwave_len: int = 6,
    baseline_temp: float = 20.0,
    coldwave_temp: float = 5.0,
    n_stations: int = 2,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2018-01-01")
    n_days = 365 * n_years
    dates = pd.date_range(start, periods=n_days, freq="D")

    cw_start_ts = pd.Timestamp(coldwave_start) if coldwave_start else None
    frames = []
    for s in range(n_stations):
        station_code = f"A{700 + s}"
        tmax_daily = baseline_temp + 3 + rng.normal(0, 0.5, n_days)
        tmin_daily = baseline_temp - 3 + rng.normal(0, 0.5, n_days)

        if cw_start_ts is not None:
            cw_idx = (dates >= cw_start_ts) & (
                dates < cw_start_ts + pd.Timedelta(days=coldwave_len)
            )
            cw_idx = np.asarray(cw_idx)
            tmax_daily[cw_idx] = coldwave_temp + 3
            tmin_daily[cw_idx] = coldwave_temp - 3

        hours = pd.date_range(start, periods=n_days * 24, freq="h")
        day_idx = np.repeat(np.arange(n_days), 24)
        hour_of_day = np.tile(np.arange(24), n_days)
        frac = (1 - np.cos(2 * np.pi * (hour_of_day - 6) / 24)) / 2
        tmax_h = tmax_daily[day_idx]
        tmin_h = tmin_daily[day_idx]
        tair = tmin_h + frac * (tmax_h - tmin_h)

        frames.append(pd.DataFrame({
            "station_code": station_code,
            "date": hours,
            "tair_dry_bulb_c": tair,
            "tair_max_c": tmax_h,
            "tair_min_c": tmin_h,
        }))

    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def cw_result():
    df = _make_daily_hourly_df()
    return sus_climate_compute_coldwaves(
        df,
        method=["WHO", "INMET", "EHF"],
        baseline_start="2018-01-01",
        baseline_end="2020-12-31",
        verbose=False,
    )


# ---------------------------------------------------------------------------
# Returns a ggplot object — one test per `type`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("plot_type", ["timeline", "calendar", "intensity", "trend"])
def test_returns_ggplot_object(cw_result, plot_type):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_coldwaves(cw_result, type=plot_type)
    assert isinstance(result, ggplot)
    result.draw()  # smoke-test that it actually renders


@pytest.mark.parametrize("lang", ["pt", "en", "es"])
def test_multilingual_does_not_raise(cw_result, lang):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_coldwaves(cw_result, type="trend", lang=lang)
    assert isinstance(result, ggplot)


def test_station_code_filter(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_coldwaves(cw_result, type="timeline", station_code="A700")
    assert isinstance(result, ggplot)


def test_method_filter_intensity(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_coldwaves(cw_result, type="intensity", method="WHO")
    assert isinstance(result, ggplot)


def test_year_filter_calendar(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_coldwaves(cw_result, type="calendar", method="WHO", year=2020)
    assert isinstance(result, ggplot)


# ---------------------------------------------------------------------------
# No-events-after-filter path warns and returns None
# ---------------------------------------------------------------------------


def test_no_events_after_filter_warns_and_returns_none(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.warns(UserWarning, match="onda|coldwave|ola"):
        result = sus_climate_plot_coldwaves(cw_result, type="timeline", year=1900)
    assert result is None


# ---------------------------------------------------------------------------
# Validation errors that don't need plotnine to fail fast on
# ---------------------------------------------------------------------------


def test_invalid_cw_result_raises():
    with pytest.raises(ValueError, match="sus_climate_compute_coldwaves"):
        sus_climate_plot_coldwaves({"events": pd.DataFrame()}, type="timeline")


def test_invalid_type_raises(cw_result):
    with pytest.raises(ValueError, match="type"):
        sus_climate_plot_coldwaves(cw_result, type="bogus")


def test_calendar_missing_method_column_raises(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    # Drop the cw_who column from `daily` while `events` still has WHO rows,
    # so the events-filter doesn't short-circuit to the no-events branch
    # before the calendar column lookup is reached.
    broken = dict(cw_result)
    broken["daily"] = cw_result["daily"].drop(columns=["cw_who"])
    with pytest.raises(ValueError, match="cw_who"):
        sus_climate_plot_coldwaves(broken, type="calendar", method="WHO")


def test_calendar_all_nan_flag_column_renders(cw_result):
    """A requested-but-skipped method (all-NaN cw_<method>) must still render.

    sus_climate_compute_coldwaves() sets cw_utci to all-NaN when 'UTCI' is
    requested but utci_c isn't present in the input — this pushes an
    all-<NA> boolean column into the calendar plot's fill aesthetic.
    """
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    broken = dict(cw_result)
    daily = cw_result["daily"].copy()
    daily["cw_utci"] = float("nan")
    broken["daily"] = daily
    # UTCI wasn't among the computed methods, so `events`/`summary` have no
    # UTCI rows; inject one synthetic UTCI event so the outer method filter
    # doesn't empty `events_df` before the calendar branch is reached.
    fake_event = cw_result["events"].iloc[[0]].copy()
    fake_event["method"] = "UTCI"
    broken["events"] = pd.concat([cw_result["events"], fake_event], ignore_index=True)
    result = sus_climate_plot_coldwaves(broken, type="calendar", method="UTCI")
    assert isinstance(result, ggplot)
    result.draw()


def test_missing_plotnine_raises_clear_error(cw_result):
    with patch(
        "climasus4py.viz.climate_plot_coldwaves._require_plotnine",
        side_effect=ImportError(
            "sus_climate_plot_coldwaves requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ),
    ), pytest.raises(ImportError, match="pip install climasus4py"):
        sus_climate_plot_coldwaves(cw_result, type="timeline")


# ---------------------------------------------------------------------------
# R quirk: `color_palette` is accepted but has no effect on the output colours
# ---------------------------------------------------------------------------


def test_color_palette_has_no_effect(cw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    p1 = sus_climate_plot_coldwaves(cw_result, type="trend", color_palette="npg")
    p2 = sus_climate_plot_coldwaves(cw_result, type="trend", color_palette="totally-bogus")
    assert isinstance(p1, ggplot)
    assert isinstance(p2, ggplot)


# ---------------------------------------------------------------------------
# Helpers — no plotnine needed
# ---------------------------------------------------------------------------


def test_as_list_scalar_wraps():
    from climasus4py.viz.climate_plot_coldwaves import _as_list

    assert _as_list("A700") == ["A700"]
    assert _as_list(2020) == [2020]


def test_as_list_passthrough_and_none():
    from climasus4py.viz.climate_plot_coldwaves import _as_list

    assert _as_list(["A700", "A701"]) == ["A700", "A701"]
    assert _as_list(None) is None


def test_translate_intensity_maps_known_and_preserves_unknown():
    from climasus4py.viz.climate_plot_coldwaves import _INTENSITY_LABELS, _translate_intensity

    labels = _INTENSITY_LABELS["pt"]
    series = pd.Series(["Low Intensity (LICW)", "Severe (SCW)", None, "Extreme (ECW)"])
    translated = _translate_intensity(series, labels)
    assert translated.tolist() == [
        "Baixa Intensidade (LICW)",
        "Severa (SCW)",
        None,
        "Extrema (ECW)",
    ]
