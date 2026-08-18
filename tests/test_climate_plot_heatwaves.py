"""Tests for sus_climate_plot_heatwaves — plotnine visualisation."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_heatwaves import sus_climate_compute_heatwaves
from climasus4py.viz.climate_plot_heatwaves import sus_climate_plot_heatwaves

# ---------------------------------------------------------------------------
# Fixture — reuse the synthetic hourly-series pattern from
# tests/test_climate_heatwaves.py to get a realistic hw_result dict.
# ---------------------------------------------------------------------------


def _make_hourly_df(
    n_years: int = 3,
    heatwave_start: str = "2020-06-10",
    heatwave_len: int = 5,
    baseline_temp: float = 20.0,
    heatwave_temp: float = 40.0,
    seed: int = 0,
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2018-01-01")
    n_days = 365 * n_years
    dates = pd.date_range(start, periods=n_days, freq="D")
    hw_start_ts = pd.Timestamp(heatwave_start)

    tmax_daily = baseline_temp + 3 + rng.normal(0, 0.5, n_days)
    tmin_daily = baseline_temp - 3 + rng.normal(0, 0.5, n_days)

    hw_idx = np.asarray(
        (dates >= hw_start_ts) & (dates < hw_start_ts + pd.Timedelta(days=heatwave_len))
    )
    tmax_daily[hw_idx] = heatwave_temp + 3
    tmin_daily[hw_idx] = heatwave_temp - 3

    hours = pd.date_range(start, periods=n_days * 24, freq="h")
    day_idx = np.repeat(np.arange(n_days), 24)
    hour_of_day = np.tile(np.arange(24), n_days)
    frac = (1 - np.cos(2 * np.pi * (hour_of_day - 6) / 24)) / 2
    tmax_h = tmax_daily[day_idx]
    tmin_h = tmin_daily[day_idx]
    tair = tmin_h + frac * (tmax_h - tmin_h)

    return pd.DataFrame(
        {
            "station_code": "A701",
            "date": hours,
            "tair_dry_bulb_c": tair,
            "tair_max_c": tmax_h,
            "tair_min_c": tmin_h,
        }
    )


@pytest.fixture(scope="module")
def hw_result():
    df = _make_hourly_df()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return sus_climate_compute_heatwaves(
            df, method=["WHO", "INMET", "EHF"], verbose=False
        )


# ---------------------------------------------------------------------------
# Validation error paths — do not require plotnine
# ---------------------------------------------------------------------------


def test_invalid_hw_result_type_raises():
    with pytest.raises(ValueError, match="sus_climate_compute_heatwaves"):
        sus_climate_plot_heatwaves(pd.DataFrame({"a": [1]}))


def test_hw_result_missing_keys_raises():
    with pytest.raises(ValueError, match="sus_climate_compute_heatwaves"):
        sus_climate_plot_heatwaves({"events": pd.DataFrame()})


def test_invalid_type_raises(hw_result):
    with pytest.raises(ValueError, match="Invalid type"):
        sus_climate_plot_heatwaves(hw_result, type="bogus")


def test_invalid_lang_raises(hw_result):
    with pytest.raises(ValueError, match="Invalid lang"):
        sus_climate_plot_heatwaves(hw_result, lang="fr")


def test_empty_after_filter_warns_and_returns_none(hw_result):
    with pytest.warns(UserWarning, match="No heatwave events found"):
        result = sus_climate_plot_heatwaves(hw_result, station_code="NONEXISTENT")
    assert result is None


# ---------------------------------------------------------------------------
# Plot-type smoke tests — require plotnine
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("plot_type", ["timeline", "calendar", "intensity", "trend"])
def test_returns_ggplot_object(hw_result, plot_type):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # interactive=True default emits a warning
        result = sus_climate_plot_heatwaves(hw_result, type=plot_type)
    assert isinstance(result, ggplot)


@pytest.mark.parametrize("lang", ["en", "pt", "es"])
def test_multilingual_does_not_raise(hw_result, lang):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = sus_climate_plot_heatwaves(hw_result, type="timeline", lang=lang)
    assert isinstance(result, ggplot)


def test_interactive_true_warns(hw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.warns(UserWarning, match="interactive=True has no plotnine"):
        sus_climate_plot_heatwaves(hw_result, type="trend", interactive=True)


def test_unknown_palette_falls_back_to_npg(hw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    with pytest.warns(UserWarning, match="Using 'npg' instead"):
        result = sus_climate_plot_heatwaves(
            hw_result, type="trend", color_palette="aaas", interactive=False
        )
    assert isinstance(result, ggplot)


def test_method_filter_applies_to_events_and_summary(hw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = sus_climate_plot_heatwaves(
            hw_result, type="intensity", method="EHF", interactive=False
        )
    assert isinstance(result, ggplot)


def test_calendar_unknown_method_column_raises(hw_result):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from climasus4py.viz.climate_plot_heatwaves import _get_hw_palette, _plot_hw_calendar

    with pytest.raises(ValueError, match="not found in daily data"):
        _plot_hw_calendar(
            hw_result["daily"], "NOTAMETHOD", _get_hw_palette("npg"), "en"
        )


def test_missing_plotnine_raises_clear_error(hw_result, monkeypatch):
    monkeypatch.setattr(
        "climasus4py.viz.climate_plot_heatwaves._require_plotnine",
        lambda: (_ for _ in ()).throw(
            ImportError(
                "sus_climate_plot_heatwaves requires plotnine. "
                "Install with: pip install climasus4py[plot]"
            )
        ),
    )
    with pytest.raises(ImportError, match="pip install climasus4py"):
        sus_climate_plot_heatwaves(hw_result, type="trend")
