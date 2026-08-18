"""Tests for sus_climate_compute_heatwaves and its convenience helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_heatwaves import (
    hw_active_days,
    hw_count_by_year,
    hw_get_events,
    sus_climate_compute_heatwaves,
)


def _make_daily_hourly_df(
    n_years: int = 3,
    heatwave_start: str | None = "2020-06-10",
    heatwave_len: int = 5,
    baseline_temp: float = 20.0,
    heatwave_temp: float = 40.0,
    n_stations: int = 1,
    seed: int = 0,
) -> pd.DataFrame:
    """Hourly synthetic INMET-like series with an optional injected heatwave.

    Baseline days oscillate mildly around *baseline_temp*; the injected
    episode holds *heatwave_temp* for *heatwave_len* consecutive days so
    detection methods have an unambiguous signal to find.
    """
    rng = np.random.default_rng(seed)
    start = pd.Timestamp("2018-01-01")
    n_days = 365 * n_years
    dates = pd.date_range(start, periods=n_days, freq="D")

    hw_start_ts = pd.Timestamp(heatwave_start) if heatwave_start else None
    frames = []
    for s in range(n_stations):
        station_code = f"A{700 + s}"
        tmax_daily = baseline_temp + 3 + rng.normal(0, 0.5, n_days)
        tmin_daily = baseline_temp - 3 + rng.normal(0, 0.5, n_days)

        if hw_start_ts is not None:
            hw_idx = (dates >= hw_start_ts) & (
                dates < hw_start_ts + pd.Timedelta(days=heatwave_len)
            )
            hw_idx = np.asarray(hw_idx)
            tmax_daily[hw_idx] = heatwave_temp + 3
            tmin_daily[hw_idx] = heatwave_temp - 3

        # Expand to 24 hourly rows/day: dry-bulb oscillates between tmin/tmax.
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


class TestValidation:
    def test_missing_required_columns_raises(self):
        df = pd.DataFrame({"tair_dry_bulb_c": [20.0, 21.0]})
        with pytest.raises(ValueError, match="date|station_code"):
            sus_climate_compute_heatwaves(df, verbose=False)

    def test_no_temperature_column_raises(self):
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=10, freq="D"),
            "station_code": ["A701"] * 10,
        })
        with pytest.raises(ValueError, match="temperature|temperatura"):
            sus_climate_compute_heatwaves(df, verbose=False)

    def test_invalid_method_raises(self):
        df = _make_daily_hourly_df(n_years=1, heatwave_start=None)
        with pytest.raises(ValueError, match="method|metodo|Metodo"):
            sus_climate_compute_heatwaves(df, method=["BOGUS"], verbose=False)

    def test_missing_optional_column_warns_and_skips(self):
        df = _make_daily_hourly_df(n_years=1, heatwave_start=None)
        with pytest.warns(UserWarning, match="UTCI|utci"):
            result = sus_climate_compute_heatwaves(df, method=["UTCI"], verbose=False)
        assert result["daily"]["hw_utci"].isna().all()


class TestDetection:
    def test_injected_heatwave_is_flagged_by_who_method(self):
        df = _make_daily_hourly_df(
            n_years=3, heatwave_start="2020-06-10", heatwave_len=6
        )
        result = sus_climate_compute_heatwaves(
            df, method=["WHO"], baseline_start="2018-01-01", baseline_end="2020-12-31",
            verbose=False,
        )
        daily = result["daily"]
        flagged_dates = set(daily.loc[daily["hw_who"] == True, "date_day"])  # noqa: E712
        expected = {
            d.normalize()
            for d in pd.date_range("2020-06-10", periods=6, freq="D")
        }
        assert expected <= flagged_dates

    def test_no_heatwave_yields_no_events(self):
        df = _make_daily_hourly_df(n_years=2, heatwave_start=None)
        result = sus_climate_compute_heatwaves(df, method=["WHO"], verbose=False)
        assert result["events"].empty
        assert not result["daily"]["hw_who"].any()

    def test_short_episode_below_min_duration_not_flagged(self):
        # A 2-day spike with min_duration=5 (WMO default) should not qualify.
        df = _make_daily_hourly_df(
            n_years=2, heatwave_start="2019-06-10", heatwave_len=2
        )
        result = sus_climate_compute_heatwaves(df, method=["WMO"], verbose=False)
        assert result["events"].empty

    def test_ehf_method_flags_injected_episode(self):
        df = _make_daily_hourly_df(
            n_years=3, heatwave_start="2020-06-10", heatwave_len=6
        )
        result = sus_climate_compute_heatwaves(df, method=["EHF"], verbose=False)
        events = result["events"]
        assert not events.empty
        assert (events["method"] == "EHF").all()
        assert events["intensity_class"].notna().any()

    def test_events_table_has_expected_columns(self):
        df = _make_daily_hourly_df(
            n_years=2, heatwave_start="2019-06-10", heatwave_len=5
        )
        result = sus_climate_compute_heatwaves(df, method=["WHO"], verbose=False)
        expected_cols = {
            "station_code", "method", "start_date", "end_date", "duration_days",
            "temp_mean", "temp_peak", "anomaly_mean", "anomaly_cumulative",
            "severity_index", "event_id", "intensity_class",
        }
        assert expected_cols <= set(result["events"].columns)

    def test_sus_meta_in_result(self):
        df = _make_daily_hourly_df(n_years=1, heatwave_start=None)
        result = sus_climate_compute_heatwaves(df, method=["WHO"], verbose=False)
        for key in ("events", "daily", "summary"):
            meta = result[key].attrs["sus_meta"]
            assert meta["stage"] == "climate"
            assert meta["type"] == "heatwaves"


class TestConvenienceHelpers:
    def test_hw_get_events_filters_by_method(self):
        df = _make_daily_hourly_df(
            n_years=3, heatwave_start="2020-06-10", heatwave_len=6
        )
        result = sus_climate_compute_heatwaves(df, method=["WHO", "EHF"], verbose=False)
        who_only = hw_get_events(result, method_filter=["WHO"])
        assert (who_only["method"] == "WHO").all()

    def test_hw_count_by_year_returns_expected_columns(self):
        df = _make_daily_hourly_df(
            n_years=3, heatwave_start="2020-06-10", heatwave_len=6
        )
        result = sus_climate_compute_heatwaves(df, method=["WHO"], verbose=False)
        counts = hw_count_by_year(result)
        assert "n_events" in counts.columns

    def test_hw_active_days_only_returns_flagged_rows(self):
        df = _make_daily_hourly_df(
            n_years=3, heatwave_start="2020-06-10", heatwave_len=6
        )
        result = sus_climate_compute_heatwaves(df, method=["WHO"], verbose=False)
        active = hw_active_days(result)
        assert len(active) <= len(result["daily"])
        if not active.empty:
            assert active["station_code"].notna().all()
