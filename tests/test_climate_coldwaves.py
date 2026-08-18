"""Tests for sus_climate_compute_coldwaves and its convenience helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_coldwaves import sus_climate_compute_coldwaves


def _make_daily_hourly_df(
    n_years: int = 3,
    coldwave_start: str | None = "2020-06-10",
    coldwave_len: int = 5,
    baseline_temp: float = 20.0,
    coldwave_temp: float = 5.0,
    n_stations: int = 1,
    seed: int = 0,
) -> pd.DataFrame:
    """Hourly synthetic INMET-like series with an optional injected coldwave."""
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


class TestValidation:
    def test_missing_required_columns_raises(self):
        df = pd.DataFrame({"tair_dry_bulb_c": [20.0, 21.0]})
        with pytest.raises(ValueError, match="date|station_code"):
            sus_climate_compute_coldwaves(df, verbose=False)

    def test_no_temperature_column_raises(self):
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=10, freq="D"),
            "station_code": ["A701"] * 10,
        })
        with pytest.raises(ValueError, match="temperature|temperatura"):
            sus_climate_compute_coldwaves(df, verbose=False)

    def test_invalid_method_raises(self):
        df = _make_daily_hourly_df(n_years=1, coldwave_start=None)
        with pytest.raises(ValueError, match="method|[Mm]etodo|[Mm]étodo"):
            sus_climate_compute_coldwaves(df, method=["BOGUS"], verbose=False)

    def test_missing_optional_column_warns_and_skips(self):
        df = _make_daily_hourly_df(n_years=1, coldwave_start=None)
        with pytest.warns(UserWarning, match="UTCI|utci"):
            result = sus_climate_compute_coldwaves(df, method=["UTCI"], verbose=False)
        assert result["daily"]["cw_utci"].isna().all()

    def test_default_percentile_is_10_not_90(self):
        # sus_climate_compute_coldwaves defaults percentile=10 (vs heatwaves'
        # 90) — a P10 threshold on tmin, not P90 — verified against the R
        # source default `percentile = 10`.
        import inspect

        sig = inspect.signature(sus_climate_compute_coldwaves)
        assert sig.parameters["percentile"].default == 10


class TestDetection:
    def test_injected_coldwave_is_flagged_by_who_method(self):
        df = _make_daily_hourly_df(
            n_years=3, coldwave_start="2020-06-10", coldwave_len=6
        )
        result = sus_climate_compute_coldwaves(
            df, method=["WHO"], baseline_start="2018-01-01", baseline_end="2020-12-31",
            verbose=False,
        )
        daily = result["daily"]
        flagged_dates = set(daily.loc[daily["cw_who"] == True, "date_day"])  # noqa: E712
        expected = {
            d.normalize()
            for d in pd.date_range("2020-06-10", periods=6, freq="D")
        }
        assert expected <= flagged_dates

    def test_no_coldwave_yields_no_events(self):
        df = _make_daily_hourly_df(n_years=2, coldwave_start=None)
        result = sus_climate_compute_coldwaves(df, method=["WHO"], verbose=False)
        assert result["events"].empty
        assert not result["daily"]["cw_who"].any()

    def test_short_episode_below_min_duration_not_flagged(self):
        df = _make_daily_hourly_df(
            n_years=2, coldwave_start="2019-06-10", coldwave_len=2
        )
        result = sus_climate_compute_coldwaves(df, method=["WMO"], verbose=False)
        assert result["events"].empty

    def test_ehf_method_flags_injected_episode(self):
        df = _make_daily_hourly_df(
            n_years=3, coldwave_start="2020-06-10", coldwave_len=6
        )
        result = sus_climate_compute_coldwaves(df, method=["EHF"], verbose=False)
        events = result["events"]
        assert not events.empty
        assert (events["method"] == "EHF").all()

    def test_events_table_has_expected_columns(self):
        df = _make_daily_hourly_df(
            n_years=2, coldwave_start="2019-06-10", coldwave_len=5
        )
        result = sus_climate_compute_coldwaves(df, method=["WHO"], verbose=False)
        expected_cols = {
            "station_code", "method", "start_date", "end_date", "duration_days",
            "temp_mean", "temp_peak", "anomaly_mean", "anomaly_cumulative",
            "severity_index", "event_id", "intensity_class",
        }
        assert expected_cols <= set(result["events"].columns)

    def test_sus_meta_in_result(self):
        df = _make_daily_hourly_df(n_years=1, coldwave_start=None)
        result = sus_climate_compute_coldwaves(df, method=["WHO"], verbose=False)
        for key in ("events", "daily", "summary"):
            meta = result[key].attrs["sus_meta"]
            assert meta["stage"] == "climate"
            assert meta["type"] == "coldwaves"

    def test_inmet_method_uses_minus_5_threshold(self):
        # sus_climate_compute_coldwaves INMET method: tmin < tmin_hist - 5
        # (mirror-sign of heatwaves' tmax > tmax_hist + 5).
        df = _make_daily_hourly_df(
            n_years=3, coldwave_start="2020-06-10", coldwave_len=6,
            baseline_temp=20.0, coldwave_temp=5.0,
        )
        result = sus_climate_compute_coldwaves(df, method=["INMET"], verbose=False)
        assert not result["events"].empty
