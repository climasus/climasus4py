"""sus_climate_fill_inmet follows climasus4r (06/11/2026).

The port was checked against the R package directly: on station A705 (SP
2023, every variable masked in 1-720 h blocks) the 53 features match R's
create_time_features / create_lag_features / create_rolling_features column
by column (rtol 1e-5). These tests pin the rules that make that true, plus
the R behaviours the fill now copies (quality filter, limits, transforms)
and the two R defects it deliberately does not copy (night radiation on the
UTC clock, and counting the night as missing in the quality filter).
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_fill import (
    _clip_predictions,
    _engineer_features,
    _is_night,
    _smooth_extreme_predictions,
    _time_features,
    _transform_rule,
    _xgboost_available,
    sus_climate_fill_inmet,
)

needs_xgb = pytest.mark.skipif(not _xgboost_available(), reason="xgboost not installed")

R_FEATURES_TAIR = [
    "hour", "day", "month", "dayofyear", "quarter", "weekofyear", "weekday",
    "is_weekend", "season", "hour_sin", "hour_cos", "month_sin", "month_cos",
    "dayofyear_sin", "dayofyear_cos", "weekday_sin", "weekday_cos",
    "quarter_sin", "quarter_cos", "weekofyear_sin", "weekofyear_cos",
    "season_0", "season_1", "season_2", "season_3", "is_morning",
    "is_afternoon", "is_evening", "is_night", "hour_decimal",
    "tair_dry_bulb_c_same_hour_yesterday", "tair_dry_bulb_c_same_hour_last_week",
    "tair_dry_bulb_c_lag_1", "tair_dry_bulb_c_lag_2", "tair_dry_bulb_c_lag_3",
    "tair_dry_bulb_c_lag_12", "tair_dry_bulb_c_lag_24", "tair_dry_bulb_c_lag_168",
    "tair_dry_bulb_c_lead_1", "tair_dry_bulb_c_lead_2", "tair_dry_bulb_c_lead_3",
    "tair_dry_bulb_c_lead_12", "tair_dry_bulb_c_lead_24", "tair_dry_bulb_c_lead_168",
    "tair_dry_bulb_c_diff_current_lag_1",
    "tair_dry_bulb_c_rolling_mean_3", "tair_dry_bulb_c_rolling_sd_3",
    "tair_dry_bulb_c_rolling_mean_6", "tair_dry_bulb_c_rolling_sd_6",
    "tair_dry_bulb_c_rolling_mean_12", "tair_dry_bulb_c_rolling_sd_12",
    "tair_dry_bulb_c_rolling_mean_24", "tair_dry_bulb_c_rolling_sd_24",
]


def _hourly(n_hours: int = 24 * 15, station: str = "A700", seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2023-03-01", periods=n_hours, freq="h")
    hours = dates.hour.to_numpy()
    return pd.DataFrame({
        "station_code": station,
        "date": dates,
        "longitude": -46.6,
        "tair_dry_bulb_c": (24 + 5 * np.sin(2 * np.pi * (hours - 9) / 24)
                            + rng.normal(0, 0.3, n_hours)).astype("float32"),
        "rainfall_mm": np.where(rng.random(n_hours) < 0.1, 2.0, 0.0).astype("float32"),
    })


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------


def test_features_are_rs_53_in_rs_order():
    df = _hourly()
    f = _engineer_features(df["date"], df["tair_dry_bulb_c"].astype("float64"),
                           "tair_dry_bulb_c")
    assert list(f.columns) == R_FEATURES_TAIR


def test_no_other_meteorological_variable_is_a_feature():
    """When a station goes down every variable is missing in the same hour."""
    df = _hourly()
    f = _engineer_features(df["date"], df["tair_dry_bulb_c"].astype("float64"),
                           "tair_dry_bulb_c")
    assert not any(c.startswith("rainfall") for c in f.columns)


def test_time_features_of_new_year_2023():
    # 2023-01-01 00:00 UTC is a Sunday in ISO week 52 of 2022, summer.
    f = _time_features(pd.Series(pd.to_datetime(["2023-01-01 00:00"])))
    row = f.iloc[0]
    assert (row["hour"], row["day"], row["month"], row["dayofyear"]) == (0, 1, 1, 1)
    assert (row["quarter"], row["weekofyear"], row["weekday"]) == (1, 52, 6)
    assert (row["is_weekend"], row["season"], row["season_0"], row["is_night"]) == (1, 0, 1, 1)


def test_time_features_are_plain_ints_not_int32():
    """The old fill dropped hour/day/month because pandas gave int32."""
    f = _time_features(pd.Series(pd.date_range("2023-01-01", periods=3, freq="h")))
    assert f["hour"].dtype == np.int64


def test_lag_feature_gaps_follow_na_approx_rule_2():
    y = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0])
    f = _engineer_features(pd.Series(pd.date_range("2023-01-01", periods=5, freq="h")), y, "v")
    # lag_1 = [NA, 1, 2, NA, 4] -> interpolated, ends extended: [1, 1, 2, 3, 4]
    np.testing.assert_allclose(f["v_lag_1"], [1, 1, 2, 3, 4])


def test_rolling_mean_skips_na_and_extends_the_first_positions():
    y = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0, 6.0, 7.0])
    f = _engineer_features(pd.Series(pd.date_range("2023-01-01", periods=7, freq="h")), y, "v")
    # zoo::rollmean(na.rm = TRUE): NA NA 1.5 3 4.5 5 6 -> rule 2 fills the head
    np.testing.assert_allclose(f["v_rolling_mean_3"], [1.5, 1.5, 1.5, 3, 4.5, 5, 6])


# ---------------------------------------------------------------------------
# Per-variable rules
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("var", "rule"), [
    ("rainfall_mm", "log1p"), ("ws_gust_m_s", "sqrt"), ("ws_2_m_s", "sqrt"),
    ("sr_kj_m2", "sqrt"), ("tair_dry_bulb_c", "none"), ("rh_mean_porc", "none"),
    ("patm_mb", "none"),
])
def test_transform_rule_matches_r(var, rule):
    assert _transform_rule(var) == rule


@pytest.mark.parametrize(("var", "raw", "expected"), [
    ("rh_mean_porc", [-5, 50, 105], [0, 50, 100]),
    ("tair_dry_bulb_c", [-100, 20, 70], [-90, 20, 60]),
    ("dew_tmean_c", [-100, 70], [-90, 60]),
    ("patm_mb", [600, 900, 1200], [700, 900, 1100]),
    ("rainfall_mm", [-1, 3], [0, 3]),
    ("ws_2_m_s", [-1, 3], [0, 3]),
])
def test_physical_limits_match_r(var, raw, expected):
    np.testing.assert_allclose(_clip_predictions(np.array(raw, float), var), expected)


def test_extremes_beyond_5_iqr_are_pulled_30pct_to_the_mean():
    train = np.arange(11, dtype=float)          # q1 2.5, q3 7.5, bounds -22.5 / 32.5
    out = _smooth_extreme_predictions(np.array([5.0, 100.0]), train)
    np.testing.assert_allclose(out, [5.0, 0.3 * 5 + 0.7 * 100])


# ---------------------------------------------------------------------------
# Quality filter (R semantics)
# ---------------------------------------------------------------------------


def _two_stations(rain_missing_b: float) -> pd.DataFrame:
    a, b = _hourly(station="A700", seed=1), _hourly(station="A701", seed=2)
    n = int(len(b) * rain_missing_b)
    b.loc[: n - 1, "rainfall_mm"] = np.nan
    return pd.concat([a, b], ignore_index=True)


def test_one_bad_variable_removes_the_whole_station():
    df = _two_stations(0.6)
    out = sus_climate_fill_inmet(df, target_var=["tair_dry_bulb_c", "rainfall_mm"],
                                 backend="linear", verbose=False)
    assert set(out["station_code"]) == {"A700"}


def test_station_kept_when_bad_variable_not_requested():
    df = _two_stations(0.6)
    out = sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c",
                                 backend="linear", verbose=False)
    assert set(out["station_code"]) == {"A700", "A701"}


def test_no_station_passing_skips_the_variable_and_keeps_everyone():
    a, b = _hourly(station="A700", seed=1), _hourly(station="A701", seed=2)
    for d in (a, b):
        d.loc[: int(len(d) * 0.6), "rainfall_mm"] = np.nan
    df = pd.concat([a, b], ignore_index=True)
    out = sus_climate_fill_inmet(df, target_var=["tair_dry_bulb_c", "rainfall_mm"],
                                 backend="linear", verbose=False)
    assert set(out["station_code"]) == {"A700", "A701"}
    assert out["rainfall_mm"].isna().sum() == df["rainfall_mm"].isna().sum()
    assert not out["is_imputed_rainfall_mm"].any()


# ---------------------------------------------------------------------------
# Solar radiation (R defects not copied)
# ---------------------------------------------------------------------------


def test_night_is_judged_by_solar_time_not_utc():
    utc = pd.Series(pd.to_datetime(["2023-03-01 03:00", "2023-03-01 19:00",
                                    "2023-03-01 22:00"]))
    # Sao Paulo (lon -46.6, ~UTC-3.1): 03 UTC is midnight, 19 UTC is ~16h, 22 UTC ~19h
    np.testing.assert_array_equal(_is_night(utc, pd.Series([-46.6] * 3)),
                                  [True, False, True])


def test_night_radiation_is_zero_and_does_not_fail_the_quality_filter():
    df = _hourly()
    solar = (df["date"].dt.hour - 46.6 / 15) % 24
    df["sr_kj_m2"] = np.where((solar >= 6) & (solar < 18), 1500.0, np.nan).astype("float32")
    day_gap = df.index[(df["date"].dt.hour == 19)][:3]   # ~16h local: daylight
    df.loc[day_gap, "sr_kj_m2"] = np.nan
    assert df["sr_kj_m2"].isna().mean() > 0.4           # R would drop this station

    out = sus_climate_fill_inmet(df, target_var="sr_kj_m2", backend="linear",
                                 verbose=False)
    assert len(out) == len(df)
    night = ~((solar >= 6) & (solar < 18))
    assert (out.loc[night, "sr_kj_m2"] == 0).all()
    assert not out.loc[night, "is_imputed_sr_kj_m2"].any()
    assert (out.loc[day_gap, "sr_kj_m2"] > 0).all()
    assert out.loc[day_gap, "is_imputed_sr_kj_m2"].all()


# ---------------------------------------------------------------------------
# XGBoost path
# ---------------------------------------------------------------------------


@needs_xgb
def test_fewer_than_30_observations_leaves_the_gaps():
    df = _hourly(n_hours=35)
    df.loc[5:10, "tair_dry_bulb_c"] = np.nan              # 29 observed
    out = sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", backend="xgboost",
                                 parallel=False, verbose=False)
    assert out["tair_dry_bulb_c"].isna().sum() == 6
    assert not out["is_imputed_tair_dry_bulb_c"].any()


@needs_xgb
def test_long_gap_follows_the_daily_cycle_not_a_constant():
    """The old fill returned one value for a whole outage (A716: 138 days at 21.7)."""
    df = _hourly(n_hours=24 * 30)
    df.loc[24 * 10: 24 * 13 - 1, "tair_dry_bulb_c"] = np.nan   # 3 days
    out = sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", backend="xgboost",
                                 parallel=False, verbose=False)
    filled = out.loc[24 * 10: 24 * 13 - 1, "tair_dry_bulb_c"]
    assert filled.notna().all()
    assert filled.round(2).nunique() > 10


@needs_xgb
def test_rain_predictions_are_never_negative():
    df = _hourly(n_hours=24 * 20)
    df.loc[100:160, "rainfall_mm"] = np.nan
    out = sus_climate_fill_inmet(df, target_var="rainfall_mm", backend="xgboost",
                                 parallel=False, verbose=False)
    assert (out["rainfall_mm"] >= 0).all()


@needs_xgb
def test_output_keeps_input_order_and_float32():
    df = _two_stations(0.0).sample(frac=1, random_state=3).reset_index(drop=True)
    df.loc[df.index[:40], "tair_dry_bulb_c"] = np.nan
    out = sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", backend="xgboost",
                                 parallel=False, verbose=False)
    pd.testing.assert_series_equal(out["date"], df["date"])
    pd.testing.assert_series_equal(out["station_code"], df["station_code"])
    assert out["tair_dry_bulb_c"].dtype == np.float32


def test_humidity_is_rounded_like_r():
    df = _hourly()
    df["rh_mean_porc"] = np.linspace(40.2, 90.7, len(df)).astype("float32")
    df.loc[10:12, "rh_mean_porc"] = np.nan
    out = sus_climate_fill_inmet(df, target_var="rh_mean_porc", backend="linear",
                                 verbose=False)
    assert (out["rh_mean_porc"] == out["rh_mean_porc"].round()).all()


@needs_xgb
def test_cache_dir_is_ignored_with_a_warning(tmp_path):
    df = _hourly()
    df.loc[50:60, "tair_dry_bulb_c"] = np.nan
    with pytest.warns(DeprecationWarning, match="cache_dir is ignored"):
        sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", backend="xgboost",
                               parallel=False, verbose=False, cache_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


@needs_xgb
def test_evaluation_returns_r_style_comparison():
    df = _hourly()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", backend="xgboost",
                                     run_evaluation=True, parallel=False, verbose=False)
    data, metrics = res["tair_dry_bulb_c"]["data"], res["tair_dry_bulb_c"]["metrics"]
    assert list(data.columns) == ["date", "station", "true_value", "pred_value"]
    assert {"mae", "rmse", "r2", "n"} <= set(metrics.columns)
    assert metrics["rmse"].iloc[0] < 2.0
