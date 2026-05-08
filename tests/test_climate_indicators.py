"""Tests for sus_climate_compute_indicators — bioclimatic indicators (B.2)."""

from __future__ import annotations

import math

import duckdb
import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate_indicators import (
    sus_climate_compute_indicators,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_full_inmet_rel(n: int = 20) -> duckdb.DuckDBPyRelation:
    """Synthetic INMET relation with all 17 canonical variables."""
    import numpy as np

    rng = np.random.default_rng(0)
    dates = pd.date_range("2023-06-01", periods=n, freq="D")
    df = pd.DataFrame(
        {
            "station_code": ["A701"] * n,
            "date": dates,
            # Temperature
            "tair_dry_bulb_c": rng.uniform(25.0, 32.0, n),
            "tair_max_c": rng.uniform(33.0, 38.0, n),
            "tair_min_c": rng.uniform(18.0, 24.0, n),
            # Humidity
            "rh_mean_porc": rng.uniform(55.0, 85.0, n),
            # Dew point
            "dew_tmean_c": rng.uniform(16.0, 22.0, n),
            # Wind
            "ws_2_m_s": rng.uniform(0.5, 4.0, n),
            # Solar
            "sr_kj_m2": rng.uniform(500.0, 2200.0, n),
        }
    )
    return get_connection().from_df(df)


@pytest.fixture
def full_rel():
    return _make_full_inmet_rel()


@pytest.fixture
def full_df():
    """Same data as a DataFrame."""
    return _make_full_inmet_rel().df()


# ---------------------------------------------------------------------------
# Core: returns DuckDBPyRelation (lazy)
# ---------------------------------------------------------------------------


def test_returns_duckdb_relation(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


def test_accepts_dataframe_input(full_df):
    result = sus_climate_compute_indicators(full_df, verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


# ---------------------------------------------------------------------------
# Individual indicators
# ---------------------------------------------------------------------------


def test_heat_index_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["heat_index"], verbose=False
    )
    df = result.df()
    assert "hi_c" in df.columns


def test_thi_column_present(full_rel):
    result = sus_climate_compute_indicators(full_rel, indicators=["thi"], verbose=False)
    df = result.df()
    assert "thi_c" in df.columns


def test_apparent_temperature_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["apparent_temperature"], verbose=False
    )
    df = result.df()
    assert "at_c" in df.columns


def test_vapor_pressure_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["vapor_pressure"], verbose=False
    )
    df = result.df()
    assert "vapor_pressure_kpa" in df.columns
    # All values must be positive (vapor pressure is always > 0)
    assert (result.df()["vapor_pressure_kpa"] > 0).all()


def test_dew_point_depression_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["dew_point_depression"], verbose=False
    )
    df = result.df()
    assert "dpd_c" in df.columns


def test_diurnal_range_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["diurnal_range"], verbose=False
    )
    df = result.df()
    assert "diurnal_range_c" in df.columns
    # DTR must be ≥ 0 (max − min of same series)
    assert (df["diurnal_range_c"] >= 0).all()


def test_consecutive_hot_days_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["consecutive_hot_days"], verbose=False
    )
    df = result.df()
    assert "consecutive_hot_days" in df.columns
    # CHD is a true run length (not a 7-day rolling count) — values are
    # non-negative integers, no upper bound from the window.
    assert (df["consecutive_hot_days"] >= 0).all()
    assert df["consecutive_hot_days"].dtype.kind in ("i", "u")


def test_heat_wave_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["heat_wave"], verbose=False
    )
    df = result.df()
    assert "heat_wave" in df.columns
    # Binary: 0 or 1
    assert df["heat_wave"].isin([0, 1]).all()


# ---------------------------------------------------------------------------
# BUG-01 regression: CHD must report TRUE run lengths (not 7-day rolling sum)
# ---------------------------------------------------------------------------


def test_chd_returns_true_run_length_alternating_pattern():
    """Sequence Q-F-Q-Q-Q-Q-Q (Q = Tmax > 32, F = Tmax <= 32):

    CHD on the last hot day must be 5 (length of the trailing run),
    not 6 (count of hot days within the 7-day window).
    """
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    days = pd.date_range("2023-01-01", periods=7, freq="D")
    # Q F Q Q Q Q Q
    tmax = [33.0, 25.0, 33.0, 33.0, 33.0, 33.0, 33.0]
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 7,
        "date": days,
        "tair_max_c": tmax,
        "tair_dry_bulb_c": tmax,
        "rh_mean_porc": [50.0] * 7,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["consecutive_hot_days"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    # day 0 (Q) → run length 1
    # day 1 (F) → 0
    # days 2..6 (Q×5) → 1, 2, 3, 4, 5
    expected = [1, 0, 1, 2, 3, 4, 5]
    assert out["consecutive_hot_days"].tolist() == expected


def test_chd_zero_when_not_hot():
    """Tmax <= 32 → CHD = 0."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 3,
        "date": pd.date_range("2023-01-01", periods=3, freq="D"),
        "tair_max_c": [25.0, 30.0, 32.0],  # all <= 32
        "tair_dry_bulb_c": [25.0, 30.0, 32.0],
        "rh_mean_porc": [50.0] * 3,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["consecutive_hot_days"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    assert out["consecutive_hot_days"].tolist() == [0, 0, 0]


# ---------------------------------------------------------------------------
# BUG-02 regression: heat_wave must flag ALL days of a run (not skip the
# first two days of the episode)
# ---------------------------------------------------------------------------


def test_heat_wave_flags_all_three_days_of_episode():
    """A 3-day run of Tmax > 35 must produce heat_wave = 1 on ALL THREE days."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 5,
        "date": pd.date_range("2023-01-01", periods=5, freq="D"),
        # cool, hot, hot, hot, cool — the 3 hot days are an episode
        "tair_max_c": [30.0, 36.0, 36.0, 36.0, 30.0],
        "tair_dry_bulb_c": [30.0, 36.0, 36.0, 36.0, 30.0],
        "rh_mean_porc": [50.0] * 5,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_wave"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    # Days 1, 2, 3 are all part of the 3-day run → all flagged
    assert out["heat_wave"].tolist() == [0, 1, 1, 1, 0]


def test_heat_wave_does_not_flag_two_day_run():
    """Run of only 2 hot days → no heat_wave."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 4,
        "date": pd.date_range("2023-01-01", periods=4, freq="D"),
        "tair_max_c": [30.0, 36.0, 36.0, 30.0],
        "tair_dry_bulb_c": [30.0, 36.0, 36.0, 30.0],
        "rh_mean_porc": [50.0] * 4,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_wave"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    assert out["heat_wave"].tolist() == [0, 0, 0, 0]


# ---------------------------------------------------------------------------
# BUG-04 regression: heat_index returns NULL outside the Rothfusz domain
# ---------------------------------------------------------------------------


def test_heat_index_null_below_temperature_threshold():
    """T < 27°C → hi_c = NULL (outside Rothfusz domain)."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 2,
        "date": pd.date_range("2023-01-01", periods=2, freq="D"),
        "tair_dry_bulb_c": [20.0, 35.0],   # 20 = below domain, 35 = inside
        "rh_mean_porc": [60.0, 60.0],
        "tair_max_c": [25.0, 40.0],
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_index"], verbose=False
    ).df()
    assert pd.isna(out["hi_c"].iloc[0])
    assert pd.notna(out["hi_c"].iloc[1])


def test_heat_index_null_below_humidity_threshold():
    """RH < 40% → hi_c = NULL."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 2,
        "date": pd.date_range("2023-01-01", periods=2, freq="D"),
        "tair_dry_bulb_c": [35.0, 35.0],
        "rh_mean_porc": [25.0, 60.0],   # 25 = below domain, 60 = inside
        "tair_max_c": [40.0, 40.0],
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_index"], verbose=False
    ).df()
    assert pd.isna(out["hi_c"].iloc[0])
    assert pd.notna(out["hi_c"].iloc[1])


# ---------------------------------------------------------------------------
# BUG-03 regression: WBGT exists, returns wbgt_c column
# ---------------------------------------------------------------------------


def test_wbgt_indicator_available_and_returns_column(full_rel):
    """WBGT documented in module — must exist and produce wbgt_c."""
    out = sus_climate_compute_indicators(
        full_rel, indicators=["wbgt"], verbose=False
    ).df()
    assert "wbgt_c" in out.columns
    # WBGT (simplified outdoor) should fall in a plausible thermal range
    valid = out["wbgt_c"].dropna()
    assert (valid > -10).all() and (valid < 50).all()


# ---------------------------------------------------------------------------
# "all" indicators (default)
# ---------------------------------------------------------------------------


def test_all_indicators_default(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    df = result.df()
    expected_output_cols = [
        "hi_c", "thi_c", "at_c", "vapor_pressure_kpa", "dpd_c",
        "diurnal_range_c", "consecutive_hot_days", "heat_wave",
    ]
    for col in expected_output_cols:
        assert col in df.columns, f"Missing output column: {col}"


def test_all_indicators_preserves_original_columns(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    df = result.df()
    assert "tair_dry_bulb_c" in df.columns
    assert "station_code" in df.columns


# ---------------------------------------------------------------------------
# Combinations
# ---------------------------------------------------------------------------


def test_subset_of_indicators(full_rel):
    result = sus_climate_compute_indicators(
        full_rel,
        indicators=["heat_index", "vapor_pressure"],
        verbose=False,
    )
    df = result.df()
    assert "hi_c" in df.columns
    assert "vapor_pressure_kpa" in df.columns
    # Other indicators NOT present
    assert "thi_c" not in df.columns
    assert "at_c" not in df.columns


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_unknown_indicator_raises(full_rel):
    with pytest.raises(ValueError, match="Unknown indicator code"):
        sus_climate_compute_indicators(full_rel, indicators=["mode"], verbose=False)


def test_missing_required_column_raises():
    """Requesting 'apparent_temperature' without ws_2_m_s → clear error."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"] * 5,
            "date": pd.date_range("2023-01-01", periods=5),
            "tair_dry_bulb_c": [28.0] * 5,
            "rh_mean_porc": [70.0] * 5,
            # ws_2_m_s is intentionally missing
        }
    )
    rel = conn.from_df(df)
    with pytest.raises(ValueError, match="ws_2_m_s"):
        sus_climate_compute_indicators(
            rel, indicators=["apparent_temperature"], verbose=False
        )


# ---------------------------------------------------------------------------
# Numeric sanity checks
# ---------------------------------------------------------------------------


def test_heat_index_value_sanity():
    """HI at T=30, RH=80 should be roughly 35-40°C (hot and humid)."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"],
            "date": pd.to_datetime(["2023-07-15"]),
            "tair_dry_bulb_c": [30.0],
            "rh_mean_porc": [80.0],
        }
    )
    rel = conn.from_df(df)
    result = sus_climate_compute_indicators(
        rel, indicators=["heat_index"], verbose=False
    )
    hi = result.df()["hi_c"].iloc[0]
    assert 30.0 < hi < 50.0, f"Unexpected HI value: {hi}"


def test_vapor_pressure_sanity():
    """At T=25°C, RH=60% → e ≈ 1.90 kPa (within 10% tolerance)."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"],
            "date": pd.to_datetime(["2023-01-01"]),
            "tair_dry_bulb_c": [25.0],
            "rh_mean_porc": [60.0],
        }
    )
    rel = conn.from_df(df)
    result = sus_climate_compute_indicators(
        rel, indicators=["vapor_pressure"], verbose=False
    )
    vp = result.df()["vapor_pressure_kpa"].iloc[0]
    expected = 0.60 * 0.6108 * math.exp(17.27 * 25 / (25 + 237.3))
    assert abs(vp - expected) / expected < 0.01, f"VP={vp}, expected~{expected:.4f}"


# ---------------------------------------------------------------------------
# Verbose output
# ---------------------------------------------------------------------------


def test_verbose_does_not_raise(full_rel, capsys):
    sus_climate_compute_indicators(full_rel, indicators=["heat_index"], verbose=True, lang="en")
    out = capsys.readouterr().out
    assert "sus_climate_compute_indicators" in out
