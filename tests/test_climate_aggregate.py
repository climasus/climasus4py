"""Tests for sus_climate_aggregate — lazy SQL aggregation (B.1)."""

from __future__ import annotations

import pandas as pd
import pytest

import duckdb

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate_aggregate import sus_climate_aggregate


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_inmet_rel(n_days: int = 30, n_stations: int = 2) -> duckdb.DuckDBPyRelation:
    """Synthetic INMET-like relation with 2 numeric climate variables."""
    import numpy as np

    rng = np.random.default_rng(42)
    rows = []
    for station in [f"A{700 + i}" for i in range(n_stations)]:
        for date in pd.date_range("2023-01-01", periods=n_days, freq="D"):
            rows.append(
                {
                    "station_code": station,
                    "date": date,
                    "tair_dry_bulb_c": float(rng.uniform(20.0, 35.0)),
                    "rh_mean_porc": float(rng.uniform(50.0, 95.0)),
                }
            )
    df = pd.DataFrame(rows)
    return get_connection().from_df(df)


@pytest.fixture
def inmet_rel():
    return _make_inmet_rel()


@pytest.fixture
def single_station_rel():
    return _make_inmet_rel(n_days=30, n_stations=1)


# ---------------------------------------------------------------------------
# B.1.1 — Returns DuckDBPyRelation (lazy)
# ---------------------------------------------------------------------------


def test_monthly_aggregation_keeps_lazy(inmet_rel):
    result = sus_climate_aggregate(inmet_rel, time_resolution="monthly", verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


def test_result_can_be_collected(inmet_rel):
    result = sus_climate_aggregate(inmet_rel, time_resolution="monthly", verbose=False)
    df = result.df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


# ---------------------------------------------------------------------------
# B.1.2 — Seasonal bucketing (DJF/MAM/JJA/SON)
# ---------------------------------------------------------------------------


def test_seasonal_aggregation_buckets(inmet_rel):
    result = sus_climate_aggregate(inmet_rel, time_resolution="seasonal", verbose=False)
    df = result.df()
    assert "time_bucket" in df.columns
    valid_seasons = {"DJF", "MAM", "JJA", "SON"}
    assert set(df["time_bucket"].unique()).issubset(valid_seasons)


def test_seasonal_aggregation_keeps_lazy(inmet_rel):
    result = sus_climate_aggregate(inmet_rel, time_resolution="seasonal", verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


# ---------------------------------------------------------------------------
# B.1.3 — Yearly bucketing
# ---------------------------------------------------------------------------


def test_yearly_aggregation(inmet_rel):
    result = sus_climate_aggregate(inmet_rel, time_resolution="yearly", verbose=False)
    df = result.df()
    assert "time_bucket" in df.columns
    # All rows are from 2023
    assert set(df["time_bucket"].unique()) == {"2023"}


# ---------------------------------------------------------------------------
# B.1.4 — days_above_threshold
# ---------------------------------------------------------------------------


def test_threshold_adds_days_above_column(inmet_rel):
    result = sus_climate_aggregate(
        inmet_rel,
        time_resolution="monthly",
        stats=["days_above_threshold"],
        threshold=30.0,
        verbose=False,
    )
    df = result.df()
    assert any("days_above_threshold" in c for c in df.columns)


def test_threshold_ignored_when_stat_not_requested(inmet_rel):
    # Should not raise even if threshold is given but stat is not requested
    result = sus_climate_aggregate(
        inmet_rel,
        time_resolution="monthly",
        stats=["mean"],
        threshold=30.0,
        verbose=False,
    )
    df = result.df()
    assert all("days_above_threshold" not in c for c in df.columns)


# ---------------------------------------------------------------------------
# B.1.5 — Invalid stat raises ValueError
# ---------------------------------------------------------------------------


def test_invalid_stat_raises(inmet_rel):
    with pytest.raises(ValueError, match="Unknown stat"):
        sus_climate_aggregate(inmet_rel, stats=["mode"], verbose=False)


# ---------------------------------------------------------------------------
# B.1.6 — Invalid resolution raises ValueError
# ---------------------------------------------------------------------------


def test_invalid_resolution_raises(inmet_rel):
    with pytest.raises(ValueError, match="Invalid time_resolution"):
        sus_climate_aggregate(inmet_rel, time_resolution="weekly", verbose=False)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# B.1.7 — Empty relation returns 0 rows without crash
# ---------------------------------------------------------------------------


def test_empty_relation_returns_zero_rows():
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": pd.Series([], dtype=str),
            "date": pd.Series([], dtype="datetime64[ns]"),
            "tair_dry_bulb_c": pd.Series([], dtype=float),
        }
    )
    rel = conn.from_df(df)
    result = sus_climate_aggregate(rel, time_resolution="monthly", verbose=False)
    assert result.df().shape[0] == 0


# ---------------------------------------------------------------------------
# B.1.8 — Output columns sanity
# ---------------------------------------------------------------------------


def test_output_contains_station_and_time_bucket(inmet_rel):
    result = sus_climate_aggregate(
        inmet_rel, time_resolution="monthly", stats=["mean"], verbose=False
    )
    df = result.df()
    assert "station_code" in df.columns
    assert "time_bucket" in df.columns
    # mean columns should be present
    assert "tair_dry_bulb_c_mean" in df.columns or "rh_mean_porc_mean" in df.columns


def test_all_stats_produce_expected_columns(single_station_rel):
    stats = ["mean", "min", "max", "std", "p10", "p90"]
    result = sus_climate_aggregate(
        single_station_rel, time_resolution="monthly", stats=stats, verbose=False
    )
    df = result.df()
    for stat in stats:
        col = f"tair_dry_bulb_c_{stat}"
        assert col in df.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# B.1.9 — Verbose output (smoke)
# ---------------------------------------------------------------------------


def test_verbose_does_not_raise(inmet_rel, capsys):
    sus_climate_aggregate(inmet_rel, time_resolution="monthly", verbose=True, lang="en")
    out = capsys.readouterr().out
    assert "sus_climate_aggregate" in out
