"""Tests for sus_climate_uniplu.

Strategy: monkeypatch _ensure_uniplu_cache to write two tiny synthetic
Parquet files (mirroring UNIPLU-BR's table_info/table_data schema) into a
tmp_path cache dir, so the public API body (validation, join/rename SQL,
filtering, aggregation, metadata assembly) is exercised without any
network I/O or real Zenodo download.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from climasus4py.core.climate_uniplu import (
    _VALID_NETWORKS,
    sus_climate_uniplu,
)

_PATCH_TARGET = "climasus4py.core.climate_uniplu._ensure_uniplu_cache"


def _write_fixture_parquets(tmp_path: Path) -> tuple[Path, Path]:
    """Write synthetic table_info.parquet / table_data.parquet."""
    info = pd.DataFrame({
        "gauge_code": ["G1", "G2"],
        "city": ["NATAL", "SAO PAULO"],
        "state": ["rn", "sp"],
        "lat": [-5.79, -23.55],
        "long": [-35.21, -46.63],
        "elevation": [30.0, 760.0],
        "time_step": [1440, 60],
        "network": ["Hidroweb", "CEMADEN"],
        "responsible": ["ANA", "CEMADEN"],
        "utc": [-3, -3],
    })
    data = pd.DataFrame({
        "gauge_code": ["G1", "G1", "G2", "G2", "G2"],
        "datetime": pd.to_datetime([
            "2023-01-01 00:00:00",
            "2023-01-02 00:00:00",
            "2023-06-15 10:00:00",
            "2023-06-15 11:00:00",
            "2022-01-01 00:00:00",
        ]),
        "rain_mm": [5.0, 10.0, 2.0, 3.0, 1.0],
    })
    info_path = tmp_path / "table_info.parquet"
    data_path = tmp_path / "table_data.parquet"
    info.to_parquet(info_path, index=False)
    data.to_parquet(data_path, index=False)
    return info_path, data_path


@pytest.fixture()
def patched_cache(tmp_path, monkeypatch):
    """Patch cache resolution to point at synthetic fixture Parquet files."""
    info_path, data_path = _write_fixture_parquets(tmp_path)

    def _fake_ensure_cache(uniplu_dir, use_cache, msg, verbose):
        return info_path, data_path

    monkeypatch.setattr(_PATCH_TARGET, _fake_ensure_cache)
    return info_path, data_path


class TestValidation:
    def test_invalid_years_raises(self, patched_cache):
        with pytest.raises(ValueError, match="Invalid values in 'years'"):
            sus_climate_uniplu(years=1800, verbose=False)

    def test_invalid_lang_raises(self, patched_cache):
        with pytest.raises(ValueError, match="'lang' must be one of"):
            sus_climate_uniplu(years=2023, lang="fr", verbose=False)

    def test_invalid_uf_raises(self, patched_cache):
        with pytest.raises(ValueError, match="Invalid values in 'uf'"):
            sus_climate_uniplu(years=2023, uf="XX", verbose=False)

    def test_invalid_network_raises(self, patched_cache):
        with pytest.raises(ValueError, match="Invalid 'network' value"):
            sus_climate_uniplu(years=2023, network="NotANetwork", verbose=False)

    def test_invalid_aggregate_to_raises(self, patched_cache):
        with pytest.raises(ValueError, match="'aggregate_to' must be one of"):
            sus_climate_uniplu(years=2023, aggregate_to="week", verbose=False)

    def test_no_observations_for_year_raises(self, patched_cache):
        with pytest.raises(ValueError, match="No observations found for years"):
            sus_climate_uniplu(years=1900, verbose=False)

    def test_no_observations_for_uf_raises(self, patched_cache):
        with pytest.raises(ValueError, match="No observations found for UF"):
            sus_climate_uniplu(years=2023, uf="AM", verbose=False)

    def test_no_observations_for_network_raises(self, patched_cache):
        with pytest.raises(ValueError, match="No observations found for network"):
            sus_climate_uniplu(years=2023, network="ICEA", verbose=False)


class TestHappyPath:
    def test_returns_dataframe_with_metadata_daily(self, patched_cache):
        df = sus_climate_uniplu(years=2023, aggregate_to="day", verbose=False)

        assert set(df.columns) >= {
            "station_code", "station_name", "uf", "latitude", "longitude",
            "altitude", "network", "date", "rainfall_mm",
        }
        # aggregated: per-observation columns are dropped
        assert "time_step_min" not in df.columns
        assert "utc_offset" not in df.columns

        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "uniplu"
        assert meta["source"] == "UNIPLU-BR"
        assert meta["aggregate_to"] == "day"
        assert meta["n_observations"] == len(df)
        assert meta["years"] == [2023]

        # G1 has two distinct days in 2023 -> two rows, sums preserved per day
        g1 = df[df["station_code"] == "G1"].sort_values("date")
        assert len(g1) == 2
        assert list(g1["rainfall_mm"]) == [5.0, 10.0]

    def test_aggregate_none_keeps_raw_columns(self, patched_cache):
        df = sus_climate_uniplu(years=2023, aggregate_to="none", verbose=False)
        assert "time_step_min" in df.columns
        assert "utc_offset" in df.columns
        # G2 has two raw observations in 2023 (June 15, two different hours)
        assert len(df[df["station_code"] == "G2"]) == 2

    def test_month_aggregation_sums_within_month(self, patched_cache):
        df = sus_climate_uniplu(years=2023, aggregate_to="month", verbose=False)
        g1 = df[df["station_code"] == "G1"]
        # Both G1 observations fall in January 2023 -> summed into one row
        assert len(g1) == 1
        assert g1["rainfall_mm"].iloc[0] == 15.0

    def test_uf_filter_narrows_stations(self, patched_cache):
        df = sus_climate_uniplu(years=2023, uf="RN", aggregate_to="none", verbose=False)
        assert set(df["station_code"]) == {"G1"}
        assert (df["uf"] == "RN").all()

    def test_network_filter_narrows_stations(self, patched_cache):
        df = sus_climate_uniplu(
            years=2023, network="cemaden", aggregate_to="none", verbose=False
        )
        assert set(df["station_code"]) == {"G2"}
        assert (df["network"] == "CEMADEN").all()

    def test_years_default_last_two_years(self, patched_cache):
        # Fixture data only covers 2022/2023, so the default (last 2
        # calendar years) legitimately finds nothing — this still proves
        # the default-years code path runs without raising a validation
        # error before reaching the "no observations" check.
        with pytest.raises(ValueError, match="No observations found for years"):
            sus_climate_uniplu(verbose=False)


class TestConstants:
    def test_valid_networks(self):
        assert _VALID_NETWORKS == ("Hidroweb", "INMET", "ICEA", "CEMADEN", "Telemetria")
