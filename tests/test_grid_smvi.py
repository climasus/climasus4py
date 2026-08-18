"""Tests for sus_grid_smvi.

Strategy: monkeypatch _smvi_download_once so it writes synthetic LONLAT.csv
/ events tar.gz content directly to the cache paths it's given (mirroring
the real HydroShare LONLAT.csv + SMVI_GLDAS_<year>.csv schema), so the
public API body (validation, extraction, event filtering, point-in-polygon
spatial join, aggregation, metadata assembly) is exercised without any
network I/O.
"""

from __future__ import annotations

import tarfile
from pathlib import Path

import pandas as pd
import pytest

from climasus4py.enrichment import grid_smvi
from climasus4py.enrichment.grid_smvi import sus_grid_smvi

# Two Brazilian cells (inside the -75/-28/-35/6 bbox) + one cell outside Brazil.
_LONLAT_ROWS = [
    {"lon": -55.0, "lat": -15.0},  # cell_id 1, inside a test municipality polygon
    {"lon": -45.0, "lat": -10.0},  # cell_id 2, inside a second municipality polygon
    {"lon": 10.0, "lat": 40.0},  # cell_id 3, outside Brazil bbox entirely
]


def _write_fake_lonlat(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(_LONLAT_ROWS).to_csv(path, index=False)


def _write_fake_events_archive(path: Path, tmp_path: Path) -> None:
    """Build a tar.gz with one SMVI_GLDAS_<year>.csv per requested year."""
    path.parent.mkdir(parents=True, exist_ok=True)
    stage = tmp_path / "stage_events"
    stage.mkdir(exist_ok=True)

    events_2020 = pd.DataFrame({
        "cell_id": [1, 1, 2, 3],
        "fstdate": ["2020-01-05", "2020-06-01", "2020-03-10", "2020-01-01"],
        "lstdate": ["2020-01-10", "2020-06-08", "2020-03-15", "2020-01-05"],
        "SV": [1.5, 2.5, 3.0, 9.9],
    })
    events_2021 = pd.DataFrame({
        "cell_id": [2],
        "fstdate": ["2021-02-01"],
        "lstdate": ["2021-02-04"],
        "SV": [4.0],
    })
    events_2020.to_csv(stage / "SMVI_GLDAS_2020.csv", index=False)
    events_2021.to_csv(stage / "SMVI_GLDAS_2021.csv", index=False)

    with tarfile.open(path, mode="w:gz") as tf:
        for f in stage.glob("*.csv"):
            tf.add(f, arcname=f.name)


def _fake_download_once_factory(tmp_path: Path):
    def _fake(url, cache_path, use_cache, verbose, msg, label):
        if "LONLAT" in label:
            _write_fake_lonlat(cache_path)
        elif "tar.gz" in label:
            _write_fake_events_archive(cache_path, tmp_path)
    return _fake


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_smvi(lang="fr", verbose=False)

    def test_invalid_aggregate_by_raises(self):
        with pytest.raises(ValueError, match="aggregate_by"):
            sus_grid_smvi(aggregate_by="week", verbose=False)  # type: ignore[arg-type]

    def test_invalid_years_range_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_smvi(years=1900, verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_smvi(use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_smvi(cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_smvi(
                municipalities=pd.DataFrame({"code_muni": ["12345"]}), verbose=False
            )


class TestRawEventsMode:
    """municipalities=None: no geopandas needed at all."""

    def test_returns_raw_events(self, tmp_path, monkeypatch):
        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake_download_once_factory(tmp_path))

        result = sus_grid_smvi(cache_dir=tmp_path, use_cache=False, verbose=False)

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {
            "cell_id", "Lon", "Lat", "fstdate", "lstdate", "duration_days", "SV", "source",
        }
        # cell_id 3 is outside the Brazil bbox and must be excluded.
        assert set(result["cell_id"]) == {1, 2}
        assert (result["source"] == "SMVI_GLDAS").all()
        # duration = lstdate - fstdate + 1 (inclusive)
        row = result[(result["cell_id"] == 1) & (result["fstdate"] == "2020-01-05")].iloc[0]
        assert row["duration_days"] == 6

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "smvi"
        assert meta["temporal"]["unit"] == "event"

    def test_years_filter_restricts_events(self, tmp_path, monkeypatch):
        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake_download_once_factory(tmp_path))

        result = sus_grid_smvi(years=2021, cache_dir=tmp_path, use_cache=False, verbose=False)
        assert len(result) == 1
        assert result.iloc[0]["cell_id"] == 2


class TestMunicipalityAggregation:
    def _make_municipalities(self):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        return gpd.GeoDataFrame(
            {"code_muni": ["5100000", "2900000"]},
            geometry=[box(-56, -16, -54, -14), box(-46, -11, -44, -9)],
            crs="EPSG:4326",
        )

    def test_annual_aggregation(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake_download_once_factory(tmp_path))

        result = sus_grid_smvi(
            municipalities=muni, aggregate_by="year", cache_dir=tmp_path,
            use_cache=False, verbose=False,
        )

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            "code_muni", "date", "n_fd_events", "fd_total_days",
            "fd_mean_severity", "fd_max_severity",
        ]
        # cell 1 -> muni 5100000 (2 events in 2020); cell 2 -> muni 2900000
        # (1 event in 2020, 1 in 2021).
        assert len(result) == 3

        row_5100000 = result[
            (result["code_muni"] == "5100000") & (result["date"] == pd.Timestamp("2020-01-01"))
        ].iloc[0]
        assert row_5100000["n_fd_events"] == 2
        assert row_5100000["fd_total_days"] == 6 + 8  # (10-5+1) + (8-1+1)
        assert row_5100000["fd_mean_severity"] == pytest.approx(2.0)
        assert row_5100000["fd_max_severity"] == pytest.approx(2.5)

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "smvi"
        assert meta["aggregate_by"] == "year"
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 3

    def test_monthly_aggregation(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake_download_once_factory(tmp_path))

        result = sus_grid_smvi(
            municipalities=muni, aggregate_by="month", cache_dir=tmp_path,
            use_cache=False, verbose=False,
        )
        # cell 1: Jan 2020 + Jun 2020 (2 distinct months); cell 2: Mar 2020 + Feb 2021
        assert len(result) == 4
        assert result["date"].dt.day.eq(1).all()

    def test_no_events_in_muni_returns_empty_not_raises(self, tmp_path, monkeypatch):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        far_muni = gpd.GeoDataFrame(
            {"code_muni": ["9999999"]}, geometry=[box(10, 10, 11, 11)], crs="EPSG:4326"
        )
        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake_download_once_factory(tmp_path))

        result = sus_grid_smvi(
            municipalities=far_muni, cache_dir=tmp_path, use_cache=False, verbose=False
        )
        assert len(result) == 0
        assert result.attrs["sus_meta"]["n_observations"] == 0


class TestErrorPaths:
    def test_missing_lonlat_download_raises(self, tmp_path, monkeypatch):
        def _fake(url, cache_path, use_cache, verbose, msg, label):
            pass  # never writes anything -> file stays missing

        monkeypatch.setattr(grid_smvi, "_smvi_download_once", _fake)
        with pytest.raises(ValueError, match="LONLAT"):
            sus_grid_smvi(cache_dir=tmp_path, use_cache=False, verbose=False)
