"""Tests for sus_grid_pdsi."""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment import grid_pdsi
from climasus4py.enrichment.grid_pdsi import sus_grid_pdsi


def _fake_download_writes_placeholder(url, cache_path, use_cache, verbose, msg):
    """Stand-in for _download_pdsi_file: writes a tiny placeholder file.

    Avoids any network access and any real raster I/O — the raster
    reading itself is monkeypatched separately wherever a test needs
    actual pixel values.
    """
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not-a-real-netcdf")


def _make_fake_process_row(value: float = -1.5, n_months: int = 2):
    def _fn(row, source, months, municipalities, bbox, agg_fun, msg):
        frames = []
        for m in months[:n_months]:
            frames.append(pd.DataFrame({
                "code_muni": municipalities["code_muni"].to_numpy(),
                "date": pd.Timestamp(year=row.years[0], month=m, day=1),
                "pdsi": np.full(len(municipalities), value, dtype="float64"),
            }))
        return pd.concat(frames, ignore_index=True)
    return _fn


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_pdsi(lang="fr", verbose=False)

    def test_invalid_source_raises(self):
        with pytest.raises(ValueError, match="source"):
            sus_grid_pdsi(source="cru_ts", verbose=False)

    def test_invalid_years_range_terraclimate_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_pdsi(years=1900, verbose=False)

    def test_invalid_years_range_noaa_psl_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_pdsi(years=2020, source="noaa_psl", verbose=False)

    def test_invalid_months_raises(self):
        with pytest.raises(ValueError, match="months"):
            sus_grid_pdsi(years=2020, months=[0, 13], verbose=False)

    def test_invalid_agg_fun_raises(self):
        with pytest.raises(ValueError, match="agg_fun"):
            sus_grid_pdsi(years=2020, agg_fun="bogus", verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_pdsi(years=2020, use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_pdsi(years=2020, cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_pdsi(
                years=2020, municipalities=pd.DataFrame({"code_muni": ["12345"]}), verbose=False
            )


class TestPathsOnlyMode:
    """municipalities=None: no geopandas/rioxarray/exactextract needed at all."""

    def test_returns_dict_of_paths(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pdsi(
            years=[2020, 2021],
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )
        assert isinstance(result, dict)
        assert len(result) == 2
        for filename, path in result.items():
            assert filename in path
            assert Path(path).is_file()

    def test_noaa_psl_returns_single_shared_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pdsi(
            years=[1990, 1991, 1992],
            source="noaa_psl",
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "pdsi.mon.mean.selfcalibrated.nc" in result

    def test_default_years_used_when_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pdsi(cache_dir=tmp_path, verbose=False)
        assert isinstance(result, dict)
        assert len(result) == 2  # last 2 complete years, TerraClimate

    def test_default_years_clamped_for_noaa_psl(self, tmp_path, monkeypatch):
        # NOAA PSL max_year is 2018 - the default-years quirk must shift
        # both candidate years down rather than raising, mirroring R.
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pdsi(
            source="noaa_psl", cache_dir=tmp_path, use_cache=False, verbose=False
        )
        assert isinstance(result, dict)
        assert len(result) == 1


class TestHappyPathWithMunicipalities:
    def _make_municipalities(self):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        return gpd.GeoDataFrame(
            {"code_muni": ["3106200", "5103403"]},
            geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
            crs="EPSG:4326",
        )

    def test_terraclimate_aggregation_shape_and_values(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(
            grid_pdsi, "_process_manifest_row", _make_fake_process_row(-1.5, n_months=2)
        )

        result = sus_grid_pdsi(
            years=2020,
            months=[1, 2],
            municipalities=muni,
            agg_fun="mean",
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {"code_muni", "date", "pdsi"}
        assert len(result) == 4  # 2 municipalities x 2 months
        assert (result["pdsi"] == -1.5).all()
        assert set(result["code_muni"]) == {"3106200", "5103403"}

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "pdsi"
        assert meta["source"] == "terraclimate"
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 4

    def test_parquet_cache_hit_skips_extraction(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()

        def _boom(*args, **kwargs):
            raise AssertionError("should not be called: Parquet cache should short-circuit")

        monkeypatch.setattr(grid_pdsi, "_download_pdsi_file", _boom)
        monkeypatch.setattr(grid_pdsi, "_process_manifest_row", _boom)

        codes = sorted(str(c) for c in muni["code_muni"])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

        pq_dir = Path(tmp_path) / "parquet"
        pq_dir.mkdir(parents=True)
        cached = pd.DataFrame({
            "code_muni": ["3106200", "5103403"],
            "date": [pd.Timestamp("2020-01-01")] * 2,
            "pdsi": [-2.0, 1.0],
        })
        cached.to_parquet(pq_dir / f"pdsi_terraclimate_2020{muni_hash}.parquet", index=False)

        result = sus_grid_pdsi(
            years=2020,
            months=[1],
            municipalities=muni,
            cache_dir=tmp_path,
            use_cache=True,
            verbose=False,
        )
        assert len(result) == 2
        assert set(result["pdsi"]) == {-2.0, 1.0}

    def test_extraction_error_is_skipped_not_raised(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )

        def _raise_read(nc_path, source):
            raise ValueError("corrupt raster")

        monkeypatch.setattr(grid_pdsi, "_read_pdsi_raster", _raise_read)

        with pytest.raises(ValueError, match="extra"):
            sus_grid_pdsi(
                years=2020,
                months=[1],
                municipalities=muni,
                cache_dir=tmp_path,
                use_cache=False,
                verbose=False,
            )

    def test_muni_hash_disambiguates_cache_across_calls(self, tmp_path, monkeypatch):
        """Two different municipality sets must not collide on the same Parquet cache."""
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        monkeypatch.setattr(
            grid_pdsi, "_download_pdsi_file", _fake_download_writes_placeholder
        )

        muni_a = gpd.GeoDataFrame(
            {"code_muni": ["3106200"]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
        )
        muni_b = gpd.GeoDataFrame(
            {"code_muni": ["5103403"]}, geometry=[box(1, 1, 2, 2)], crs="EPSG:4326"
        )

        monkeypatch.setattr(
            grid_pdsi, "_process_manifest_row", _make_fake_process_row(-3.0, n_months=1)
        )
        result_a = sus_grid_pdsi(
            years=2020, months=[1], municipalities=muni_a,
            cache_dir=tmp_path, use_cache=True, verbose=False,
        )

        monkeypatch.setattr(
            grid_pdsi, "_process_manifest_row", _make_fake_process_row(4.0, n_months=1)
        )
        result_b = sus_grid_pdsi(
            years=2020, months=[1], municipalities=muni_b,
            cache_dir=tmp_path, use_cache=True, verbose=False,
        )

        assert result_a["pdsi"].iloc[0] == -3.0
        assert result_b["pdsi"].iloc[0] == 4.0


class TestRasterZonalStatsIntegration:
    """Real rioxarray + xarray + exactextract path — gated on all being installed."""

    def test_read_raster_and_zonal_stats_on_synthetic_netcdf(self, tmp_path):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        pytest.importorskip("exactextract", reason="exactextract not installed")
        xr = pytest.importorskip("xarray", reason="xarray not installed")
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        import rioxarray  # noqa: F401
        from shapely.geometry import box

        data = np.full((2, 10, 10), 3.0, dtype="float64")
        da = xr.DataArray(
            data,
            dims=("time", "y", "x"),
            coords={
                "time": pd.date_range("2020-01-01", periods=2, freq="MS"),
                "y": np.linspace(5.5, -3.5, 10),
                "x": np.linspace(-4.5, 4.5, 10),
            },
            name="PDSI",
        )
        ds = da.to_dataset()
        nc_path = tmp_path / "TerraClimate_PDSI_2020.nc"
        ds.to_netcdf(nc_path)

        raster = grid_pdsi._read_pdsi_raster(nc_path, "terraclimate")
        assert "time" in raster.dims

        muni = gpd.GeoDataFrame(
            {"code_muni": ["0000001"]}, geometry=[box(-2, -2, 2, 2)], crs="EPSG:4326"
        )
        band = raster.isel(time=0)
        values = grid_pdsi._zonal_stats(band, muni, "mean")
        assert len(values) == 1
        assert values[0] == pytest.approx(3.0, abs=0.5)
