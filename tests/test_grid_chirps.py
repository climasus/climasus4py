"""Tests for sus_grid_chirps."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment import grid_chirps
from climasus4py.enrichment.grid_chirps import sus_grid_chirps


def _fake_download_writes_placeholder(url, cache_path, use_cache, verbose, msg):
    """Stand-in for _download_chirps_file: writes a tiny placeholder file.

    Avoids any network access and any real raster I/O — the raster
    reading itself is monkeypatched separately wherever a test needs
    actual pixel values.
    """
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not-a-real-tif")


def _fake_read_raster(tif_path, bbox):
    return "fake-raster-object"


def _make_fake_zonal_stats(value: float = 5.0):
    def _fn(raster, municipalities, agg_fun):
        return np.full(len(municipalities), value, dtype="float64")
    return _fn


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_chirps(lang="fr", verbose=False)

    def test_invalid_resolution_raises(self):
        with pytest.raises(ValueError, match="resolution"):
            sus_grid_chirps(resolution="weekly", verbose=False)

    def test_invalid_years_range_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_chirps(years=1900, verbose=False)

    def test_annual_year_beyond_2024_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_chirps(resolution="annual", years=2030, verbose=False)

    def test_invalid_months_raises(self):
        with pytest.raises(ValueError, match="months"):
            sus_grid_chirps(years=2020, months=[0, 13], verbose=False)

    def test_invalid_agg_fun_raises(self):
        with pytest.raises(ValueError, match="agg_fun"):
            sus_grid_chirps(years=2020, agg_fun="bogus", verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_chirps(years=2020, use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_chirps(years=2020, cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_chirps(
                years=2020, municipalities=pd.DataFrame({"code_muni": ["12345"]}), verbose=False
            )


class TestPathsOnlyMode:
    """municipalities=None: no geopandas/rioxarray/exactextract needed at all."""

    def test_returns_dict_of_paths(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_chirps, "_download_chirps_file", _fake_download_writes_placeholder
        )
        result = sus_grid_chirps(
            resolution="annual",
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

    def test_default_years_used_when_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_chirps, "_download_chirps_file", _fake_download_writes_placeholder
        )
        result = sus_grid_chirps(
            resolution="monthly", years=None, months=[1], cache_dir=tmp_path, verbose=False
        )
        assert isinstance(result, dict)
        assert len(result) == 2  # last 2 complete years, 1 month each


class TestHappyPathWithMunicipalities:
    def _make_municipalities(self):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        return gpd.GeoDataFrame(
            {"code_muni": ["3106200", "5103403"]},
            geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
            crs="EPSG:4326",
        )

    def test_monthly_aggregation_shape_and_values(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(
            grid_chirps, "_download_chirps_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(grid_chirps, "_read_raster", _fake_read_raster)
        monkeypatch.setattr(grid_chirps, "_zonal_stats", _make_fake_zonal_stats(42.0))

        result = sus_grid_chirps(
            resolution="monthly",
            years=2020,
            months=[1, 2],
            municipalities=muni,
            agg_fun="mean",
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {"code_muni", "date", "rainfall_chirps_mm"}
        assert len(result) == 4  # 2 municipalities x 2 months
        assert (result["rainfall_chirps_mm"] == 42.0).all()
        assert set(result["code_muni"]) == {"3106200", "5103403"}

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "chirps"
        assert meta["resolution"] == "monthly"
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 4

        # The per-group Parquet cache must actually get written to disk —
        # otherwise use_cache=True silently never speeds up a repeat call.
        written = list((Path(tmp_path) / "parquet").glob("*.parquet"))
        assert len(written) == 2  # one per (year, month) group

    def test_missing_raster_package_fails_before_any_download(self, tmp_path, monkeypatch):
        """Mirrors R's check_installed("terra")/("exactextractr") pre-flight.

        Fails fast on a missing rioxarray/exactextract install, before the
        download loop runs — not just at the first raster read.
        """
        muni = self._make_municipalities()

        def _boom(*args, **kwargs):
            raise AssertionError("download must not run before the package pre-flight check")

        monkeypatch.setattr(grid_chirps, "_download_chirps_file", _boom)
        monkeypatch.setattr(
            grid_chirps, "find_spec", lambda name: None if name == "rioxarray" else object()
        )

        with pytest.raises(ImportError, match="rioxarray"):
            sus_grid_chirps(
                years=2020, months=[1], municipalities=muni, cache_dir=tmp_path, verbose=False
            )

    def test_parquet_cache_hit_skips_extraction(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()

        def _boom(*args, **kwargs):
            raise AssertionError("should not be called: Parquet cache should short-circuit")

        monkeypatch.setattr(grid_chirps, "_download_chirps_file", _boom)
        monkeypatch.setattr(grid_chirps, "_read_raster", _boom)
        monkeypatch.setattr(grid_chirps, "_zonal_stats", _boom)

        muni_col = grid_chirps._detect_muni_col(muni, grid_chirps._MESSAGES["pt"])
        codes = sorted(str(c) for c in muni[muni_col])
        import hashlib

        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

        pq_dir = Path(tmp_path) / "parquet"
        pq_dir.mkdir(parents=True)
        cached = pd.DataFrame({
            "code_muni": ["3106200", "5103403"],
            "date": [pd.Timestamp("2020-01-01")] * 2,
            "rainfall_chirps_mm": [10.0, 20.0],
        })
        cached.to_parquet(pq_dir / f"chirps_monthly_202001{muni_hash}.parquet", index=False)

        result = sus_grid_chirps(
            resolution="monthly",
            years=2020,
            months=[1],
            municipalities=muni,
            cache_dir=tmp_path,
            use_cache=True,
            verbose=False,
        )
        assert len(result) == 2
        assert set(result["rainfall_chirps_mm"]) == {10.0, 20.0}

        # Preserved R quirk: the Parquet-cache early-return path builds a
        # slimmer sus_meta than the full-extraction path (no resolution/
        # years/months/agg_fun, distinct temporal.source) — lock it in
        # rather than leaving it incidental.
        meta = result.attrs["sus_meta"]
        assert meta["temporal"]["source"] == "ucsb_chirps_v2_cache"
        assert "resolution" not in meta

    def test_extraction_error_is_skipped_not_raised(self, tmp_path, monkeypatch):
        muni = self._make_municipalities()
        monkeypatch.setattr(
            grid_chirps, "_download_chirps_file", _fake_download_writes_placeholder
        )

        def _raise_read(tif_path, bbox):
            raise ValueError("corrupt raster")

        monkeypatch.setattr(grid_chirps, "_read_raster", _raise_read)

        with pytest.raises(ValueError, match="extra"):
            sus_grid_chirps(
                resolution="monthly",
                years=2020,
                months=[1],
                municipalities=muni,
                cache_dir=tmp_path,
                use_cache=False,
                verbose=False,
            )


class TestRasterZonalStatsIntegration:
    """Real rioxarray + exactextract path — gated on both being installed."""

    def _make_synthetic_raster(self, tmp_path):
        import rioxarray  # noqa: F401
        import xarray as xr

        data = np.full((1, 10, 10), 3.0, dtype="float64")
        data[0, 0, 0] = -9999.0  # CHIRPS no-data sentinel, outside the test polygon
        da = xr.DataArray(
            data,
            dims=("band", "y", "x"),
            coords={
                "band": [1],
                "y": np.linspace(5.5, -3.5, 10),
                "x": np.linspace(-4.5, 4.5, 10),
            },
        )
        da = da.rio.write_crs("EPSG:4326")
        tif_path = tmp_path / "synthetic.tif"
        da.rio.to_raster(tif_path)
        return tif_path

    def test_read_raster_masks_nodata(self, tmp_path):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        tif_path = self._make_synthetic_raster(tmp_path)
        raster = grid_chirps._read_raster(tif_path, bbox=None)
        assert float(raster.min()) >= -999  # nodata masked to NaN, not -9999

    @pytest.mark.parametrize("agg_fun", grid_chirps._VALID_AGG)
    def test_zonal_stats_picks_correct_column_per_agg_fun(self, tmp_path, agg_fun):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        pytest.importorskip("exactextract", reason="exactextract not installed")
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        tif_path = self._make_synthetic_raster(tmp_path)
        raster = grid_chirps._read_raster(tif_path, bbox=None)

        # Polygon fully inside the constant-3.0 region, away from the
        # NaN corner cell — every stat except "sum" must equal 3.0
        # exactly; "sum" scales with covered-cell count, so just check
        # it's a finite positive number and that the right column was
        # actually selected (not a stray fallback to some other column).
        muni = gpd.GeoDataFrame(
            {"code_muni": ["0000001"]}, geometry=[box(-2, -2, 2, 2)], crs="EPSG:4326"
        )
        values = grid_chirps._zonal_stats(raster, muni, agg_fun)
        assert len(values) == 1
        assert np.isfinite(values[0])
        if agg_fun != "sum":
            assert values[0] == pytest.approx(3.0, abs=1e-6)
        else:
            assert values[0] > 0
