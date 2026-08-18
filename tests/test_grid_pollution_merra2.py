"""Tests for sus_grid_pollution_merra2."""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment import grid_pollution_merra2
from climasus4py.enrichment.grid_pollution_merra2 import sus_grid_pollution_merra2

_ENV = {"EARTHDATA_USER": "test_user", "EARTHDATA_PASSWORD": "test_pass"}


def _with_auth(monkeypatch):
    for k, v in _ENV.items():
        monkeypatch.setenv(k, v)


def _fake_download_noop(url, cache_path, use_cache, user, password, netrc_path, verbose, msg):
    """Stand-in for _merra2_download_file: writes a tiny placeholder file."""
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not-a-real-netcdf")


def _make_fake_zonal_stats(value: float = 5.0):
    def _fn(raster, municipalities, agg_fun):
        return np.full(len(municipalities), value, dtype="float64")
    return _fn


def _fake_read_nc_var(nc_path, var_name, bbox, p_res, agg_fun):
    return f"fake-raster::{var_name}"


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_pollution_merra2(lang="fr", verbose=False)

    def test_invalid_pollutants_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="pollutants"):
            sus_grid_pollution_merra2(pollutants=["ozone"], verbose=False)

    def test_invalid_resolution_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="resolution"):
            sus_grid_pollution_merra2(resolution="weekly", verbose=False)

    def test_so2_daily_drops_to_no_pollutants_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="poluente|pollutant"):
            sus_grid_pollution_merra2(pollutants=["so2"], resolution="daily", verbose=False)

    def test_so2_daily_warns_and_drops_so2_but_keeps_others(self, monkeypatch, tmp_path):
        _with_auth(monkeypatch)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)
        result = sus_grid_pollution_merra2(
            pollutants=["pm25", "so2"], resolution="daily", years=2020, months=[1],
            cache_dir=tmp_path, verbose=False,
        )
        assert isinstance(result, dict)
        # so2 dropped -> only pm25's file remains in the manifest.
        assert len(result) == 1

    def test_invalid_years_range_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="years"):
            sus_grid_pollution_merra2(years=1900, verbose=False)

    def test_invalid_months_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="months"):
            sus_grid_pollution_merra2(years=2020, months=[0, 13], verbose=False)

    def test_invalid_agg_fun_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="agg_fun"):
            sus_grid_pollution_merra2(years=2020, agg_fun="bogus", verbose=False)

    def test_invalid_use_cache_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_pollution_merra2(years=2020, use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self, monkeypatch):
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_pollution_merra2(years=2020, cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self, monkeypatch):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        _with_auth(monkeypatch)
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_pollution_merra2(
                years=2020, municipalities=pd.DataFrame({"code_muni": ["12345"]}), verbose=False
            )

    def test_missing_auth_raises_before_manifest(self, monkeypatch, tmp_path):
        monkeypatch.delenv("EARTHDATA_USER", raising=False)
        monkeypatch.delenv("EARTHDATA_PASSWORD", raising=False)

        def _boom(*args, **kwargs):
            raise AssertionError("manifest/download must not run before the auth check")

        monkeypatch.setattr(grid_pollution_merra2, "_build_manifest", _boom)
        with pytest.raises(ValueError, match="Earthdata|Credenciais"):
            sus_grid_pollution_merra2(years=2020, cache_dir=tmp_path, verbose=False)

    def test_explicit_credentials_satisfy_auth_check(self, monkeypatch, tmp_path):
        monkeypatch.delenv("EARTHDATA_USER", raising=False)
        monkeypatch.delenv("EARTHDATA_PASSWORD", raising=False)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)
        result = sus_grid_pollution_merra2(
            years=2020, months=[1], earthdata_user="u", earthdata_pass="p",
            cache_dir=tmp_path, verbose=False,
        )
        assert isinstance(result, dict)


class TestPathsOnlyMode:
    """municipalities=None: no geopandas/rioxarray/exactextract needed at all."""

    def test_returns_dict_of_paths(self, monkeypatch, tmp_path):
        _with_auth(monkeypatch)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)
        result = sus_grid_pollution_merra2(
            pollutants=["pm25", "aod"], years=2020, months=[1, 2],
            cache_dir=tmp_path, use_cache=False, verbose=False,
        )
        assert isinstance(result, dict)
        # pm25 and aod share the same underlying NetCDF file per (year, month).
        assert len(result) == 2
        for filename, path in result.items():
            assert filename in path
            assert Path(path).is_file()

    def test_default_years_used_when_none(self, monkeypatch, tmp_path):
        _with_auth(monkeypatch)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)
        result = sus_grid_pollution_merra2(
            pollutants=["aod"], years=None, months=[1], cache_dir=tmp_path, verbose=False
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

    def test_multi_pollutant_merge_shape_and_columns(self, monkeypatch, tmp_path):
        muni = self._make_municipalities()
        _with_auth(monkeypatch)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)
        monkeypatch.setattr(grid_pollution_merra2, "_read_nc_var", _fake_read_nc_var)
        monkeypatch.setattr(grid_pollution_merra2, "_zonal_stats", _make_fake_zonal_stats(1.0))

        result = sus_grid_pollution_merra2(
            pollutants=["pm25", "aod"], resolution="monthly", years=2020, months=[1, 2],
            municipalities=muni, agg_fun="mean", cache_dir=tmp_path, use_cache=False,
            verbose=False,
        )

        assert isinstance(result, pd.DataFrame)
        # One row per (municipality, month) even with 2 pollutants — the
        # fixed merge (concat-within-pollutant, then outer-merge across
        # pollutants), not R's literal full_join-per-manifest-row which
        # would suffix duplicate columns across different months.
        assert len(result) == 4  # 2 municipalities x 2 months
        assert set(result.columns) == {"code_muni", "date", "pm25_merra2", "aod_merra2"}
        # PM2.5 = (1+1+1+1.4*1+1) * 1e9 = 5.4e9; AOD = 1.0 directly.
        assert result["pm25_merra2"].to_numpy() == pytest.approx(5.4e9)
        assert result["aod_merra2"].to_numpy() == pytest.approx(1.0)

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "pollution_merra2"
        assert meta["pollutants"] == ["pm25", "aod"]
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 4

        written = list((Path(tmp_path) / "parquet").glob("*.parquet"))
        assert len(written) == 4  # one per (pollutant, year, month)

    def test_missing_raster_package_fails_before_any_download(self, monkeypatch, tmp_path):
        """Mirrors R's check_installed("terra")/("exactextractr") pre-flight."""
        muni = self._make_municipalities()
        _with_auth(monkeypatch)

        def _boom(*args, **kwargs):
            raise AssertionError("download must not run before the package pre-flight check")

        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _boom)
        monkeypatch.setattr(
            grid_pollution_merra2, "find_spec",
            lambda name: None if name == "rioxarray" else object(),
        )

        with pytest.raises(ImportError, match="rioxarray"):
            sus_grid_pollution_merra2(
                years=2020, months=[1], municipalities=muni, cache_dir=tmp_path, verbose=False
            )

    def test_parquet_cache_hit_skips_extraction(self, monkeypatch, tmp_path):
        muni = self._make_municipalities()
        _with_auth(monkeypatch)

        def _boom(*args, **kwargs):
            raise AssertionError("should not be called: Parquet cache should short-circuit")

        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _boom)
        monkeypatch.setattr(grid_pollution_merra2, "_read_nc_var", _boom)
        monkeypatch.setattr(grid_pollution_merra2, "_zonal_stats", _boom)

        muni_col = grid_pollution_merra2._detect_muni_col(muni)
        codes = sorted(str(c) for c in muni[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

        pq_dir = Path(tmp_path) / "parquet"
        pq_dir.mkdir(parents=True)
        cached = pd.DataFrame({
            "code_muni": ["3106200", "5103403"],
            "date": [pd.Timestamp("2020-01-01")] * 2,
            "aod_merra2": [0.1, 0.2],
        })
        cached.to_parquet(pq_dir / f"aod_monthly_202001{muni_hash}.parquet", index=False)

        result = sus_grid_pollution_merra2(
            pollutants=["aod"], resolution="monthly", years=2020, months=[1],
            municipalities=muni, cache_dir=tmp_path, use_cache=True, verbose=False,
        )
        assert len(result) == 2
        assert set(result["aod_merra2"]) == {0.1, 0.2}

        meta = result.attrs["sus_meta"]
        assert meta["temporal"]["source"] == "nasa_merra2_cache"

    def test_extraction_error_is_skipped_not_raised(self, monkeypatch, tmp_path):
        muni = self._make_municipalities()
        _with_auth(monkeypatch)
        monkeypatch.setattr(grid_pollution_merra2, "_merra2_download_file", _fake_download_noop)

        def _raise_read(nc_path, var_name, bbox, p_res, agg_fun):
            raise ValueError("corrupt netcdf")

        monkeypatch.setattr(grid_pollution_merra2, "_read_nc_var", _raise_read)

        with pytest.raises(ValueError, match="extra|no.*data|Nenhum"):
            sus_grid_pollution_merra2(
                pollutants=["aod"], resolution="monthly", years=2020, months=[1],
                municipalities=muni, cache_dir=tmp_path, use_cache=False, verbose=False,
            )


class TestFileInfo:
    def test_daily_and_monthly_urls_are_identical_for_non_so2(self):
        """Preserved R quirk: .merra2_file_info()'s daily branch never points
        at the hourly M2T1NXAER collection; both resolutions build the exact
        same M2TMNXAER filename/URL."""
        monthly = grid_pollution_merra2._merra2_file_info("pm25", "monthly", 2020, 1)
        daily = grid_pollution_merra2._merra2_file_info("pm25", "daily", 2020, 1)
        assert monthly == daily

    def test_so2_uses_different_collection(self):
        so2 = grid_pollution_merra2._merra2_file_info("so2", "monthly", 2020, 1)
        pm25 = grid_pollution_merra2._merra2_file_info("pm25", "monthly", 2020, 1)
        assert "M2I3NVAER" in so2[1]
        assert "M2TMNXAER" in pm25[1]

    @pytest.mark.parametrize(
        ("year", "expected_ver"),
        [(1991, "100"), (1992, "200"), (2000, "200"), (2001, "300"), (2010, "300"), (2011, "400")],
    )
    def test_version_by_year(self, year, expected_ver):
        assert grid_pollution_merra2._merra2_version(year) == expected_ver


class TestMergePollutantFrames:
    def test_concat_within_pollutant_then_outer_merge_across(self):
        """The R source's literal per-manifest-row full_join would instead
        suffix same-pollutant columns across different months — this port
        deliberately concats within a pollutant before merging across
        pollutants (see IDEIAS.md)."""
        frames_by_pollutant = {
            "pm25": [
                pd.DataFrame({
                    "code_muni": ["A", "B"], "date": [pd.Timestamp("2020-01-01")] * 2,
                    "pm25_merra2": [1.0, 2.0],
                }),
                pd.DataFrame({
                    "code_muni": ["A", "B"], "date": [pd.Timestamp("2020-02-01")] * 2,
                    "pm25_merra2": [3.0, 4.0],
                }),
            ],
            "aod": [
                pd.DataFrame({
                    "code_muni": ["A", "B"], "date": [pd.Timestamp("2020-01-01")] * 2,
                    "aod_merra2": [0.1, 0.2],
                }),
            ],
        }
        result = grid_pollution_merra2._merge_pollutant_frames(
            frames_by_pollutant, ["pm25", "aod"], grid_pollution_merra2._MESSAGES["en"]
        )
        assert set(result.columns) == {"code_muni", "date", "pm25_merra2", "aod_merra2"}
        assert len(result) == 4  # 2 municipalities x 2 months, outer-joined
        jan = pd.Timestamp("2020-01-01")
        feb = pd.Timestamp("2020-02-01")
        jan_a = result[(result["code_muni"] == "A") & (result["date"] == jan)]
        assert jan_a["pm25_merra2"].iloc[0] == 1.0
        assert jan_a["aod_merra2"].iloc[0] == 0.1
        feb_a = result[(result["code_muni"] == "A") & (result["date"] == feb)]
        assert feb_a["pm25_merra2"].iloc[0] == 3.0
        assert pd.isna(feb_a["aod_merra2"].iloc[0])  # no AOD row for February

    def test_raises_when_nothing_extracted(self):
        with pytest.raises(ValueError):
            grid_pollution_merra2._merge_pollutant_frames(
                {"pm25": []}, ["pm25"], grid_pollution_merra2._MESSAGES["en"]
            )


class TestRasterZonalStatsIntegration:
    """Real rioxarray/xarray + exactextract path — gated on both being installed."""

    def _make_synthetic_nc(self, tmp_path):
        import xarray as xr

        data = np.full((1, 5, 5), 2.0e-9, dtype="float64")
        da = xr.DataArray(
            data,
            dims=("time", "lat", "lon"),
            coords={
                "time": [pd.Timestamp("2020-01-01")],
                "lat": np.linspace(-4, 4, 5),
                "lon": np.linspace(-4, 4, 5),
            },
            name="TOTEXTTAU",
        )
        ds = da.to_dataset()
        nc_path = tmp_path / "synthetic.nc4"
        ds.to_netcdf(nc_path)
        return nc_path

    def test_read_nc_var_monthly_takes_single_time_layer(self, tmp_path):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        pytest.importorskip("xarray", reason="xarray not installed")
        nc_path = self._make_synthetic_nc(tmp_path)
        da = grid_pollution_merra2._read_nc_var(nc_path, "TOTEXTTAU", None, "monthly", "mean")
        assert "time" not in da.dims
        assert float(da.mean()) == pytest.approx(2.0e-9)

    @pytest.mark.parametrize("agg_fun", grid_pollution_merra2._VALID_AGG)
    def test_zonal_stats_picks_correct_column_per_agg_fun(self, tmp_path, agg_fun):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        pytest.importorskip("xarray", reason="xarray not installed")
        pytest.importorskip("exactextract", reason="exactextract not installed")
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        nc_path = self._make_synthetic_nc(tmp_path)
        da = grid_pollution_merra2._read_nc_var(nc_path, "TOTEXTTAU", None, "monthly", agg_fun)

        muni = gpd.GeoDataFrame(
            {"code_muni": ["0000001"]}, geometry=[box(-2, -2, 2, 2)], crs="EPSG:4326"
        )
        values = grid_pollution_merra2._zonal_stats(da, muni, agg_fun)
        assert len(values) == 1
        assert np.isfinite(values[0])
        if agg_fun != "sum":
            assert values[0] == pytest.approx(2.0e-9, rel=1e-3)
        else:
            assert values[0] > 0
