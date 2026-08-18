"""Tests for sus_grid_pollution_ghap."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment import grid_pollution_ghap
from climasus4py.enrichment.grid_pollution_ghap import sus_grid_pollution_ghap


def _fake_download_writes_placeholder(url, cache_path, use_cache, verbose, msg):
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not-a-real-netcdf")


def _make_fake_extract_row(value: float = 5.0):
    def _fn(row, municipalities, agg_fun, bbox, verbose, msg):
        out_col = f"{row.pollutant}_{agg_fun}"
        mo = int(row.month) if row.month else 1
        return pd.DataFrame({
            "code_muni": municipalities["code_muni"].to_numpy(),
            "date": pd.Timestamp(year=int(row.year), month=mo, day=1),
            out_col: np.full(len(municipalities), value, dtype="float64"),
        })
    return _fn


def _make_municipalities():
    gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
    from shapely.geometry import box

    return gpd.GeoDataFrame(
        {"code_muni": ["3106200", "5103403"]},
        geometry=[box(0, 0, 1, 1), box(1, 1, 2, 2)],
        crs="EPSG:4326",
    )


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_pollution_ghap(lang="fr", verbose=False)

    def test_invalid_pollutant_raises(self):
        with pytest.raises(ValueError, match="pollutants"):
            sus_grid_pollution_ghap(pollutants="pm10", verbose=False)

    def test_no2_only_raises_no_pollutants(self):
        with pytest.raises(ValueError, match="poluente"):
            sus_grid_pollution_ghap(pollutants="no2", verbose=False)

    def test_mixed_case_no2_still_errors(self):
        """R quirk: detection is case-insensitive but removal is not, so a
        mixed-case 'No2' warns AND then fails the invalid-pollutants check."""
        with pytest.raises(ValueError, match="pollutants"):
            sus_grid_pollution_ghap(pollutants="No2", verbose=False)

    def test_all_expands_to_three_pollutants(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pollution_ghap(
            pollutants="all", resolution="annual", years=2020, cache_dir=tmp_path, verbose=False
        )
        assert isinstance(result, dict)
        assert {k.split("_")[0] for k in result} == {"pm25", "o3", "co"}

    def test_invalid_resolution_raises(self):
        with pytest.raises(ValueError, match="resolution"):
            sus_grid_pollution_ghap(resolution="weekly", verbose=False)

    def test_invalid_months_raises(self):
        with pytest.raises(ValueError, match="months"):
            sus_grid_pollution_ghap(years=2020, months=[0, 13], verbose=False)

    def test_invalid_agg_fun_raises(self):
        with pytest.raises(ValueError, match="agg_fun"):
            sus_grid_pollution_ghap(years=2020, agg_fun="bogus", verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_pollution_ghap(years=2020, use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_pollution_ghap(years=2020, cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_pollution_ghap(
                years=2020, municipalities=pd.DataFrame({"code_muni": ["12345"]}), verbose=False
            )

    def test_years_outside_range_skips_pollutant(self):
        with pytest.raises(ValueError, match="Nenhum dado"):
            sus_grid_pollution_ghap(pollutants="pm25", years=1990, verbose=False)


class TestPathsOnlyMode:
    """municipalities=None: no geopandas/rioxarray/exactextract needed at all."""

    def test_returns_dict_of_paths(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pollution_ghap(
            resolution="annual",
            years=[2020, 2021],
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )
        assert isinstance(result, dict)
        assert len(result) == 2
        for key, path in result.items():
            assert key.startswith("pm25_")
            assert Path(path).is_file()

    def test_annual_only_pollutant_falls_back_from_monthly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        result = sus_grid_pollution_ghap(
            pollutants="o3",
            resolution="monthly",  # must fall back to annual
            years=2015,
            cache_dir=tmp_path,
            verbose=False,
        )
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "o3_2015_annual" in result

    def test_daily_year_outside_available_range_raises_no_data(self, tmp_path):
        """2016 is pre-filtered by _AVAIL_YEARS (2017-2022), so this hits the
        'no data for these parameters' error before the daily-ZIP branch —
        the record-lookup ValueError inside _file_info is unreachable for
        any valid pollutant since its record-id table's keys always match
        _AVAIL_YEARS exactly."""
        with pytest.raises(ValueError, match="Nenhum dado"):
            sus_grid_pollution_ghap(
                pollutants="pm25", resolution="daily", years=2016,
                cache_dir=tmp_path, verbose=False,
            )


class TestHappyPathWithMunicipalities:
    def test_monthly_aggregation_shape_and_values(self, tmp_path, monkeypatch):
        muni = _make_municipalities()
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(
            grid_pollution_ghap, "_extract_manifest_row", _make_fake_extract_row(42.0)
        )

        result = sus_grid_pollution_ghap(
            pollutants="pm25",
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
        assert set(result.columns) == {"code_muni", "date", "pm25_mean"}
        assert len(result) == 4  # 2 municipalities x 2 months
        assert (result["pm25_mean"] == 42.0).all()

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "pollution_ghap"
        assert meta["resolution"] == "monthly"
        assert meta["temporal"]["unit"] == "month"
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 4

        written = list((Path(tmp_path) / "pm25" / "parquet").glob("*.parquet"))
        assert len(written) == 2  # one per (year, month)

    def test_annual_unit_metadata_is_year_not_ternary_bug(self, tmp_path, monkeypatch):
        """The R source's temporal.unit ternary mislabels annual as 'year'
        only via an accidental else-branch; confirm the Python fix keeps
        annual -> 'year' and (separately) daily -> 'day'."""
        muni = _make_municipalities()
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(
            grid_pollution_ghap, "_extract_manifest_row", _make_fake_extract_row(1.0)
        )
        result = sus_grid_pollution_ghap(
            pollutants="co", resolution="annual", years=2020,
            municipalities=muni, cache_dir=tmp_path, use_cache=False, verbose=False,
        )
        assert result.attrs["sus_meta"]["temporal"]["unit"] == "year"

    def test_multi_pollutant_merges_into_wide_columns(self, tmp_path, monkeypatch):
        """R's do.call(rbind, ...) would error on mismatched pollutant
        columns; the Python port merges into one wide row per muni/date
        instead — see IDEIAS.md."""
        muni = _make_municipalities()
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(
            grid_pollution_ghap, "_extract_manifest_row", _make_fake_extract_row(7.0)
        )
        result = sus_grid_pollution_ghap(
            pollutants=["pm25", "co"], resolution="annual", years=2020,
            municipalities=muni, cache_dir=tmp_path, use_cache=False, verbose=False,
        )
        assert set(result.columns) == {"code_muni", "date", "pm25_mean", "co_mean"}
        assert len(result) == 2  # one row per municipality, both pollutant cols populated
        assert (result["pm25_mean"] == 7.0).all()
        assert (result["co_mean"] == 7.0).all()

    def test_mixed_resolution_manifest_pm25_monthly_plus_o3_annual_fallback(
        self, tmp_path, monkeypatch
    ):
        """pm25 keeps resolution='monthly' (month='01' string) while o3
        falls back to annual (month=None) in the same manifest — confirms
        the None/"01" mix flows through _extract_manifest_row's
        int(row.month) if row.month else 1 without error."""
        muni = _make_municipalities()
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(
            grid_pollution_ghap, "_extract_manifest_row", _make_fake_extract_row(3.0)
        )
        result = sus_grid_pollution_ghap(
            pollutants=["pm25", "o3"], resolution="monthly", years=2020, months=[1],
            municipalities=muni, cache_dir=tmp_path, use_cache=False, verbose=False,
        )
        assert set(result.columns) == {"code_muni", "date", "pm25_mean", "o3_mean"}
        assert len(result) == 2

    def test_missing_raster_package_fails_before_any_download(self, tmp_path, monkeypatch):
        muni = _make_municipalities()

        def _boom(*args, **kwargs):
            raise AssertionError("download must not run before the package pre-flight check")

        monkeypatch.setattr(grid_pollution_ghap, "_download_file", _boom)
        monkeypatch.setattr(
            grid_pollution_ghap, "find_spec", lambda name: None if name == "rioxarray" else object()
        )

        with pytest.raises(ImportError, match="rioxarray"):
            sus_grid_pollution_ghap(
                years=2020, months=[1], municipalities=muni, cache_dir=tmp_path, verbose=False
            )

    def test_parquet_cache_hit_skips_download_entirely(self, tmp_path, monkeypatch):
        muni = _make_municipalities()

        def _boom(*args, **kwargs):
            raise AssertionError("should not be called: Parquet cache should short-circuit")

        monkeypatch.setattr(grid_pollution_ghap, "_download_file", _boom)
        monkeypatch.setattr(grid_pollution_ghap, "_extract_manifest_row", _boom)

        pq_dir = Path(tmp_path) / "pm25" / "parquet"
        pq_dir.mkdir(parents=True)
        cached = pd.DataFrame({
            "code_muni": ["3106200", "5103403"],
            "date": [pd.Timestamp("2020-01-01")] * 2,
            "pm25_mean": [10.0, 20.0],
        })
        import hashlib

        muni_hash = "_" + hashlib.md5(
            "|".join(sorted(muni["code_muni"])).encode("utf-8")
        ).hexdigest()[:10]
        cached.to_parquet(pq_dir / f"pm25_202001{muni_hash}.parquet", index=False)

        result = sus_grid_pollution_ghap(
            pollutants="pm25",
            resolution="monthly",
            years=2020,
            months=[1],
            municipalities=muni,
            cache_dir=tmp_path,
            use_cache=True,
            verbose=False,
        )
        assert len(result) == 2
        assert set(result["pm25_mean"]) == {10.0, 20.0}

        meta = result.attrs["sus_meta"]
        assert meta["temporal"]["source"] == "zenodo_ghap_cache"
        assert "resolution" not in meta  # preserved R quirk: slimmer meta on cache path

    def test_extraction_error_is_caught_and_skipped_not_raised(self, tmp_path, monkeypatch):
        """Exercises the real try/except inside _extract_manifest_row (not
        just a stand-in that returns None directly) — a raster-read error
        must be caught, warned about, and skipped, leaving no usable rows
        and thus surfacing as the 'no data extracted' ValueError."""
        muni = _make_municipalities()
        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )

        def _raise_read(nc_path, municipalities, bbox, agg_fun, out_col, yr, month):
            raise ValueError("corrupt raster")

        monkeypatch.setattr(grid_pollution_ghap, "_extract_grid_file", _raise_read)

        with pytest.raises(ValueError, match="extra|dado"):
            sus_grid_pollution_ghap(
                resolution="monthly",
                years=2020,
                months=[1],
                municipalities=muni,
                cache_dir=tmp_path,
                use_cache=False,
                verbose=False,
            )

    def test_daily_zip_branch_dispatches_by_extension(self, tmp_path, monkeypatch):
        """resolution='daily' manifest rows point at .zip files; confirm the
        dispatcher in _extract_manifest_row picks the ZIP branch, not the
        NetCDF branch, without needing a real ZIP (mocked at the ZIP-extract
        helper level)."""
        muni = _make_municipalities()

        def _fake_zip_extract(
            zip_path, municipalities, bbox, agg_fun, out_col, yr, month, verbose, msg
        ):
            return pd.DataFrame({
                "code_muni": municipalities["code_muni"].to_numpy(),
                "date": pd.Timestamp(year=yr, month=int(month), day=15),
                out_col: 99.0,
            })

        def _grid_boom(*args, **kwargs):
            raise AssertionError("must not use the NetCDF branch for a .zip manifest row")

        monkeypatch.setattr(
            grid_pollution_ghap, "_download_file", _fake_download_writes_placeholder
        )
        monkeypatch.setattr(grid_pollution_ghap, "_extract_daily_zip", _fake_zip_extract)
        monkeypatch.setattr(grid_pollution_ghap, "_extract_grid_file", _grid_boom)

        result = sus_grid_pollution_ghap(
            pollutants="pm25", resolution="daily", years=2020, months=[1],
            municipalities=muni, cache_dir=tmp_path, use_cache=False, verbose=False,
        )
        assert (result["pm25_mean"] == 99.0).all()


class TestPixelBboxMath:
    """Exact numeric check against the R source's .ghap_to_pixel_bbox()."""

    def test_full_globe(self):
        assert grid_pollution_ghap._to_pixel_bbox(-180, 180, -90, 90) == (0, 36000, 0, 18000)

    def test_brazil_bbox(self):
        # xmin=-75, xmax=-28, ymin=-35, ymax=6
        px = grid_pollution_ghap._to_pixel_bbox(-75, -28, -35, 6)
        assert px == (
            round((-75 + 180) * 100),
            round((-28 + 180) * 100),
            round((90 - 6) * 100),
            round((90 - (-35)) * 100),
        )


class TestRasterZonalStatsIntegration:
    """Real rioxarray + exactextract path — gated on both being installed."""

    # Bbox used by both tests below: (-180, -179.8, -90, -89.8) as
    # (xmin, xmax, ymin, ymax) -> pixel window x:[0,20), y:[17980,18000)
    # via _to_pixel_bbox — the synthetic raster's own pixel-index coords
    # must cover that exact window for clip_box() to find data in bounds.
    _BBOX = (-180.0, -179.8, -90.0, -89.8)

    def _make_synthetic_pixel_grid_netcdf(self, tmp_path):
        """A tiny 'GHAP-shaped' raster: pixel-index coords, no CRS, single band."""
        import rioxarray  # noqa: F401
        import xarray as xr

        nx, ny = 20, 20
        data = np.full((1, ny, nx), 12.0, dtype="float64")
        da = xr.DataArray(
            data,
            dims=("band", "y", "x"),
            coords={
                "band": [1],
                "y": np.arange(17980, 18000),
                "x": np.arange(0, 20),
            },
        )
        nc_path = tmp_path / "synthetic_ghap.nc"
        da.rio.to_raster(nc_path)
        return nc_path

    def test_read_and_fix_reprojects_to_wgs84(self, tmp_path):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        nc_path = self._make_synthetic_pixel_grid_netcdf(tmp_path)
        raster = grid_pollution_ghap._read_and_fix(nc_path, *self._BBOX)
        assert raster.rio.crs is not None
        assert raster.rio.crs.to_epsg() == 4326

    def test_zonal_stats_with_exactextract(self, tmp_path):
        pytest.importorskip("rioxarray", reason="rioxarray not installed")
        pytest.importorskip("exactextract", reason="exactextract not installed")
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        nc_path = self._make_synthetic_pixel_grid_netcdf(tmp_path)
        raster = grid_pollution_ghap._read_and_fix(nc_path, *self._BBOX)
        raster2d = raster.squeeze("band", drop=True)

        muni = gpd.GeoDataFrame(
            {"code_muni": ["0000001"]},
            geometry=[box(-180, -90, -179.8, -89.8)],
            crs="EPSG:4326",
        )
        values = grid_pollution_ghap._zonal_stats(raster2d, muni, "mean")
        assert len(values) == 1
        assert values[0] == pytest.approx(12.0, abs=1e-6)
