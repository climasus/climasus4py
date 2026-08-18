"""Tests for sus_grid_era5.

Strategy: validation-only tests run in the base environment (they all
raise before the module needs rioxarray/xarray/exactextract/geopandas).
Functional tests (download-only and municipality-aggregation happy
paths) are gated with ``pytest.importorskip`` and monkeypatch the
network/raster internals (``_era5_download_file`` /
``_era5_extract_monthly``) so no real download or NetCDF file is needed.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.grid_era5 import (
    _ERA5_VAR_MAP,
    _VALID_AGG_FUN,
    sus_grid_era5,
)

_DL_TARGET = "climasus4py.enrichment.grid_era5._era5_download_file"
_EXTRACT_TARGET = "climasus4py.enrichment.grid_era5._era5_extract_monthly"


def _has_geopandas() -> bool:
    try:
        import geopandas  # noqa: F401
        return True
    except ImportError:
        return False


class TestValidation:
    """All of these raise before the unconditional rioxarray/xarray check,
    so they must pass with or without the raster extras installed."""

    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="'lang'"):
            sus_grid_era5(years=2020, lang="fr", verbose=False)

    def test_missing_years_raises(self):
        with pytest.raises(ValueError):
            sus_grid_era5(years=None, verbose=False)

    def test_invalid_years_range_raises(self):
        with pytest.raises(ValueError, match="1949|Invalid|inválido|inválido"):
            sus_grid_era5(years=1949, verbose=False)

    def test_invalid_months_raises(self):
        with pytest.raises(ValueError, match="months"):
            sus_grid_era5(years=2020, months=[0, 13], verbose=False)

    def test_invalid_vars_raises(self):
        with pytest.raises(ValueError, match="vars"):
            sus_grid_era5(years=2020, vars=["not_a_var"], verbose=False)

    def test_invalid_agg_fun_raises(self):
        with pytest.raises(ValueError, match="agg_fun"):
            sus_grid_era5(years=2020, agg_fun="bogus", verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_era5(years=2020, use_cache="yes", verbose=False)

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_era5(years=2020, cache_dir="", verbose=False)

    def test_invalid_parallel_raises(self):
        with pytest.raises(ValueError, match="parallel"):
            sus_grid_era5(years=2020, parallel="yes", verbose=False)

    def test_invalid_workers_raises(self):
        with pytest.raises(ValueError, match="workers"):
            sus_grid_era5(years=2020, workers=0, verbose=False)

    def test_municipalities_not_gdf_raises(self):
        if not _has_geopandas():
            pytest.skip("requires geopandas installed to reach the isinstance check")
        with pytest.raises(ValueError, match="GeoDataFrame"):
            sus_grid_era5(years=2020, municipalities=pd.DataFrame({"code_muni": ["310010"]}))

    def test_missing_geopandas_raises_friendly_error(self, monkeypatch):
        if _has_geopandas():
            pytest.skip("geopandas is installed — cannot test the missing-dependency path")
        with pytest.raises(ImportError, match="geopandas"):
            sus_grid_era5(years=2020, municipalities=pd.DataFrame({"code_muni": ["310010"]}))


def test_missing_raster_libs_raises_friendly_error(monkeypatch):
    """Simulate rioxarray/xarray being unavailable even if actually installed,
    by making the module's import fail (mirrors the base-env scenario)."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name in ("rioxarray", "xarray"):
            raise ImportError(f"No module named '{name}'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="rioxarray"):
        sus_grid_era5(years=2020, months=[1], vars=["t2m"], verbose=False)


class TestConstants:
    def test_valid_agg_fun(self):
        assert _VALID_AGG_FUN == (
            "mean", "sum", "median", "min", "max", "majority", "minority", "count", "variety",
        )

    def test_var_map_aliases(self):
        assert set(_ERA5_VAR_MAP) == {
            "t2m", "t2m_max", "t2m_min", "td2m", "u10", "v10", "sp", "tp",
        }

    def test_temperature_conversion_k_to_c(self):
        assert _ERA5_VAR_MAP["t2m"]["conv"](273.15) == 0.0

    def test_precipitation_conversion_m_to_mm(self):
        assert _ERA5_VAR_MAP["tp"]["conv"](0.01) == pytest.approx(10.0)

    def test_pressure_conversion_pa_to_hpa(self):
        assert _ERA5_VAR_MAP["sp"]["conv"](101300) == pytest.approx(1013.0)


# ---------------------------------------------------------------------------
# Functional tests: gated behind rioxarray/xarray/exactextract/geopandas.
# Module-level importorskip would skip the whole file (including the
# validation tests above, which must run in the base env) — so the skip
# is applied per-class via setup_method instead, mirroring
# TestComputation in test_climate_spi.py.
# ---------------------------------------------------------------------------

def _require_raster_extras():
    pytest.importorskip("rioxarray", reason="rioxarray not installed")
    pytest.importorskip("xarray", reason="xarray not installed")
    pytest.importorskip("exactextract", reason="exactextract not installed")
    return pytest.importorskip("geopandas", reason="geopandas not installed")


def _make_municipalities():
    gpd = _require_raster_extras()
    from shapely.geometry import box

    return gpd.GeoDataFrame(
        {"code_muni": ["3106200", "3550308"]},
        geometry=[box(-45.0, -20.0, -44.0, -19.0), box(-47.0, -24.0, -46.0, -23.0)],
        crs="EPSG:4326",
    )


class TestDownloadOnly:
    def setup_method(self):
        _require_raster_extras()

    def test_returns_dict_of_paths_keyed_by_indicator(self, monkeypatch, tmp_path):
        def fake_download(url, cache_path, use_cache, verbose, msg):
            return str(cache_path)

        monkeypatch.setattr(_DL_TARGET, fake_download)
        result = sus_grid_era5(
            years=2020, months=[1], vars=["t2m"], municipalities=None,
            cache_dir=tmp_path, verbose=False,
        )
        assert isinstance(result, dict)
        assert list(result.keys()) == ["2020_01_2m_temperature_mean"]
        assert result["2020_01_2m_temperature_mean"] is not None

    def test_max_and_min_are_separate_files(self, monkeypatch, tmp_path):
        """t2m_max/t2m_min differ in agg_label, which is embedded in the
        Zenodo filename -> they are two distinct files, not deduped
        (the R source's own comment about this dedup is misleading, see
        IDEIAS.md)."""
        calls = []

        def fake_download(url, cache_path, use_cache, verbose, msg):
            calls.append(url)
            return str(cache_path)

        monkeypatch.setattr(_DL_TARGET, fake_download)
        result = sus_grid_era5(
            years=2020, months=[1], vars=["t2m_max", "t2m_min"], municipalities=None,
            cache_dir=tmp_path, verbose=False,
        )
        assert len(calls) == 2
        assert set(result.keys()) == {"2020_01_2m_temperature_max", "2020_01_2m_temperature_min"}

    def test_repeated_alias_is_deduped(self, monkeypatch, tmp_path):
        """The dedup that does happen: an exact duplicate (year, month,
        indicator, agg_label) row only triggers one download."""
        calls = []

        def fake_download(url, cache_path, use_cache, verbose, msg):
            calls.append(url)
            return str(cache_path)

        monkeypatch.setattr(_DL_TARGET, fake_download)
        result = sus_grid_era5(
            years=2020, months=[1], vars=["t2m", "t2m"], municipalities=None,
            cache_dir=tmp_path, verbose=False,
        )
        assert len(calls) == 1
        assert list(result.keys()) == ["2020_01_2m_temperature_mean"]

    def test_vars_all_expands_to_every_alias_in_order(self, monkeypatch, tmp_path):
        monkeypatch.setattr(_DL_TARGET, lambda url, cache_path, *a, **k: str(cache_path))
        result = sus_grid_era5(
            years=2020, months=[1], vars="all", municipalities=None,
            cache_dir=tmp_path, verbose=False,
        )
        # All 8 aliases have a distinct (indicator, agg_label) pair, so
        # each maps to its own file (see test_max_and_min_are_separate_files).
        assert len(result) == 8
        assert "2020_01_total_precipitation_sum" in result

    def test_failed_download_keeps_none_entry(self, monkeypatch, tmp_path):
        monkeypatch.setattr(_DL_TARGET, lambda *a, **k: None)
        result = sus_grid_era5(
            years=2020, months=[1], vars=["t2m"], municipalities=None,
            cache_dir=tmp_path, verbose=False,
        )
        assert result["2020_01_2m_temperature_mean"] is None


class TestAggregationHappyPath:
    def setup_method(self):
        _require_raster_extras()

    def test_merges_multiple_vars_and_sets_metadata(self, monkeypatch, tmp_path):
        gdf = _make_municipalities()
        monkeypatch.setattr(_DL_TARGET, lambda url, cache_path, *a, **k: str(cache_path))

        def fake_extract(nc_path, municipalities, agg_fun, out_col, conv_fn, start_date):
            return pd.DataFrame({
                "code_muni": list(municipalities["code_muni"]),
                "date": [pd.Timestamp(start_date)] * len(municipalities),
                out_col: conv_fn(pd.Series([300.0, 305.0])),
            })

        monkeypatch.setattr(_EXTRACT_TARGET, fake_extract)

        df = sus_grid_era5(
            years=2020, months=[1], vars=["t2m", "tp"], municipalities=gdf,
            cache_dir=tmp_path, verbose=False,
        )

        assert set(df.columns) >= {"code_muni", "date", "tair_dry_bulb_c", "rainfall_mm"}
        assert len(df) == 2

        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "era5_land"
        assert meta["agg_fun"] == "mean"
        assert meta["n_municipalities"] == 2
        assert meta["doi"] == "10.5281/zenodo.10013254"
        assert meta["vars"] == ["t2m", "tp"]

    def test_detects_alternate_muni_id_column(self, monkeypatch, tmp_path):
        gpd = _require_raster_extras()
        from shapely.geometry import box

        gdf = gpd.GeoDataFrame(
            {"CD_MUN": ["3106200"]},
            geometry=[box(-45.0, -20.0, -44.0, -19.0)],
            crs="EPSG:4326",
        )
        monkeypatch.setattr(_DL_TARGET, lambda url, cache_path, *a, **k: str(cache_path))

        def fake_extract(nc_path, municipalities, agg_fun, out_col, conv_fn, start_date):
            assert "code_muni" in municipalities.columns
            return pd.DataFrame({
                "code_muni": list(municipalities["code_muni"]),
                "date": [pd.Timestamp(start_date)] * len(municipalities),
                out_col: [0.0],
            })

        monkeypatch.setattr(_EXTRACT_TARGET, fake_extract)
        df = sus_grid_era5(
            years=2020, months=[1], vars=["t2m"], municipalities=gdf,
            cache_dir=tmp_path, verbose=False,
        )
        assert df["code_muni"].iloc[0] == "310620"

    def test_no_data_extracted_raises(self, monkeypatch, tmp_path):
        gdf = _make_municipalities()
        monkeypatch.setattr(_DL_TARGET, lambda *a, **k: None)  # all downloads fail
        with pytest.raises(ValueError, match="No data|extraído|extrajo"):
            sus_grid_era5(
                years=2020, months=[1], vars=["t2m"], municipalities=gdf,
                cache_dir=tmp_path, verbose=False,
            )
