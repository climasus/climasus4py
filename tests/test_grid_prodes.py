"""Tests for sus_grid_prodes."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from climasus4py.enrichment import grid_prodes
from climasus4py.enrichment.grid_prodes import sus_grid_prodes


def _make_defor_gdf(gpd, box, year, n=2):
    """Synthetic deforestation polygons, mimicking one WFS response."""
    return gpd.GeoDataFrame(
        {
            "year": [year] * n,
            "state": ["MT"] * n,
            "uid": [f"u{year}_{i}" for i in range(n)],
        },
        geometry=[box(i, i, i + 1, i + 1) for i in range(n)],
        crs="EPSG:4326",
    )


def _make_municipalities(gpd, box):
    return gpd.GeoDataFrame(
        {"code_muni": ["5103403", "5106224"]},
        geometry=[box(-0.5, -0.5, 1.5, 1.5), box(10, 10, 11, 11)],
        crs="EPSG:4326",
    )


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_prodes(years=2020, lang="fr", verbose=False)

    def test_missing_years_raises(self):
        with pytest.raises(ValueError):
            sus_grid_prodes(years=None, verbose=False)  # type: ignore[arg-type]

    def test_invalid_years_type_raises(self):
        with pytest.raises(ValueError):
            sus_grid_prodes(years=["abc"], verbose=False)  # type: ignore[list-item]

    def test_invalid_years_range_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_prodes(years=1900, verbose=False)

    def test_invalid_biomes_raises(self):
        with pytest.raises(ValueError, match="biomes"):
            sus_grid_prodes(years=2020, biomes=["NotABiome"], verbose=False)

    def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="uf"):
            sus_grid_prodes(years=2020, uf="ZZ", verbose=False)

    def test_invalid_use_cache_raises(self):
        # geopandas is required unconditionally (mirrors R's unconditional
        # check_installed("sf")) and is checked before use_cache/cache_dir,
        # same order as the R source's validation block.
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_prodes(years=2020, use_cache="yes", verbose=False)  # type: ignore[arg-type]

    def test_invalid_cache_dir_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_prodes(years=2020, cache_dir="   ", verbose=False)

    def test_municipalities_not_geodataframe_raises(self):
        pytest.importorskip("geopandas", reason="geopandas not installed")
        with pytest.raises(ValueError, match="municipalities"):
            sus_grid_prodes(
                years=2020,
                municipalities=pd.DataFrame({"code_muni": ["12345"]}),
                verbose=False,
            )

    def test_all_years_before_biome_start_raises(self):
        # Amazon starts 2007; requesting 2001 with only Amazon leaves no data.
        with pytest.raises(ValueError, match="parâmetros|parameters|parámetros"):
            sus_grid_prodes(years=2001, biomes=["Amazon"], verbose=False)


class TestRawPolygonMode:
    """municipalities=None: raw deforestation polygons returned, no aggregation."""

    def test_returns_raw_dataframe(self, tmp_path, monkeypatch):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        def _fake_fetch_wfs(biome, year, cache_path, use_cache, verbose, msg):
            return _make_defor_gdf(gpd, box, year)

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _fake_fetch_wfs)

        result = sus_grid_prodes(
            years=2020, biomes=["Amazon"], cache_dir=tmp_path, use_cache=False, verbose=False
        )
        assert isinstance(result, pd.DataFrame)
        # Mirrors R's intersect(names(defor_sf), c("year","state","area_km","biome","uid")):
        # "uid" is kept when present in the source layer (the synthetic fixture
        # includes it), on top of the doc-listed year/state/area_km/biome.
        assert {"year", "state", "area_km", "biome"} <= set(result.columns)
        assert len(result) == 2
        assert (result["biome"] == "Amazon").all()

    def test_missing_year_data_is_skipped_not_raised(self, tmp_path, monkeypatch):
        pytest.importorskip("geopandas", reason="geopandas not installed")

        def _fake_fetch_wfs(biome, year, cache_path, use_cache, verbose, msg):
            return None

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _fake_fetch_wfs)
        with pytest.raises(ValueError, match="no_data|dado|data"):
            sus_grid_prodes(
                years=2020, biomes=["Amazon"], cache_dir=tmp_path, use_cache=False, verbose=False
            )


class TestHappyPathWithMunicipalities:
    def test_aggregation_shape_and_metadata(self, tmp_path, monkeypatch):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        muni = _make_municipalities(gpd, box)

        def _fake_fetch_wfs(biome, year, cache_path, use_cache, verbose, msg):
            return _make_defor_gdf(gpd, box, year)

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _fake_fetch_wfs)

        result = sus_grid_prodes(
            years=2020,
            biomes=["Amazon"],
            municipalities=muni,
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )

        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {
            "code_muni", "date", "year", "deforested_area_km2", "n_patches", "biome",
        }
        # Only the first municipality's box overlaps the synthetic polygons.
        assert set(result["code_muni"]) == {"5103403"}
        assert (result["n_patches"] > 0).all()
        assert (result["deforested_area_km2"] > 0).all()

        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "prodes"
        assert meta["temporal"]["unit"] == "year"
        assert meta["temporal"]["source"] == "terrabrasilis_inpe_prodes"
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == len(result)

        # Aggregated Parquet cache actually gets written to disk.
        written = list((Path(tmp_path) / "parquet").glob("*.parquet"))
        assert len(written) == 1

    def test_parquet_cache_key_includes_muni_hash(self, tmp_path, monkeypatch):
        """Correctness fix: cache filename must depend on the municipality set.

        The R source only tags the aggregated Parquet cache with `uf`
        (or "all"); two calls with the same `uf` but different
        `municipalities` would silently share (and thus corrupt) the
        cache. This locks in the Python-side fix (mirrors grid_chirps's
        muni_hash / the documented grid_pdsi bugfix).
        """
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        muni_a = _make_municipalities(gpd, box)
        muni_b = gpd.GeoDataFrame(
            {"code_muni": ["1100015"]},
            geometry=[box(-0.5, -0.5, 1.5, 1.5)],
            crs="EPSG:4326",
        )

        def _fake_fetch_wfs(biome, year, cache_path, use_cache, verbose, msg):
            return _make_defor_gdf(gpd, box, year)

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _fake_fetch_wfs)

        sus_grid_prodes(
            years=2020, biomes=["Amazon"], municipalities=muni_a,
            cache_dir=tmp_path, use_cache=True, verbose=False,
        )
        sus_grid_prodes(
            years=2020, biomes=["Amazon"], municipalities=muni_b,
            cache_dir=tmp_path, use_cache=True, verbose=False,
        )
        written = list((Path(tmp_path) / "parquet").glob("*.parquet"))
        assert len(written) == 2  # distinct cache files, not one reused/corrupted

    def test_empty_intersection_is_skipped_not_fatal(self, tmp_path, monkeypatch):
        """A biome/year with zero overlapping municipalities must not abort the run."""
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        muni = _make_municipalities(gpd, box)

        calls = {"n": 0}

        def _fake_fetch_wfs(biome, year, cache_path, use_cache, verbose, msg):
            calls["n"] += 1
            if year == 2019:
                # Far away from any municipality polygon -> empty intersection.
                return gpd.GeoDataFrame(
                    {"year": [2019], "state": ["MT"], "uid": ["far"]},
                    geometry=[box(100, 100, 101, 101)],
                    crs="EPSG:4326",
                )
            return _make_defor_gdf(gpd, box, year)

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _fake_fetch_wfs)

        result = sus_grid_prodes(
            years=[2019, 2020],
            biomes=["Amazon"],
            municipalities=muni,
            cache_dir=tmp_path,
            use_cache=False,
            verbose=False,
        )
        assert calls["n"] == 2
        assert set(result["year"]) == {2020}

    def test_parquet_cache_hit_skips_fetch(self, tmp_path, monkeypatch):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        muni = _make_municipalities(gpd, box)

        def _boom(*args, **kwargs):
            raise AssertionError("should not be called: Parquet cache should short-circuit")

        monkeypatch.setattr(grid_prodes, "_fetch_wfs", _boom)

        muni_col = grid_prodes._detect_muni_col(muni, grid_prodes._MESSAGES["pt"])
        import hashlib
        codes = sorted(str(c) for c in muni[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

        pq_dir = Path(tmp_path) / "parquet"
        pq_dir.mkdir(parents=True)
        cached = pd.DataFrame({
            "code_muni": ["5103403"],
            "date": [pd.Timestamp("2020-01-01")],
            "year": [2020],
            "deforested_area_km2": [12.5],
            "n_patches": [3],
            "biome": ["Amazon"],
        })
        cached.to_parquet(pq_dir / f"prodes_amazon_2020_all{muni_hash}.parquet", index=False)

        result = sus_grid_prodes(
            years=2020, biomes=["Amazon"], municipalities=muni,
            cache_dir=tmp_path, use_cache=True, verbose=False,
        )
        assert len(result) == 1
        assert result.iloc[0]["deforested_area_km2"] == 12.5

        meta = result.attrs["sus_meta"]
        # Preserved R quirk: the Parquet-cache early-return path builds a
        # slimmer sus_meta than the full-extraction path (no biomes/years/uf,
        # no temporal.unit, distinct temporal.source) — mirrors
        # .prodes_build_from_parquet in the R source exactly.
        assert meta["temporal"]["source"] == "terrabrasilis_cache"
        assert "unit" not in meta["temporal"]
        assert "biomes" not in meta


class TestGeodesicArea:
    def test_area_is_positive_and_reasonable(self):
        gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
        from shapely.geometry import box

        # Roughly a 1deg x 1deg box near the equator ~ 111km x 111km ~ 12321 km^2.
        gs = gpd.GeoSeries([box(0, 0, 1, 1)], crs="EPSG:4326")
        areas = grid_prodes._geodesic_area_km2(gs)
        assert areas.iloc[0] == pytest.approx(12321, rel=0.05)
