"""Tests for sus_grid_koppen.

Strategy: monkeypatch _load_municipio_meta to return a tiny synthetic
municipality centroid table (mirroring climasus_data's geo/municipios.json
schema after renaming), so the public API body (validation, rule-based
approx classification, factor coercion, metadata assembly) is exercised
without depending on the real climasus_data JSON file or any network I/O.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.grid_koppen import (
    _KOPPEN_LEVELS,
    _classify_one,
    sus_grid_koppen,
)

_PATCH_TARGET = "climasus4py.enrichment.grid_koppen._load_municipio_meta"


def _fake_meta() -> pd.DataFrame:
    """Synthetic municipality centroid table (code_muni_6, uf_code, lon, lat)."""
    return pd.DataFrame({
        "code_muni_6": ["260960", "431490", "150080", "355030"],
        "uf_code": [26, 43, 15, 35],  # PE (BSh belt), RS (South), PA (Amazon), SP
        "lon": [-38.0, -51.2, -48.5, -46.6],
        "lat": [-8.05, -30.03, -1.45, -23.55],
    })


@pytest.fixture()
def patched_meta(monkeypatch):
    meta = _fake_meta()
    monkeypatch.setattr(_PATCH_TARGET, lambda: meta)
    return meta


def _health_df() -> pd.DataFrame:
    return pd.DataFrame({
        "code_muni": ["2609600", "4314902", "1500800", "3550308", "9999999"],
        "value": [1, 2, 3, 4, 5],
    })


class TestValidation:
    def test_missing_code_muni_raises(self, patched_meta):
        df = pd.DataFrame({"other_col": [1, 2]})
        with pytest.raises(ValueError, match="code_muni"):
            sus_grid_koppen(df, verbose=False)

    def test_invalid_mode_raises(self, patched_meta):
        with pytest.raises(ValueError, match="'mode' must be one of"):
            sus_grid_koppen(_health_df(), mode="bogus", verbose=False)

    def test_exact_mode_without_koppen_sf_raises(self, patched_meta):
        pytest.importorskip("geopandas")
        with pytest.raises(ValueError, match="koppen_sf"):
            sus_grid_koppen(_health_df(), mode="exact", verbose=False)

    def test_unsupported_lang_falls_back_to_pt(self, patched_meta):
        # Should not raise -- falls back to 'pt' with a warning printed.
        df = sus_grid_koppen(_health_df(), lang="fr", verbose=False)
        assert "zona_koppen" in df.columns


class TestHappyPathApprox:
    def test_adds_zona_koppen_column(self, patched_meta):
        df = sus_grid_koppen(_health_df(), mode="approx", verbose=False)
        assert "zona_koppen" in df.columns
        assert "code_muni_6" not in df.columns
        assert len(df) == 5

    def test_unmatched_municipality_is_na(self, patched_meta):
        df = sus_grid_koppen(_health_df(), mode="approx", as_factor=False, verbose=False)
        row = df[df["code_muni"] == "9999999"]
        assert row["zona_koppen"].isna().all()

    def test_matched_municipalities_get_known_zones(self, patched_meta):
        df = sus_grid_koppen(_health_df(), mode="approx", as_factor=False, verbose=False)
        by_code = df.set_index("code_muni")["zona_koppen"]
        assert by_code["2609600"] == _classify_one(26, -38.0, -8.05)
        assert by_code["4314902"] == _classify_one(43, -51.2, -30.03)
        assert by_code["1500800"] == _classify_one(15, -48.5, -1.45)
        assert by_code["3550308"] == _classify_one(35, -46.6, -23.55)

    def test_as_factor_true_gives_ordered_categorical(self, patched_meta):
        df = sus_grid_koppen(_health_df(), mode="approx", as_factor=True, verbose=False)
        assert isinstance(df["zona_koppen"].dtype, pd.CategoricalDtype)
        assert df["zona_koppen"].cat.ordered
        assert list(df["zona_koppen"].cat.categories) == list(_KOPPEN_LEVELS)

    def test_as_factor_false_gives_plain_strings(self, patched_meta):
        df = sus_grid_koppen(_health_df(), mode="approx", as_factor=False, verbose=False)
        assert not isinstance(df["zona_koppen"].dtype, pd.CategoricalDtype)
        matched = df["zona_koppen"].dropna()
        assert matched.map(type).eq(str).all()

    def test_preserves_original_columns(self, patched_meta):
        df = sus_grid_koppen(_health_df(), verbose=False)
        assert list(df["value"]) == [1, 2, 3, 4, 5]

    def test_duckdb_relation_input(self, patched_meta):
        duckdb = pytest.importorskip("duckdb")
        conn = duckdb.connect()
        rel = conn.from_df(_health_df())
        df = sus_grid_koppen(rel, verbose=False)
        assert "zona_koppen" in df.columns
        assert len(df) == 5


class TestMetadata:
    def test_sus_meta_attrs(self, patched_meta):
        df = sus_grid_koppen(_health_df(), verbose=False)
        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "koppen"
        assert meta["doi"] == "10.1127/0941-2948/2013/0507"
        assert any("sus_grid_koppen" in h for h in meta["history"])

    def test_sus_meta_preserves_input_attrs(self, patched_meta):
        health = _health_df()
        health.attrs["sus_meta"] = {"stage": "aggregate", "type": "health", "history": ["prev"]}
        df = sus_grid_koppen(health, verbose=False)
        meta = df.attrs["sus_meta"]
        assert meta["history"][0] == "prev"
        assert len(meta["history"]) == 2
        assert meta["stage"] == "climate"


class TestClassifyOne:
    def test_bsh_belt(self):
        assert _classify_one(26, -38.0, -8.05) == "BSh"

    def test_south_cf(self):
        assert _classify_one(41, -51.0, -25.0) == "Cf"

    def test_default_south_cf(self):
        # No UF-specific rule matches and lat <= -23 -> falls through to "Cf".
        assert _classify_one(99, -60.0, -30.0) == "Cf"

    def test_default_tropical_aw(self):
        # No UF-specific rule matches and lat > -23 -> falls through to "Aw".
        assert _classify_one(99, -60.0, -10.0) == "Aw"


class TestExactMode:
    def test_exact_mode_spatial_join(self, patched_meta):
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        koppen_sf = gpd.GeoDataFrame(
            {"koppen": ["BSh", "Cf"]},
            geometry=[
                box(-40, -10, -30, -5),   # covers PE point (-38, -8.05)
                box(-55, -35, -45, -20),  # covers RS/SP points
            ],
            crs="EPSG:4326",
        )
        df = sus_grid_koppen(_health_df(), mode="exact", koppen_sf=koppen_sf, verbose=False)
        by_code = df.set_index("code_muni")["zona_koppen"]
        assert by_code["2609600"] == "BSh"
        assert by_code["4314902"] == "Cf"
        assert by_code["3550308"] == "Cf"

    def test_exact_mode_out_of_vocab_zone_becomes_na_with_as_factor(self, patched_meta):
        # Alvares et al. (2013) actually publishes sub-types (e.g. "Cfa",
        # "Cwb", "BSk") beyond the 7 canonical levels this module keeps.
        # R's factor(x, levels=_KOPPEN_LEVELS) silently drops any
        # unlisted value to NA; pandas.Categorical does the same. This
        # pins that (preserved-from-R) behavior so it isn't "fixed" by
        # accident later.
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        koppen_sf = gpd.GeoDataFrame(
            {"koppen": ["Cfa"]},
            geometry=[box(-60, -35, -30, -5)],  # covers every fixture point
            crs="EPSG:4326",
        )
        df = sus_grid_koppen(
            _health_df(), mode="exact", koppen_sf=koppen_sf, as_factor=True, verbose=False
        )
        matched = df[df["code_muni"] != "9999999"]
        assert matched["zona_koppen"].isna().all()
