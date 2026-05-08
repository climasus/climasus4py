"""Tests for sus_spatial_join — health data spatial enrichment."""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.spatial import sus_spatial_join

gpd = pytest.importorskip("geopandas", reason="geopandas not installed")
Point = pytest.importorskip("shapely.geometry", reason="shapely not installed").Point


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _health_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "municipality_code": ["355030", "330455"],
            "count": [10, 5],
        }
    )


def _municipality_shapefile() -> "gpd.GeoDataFrame":
    return gpd.GeoDataFrame(
        {
            "code_muni": ["355030", "330455"],
            "name_muni": ["São Paulo", "Rio de Janeiro"],
        },
        geometry=[Point(-46.63, -23.55), Point(-43.17, -22.90)],
        crs="EPSG:4326",
    )


def _state_shapefile() -> "gpd.GeoDataFrame":
    return gpd.GeoDataFrame(
        {
            "abbrev_state": ["SP", "RJ"],
            "name_state": ["São Paulo", "Rio de Janeiro"],
        },
        geometry=[Point(-46.63, -23.55), Point(-43.17, -22.90)],
        crs="EPSG:4326",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSusSpatialJoin:
    def test_basic_join_returns_geodataframe(self):
        result = sus_spatial_join(_health_df(), shapefile=_municipality_shapefile())
        assert type(result).__name__ == "GeoDataFrame"

    def test_basic_join_has_geometry(self):
        result = sus_spatial_join(_health_df(), shapefile=_municipality_shapefile())
        assert "geometry" in result.columns

    def test_basic_join_row_count(self):
        result = sus_spatial_join(_health_df(), shapefile=_municipality_shapefile())
        assert len(result) == 2

    def test_count_column_preserved(self):
        result = sus_spatial_join(_health_df(), shapefile=_municipality_shapefile())
        assert "count" in result.columns

    def test_duckdb_relation_input(self):
        """DuckDB relation must be materialised before joining."""
        conn = get_connection()
        rel = conn.from_df(_health_df())
        result = sus_spatial_join(rel, shapefile=_municipality_shapefile())
        assert type(result).__name__ == "GeoDataFrame"
        assert len(result) == 2

    def test_state_level_join(self):
        health = pd.DataFrame({"state": ["SP", "RJ"], "count": [10, 5]})
        result = sus_spatial_join(
            health, shapefile=_state_shapefile(), geo_level="state"
        )
        assert type(result).__name__ == "GeoDataFrame"
        assert len(result) == 2

    def test_inner_join_removes_unmatched(self):
        health = pd.DataFrame({"municipality_code": ["355030", "999999"], "count": [10, 99]})
        result = sus_spatial_join(
            health, shapefile=_municipality_shapefile(), join_type="inner"
        )
        assert len(result) == 1

    def test_no_geo_column_raises_valueerror(self):
        df = pd.DataFrame({"other_col": ["x"], "count": [1]})
        with pytest.raises(ValueError, match="No municipality"):
            sus_spatial_join(df, shapefile=_municipality_shapefile())

    def test_no_join_col_in_shapefile_raises_valueerror(self):
        shp = gpd.GeoDataFrame(
            {"unexpected_col": ["355030"]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )
        with pytest.raises(ValueError, match="join column"):
            sus_spatial_join(_health_df(), shapefile=shp)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
