"""Tests for sus_climate — health + climate join enrichment."""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate import sus_climate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _health_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "municipality_code": ["355030", "355030", "330455"],
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01"]),
            "count": [10, 12, 5],
        }
    )


def _climate_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "municipality_code": ["355030", "355030", "330455"],
            "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01"]),
            "temp_mean": [25.0, 26.0, 27.0],
            "precipitation": [0.0, 5.0, 2.0],
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSusClimate:
    def test_basic_join_returns_dataframe(self):
        result = sus_climate(_health_df(), _climate_df())
        assert isinstance(result, pd.DataFrame)

    def test_basic_join_columns_added(self):
        result = sus_climate(_health_df(), _climate_df())
        assert "temp_mean" in result.columns
        assert "precipitation" in result.columns

    def test_basic_join_row_count_preserved(self):
        result = sus_climate(_health_df(), _climate_df())
        assert len(result) == 3

    def test_duckdb_relation_input(self):
        """DuckDB relation input must be materialised before joining."""
        conn = get_connection()
        rel = conn.from_df(_health_df())
        result = sus_climate(rel, _climate_df())
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_no_match_gives_nan_climate(self):
        """Health rows with no matching climate date should have NaN."""
        health = pd.DataFrame(
            {
                "municipality_code": ["999999"],
                "date": pd.to_datetime(["2023-06-01"]),
                "count": [1],
            }
        )
        result = sus_climate(health, _climate_df())
        assert result["temp_mean"].isna().all()

    def test_lags_columns_added(self):
        result = sus_climate(_health_df(), _climate_df(), lags=[7])
        assert "temp_mean_lag7d" in result.columns
        assert "precipitation_lag7d" in result.columns

    def test_multiple_lags(self):
        result = sus_climate(_health_df(), _climate_df(), lags=[7, 14])
        assert "temp_mean_lag7d" in result.columns
        assert "temp_mean_lag14d" in result.columns

    def test_no_lags_when_none(self):
        result = sus_climate(_health_df(), _climate_df(), lags=None)
        assert not any(c.endswith("_lag7d") for c in result.columns)

    def test_missing_geo_column_raises_valueerror(self):
        health_no_geo = pd.DataFrame(
            {"date": pd.to_datetime(["2023-01-01"]), "count": [1]}
        )
        with pytest.raises(ValueError, match="municipality"):
            sus_climate(health_no_geo, _climate_df())

    def test_missing_date_column_raises_valueerror(self):
        health_no_date = pd.DataFrame(
            {"municipality_code": ["355030"], "count": [1]}
        )
        with pytest.raises(ValueError, match="municipality"):
            sus_climate(health_no_date, _climate_df())

    def test_time_window_param_accepted(self):
        """time_window is reserved; function should accept without error."""
        result = sus_climate(_health_df(), _climate_df(), time_window=7)
        assert isinstance(result, pd.DataFrame)

    def test_climate_date_parsed_correctly(self):
        """String dates in climate df should be parsed to datetime."""
        clim = _climate_df().copy()
        clim["date"] = clim["date"].astype(str)
        result = sus_climate(_health_df(), clim)
        assert len(result) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
