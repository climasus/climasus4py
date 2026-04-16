"""Tests for sus_quality — data quality profiling."""

import pandas as pd
import pytest

from climasus.core.engine import get_connection
from climasus.utils.quality import sus_quality


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


class TestQualityRelation:
    def test_basic_quality(self):
        rel = _make_rel({
            "A": [1, 2, None],
            "B": ["x", None, "z"],
        })
        result = sus_quality(rel)
        assert result["total_rows"] == 3
        assert result["total_cols"] == 2
        assert "A" in result["completeness"]
        assert "B" in result["completeness"]

    def test_full_completeness(self):
        rel = _make_rel({"A": [1, 2, 3], "B": [4, 5, 6]})
        result = sus_quality(rel)
        assert result["completeness"]["A"] == 100.0
        assert result["completeness"]["B"] == 100.0

    def test_empty_relation(self):
        rel = _make_rel({"A": pd.Series([], dtype="int64")})
        result = sus_quality(rel)
        assert result["total_rows"] == 0


class TestQualityDataFrame:
    def test_dataframe_quality(self):
        df = pd.DataFrame({
            "A": [1, None, 3, None],
            "B": ["x", "y", "z", "w"],
        })
        result = sus_quality(df)
        assert result["total_rows"] == 4
        assert result["completeness"]["A"] == 50.0
        assert result["completeness"]["B"] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
