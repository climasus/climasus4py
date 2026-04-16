"""Tests for sus_aggregate — time/geo grouping and summarisation."""

import pandas as pd
import pytest

from climasus.core.engine import get_connection
from climasus.core.aggregate import sus_aggregate


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


class TestTimeAggregation:
    @pytest.fixture
    def rel(self):
        return _make_rel({
            "DTOBITO": pd.to_datetime([
                "2023-01-10", "2023-01-20", "2023-02-15",
                "2023-06-01", "2023-06-15", "2023-12-25",
            ]),
            "CODMUNRES": ["355030", "330455", "355030",
                          "310620", "355030", "330455"],
            "CAUSABAS": ["J189"] * 6,
        })

    def test_month_aggregation(self, rel):
        result = sus_aggregate(rel, time="month", geo="municipality")
        df = result.df()
        assert "time_group" in df.columns
        assert "count" in df.columns
        # At least 4 distinct months
        assert len(df["time_group"].unique()) >= 4

    def test_year_aggregation(self, rel):
        result = sus_aggregate(rel, time="year", geo="municipality")
        df = result.df()
        # All same year
        assert len(df["time_group"].unique()) == 1

    def test_quarter_aggregation(self, rel):
        result = sus_aggregate(rel, time="quarter", geo="municipality")
        df = result.df()
        assert len(df["time_group"].unique()) >= 3

    def test_extra_groups(self, rel):
        """extra_groups should add grouping columns."""
        result = sus_aggregate(rel, time="month", geo="municipality", extra_groups=["CAUSABAS"])
        df = result.df()
        assert "CAUSABAS" in df.columns

    def test_no_date_col_returns_count(self):
        """Without date/geo columns, should return total count."""
        rel = _make_rel({"VALUE": [1, 2, 3]})
        result = sus_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "count" in df.columns
        assert df["count"].iloc[0] == 3


class TestGeoAggregation:
    def test_state_geo(self):
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "UF": ["SP", "RJ", "SP"],
        })
        result = sus_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "UF" in df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
