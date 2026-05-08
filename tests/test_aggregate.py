"""Tests for sus_data_aggregate — time/geo grouping and summarisation."""

import re

import pandas as pd
import pytest

from climasus4py.core.aggregate import sus_data_aggregate
from climasus4py.core.engine import get_connection
from climasus4py.core.variables import sus_data_create_variables


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
        result = sus_data_aggregate(rel, time="month", geo="municipality")
        df = result.df()
        assert "time_group" in df.columns
        assert "count" in df.columns
        # At least 4 distinct months
        assert len(df["time_group"].unique()) >= 4

    def test_year_aggregation(self, rel):
        result = sus_data_aggregate(rel, time="year", geo="municipality")
        df = result.df()
        # All same year
        assert len(df["time_group"].unique()) == 1

    def test_quarter_aggregation(self, rel):
        result = sus_data_aggregate(rel, time="quarter", geo="municipality")
        df = result.df()
        assert len(df["time_group"].unique()) >= 3

    def test_extra_groups(self, rel):
        """extra_groups should add grouping columns."""
        result = sus_data_aggregate(rel, time="month", geo="municipality", extra_groups=["CAUSABAS"])  # noqa: E501
        df = result.df()
        assert "CAUSABAS" in df.columns

    def test_no_date_col_returns_count(self):
        """Without date/geo columns, should return total count."""
        rel = _make_rel({"VALUE": [1, 2, 3]})
        result = sus_data_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "count" in df.columns
        assert df["count"].iloc[0] == 3


class TestGeoAggregation:
    def test_state_geo(self):
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "UF": ["SP", "RJ", "SP"],
        })
        result = sus_data_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "UF" in df.columns


class TestWeekFormatSVS:
    """P1 Sprint 2 — sus_data_aggregate week format aligned with SVS."""

    @pytest.fixture
    def rel_week(self):
        # 2023-01-01 is a Sunday → SVS week 01/2023
        return _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-08"]),
        })

    def test_default_is_svs_format(self, rel_week):
        """Default week_format='svs' → 'WW/YYYY' pattern."""
        df = sus_data_aggregate(rel_week, time="week", geo="state").df()
        for val in df["time_group"].dropna():
            assert re.fullmatch(r"\d{2}/\d{4}", val), f"Expected WW/YYYY, got {val!r}"

    def test_iso_format_option(self, rel_week):
        """week_format='iso' preserves old 'YYYY-WXX' pattern."""
        df = sus_data_aggregate(rel_week, time="week", geo="state", week_format="iso").df()
        for val in df["time_group"].dropna():
            assert re.fullmatch(r"\d{4}-W\d{2}", val), f"Expected YYYY-WXX, got {val!r}"

    def test_aggregate_and_variables_aligned(self):
        """sus_data_aggregate(time='week') and sus_data_create_variables(epi_week=True) must return identical values."""  # noqa: E501
        dates = pd.to_datetime([
            "2023-01-01", "2023-06-15", "2023-12-25",
            "2023-03-19", "2023-09-03",
        ])
        rel = _make_rel({"DTOBITO": dates})
        agg_weeks = set(sus_data_aggregate(rel, time="week", geo="state").df()["time_group"].dropna())  # noqa: E501
        var_weeks = set(sus_data_create_variables(rel, epi_week=True).df()["epi_week"].dropna())
        assert agg_weeks == var_weeks, (
            f"Mismatch between aggregate and variables week values:\n"
            f"aggregate: {sorted(agg_weeks)}\nvariables: {sorted(var_weeks)}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
