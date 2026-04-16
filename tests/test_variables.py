"""Tests for sus_variables — age groups, epi weeks, season, quarter, etc.

Focuses on:
  - Age group bucketing with IDADE decoding
  - Temporal derived columns (epi_week, season, quarter, month_name, day_of_week)
"""

import pandas as pd
import pytest

from climasus.core.engine import get_connection
from climasus.core.variables import sus_variables


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


# ---------------------------------------------------------------------------
# Age group with IDADE decoding
# ---------------------------------------------------------------------------

class TestAgeGroup:
    @pytest.fixture
    def rel_with_idade(self):
        return _make_rel({
            "IDADE": ["405", "420", "450", "468", "490", "500", "301", "110"],
            # decoded:   5,   20,   50,   68,   90,  100,    0,    0
            "DTOBITO": ["01012023"] * 8,
            "CAUSABAS": ["J189"] * 8,
        })

    def test_decadal_groups(self, rel_with_idade):
        """Decadal age groups should use decoded IDADE."""
        result = sus_variables(rel_with_idade, age_group="decadal")
        df = result.df()
        assert "age_group" in df.columns
        groups = df["age_group"].tolist()
        # 5→0-9, 20→20-29, 50→50-59, 68→60-69, 90→90-998, 100→90-998, 0→0-9, 0→0-9
        assert groups.count("0-9") == 3
        assert "20-29" in groups
        assert "50-59" in groups
        assert "60-69" in groups

    def test_who_groups(self, rel_with_idade):
        result = sus_variables(rel_with_idade, age_group="who")
        df = result.df()
        assert "age_group" in df.columns
        # Check some expected buckets
        groups = set(df["age_group"].tolist())
        assert "0-0" in groups  # infants (age 0)
        assert "20-24" in groups  # age 20

    def test_custom_breaks(self, rel_with_idade):
        result = sus_variables(rel_with_idade, age_group=[0, 18, 65])
        df = result.df()
        groups = df["age_group"].tolist()
        # 5→0-17, 20→18-64, 50→18-64, 68→65+, 90→65+, 100→65+, 0→0-17, 0→0-17
        assert groups.count("0-17") == 3
        assert groups.count("18-64") == 2
        assert groups.count("65+") == 3

    def test_no_age_group(self, rel_with_idade):
        """When age_group=None, no age_group column added."""
        result = sus_variables(rel_with_idade)
        df = result.df()
        assert "age_group" not in df.columns


# ---------------------------------------------------------------------------
# Temporal variables
# ---------------------------------------------------------------------------

class TestTemporalVariables:
    @pytest.fixture
    def rel_with_dates(self):
        return _make_rel({
            "DTOBITO": pd.to_datetime([
                "2023-01-15", "2023-04-20", "2023-07-10", "2023-12-25",
            ]),
            "CAUSABAS": ["J189", "I219", "A90", "E149"],
        })

    def test_epi_week(self, rel_with_dates):
        result = sus_variables(rel_with_dates, epi_week=True)
        df = result.df()
        assert "epi_week" in df.columns
        assert all(df["epi_week"].notna())

    def test_season(self, rel_with_dates):
        """Southern hemisphere seasons."""
        result = sus_variables(rel_with_dates, season=True)
        df = result.df()
        assert "season" in df.columns
        seasons = df["season"].tolist()
        # Jan→Summer, Apr→Autumn, Jul→Winter, Dec→Summer
        assert seasons[0] == "Summer"
        assert seasons[1] == "Autumn"
        assert seasons[2] == "Winter"
        assert seasons[3] == "Summer"

    def test_quarter(self, rel_with_dates):
        result = sus_variables(rel_with_dates, quarter=True)
        df = result.df()
        assert "quarter" in df.columns

    def test_month_name(self, rel_with_dates):
        result = sus_variables(rel_with_dates, month_name=True)
        df = result.df()
        assert "month_name" in df.columns

    def test_day_of_week(self, rel_with_dates):
        result = sus_variables(rel_with_dates, day_of_week=True)
        df = result.df()
        assert "day_of_week" in df.columns

    def test_no_date_col_skips_temporal(self):
        """When no date column exists, temporal vars are silently skipped."""
        rel = _make_rel({"VALUE": [1, 2, 3]})
        result = sus_variables(rel, epi_week=True, season=True)
        df = result.df()
        assert "epi_week" not in df.columns
        assert "season" not in df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
