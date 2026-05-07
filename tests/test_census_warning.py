"""P2 Sprint 2 — sus_census emits UserWarning when year=2010 (synthetic data)."""

import warnings

import pandas as pd
import pytest

from climasus4py.enrichment.census import sus_census


def _health_df():
    return pd.DataFrame({
        "CODMUNRES": ["355030", "330455"],
        "count": [10, 5],
    })


def _census_df():
    return pd.DataFrame({
        "municipality_code": ["355030", "330455"],
        "pop_total": [12_500_000, 6_700_000],
    })


class TestCensusYear2010Warning:
    def test_year_2010_raises_user_warning(self):
        """year=2010 must emit UserWarning about synthetic data."""
        with pytest.warns(UserWarning, match="SINTÉTICOS"):
            sus_census(_health_df(), census=_census_df(), year=2010)

    def test_warning_contains_ibge_guidance(self):
        """Warning must mention IBGE."""
        with pytest.warns(UserWarning, match="IBGE"):
            sus_census(_health_df(), census=_census_df(), year=2010)

    def test_other_years_no_warning(self):
        """For years other than 2010, no UserWarning is emitted."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            # Should not raise
            sus_census(_health_df(), census=_census_df(), year=2022)

    def test_no_year_no_warning(self):
        """Omitting year altogether must not emit a warning."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            sus_census(_health_df(), census=_census_df())

    def test_join_still_works_after_warning(self):
        """Warning must not affect the join result."""
        with pytest.warns(UserWarning):
            result = sus_census(_health_df(), census=_census_df(), year=2010)
        assert "pop_total" in result.columns
        assert len(result) == 2
