"""Tests for lazy sus_fill_gaps."""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.fill_gaps import sus_fill_gaps


@pytest.fixture
def gap_rel():
    dates = pd.date_range("2023-01-01", periods=6, freq="D")
    df = pd.DataFrame(
        {
            "municipality_code": ["355030"] * 6,
            "date": dates,
            "temperature": [25.0, None, 27.0, None, None, 31.0],
            "humidity": [80.0, 78.0, None, 75.0, None, 70.0],
        }
    )
    return get_connection().from_df(df)


def test_linear_returns_relation_and_fills(gap_rel):
    result = sus_fill_gaps(gap_rel, method="linear")
    assert type(result).__name__ == "DuckDBPyRelation"
    df = result.df()
    assert df["temperature"].notna().all()
    assert df["temperature"].iloc[0] == 25.0
    assert df["temperature"].iloc[2] == 27.0


def test_locf_returns_relation_and_fills(gap_rel):
    result = sus_fill_gaps(gap_rel, method="locf")
    assert type(result).__name__ == "DuckDBPyRelation"
    df = result.df()
    assert df["temperature"].iloc[3] == 27.0


def test_unknown_method_raises(gap_rel):
    with pytest.raises(ValueError, match="Unknown method"):
        sus_fill_gaps(gap_rel, method="unknown_method")


def test_opt_in_materialized_warns(gap_rel):
    with pytest.warns(UserWarning, match="materializes"):
        result = sus_fill_gaps(gap_rel, method="spline")
    assert type(result).__name__ == "DuckDBPyRelation"


def test_no_numeric_columns_returns_unchanged():
    """When no numeric columns are detected, the relation is returned as-is."""
    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "municipality_code": ["355030", "355030"],
        "date": pd.date_range("2023-01-01", periods=2, freq="D"),
        "label": ["a", "b"],
    }))
    result = sus_fill_gaps(rel, method="linear")
    assert type(result).__name__ == "DuckDBPyRelation"
    # shape unchanged
    assert result.count("*").fetchone()[0] == 2


def test_spline_fallback_with_few_data_points():
    """With <4 valid data points spline falls back to linear without error."""
    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "municipality_code": ["355030"] * 4,
        "date": pd.date_range("2023-01-01", periods=4, freq="D"),
        "temperature": [20.0, None, None, 24.0],
    }))
    with pytest.warns(UserWarning, match="materializes"):
        result = sus_fill_gaps(rel, method="spline")
    df = result.df()
    assert df["temperature"].notna().all()
