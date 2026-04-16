"""Tests for sus_fill_gaps — gap filling / interpolation."""

import numpy as np
import pandas as pd
import pytest

from climasus.enrichment.fill_gaps import sus_fill_gaps


@pytest.fixture
def gap_data():
    """Time series with intentional gaps."""
    dates = pd.date_range("2023-01-01", periods=10, freq="D")
    return pd.DataFrame({
        "municipality_code": ["355030"] * 10,
        "date": dates,
        "temperature": [25.0, np.nan, 27.0, np.nan, np.nan, 30.0, 31.0, np.nan, 29.0, 28.0],
        "humidity": [80.0, 78.0, np.nan, 75.0, np.nan, 70.0, np.nan, 68.0, 65.0, 63.0],
    })


class TestLinearInterpolation:
    def test_fills_gaps(self, gap_data):
        result = sus_fill_gaps(gap_data, method="linear")
        assert result["temperature"].notna().all()
        assert result["humidity"].notna().all()

    def test_preserves_known_values(self, gap_data):
        result = sus_fill_gaps(gap_data, method="linear")
        assert result["temperature"].iloc[0] == 25.0
        assert result["temperature"].iloc[2] == 27.0

    def test_max_gap_limits_fill(self, gap_data):
        result = sus_fill_gaps(gap_data, method="linear", max_gap=1)
        # Gap of 2 consecutive NAs (indices 3-4) should NOT be fully filled
        # with max_gap=1
        assert result["temperature"].isna().any()


class TestLOCF:
    def test_locf_fills(self, gap_data):
        result = sus_fill_gaps(gap_data, method="locf")
        # After LOCF, only leading NAs (if any) remain
        assert result["temperature"].iloc[2] == 27.0  # original
        assert result["temperature"].iloc[3] == 27.0  # carried forward


class TestInvalidMethod:
    def test_unknown_method_raises(self, gap_data):
        with pytest.raises(ValueError, match="Unknown method"):
            sus_fill_gaps(gap_data, method="unknown_method")


class TestNoGaps:
    def test_complete_data_unchanged(self):
        df = pd.DataFrame({
            "municipality_code": ["SP"] * 5,
            "date": pd.date_range("2023-01-01", periods=5),
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
        })
        result = sus_fill_gaps(df, method="linear")
        pd.testing.assert_frame_equal(result[["value"]], df[["value"]])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
