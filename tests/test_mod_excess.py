"""Tests for sus_mod_excess (excess mortality/morbidity from a time series).

Ground truth generated from real R climasus4r output on the shared fixture
(tests/fixtures/dlnm_synthetic_input.csv). ``method="spline"``/``"serfling"``
match R's raw output exactly (bit-for-bit on rounded totals) since those
paths don't touch the buggy ``dlnm::crosspred(at=x)`` sort/dedup path.
``method="from_dlnm"`` is pinned against the CORRECTED R values (same fix
as ``sus_mod_af`` — see ``mod_excess.py``'s module docstring and
``IDEIAS.md``), since R's raw ``"from_dlnm"`` output has the same
misalignment bug.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from climasus4py.enrichment.mod_dlnm import sus_mod_dlnm
from climasus4py.enrichment.mod_excess import sus_mod_excess

FIXTURE = Path(__file__).parent / "fixtures" / "dlnm_synthetic_input.csv"

R_SPLINE_TOTAL = {
    "observed": 16122.0,
    "expected": 16122.0,
    "expected_lo": 15086.0,
    "expected_hi": 17231.0,
    "excess": 0.0,
}
R_SERFLING_TOTAL = {
    "observed": 16122.0,
    "expected": 16122.0,
    "expected_lo": 15459.0,
    "expected_hi": 16814.0,
    "excess": 0.0,
}
R_FROM_DLNM_TOTAL_FIXED = {
    "observed": 16122.0,
    "expected": 16981.08,
    "expected_lo": 14158.09,
    "expected_hi": 20580.17,
    "excess": -859.077,
}
R_MONTH1_SPLINE = {"n_days": 17, "observed": 342, "expected": 352.7, "excess": -10.7}
R_SUBSET_TOTAL = {"observed": 4117.0, "expected": 2187.0, "n_control": 516, "n_study": 215}


@pytest.fixture
def daily_df() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE, parse_dates=["date"])
    return df[["date", "n_obitos"]].copy()


@pytest.fixture
def dlnm_fit() -> dict:
    df = pd.read_csv(FIXTURE, parse_dates=["date"])
    return sus_mod_dlnm(
        df, climate_col="tair_dry_bulb_c", lag_max=14, dof_per_year=4, verbose=False
    )


class TestNumericParityWithR:
    def test_spline_matches_r(self, daily_df):
        exc = sus_mod_excess(
            daily_df, outcome_col="n_obitos", method="spline", dof_per_year=8,
            lang="en", verbose=False,
        )
        t = exc["total"].iloc[0]
        for col, expected in R_SPLINE_TOTAL.items():
            assert t[col] == pytest.approx(expected, abs=1.0)

    def test_serfling_matches_r(self, daily_df):
        exc = sus_mod_excess(
            daily_df, outcome_col="n_obitos", method="serfling", harmonics=2,
            lang="en", verbose=False,
        )
        t = exc["total"].iloc[0]
        for col, expected in R_SERFLING_TOTAL.items():
            assert t[col] == pytest.approx(expected, abs=1.0)

    def test_from_dlnm_matches_corrected_r(self, dlnm_fit):
        exc = sus_mod_excess(dlnm_fit, method="from_dlnm", lang="en", verbose=False)
        t = exc["total"].iloc[0]
        for col, expected in R_FROM_DLNM_TOTAL_FIXED.items():
            assert t[col] == pytest.approx(expected, abs=1.0)

    def test_by_period_month_matches_r(self, daily_df):
        exc = sus_mod_excess(
            daily_df, outcome_col="n_obitos", method="spline", dof_per_year=8,
            by="month", lang="en", verbose=False,
        )
        first = exc["by_period"].iloc[0]
        for col, expected in R_MONTH1_SPLINE.items():
            assert first[col] == pytest.approx(expected, abs=0.5)

    def test_control_study_subset_matches_r(self, daily_df):
        exc = sus_mod_excess(
            daily_df, outcome_col="n_obitos", method="spline",
            control_period=(pd.Timestamp("2020-02-01"), pd.Timestamp("2021-06-30")),
            study_period=(pd.Timestamp("2021-07-01"), pd.Timestamp("2022-01-31")),
            dof_per_year=8, lang="en", verbose=False,
        )
        t = exc["total"].iloc[0]
        assert t["observed"] == pytest.approx(R_SUBSET_TOTAL["observed"], abs=1.0)
        assert t["expected"] == pytest.approx(R_SUBSET_TOTAL["expected"], abs=1.0)
        assert exc["meta"]["n_control"] == R_SUBSET_TOTAL["n_control"]
        assert exc["meta"]["n_study"] == R_SUBSET_TOTAL["n_study"]


class TestShapeAndOutput:
    def test_output_keys(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", verbose=False)
        for key in ("daily", "total", "by_period", "model", "meta"):
            assert key in exc

    def test_default_method_for_dataframe_is_spline(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", verbose=False)
        assert exc["meta"]["method"] == "spline"

    def test_default_method_for_dlnm_is_from_dlnm(self, dlnm_fit):
        exc = sus_mod_excess(dlnm_fit, verbose=False)
        assert exc["meta"]["method"] == "from_dlnm"

    def test_from_dlnm_model_is_none(self, dlnm_fit):
        exc = sus_mod_excess(dlnm_fit, method="from_dlnm", verbose=False)
        assert exc["model"] is None

    def test_spline_model_is_not_none(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", method="spline", verbose=False)
        assert exc["model"] is not None

    def test_by_period_none_without_by(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", verbose=False)
        assert exc["by_period"] is None

    def test_by_period_year(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", by="year", verbose=False)
        assert "year" in exc["by_period"].columns

    def test_by_period_season(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", by="season", verbose=False)
        assert "season" in exc["by_period"].columns

    def test_is_excess_flag_uses_threshold_z(self, daily_df):
        exc_strict = sus_mod_excess(
            daily_df, outcome_col="n_obitos", threshold_z=0.5, verbose=False
        )
        exc_loose = sus_mod_excess(
            daily_df, outcome_col="n_obitos", threshold_z=5.0, verbose=False
        )
        assert exc_strict["daily"]["is_excess"].sum() >= exc_loose["daily"]["is_excess"].sum()

    def test_cum_excess_is_cumsum(self, daily_df):
        exc = sus_mod_excess(daily_df, outcome_col="n_obitos", verbose=False)
        import numpy as np

        np.testing.assert_allclose(
            exc["daily"]["cum_excess"].to_numpy(), exc["daily"]["excess"].cumsum().to_numpy()
        )


class TestValidation:
    def test_bad_method_raises(self, daily_df):
        with pytest.raises(ValueError):
            sus_mod_excess(daily_df, outcome_col="n_obitos", method="bogus", verbose=False)

    def test_from_dlnm_with_dataframe_raises(self, daily_df):
        with pytest.raises(ValueError):
            sus_mod_excess(daily_df, outcome_col="n_obitos", method="from_dlnm", verbose=False)

    def test_bad_by_raises(self, daily_df):
        with pytest.raises(ValueError):
            sus_mod_excess(daily_df, outcome_col="n_obitos", by="decade", verbose=False)

    def test_missing_outcome_col_raises(self, daily_df):
        with pytest.raises(ValueError):
            sus_mod_excess(daily_df, outcome_col="not_a_column", verbose=False)

    def test_bad_input_type_raises(self):
        with pytest.raises(ValueError):
            sus_mod_excess([1, 2, 3], verbose=False)

    def test_empty_control_period_raises(self, daily_df):
        with pytest.raises(ValueError):
            sus_mod_excess(
                daily_df, outcome_col="n_obitos",
                control_period=(pd.Timestamp("2099-01-01"), pd.Timestamp("2099-12-31")),
                verbose=False,
            )
