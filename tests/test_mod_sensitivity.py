"""Tests for sus_mod_sensitivity (stratified comparison of DLNM fits).

Ground truth generated from real R climasus4r output using two DLNM fits
built on overlapping halves of the shared fixture
(tests/fixtures/dlnm_synthetic_input.csv), rows [0:443) and [343:786) —
0-indexed slices matching R's 1-indexed df[1:443,]/df[344:786,].
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from climasus4py.enrichment.mod_dlnm import sus_mod_dlnm
from climasus4py.enrichment.mod_sensitivity import sus_mod_sensitivity

FIXTURE = Path(__file__).parent / "fixtures" / "dlnm_synthetic_input.csv"

R_COMPARISON = {
    "first_half": {"hot_rr": 1.605102, "cold_rr": 1.614036, "sensitivity_index": 0.951925},
    "second_half": {"hot_rr": 0.867479, "cold_rr": 0.988648, "sensitivity_index": -0.153580},
}
R_CUSTOM_PCT_COMPARISON = {
    "first_half": {"hot_rr": 1.47, "cold_rr": 1.39},
    "second_half": {"hot_rr": 0.859, "cold_rr": 0.919},
}


@pytest.fixture
def two_fits() -> dict:
    df = pd.read_csv(FIXTURE, parse_dates=["date"])
    fit_a = sus_mod_dlnm(
        df.iloc[:443].reset_index(drop=True),
        climate_col="tair_dry_bulb_c", lag_max=14, dof_per_year=4, verbose=False,
    )
    fit_b = sus_mod_dlnm(
        df.iloc[343:].reset_index(drop=True),
        climate_col="tair_dry_bulb_c", lag_max=14, dof_per_year=4, verbose=False,
    )
    return {"first_half": fit_a, "second_half": fit_b}


class TestNumericParityWithR:
    def test_comparison_matches_r(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, lang="en", verbose=False)
        cmp = sens["comparison"].set_index("stratum")
        for stratum, expected in R_COMPARISON.items():
            row = cmp.loc[stratum]
            assert row["hot_rr"] == pytest.approx(expected["hot_rr"], abs=1e-4)
            assert row["cold_rr"] == pytest.approx(expected["cold_rr"], abs=1e-4)
            assert row["sensitivity_index"] == pytest.approx(
                expected["sensitivity_index"], abs=1e-4
            )

    def test_custom_percentiles_match_r(self, two_fits):
        sens = sus_mod_sensitivity(
            two_fits, hot_percentile=0.95, cold_percentile=0.05,
            stratum_labels={"first_half": "Primeira Metade"}, lang="en", verbose=False,
        )
        cmp = sens["comparison"].set_index("stratum")
        for stratum, expected in R_CUSTOM_PCT_COMPARISON.items():
            row = cmp.loc[stratum]
            assert row["hot_rr"] == pytest.approx(expected["hot_rr"], abs=1e-2)
            assert row["cold_rr"] == pytest.approx(expected["cold_rr"], abs=1e-2)
        assert cmp.loc["first_half", "label"] == "Primeira Metade"
        assert cmp.loc["second_half", "label"] == "second_half"

    def test_ranked_first_half_is_more_sensitive(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, lang="en", verbose=False)
        assert sens["comparison"].iloc[0]["stratum"] == "first_half"
        assert sens["comparison"].iloc[0]["hot_rank"] == 1
        assert sens["comparison"].iloc[1]["hot_rank"] == 2


class TestShapeAndOutput:
    def test_output_keys(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, verbose=False)
        for key in ("rr_table", "comparison", "stratum_curves", "meta"):
            assert key in sens

    def test_rr_table_has_hot_cold_per_stratum(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, verbose=False)
        assert len(sens["rr_table"]) == 4  # 2 strata x 2 components
        assert set(sens["rr_table"]["component"]) == {"hot", "cold"}

    def test_stratum_curves_has_full_grid_per_stratum(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, verbose=False)
        counts = sens["stratum_curves"].groupby("stratum").size()
        assert counts["first_half"] == 100
        assert counts["second_half"] == 100

    def test_unnamed_list_auto_labels_strata(self, two_fits):
        sens = sus_mod_sensitivity(list(two_fits.values()), verbose=False)
        assert set(sens["meta"]["stratum_names"]) == {"Stratum 1", "Stratum 2"}

    def test_meta_fields(self, two_fits):
        sens = sus_mod_sensitivity(two_fits, verbose=False)
        assert sens["meta"]["climate_col"] == "tair_dry_bulb_c"
        assert sens["meta"]["n_strata"] == 2
        assert sens["meta"]["hot_percentile"] == 0.99
        assert sens["meta"]["cold_percentile"] == 0.01


class TestValidation:
    def test_single_fit_raises(self, two_fits):
        with pytest.raises(ValueError):
            sus_mod_sensitivity({"only": next(iter(two_fits.values()))}, verbose=False)

    def test_non_dlnm_element_raises(self, two_fits):
        bad = dict(two_fits)
        bad["not_a_fit"] = {"foo": "bar"}
        with pytest.raises(ValueError):
            sus_mod_sensitivity(bad, verbose=False)

    def test_different_climate_vars_raise(self, two_fits):
        df = pd.read_csv(FIXTURE, parse_dates=["date"])
        df2 = df.rename(columns=lambda c: c.replace("tair_dry_bulb_c", "rainfall_mm"))
        other_fit = sus_mod_dlnm(
            df2.iloc[:443].reset_index(drop=True),
            climate_col="rainfall_mm", lag_max=14, dof_per_year=4, verbose=False,
        )
        bad = {**two_fits, "other": other_fit}
        with pytest.raises(ValueError):
            sus_mod_sensitivity(bad, verbose=False)

    def test_bad_percentiles_raise(self, two_fits):
        with pytest.raises(ValueError):
            sus_mod_sensitivity(two_fits, hot_percentile=0.01, cold_percentile=0.99, verbose=False)

    def test_percentile_out_of_range_raises(self, two_fits):
        with pytest.raises(ValueError):
            sus_mod_sensitivity(two_fits, hot_percentile=1.5, verbose=False)
