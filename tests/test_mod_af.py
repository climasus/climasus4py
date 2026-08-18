"""Tests for sus_mod_af (Attributable Fraction / Number from a DLNM fit).

Ground truth was generated from real R climasus4r/dlnm output on the
shared fixture (tests/fixtures/dlnm_synthetic_input.csv), then CORRECTED
for a real bug discovered in R's sus_mod_af.R: dlnm::crosspred(at=x)
internally does `at <- sort(unique(at))`, so the returned allRRfit comes
back in ascending-exposure order, not in the caller's original order.
R's .saf_component() then does a positional `cases * (1 - 1/rr_obs)`
multiply, silently pairing each day's case count with a *different* day's
RR whenever the exposure series isn't already sorted (i.e. always, for a
real time series) — a silent-incorrect-result bug (CLAUDE.md rule 4's
explicit fix-don't-preserve exception, unlike an R quirk/bug that would
otherwise be replicated). The Python port never introduces this reorder
(RR is computed already aligned to the caller's row order), so its output
correctly disagrees with R's raw, buggy numbers and is pinned here against
the CORRECTED (name-matched) R values instead — see IDEIAS.md.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.mod_af import sus_mod_af
from climasus4py.enrichment.mod_dlnm import sus_mod_dlnm

FIXTURE = Path(__file__).parent / "fixtures" / "dlnm_synthetic_input.csv"

# Corrected (name-matched, bug-free) ground truth — see module docstring.
R_TOTAL = pd.DataFrame(
    {
        "component": ["total", "heat", "cold"],
        "af": [-0.05322458, -0.03899082, -0.01423376],
        "an": [-858.0867, -628.6101, -229.4766],
    }
)
R_BY_QUANTILE_P75_HOT_AF = -0.0280381909
R_BY_QUANTILE_P25_COLD_AF = -0.0098886282
R_MONTH_1_AN = -14.6
R_MONTH_1_CASES = 342


@pytest.fixture
def fit():
    df = pd.read_csv(FIXTURE, parse_dates=["date"])
    return sus_mod_dlnm(
        df, climate_col="tair_dry_bulb_c", lag_max=14, dof_per_year=4, verbose=False
    )


class TestNumericParityWithR:
    def test_total_matches_corrected_r(self, fit):
        af = sus_mod_af(fit, nsim=0, lang="en", verbose=False)
        cmp = af["total"].merge(R_TOTAL, on="component", suffixes=("_py", "_R"))
        np.testing.assert_allclose(cmp["af_py"], cmp["af_R"], atol=1e-6)
        np.testing.assert_allclose(cmp["an_py"], cmp["an_R"], atol=1e-3)

    def test_by_quantile_matches_corrected_r(self, fit):
        af = sus_mod_af(fit, nsim=0, lang="en", verbose=False)
        hot75 = af["by_quantile"].query("quantile_label == 'Above P75'").iloc[0]
        cold25 = af["by_quantile"].query("quantile_label == 'Below P25'").iloc[0]
        assert hot75["af"] == pytest.approx(R_BY_QUANTILE_P75_HOT_AF, abs=1e-6)
        assert cold25["af"] == pytest.approx(R_BY_QUANTILE_P25_COLD_AF, abs=1e-6)

    def test_by_period_month_matches_corrected_r(self, fit):
        af = sus_mod_af(fit, nsim=0, by="month", lang="en", verbose=False)
        first = af["by_period"].iloc[0]
        assert first["cases"] == R_MONTH_1_CASES
        assert first["an"] == pytest.approx(R_MONTH_1_AN, abs=1e-6)

    def test_daily_rr_is_row_aligned_not_sorted(self, fit):
        """Regression guard for the exact bug being avoided: AN/day must stay
        paired with that day's own case count, not a reordered one."""
        af = sus_mod_af(fit, nsim=0, lang="en", verbose=False)
        daily = af["daily"]
        assert list(daily["date"]) == list(fit["data_daily"]["date"])
        # An AF computed from a positionally-shuffled RR would not satisfy
        # this per-row identity: an == cases * (1 - 1/RR_of_that_same_row).
        recomputed_af = np.where(
            daily["cases"] == 0, np.nan, daily["an"] / daily["cases"]
        )
        np.testing.assert_allclose(
            daily["af"].to_numpy(), recomputed_af, equal_nan=True, atol=1e-9
        )


class TestShapeAndOutput:
    def test_output_keys(self, fit):
        af = sus_mod_af(fit, nsim=0, verbose=False)
        for key in ("total", "by_quantile", "by_period", "daily", "custom", "meta"):
            assert key in af

    def test_total_has_three_components(self, fit):
        af = sus_mod_af(fit, nsim=0, verbose=False)
        assert set(af["total"]["component"]) == {"total", "heat", "cold"}

    def test_by_period_none_without_by(self, fit):
        af = sus_mod_af(fit, nsim=0, verbose=False)
        assert af["by_period"] is None

    def test_by_period_year(self, fit):
        af = sus_mod_af(fit, nsim=0, by="year", verbose=False)
        assert af["by_period"] is not None
        assert "year" in af["by_period"].columns

    def test_by_period_season(self, fit):
        af = sus_mod_af(fit, nsim=0, by="season", verbose=False)
        assert af["by_period"] is not None
        assert "season" in af["by_period"].columns

    def test_custom_range(self, fit):
        af = sus_mod_af(fit, nsim=0, range=(20.0, 25.0), verbose=False)
        assert af["custom"] is not None
        assert af["custom"]["range_lo"].iloc[0] == 20.0
        assert af["custom"]["range_hi"].iloc[0] == 25.0

    def test_custom_none_without_range(self, fit):
        af = sus_mod_af(fit, nsim=0, verbose=False)
        assert af["custom"] is None

    def test_ci_method_reflects_nsim(self, fit):
        af_delta = sus_mod_af(fit, nsim=0, verbose=False)
        assert af_delta["meta"]["ci_method"] == "delta"
        af_mc = sus_mod_af(fit, nsim=50, verbose=False)
        assert af_mc["meta"]["ci_method"] == "monte_carlo"

    def test_monte_carlo_ci_brackets_point_estimate(self, fit):
        af = sus_mod_af(fit, nsim=500, alpha=0.05, verbose=False)
        row = af["total"].iloc[0]
        assert row["af_lo"] <= row["af"] <= row["af_hi"]

    def test_threshold_override(self, fit):
        af_default = sus_mod_af(fit, nsim=0, verbose=False)
        af_custom = sus_mod_af(fit, nsim=0, threshold=26.0, verbose=False)
        assert af_default["meta"]["threshold"] != af_custom["meta"]["threshold"]
        assert af_custom["meta"]["threshold"] == 26.0


class TestValidation:
    def test_bad_by_raises(self, fit):
        with pytest.raises(ValueError):
            sus_mod_af(fit, by="decade", verbose=False)
