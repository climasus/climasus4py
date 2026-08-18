"""Tests for sus_mod_its."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("statsmodels")
pytest.importorskip("scipy")

from climasus4py.enrichment.mod_its import ClimasusITS, sus_mod_its  # noqa: E402


def _make_its_df(
    n_days: int = 1500,
    intervention: str = "2020-06-01",
    b0: float = 3.0,
    b_trend: float = 0.0002,
    b_step: float = -0.5,
    b_slope: float = 0.001,
    seed: int = 42,
) -> tuple[pd.DataFrame, float, float]:
    """Synthetic daily count series with a known injected level+slope change.

    log(mu_t) = b0 + b_trend*t + b_step*1(t>=T) + b_slope*max(0, t-T)
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2018-01-01", periods=n_days, freq="D")
    t_int = pd.Timestamp(intervention)
    t = np.arange(n_days, dtype=float)
    t0 = (t_int - dates[0]).days

    step = (t >= t0).astype(float)
    slope = np.maximum(0.0, t - t0)

    log_mu = b0 + b_trend * t + b_step * step + b_slope * slope
    mu = np.exp(log_mu)
    counts = rng.poisson(mu)

    df = pd.DataFrame({"date": dates, "n_obitos": counts})
    return df, b_step, b_slope


class TestRecoversInjectedEffect:
    def test_level_and_slope_change_recovered(self):
        df, b_step, b_slope = _make_its_df()
        result = sus_mod_its(
            df,
            outcome_col="n_obitos",
            interruption_dates="2020-06-01",
            harmonics=0,
            family="poisson",
            counterfactual=True,
            verbose=False,
        )

        assert isinstance(result, ClimasusITS)
        row = result.effects.iloc[0]

        # Injected step is negative (b_step=-0.5) -> level_ratio should be < 1
        # and in the right ballpark of exp(-0.5) ~= 0.607.
        assert row["level_ratio"] < 1.0
        assert 0.45 < row["level_ratio"] < 0.85

        # Injected slope is positive (b_slope=0.001/day) -> recovered
        # slope_daily_log should be positive and near 0.001, and the
        # annualised ratio near exp(365.25*0.001) ~= 1.44.
        assert 0.0005 < row["slope_daily_log"] < 0.0015
        assert 1.2 < row["slope_ratio_annual"] < 1.7

        # Both effects should be clearly significant given the strong signal
        # and large synthetic sample.
        assert row["level_p"] < 0.01
        assert row["slope_p"] < 0.01

    def test_counterfactual_and_segments_shapes(self):
        df, _, _ = _make_its_df()
        result = sus_mod_its(
            df,
            outcome_col="n_obitos",
            interruption_dates="2020-06-01",
            harmonics=1,
            counterfactual=True,
            verbose=False,
        )
        assert result.counterfactual is not None
        assert len(result.counterfactual) == len(df)
        assert set(result.segments["segment"]) == {"Pre-interruption", "Post-interruption"}
        # Counterfactual should generally exceed observed post-intervention
        # (b_step is a strong negative level drop -> "prevented" positive on
        # average after the intervention).
        post_mask = result.counterfactual["date"] >= pd.Timestamp("2020-06-01")
        assert result.counterfactual.loc[post_mask, "prevented"].mean() > 0

    def test_multiple_interruptions(self):
        n_days = 2200
        rng = np.random.default_rng(7)
        dates = pd.date_range("2016-01-01", periods=n_days, freq="D")
        t = np.arange(n_days, dtype=float)
        t1 = (pd.Timestamp("2018-06-01") - dates[0]).days
        t2 = (pd.Timestamp("2020-03-17") - dates[0]).days
        log_mu = (
            3.0
            + 0.0001 * t
            - 0.4 * (t >= t1)
            + 0.3 * (t >= t2)
        )
        df = pd.DataFrame({"date": dates, "n_obitos": rng.poisson(np.exp(log_mu))})

        result = sus_mod_its(
            df,
            outcome_col="n_obitos",
            interruption_dates=["2018-06-01", "2020-03-17"],
            harmonics=0,
            family="poisson",
            verbose=False,
        )
        assert len(result.effects) == 2
        assert result.effects.iloc[0]["level_ratio"] < 1.0
        assert result.effects.iloc[1]["level_ratio"] > 1.0
        assert list(result.segments["segment"]) == [
            "Pre-interruption",
            "Post-interruption 1",
            "Post-interruption 2",
        ]

    def test_no_counterfactual_when_disabled(self):
        df, _, _ = _make_its_df(n_days=800)
        result = sus_mod_its(
            df,
            outcome_col="n_obitos",
            interruption_dates="2019-06-01",
            counterfactual=False,
            verbose=False,
        )
        assert result.counterfactual is None

    def test_tidy_prepends_metadata(self):
        df, _, _ = _make_its_df(n_days=800)
        result = sus_mod_its(
            df,
            outcome_col="n_obitos",
            interruption_dates="2019-06-01",
            counterfactual=False,
            verbose=False,
        )
        tidy_df = result.tidy()
        assert "n_obs" in tidy_df.columns
        assert "level_ratio" in tidy_df.columns
        assert len(tidy_df) == 1


class TestValidation:
    def test_missing_interruption_dates_raises(self):
        df, _, _ = _make_its_df(n_days=500)
        with pytest.raises(ValueError, match="interruption_dates"):
            sus_mod_its(df, outcome_col="n_obitos", verbose=False)

    def test_missing_date_col_raises(self):
        df, _, _ = _make_its_df(n_days=500)
        df = df.rename(columns={"date": "dt"})
        with pytest.raises(ValueError, match="date"):
            sus_mod_its(
                df, outcome_col="n_obitos", interruption_dates="2019-06-01", verbose=False
            )

    def test_missing_outcome_col_raises(self):
        df, _, _ = _make_its_df(n_days=500)
        with pytest.raises(ValueError, match="obitos"):
            sus_mod_its(
                df, outcome_col="n_obitos_xyz", interruption_dates="2019-06-01", verbose=False
            )

    def test_bad_family_raises(self):
        df, _, _ = _make_its_df(n_days=500)
        with pytest.raises(ValueError, match="family"):
            sus_mod_its(
                df,
                outcome_col="n_obitos",
                interruption_dates="2019-06-01",
                family="gaussian",
                verbose=False,
            )

    def test_interruption_outside_range_raises(self):
        df, _, _ = _make_its_df(n_days=500)
        with pytest.raises(ValueError, match="intervalo"):
            sus_mod_its(
                df, outcome_col="n_obitos", interruption_dates="2030-01-01", verbose=False
            )
