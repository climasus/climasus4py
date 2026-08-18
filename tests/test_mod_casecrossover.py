"""Tests for sus_mod_casecrossover."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.mod_casecrossover import (
    CaseCrossoverResult,
    sus_mod_casecrossover,
)


def _make_daily_df(n_days: int = 900, seed: int = 0, beta: float = 0.04) -> pd.DataFrame:
    """Synthetic daily data with a KNOWN positive exposure -> outcome effect.

    Outcome counts are Poisson with rate depending on a seasonal stratum
    baseline plus ``beta * temperature`` — a higher temperature should
    increase the expected count, so the fitted log-rate-ratio for
    ``exposure_val`` should come out positive (OR > 1) and close to
    ``beta``.
    """
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2018-01-01", periods=n_days, freq="D")
    doy = dates.dayofyear.to_numpy()
    # Smooth seasonal temperature cycle (Southern Hemisphere-ish) + noise.
    temp = 24.0 + 6.0 * np.sin(2 * np.pi * (doy - 30) / 365.0) + rng.normal(0, 1.0, n_days)
    # Baseline log-rate varies by month (absorbed by strata) + the injected effect.
    month_effect = 0.05 * np.sin(2 * np.pi * dates.month.to_numpy() / 12.0)
    log_rate = 1.2 + month_effect + beta * (temp - temp.mean())
    counts = rng.poisson(np.exp(log_rate))
    return pd.DataFrame({"date": dates, "n_obitos": counts, "tair_dry_bulb_c": temp})


class TestValidation:
    def test_missing_date_column_raises(self):
        df = pd.DataFrame({"n_obitos": [1, 2], "tair_dry_bulb_c": [20.0, 21.0]})
        with pytest.raises(ValueError, match="date"):
            sus_mod_casecrossover(df, exposure_col="tair_dry_bulb_c", verbose=False)

    def test_missing_outcome_column_raises(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="n_obitos"):
            sus_mod_casecrossover(
                df.drop(columns=["n_obitos"]),
                exposure_col="tair_dry_bulb_c",
                verbose=False,
            )

    def test_missing_exposure_col_arg_raises(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="missing"):
            sus_mod_casecrossover(df, verbose=False)

    def test_unknown_exposure_column_raises(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="not_a_col"):
            sus_mod_casecrossover(df, exposure_col="not_a_col", verbose=False)

    def test_bad_method_raises(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="method"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", method="bogus", verbose=False
            )

    def test_bad_stratum_raises(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="stratum"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", stratum="not_a_col", verbose=False
            )

    def test_bad_covariates_raise(self):
        df = _make_daily_df(n_days=60)
        with pytest.raises(ValueError, match="not_a_col"):
            sus_mod_casecrossover(
                df,
                exposure_col="tair_dry_bulb_c",
                covariates=["not_a_col"],
                verbose=False,
            )

    def test_no_cases_raises(self):
        df = _make_daily_df(n_days=60)
        df["n_obitos"] = 0
        with pytest.raises(ValueError, match="cases"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", lang="en", verbose=False
            )

    def test_few_strata_warns(self):
        df = _make_daily_df(n_days=20)  # < 1 month => 1 stratum
        with pytest.warns(UserWarning, match="strata"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", lang="en", verbose=False
            )

    def test_unsupported_lang_warns_and_falls_back(self):
        df = _make_daily_df(n_days=60)
        with pytest.warns(UserWarning, match="Idioma|Unsupported|Idioma"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", lang="fr", verbose=False
            )


class TestConditionalPoisson:
    """method="conditional_poisson" via statsmodels GLM."""

    def test_recovers_known_positive_effect(self):
        statsmodels = pytest.importorskip("statsmodels")
        del statsmodels
        df = _make_daily_df(n_days=1000, beta=0.04, seed=1)
        result = sus_mod_casecrossover(
            df,
            outcome_col="n_obitos",
            exposure_col="tair_dry_bulb_c",
            stratum="month",
            lag=0,
            method="conditional_poisson",
            verbose=False,
        )
        assert isinstance(result, CaseCrossoverResult)
        assert len(result.or_table) == 1
        row = result.or_table.iloc[0]
        # Direction: positive injected effect -> OR (here, a rate ratio) > 1.
        assert row["or"] > 1.0
        assert row["or_lo"] < row["or"] < row["or_hi"]
        # Magnitude: estimated log-rate-ratio should be within a reasonable
        # band around the injected beta=0.04 (per-degree-C effect).
        assert 0.01 < row["estimate"] < 0.08
        assert result.diagnostics["n_cases"] > 0
        assert result.diagnostics["method"] == "conditional_poisson"

    def test_no_effect_gives_null_result(self):
        pytest.importorskip("statsmodels")
        df = _make_daily_df(n_days=1000, beta=0.0, seed=2)
        result = sus_mod_casecrossover(
            df, exposure_col="tair_dry_bulb_c", method="conditional_poisson", verbose=False
        )
        row = result.or_table.iloc[0]
        # No true effect: estimate should be small in magnitude either way.
        assert abs(row["estimate"]) < 0.03

    def test_moving_average_lag(self):
        pytest.importorskip("statsmodels")
        df = _make_daily_df(n_days=1000, beta=0.04, seed=3)
        result = sus_mod_casecrossover(
            df, exposure_col="tair_dry_bulb_c", lag=list(range(7)), verbose=False
        )
        assert result.meta["lag"] == [0, 1, 2, 3, 4, 5, 6]
        assert result.data["exposure_val"].notna().any()

    def test_missing_statsmodels_raises_friendly_error(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("statsmodels"):
                raise ImportError("mocked: no statsmodels")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        df = _make_daily_df(n_days=60)
        with pytest.raises(ImportError, match="statsmodels"):
            sus_mod_casecrossover(df, exposure_col="tair_dry_bulb_c", verbose=False)


class TestClogit:
    """method="clogit" via the lifelines Cox-model trick."""

    def test_recovers_known_positive_effect(self):
        pytest.importorskip("lifelines")
        df = _make_daily_df(n_days=1200, beta=0.06, seed=4)
        with pytest.warns(UserWarning, match="clogit|binary|binario|binario"):
            result = sus_mod_casecrossover(
                df,
                exposure_col="tair_dry_bulb_c",
                method="clogit",
                verbose=False,
            )
        assert len(result.or_table) == 1
        row = result.or_table.iloc[0]
        assert row["or"] > 1.0
        assert row["or_lo"] < row["or"] < row["or_hi"]
        assert result.diagnostics["family"] == "binomial"

    def test_missing_lifelines_raises_friendly_error(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("lifelines"):
                raise ImportError("mocked: no lifelines")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        df = _make_daily_df(n_days=60)
        with pytest.raises(ImportError, match="lifelines"):
            sus_mod_casecrossover(
                df, exposure_col="tair_dry_bulb_c", method="clogit", verbose=False
            )
