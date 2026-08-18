"""Tests for sus_mod_dlnm (Distributed Lag Non-linear Model).

Includes a numeric-parity regression test against real output from R's
`dlnm`/`climasus4r` packages, computed once on a fixed synthetic dataset
(`tests/fixtures/dlnm_synthetic_input.csv`) and pinned as constants below —
see the module docstring in `climasus4py/enrichment/mod_dlnm.py` for how
this was generated and validated.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.mod_dlnm import sus_mod_dlnm

FIXTURE = Path(__file__).parent / "fixtures" / "dlnm_synthetic_input.csv"

# Pinned ground truth from real R climasus4r::sus_mod_dlnm() on the fixture
# dataset (argvar=ns(df=4), arglag=ns(df=3), family="quasipoisson",
# dof_per_year=4L, lag_max=14).
R_EXPOSURE_RESPONSE = pd.DataFrame(
    {
        "pct": [0.25, 0.50, 0.75, 0.90, 0.95, 0.99],
        "exposure": [20.745, 24.415, 27.314, 28.914, 29.793, 31.082],
        "rr": [0.9539, 1.0000, 0.9242, 0.9040, 0.9034, 0.9134],
        "lo": [0.7889, 0.9988, 0.7630, 0.6947, 0.6694, 0.6286],
        "hi": [1.1541, 1.0008, 1.1196, 1.1755, 1.2189, 1.3273],
    }
)
R_RR_P75 = 0.924222889822329
R_LO_P75 = 0.762961799047585
R_HI_P75 = 1.1195684386005
R_LAG_PEAK = 0
R_DISP_RATIO = 1.049562
R_AIC_POISSON = 4646.505
R_AUTOCORR_PVAL = 0.4314884


@pytest.fixture
def synthetic_df() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE, parse_dates=["date"])
    return df


class TestNumericParityWithR:
    def test_exposure_response_matches_r(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            outcome_col="n_obitos",
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            argvar={"fun": "ns", "df": 4},
            arglag={"fun": "ns", "df": 3},
            family="quasipoisson",
            dof_per_year=4,
            verbose=False,
        )
        py = fit["exposure_response"]
        for col in ("rr", "lo", "hi"):
            np.testing.assert_allclose(
                py[col].to_numpy(), R_EXPOSURE_RESPONSE[col].to_numpy(), atol=1e-3
            )
        np.testing.assert_allclose(
            py["exposure"].to_numpy(), R_EXPOSURE_RESPONSE["exposure"].to_numpy(), atol=1e-2
        )

    def test_models_summary_matches_r(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            outcome_col="n_obitos",
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            dof_per_year=4,
            verbose=False,
        )
        m = fit["models"].iloc[0]
        assert m["rr"] == pytest.approx(R_RR_P75, abs=1e-3)
        assert m["lo"] == pytest.approx(R_LO_P75, abs=1e-3)
        assert m["hi"] == pytest.approx(R_HI_P75, abs=1e-3)
        assert int(m["lag_peak"]) == R_LAG_PEAK

    def test_diagnostics_match_r(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            outcome_col="n_obitos",
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            dof_per_year=4,
            verbose=False,
        )
        diag = fit["diagnostics"]
        assert diag["disp_ratio"] == pytest.approx(R_DISP_RATIO, abs=1e-3)
        assert diag["aic_poisson"] == pytest.approx(R_AIC_POISSON, abs=0.1)
        assert diag["autocorr_pval"] == pytest.approx(R_AUTOCORR_PVAL, abs=1e-3)
        assert diag["has_autocorr"] is False

    def test_lag_response_cumprod_matches_r_shape(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            outcome_col="n_obitos",
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            dof_per_year=4,
            verbose=False,
        )
        lag_resp = fit["lag_response"]
        assert list(lag_resp["lag"]) == list(range(15))
        np.testing.assert_allclose(
            lag_resp["rr_cum"].to_numpy(), np.cumprod(lag_resp["rr"].to_numpy())
        )
        assert lag_resp["rr_cum"].iloc[-1] == pytest.approx(R_RR_P75, abs=1e-3)


class TestShapeAndOutput:
    def test_output_keys(self, synthetic_df):
        fit = sus_mod_dlnm(synthetic_df, climate_col="tair_dry_bulb_c", lag_max=14, verbose=False)
        for key in (
            "model",
            "crossbasis",
            "pred",
            "exposure_response",
            "lag_response",
            "models",
            "data_daily",
            "diagnostics",
            "meta",
        ):
            assert key in fit

    def test_crossbasis_shape(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            argvar={"fun": "ns", "df": 4},
            arglag={"fun": "ns", "df": 3},
            verbose=False,
        )
        # df_var=4, df_lag=3 -> 12 crossbasis columns
        assert fit["crossbasis"].shape[1] == 12

    def test_auto_detect_climate_col_and_lag_max(self, synthetic_df):
        fit = sus_mod_dlnm(synthetic_df, verbose=False)
        assert fit["meta"]["climate_col"] == "tair_dry_bulb_c"
        assert fit["meta"]["lag_max"] == 14

    def test_ns_df_zero_suppresses_time_control(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df, climate_col="tair_dry_bulb_c", lag_max=14, ns_df=0, verbose=False
        )
        assert fit["meta"]["ns_df"] == 0

    def test_lin_basis_runs(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            argvar={"fun": "lin"},
            verbose=False,
        )
        assert fit["crossbasis"].shape[1] == 3  # df_var=1 * df_lag=3

    def test_negbin_redirects_to_quasipoisson(self, synthetic_df):
        fit = sus_mod_dlnm(
            synthetic_df,
            climate_col="tair_dry_bulb_c",
            lag_max=14,
            family="negbin",
            verbose=False,
        )
        assert fit["meta"]["family"] == "quasipoisson"


class TestValidation:
    def test_missing_outcome_col_raises(self, synthetic_df):
        with pytest.raises(ValueError, match="n_missing"):
            sus_mod_dlnm(synthetic_df, outcome_col="n_missing", verbose=False)

    def test_missing_lag_cols_raises(self, synthetic_df):
        with pytest.raises(ValueError):
            sus_mod_dlnm(synthetic_df, climate_col="nonexistent_var", verbose=False)

    def test_unsupported_basis_fun_raises(self, synthetic_df):
        with pytest.raises(ValueError, match="strata"):
            sus_mod_dlnm(
                synthetic_df,
                climate_col="tair_dry_bulb_c",
                argvar={"fun": "strata"},
                verbose=False,
            )

    def test_bad_covariate_raises(self, synthetic_df):
        with pytest.raises(ValueError):
            sus_mod_dlnm(
                synthetic_df,
                climate_col="tair_dry_bulb_c",
                covariates=["not_a_column"],
                verbose=False,
            )
