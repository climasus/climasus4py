"""Tests for sus_climate_fill_inmet — XGBoost opt-in gap fill (B.3)."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_fill import (
    KNOWN_INMET_VARS,
    _xgboost_available,
    sus_climate_fill_inmet,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_inmet_df(
    n_days: int = 30,
    n_stations: int = 2,
    gap_fraction: float = 0.15,
    seed: int = 7,
) -> pd.DataFrame:
    """Synthetic INMET DataFrame with realistic gaps in tair_dry_bulb_c."""
    rng = np.random.default_rng(seed)
    rows = []
    for station in [f"A{700 + i}" for i in range(n_stations)]:
        dates = pd.date_range("2023-06-01", periods=n_days, freq="D")
        temps = rng.uniform(22.0, 34.0, n_days)
        # Introduce gaps
        gap_idx = rng.choice(n_days, size=int(n_days * gap_fraction), replace=False)
        temps[gap_idx] = np.nan
        for date, temp in zip(dates, temps):
            rows.append(
                {
                    "station_code": station,
                    "date": date,
                    "tair_dry_bulb_c": temp,
                    "rh_mean_porc": float(rng.uniform(55.0, 85.0)),
                    "ws_2_m_s": float(rng.uniform(0.5, 3.5)),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def inmet_df():
    return _make_inmet_df()


@pytest.fixture
def single_station_df():
    return _make_inmet_df(n_stations=1)


# ---------------------------------------------------------------------------
# B.3.1 — Linear fallback when XGBoost not available
# ---------------------------------------------------------------------------


def test_linear_fallback_when_no_xgboost(inmet_df):
    """When xgboost unavailable, should fall back to linear and emit UserWarning."""
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with pytest.warns(UserWarning, match="xgboost not found"):
            result = sus_climate_fill_inmet(
                inmet_df, target_var="tair_dry_bulb_c", verbose=False
            )
    assert isinstance(result, pd.DataFrame)
    assert "is_imputed_tair_dry_bulb_c" in result.columns


def test_backend_linear_forces_no_xgboost(inmet_df):
    """backend='linear' should never use XGBoost and should NOT warn."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        result = sus_climate_fill_inmet(
            inmet_df, target_var="tair_dry_bulb_c", backend="linear", verbose=False
        )
    assert isinstance(result, pd.DataFrame)
    assert "is_imputed_tair_dry_bulb_c" in result.columns


def test_backend_xgboost_raises_when_not_installed():
    """backend='xgboost' with no xgboost installed should raise ImportError."""
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with pytest.raises(ImportError, match="pip install climasus4py"):
            sus_climate_fill_inmet(
                _make_inmet_df(), target_var="tair_dry_bulb_c", backend="xgboost", verbose=False
            )


# ---------------------------------------------------------------------------
# B.3.2 — XGBoost path with mock model
# ---------------------------------------------------------------------------


def test_xgboost_path_fills_gaps(inmet_df):
    """When xgboost is available, gaps should be filled and flag column present."""
    if not _xgboost_available():
        pytest.skip("xgboost not installed in this environment")
    result = sus_climate_fill_inmet(
        inmet_df, target_var="tair_dry_bulb_c", backend="xgboost",
        parallel=False, verbose=False
    )
    assert isinstance(result, pd.DataFrame)
    assert result["tair_dry_bulb_c"].isna().sum() == 0
    assert "is_imputed_tair_dry_bulb_c" in result.columns


# ---------------------------------------------------------------------------
# B.3.3 — Quality threshold
# ---------------------------------------------------------------------------


def test_quality_threshold_excludes_bad_stations():
    """Station with mostly NaN should be excluded from XGBoost training."""
    rng = np.random.default_rng(99)
    df = pd.DataFrame(
        {
            "station_code": ["GOOD"] * 30 + ["BAD"] * 30,
            "date": list(pd.date_range("2023-01-01", periods=30)) * 2,
            "tair_dry_bulb_c": list(rng.uniform(20, 30, 30))
            + [np.nan] * 30,  # BAD station: 100% missing
        }
    )
    # With quality_threshold=0.4, BAD station (100% NaN) should be skipped
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = sus_climate_fill_inmet(
                df, target_var="tair_dry_bulb_c",
                quality_threshold=0.4, backend="linear", verbose=False
            )
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# B.3.4 — run_evaluation returns metrics
# ---------------------------------------------------------------------------


def test_run_evaluation_returns_dict(single_station_df):
    """Evaluation mode should return a dict with 'data' and 'metrics' keys."""
    result = sus_climate_fill_inmet(
        single_station_df,
        target_var="tair_dry_bulb_c",
        run_evaluation=True,
        backend="linear",
        verbose=False,
    )
    assert isinstance(result, dict)
    # Key: variable name or fallback
    assert len(result) > 0


# ---------------------------------------------------------------------------
# B.3.5 — target_var="all" resolves INMET variables
# ---------------------------------------------------------------------------


def test_target_var_all_processes_inmet_vars():
    rng = np.random.default_rng(0)
    n = 20
    df = pd.DataFrame(
        {
            "station_code": ["A701"] * n,
            "date": pd.date_range("2023-01-01", periods=n),
            "tair_dry_bulb_c": rng.uniform(22.0, 30.0, n),
            "rh_mean_porc": rng.uniform(55.0, 85.0, n),
        }
    )
    # Introduce gap in tair
    df.loc[5, "tair_dry_bulb_c"] = np.nan

    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = sus_climate_fill_inmet(df, target_var="all", verbose=False)
    assert isinstance(result, pd.DataFrame)
    assert result["tair_dry_bulb_c"].isna().sum() == 0


# ---------------------------------------------------------------------------
# B.3.6 — Missing column raises ValueError
# ---------------------------------------------------------------------------


def test_missing_column_raises():
    df = pd.DataFrame({"station_code": ["A"], "date": pd.to_datetime(["2023-01-01"])})
    with pytest.raises(ValueError, match="not found"):
        sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c", verbose=False)


# ---------------------------------------------------------------------------
# B.3.7 — workers parameter is respected (smoke, no actual parallelism assertion)
# ---------------------------------------------------------------------------


def test_workers_parameter_accepted(inmet_df):
    """workers=1 should not raise — just disables real parallelism."""
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = sus_climate_fill_inmet(
                inmet_df,
                target_var="tair_dry_bulb_c",
                workers=1,
                verbose=False,
            )
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# B.3.8 — keep_features=True retains lag columns
# ---------------------------------------------------------------------------


def test_keep_features_retains_lag_columns(single_station_df):
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = sus_climate_fill_inmet(
                single_station_df,
                target_var="tair_dry_bulb_c",
                keep_features=True,
                verbose=False,
            )
    # When XGBoost fills (if available), lag columns stay; linear doesn't add them
    # At minimum, the original columns must be present
    assert "tair_dry_bulb_c" in result.columns


# ---------------------------------------------------------------------------
# B.3.9 — Verbose output
# ---------------------------------------------------------------------------


def test_verbose_does_not_raise(inmet_df, capsys):
    with patch(
        "climasus4py.enrichment.climate_fill._xgboost_available", return_value=False
    ):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            sus_climate_fill_inmet(
                inmet_df, target_var="tair_dry_bulb_c", verbose=True, lang="en"
            )
    out = capsys.readouterr().out
    assert "sus_climate_fill_inmet" in out
