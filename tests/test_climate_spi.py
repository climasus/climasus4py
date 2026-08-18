"""Tests for sus_climate_compute_spi."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_spi import sus_climate_compute_spi


def _make_monthly_df(n_months: int = 60, n_muni: int = 2, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2015-01-01", periods=n_months, freq="MS")
    frames = []
    for i in range(n_muni):
        rain = rng.gamma(shape=2.0, scale=50.0, size=n_months)
        frames.append(pd.DataFrame({
            "code_muni": [f"310{i}0"] * n_months,
            "date": dates,
            "rainfall_chirps_mm": rain,
        }))
    return pd.concat(frames, ignore_index=True)


def test_missing_scipy_raises_friendly_error():
    try:
        import scipy  # noqa: F401
        pytest.skip("scipy is installed — cannot test the missing-dependency path")
    except ImportError:
        pass
    df = _make_monthly_df(n_months=36, n_muni=1)
    with pytest.raises(ImportError, match="scipy is required"):
        sus_climate_compute_spi(df, scales=[1], min_n=2, verbose=False)


class TestValidation:
    def test_missing_column_raises(self):
        df = pd.DataFrame({"code_muni": ["3100"], "date": ["2020-01-01"]})
        with pytest.raises(ValueError, match="rainfall_chirps_mm"):
            sus_climate_compute_spi(df, verbose=False)

    def test_invalid_scales_raises(self):
        df = _make_monthly_df()
        with pytest.raises(ValueError, match="scales"):
            sus_climate_compute_spi(df, scales=[0], verbose=False)

    def test_invalid_ref_period_raises(self):
        df = _make_monthly_df()
        with pytest.raises(ValueError, match="ref_start"):
            sus_climate_compute_spi(
                df, ref_start="2020-01-01", ref_end="2019-01-01", verbose=False
            )

    def test_invalid_min_n_raises(self):
        df = _make_monthly_df()
        with pytest.raises(ValueError, match="min_n"):
            sus_climate_compute_spi(df, min_n=1, verbose=False)


class TestComputation:
    """Requires scipy (not a base dependency — see IDEIAS.md)."""

    def setup_method(self):
        pytest.importorskip("scipy", reason="scipy not installed")

    def test_adds_one_column_per_scale(self):
        df = _make_monthly_df(n_months=60)
        out = sus_climate_compute_spi(df, scales=[1, 3, 6], verbose=False)
        assert {"spi_1mo", "spi_3mo", "spi_6mo"} <= set(out.columns)

    def test_short_series_below_min_n_is_all_nan(self):
        df = _make_monthly_df(n_months=10, n_muni=1)
        out = sus_climate_compute_spi(df, scales=[1], min_n=24, verbose=False)
        assert out["spi_1mo"].isna().all()

    def test_spi_has_approximately_standard_normal_moments(self):
        # A long, well-behaved gamma-distributed series should produce
        # SPI values with mean ~0 and sd ~1 over the calibration period,
        # by construction of the gamma -> normal transform.
        df = _make_monthly_df(n_months=240, n_muni=1, seed=1)
        out = sus_climate_compute_spi(df, scales=[1], min_n=24, verbose=False)
        valid = out["spi_1mo"].dropna()
        assert len(valid) > 100
        assert abs(valid.mean()) < 0.3
        assert 0.7 < valid.std() < 1.3

    def test_sus_meta_attrs(self):
        df = _make_monthly_df(n_months=36, n_muni=1)
        out = sus_climate_compute_spi(df, scales=[1], verbose=False)
        meta = out.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "spi"

    def test_looks_daily_warns(self):
        dates = pd.date_range("2020-01-01", periods=400, freq="D")
        df = pd.DataFrame({
            "code_muni": ["3100"] * 400,
            "date": dates,
            "rainfall_chirps_mm": np.random.default_rng(0).gamma(2, 5, 400),
        })
        with pytest.warns(UserWarning, match="dai|diári|diari"):
            sus_climate_compute_spi(df, scales=[1], min_n=2, verbose=False)
