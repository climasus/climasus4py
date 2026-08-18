"""Tests for sus_climate_compute_spei."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_spei import sus_climate_compute_spei


def _make_monthly_df(n_months: int = 60, n_muni: int = 2, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2015-01-01", periods=n_months, freq="MS")
    frames = []
    for i in range(n_muni):
        rain = rng.gamma(shape=2.0, scale=50.0, size=n_months)
        pet = rng.normal(loc=90.0, scale=10.0, size=n_months)
        temp = 20 + 8 * np.sin(2 * np.pi * (dates.month.to_numpy() / 12)) + rng.normal(
            0, 1, n_months
        )
        frames.append(pd.DataFrame({
            "code_muni": [f"310{i}0"] * n_months,
            "date": dates,
            "rainfall_chirps_mm": rain,
            "pet_mm": pet,
            "tair_dry_bulb_c": temp,
        }))
    return pd.concat(frames, ignore_index=True)


class TestValidation:
    def test_missing_column_raises(self):
        df = pd.DataFrame({"code_muni": ["3100"], "date": ["2020-01-01"]})
        with pytest.raises(ValueError, match="rainfall_chirps_mm|precipita"):
            sus_climate_compute_spei(df, verbose=False)

    def test_invalid_pet_method_raises(self):
        df = _make_monthly_df()
        with pytest.raises(ValueError, match="pet_method"):
            sus_climate_compute_spei(df, pet_method="bogus", verbose=False)

    def test_missing_pet_column_raises(self):
        df = _make_monthly_df().drop(columns=["pet_mm"])
        with pytest.raises(ValueError, match="pet_mm|PET"):
            sus_climate_compute_spei(df, pet_method="column", verbose=False)

    def test_missing_temp_column_for_thornthwaite_raises(self):
        df = _make_monthly_df().drop(columns=["tair_dry_bulb_c"])
        with pytest.raises(ValueError, match="tair_dry_bulb_c|temperatura|temperature"):
            sus_climate_compute_spei(df, pet_method="thornthwaite", verbose=False)

    def test_invalid_ref_period_raises(self):
        df = _make_monthly_df()
        with pytest.raises(ValueError, match="ref_start"):
            sus_climate_compute_spei(
                df, ref_start="2020-01-01", ref_end="2019-01-01", verbose=False
            )


class TestComputation:
    def setup_method(self):
        pytest.importorskip("scipy", reason="scipy not installed")

    def test_adds_one_column_per_scale_column_method(self):
        df = _make_monthly_df(n_months=60)
        out = sus_climate_compute_spei(
            df, pet_method="column", pet_var="pet_mm", scales=[1, 3, 6], verbose=False
        )
        assert {"spei_1mo", "spei_3mo", "spei_6mo"} <= set(out.columns)
        assert "_pet_thornthwaite" not in out.columns

    def test_thornthwaite_method_adds_columns_and_drops_internal_pet(self):
        df = _make_monthly_df(n_months=60, n_muni=1)
        out = sus_climate_compute_spei(
            df, pet_method="thornthwaite", temp_var="tair_dry_bulb_c",
            scales=[3], verbose=False,
        )
        assert "spei_3mo" in out.columns
        assert "_pet_thornthwaite" not in out.columns

    def test_short_series_below_min_n_is_all_nan(self):
        df = _make_monthly_df(n_months=10, n_muni=1)
        out = sus_climate_compute_spei(df, scales=[1], min_n=24, verbose=False)
        assert out["spei_1mo"].isna().all()

    def test_spei_has_approximately_standard_normal_moments(self):
        df = _make_monthly_df(n_months=240, n_muni=1, seed=1)
        out = sus_climate_compute_spei(df, scales=[1], min_n=24, verbose=False)
        valid = out["spei_1mo"].dropna()
        assert len(valid) > 100
        assert abs(valid.mean()) < 0.3
        assert 0.7 < valid.std() < 1.3

    def test_sus_meta_attrs(self):
        df = _make_monthly_df(n_months=36, n_muni=1)
        out = sus_climate_compute_spei(df, scales=[1], verbose=False)
        meta = out.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "spei"

    def test_thornthwaite_pet_is_nonnegative(self):
        df = _make_monthly_df(n_months=36, n_muni=1)
        # Thornthwaite PET must never be negative for any real temperature input.
        from climasus4py.enrichment.climate_spei import _thornthwaite_pet
        pet = _thornthwaite_pet(df.sort_values(["code_muni", "date"]), temp_var="tair_dry_bulb_c")
        assert np.all(pet[~np.isnan(pet)] >= 0)
