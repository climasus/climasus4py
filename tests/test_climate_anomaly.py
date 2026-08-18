"""Tests for sus_climate_anomaly."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment.climate_anomaly import sus_climate_anomaly


def _make_observed(n_years: int = 3, station_code: str = "A701", seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2018-01-01", periods=365 * n_years, freq="D")
    seasonal = 3 * np.sin(2 * np.pi * dates.dayofyear.to_numpy() / 365)
    tmax = 30.0 + seasonal + rng.normal(0, 0.5, len(dates))
    return pd.DataFrame({
        "station_code": station_code,
        "date": dates,
        "tair_max_c": tmax,
    })


def _make_normals(station_code: str = "A701") -> pd.DataFrame:
    rows = []
    for month_num, month_pt in [
        (1, "janeiro"), (2, "fevereiro"), (3, "marco"), (4, "abril"),
        (5, "maio"), (6, "junho"), (7, "julho"), (8, "agosto"),
        (9, "setembro"), (10, "outubro"), (11, "novembro"), (12, "dezembro"),
    ]:
        # A mild seasonal cycle matching _make_observed's sine wave roughly.
        base = 30.0 + 3 * np.sin(2 * np.pi * (month_num * 30) / 365)
        for decada in ("1", "2", "3"):
            rows.append({
                "codigo": station_code,
                "mes": month_pt,
                "decada": decada,
                "var_code": "t_max",
                "valor": base,
            })
    df = pd.DataFrame(rows)
    df.attrs["sus_meta"] = {"period": "1991-2020"}
    return df


class TestValidation:
    def test_missing_station_col_raises(self):
        obs = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)})
        norm = _make_normals()
        with pytest.raises(ValueError, match="station_code|station_col"):
            sus_climate_anomaly(obs, norm, verbose=False)

    def test_missing_normals_columns_raises(self):
        obs = _make_observed()
        norm = pd.DataFrame({"codigo": ["A701"]})
        with pytest.raises(ValueError, match="mes|var_code|valor"):
            sus_climate_anomaly(obs, norm, verbose=False)

    def test_no_mappable_vars_raises(self):
        obs = pd.DataFrame({
            "station_code": ["A701"], "date": ["2020-01-01"], "unrelated_col": [1.0],
        })
        norm = _make_normals()
        with pytest.raises(ValueError, match="vari|var"):
            sus_climate_anomaly(obs, norm, verbose=False)

    def test_explicit_vars_not_in_observed_raises(self):
        obs = _make_observed()
        norm = _make_normals()
        with pytest.raises(ValueError):
            sus_climate_anomaly(obs, norm, vars={"nonexistent_col": "t_max"}, verbose=False)

    def test_no_matching_station_yields_nan_normals_not_error(self):
        # A left join never produces zero rows unless the left side is
        # already empty — the R source's `nrow(joined) == 0` guard is
        # effectively dead code for a non-empty `observed`. Preserved
        # as-is (see IDEIAS.md); this just documents that behavior.
        obs = _make_observed(station_code="A701")
        norm = _make_normals(station_code="B999")
        result = sus_climate_anomaly(obs, norm, verbose=False)
        assert result["tair_max_c_normal"].isna().all()


class TestComputation:
    def test_absolute_method_adds_expected_columns(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, method="absolute", verbose=False)
        assert {"tair_max_c_obs", "tair_max_c_normal", "tair_max_c_anomaly"} <= set(
            result.columns
        )
        assert "tair_max_c_anomaly_pct" not in result.columns
        assert "tair_max_c_anomaly_std" not in result.columns

    def test_relative_method_adds_pct_column(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, method="relative", verbose=False)
        assert "tair_max_c_anomaly_pct" in result.columns

    def test_standardized_method_adds_std_column(self):
        obs = _make_observed(n_years=4)
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, method="standardized", verbose=False)
        assert "tair_max_c_anomaly_std" in result.columns

    def test_all_method_adds_every_column(self):
        obs = _make_observed(n_years=4)
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, method="all", verbose=False)
        for suffix in ("_obs", "_normal", "_anomaly", "_anomaly_pct", "_anomaly_std"):
            assert f"tair_max_c{suffix}" in result.columns

    def test_decadal_time_scale_adds_decade_num(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, time_scale="decadal", verbose=False)
        assert "decade_num" in result.columns
        assert set(result["decade_num"].unique()) <= {1, 2, 3}

    def test_month_name_is_portuguese(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, verbose=False)
        assert set(result["month_name"].dropna().unique()) <= {
            "janeiro", "fevereiro", "marco", "abril", "maio", "junho",
            "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
        }

    def test_explicit_vars_mapping(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(
            obs, norm, vars={"tair_max_c": "t_max"}, verbose=False
        )
        assert "tair_max_c_anomaly" in result.columns

    def test_sus_meta_attrs(self):
        obs = _make_observed()
        norm = _make_normals()
        result = sus_climate_anomaly(obs, norm, method="all", verbose=False)
        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "anomaly"
        assert meta["normal_period"] == "1991-2020"
        assert meta["method"] == "all"
