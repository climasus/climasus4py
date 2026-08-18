"""Tests for sus_grid_join."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.grid_join import sus_grid_join


def _make_health(n: int = 6) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "code_muni": ["5103403"] * n,
        "date": dates,
        "n_obitos": range(n),
    })
    df.attrs["sus_meta"] = {"stage": "spatial", "type": "health", "history": ["imported"]}
    return df


def _make_grid(n: int = 6, var: str = "t2m", type_: str = "era5_land") -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "code_muni": ["5103403"] * n,
        "date": dates,
        var: [25.0 + i for i in range(n)],
    })
    df.attrs["sus_meta"] = {"stage": "climate", "type": type_}
    return df


class TestValidation:
    def test_empty_by_raises(self):
        with pytest.raises(ValueError, match="by"):
            sus_grid_join(_make_health(), _make_grid(), by=[], verbose=False)

    def test_missing_join_col_in_health_raises(self):
        health = _make_health().drop(columns=["code_muni"])
        with pytest.raises(ValueError, match="health_data"):
            sus_grid_join(health, _make_grid(), verbose=False)

    def test_missing_join_col_in_grid_raises(self):
        grid = _make_grid().drop(columns=["code_muni"])
        with pytest.raises(ValueError, match="grid_data"):
            sus_grid_join(_make_health(), grid, verbose=False)

    def test_grid_with_only_join_keys_raises(self):
        grid = _make_grid()[["code_muni", "date"]]
        with pytest.raises(ValueError, match="grid_data"):
            sus_grid_join(_make_health(), grid, verbose=False)

    def test_column_collision_does_not_raise(self):
        # Collision is a non-fatal console notice (matches R's cli_alert_warning,
        # not an abort) — just confirm the join still completes and overwrites.
        health = _make_health()
        health["t2m"] = -1.0
        result = sus_grid_join(health, _make_grid(), verbose=False)
        assert (result["t2m"] != -1.0).any()

    def test_temporal_mismatch_does_not_raise(self):
        health = _make_health(n=2)
        grid = _make_grid(n=20)
        result = sus_grid_join(health, grid, verbose=False)
        assert len(result) == 2


class TestJoin:
    def test_adds_grid_columns_and_preserves_health_rows(self):
        health = _make_health()
        grid = _make_grid()
        result = sus_grid_join(health, grid, verbose=False)
        assert len(result) == len(health)
        assert "t2m" in result.columns
        assert "n_obitos" in result.columns

    def test_unmatched_rows_get_nan(self):
        health = _make_health(n=6)
        grid = _make_grid(n=3)  # only first 3 dates match
        result = sus_grid_join(health, grid, verbose=False)
        assert result["t2m"].isna().sum() == 3

    def test_annual_broadcast_join_by_code_muni_only(self):
        health = _make_health(n=12)
        annual_grid = pd.DataFrame({
            "code_muni": ["5103403"],
            "year": [2020],
            "deforestation_ha": [42.0],
        })
        result = sus_grid_join(health, annual_grid, by=["code_muni"], verbose=False)
        assert (result["deforestation_ha"] == 42.0).all()
        assert len(result) == len(health)

    def test_sus_meta_stage_and_type(self):
        result = sus_grid_join(_make_health(), _make_grid(type_="era5_land"), verbose=False)
        meta = result.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "era5_land"

    def test_type_out_overrides_inherited_type(self):
        result = sus_grid_join(
            _make_health(), _make_grid(type_="era5_land"), type_out="custom", verbose=False
        )
        assert result.attrs["sus_meta"]["type"] == "custom"

    def test_history_is_appended_not_replaced(self):
        result = sus_grid_join(_make_health(), _make_grid(), verbose=False)
        history = result.attrs["sus_meta"]["history"]
        assert history[0] == "imported"
        assert "sus_grid_join" in history[-1]

    def test_geometry_column_dropped_from_health(self):
        health = _make_health()
        health["geometry"] = "POINT(0 0)"
        result = sus_grid_join(health, _make_grid(), verbose=False)
        assert "geometry" not in result.columns

    def test_duckdb_relation_inputs_are_materialized(self):
        import duckdb

        conn = duckdb.connect()
        health_rel = conn.from_df(_make_health())
        grid_rel = conn.from_df(_make_grid())
        result = sus_grid_join(health_rel, grid_rel, verbose=False)
        assert isinstance(result, pd.DataFrame)
        assert "t2m" in result.columns
