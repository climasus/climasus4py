"""Tests for lazy SQL enrichments against local parquet fixtures."""

import json

import pandas as pd
import pytest

import climasus4py as cs
import climasus4py.utils.data as data_utils
from climasus4py.core.engine import get_connection


def _use_data_root(monkeypatch, root):
    (root / "manifest.json").write_text(json.dumps({"version": "test"}), encoding="utf-8")
    monkeypatch.setenv("CLIMASUS_DATA_DIR", str(root))
    monkeypatch.setattr(data_utils, "_DATA_DIR", None)


def test_sus_spatial_joins_geometry_wkt(tmp_path, monkeypatch):
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "spatial").mkdir(parents=True)
    pd.DataFrame(
        {
            "code_muni": ["355030"],
            "name": ["Sao Paulo"],
            "state": ["SP"],
            "region": ["Sudeste"],
            "geometry_wkt": ["POINT (-46.63 -23.55)"],
        }
    ).to_parquet(tmp_path / "assets" / "spatial" / "municipalities.parquet")
    rel = get_connection().from_df(pd.DataFrame({"municipality_code": ["355030"], "count": [10]}))

    out = cs.sus_spatial(rel)

    df = out.df()
    assert type(out).__name__ == "DuckDBPyRelation"
    assert df.loc[0, "geometry_wkt"] == "POINT (-46.63 -23.55)"


def test_sus_spatial_accepts_custom_spatial_path(tmp_path):
    custom_path = tmp_path / "custom_spatial.parquet"
    pd.DataFrame(
        {
            "code_muni": ["999999"],
            "name": ["Custom City"],
            "geometry_wkt": ["POINT (1 2)"],
        }
    ).to_parquet(custom_path)
    rel = get_connection().from_df(pd.DataFrame({"municipality_code": ["999999"], "count": [1]}))

    out = cs.sus_spatial(rel, spatial_path=custom_path)

    df = out.df()
    assert df.loc[0, "spatial_name"] == "Custom City"
    assert df.loc[0, "geometry_wkt"] == "POINT (1 2)"


def test_sus_spatial_rejects_custom_spatial_path_without_schema(tmp_path):
    custom_path = tmp_path / "bad_spatial.parquet"
    pd.DataFrame({"code_muni": ["999999"]}).to_parquet(custom_path)
    rel = get_connection().from_df(pd.DataFrame({"municipality_code": ["999999"], "count": [1]}))

    with pytest.raises(ValueError, match="geometry_wkt|name"):
        cs.sus_spatial(rel, spatial_path=custom_path)


def test_sus_census_joins_requested_variables(tmp_path, monkeypatch):
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "census").mkdir(parents=True)
    pd.DataFrame(
        {
            "municipality_code": ["355030"],
            "population": [11_000_000],
            "income_per_capita": [1500.0],
        }
    ).to_parquet(tmp_path / "assets" / "census" / "census_2022.parquet")
    rel = get_connection().from_df(pd.DataFrame({"municipality_code": ["355030"], "count": [10]}))

    out = cs.sus_census(rel, variables=["population"])

    df = out.df()
    assert type(out).__name__ == "DuckDBPyRelation"
    assert df.loc[0, "population"] == 11_000_000


def test_sus_climate_idw_joins_weighted_variables(tmp_path, monkeypatch):
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "climate").mkdir(parents=True)
    pd.DataFrame(
        {
            "municipality_code": ["355030", "355030"],
            "station_id": ["A001", "A002"],
            "weight": [0.25, 0.75],
        }
    ).to_parquet(tmp_path / "assets" / "climate" / "idw_weights_municipality.parquet")
    pd.DataFrame(
        {
            "station_id": ["A001", "A002"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "temp_mean": [20.0, 28.0],
            "precipitation": [0.0, 4.0],
        }
    ).to_parquet(tmp_path / "assets" / "climate" / "inmet_observations_2024.parquet")
    rel = get_connection().from_df(
        pd.DataFrame({"municipality_code": ["355030"], "date": pd.to_datetime(["2024-01-01"])})
    )

    out = cs.sus_climate(rel, variables=["temp_mean", "precipitation"], years=[2024])

    df = out.df()
    assert type(out).__name__ == "DuckDBPyRelation"
    assert df.loc[0, "temp_mean"] == 26.0
    assert df.loc[0, "precipitation"] == 3.0


# ---------------------------------------------------------------------------
# 6-digit vs 7-digit municipality code normalisation (LEFT(...,6) join fix)
# ---------------------------------------------------------------------------

def _climate_fixture(tmp_path):
    """Returns (obs_path, idw_path) with 7-digit municipality_code in IDW."""
    obs = tmp_path / "inmet_observations_2023.parquet"
    idw = tmp_path / "idw_weights_municipality.parquet"
    pd.DataFrame({
        "station_id": ["A701"],
        "date": pd.to_datetime(["2023-06-01"]),
        "temp_mean": [17.5],
        "precipitation": [0.0],
    }).to_parquet(obs)
    pd.DataFrame({
        "municipality_code": ["3550308"],   # 7-digit IBGE code
        "station_id": ["A701"],
        "weight": [1.0],
    }).to_parquet(idw)
    return obs, idw


def test_sus_climate_idw_6digit_code_joins_correctly(tmp_path, monkeypatch):
    """DATASUS 6-digit code '355030' must join with IBGE 7-digit '3550308'."""
    _use_data_root(monkeypatch, tmp_path)
    climate_dir = tmp_path / "assets" / "climate"
    climate_dir.mkdir(parents=True)
    _obs, _idw = _climate_fixture(climate_dir)

    rel = get_connection().from_df(pd.DataFrame({
        "municipality_code": ["355030"],    # 6-digit DATASUS code
        "date": pd.to_datetime(["2023-06-01"]),
    }))

    out = cs.sus_climate(rel, variables=["temp_mean"], years=[2023])
    df = out.df()

    assert df.loc[0, "temp_mean"] == pytest.approx(17.5)


def test_sus_climate_direct_6digit_code_joins_correctly(tmp_path, monkeypatch):
    """Direct (no-IDW) path also normalises 6-digit → 7-digit join."""
    _use_data_root(monkeypatch, tmp_path)
    climate_dir = tmp_path / "assets" / "climate"
    climate_dir.mkdir(parents=True)
    pd.DataFrame({
        "station_id": ["A701"],
        "date": pd.to_datetime(["2023-06-01"]),
        "temp_mean": [18.0],
        "municipality_code": ["3550308"],   # 7-digit in observations
    }).to_parquet(climate_dir / "inmet_observations_2023.parquet")

    rel = get_connection().from_df(pd.DataFrame({
        "municipality_code": ["355030"],    # 6-digit DATASUS code
        "date": pd.to_datetime(["2023-06-01"]),
    }))

    out = cs.sus_climate(rel, variables=["temp_mean"], years=[2023], idw=False)
    df = out.df()

    assert df.loc[0, "temp_mean"] == pytest.approx(18.0)


def test_sus_census_6digit_code_joins_correctly(tmp_path, monkeypatch):
    """DATASUS 6-digit '355030' must join with census 7-digit '3550308'."""
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "census").mkdir(parents=True)
    pd.DataFrame({
        "municipality_code": ["3550308"],   # 7-digit IBGE code in census
        "population": [11_000_000],
    }).to_parquet(tmp_path / "assets" / "census" / "census_2022.parquet")

    rel = get_connection().from_df(pd.DataFrame({
        "municipality_code": ["355030"],    # 6-digit DATASUS code
        "count": [5],
    }))

    out = cs.sus_census(rel, variables=["population"])
    df = out.df()

    assert df.loc[0, "population"] == 11_000_000


def test_sus_spatial_6digit_code_joins_correctly(tmp_path, monkeypatch):
    """Spatial join also normalises 6-digit municipality code."""
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "spatial").mkdir(parents=True)
    pd.DataFrame({
        "code_muni": ["3550308"],           # 7-digit IBGE code in spatial
        "name": ["Sao Paulo"],
        "state": ["SP"],
        "region": ["Sudeste"],
        "geometry_wkt": ["POINT (-46.63 -23.55)"],
    }).to_parquet(tmp_path / "assets" / "spatial" / "municipalities.parquet")

    rel = get_connection().from_df(pd.DataFrame({
        "municipality_code": ["355030"],    # 6-digit DATASUS code
        "count": [10],
    }))

    out = cs.sus_spatial(rel)
    df = out.df()

    assert df.loc[0, "geometry_wkt"] == "POINT (-46.63 -23.55)"


def test_sus_climate_raises_for_monthly_date_column(tmp_path, monkeypatch):
    """sus_climate must raise ValueError when date column has monthly granularity."""
    _use_data_root(monkeypatch, tmp_path)
    (tmp_path / "assets" / "climate").mkdir(parents=True)
    pd.DataFrame({
        "station_id": ["A701"],
        "date": pd.to_datetime(["2023-06-01"]),
        "temp_mean": [17.5],
    }).to_parquet(tmp_path / "assets" / "climate" / "inmet_observations_2023.parquet")

    rel = get_connection().from_df(pd.DataFrame({
        "municipality_code": ["355030"],
        "date": ["2023-06"],               # monthly, not daily
    }))

    with pytest.raises(ValueError, match="month|granularity|YYYY-MM"):
        cs.sus_climate(rel, variables=["temp_mean"], years=[2023])
