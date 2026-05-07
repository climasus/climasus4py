"""Tests for lazy entry points and explicit materialization."""

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection


def test_sus_read_reads_parquet_lazily(tmp_path):
    path = tmp_path / "data.parquet"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path)

    rel = cs.sus_data_read(path)

    assert type(rel).__name__ == "DuckDBPyRelation"
    assert rel.count("*").fetchone()[0] == 2


def test_sus_read_rejects_non_parquet(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("a\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Parquet"):
        cs.sus_data_read(path)


def test_sus_sql_entrypoint_and_pipe_mode():
    rel = cs.sus_sql("SELECT 1 AS value UNION ALL SELECT 2 AS value")
    out = rel.pipe(cs.sus_sql, "SELECT SUM(value) AS total FROM {data}")

    assert out.fetchone()[0] == 3


def test_materialize_auto_returns_pandas():
    rel = get_connection().from_df(pd.DataFrame({"value": [1, 2, 3]}))

    out = cs.materialize(rel, quiet=True)

    assert isinstance(out, pd.DataFrame)
    assert out["value"].sum() == 6


def test_materialize_pyarrow():
    rel = get_connection().from_df(pd.DataFrame({"value": [1, 2, 3]}))

    out = cs.materialize(rel, how="pyarrow", quiet=True)

    assert out.num_rows == 3


def test_materialize_unknown_how_raises():
    rel = get_connection().from_df(pd.DataFrame({"value": [1]}))
    with pytest.raises(ValueError, match="how must be one of"):
        cs.materialize(rel, how="unknown", quiet=True)  # type: ignore[arg-type]


def test_materialize_geopandas_raises_without_geometry():
    gp = pytest.importorskip("geopandas")  # noqa: F841
    rel = get_connection().from_df(pd.DataFrame({"value": [1]}))
    with pytest.raises(ValueError, match="geometry_wkt"):
        cs.materialize(rel, how="geopandas", quiet=True)


def test_materialize_geopandas_with_geometry():
    gp = pytest.importorskip("geopandas")  # noqa: F841
    rel = get_connection().from_df(
        pd.DataFrame({"name": ["SP"], "geometry_wkt": ["POINT (-46.63 -23.55)"]})
    )
    gdf = cs.materialize(rel, how="geopandas", quiet=True)
    assert type(gdf).__name__ == "GeoDataFrame"
    assert gdf.shape[0] == 1
    assert "geometry" in gdf.columns


def test_materialize_polars():
    pytest.importorskip("polars")
    rel = get_connection().from_df(pd.DataFrame({"value": [1, 2, 3]}))
    out = cs.materialize(rel, how="polars", quiet=True)
    assert out.shape[0] == 3


def test_materialize_auto_picks_geopandas_when_geometry_wkt():
    gp = pytest.importorskip("geopandas")  # noqa: F841
    rel = get_connection().from_df(
        pd.DataFrame({"name": ["SP"], "geometry_wkt": ["POINT (-46.63 -23.55)"]})
    )
    out = cs.materialize(rel, quiet=True)
    assert type(out).__name__ == "GeoDataFrame"


def test_materialize_warns_for_large_relation(monkeypatch):
    """_warn_size emits UserWarning when row count >= 100_000."""
    from climasus4py.io.materialize import _warn_size

    conn = get_connection()
    rel = conn.sql("SELECT * FROM range(150000) t(value)")

    with pytest.warns(UserWarning, match="150,000"):
        _warn_size(rel, how="pandas", quiet=False)
