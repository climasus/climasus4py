"""DuckDB engine management.

Mirrors R: engine.R — lazy evaluation via DuckDB instead of duckplyr.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

# Singleton connection — one per process
_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the shared DuckDB in-memory connection."""
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")
    return _conn


def read_parquets(paths: list[str | Path]) -> duckdb.DuckDBPyRelation:
    """Read one or more parquet files as a lazy DuckDB relation.

    This is the Python equivalent of duckplyr::read_parquet_duckdb().
    No data is loaded into memory until .df() or .fetchdf() is called.
    Uses union_by_name=True to handle schema differences across years.
    """
    conn = get_connection()
    str_paths = [str(p) for p in paths]
    if len(str_paths) == 1:
        return conn.read_parquet(str_paths[0])
    return conn.read_parquet(str_paths, union_by_name=True)


def is_relation(obj: object) -> bool:
    """Check if object is a lazy DuckDB relation."""
    return isinstance(obj, duckdb.DuckDBPyRelation)


def collect(rel: duckdb.DuckDBPyRelation) -> "pd.DataFrame":
    """Materialize a DuckDB relation to a pandas DataFrame."""
    return rel.df()


def collect_arrow(rel: duckdb.DuckDBPyRelation) -> "pa.Table":
    """Materialize a DuckDB relation to a PyArrow Table (~100x faster than pandas)."""
    return rel.arrow()


def schema_columns(rel: duckdb.DuckDBPyRelation) -> list[str]:
    """Get column names without materializing."""
    return rel.columns
