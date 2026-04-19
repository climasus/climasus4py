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
    """Return the shared DuckDB in-memory connection, creating it if needed.

    Uses a module-level singleton so all pipeline steps share the same
    connection and can reference each other's views and temporary tables.

    Returns:
        An open ``duckdb.DuckDBPyConnection`` backed by ``:memory:``.

    Example:
        >>> conn = get_connection()
        >>> conn.sql("SELECT 1 AS x").df()
    """
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")
    return _conn


def read_parquets(paths: list[str | Path]) -> duckdb.DuckDBPyRelation:
    """Read one or more Parquet files as a lazy DuckDB relation.

    Python equivalent of ``duckplyr::read_parquet_duckdb()`` in R.
    No data is loaded into memory until ``.df()`` or ``.fetchdf()`` is
    called. Multiple files are combined with ``union_by_name=True`` to
    handle schema differences across years (e.g. a new column that
    did not exist in a prior year).

    Args:
        paths: One or more paths to ``.parquet`` files.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` over the given files.

    Example:
        >>> rel = read_parquets(["dados/cache/SIM-DO/SP_2022_all.parquet"])
        >>> rel.columns
    """
    conn = get_connection()
    str_paths = [str(p) for p in paths]
    if len(str_paths) == 1:
        return conn.read_parquet(str_paths[0])
    return conn.read_parquet(str_paths, union_by_name=True)


def is_relation(obj: object) -> bool:
    """Check whether an object is a lazy DuckDB relation.

    Args:
        obj: Any Python object to test.

    Returns:
        ``True`` if *obj* is a ``duckdb.DuckDBPyRelation``, ``False``
        otherwise.

    Example:
        >>> is_relation(conn.sql("SELECT 1"))
        True
        >>> is_relation(pd.DataFrame())
        False
    """
    return isinstance(obj, duckdb.DuckDBPyRelation)


def collect(rel: duckdb.DuckDBPyRelation) -> "pd.DataFrame":
    """Materialise a DuckDB relation to a pandas DataFrame.

    Args:
        rel: Lazy DuckDB relation to execute and collect.

    Returns:
        ``pandas.DataFrame`` with the full result set in memory.

    Example:
        >>> df = collect(rel)
        >>> type(df)
        <class 'pandas.core.frame.DataFrame'>
    """
    return rel.df()


def collect_arrow(rel: duckdb.DuckDBPyRelation) -> "pa.Table":
    """Materialise a DuckDB relation to a PyArrow Table.

    Significantly faster than ``collect()`` for large datasets (~100×)
    since DuckDB transfers data in zero-copy Arrow format. If DuckDB
    returns a ``RecordBatchReader``, ``.read_all()`` is called to
    normalise the result to a proper ``pyarrow.Table`` with
    ``.num_rows`` / ``.num_columns``.

    Args:
        rel: Lazy DuckDB relation to execute and collect.

    Returns:
        ``pyarrow.Table`` with the full result set.

    Example:
        >>> table = collect_arrow(rel)
        >>> table.num_rows
        334303
    """
    result = rel.arrow()
    # Some DuckDB versions return RecordBatchReader instead of Table
    if hasattr(result, "read_all"):
        return result.read_all()
    return result


def schema_columns(rel: duckdb.DuckDBPyRelation) -> list[str]:
    """Return column names of a DuckDB relation without materialising data.

    Args:
        rel: Lazy DuckDB relation.

    Returns:
        List of column name strings in schema order.

    Example:
        >>> schema_columns(rel)
        ['DTOBITO', 'CAUSABAS', 'IDADE', ...]
    """
    return rel.columns
