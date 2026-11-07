"""DuckDB engine management.

Mirrors R: engine.R — lazy evaluation via DuckDB instead of duckplyr.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

# Singleton connection — one per process
_conn: duckdb.DuckDBPyConnection | None = None


def _format_setting(value: Any) -> str:
    """Render a Python value as a DuckDB ``SET`` literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{value}'"


@contextmanager
def duckdb_settings(
    conn: duckdb.DuckDBPyConnection | None = None, **overrides: Any
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Apply DuckDB settings for a block, then put them back.

    The connection returned by :func:`get_connection` is a process-wide
    singleton, so a bare ``SET`` inside one function silently reconfigures
    every later query in the session. Tuning a memory-hungry step down to
    96 MB and one thread is reasonable *for that step*; leaving the whole
    process there is not, and it made unrelated functions fail on a few
    thousand rows.

    Restoration uses an explicit ``SET`` with the captured value, **not**
    ``RESET``. On DuckDB 1.5.3, ``RESET memory_limit`` updates the value
    reported by ``current_setting`` back to the default while leaving the
    limit actually enforced at the lowered value — so a query afterwards
    still fails with ``(91.5 MiB/91.5 MiB used)`` even though the setting
    reads ``6.2 GiB``. Only ``SET`` resizes the buffer pool for real.

    The cost is a small formatting loss when a size is read back as text:
    ``6.2 GiB`` reparses as ``6.1 GiB``. It is bounded, not cumulative —
    measured on 1.5.3 it converges after two cycles (6.2 → 6.1 → 6.0 →
    stable), about 3% of the budget. A restore that reads right but does
    not hold is worse than one that gives up 3%.

    Args:
        conn: Connection to configure. Defaults to the shared singleton.
        **overrides: Settings to apply, e.g. ``memory_limit="96MB"``,
            ``threads=1``.

    Yields:
        The configured connection.

    Example:
        >>> with duckdb_settings(memory_limit="96MB", threads=1) as conn:
        ...     conn.sql("SELECT 1").fetchall()
        [(1,)]
    """
    conn = conn if conn is not None else get_connection()

    previous: dict[str, Any] = {}
    for key in overrides:
        try:
            previous[key] = conn.sql(f"SELECT current_setting('{key}')").fetchone()[0]
        except duckdb.Error:
            # Unknown to this DuckDB build; skip rather than guess a value
            # to restore it to.
            pass

    for key, value in overrides.items():
        if key not in previous:
            continue
        conn.execute(f"SET {key}={_format_setting(value)}")

    try:
        yield conn
    finally:
        for key, original in previous.items():
            try:
                conn.execute(f"SET {key}={_format_setting(original)}")
            except duckdb.Error as exc:
                warnings.warn(
                    f"Could not restore DuckDB setting {key!r} to "
                    f"{original!r}: {exc}. Later queries in this session may "
                    f"behave differently.",
                    RuntimeWarning,
                    stacklevel=2,
                )


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


def read_parquets(paths: Sequence[str | Path]) -> duckdb.DuckDBPyRelation:
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


def collect(rel: duckdb.DuckDBPyRelation) -> pd.DataFrame:
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


def collect_arrow(rel: duckdb.DuckDBPyRelation) -> pa.Table:
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
        >>> import climasus4py as cs
        >>> table = cs.collect_arrow(rel)
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
