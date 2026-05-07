"""Lazy Parquet reader — sus_read.

Mirrors R: read_parquet_duckdb() from duckplyr / climasus4r.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ..core.engine import get_connection


def sus_read(path: str | Path) -> duckdb.DuckDBPyRelation:
    """Read a Parquet file lazily as a DuckDB relation.

    No data is loaded into memory until the relation is materialised
    (e.g. with ``materialize()``, ``.df()``, or ``.fetchdf()``).

    Args:
        path: Path to a ``.parquet`` file.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` backed by the Parquet file.

    Raises:
        ValueError: If *path* does not have a ``.parquet`` extension.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_read("dados/cache/SIM-DO/SP_2022_all.parquet")
        >>> rel.columns
    """
    path = Path(path)
    if path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Parquet file expected (extension .parquet), got: {path.name!r}. "
            "Pass a .parquet file path."
        )
    conn = get_connection()
    return conn.read_parquet(str(path))
