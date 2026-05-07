"""Type guards for lazy pipeline functions.

Ensures pipeline functions receive DuckDB relations instead of
materialised objects (DataFrames, GeoDataFrames, etc.).
"""

from __future__ import annotations

import duckdb

from .engine import is_relation


def _assert_lazy(rel: object) -> None:
    """Raise TypeError if *rel* is not a DuckDB relation.

    Args:
        rel: Object to check.

    Raises:
        TypeError: If *rel* is not a ``duckdb.DuckDBPyRelation``.
            The message mentions ``DuckDBPyRelation``. For GeoDataFrame
            inputs the message also mentions ``materialize`` / ``sus_export``
            to guide the user.
    """
    if is_relation(rel):
        return

    mod = type(rel).__module__ or ""
    if "geopandas" in mod:
        raise TypeError(
            "Expected DuckDBPyRelation but received a GeoDataFrame. "
            "GeoDataFrames are already materialised. To use the lazy pipeline "
            "load your data with sus_data_read() and apply materialize() or "
            "sus_export() at the end."
        )

    raise TypeError(
        f"Expected DuckDBPyRelation but got {type(rel).__name__}. "
        "Pipeline functions require a lazy DuckDB relation. "
        "Use sus_data_read() or get_connection().from_df() to create one."
    )


def _require_columns(rel: duckdb.DuckDBPyRelation, cols: list[str]) -> None:
    """Raise ValueError if any required columns are absent from *rel*.

    Args:
        rel: DuckDB relation whose schema is inspected.
        cols: Column names that must be present.

    Raises:
        ValueError: Lists the missing columns and available ones.
    """
    existing = set(rel.columns)
    missing = [c for c in cols if c not in existing]
    if missing:
        raise ValueError(
            f"Required columns not found: {missing}. "
            f"Available columns: {sorted(existing)}"
        )
