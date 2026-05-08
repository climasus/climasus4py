"""Type guards for lazy pipeline functions.

Ensures pipeline functions receive DuckDB relations instead of
materialised objects (DataFrames, GeoDataFrames, etc.).
"""

from __future__ import annotations

import warnings

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


def _unwrap_sus_relation(
    rel: object,
    fn_name: str,
) -> duckdb.DuckDBPyRelation:
    """Unwrap *_SusRelation* wrappers and emit a UserWarning; validate otherwise.

    When pipeline functions are called directly with a ``_SusRelation``
    (returned by ``sus_sql()``), this helper:

    1. Extracts the underlying ``DuckDBPyRelation`` from the wrapper.
    2. Emits a ``UserWarning`` mentioning *fn_name* to guide users towards
       the ``.pipe()`` calling convention.

    When *rel* is already a ``DuckDBPyRelation``, delegates to
    ``_assert_lazy()`` and returns it unchanged.

    Args:
        rel: Input relation — either a raw ``DuckDBPyRelation`` or a
            ``_SusRelation`` wrapper.
        fn_name: Name of the calling function (used in the warning message).

    Returns:
        The underlying ``DuckDBPyRelation``.

    Raises:
        TypeError: If *rel* is neither a ``DuckDBPyRelation`` nor a
            ``_SusRelation`` wrapper.
    """
    # Try to unwrap _SusRelation (duck-typing: has a _rel DuckDBPyRelation attribute)
    try:
        inner = object.__getattribute__(rel, "_rel")
        if is_relation(inner):
            warnings.warn(
                f"{fn_name} received a _SusRelation directly. "
                f"Consider using rel.pipe(cs.{fn_name}, ...) for explicit "
                "pipeline chaining.",
                UserWarning,
                stacklevel=3,
            )
            return inner
    except AttributeError:
        pass

    _assert_lazy(rel)
    return rel  # type: ignore[return-value]


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
