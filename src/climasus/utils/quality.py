"""Data quality profiling.

Mirrors R: quality.R
"""

from __future__ import annotations

import duckdb
import pandas as pd

from climasus.core.engine import collect, is_relation, get_connection


def sus_quality(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
) -> dict:
    """Calculate data quality metrics for a SUS dataset.

    Computes row count, column count, and per-column completeness rate
    (percentage of non-null values). Works with both lazy DuckDB
    relations and materialised ``pandas.DataFrame`` objects.

    Args:
        data: Dataset to profile — a lazy ``DuckDBPyRelation`` or a
            ``pandas.DataFrame``.

    Returns:
        Dictionary with the following keys:

        - `total_rows` (int): total number of rows.
        - `total_cols` (int): total number of columns.
        - `completeness` (dict[str, float]): per-column mapping
            of column name to percentage of non-null values (0–100).

    Example:
        >>> import climasus as cs
        >>> metrics = cs.sus_quality(rel)
        >>> metrics["total_rows"]
        334303
        >>> isinstance(metrics.get("completeness"), dict)
        True
    """
    conn = get_connection()

    if is_relation(data):
        columns = data.columns
        total_rows = data.aggregate("count(*)").fetchone()[0]

        completeness = {}
        for col in columns:
            non_null = conn.sql(
                f'SELECT COUNT("{col}") FROM data WHERE "{col}" IS NOT NULL'
            ).fetchone()[0]
            completeness[col] = round(non_null / max(total_rows, 1) * 100, 1)
    else:
        total_rows = len(data)
        columns = list(data.columns)
        completeness = {
            col: round(data[col].notna().sum() / max(total_rows, 1) * 100, 1)
            for col in columns
        }

    return {
        "total_rows": total_rows,
        "total_cols": len(columns),
        "completeness": completeness,
    }
