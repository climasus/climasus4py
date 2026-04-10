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
    """Calculate data quality metrics.

    Returns dict with total_rows, total_cols, and per-column completeness.
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
