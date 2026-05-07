"""Climate gap filling — interpolation for lazy DuckDB relations.

Mirrors R: fill_gaps.R
"""

from __future__ import annotations

import warnings

import duckdb
import numpy as np
import pandas as pd

from ..core.engine import get_connection


def sus_fill_gaps(
    data: duckdb.DuckDBPyRelation,
    *,
    method: str = "linear",
    group_col: str = "municipality_code",
    date_col: str = "date",
    max_gap: int | None = None,
) -> duckdb.DuckDBPyRelation:
    """Fill gaps in climate time series by interpolation.

    Accepts a lazy DuckDB relation and always returns a DuckDB relation.
    The ``"linear"`` and ``"locf"`` methods materialise the data
    transparently, fill the gaps in pandas, and return a new relation.
    The ``"spline"`` method emits a ``UserWarning`` before materialising
    because spline interpolation has no native DuckDB equivalent.

    Args:
        data: Lazy DuckDB relation with climate time series.
        method: Gap-filling strategy — ``"linear"``, ``"spline"``,
            or ``"locf"``.
        group_col: Column used to partition the series (e.g.
            ``"municipality_code"``).
        date_col: Column containing the observation date.
        max_gap: Maximum number of consecutive ``NaN`` values to fill.
            ``None`` fills all gaps.

    Returns:
        Lazy DuckDB relation with numeric gaps filled.

    Raises:
        ValueError: If *method* is not one of the supported values.

    Example:
        >>> import climasus4py as cs
        >>> filled = cs.sus_fill_gaps(rel, method="linear")
        >>> filled = cs.sus_fill_gaps(rel, method="locf", max_gap=3)
    """
    conn = get_connection()

    # Detect numeric columns from schema without loading data
    zero_df = data.limit(0).df()
    numeric_cols = zero_df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c != group_col]

    # No numeric columns → return unchanged (stays lazy)
    if not numeric_cols:
        return data

    if method == "spline":
        warnings.warn(
            "sus_fill_gaps with method='spline' materializes the relation to "
            "pandas for spline interpolation.",
            UserWarning,
            stacklevel=2,
        )
        df = data.df()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[numeric_cols] = df.groupby(group_col)[numeric_cols].transform(
            lambda s: (
                s.interpolate(method="spline", order=3, limit=max_gap)
                if s.notna().sum() >= 4
                else s.interpolate(method="linear", limit=max_gap)
            )
        )
        return conn.from_df(df)

    if method == "linear":
        df = data.df()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[numeric_cols] = df.groupby(group_col)[numeric_cols].transform(
            lambda s: s.interpolate(method="linear", limit=max_gap)
        )
        return conn.from_df(df)

    if method == "locf":
        df = data.df()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df[numeric_cols] = df.groupby(group_col)[numeric_cols].transform(
            lambda s: s.ffill(limit=max_gap)
        )
        return conn.from_df(df)

    raise ValueError(
        f"Unknown method: {method!r}. Use 'linear', 'spline', or 'locf'."
    )
