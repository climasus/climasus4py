"""Climate data aggregation — lazy SQL aggregation for INMET observations.

Mirrors R: sus_climate_aggregate (temporal strategies: monthly/seasonal/yearly).
Lazy ponta a ponta: returns DuckDBPyRelation, never materialises internally.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import duckdb

from ..core.engine import get_connection

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

SUPPORTED_STATS: frozenset[str] = frozenset(
    {"mean", "median", "p10", "p25", "p75", "p90", "p99", "min", "max", "std"}
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _time_bucket_sql(resolution: str, date_col: str) -> str:
    """Return a SQL expression that maps a date to a time-bucket label."""
    if resolution == "monthly":
        return f"strftime({date_col}::DATE, '%Y-%m')"
    if resolution == "seasonal":
        # DJF=1, MAM=2, JJA=3, SON=4 (Southern Hemisphere ordering)
        return (
            f"CASE "
            f"  WHEN month({date_col}::DATE) IN (12, 1, 2) THEN 'DJF' "
            f"  WHEN month({date_col}::DATE) IN (3, 4, 5)  THEN 'MAM' "
            f"  WHEN month({date_col}::DATE) IN (6, 7, 8)  THEN 'JJA' "
            f"  ELSE 'SON' "
            f"END"
        )
    if resolution == "yearly":
        return f"year({date_col}::DATE)::VARCHAR"
    raise ValueError(  # pragma: no cover
        f"Unknown time_resolution '{resolution}'. "
        "Choose from: 'monthly', 'seasonal', 'yearly'."
    )


def _stat_expr(col: str, stat: str, threshold: float | None = None) -> str:
    """Return a SQL aggregate expression for a single (col, stat) pair."""
    alias = f"{col}_{stat}"
    if stat == "mean":
        return f"AVG({col}) AS {alias}"
    if stat == "median":
        return f"MEDIAN({col}) AS {alias}"
    if stat == "min":
        return f"MIN({col}) AS {alias}"
    if stat == "max":
        return f"MAX({col}) AS {alias}"
    if stat == "std":
        return f"STDDEV({col}) AS {alias}"
    if stat == "p10":
        return f"APPROX_QUANTILE({col}, 0.10) AS {alias}"
    if stat == "p25":
        return f"APPROX_QUANTILE({col}, 0.25) AS {alias}"
    if stat == "p75":
        return f"APPROX_QUANTILE({col}, 0.75) AS {alias}"
    if stat == "p90":
        return f"APPROX_QUANTILE({col}, 0.90) AS {alias}"
    if stat == "p99":
        return f"APPROX_QUANTILE({col}, 0.99) AS {alias}"
    if stat == "days_above_threshold":
        if threshold is None:  # pragma: no cover
            raise ValueError(
                "'days_above_threshold' stat requires the 'threshold' parameter."
            )
        return f"SUM(CASE WHEN {col} > {threshold} THEN 1 ELSE 0 END) AS {col}_days_above_threshold"
    raise ValueError(  # pragma: no cover
        f"Unknown stat '{stat}'. Supported: {sorted(SUPPORTED_STATS | {'days_above_threshold'})}."
    )


def _detect_numeric_columns(rel: duckdb.DuckDBPyRelation) -> list[str]:
    """Return the names of numeric columns in a relation (via schema peek)."""
    zero_df = rel.limit(0).df()
    import pandas as pd

    return [
        c
        for c in zero_df.columns
        if pd.api.types.is_numeric_dtype(zero_df[c])
    ]


def _detect_date_column(rel: duckdb.DuckDBPyRelation) -> str:
    """Heuristic: return the first column whose name contains 'date' or 'time'."""
    zero_df = rel.limit(0).df()
    for col in zero_df.columns:
        if "date" in col.lower() or "time" in col.lower():
            return col
    raise ValueError(
        "Could not auto-detect a date/time column. "
        "Ensure the relation has a column whose name contains 'date' or 'time'."
    )


def _detect_station_column(rel: duckdb.DuckDBPyRelation) -> str | None:
    """Return the station column name if present, else None."""
    zero_df = rel.limit(0).df()
    for col in zero_df.columns:
        low = col.lower()
        if "station" in low or "estacao" in low or "estação" in low:
            return col
    return None


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_aggregate(
    rel: duckdb.DuckDBPyRelation,
    *,
    time_resolution: Literal["monthly", "seasonal", "yearly"] = "monthly",
    stats: Sequence[str] = ("mean", "p10", "p90"),
    threshold: float | None = None,
    date_col: str | None = None,
    station_col: str | None = None,
    lang: str = "pt",
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Aggregate INMET climate observations to monthly, seasonal, or yearly buckets.

    All computation is done lazily in DuckDB SQL — the result is a
    ``DuckDBPyRelation`` that is never materialised internally.

    Mirrors ``climasus4r::sus_climate_aggregate`` (temporal aggregation path).

    Args:
        rel: Lazy DuckDB relation with INMET station observations (output of
            ``sus_climate_inmet`` or ``sus_climate_fill_inmet``).
        time_resolution: Temporal resolution for aggregation.
            ``"monthly"`` (default) → ``YYYY-MM`` labels.
            ``"seasonal"`` → ``DJF/MAM/JJA/SON`` (Southern Hemisphere order).
            ``"yearly"`` → ``YYYY`` labels.
        stats: Statistics to compute for each numeric column.
            Subset of ``{"mean", "median", "p10", "p25", "p75", "p90",
            "p99", "min", "max", "std"}``.
            Also supports ``"days_above_threshold"`` when *threshold* is given.
        threshold: Temperature threshold (°C) for ``"days_above_threshold"`` stat.
            Ignored when the stat is not requested.
        date_col: Name of the date/datetime column. Auto-detected if ``None``.
        station_col: Name of the station identifier column. Auto-detected if
            ``None``; when not found, aggregation is done across all stations.
        lang: Language for messages (``"pt"``, ``"en"``, ``"es"``).
        verbose: Print progress messages when ``True``.

    Returns:
        ``DuckDBPyRelation`` with columns:
        ``[station_col,] time_bucket, <numeric_col>_<stat>, ...``

    Raises:
        ValueError: If *time_resolution* is invalid, *stats* contains unknown
            values, or no date column is found.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_climate_inmet(years=2023, uf="AM")
        >>> monthly = cs.sus_climate_aggregate(rel, time_resolution="monthly")
        >>> seasonal = cs.sus_climate_aggregate(
        ...     rel, time_resolution="seasonal",
        ...     stats=["mean", "days_above_threshold"], threshold=32.0
        ... )
    """
    valid_resolutions = {"monthly", "seasonal", "yearly"}
    if time_resolution not in valid_resolutions:
        raise ValueError(
            f"Invalid time_resolution '{time_resolution}'. "
            f"Choose from: {sorted(valid_resolutions)}."
        )

    stats_list = list(stats)
    unknown = set(stats_list) - SUPPORTED_STATS - {"days_above_threshold"}
    if unknown:
        raise ValueError(
            f"Unknown stat(s): {sorted(unknown)}. "
            f"Supported: {sorted(SUPPORTED_STATS | {'days_above_threshold'})}."
        )

    # Auto-detect columns
    _date_col = date_col or _detect_date_column(rel)
    _station_col = station_col or _detect_station_column(rel)

    numeric_cols = [
        c for c in _detect_numeric_columns(rel) if c != _date_col
    ]
    if _station_col and _station_col in numeric_cols:
        numeric_cols.remove(_station_col)

    conn = get_connection()
    conn.register("_climate_agg_input", rel)

    # Build SELECT
    bucket_expr = _time_bucket_sql(time_resolution, _date_col)
    select_parts: list[str] = []
    if _station_col:
        select_parts.append(_station_col)
    select_parts.append(f"{bucket_expr} AS time_bucket")

    for col in numeric_cols:
        for stat in stats_list:
            if stat == "days_above_threshold" and threshold is None:
                continue
            select_parts.append(_stat_expr(col, stat, threshold))

    group_parts: list[str] = []
    if _station_col:
        group_parts.append(_station_col)
    group_parts.append(bucket_expr)

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM _climate_agg_input "
        f"GROUP BY {', '.join(group_parts)} "
        f"ORDER BY {', '.join(group_parts)}"
    )

    if verbose:
        _msg = {
            "pt": f"[sus_climate_aggregate] resolução={time_resolution}, stats={stats_list}",
            "en": f"[sus_climate_aggregate] resolution={time_resolution}, stats={stats_list}",
            "es": f"[sus_climate_aggregate] resolución={time_resolution}, stats={stats_list}",
        }
        print(_msg.get(lang, _msg["pt"]))

    return conn.sql(sql)
