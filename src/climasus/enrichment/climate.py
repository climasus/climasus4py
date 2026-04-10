"""Climate data enrichment — join health + climate data.

Mirrors R: climate.R
"""

from __future__ import annotations

import duckdb
import pandas as pd

from climasus.core.engine import get_connection, collect, schema_columns
from climasus.utils.data import detect_date_column, detect_geo_column


def sus_climate(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    climate: pd.DataFrame,
    *,
    time_window: int = 0,
    lags: list[int] | None = None,
) -> pd.DataFrame:
    """Join health data with climate observations.

    Materializes the result (returns DataFrame).

    Parameters
    ----------
    data : Health data (lazy relation or DataFrame)
    climate : Climate DataFrame with columns: municipality_code, date, + climate vars
    time_window : Days before the health event to aggregate climate (0 = same day)
    lags : Additional lag days to add as columns (e.g., [7, 14, 30])
    """
    conn = get_connection()

    # Materialize health data if needed
    if isinstance(data, duckdb.DuckDBPyRelation):
        health_df = collect(data)
    else:
        health_df = data

    columns = list(health_df.columns)
    geo_col = detect_geo_column(columns, level="municipality")
    date_col = detect_date_column(columns)

    if not geo_col or not date_col:
        raise ValueError(
            "Health data must have a municipality_code and date column for climate join."
        )

    # Ensure date columns are datetime
    health_df[date_col] = pd.to_datetime(health_df[date_col], errors="coerce")
    climate["date"] = pd.to_datetime(climate["date"], errors="coerce")

    # Join on municipality + date
    result = health_df.merge(
        climate,
        left_on=[geo_col, date_col],
        right_on=["municipality_code", "date"],
        how="left",
        suffixes=("", "_clim"),
    )

    # Add lags if requested
    if lags:
        climate_cols = [c for c in climate.columns if c not in ("municipality_code", "date")]
        for lag_days in lags:
            lagged = climate.copy()
            lagged["date"] = lagged["date"] + pd.Timedelta(days=lag_days)
            lag_suffix = f"_lag{lag_days}d"
            lagged = lagged.rename(columns={c: f"{c}{lag_suffix}" for c in climate_cols})
            result = result.merge(
                lagged,
                left_on=[geo_col, date_col],
                right_on=["municipality_code", "date"],
                how="left",
                suffixes=("", lag_suffix),
            )

    return result
