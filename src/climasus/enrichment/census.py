"""Census data enrichment.

Mirrors R: census.R
"""

from __future__ import annotations

import duckdb
import pandas as pd

from climasus.core.engine import collect
from climasus.utils.data import detect_geo_column


def sus_census(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    census: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Join health data with census indicators.

    Materializes the result (returns DataFrame).

    Parameters
    ----------
    data : Health data
    census : Census DataFrame with municipality_code + indicators.
             If None, attempts to load via censobr (if installed).
    """
    # Materialize
    df = collect(data) if isinstance(data, duckdb.DuckDBPyRelation) else data

    if census is None:
        raise NotImplementedError(
            "Auto-loading census data not yet implemented in Python. "
            "Provide a census DataFrame with municipality_code column."
        )

    join_col = detect_geo_column(list(df.columns), level="municipality")
    if not join_col:
        raise ValueError("No municipality column found in health data.")

    df[join_col] = df[join_col].astype(str)
    if "municipality_code" in census.columns:
        census["municipality_code"] = census["municipality_code"].astype(str)
        result = df.merge(census, left_on=join_col, right_on="municipality_code", how="left")
    else:
        raise ValueError("Census data must have a 'municipality_code' column.")

    return result
