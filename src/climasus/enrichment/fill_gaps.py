"""Climate gap filling — interpolation and ML-based imputation.

Mirrors R: fill_gaps.R
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def sus_fill_gaps(
    data: pd.DataFrame,
    *,
    method: str = "linear",
    group_col: str = "municipality_code",
    date_col: str = "date",
    max_gap: int | None = None,
) -> pd.DataFrame:
    """Fill gaps in climate time series data.

    Materializes (operates on DataFrames).

    Parameters
    ----------
    data : DataFrame with climate data (must have date + group columns)
    method : "linear", "spline", "locf" (last observation carried forward),
             or "xgboost" (requires climasus4py[ml])
    group_col : Column to group by (e.g., station or municipality)
    date_col : Date column name
    max_gap : Maximum consecutive NAs to fill. None = fill all.
    """
    data = data.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")

    numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c != group_col]

    if method == "linear":
        data[numeric_cols] = data.groupby(group_col)[numeric_cols].transform(
            lambda s: s.interpolate(method="linear", limit=max_gap)
        )

    elif method == "spline":
        data[numeric_cols] = data.groupby(group_col)[numeric_cols].transform(
            lambda s: s.interpolate(method="spline", order=3, limit=max_gap)
            if s.notna().sum() >= 4 else s.interpolate(method="linear", limit=max_gap)
        )

    elif method == "locf":
        data[numeric_cols] = data.groupby(group_col)[numeric_cols].transform(
            lambda s: s.ffill(limit=max_gap)
        )

    elif method == "xgboost":
        try:
            import xgboost as xgb
        except ImportError:
            raise ImportError("Install ML extras: pip install climasus4py[ml]")

        # XGBoost imputation: train on non-missing, predict missing
        for col in numeric_cols:
            for _name, group in data.groupby(group_col):
                mask = group[col].isna()
                if not mask.any() or mask.all():
                    continue

                # Features: other numeric columns + temporal
                feat_cols = [c for c in numeric_cols if c != col]
                group_feat = group[feat_cols].copy()
                group_feat["_day_of_year"] = group[date_col].dt.dayofyear

                train_mask = ~mask
                if train_mask.sum() < 5:
                    continue

                model = xgb.XGBRegressor(n_estimators=50, max_depth=3, verbosity=0)
                model.fit(
                    group_feat.loc[train_mask].fillna(0),
                    group.loc[train_mask, col],
                )
                predictions = model.predict(group_feat.loc[mask].fillna(0))
                data.loc[group.index[mask], col] = predictions

    else:
        raise ValueError(f"Unknown method: {method}. Use linear, spline, locf, or xgboost.")

    return data
