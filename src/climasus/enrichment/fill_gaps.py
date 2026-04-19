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
    """Fill gaps in climate time series by interpolation or ML imputation.

    Operates on ``pandas.DataFrame`` (in-memory). Applies the chosen
    method independently within each group defined by *group_col*.

    Args:
        data: ``DataFrame`` with climate time series. Must contain at
            least *date_col*, *group_col*, and numeric variable columns.
        method: Gap-filling strategy:

            - ``"linear"`` — linear interpolation between valid values.
            - ``"spline"`` — cubic spline (requires ≥ 4 valid points per
              group; falls back to linear otherwise).
            - ``"locf"`` — last observation carried forward
              (forward fill).
            - ``"xgboost"`` — gradient-boosted tree imputation;
              requires ``pip install climasus4py[ml]``.

        group_col: Column name used to partition the series (e.g.
            ``"municipality_code"`` or a station identifier).
        date_col: Column containing the observation date.
        max_gap: Maximum number of consecutive ``NaN`` values to fill.
            ``None`` fills all gaps.

    Returns:
        Copy of *data* with missing numeric values filled according to
        *method*.

    Raises:
        ImportError: If *method* is ``"xgboost"`` and ``xgboost`` is
            not installed.
        ValueError: If *method* is not one of the supported values.

    Example:
        >>> import climasus as cs
        >>> filled = cs.sus_fill_gaps(climate_df, method="linear")
        >>> filled = cs.sus_fill_gaps(climate_df, method="locf", max_gap=3)
        >>> cs.sus_fill_gaps(climate_df, method="xgboost",
        ...                  group_col="station_id")
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
