"""INMET gap-filling using XGBoost (opt-in) or linear fallback.

Mirrors R: sus_climate_fill_inmet (sus_climate_fill_gap.R)

Backend resolution (backend="auto"):
  - xgboost if `pip install climasus4py[xgboost]` or xgboost is available
  - linear interpolation otherwise, with a UserWarning

Model cache:
  Per-station per-variable models are cached to
  ~/.climasus4py/models/<station>_<var>.joblib to avoid retraining.
"""

from __future__ import annotations

import hashlib
import warnings
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# INMET canonical variable set
# ---------------------------------------------------------------------------

KNOWN_INMET_VARS: tuple[str, ...] = (
    "patm_mb",
    "patm_max_mb",
    "patm_min_mb",
    "tair_dry_bulb_c",
    "tair_max_c",
    "tair_min_c",
    "dew_tmean_c",
    "dew_tmax_c",
    "dew_tmin_c",
    "rh_max_porc",
    "rh_min_porc",
    "rh_mean_porc",
    "rainfall_mm",
    "ws_gust_m_s",
    "ws_2_m_s",
    "wd_degrees",
    "sr_kj_m2",
)

_STATION_HINTS = ("station_code", "station", "estacao", "cd_estacao")
_DEFAULT_CACHE_DIR = Path.home() / ".climasus4py" / "models"

# ---------------------------------------------------------------------------
# Helpers: availability checks
# ---------------------------------------------------------------------------


def _xgboost_available() -> bool:
    """Return True if xgboost is importable."""
    try:
        import xgboost  # noqa: F401

        return True
    except ImportError:
        return False


def _joblib_available() -> bool:
    try:
        import joblib  # noqa: F401

        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Column auto-detection
# ---------------------------------------------------------------------------


def _detect_station_col(df: pd.DataFrame) -> str | None:
    for hint in _STATION_HINTS:
        if hint in df.columns:
            return hint
    return None


def _detect_datetime_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        low = col.lower()
        if "date" in low or "time" in low or low == "dt":
            return col
    return None


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------


def _engineer_features(df: pd.DataFrame, target_var: str) -> pd.DataFrame:
    """Add temporal lag and rolling features for XGBoost input."""
    feats = df.copy()
    feats["_hour"] = pd.to_datetime(feats["_datetime"]).dt.hour
    feats["_dayofweek"] = pd.to_datetime(feats["_datetime"]).dt.dayofweek
    feats["_month"] = pd.to_datetime(feats["_datetime"]).dt.month

    for lag in [1, 3, 7, 14]:
        feats[f"{target_var}_lag{lag}"] = feats[target_var].shift(lag)

    for window in [3, 7]:
        feats[f"{target_var}_rolling_mean{window}"] = (
            feats[target_var].rolling(window, min_periods=1).mean()
        )
        feats[f"{target_var}_rolling_std{window}"] = (
            feats[target_var].rolling(window, min_periods=1).std().fillna(0)
        )

    return feats


# ---------------------------------------------------------------------------
# Linear fallback
# ---------------------------------------------------------------------------


def _fill_linear(
    df: pd.DataFrame,
    target_var: str,
    station_col: str | None,
    datetime_col: str,
) -> pd.DataFrame:
    """Fill gaps via linear interpolation per station."""
    out = df.copy()
    if station_col:
        out[target_var] = out.groupby(station_col)[target_var].transform(
            lambda s: s.interpolate(method="linear")
        )
    else:
        out[target_var] = out[target_var].interpolate(method="linear")
    flag_col = f"is_imputed_{target_var}"
    out[flag_col] = df[target_var].isna() & out[target_var].notna()
    return out


# ---------------------------------------------------------------------------
# XGBoost fill (per station, cached)
# ---------------------------------------------------------------------------


def _cache_path(
    station: str,
    target_var: str,
    cache_dir: Path,
    feature_cols: list[str] | None = None,
) -> Path:
    """Build a cache filename keyed by station, var, package version, and feature signature.

    The signature ensures models cached under one version of
    ``_engineer_features`` are not silently reused after that function
    changes. ``usedforsecurity=False`` is required for FIPS-mode Python.
    """
    from .._version import __version__ as _pkg_version

    key = hashlib.md5(
        f"{station}_{target_var}".encode(), usedforsecurity=False
    ).hexdigest()[:12]
    feat_sig = "noinfo"
    if feature_cols:
        sig_input = "|".join(sorted(feature_cols)) + "|" + _pkg_version
        feat_sig = hashlib.md5(
            sig_input.encode(), usedforsecurity=False
        ).hexdigest()[:8]
    return cache_dir / f"{station}_{target_var}_{key}_v{_pkg_version}_{feat_sig}.joblib"


def _fill_xgboost_station(
    df_station: pd.DataFrame,
    target_var: str,
    station_id: str,
    cache_dir: Path,
    run_evaluation: bool,
    gap_percentage: float,
) -> tuple[pd.DataFrame, dict | None]:
    """Fill one station's gaps using XGBoost (train + predict on NaN rows)."""
    import joblib
    import xgboost as xgb

    df_s = df_station.copy().reset_index(drop=True)
    observed_mask = df_s[target_var].notna()

    if observed_mask.sum() < 10:
        # Not enough data to train — fall back to linear
        df_s[target_var] = df_s[target_var].interpolate(method="linear")
        df_s[f"is_imputed_{target_var}"] = df_station[target_var].isna() & df_s[target_var].notna()
        return df_s, None

    # Evaluation mode: mask gap_percentage of observed rows artificially
    eval_metrics: dict | None = None
    if run_evaluation:
        rng = np.random.default_rng(42)
        mask_idx = rng.choice(
            df_s.index[observed_mask],
            size=max(1, int(observed_mask.sum() * gap_percentage)),
            replace=False,
        )
        true_vals = df_s.loc[mask_idx, target_var].copy()
        df_s.loc[mask_idx, target_var] = np.nan
        observed_mask = df_s[target_var].notna()

    # Feature engineering
    feats = _engineer_features(df_s, target_var)
    feature_cols = [
        c for c in feats.columns
        if c not in (target_var, "_datetime", f"is_imputed_{target_var}")
        and feats[c].dtype in (np.float64, np.int64, np.float32)
    ]
    feature_cols = [c for c in feature_cols if feats[c].notna().any()]

    train_mask = observed_mask & feats[feature_cols].notna().all(axis=1)
    X_train = feats.loc[train_mask, feature_cols].values
    y_train = feats.loc[train_mask, target_var].values

    gap_mask = df_s[target_var].isna()
    if gap_mask.sum() == 0:
        df_s[f"is_imputed_{target_var}"] = False
        return df_s, None

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = _cache_path(station_id, target_var, cache_dir, feature_cols)

    if cache_file.exists():
        model = joblib.load(cache_file)
        # Validate feature compatibility — guards against stale models if a
        # bug bypassed the version+feature-signature path component.
        expected = getattr(model, "n_features_in_", None)
        if expected is not None and expected != len(feature_cols):
            cache_file.unlink(missing_ok=True)
            model = xgb.XGBRegressor(
                n_estimators=100, max_depth=5, learning_rate=0.1,
                random_state=42, n_jobs=1, verbosity=0,
            )
            model.fit(X_train, y_train)
            joblib.dump(model, cache_file)
    else:
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            n_jobs=1,
            verbosity=0,
        )
        model.fit(X_train, y_train)
        joblib.dump(model, cache_file)

    X_gap = feats.loc[gap_mask, feature_cols].fillna(feats[feature_cols].median()).values
    predictions = model.predict(X_gap)
    df_s.loc[gap_mask, target_var] = predictions
    df_s[f"is_imputed_{target_var}"] = gap_mask & df_s[target_var].notna()

    if run_evaluation:
        feats2 = _engineer_features(df_s, target_var)
        X_eval = feats2.loc[mask_idx, feature_cols].fillna(feats2[feature_cols].median()).values
        y_pred = model.predict(X_eval)
        y_true = true_vals.values
        mae = float(np.mean(np.abs(y_pred - y_true)))
        rmse = float(np.sqrt(np.mean((y_pred - y_true) ** 2)))
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - y_true.mean()) ** 2)
        r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else float("nan")
        eval_metrics = {
            "station": station_id,
            "predictions": pd.Series(y_pred, index=mask_idx),
            "metrics": {"mae": mae, "rmse": rmse, "r2": r2},
        }
        # Restore original gaps
        df_s.loc[mask_idx, target_var] = np.nan
        df_s.loc[mask_idx, f"is_imputed_{target_var}"] = False

    return df_s, eval_metrics


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_fill_inmet(
    df: pd.DataFrame,
    target_var: str | list[str],
    *,
    datetime_col: str | None = None,
    station_col: str | None = None,
    quality_threshold: float = 0.4,
    run_evaluation: bool = False,
    gap_percentage: float = 0.2,
    keep_features: bool = False,
    parallel: bool = True,
    workers: int | None = None,
    verbose: bool = True,
    lang: str = "pt",
    backend: Literal["auto", "xgboost", "linear"] = "auto",
    cache_dir: Path | str | None = None,
) -> pd.DataFrame | dict:
    """Fill gaps in INMET station data via XGBoost (opt-in) or linear fallback.

    Mirrors ``climasus4r::sus_climate_fill_inmet`` exactly.

    Backend resolution (``backend="auto"``):
      - Uses XGBoost when ``pip install climasus4py[xgboost]`` (or xgboost
        is importable); trains a model per station per variable.
      - Falls back to linear interpolation with a ``UserWarning`` when
        XGBoost is unavailable.

    Model cache: ``~/.climasus4py/models/<station>_<var>_<hash>.joblib``
    A cached model is reused on the second call with the same station/variable.

    Args:
        df: DataFrame with INMET station data, typically from
            ``sus_climate_inmet()`` or a materialised ``sus_climate_fill_inmet``
            result. Must contain a datetime column and a numeric target variable.
        target_var: Column name(s) to impute, or ``"all"`` to impute every
            INMET canonical variable present in *df*.
        datetime_col: Name of the datetime column (auto-detected if ``None``).
        station_col: Name of the station column (auto-detected if ``None``).
        quality_threshold: Max proportion of missing values per station (0-1).
            Stations above this threshold are excluded. Default: ``0.4``.
        run_evaluation: If ``True``, creates artificial MCAR gaps and evaluates
            model accuracy. Returns a ``dict`` keyed by variable name.
        gap_percentage: Proportion of observed data to mask for evaluation.
            Default: ``0.2``.
        keep_features: Retain engineered lag/rolling columns. Default: ``False``.
        parallel: Process stations in parallel using ``joblib``. Default: ``True``.
        workers: Number of parallel workers (``None`` = all CPUs - 1).
        verbose: Print progress messages. Default: ``True``.
        lang: Message language — ``"pt"``, ``"en"``, ``"es"``.
        backend: ``"auto"`` (XGBoost if available, else linear),
            ``"xgboost"`` (requires the extra), ``"linear"`` (always linear).
        cache_dir: Directory for model cache files.
            Default: ``~/.climasus4py/models/``.

    Returns:
        **Production mode** (``run_evaluation=False``): ``pd.DataFrame`` with
        imputed values and ``is_imputed_<var>`` flag columns.

        **Evaluation mode** (``run_evaluation=True``): ``dict`` with one key per
        variable, each value being ``{"data": df, "metrics": {mae, rmse, r2}}``.

    Raises:
        ValueError: If *target_var* names are not found in *df*.
        ImportError: If ``backend="xgboost"`` and xgboost is not installed.

    Example:
        >>> import climasus4py as cs
        >>> df = cs.sus_climate_inmet(years=2023, uf="AM").df()
        >>> filled = cs.sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c")
        >>> filled_all = cs.sus_climate_fill_inmet(df, target_var="all")
    """
    _cache_dir = Path(cache_dir) if cache_dir else _DEFAULT_CACHE_DIR

    # ------------------------------------------------------------------
    # Resolve backend
    # ------------------------------------------------------------------
    if backend == "xgboost":
        if not _xgboost_available():
            raise ImportError(
                "backend='xgboost' requires xgboost. "
                "Install with: pip install climasus4py[xgboost]"
            )
        use_xgboost = True
    elif backend == "linear":
        use_xgboost = False
    else:  # "auto"
        use_xgboost = _xgboost_available()
        if not use_xgboost:
            warnings.warn(
                "xgboost not found; sus_climate_fill_inmet falling back to linear "
                "interpolation. Install with: pip install climasus4py[xgboost]",
                UserWarning,
                stacklevel=2,
            )

    # ------------------------------------------------------------------
    # Normalise target_var
    # ------------------------------------------------------------------
    if target_var == "all":
        vars_to_fill = [c for c in df.columns if c in KNOWN_INMET_VARS]
        if not vars_to_fill:
            warnings.warn(
                "No canonical INMET variables found in DataFrame. "
                "Nothing to impute.",
                UserWarning,
                stacklevel=2,
            )
            return df
    elif isinstance(target_var, str):
        vars_to_fill = [target_var]
    else:
        vars_to_fill = list(target_var)

    missing_cols = [v for v in vars_to_fill if v not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Column(s) not found in DataFrame: {missing_cols}"
        )

    # ------------------------------------------------------------------
    # Auto-detect station and datetime columns
    # ------------------------------------------------------------------
    _datetime_col = datetime_col or _detect_datetime_col(df)
    _station_col = station_col or _detect_station_col(df)

    if verbose:
        _msg = {
            "pt": (
                f"[sus_climate_fill_inmet] backend={'xgboost' if use_xgboost else 'linear'}, "
                f"variáveis={vars_to_fill}"
            ),
            "en": (
                f"[sus_climate_fill_inmet] backend={'xgboost' if use_xgboost else 'linear'}, "
                f"variables={vars_to_fill}"
            ),
            "es": (
                f"[sus_climate_fill_inmet] backend={'xgboost' if use_xgboost else 'linear'}, "
                f"variables={vars_to_fill}"
            ),
        }
        print(_msg.get(lang, _msg["pt"]))

    # ------------------------------------------------------------------
    # Quality filter: exclude stations with too many NaN
    # ------------------------------------------------------------------
    result_df = df.copy()
    stations_excluded: list[str] = []

    if _station_col:
        stations = result_df[_station_col].unique().tolist()
        stations_kept = []
        for st in stations:
            mask = result_df[_station_col] == st
            n_total = mask.sum()
            n_missing = result_df.loc[mask, vars_to_fill[0]].isna().sum()
            if n_total > 0 and (n_missing / n_total) > quality_threshold:
                stations_excluded.append(str(st))
            else:
                stations_kept.append(st)
        if verbose and stations_excluded:
            print(
                f"  Estações excluídas (>{quality_threshold*100:.0f}% NaN): "
                f"{stations_excluded}"
            )
    else:
        stations_kept = [None]

    # ------------------------------------------------------------------
    # Fill loop per variable
    # ------------------------------------------------------------------
    eval_results: dict[str, dict] = {}

    for var in vars_to_fill:
        if _datetime_col:
            result_df = result_df.sort_values(_datetime_col).reset_index(drop=True)

        if use_xgboost and _joblib_available():
            from joblib import Parallel, delayed

            # Add sentinel datetime column for feature engineering
            if _datetime_col:
                result_df["_datetime"] = result_df[_datetime_col]
            else:
                result_df["_datetime"] = pd.RangeIndex(len(result_df))

            _n_jobs = workers if workers else -1
            if not parallel:
                _n_jobs = 1

            station_list = [st for st in stations_kept if st is not None]
            if not station_list:
                # No station column — treat entire df as one station
                station_list = ["_global"]
                if _station_col is None:
                    result_df["_station_tmp"] = "_global"
                    _station_col = "_station_tmp"

            def _process(
                st: str,
                _df: pd.DataFrame = result_df,
                _scol: str | None = _station_col,
                _var: str = var,
            ) -> tuple[str, pd.DataFrame, dict | None]:
                mask = _df[_scol] == st
                chunk = _df[mask].copy()
                chunk_out, metrics = _fill_xgboost_station(
                    chunk, _var, st, _cache_dir, run_evaluation, gap_percentage
                )
                return st, chunk_out, metrics

            station_results = Parallel(n_jobs=_n_jobs)(
                delayed(_process)(st) for st in station_list
            )

            # Reassemble
            chunks = []
            all_eval: list[dict] = []
            for _st, chunk_out, met in station_results:  # type: ignore[misc]
                chunks.append(chunk_out)
                if met:
                    all_eval.append(met)

            result_df = pd.concat(chunks, ignore_index=True)

            if _datetime_col:
                result_df = result_df.drop(columns=["_datetime"], errors="ignore")
            if "_station_tmp" in result_df.columns:
                result_df = result_df.drop(columns=["_station_tmp"])
                _station_col = None

            if run_evaluation and all_eval:
                eval_results[var] = {
                    "data": result_df,
                    "metrics": pd.DataFrame(
                        [e["metrics"] | {"station": e["station"]} for e in all_eval]
                    ),
                }

        else:
            # Linear fallback
            result_df = _fill_linear(result_df, var, _station_col, _datetime_col or "date")

        if not keep_features:
            drop_cols = [
                c for c in result_df.columns
                if c.startswith(f"{var}_lag")
                or c.startswith(f"{var}_rolling")
                or c in ("_hour", "_dayofweek", "_month")
            ]
            result_df = result_df.drop(columns=drop_cols, errors="ignore")

    if run_evaluation:
        return (
            eval_results
            if eval_results
            else {"_no_gaps": {"data": result_df, "metrics": pd.DataFrame()}}
        )

    return result_df
