"""INMET gap-filling using XGBoost (opt-in) or linear fallback.

Mirrors R: ``climasus4r::sus_climate_fill_inmet`` and its per-station worker
``.process_single_station_xgboost2`` (feature set, feature imputation,
hyperparameters, target transforms, physical limits, extreme-value smoothing
and the quality filter). Numbers do not match the R package value by value:
XGBoost draws its row/column subsamples from a different random generator in
each language, so the same seed gives different trees. The *method* is the
same.

Backend resolution (``backend="auto"``):
  - xgboost if ``pip install climasus4py[xgboost]`` or xgboost is available
  - linear interpolation otherwise, with a UserWarning

Deliberate departures from R (each one a confirmed R defect, see
MELHORIAS.csv):
  - Night-time solar radiation: R turns a missing ``sr_kj_m2`` into 0 between
    18h and 6h of the *UTC* clock, which is 15h-3h in Brasilia, so it zeroes
    afternoon gaps and leaves pre-dawn gaps to the model. Here night is
    18h-6h of the station's *solar* time (UTC + longitude / 15).
  - R does that zeroing after the quality filter, so a station "misses" ~46%
    of its radiation just because the sun is down and is dropped. Here the
    zeroing happens before the filter.

Models are not cached (they used to be, keyed by station + variable only, so
a model trained on one dataset was silently reused on another).
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# INMET canonical variable set (same order as R's .known_climate_vars)
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

# ---------------------------------------------------------------------------
# Constants mirrored from climasus4r
# ---------------------------------------------------------------------------

#: Fewer observed hours than this and the station is left untouched (R: 30).
_MIN_TRAIN = 30
#: create_lag_features(lag_periods = c(1, 2, 3, 12, 24, 168)).
_LAGS = (1, 2, 3, 12, 24, 168)
#: create_rolling_features(windows = c(3, 6, 12, 24), stats = c("mean", "sd")).
_ROLL_WINDOWS = (3, 6, 12, 24)
#: base_params of .process_single_station_xgboost2.
_XGB_PARAMS = {
    "booster": "gbtree",
    "eval_metric": "rmse",
    "objective": "reg:squarederror",
    "eta": 0.05,
    "max_depth": 4,
    "subsample": 0.7,
    "colsample_bytree": 0.7,
    "min_child_weight": 3,
    "alpha": 0.5,
    "lambda": 2,
    "gamma": 0.1,
    "nthread": 1,
    "tree_method": "hist",
    "seed": 42,
}
_NROUNDS = 200
_EARLY_STOPPING = 20
#: .smooth_extreme_predictions(iqr_multiplier = 5), alpha = 0.3.
_IQR_MULT = 5.0
_SMOOTH_ALPHA = 0.3
#: .handle_solar_radiation(night_start = 18, night_end = 6).
_NIGHT_START, _NIGHT_END = 18, 6
#: Fallback UTC offset (Brasilia) when there is no longitude column.
_DEFAULT_UTC_OFFSET_H = -3.0
#: After filling, R stores these as integers (as.integer(round(x))).
_ROUNDED_VARS = ("rh_max_porc", "rh_min_porc", "rh_mean_porc", "wd_degrees")

_MSG = {
    "skip": {
        "pt": "  Ignorando {n} estação(ões) com >{p}% de valores faltantes em "
              "{var} -- removidas do resultado: {st}",
        "en": "  Skipping {n} station(s) with >{p}% missing values in {var} "
              "-- removed from the output: {st}",
        "es": "  Omitiendo {n} estación(es) con >{p}% de valores faltantes en "
              "{var} -- eliminadas del resultado: {st}",
    },
    "none_pass": {
        "pt": "  Pulando '{var}': nenhuma estação passou no limite de {p}%.",
        "en": "  Skipping '{var}': no station passed the quality threshold of {p}%.",
        "es": "  Omitiendo '{var}': ninguna estación pasó el umbral de {p}%.",
    },
    "done": {
        "pt": "  Imputação concluída para {var}: {n} linha(s) de {t} ({pc:.2f}%).",
        "en": "  Imputation completed for {var}: {n} row(s) out of {t} ({pc:.2f}%).",
        "es": "  Imputación completada para {var}: {n} fila(s) de {t} ({pc:.2f}%).",
    },
}


def _msg(key: str, lang: str, **kw) -> str:
    tabela = _MSG[key]
    return tabela.get(lang, tabela["pt"]).format(**kw)


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


def _as_naive_utc(dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dt)
    if getattr(dt.dt, "tz", None) is not None:
        dt = dt.dt.tz_convert("UTC").dt.tz_localize(None)
    return dt


# ---------------------------------------------------------------------------
# Per-variable rules (R: get_transform_rule, prediction clipping)
# ---------------------------------------------------------------------------


def _transform_rule(target_var: str) -> str:
    """R get_transform_rule(): log1p for rain, sqrt for wind and radiation."""
    if re.search("rainfall", target_var):
        return "log1p"
    if re.search("ws_|gust", target_var):
        return "sqrt"
    if re.search("sr_kj", target_var):
        return "sqrt"
    return "none"


def _forward(y: np.ndarray, rule: str) -> np.ndarray:
    if rule == "log1p":
        return np.log1p(y)
    if rule == "sqrt":
        return np.sqrt(y)
    return y


def _inverse(p: np.ndarray, rule: str) -> np.ndarray:
    if rule == "log1p":
        return np.expm1(p)
    if rule == "sqrt":
        return p ** 2
    return p


def _clip_predictions(preds: np.ndarray, target_var: str) -> np.ndarray:
    """Physical limits, same regexes and order as R."""
    if re.search("rh|umid|humidity", target_var, re.IGNORECASE):
        return np.clip(preds, 0, 100)
    if re.search("t.*c|temp", target_var, re.IGNORECASE):
        return np.clip(preds, -90, 60)
    if re.search("pres|patm", target_var, re.IGNORECASE):
        return np.clip(preds, 700, 1100)
    if re.search("rain|precip", target_var, re.IGNORECASE):
        return np.maximum(preds, 0)
    if re.search("ws|wind", target_var, re.IGNORECASE):
        return np.maximum(preds, 0)
    return preds


def _smooth_extreme_predictions(preds: np.ndarray, train_vals: np.ndarray) -> np.ndarray:
    """R .smooth_extreme_predictions: pull values beyond 5 IQR 30% to the mean."""
    q1, q3 = np.nanquantile(train_vals, [0.25, 0.75])
    iqr = q3 - q1
    lo, hi = q1 - _IQR_MULT * iqr, q3 + _IQR_MULT * iqr
    out = preds.copy()
    extreme = (out < lo) | (out > hi)
    if extreme.any():
        mean = np.nanmean(train_vals)
        out[extreme] = _SMOOTH_ALPHA * mean + (1 - _SMOOTH_ALPHA) * out[extreme]
    return out


# ---------------------------------------------------------------------------
# Feature engineering (R: create_time_features, create_lag_features,
# create_rolling_features) -- 53 columns with R's names
# ---------------------------------------------------------------------------


def _impute_feature(s: pd.Series) -> pd.Series:
    """zoo::na.approx(rule = 2) -> na.locf both ways -> mean, as in R."""
    if s.isna().all():
        return s
    s = s.interpolate(method="linear", limit_direction="both")
    s = s.ffill().bfill()
    return s.fillna(s.mean())


def _time_features(dt: pd.Series) -> pd.DataFrame:
    """R create_time_features(), minus ``year`` (R's selection regex drops it)."""
    dt = _as_naive_utc(dt)
    hour = dt.dt.hour.astype("int64")
    month = dt.dt.month.astype("int64")
    doy = dt.dt.dayofyear.astype("int64")
    quarter = dt.dt.quarter.astype("int64")
    week = dt.dt.isocalendar().week.astype("int64")
    weekday = dt.dt.dayofweek.astype("int64")  # Monday = 0, as R wday(week_start=1) - 1
    season = month.map({12: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1,
                        6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}).astype("int64")
    two_pi = 2 * np.pi
    f = pd.DataFrame(index=dt.index)
    f["hour"] = hour
    f["day"] = dt.dt.day.astype("int64")
    f["month"] = month
    f["dayofyear"] = doy
    f["quarter"] = quarter
    f["weekofyear"] = week
    f["weekday"] = weekday
    f["is_weekend"] = (weekday >= 5).astype("int64")
    f["season"] = season
    f["hour_sin"] = np.sin(two_pi * hour / 24)
    f["hour_cos"] = np.cos(two_pi * hour / 24)
    f["month_sin"] = np.sin(two_pi * month / 12)
    f["month_cos"] = np.cos(two_pi * month / 12)
    f["dayofyear_sin"] = np.sin(two_pi * doy / 365.25)
    f["dayofyear_cos"] = np.cos(two_pi * doy / 365.25)
    f["weekday_sin"] = np.sin(two_pi * weekday / 7)
    f["weekday_cos"] = np.cos(two_pi * weekday / 7)
    f["quarter_sin"] = np.sin(two_pi * quarter / 4)
    f["quarter_cos"] = np.cos(two_pi * quarter / 4)
    f["weekofyear_sin"] = np.sin(two_pi * week / 52)
    f["weekofyear_cos"] = np.cos(two_pi * week / 52)
    for k in range(4):
        f[f"season_{k}"] = (season == k).astype("int64")
    f["is_morning"] = ((hour >= 6) & (hour < 12)).astype("int64")
    f["is_afternoon"] = ((hour >= 12) & (hour < 18)).astype("int64")
    f["is_evening"] = ((hour >= 18) & (hour < 22)).astype("int64")
    f["is_night"] = ((hour >= 22) | (hour < 6)).astype("int64")
    f["hour_decimal"] = hour + dt.dt.minute / 60
    return f


def _lag_features(y: pd.Series, var: str) -> pd.DataFrame:
    """R create_lag_features(): lags, leads, seasonal lags, diff; NAs imputed."""
    # Column order as R hands them to XGBoost (the "same_hour" pair comes
    # first because R's selection regex catches it with the time features).
    f = pd.DataFrame(index=y.index)
    f[f"{var}_same_hour_yesterday"] = y.shift(24)
    f[f"{var}_same_hour_last_week"] = y.shift(168)
    for k in _LAGS:
        f[f"{var}_lag_{k}"] = y.shift(k)
    for k in _LAGS:
        f[f"{var}_lead_{k}"] = y.shift(-k)
    f[f"{var}_diff_current_lag_1"] = y - f[f"{var}_lag_1"]
    return f.apply(_impute_feature)


def _rolling_features(y: pd.Series, var: str) -> pd.DataFrame:
    """R create_rolling_features(): right-aligned mean/sd ignoring NA; imputed.

    zoo::rollmean(fill = NA, align = "right") leaves the first k-1 positions NA
    and averages the non-missing values of every full window (na.rm = TRUE).
    """
    f = pd.DataFrame(index=y.index)
    n = len(y)
    for w in _ROLL_WINDOWS:
        if n < w:
            f[f"{var}_rolling_mean_{w}"] = y.mean()
            f[f"{var}_rolling_sd_{w}"] = y.mean()
            continue
        r = y.rolling(w, min_periods=1)
        mean, sd = r.mean(), r.std()
        mean.iloc[: w - 1] = np.nan
        sd.iloc[: w - 1] = np.nan
        f[f"{var}_rolling_mean_{w}"] = _impute_feature(mean)
        f[f"{var}_rolling_sd_{w}"] = _impute_feature(sd)
    return f


def _engineer_features(dt: pd.Series, y: pd.Series, var: str) -> pd.DataFrame:
    """The 53 features R gives XGBoost: time + lags/leads/rolling of the target.

    No other meteorological variable is used: when an INMET station goes
    down, every variable goes missing in the same hour.
    """
    return pd.concat([_time_features(dt), _lag_features(y, var),
                      _rolling_features(y, var)], axis=1)


# ---------------------------------------------------------------------------
# Solar radiation at night
# ---------------------------------------------------------------------------


def _is_night(dt_utc: pd.Series, longitude: pd.Series | None) -> np.ndarray:
    """Night by the station's solar clock (UTC + longitude / 15 h)."""
    h = dt_utc.dt.hour + dt_utc.dt.minute / 60
    if longitude is not None:
        offset = pd.to_numeric(longitude, errors="coerce").fillna(
            _DEFAULT_UTC_OFFSET_H * 15) / 15
    else:
        offset = _DEFAULT_UTC_OFFSET_H
    solar = (h + offset) % 24
    return ((solar >= _NIGHT_START) | (solar < _NIGHT_END)).to_numpy()


def _zero_night_radiation(df: pd.DataFrame, var: str, dt_col: str) -> pd.DataFrame:
    lon = df["longitude"] if "longitude" in df.columns else None
    night = _is_night(_as_naive_utc(df[dt_col]), lon)
    fill = night & df[var].isna().to_numpy()
    if fill.any():
        df.loc[fill, var] = 0
    return df


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
# XGBoost fill, one station (R: .process_single_station_xgboost2)
# ---------------------------------------------------------------------------


def _fill_xgboost_station(
    dt: pd.Series,
    y: pd.Series,
    target_var: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Fill one station's series. Inputs must be sorted by time.

    Returns ``(values, imputed_mask)``. With fewer than 30 observed values the
    series comes back untouched, as in R.
    """
    import xgboost as xgb

    y = pd.Series(y.to_numpy(dtype="float64"), index=range(len(y)))
    dt = pd.Series(pd.to_datetime(dt).to_numpy(), index=y.index)
    gaps = y.isna().to_numpy()
    observed = ~gaps
    if observed.sum() < _MIN_TRAIN or not gaps.any():
        return y.to_numpy(), np.zeros(len(y), dtype=bool)

    rule = _transform_rule(target_var)
    y_obs = y.to_numpy()[observed]
    with np.errstate(invalid="ignore"):
        y_train = _forward(y_obs, rule)

    feats = _engineer_features(dt, y, target_var)
    x_train = feats.loc[observed]
    x_train = x_train.loc[:, x_train.notna().any()]
    x_train = x_train.fillna(x_train.mean())
    cols = list(x_train.columns)

    try:
        dtrain = xgb.DMatrix(x_train.to_numpy(dtype="float64"), label=y_train)
        booster = xgb.train(
            _XGB_PARAMS, dtrain, num_boost_round=_NROUNDS,
            evals=[(dtrain, "train")], early_stopping_rounds=_EARLY_STOPPING,
            verbose_eval=False,
        )
    except Exception as exc:  # noqa: BLE001 -- R also catches and skips
        warnings.warn(f"Model training failed for {target_var}: {exc}", UserWarning,
                      stacklevel=3)
        return y.to_numpy(), np.zeros(len(y), dtype=bool)

    try:
        x_pred = xgb.DMatrix(feats.loc[gaps, cols].to_numpy(dtype="float64"))
        preds = booster.predict(
            x_pred, iteration_range=(0, booster.best_iteration + 1))
        preds = _inverse(preds.astype("float64"), rule)
        preds = _clip_predictions(preds, target_var)
        preds = _smooth_extreme_predictions(preds, y_obs)
    except Exception:  # noqa: BLE001 -- R falls back to the training mean
        preds = np.full(int(gaps.sum()), np.nanmean(y_obs))

    out = y.to_numpy().copy()
    out[gaps] = preds
    return out, gaps


def _station_groups(df: pd.DataFrame, station_col: str | None, dt_col: str | None):
    """Yield (station, row labels sorted by time; input order if no time)."""
    def _ordem(g: pd.DataFrame) -> pd.Index:
        return g.sort_values(dt_col, kind="stable").index if dt_col else g.index

    if station_col is None:
        yield None, _ordem(df)
        return
    for st, g in df.groupby(station_col, sort=False):
        yield st, _ordem(g)


def _fill_var_xgboost(
    df: pd.DataFrame,
    var: str,
    station_col: str | None,
    dt_col: str,
    n_jobs: int,
) -> tuple[pd.Series, pd.Series]:
    """Run the per-station fill for one variable; returns (values, flags)."""
    groups = list(_station_groups(df, station_col, dt_col))
    tasks = [(df.loc[idx, dt_col], df.loc[idx, var]) for _, idx in groups]

    if n_jobs != 1 and len(tasks) > 1 and _joblib_available():
        from joblib import Parallel, delayed

        results = Parallel(n_jobs=n_jobs)(
            delayed(_fill_xgboost_station)(d, y, var) for d, y in tasks)
    else:
        results = [_fill_xgboost_station(d, y, var) for d, y in tasks]

    values = pd.Series(np.nan, index=df.index, dtype="float64")
    flags = pd.Series(False, index=df.index)
    for (_, idx), (v, m) in zip(groups, results, strict=True):
        values.loc[idx] = v
        flags.loc[idx] = m
    return values, flags


def _store(df: pd.DataFrame, var: str, values: pd.Series) -> None:
    """Write filled values back keeping the column's float dtype (float32 stays)."""
    dtype = df[var].dtype
    if pd.api.types.is_float_dtype(dtype):
        df[var] = values.astype(dtype)
    else:
        df[var] = values


# ---------------------------------------------------------------------------
# Evaluation mode
# ---------------------------------------------------------------------------


def _evaluate_var(
    df: pd.DataFrame,
    var: str,
    station_col: str | None,
    dt_col: str | None,
    gap_percentage: float,
    use_xgboost: bool,
) -> dict:
    """Mask ``gap_percentage`` of the observed values per station and score."""
    rng = np.random.default_rng(42)
    rows, metrics = [], []
    for st, idx in _station_groups(df, station_col, dt_col):
        dt = df.loc[idx, dt_col] if dt_col else pd.Series(np.arange(len(idx)))
        y = df.loc[idx, var].astype("float64").reset_index(drop=True)
        obs = np.flatnonzero(y.notna().to_numpy())
        if len(obs) == 0:
            continue
        hide = rng.choice(obs, size=max(1, int(len(obs) * gap_percentage)), replace=False)
        truth = y.iloc[hide].to_numpy()
        y_masked = y.copy()
        y_masked.iloc[hide] = np.nan
        if use_xgboost:
            filled, _ = _fill_xgboost_station(dt, y_masked, var)
        else:
            filled = y_masked.interpolate(method="linear").to_numpy()
        pred = filled[hide]
        ok = ~np.isnan(pred)
        err = pred[ok] - truth[ok]
        ss_tot = np.sum((truth[ok] - truth[ok].mean()) ** 2)
        metrics.append({
            "station": st,
            "mae": float(np.mean(np.abs(err))) if ok.any() else np.nan,
            "rmse": float(np.sqrt(np.mean(err ** 2))) if ok.any() else np.nan,
            "r2": float(1 - np.sum(err ** 2) / ss_tot) if ss_tot > 0 else np.nan,
            "n": int(ok.sum()),
        })
        rows.append(pd.DataFrame({
            "date": dt.to_numpy()[hide], "station": st,
            "true_value": truth, "pred_value": pred,
        }))
    data = (pd.concat(rows, ignore_index=True).sort_values("date", ignore_index=True)
            if rows else pd.DataFrame(columns=["date", "station", "true_value", "pred_value"]))
    return {"data": data, "metrics": pd.DataFrame(metrics)}


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

    Follows ``climasus4r::sus_climate_fill_inmet``: one XGBoost model per
    station and variable, trained on 53 features built from the time stamp
    and from the variable's own history (lags and leads of 1, 2, 3, 12, 24
    and 168 hours, the same hour yesterday and last week, rolling mean and
    standard deviation over 3, 6, 12 and 24 hours). Rain is modelled on
    ``log1p`` and wind/radiation on the square root; predictions are clipped
    to physical limits and values beyond 5 IQR are pulled towards the mean.
    The same seed does not reproduce R's numbers -- the random generators
    differ -- but the method is the same.

    Backend resolution (``backend="auto"``):
      - Uses XGBoost when ``pip install climasus4py[xgboost]`` (or xgboost
        is importable).
      - Falls back to linear interpolation with a ``UserWarning`` when
        XGBoost is unavailable.

    Quality filter (as in R): variables are processed in order and, for each
    one, a station with more than ``quality_threshold`` of that variable
    missing is **removed from the output entirely**, with its other
    variables. If no station passes for a variable, the variable is skipped
    and nothing is removed. Night-time missing ``sr_kj_m2`` is set to 0 by
    the station's solar clock *before* the filter, so the dark hours do not
    count as missing.

    Args:
        df: DataFrame with INMET station data, typically from
            ``sus_climate_inmet()``. Must contain a datetime column and the
            target variable(s).
        target_var: Column name(s) to impute, or ``"all"`` to impute every
            INMET canonical variable present in *df*.
        datetime_col: Name of the datetime column (auto-detected if ``None``).
        station_col: Name of the station column (auto-detected if ``None``).
        quality_threshold: Max proportion of missing values per station and
            variable (0-1). Default: ``0.4``.
        run_evaluation: If ``True``, hides ``gap_percentage`` of the observed
            values of each station, fills them and scores the result. Returns
            a ``dict`` keyed by variable name.
        gap_percentage: Proportion of observed data to hide for evaluation.
            Default: ``0.2``.
        keep_features: Kept for compatibility with R; as in R the engineered
            features are internal and never returned.
        parallel: Process stations in parallel using ``joblib``. Default: ``True``.
        workers: Number of parallel workers (``None`` = all CPUs - 1).
        verbose: Print progress messages. Default: ``True``.
        lang: Message language -- ``"pt"``, ``"en"``, ``"es"``.
        backend: ``"auto"`` (XGBoost if available, else linear),
            ``"xgboost"`` (requires the extra), ``"linear"`` (always linear).
        cache_dir: Deprecated and ignored. Models are no longer cached.

    Returns:
        **Production mode** (``run_evaluation=False``): ``pd.DataFrame`` in
        the input row order (minus removed stations), with imputed values and
        ``is_imputed_<var>`` flag columns.

        **Evaluation mode** (``run_evaluation=True``): ``dict`` with one key
        per variable, each value being ``{"data": df, "metrics": df}`` --
        ``data`` holds ``date, station, true_value, pred_value`` for every
        hidden value and ``metrics`` one row per station (mae, rmse, r2, n).

    Raises:
        ValueError: If *target_var* names are not found in *df*, or the
            XGBoost backend is used without a datetime column.
        ImportError: If ``backend="xgboost"`` and xgboost is not installed.

    Example:
        >>> import climasus4py as cs
        >>> df = cs.sus_climate_inmet(years=2023, uf="AM").df()
        >>> filled = cs.sus_climate_fill_inmet(df, target_var="tair_dry_bulb_c")
        >>> filled_all = cs.sus_climate_fill_inmet(df, target_var="all")
    """
    if cache_dir is not None:
        warnings.warn(
            "cache_dir is ignored: sus_climate_fill_inmet no longer caches "
            "models. Old files in ~/.climasus4py/models can be deleted.",
            DeprecationWarning,
            stacklevel=2,
        )

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
    # Normalise target_var (R order for "all")
    # ------------------------------------------------------------------
    if isinstance(target_var, str) and target_var == "all":
        vars_to_fill = [v for v in KNOWN_INMET_VARS if v in df.columns]
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
    _dt_col = datetime_col or _detect_datetime_col(df)
    _station_col = station_col or _detect_station_col(df)
    if use_xgboost and _dt_col is None:
        raise ValueError(
            "sus_climate_fill_inmet needs a datetime column for the XGBoost "
            "backend (pass datetime_col=...)."
        )

    if verbose:
        rotulo = {"pt": "variáveis", "en": "variables", "es": "variables"}
        print(
            f"[sus_climate_fill_inmet] backend={'xgboost' if use_xgboost else 'linear'}, "
            f"{rotulo.get(lang, rotulo['pt'])}={vars_to_fill}"
        )

    n_jobs = 1 if not parallel else (workers if workers else -1)
    result = df.copy()

    # Night radiation -> 0 BEFORE the quality filter (R does it after).
    if "sr_kj_m2" in vars_to_fill and _dt_col is not None:
        result = _zero_night_radiation(result, "sr_kj_m2", _dt_col)

    pct = round(quality_threshold * 100)

    # ------------------------------------------------------------------
    # Evaluation mode
    # ------------------------------------------------------------------
    if run_evaluation:
        eval_results: dict[str, dict] = {}
        for var in vars_to_fill:
            if _station_col:
                miss = result[var].isna().groupby(result[_station_col]).mean()
                valid = miss.index[miss <= quality_threshold]
                if len(valid) == 0:
                    if verbose:
                        print(_msg("none_pass", lang, var=var, p=pct))
                    continue
                sub = result[result[_station_col].isin(valid)]
            else:
                sub = result
            eval_results[var] = _evaluate_var(
                sub, var, _station_col, _dt_col, gap_percentage, use_xgboost)
        return eval_results

    # ------------------------------------------------------------------
    # Fill loop per variable (R order and R quality filter)
    # ------------------------------------------------------------------
    for var in vars_to_fill:
        flag_col = f"is_imputed_{var}"
        if _station_col:
            miss = result[var].isna().groupby(result[_station_col], sort=True).mean()
            bad = miss.index[miss > quality_threshold].tolist()
            valid = miss.index[miss <= quality_threshold]
            if len(valid) == 0:
                if verbose:
                    print(_msg("none_pass", lang, var=var, p=pct))
                # R skips without creating the flag; an all-False flag keeps
                # the output schema stable for code that reads it.
                result[flag_col] = False
                continue
            if bad:
                if verbose:
                    print(_msg("skip", lang, n=len(bad), p=pct, var=var,
                               st=", ".join(map(str, bad))))
                result = result[result[_station_col].isin(valid)].copy()

        if use_xgboost:
            values, flags = _fill_var_xgboost(result, var, _station_col, _dt_col, n_jobs)
            _store(result, var, values)
            result[flag_col] = flags
        else:
            result = _fill_linear(result, var, _station_col, _dt_col or "date")

        if var in _ROUNDED_VARS:
            result[var] = np.round(result[var])

        if verbose:
            n_imp, n_tot = int(result[flag_col].sum()), len(result)
            print(_msg("done", lang, var=var, n=n_imp, t=n_tot,
                       pc=100 * n_imp / n_tot if n_tot else 0.0))

    return result.reset_index(drop=True)
