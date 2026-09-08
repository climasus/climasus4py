"""Climate-health data integration — 10 temporal strategies.

Mirrors R: climasus4r::sus_climate_aggregate()

Pipeline
--------
1. Validate health_data and climate_data
2. Validate temporal overlap
3. Detect climate region and apply defaults
4. Validate strategy parameters
5. Pre-aggregate INMET data to daily resolution
6. Spatial matching (nearest station per municipality)
7. Temporal join (10 strategies)
8. Register sus_meta
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from datetime import datetime
from typing import Literal

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
from scipy.spatial import cKDTree

from ..core._stage import add_history, set_stage
from ..core.engine import duckdb_settings, get_connection
from ..utils.data import detect_date_column, detect_geo_column

# ---------------------------------------------------------------------------
# Global metadata store (DuckDBPyRelation does not support .attrs)
# ---------------------------------------------------------------------------

_CLIMASUS_META: dict[int, dict] = {}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_KNOWN_CLIMATE_VARS = [
    "patm_mb", "patm_max_mb", "patm_min_mb",
    "tair_dry_bulb_c", "tair_max_c", "tair_min_c",
    "dew_tmean_c", "dew_tmax_c", "dew_tmin_c",
    "rh_mean_porc", "rh_max_porc", "rh_min_porc",
    "rainfall_mm", "ws_gust_m_s", "ws_2_m_s",
    "wd_degrees", "sr_kj_m2",
]

_MUNI_CANDIDATES = [
    "code_muni", "residence_municipality_code",
    "occurrence_municipality_code", "notification_municipality_code",
    "municipality_code", "CODMUNRES", "MUNI_RES",
]

_DATE_CANDIDATES = ["death_date", "DTOBITO", "date", "DT_NOTIFIC",
                    "DT_INTER", "DTNASC"]

_STATION_CANDIDATES = ["station_code", "wmo_code"]

# ---------------------------------------------------------------------------
# QC helpers
# ---------------------------------------------------------------------------

def _calculate_dew_point(temp_c: pd.Series, rh_pct: pd.Series) -> pd.Series:
    """Magnus formula for dew point. Used internally by _apply_quality_control."""
    a = 17.27
    b = 237.7
    rh    = rh_pct / 100.0
    alpha = (a * temp_c) / (b + temp_c) + np.log(rh.clip(lower=1e-10))
    return (b * alpha) / (a - alpha)


def _apply_quality_control(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Apply meteorological QC to hourly INMET data.

    Checks (in order):
    1. Internal consistency (Tmin ≤ Tmean ≤ Tmax, etc.)
    2. Physically impossible ranges
    3. Dew point consistency (Magnus formula)
    4. Stuck sensor (static values for too many hours)
    5. Temporal gaps (warns only, does not remove)
    """
    df = df.copy()
    n_modified = 0

    # 1. Internal consistency
    if all(c in df.columns for c in ["tair_min_c", "tair_dry_bulb_c", "tair_max_c"]):
        for lo, hi in [("tair_min_c", "tair_dry_bulb_c"),
                       ("tair_dry_bulb_c", "tair_max_c")]:
            mask = df[lo] > df[hi]
            n = mask.sum()
            if n:
                df.loc[mask, lo] = np.nan
                n_modified += n
                if verbose:
                    print(f"  QC: {n} {lo} > {hi} → NaN")

    if all(c in df.columns for c in ["dew_tmean_c", "tair_dry_bulb_c"]):
        mask = df["dew_tmean_c"] > df["tair_dry_bulb_c"]
        n = mask.sum()
        if n:
            df.loc[mask, "dew_tmean_c"] = np.nan
            n_modified += n

    if all(c in df.columns for c in ["rh_min_porc", "rh_mean_porc", "rh_max_porc"]):
        for lo, hi in [("rh_min_porc", "rh_mean_porc"),
                       ("rh_mean_porc", "rh_max_porc")]:
            mask = df[lo] > df[hi]
            n = mask.sum()
            if n:
                df.loc[mask, lo] = np.nan
                n_modified += n

    if all(c in df.columns for c in ["patm_min_mb", "patm_mb", "patm_max_mb"]):
        for lo, hi in [("patm_min_mb", "patm_mb"), ("patm_mb", "patm_max_mb")]:
            mask = df[lo] > df[hi]
            n = mask.sum()
            if n:
                df.loc[mask, lo] = np.nan
                n_modified += n

    # 2. Physical ranges
    for col in ["tair_dry_bulb_c", "tair_min_c", "tair_max_c",
                "dew_tmean_c", "dew_tmax_c", "dew_tmin_c"]:
        if col in df.columns:
            mask = (df[col] < -90) | (df[col] > 60)
            n = mask.sum()
            if n:
                df.loc[mask, col] = np.nan
                n_modified += n

    for col in ["rh_mean_porc", "rh_min_porc", "rh_max_porc"]:
        if col in df.columns:
            mask = (df[col] < 0) | (df[col] > 100)
            n = mask.sum()
            if n:
                df.loc[mask, col] = np.nan
                n_modified += n

    for col in ["patm_mb", "patm_min_mb", "patm_max_mb"]:
        if col in df.columns:
            mask = (df[col] < 700) | (df[col] > 1100)
            n = mask.sum()
            if n:
                df.loc[mask, col] = np.nan
                n_modified += n

    if "rainfall_mm" in df.columns:
        mask_neg = df["rainfall_mm"] < 0
        df.loc[mask_neg, "rainfall_mm"] = 0.0
        mask_hi  = df["rainfall_mm"] > 500
        df.loc[mask_hi, "rainfall_mm"] = np.nan
        n_modified += mask_neg.sum() + mask_hi.sum()

    if "sr_kj_m2" in df.columns:
        df.loc[df["sr_kj_m2"] < 0, "sr_kj_m2"] = 0.0
        mask_hi = df["sr_kj_m2"] > 40000
        df.loc[mask_hi, "sr_kj_m2"] = np.nan
        n_modified += mask_hi.sum()

    for col in ["ws_2_m_s", "ws_gust_m_s"]:
        if col in df.columns:
            df.loc[df[col] < 0, col] = 0.0
            mask_hi = df[col] > 100
            df.loc[mask_hi, col] = np.nan
            n_modified += mask_hi.sum()

    if "wd_degrees" in df.columns:
        mask = (df["wd_degrees"] < 0) | (df["wd_degrees"] > 360)
        df.loc[mask, "wd_degrees"] = np.nan
        n_modified += mask.sum()

    # 3. Dew point consistency
    if all(c in df.columns for c in ["tair_dry_bulb_c", "rh_mean_porc", "dew_tmean_c"]):
        valid = (df["tair_dry_bulb_c"].notna() & df["rh_mean_porc"].notna()
                 & df["dew_tmean_c"].notna() & (df["rh_mean_porc"] > 0))
        if valid.sum() > 0:
            dew_calc = _calculate_dew_point(
                df.loc[valid, "tair_dry_bulb_c"],
                df.loc[valid, "rh_mean_porc"],
            )
            bad = (df.loc[valid, "dew_tmean_c"] - dew_calc).abs() > 3.0
            n = bad.sum()
            if n:
                df.loc[bad[bad].index, "dew_tmean_c"] = np.nan
                n_modified += n

    # 4. Stuck sensor
    def _check_static(series: pd.Series, max_h: int) -> pd.Series:
        mask  = pd.Series(False, index=series.index)
        count = 1
        for i in range(1, len(series)):
            if pd.isna(series.iloc[i]) or pd.isna(series.iloc[i - 1]):
                count = 1
                continue
            if series.iloc[i] == series.iloc[i - 1]:
                count += 1
                if count > max_h:
                    mask.iloc[i - count + 1: i + 1] = True
            else:
                count = 1
        return mask

    for col, max_h in [("tair_dry_bulb_c", 24), ("rh_mean_porc", 48), ("patm_mb", 72)]:
        if col in df.columns:
            mask = _check_static(df[col], max_h)
            n = mask.sum()
            if n:
                df.loc[mask, col] = np.nan
                n_modified += n

    # 5. Temporal gaps (warn only)
    if "date" in df.columns and len(df) > 1:
        diffs = df.sort_values("date")["date"].diff().dt.total_seconds() / 3600
        gaps  = (diffs > 6).sum()
        if gaps > 0 and verbose:
            print(f"  QC: {gaps} temporal gap(s) > 6h (data not removed)")

    if verbose:
        print(f"  QC: {n_modified} value(s) modified total")

    return df


# ---------------------------------------------------------------------------
# Aggregation rules
# ---------------------------------------------------------------------------

def _build_agg_rules(vars: list[str]) -> dict[str, str]:
    """Return aggregation rule per climate variable."""
    sum_vars  = {"rainfall_mm", "sr_kj_m2"}
    circ_vars = {"wd_degrees"}
    return {
        v: ("sum" if v in sum_vars else "mean_circular" if v in circ_vars else "mean")
        for v in vars
    }


def _agg_expr_sql(var: str, rule: str, suffix: str = "", alias_prefix: str = "c.") -> str:
    """Generate a SQL aggregation expression for a variable."""
    col_name = f"{var}{suffix}" if suffix else var
    src = f"{alias_prefix}{var}"
    if rule == "sum":
        return f"SUM({src}) AS {col_name}"
    if rule == "mean_circular":
        return (
            f"DEGREES(ATAN2(AVG(SIN(RADIANS({src}))),"
            f"AVG(COS(RADIANS({src}))))) AS {col_name}"
        )
    return f"AVG({src}) AS {col_name}"


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_health_data(health_data) -> None:
    if not isinstance(health_data, duckdb.DuckDBPyRelation):
        raise TypeError(
            f"health_data must be a DuckDBPyRelation (output of sus_spatial_join()). "
            f"Got: {type(health_data).__name__}"
        )
    n    = health_data.count("*").fetchone()[0]
    cols = list(health_data.columns)
    if n == 0:
        raise ValueError("health_data is empty.")
    if not any(c in cols for c in _MUNI_CANDIDATES):
        raise ValueError(
            f"health_data has no municipality column.\n"
            f"Expected one of: {_MUNI_CANDIDATES}\nAvailable: {cols}"
        )
    if not any(c in cols for c in _DATE_CANDIDATES):
        raise ValueError(
            f"health_data has no date column.\n"
            f"Expected one of: {_DATE_CANDIDATES}\nAvailable: {cols}"
        )
    if "geometry_wkt" not in cols:
        raise ValueError(
            "health_data has no 'geometry_wkt' column.\n"
            "Tip: run cs.sus_spatial_join() to add geometry."
        )


def _validate_climate_data(climate_data) -> None:
    if isinstance(climate_data, duckdb.DuckDBPyRelation):
        # materialise to DataFrame for validation
        climate_data = climate_data.df()
    if not isinstance(climate_data, (pa.Table, pd.DataFrame)):
        raise TypeError(
            f"climate_data must be a pd.DataFrame or pa.Table. "
            f"Got: {type(climate_data).__name__}"
        )
    if len(climate_data) == 0:
        raise ValueError("climate_data is empty.")
    cols = set(climate_data.schema.names if isinstance(climate_data, pa.Table)
               else climate_data.columns)
    if not cols & set(_STATION_CANDIDATES):
        raise ValueError(
            f"climate_data has no station column.\n"
            f"Expected one of: {_STATION_CANDIDATES}\nAvailable: {cols}"
        )
    missing = {"latitude", "longitude", "date"} - cols
    if missing:
        raise ValueError(f"climate_data missing required columns: {missing}")
    if not cols & set(_KNOWN_CLIMATE_VARS):
        raise ValueError(
            f"climate_data has no known climate variables.\n"
            f"Expected: {_KNOWN_CLIMATE_VARS}"
        )


def _validate_date_overlap(health_data, climate_data) -> None:
    date_col = next((c for c in _DATE_CANDIDATES if c in health_data.columns), None)
    if date_col is None:
        return
    row = health_data.aggregate(
        f"MIN(CAST({date_col} AS DATE)) AS d_min, "
        f"MAX(CAST({date_col} AS DATE)) AS d_max"
    ).fetchone()
    h_min, h_max = row[0], row[1]

    if isinstance(climate_data, (duckdb.DuckDBPyRelation,)):
        climate_data = climate_data.df()
    dates = (pd.to_datetime(climate_data["date"].to_pandas()
                            if isinstance(climate_data, pa.Table)
                            else climate_data["date"]))
    c_min, c_max = dates.min().date(), dates.max().date()

    if h_min > c_max or h_max < c_min:
        raise ValueError(
            f"health_data and climate_data have no temporal overlap.\n"
            f"health_data:  {h_min} → {h_max}\n"
            f"climate_data: {c_min} → {c_max}"
        )
    if h_min < c_min:
        warnings.warn(
            f"health_data starts {h_min} but climate_data starts {c_min}. "
            f"Events before {c_min} will have no climate data."
        )
    if h_max > c_max:
        warnings.warn(
            f"health_data ends {h_max} but climate_data ends {c_max}. "
            f"Events after {c_max} will have no climate data."
        )


def _validate_strategy_params(
    temporal_strategy, window_days, lag_days, offset_days,
    temp_base, threshold_value, threshold_direction, weights, climate_vars,
) -> None:
    needs_window = {"moving_window", "degree_days", "threshold_exceedance",
                    "cold_wave_exceedance", "weighted_window"}
    if temporal_strategy in needs_window and window_days is None:
        raise ValueError(f"'window_days' is required for strategy='{temporal_strategy}'.")
    if window_days is not None and window_days < 1:
        raise ValueError(f"'window_days' must be >= 1. Got: {window_days}")
    if temporal_strategy in {"discrete_lag", "distributed_lag"} and lag_days is None:
        raise ValueError(f"'lag_days' is required for strategy='{temporal_strategy}'.")
    if lag_days is not None and not all(isinstance(l, int) and l >= 0 for l in lag_days):
        raise ValueError(f"'lag_days' must be a list of non-negative integers.")
    if temporal_strategy == "offset_window" and offset_days is None:
        raise ValueError(f"'offset_days' is required for strategy='offset_window'.")
    if offset_days is not None and (len(offset_days) != 2 or offset_days[0] >= offset_days[1]):
        raise ValueError(f"'offset_days' must be [W1, W2] with W1 < W2.")
    if temporal_strategy == "degree_days" and temp_base is None:
        raise ValueError(f"'temp_base' is required for strategy='degree_days'.")
    if temporal_strategy in {"threshold_exceedance", "cold_wave_exceedance"} and threshold_value is None:
        raise ValueError(f"'threshold_value' is required for strategy='{temporal_strategy}'.")
    if threshold_direction not in ("above", "below"):
        raise ValueError(f"'threshold_direction' must be 'above' or 'below'.")
    if weights is not None and window_days is not None:
        if len(weights) != window_days + 1:
            raise ValueError(f"'weights' must have size window_days + 1 = {window_days + 1}.")
    if not climate_vars:
        raise ValueError("No valid climate variables found.")


# ---------------------------------------------------------------------------
# Climate region detection
# ---------------------------------------------------------------------------

def _detect_climate_region(climate_data, climate_region: str = "auto",
                            verbose: bool = False) -> str:
    valid = {"auto", "tropical", "subtropical", "temperate"}
    if climate_region not in valid:
        raise ValueError(f"climate_region must be one of {sorted(valid)}.")
    if climate_region != "auto":
        if verbose:
            print(f"Climate region (provided): {climate_region}")
        return climate_region
    lats = (climate_data["latitude"].to_pandas()
            if isinstance(climate_data, pa.Table)
            else climate_data["latitude"])
    mean_lat = lats.dropna().mean()
    detected = ("tropical" if mean_lat > -5
                else "subtropical" if mean_lat > -23
                else "temperate")
    if verbose:
        print(f"Climate region (auto): {detected} (mean lat: {mean_lat:.1f}°)")
    return detected


def _get_region_defaults(climate_region: str) -> dict:
    defaults = {
        "tropical":    {"temp_base_health": 20.0, "temp_base_vector": 11.0,
                        "heatwave_threshold": 35.0, "coldwave_threshold": None,
                        "description": "Tropical (Norte, Nordeste)"},
        "subtropical": {"temp_base_health": 18.0, "temp_base_vector": 11.0,
                        "heatwave_threshold": 32.0, "coldwave_threshold": 10.0,
                        "description": "Subtropical (Centro-Oeste, Sudeste)"},
        "temperate":   {"temp_base_health": 15.0, "temp_base_vector": 11.0,
                        "heatwave_threshold": 30.0, "coldwave_threshold": 5.0,
                        "description": "Temperado (Sul)"},
    }
    if climate_region not in defaults:
        raise ValueError(f"climate_region must be one of {list(defaults)}.")
    return defaults[climate_region]


# ---------------------------------------------------------------------------
# Daily pre-aggregation (Block 6)
# ---------------------------------------------------------------------------

def _aggregate_meteo_daily(climate_data, time_unit: str = "day") -> pa.Table:
    """Aggregate hourly INMET data to daily (or coarser) resolution."""
    if isinstance(climate_data, pd.DataFrame):
        climate_data = pa.Table.from_pandas(climate_data, preserve_index=False)
    elif isinstance(climate_data, duckdb.DuckDBPyRelation):
        climate_data = climate_data.arrow()

    valid_units = {"day", "week", "month", "season", "year"}
    if time_unit not in valid_units:
        raise ValueError(f"time_unit must be one of {sorted(valid_units)}.")

    con = duckdb.connect()
    con.register("climate", climate_data)
    cols = climate_data.schema.names

    station_col = next((c for c in _STATION_CANDIDATES if c in cols), None)
    if station_col is None:
        raise ValueError(f"climate_data has no station column. Available: {cols}")

    check = con.execute(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT CAST(date AS DATE)) AS nd,
               COUNT(DISTINCT {station_col}) AS ns
        FROM climate
    """).fetchone()
    n_rows, n_dates, n_stations = check
    rows_per = n_rows / max(n_dates * n_stations, 1)

    if rows_per <= 1.5 and time_unit == "day":
        con.close()
        return climate_data

    if time_unit == "day":
        time_expr, time_alias = "CAST(date AS DATE)", "date"
    elif time_unit == "week":
        time_expr, time_alias = "DATE_TRUNC('week', CAST(date AS DATE))", "date"
    elif time_unit == "month":
        time_expr, time_alias = "DATE_TRUNC('month', CAST(date AS DATE))", "date"
    elif time_unit == "year":
        time_expr, time_alias = "DATE_TRUNC('year', CAST(date AS DATE))", "date"
    else:  # season
        time_expr = ("CASE WHEN MONTH(CAST(date AS DATE)) IN (12,1,2) THEN 'DJF'"
                     " WHEN MONTH(CAST(date AS DATE)) IN (3,4,5) THEN 'MAM'"
                     " WHEN MONTH(CAST(date AS DATE)) IN (6,7,8) THEN 'JJA'"
                     " ELSE 'SON' END")
        time_alias = "season"

    meta_cols = [c for c in ["station_name", "UF", "region", "latitude",
                              "longitude", "altitude", "founded_date"] if c in cols]
    meta_sql  = (", " + ", ".join(f"FIRST({c}) AS {c}" for c in meta_cols)
                 if meta_cols else "")

    agg_sum  = [c for c in ["rainfall_mm", "sr_kj_m2"] if c in cols]
    agg_max  = [c for c in ["tair_max_c", "dew_tmax_c", "rh_max_porc",
                             "ws_gust_m_s", "patm_max_mb"] if c in cols]
    agg_min  = [c for c in ["tair_min_c", "dew_tmin_c", "rh_min_porc",
                             "patm_min_mb"] if c in cols]
    agg_mean = [c for c in ["tair_dry_bulb_c", "dew_tmean_c", "rh_mean_porc",
                             "patm_mb", "ws_2_m_s"] if c in cols]

    agg_parts = (
        [f"SUM({c}) AS {c}" for c in agg_sum]
        + [f"MAX({c}) AS {c}" for c in agg_max]
        + [f"MIN({c}) AS {c}" for c in agg_min]
        + [f"AVG({c}) AS {c}" for c in agg_mean]
    )
    if "wd_degrees" in cols:
        agg_parts.append(
            "DEGREES(ATAN2(AVG(SIN(RADIANS(wd_degrees))),"
            "AVG(COS(RADIANS(wd_degrees))))) AS wd_degrees"
        )

    if time_unit == "season":
        group_by    = f"{station_col}, YEAR(CAST(date AS DATE)), {time_expr}"
        select_time = f"{time_expr} AS {time_alias}, YEAR(CAST(date AS DATE)) AS year"
    else:
        group_by    = f"{station_col}, {time_expr}"
        select_time = f"{time_expr} AS {time_alias}"

    query = f"""
        SELECT {station_col} AS station_code, {select_time}{meta_sql},
               {', '.join(agg_parts)}
        FROM climate
        GROUP BY {group_by}
        ORDER BY station_code, {time_alias}
    """
    result = con.execute(query).to_arrow_table()
    con.close()
    return result


# ---------------------------------------------------------------------------
# Spatial matching (Block 7)
# ---------------------------------------------------------------------------

def _match_spatial(climate_data: pa.Table, health_data: duckdb.DuckDBPyRelation,
                   verbose: bool = False) -> pa.Table:
    """Associate each municipality to its nearest INMET station."""
    from shapely import wkt as shapely_wkt

    climate_cols = climate_data.schema.names
    station_col  = next((c for c in _STATION_CANDIDATES if c in climate_cols), None)
    if station_col is None:
        raise ValueError(f"climate_data has no station column.")

    muni_col = next((c for c in _MUNI_CANDIDATES if c in health_data.columns), None)
    if muni_col is None:
        raise ValueError(f"health_data has no municipality column.")

    # unique municipalities with geometry
    # Project only the two needed columns before any materialisation —
    # avoids OOM caused by geometry_wkt multipolygons on large datasets.
    # The budget below is scoped to this materialisation only. Applied with
    # bare SET on the singleton from get_connection(), as it was before, it
    # left the whole session at 2 GB and two threads after any call that
    # reached here -- and the final state then depended on call order,
    # since sus_climate_inmet() sets its own 96 MB budget.
    with duckdb_settings(
        memory_limit="2GB",
        preserve_insertion_order=False,
        threads=2,
    ) as conn_h:
        slim = health_data.select(
            f"LEFT(CAST({muni_col} AS VARCHAR), 6) AS code_muni, geometry_wkt"
        )
        conn_h.register("_h_slim", slim)
        munic_df = conn_h.execute("""
            SELECT DISTINCT code_muni, geometry_wkt
            FROM _h_slim
            WHERE code_muni IS NOT NULL AND geometry_wkt IS NOT NULL
        """).df()

    munic_df["geometry"] = munic_df["geometry_wkt"].apply(
        lambda x: shapely_wkt.loads(x) if x else None
    )
    munic_df = munic_df.drop(columns=["geometry_wkt"])
    munic_df = munic_df[
        munic_df["code_muni"].notna() &
        (munic_df["code_muni"] != "nan") &
        (munic_df["code_muni"] != "")
    ].reset_index(drop=True)

    if len(munic_df) == 0:
        raise ValueError("No valid municipalities found in health_data.")

    # representative points
    centroids      = []
    fallback_count = 0
    for geom in munic_df["geometry"]:
        if geom is None:
            centroids.append((0.0, 0.0))
            fallback_count += 1
            continue
        try:
            pt = geom.representative_point()
        except Exception:
            pt = geom.centroid
            fallback_count += 1
        centroids.append((pt.x, pt.y))

    centroids = np.array(centroids)
    if verbose and fallback_count > 0:
        print(f"  Spatial: {fallback_count} municipality(ies) used centroid fallback.")

    # unique stations
    con = duckdb.connect()
    con.register("climate", climate_data)
    stations = con.execute(f"""
        SELECT DISTINCT {station_col} AS station_code, latitude, longitude
        FROM climate
        WHERE {station_col} IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL
    """).to_arrow_table()

    if len(stations) == 0:
        raise ValueError("No valid stations found in climate_data.")

    station_coords = np.column_stack([
        stations["longitude"].to_pylist(),
        stations["latitude"].to_pylist(),
    ])

    # KD-Tree nearest station
    tree                = cKDTree(station_coords)
    distances_deg, idxs = tree.query(centroids, k=1)
    distances_km        = distances_deg * 111.0
    station_codes_list  = stations["station_code"].to_pylist()

    mun_station_map = pa.table({
        "code_muni":    pa.array(munic_df["code_muni"].tolist(), type=pa.string()),
        "station_code": pa.array([station_codes_list[i] for i in idxs], type=pa.string()),
        "distance_km":  pa.array(distances_km.tolist(), type=pa.float32()),
    })

    if verbose:
        d = distances_km
        print(f"  Spatial matching: {len(mun_station_map)} municipality(ies) → "
              f"{len(stations)} station(s)")
        print(f"  Distance (km): min={d.min():.1f} | "
              f"median={np.median(d):.1f} | max={d.max():.1f}")

    con.register("map", mun_station_map)
    result = con.execute(f"""
        SELECT m.code_muni, m.distance_km, c.*
        FROM climate c
        JOIN map m ON CAST(c.{station_col} AS VARCHAR) = m.station_code
        ORDER BY m.code_muni, c.date, c.{station_col}
    """).to_arrow_table()
    con.close()
    return result


# ---------------------------------------------------------------------------
# Temporal join strategies (Block 8)
# ---------------------------------------------------------------------------

def _health_view(conn, health_data, muni_col, date_col, view_name):
    """Register health data with row_id, _muni_join, _date_join."""
    conn.register(f"_{view_name}_src", health_data)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT
            ROW_NUMBER() OVER () AS _row_id,
            LEFT(CAST({muni_col} AS VARCHAR), 6) AS _muni_join,
            CAST({date_col} AS DATE)             AS _date_join,
            *
        FROM _{view_name}_src
    """)


def _drop_internals(result: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    final = [c for c in result.columns
             if c not in ("_row_id", "_muni_join", "_date_join")]
    return result.select(", ".join(final))


def _join_exact(health_data, climate_data, climate_vars):
    date_col = detect_date_column(list(health_data.columns))
    muni_col = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av  = [v for v in climate_vars if v in climate_data.schema.names]
    rules    = _build_agg_rules(vars_av)
    agg_sql  = ", ".join(_agg_expr_sql(v, rules[v], alias_prefix="") for v in vars_av)
    vars_sel = ", ".join(f"c.{v}" for v in vars_av)
    conn     = get_connection()
    conn.register("_h_ex", health_data)
    conn.register("_c_ex", climate_data)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _c_ex_agg AS
        SELECT CAST(code_muni AS VARCHAR) AS code_muni,
               CAST(date AS DATE) AS date, {agg_sql}
        FROM _c_ex
        GROUP BY CAST(code_muni AS VARCHAR), CAST(date AS DATE)
    """)
    return conn.sql(f"""
        SELECT h.*, {vars_sel}
        FROM _h_ex h
        LEFT JOIN _c_ex_agg c
            ON  LEFT(CAST(h.{muni_col} AS VARCHAR), 6) = c.code_muni
            AND CAST(h.{date_col} AS DATE) = c.date
    """)


def _join_moving_window(health_data, climate_data, climate_vars,
                        window_days, min_obs=0.7):
    date_col    = detect_date_column(list(health_data.columns))
    muni_col    = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av     = [v for v in climate_vars if v in climate_data.schema.names]
    rules       = _build_agg_rules(vars_av)
    window_size = window_days + 1
    col_names   = []
    agg_exprs   = []
    for v in vars_av:
        rule = rules[v]
        sfx  = f"_sum_w{window_days}" if rule == "sum" else f"_mean_w{window_days}"
        col_names.append(f"{v}{sfx}")
        agg_exprs.append(_agg_expr_sql(v, rule, sfx, alias_prefix=""))
    conn = get_connection()
    conn.register("_h_mw", health_data)
    conn.register("_c_mw", climate_data)
    _health_view(conn, health_data, muni_col, date_col, "_h_mw_id")
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _mw_agg AS
        SELECT h._row_id, {", ".join(agg_exprs)}, COUNT(*) AS _n_obs
        FROM _h_mw_id h
        LEFT JOIN (SELECT CAST(code_muni AS VARCHAR) AS code_muni,
                          CAST(date AS DATE) AS date,
                          {", ".join(vars_av)} FROM _c_mw) c
            ON  h._muni_join = c.code_muni
            AND c.date BETWEEN h._date_join - INTERVAL ({window_days}) DAYS
                           AND h._date_join
        GROUP BY h._row_id
    """)
    min_sql = ", ".join(
        f"CASE WHEN (_n_obs::FLOAT/{window_size})>={min_obs} THEN {col} ELSE NULL END AS {col}"
        for col in col_names
    )
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _mw_filt AS
        SELECT _row_id, {min_sql} FROM _mw_agg
    """)
    result = conn.sql(
        f"SELECT h.*, {', '.join(f'c.{col}' for col in col_names)} "
        f"FROM _h_mw_id h LEFT JOIN _mw_filt c ON h._row_id = c._row_id"
    )
    return _drop_internals(result)


def _join_discrete_lag(health_data, climate_data, climate_vars, lag_days):
    date_col = detect_date_column(list(health_data.columns))
    muni_col = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av  = [v for v in climate_vars if v in climate_data.schema.names]
    conn     = get_connection()
    conn.register("_h_dl", health_data)
    conn.register("_c_dl", climate_data)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _c_dl_agg AS
        SELECT CAST(code_muni AS VARCHAR) AS code_muni,
               CAST(date AS DATE) AS date,
               {", ".join(f"AVG({v}) AS {v}" for v in vars_av)}
        FROM _c_dl
        GROUP BY CAST(code_muni AS VARCHAR), CAST(date AS DATE)
    """)
    _health_view(conn, health_data, muni_col, date_col, "_h_dl_id")
    for lag in sorted(lag_days):
        conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW _lag_{lag} AS
            SELECT h._row_id, {", ".join(f"c.{v} AS {v}_lag{lag}" for v in vars_av)}
            FROM _h_dl_id h
            LEFT JOIN _c_dl_agg c
                ON h._muni_join = c.code_muni
                AND c.date = h._date_join - INTERVAL ({lag}) DAYS
        """)
    lag_joins = "\n".join(
        f"LEFT JOIN _lag_{lag} l{lag} ON h._row_id = l{lag}._row_id"
        for lag in sorted(lag_days)
    )
    lag_cols = ", ".join(
        f"l{lag}.{v}_lag{lag}"
        for lag in sorted(lag_days) for v in vars_av
    )
    result = conn.sql(
        f"SELECT h.*, {lag_cols} FROM _h_dl_id h {lag_joins}"
    )
    return _drop_internals(result)


def _join_distributed_lag(health_data, climate_data, climate_vars, max_lag):
    return _join_discrete_lag(health_data, climate_data, climate_vars,
                               lag_days=list(range(0, max_lag + 1)))


def _join_offset_window(health_data, climate_data, climate_vars,
                        offset_days, min_obs=0.7):
    date_col    = detect_date_column(list(health_data.columns))
    muni_col    = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av     = [v for v in climate_vars if v in climate_data.schema.names]
    rules       = _build_agg_rules(vars_av)
    w1, w2      = offset_days[0], offset_days[1]
    window_size = (w2 - w1) + 1
    col_names   = [f"{v}_off{w1}to{w2}" for v in vars_av]
    agg_exprs   = [_agg_expr_sql(v, rules[v], f"_off{w1}to{w2}", "") for v in vars_av]
    conn        = get_connection()
    conn.register("_h_ow", health_data)
    conn.register("_c_ow", climate_data)
    _health_view(conn, health_data, muni_col, date_col, "_h_ow_id")
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _ow_agg AS
        SELECT h._row_id, {", ".join(agg_exprs)}, COUNT(*) AS _n_obs
        FROM _h_ow_id h
        LEFT JOIN (SELECT CAST(code_muni AS VARCHAR) AS code_muni,
                          CAST(date AS DATE) AS date,
                          {", ".join(vars_av)} FROM _c_ow) c
            ON  h._muni_join = c.code_muni
            AND c.date BETWEEN h._date_join - INTERVAL ({w2}) DAYS
                           AND h._date_join - INTERVAL ({w1}) DAYS
        GROUP BY h._row_id
    """)
    min_sql = ", ".join(
        f"CASE WHEN (_n_obs::FLOAT/{window_size})>={min_obs} THEN {col} ELSE NULL END AS {col}"
        for col in col_names
    )
    conn.execute(f"CREATE OR REPLACE TEMP VIEW _ow_filt AS SELECT _row_id, {min_sql} FROM _ow_agg")
    result = conn.sql(
        f"SELECT h.*, {', '.join(f'c.{col}' for col in col_names)} "
        f"FROM _h_ow_id h LEFT JOIN _ow_filt c ON h._row_id = c._row_id"
    )
    return _drop_internals(result)


def _join_degree_days(health_data, climate_data, window_days,
                      temp_base, gdd_temp_var="tair_dry_bulb_c", min_obs=0.7):
    date_col    = detect_date_column(list(health_data.columns))
    muni_col    = detect_geo_column(list(health_data.columns), level="municipality")
    window_size = window_days + 1
    tb_str      = str(temp_base).replace(".", "p")
    col_name    = f"gdd_w{window_days}_tbase{tb_str}"
    conn        = get_connection()
    conn.register("_h_dd", health_data)
    conn.register("_c_dd", climate_data)
    _health_view(conn, health_data, muni_col, date_col, "_h_dd_id")
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _gdd_agg AS
        SELECT h._row_id,
               SUM(GREATEST(0.0, c.{gdd_temp_var} - {temp_base})) AS {col_name},
               COUNT(c.{gdd_temp_var}) AS _n_obs
        FROM _h_dd_id h
        LEFT JOIN (SELECT CAST(code_muni AS VARCHAR) AS code_muni,
                          CAST(date AS DATE) AS date, {gdd_temp_var} FROM _c_dd) c
            ON  h._muni_join = c.code_muni
            AND c.date BETWEEN h._date_join - INTERVAL ({window_days}) DAYS
                           AND h._date_join
        GROUP BY h._row_id
    """)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _gdd_filt AS
        SELECT _row_id,
               CASE WHEN (_n_obs::FLOAT/{window_size})>={min_obs}
                    THEN {col_name} ELSE NULL END AS {col_name}
        FROM _gdd_agg
    """)
    result = conn.sql(
        f"SELECT h.*, c.{col_name} FROM _h_dd_id h "
        f"LEFT JOIN _gdd_filt c ON h._row_id = c._row_id"
    )
    return _drop_internals(result)


def _join_threshold_exceedance(health_data, climate_data, climate_vars,
                                window_days, threshold_value,
                                threshold_direction="above", min_obs=0.7):
    date_col    = detect_date_column(list(health_data.columns))
    muni_col    = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av     = [v for v in climate_vars if v in climate_data.schema.names]
    window_size = window_days + 1
    op          = ">" if threshold_direction == "above" else "<"
    dir_lbl     = "gt" if threshold_direction == "above" else "lt"
    thr_lbl     = str(threshold_value).replace(".", "p")
    conn        = get_connection()
    conn.register("_h_te", health_data)
    conn.register("_c_te", climate_data)
    _health_view(conn, health_data, muni_col, date_col, "_h_te_id")
    col_names, agg_ex, cnt_ex = [], [], []
    for v in vars_av:
        n_col = f"nexc{window_days}_{dir_lbl}{thr_lbl}_{v}"
        p_col = f"pexc{window_days}_{dir_lbl}{thr_lbl}_{v}"
        col_names += [n_col, p_col]
        agg_ex += [
            f"SUM(CASE WHEN c.{v} {op} {threshold_value} THEN 1.0 ELSE 0.0 END) AS {n_col}",
            f"SUM(CASE WHEN c.{v} {op} {threshold_value} THEN 1.0 ELSE 0.0 END)"
            f"/NULLIF(COUNT(c.{v}),0) AS {p_col}",
        ]
        cnt_ex.append(f"COUNT(c.{v}) AS _n_{v}")
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _te_agg AS
        SELECT h._row_id, {", ".join(agg_ex)}, {", ".join(cnt_ex)}
        FROM _h_te_id h
        LEFT JOIN (SELECT CAST(code_muni AS VARCHAR) AS code_muni,
                          CAST(date AS DATE) AS date,
                          {", ".join(vars_av)} FROM _c_te) c
            ON  h._muni_join = c.code_muni
            AND c.date BETWEEN h._date_join - INTERVAL ({window_days}) DAYS
                           AND h._date_join
        GROUP BY h._row_id
    """)
    min_sql = ", ".join(
        f"CASE WHEN (_n_{v}::FLOAT/{window_size})>={min_obs} "
        f"THEN {n} ELSE NULL END AS {n}, "
        f"CASE WHEN (_n_{v}::FLOAT/{window_size})>={min_obs} "
        f"THEN {p} ELSE NULL END AS {p}"
        for v, n, p in zip(vars_av,
                           col_names[::2],
                           col_names[1::2])
    )
    conn.execute(f"CREATE OR REPLACE TEMP VIEW _te_filt AS SELECT _row_id, {min_sql} FROM _te_agg")
    result = conn.sql(
        f"SELECT h.*, {', '.join(f'c.{col}' for col in col_names)} "
        f"FROM _h_te_id h LEFT JOIN _te_filt c ON h._row_id = c._row_id"
    )
    return _drop_internals(result)


def _join_cold_wave_exceedance(health_data, climate_data, climate_vars,
                                window_days, threshold_value, min_obs=0.7):
    return _join_threshold_exceedance(
        health_data, climate_data, climate_vars,
        window_days=window_days,
        threshold_value=threshold_value,
        threshold_direction="below",
        min_obs=min_obs,
    )


def _join_weighted_window(health_data, climate_data, climate_vars,
                           window_days, weights=None, min_obs=0.7):
    date_col    = detect_date_column(list(health_data.columns))
    muni_col    = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av     = [v for v in climate_vars if v in climate_data.schema.names]
    rules       = _build_agg_rules(vars_av)
    window_size = window_days + 1
    if weights is None:
        weights = list(np.linspace(1.0, 0.1, window_size))
    conn = get_connection()
    conn.register("_h_ww", health_data)
    conn.register("_c_ww", climate_data)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _c_ww_agg AS
        SELECT CAST(code_muni AS VARCHAR) AS code_muni,
               CAST(date AS DATE) AS date,
               {", ".join(f"AVG({v}) AS {v}" for v in vars_av)}
        FROM _c_ww GROUP BY CAST(code_muni AS VARCHAR), CAST(date AS DATE)
    """)
    _health_view(conn, health_data, muni_col, date_col, "_h_ww_id")
    for lag in range(window_size):
        w = weights[lag]
        conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW _ww_lag_{lag} AS
            SELECT h._row_id,
                   {", ".join(f"c.{v}*{w} AS {v}_w{lag}" for v in vars_av)},
                   {", ".join(f"CASE WHEN c.{v} IS NOT NULL THEN {w} ELSE 0.0 END AS {v}_sw{lag}" for v in vars_av)}
            FROM _h_ww_id h
            LEFT JOIN _c_ww_agg c
                ON h._muni_join = c.code_muni
                AND c.date = h._date_join - INTERVAL ({lag}) DAYS
        """)
    lag_joins = "\n".join(
        f"LEFT JOIN _ww_lag_{lag} l{lag} ON h._row_id = l{lag}._row_id"
        for lag in range(window_size)
    )
    col_names = []
    wwin_exprs = []
    for v in vars_av:
        col = f"{v}_wwin{window_days}"
        col_names.append(col)
        rule    = rules[v]
        v_sum   = "+".join(f"COALESCE(l{lag}.{v}_w{lag},0.0)" for lag in range(window_size))
        sw_sum  = "+".join(f"COALESCE(l{lag}.{v}_sw{lag},0.0)" for lag in range(window_size))
        n_valid = "+".join(f"CASE WHEN l{lag}.{v}_sw{lag}>0 THEN 1 ELSE 0 END" for lag in range(window_size))
        expr = (f"SUM" if rule == "sum" else "AVG")
        if rule == "sum":
            wwin_exprs.append(
                f"CASE WHEN (({n_valid})::FLOAT/{window_size})>={min_obs} "
                f"THEN ({v_sum}) ELSE NULL END AS {col}"
            )
        else:
            wwin_exprs.append(
                f"CASE WHEN (({n_valid})::FLOAT/{window_size})>={min_obs} "
                f"THEN ({v_sum})/NULLIF({sw_sum},0) ELSE NULL END AS {col}"
            )
    result = conn.sql(
        f"SELECT h.*, {', '.join(wwin_exprs)} FROM _h_ww_id h {lag_joins}"
    )
    return _drop_internals(result)


def _join_seasonal(health_data, climate_data, climate_vars, min_days=60):
    date_col = detect_date_column(list(health_data.columns))
    muni_col = detect_geo_column(list(health_data.columns), level="municipality")
    vars_av  = [v for v in climate_vars if v in climate_data.schema.names]
    rules    = _build_agg_rules(vars_av)
    season_agg = []
    for v in vars_av:
        rule = rules[v]
        if rule == "sum":
            season_agg.append(f"SUM({v}) AS season_{v}")
        elif rule == "mean_circular":
            season_agg.append(
                f"DEGREES(ATAN2(AVG(SIN(RADIANS({v}))),"
                f"AVG(COS(RADIANS({v}))))) AS season_{v}"
            )
        else:
            season_agg.append(f"AVG({v}) AS season_{v}")
    season_expr = ("CASE WHEN MONTH(CAST(date AS DATE)) IN (12,1,2) THEN 'DJF'"
                   " WHEN MONTH(CAST(date AS DATE)) IN (3,4,5) THEN 'MAM'"
                   " WHEN MONTH(CAST(date AS DATE)) IN (6,7,8) THEN 'JJA'"
                   " ELSE 'SON' END")
    health_season = ("CASE WHEN MONTH(CAST({dc} AS DATE)) IN (12,1,2) THEN 'DJF'"
                     " WHEN MONTH(CAST({dc} AS DATE)) IN (3,4,5) THEN 'MAM'"
                     " WHEN MONTH(CAST({dc} AS DATE)) IN (6,7,8) THEN 'JJA'"
                     " ELSE 'SON' END").format(dc=date_col)
    conn = get_connection()
    conn.register("_h_ss", health_data)
    conn.register("_c_ss", climate_data)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _climate_seasonal AS
        SELECT CAST(code_muni AS VARCHAR) AS code_muni,
               YEAR(CAST(date AS DATE)) AS year,
               {season_expr} AS season,
               {", ".join(season_agg)},
               COUNT(*) AS n_days
        FROM _c_ss
        GROUP BY CAST(code_muni AS VARCHAR), YEAR(CAST(date AS DATE)), {season_expr}
        HAVING COUNT(*) >= {min_days}
    """)
    conn.execute(f"""
        CREATE OR REPLACE TEMP VIEW _h_ss_id AS
        SELECT LEFT(CAST({muni_col} AS VARCHAR),6) AS _muni_join,
               YEAR(CAST({date_col} AS DATE))      AS _year,
               {health_season}                     AS _season, *
        FROM _h_ss
    """)
    season_sel = ", ".join(f"c.season_{v}" for v in vars_av)
    result = conn.sql(f"""
        SELECT h.*, {season_sel}
        FROM _h_ss_id h
        LEFT JOIN _climate_seasonal c
            ON h._muni_join = c.code_muni
            AND h._year = c.year
            AND h._season = c.season
    """)
    return result.select(", ".join(
        c for c in result.columns if c not in ("_muni_join", "_year", "_season")
    ))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_climate_aggregate(
    health_data,
    climate_data,
    temporal_strategy: str = "exact",
    climate_vars: list[str] | str = "all",
    climate_region: str = "auto",
    time_unit: str = "day",
    window_days: int | None = None,
    lag_days: list[int] | None = None,
    offset_days: list[int] | None = None,
    temp_base: float | None = None,
    gdd_temp_var: str = "tair_dry_bulb_c",
    threshold_value: float | None = None,
    threshold_direction: str = "above",
    weights: list[float] | None = None,
    min_obs: float = 0.7,
    min_days: int = 60,
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Integrate climate and health data using 10 temporal strategies.

    Mirrors ``climasus4r::sus_climate_aggregate()``.

    Receives health data with geometry (output of ``sus_spatial_join()``) and
    INMET climate data, and returns health data enriched with climate variables
    aggregated by the chosen temporal strategy.

    Args:
        health_data: ``DuckDBPyRelation`` from ``sus_spatial_join()``.
            Must contain municipality and date columns, plus ``geometry_wkt``.
        climate_data: ``pd.DataFrame`` or ``DuckDBPyRelation`` from
            ``sus_climate_inmet()``. Can be hourly or daily — auto-detected.
        temporal_strategy: One of ``"exact"``, ``"moving_window"``,
            ``"discrete_lag"``, ``"distributed_lag"``, ``"offset_window"``,
            ``"degree_days"``, ``"threshold_exceedance"``,
            ``"cold_wave_exceedance"``, ``"weighted_window"``, ``"seasonal"``.
        climate_vars: Variables to include. ``"all"`` uses all available.
        climate_region: ``"auto"`` (detect from latitude), ``"tropical"``,
            ``"subtropical"``, or ``"temperate"``.
        time_unit: Pre-aggregation unit — ``"day"`` (default), ``"week"``,
            ``"month"``, ``"season"``, ``"year"``.
        window_days: Window size in days. Required for ``moving_window``,
            ``degree_days``, ``threshold_exceedance``, ``cold_wave_exceedance``,
            ``weighted_window``.
        lag_days: List of lag days. Required for ``discrete_lag`` and
            ``distributed_lag``.
        offset_days: ``[W1, W2]`` with W1 < W2. Required for ``offset_window``.
        temp_base: Base temperature for degree days.
        gdd_temp_var: Temperature variable for degree days.
            Default: ``"tair_dry_bulb_c"``.
        threshold_value: Threshold for exceedance strategies.
        threshold_direction: ``"above"`` (default) or ``"below"``.
        weights: Weights for ``weighted_window`` (size ``window_days + 1``).
            Auto-generates linear decay 1.0 → 0.1 if ``None``.
        min_obs: Minimum proportion of valid observations in window (0-1).
            Default: 0.7.
        min_days: Minimum days per station for ``seasonal``. Default: 60.
        verbose: Print progress messages.

    Returns:
        ``DuckDBPyRelation`` with health data enriched with climate columns.
        Lazy — materialise with ``.df()`` or ``cs.materialize()``.
        Use ``cs.sus_climate_info(result)`` to inspect metadata.

    Example:
        >>> health_geo = cs.sus_spatial_join(stand)
        >>> climate    = cs.sus_climate_inmet(years=2023, uf="SE")
        >>> result     = cs.sus_climate_aggregate(health_geo, climate,
        ...                 temporal_strategy="moving_window", window_days=14)
        >>> cs.sus_climate_info(result)
    """
    _VALID_STRATEGIES = {
        "exact", "moving_window", "discrete_lag", "distributed_lag",
        "offset_window", "degree_days", "threshold_exceedance",
        "cold_wave_exceedance", "weighted_window", "seasonal",
    }
    if temporal_strategy not in _VALID_STRATEGIES:
        raise ValueError(
            f"temporal_strategy '{temporal_strategy}' is not valid.\n"
            f"Options: {sorted(_VALID_STRATEGIES)}"
        )

    # materialise climate if it's a DuckDBPyRelation
    if isinstance(climate_data, duckdb.DuckDBPyRelation):
        climate_data = climate_data.df()

    # --- Block 1-3: validation ---
    _validate_health_data(health_data)
    _validate_climate_data(climate_data)
    _validate_date_overlap(health_data, climate_data)

    # --- resolve climate_vars ---
    if isinstance(climate_data, pa.Table):
        climate_cols = climate_data.schema.names
    else:
        climate_cols = list(climate_data.columns)

    avail = [v for v in _KNOWN_CLIMATE_VARS if v in climate_cols]
    if climate_vars == "all" or climate_vars is None:
        climate_vars_resolved = avail
    else:
        climate_vars_resolved = [v for v in climate_vars if v in climate_cols]
        missing = [v for v in climate_vars if v not in climate_cols]
        if missing and verbose:
            print(f"Warning: variables not found and ignored: {missing}")

    # --- Block 4: climate region ---
    region  = _detect_climate_region(climate_data, climate_region, verbose=verbose)
    defs    = _get_region_defaults(region)

    if temp_base is None and temporal_strategy == "degree_days":
        temp_base = defs["temp_base_health"]
        if verbose:
            print(f"temp_base not set — using default for {region}: {temp_base}°C")

    if threshold_value is None and temporal_strategy == "threshold_exceedance":
        threshold_value = defs["heatwave_threshold"]
        if verbose:
            print(f"threshold_value not set — using default for {region}: {threshold_value}°C")

    if threshold_value is None and temporal_strategy == "cold_wave_exceedance":
        threshold_value = defs["coldwave_threshold"]
        if verbose:
            print(f"threshold_value not set — using default for {region}: {threshold_value}°C")

    # --- Block 5: validate strategy params ---
    _validate_strategy_params(
        temporal_strategy, window_days, lag_days, offset_days,
        temp_base, threshold_value, threshold_direction,
        weights, climate_vars_resolved,
    )

    if verbose:
        print(f"Strategy: {temporal_strategy}")

    # --- Block 6: daily pre-aggregation ---
    climate_daily = _aggregate_meteo_daily(climate_data, time_unit=time_unit)
    if verbose:
        print(f"Daily data: {len(climate_daily):,} rows")

    # --- Block 7: spatial matching ---
    if verbose:
        print("Performing spatial matching...")
    climate_matched = _match_spatial(climate_daily, health_data, verbose=verbose)

    # --- Block 8: temporal join ---
    if verbose:
        print(f"Applying strategy '{temporal_strategy}'...")

    dispatch = {
        "exact":               lambda: _join_exact(health_data, climate_matched, climate_vars_resolved),
        "moving_window":       lambda: _join_moving_window(health_data, climate_matched, climate_vars_resolved, window_days, min_obs),
        "discrete_lag":        lambda: _join_discrete_lag(health_data, climate_matched, climate_vars_resolved, lag_days),
        "distributed_lag":     lambda: _join_distributed_lag(health_data, climate_matched, climate_vars_resolved, max(lag_days) if lag_days else 0),
        "offset_window":       lambda: _join_offset_window(health_data, climate_matched, climate_vars_resolved, offset_days, min_obs),
        "degree_days":         lambda: _join_degree_days(health_data, climate_matched, window_days, temp_base, gdd_temp_var, min_obs),
        "threshold_exceedance":lambda: _join_threshold_exceedance(health_data, climate_matched, climate_vars_resolved, window_days, threshold_value, threshold_direction, min_obs),
        "cold_wave_exceedance":lambda: _join_cold_wave_exceedance(health_data, climate_matched, climate_vars_resolved, window_days, threshold_value, min_obs),
        "weighted_window":     lambda: _join_weighted_window(health_data, climate_matched, climate_vars_resolved, window_days, weights, min_obs),
        "seasonal":            lambda: _join_seasonal(health_data, climate_matched, climate_vars_resolved, min_days),
    }
    result = dispatch[temporal_strategy]()

    # --- sus_meta ---
    result = set_stage(result, "climate", _inherit_from=health_data)
    result = add_history(
        result,
        f"Climate aggregation: strategy={temporal_strategy}; "
        f"vars={len(climate_vars_resolved)}; region={region}; time_unit={time_unit}"
    )

    # --- Block 9: metadata dict ---
    date_col = next((c for c in _DATE_CANDIDATES if c in result.columns), None)
    new_cols = [c for c in result.columns if c not in health_data.columns]

    match_rate = None
    if new_cols:
        total      = result.count("*").fetchone()[0]
        n_not_null = result.filter(f"{new_cols[0]} IS NOT NULL").count("*").fetchone()[0]
        match_rate = round(n_not_null / total * 100, 1) if total > 0 else 0.0

    period_start = period_end = None
    if date_col:
        row = result.aggregate(
            f"MIN(CAST({date_col} AS DATE)) AS d_min, "
            f"MAX(CAST({date_col} AS DATE)) AS d_max"
        ).fetchone()
        period_start = str(row[0]) if row[0] else None
        period_end   = str(row[1]) if row[1] else None

    muni_col = next((c for c in _MUNI_CANDIDATES if c in result.columns), None)
    n_munic  = None
    if muni_col:
        n_munic = result.aggregate(
            f"COUNT(DISTINCT LEFT(CAST({muni_col} AS VARCHAR), 6)) AS n"
        ).fetchone()[0]

    n_events = result.count("*").fetchone()[0]

    _CLIMASUS_META[id(result)] = {
        "strategy":            temporal_strategy,
        "climate_vars":        climate_vars_resolved,
        "climate_region":      region,
        "time_unit":           time_unit,
        "window_days":         window_days,
        "lag_days":            lag_days,
        "offset_days":         offset_days,
        "temp_base":           temp_base,
        "threshold_value":     threshold_value,
        "threshold_direction": threshold_direction,
        "weights":             weights,
        "min_obs":             min_obs,
        "min_days":            min_days,
        "n_events":            n_events,
        "n_municipalities":    n_munic,
        "match_rate_pct":      match_rate,
        "new_columns":         new_cols,
        "period_start":        period_start,
        "period_end":          period_end,
    }

    if verbose:
        print(f"\nDone:")
        print(f"  Events:          {n_events:,}")
        print(f"  Municipalities:  {n_munic}")
        print(f"  Climate columns: {len(new_cols)}")
        print(f"  Match rate:      {match_rate}%")
        print(f"  Period:          {period_start} → {period_end}")

    return result


def sus_climate_info(result: duckdb.DuckDBPyRelation) -> None:
    """Display metadata from the last sus_climate_aggregate() call.

    Args:
        result: Output of ``sus_climate_aggregate()``.
    """
    meta = _CLIMASUS_META.get(id(result))
    if meta is None:
        print("No metadata — object was not generated by sus_climate_aggregate().")
        return

    print("=" * 55)
    print("  climasus4py — sus_climate_aggregate() result")
    print("=" * 55)
    print(f"\n  Strategy:       {meta['strategy']}")
    print(f"  Region:         {meta['climate_region']}")
    print(f"  Period:         {meta['period_start']} → {meta['period_end']}")
    print(f"  Events:         {meta['n_events']:,}")
    print(f"  Municipalities: {meta['n_municipalities']}")
    print(f"  Match rate:     {meta['match_rate_pct']}%")
    print(f"\n  Climate variables ({len(meta['climate_vars'])}):")
    for v in meta["climate_vars"]:
        print(f"    - {v}")
    print(f"\n  Added columns ({len(meta['new_columns'])}):")
    for c in meta["new_columns"]:
        print(f"    - {c}")
    params = {k: v for k, v in meta.items()
              if k in ("window_days", "lag_days", "offset_days", "temp_base",
                       "threshold_value", "threshold_direction", "min_obs", "min_days")
              and v is not None}
    if params:
        print(f"\n  Parameters:")
        for k, v in params.items():
            print(f"    {k}: {v}")
    print("=" * 55)
