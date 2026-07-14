"""Temporal aggregation of SUS health microdata.

Mirrors R: aggregate.R
Pipeline stage: "aggregate" (registered in sus_meta after processing).
"""

from __future__ import annotations

import duckdb
import pandas as pd

from ..utils.data import load_json
from ._stage import add_history, set_stage
from .engine import get_connection, schema_columns


# ---------------------------------------------------------------------------
# JSON loaders (with fallback defaults)
# ---------------------------------------------------------------------------

def _agg_labels_config() -> dict:
    try:
        return load_json("templates/aggregate_labels.json")
    except FileNotFoundError:
        return {}


def _agg_config() -> dict:
    try:
        return load_json("templates/aggregate_config.json")
    except FileNotFoundError:
        return {}


# ---------------------------------------------------------------------------
# Dictionaries (SQL stays in code — only labels/config go to JSON)
# ---------------------------------------------------------------------------

AGG_TIME_EXPRS: dict[str, str | None] = {
    "year":    "DATE_TRUNC('year',    TRY_CAST({date} AS DATE))",
    "quarter": "DATE_TRUNC('quarter', TRY_CAST({date} AS DATE))",
    "month":   "DATE_TRUNC('month',   TRY_CAST({date} AS DATE))",
    "week":    "DATE_TRUNC('week',    TRY_CAST({date} AS DATE))",
    "day":     "TRY_CAST({date} AS DATE)",
    "5 days":  "DATE_TRUNC('day', TRY_CAST({date} AS DATE)) - INTERVAL (((DAYOFYEAR(TRY_CAST({date} AS DATE)) - 1) % 5)) DAY",
    "14 days": "DATE_TRUNC('day', TRY_CAST({date} AS DATE)) - INTERVAL (((DAYOFYEAR(TRY_CAST({date} AS DATE)) - 1) % 14)) DAY",
    "season":  None,
}

AGG_FUN_EXPRS: dict[str, str] = {
    "count":  "COUNT(*)",
    "sum":    "SUM(TRY_CAST({col} AS DOUBLE))",
    "mean":   "AVG(TRY_CAST({col} AS DOUBLE))",
    "median": "MEDIAN(TRY_CAST({col} AS DOUBLE))",
    "min":    "MIN(TRY_CAST({col} AS DOUBLE))",
    "max":    "MAX(TRY_CAST({col} AS DOUBLE))",
    "sd":     "STDDEV(TRY_CAST({col} AS DOUBLE))",
    "q25":    "QUANTILE_CONT(TRY_CAST({col} AS DOUBLE), 0.25)",
    "q75":    "QUANTILE_CONT(TRY_CAST({col} AS DOUBLE), 0.75)",
    "q95":    "QUANTILE_CONT(TRY_CAST({col} AS DOUBLE), 0.95)",
    "q99":    "QUANTILE_CONT(TRY_CAST({col} AS DOUBLE), 0.99)",
}

AGG_SEASON_SQL = """
    CASE
        WHEN EXTRACT(MONTH FROM {date}) IN (12, 1, 2) THEN
            CAST(
                CASE WHEN EXTRACT(MONTH FROM {date}) IN (1, 2)
                     THEN CAST(EXTRACT(YEAR FROM {date}) - 1 AS VARCHAR)
                     ELSE CAST(EXTRACT(YEAR FROM {date}) AS VARCHAR)
                END || '-12-01' AS DATE
            )
        WHEN EXTRACT(MONTH FROM {date}) IN (3, 4, 5)   THEN
            CAST(CAST(EXTRACT(YEAR FROM {date}) AS VARCHAR) || '-03-01' AS DATE)
        WHEN EXTRACT(MONTH FROM {date}) IN (6, 7, 8)   THEN
            CAST(CAST(EXTRACT(YEAR FROM {date}) AS VARCHAR) || '-06-01' AS DATE)
        WHEN EXTRACT(MONTH FROM {date}) IN (9, 10, 11) THEN
            CAST(CAST(EXTRACT(YEAR FROM {date}) AS VARCHAR) || '-09-01' AS DATE)
        ELSE NULL
    END
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _agg_detect_date_col(columns: list[str], system: str | None = None) -> str | None:
    base = (system or "").split("-")[0].upper()
    cfg  = _agg_config().get("date_candidates", {})
    candidates = cfg.get(base, []) + cfg.get("common", [])
    if not candidates:
        # fallback hardcoded
        candidates = ["death_date", "birth_date", "admission_date",
                      "notification_date", "date", "DTOBITO", "DT_NOTIFIC"]
    return next((c for c in candidates if c in columns), None)


def _agg_detect_geo_col(columns: list[str], system: str | None = None) -> str | None:
    base = (system or "").split("-")[0].upper()
    cfg  = _agg_config().get("geo_candidates", {})
    candidates = cfg.get(base, []) + cfg.get("common", [])
    if not candidates:
        candidates = ["residence_municipality_code", "municipality_code", "CODMUNRES"]
    return next((c for c in candidates if c in columns), None)


def _agg_smart_name(system: str | None, fun: str, lang: str) -> str:
    if fun != "count":
        return f"{fun}_value"
    base   = (system or "").split("-")[0].upper()
    names  = _agg_labels_config().get("smart_names", {})
    lang_names = names.get(lang, names.get("en", {}))
    return lang_names.get(base, "n")


def _agg_time_label(time_unit: str, lang: str) -> str:
    labels = _agg_labels_config().get("time_labels", {})
    return labels.get(time_unit, {}).get(lang, time_unit)


def _agg_time_expr(date_expr: str, time_unit: str) -> str:
    if time_unit == "season":
        return AGG_SEASON_SQL.format(date=date_expr)
    template = AGG_TIME_EXPRS.get(time_unit)
    if template is None:
        raise ValueError(
            f"time_unit {time_unit!r} not valid. "
            f"Options: {list(AGG_TIME_EXPRS.keys())}"
        )
    return template.format(date=date_expr)


def _agg_fun_expr(fun: str, col: str | None = None) -> str:
    template = AGG_FUN_EXPRS.get(fun)
    if template is None:
        raise ValueError(
            f"fun {fun!r} not valid. "
            f"Options: {list(AGG_FUN_EXPRS.keys())}"
        )
    if fun == "count":
        return template
    if col is None:
        raise ValueError(f"value_col is required when fun='{fun}'")
    return template.format(col=f'"{col}"')


def _agg_complete_dates(
    df: pd.DataFrame,
    time_unit: str,
    group_by: list[str] | None,
    count_col: str,
    fill_value: int | float = 0,
) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df

    df["date"] = df["date"].astype(str)
    min_d = df["date"].min()
    max_d = df["date"].max()

    if time_unit in ("season", "quarter", "week"):
        dates = sorted(df["date"].unique().tolist())
    elif time_unit == "year":
        years = range(int(min_d[:4]), int(max_d[:4]) + 1)
        dates = [str(y) + "-01-01" for y in years]
    elif time_unit == "month":
        dates = (
            pd.date_range(
                start=pd.to_datetime(min_d),
                end=pd.to_datetime(max_d),
                freq="MS",
            )
            .strftime("%Y-%m-%d")
            .tolist()
        )
    elif time_unit == "day":
        dates = pd.date_range(min_d, max_d, freq="D").strftime("%Y-%m-%d").tolist()
    elif time_unit in ("5 days", "14 days"):
        n = int(time_unit.split()[0])
        dates = (
            pd.date_range(pd.to_datetime(min_d), pd.to_datetime(max_d), freq=f"{n}D")
            .strftime("%Y-%m-%d")
            .tolist()
        )
    else:
        dates = sorted(df["date"].unique().tolist())

    date_df = pd.DataFrame({"date": dates})

    if not group_by:
        result = date_df.merge(df, on="date", how="left")
    else:
        groups  = df[group_by].drop_duplicates()
        complete = date_df.merge(groups, how="cross")
        result   = complete.merge(df, on=["date"] + group_by, how="left")

    if count_col in result.columns:
        result[count_col] = result[count_col].fillna(fill_value)

    return result.sort_values(["date"] + (group_by or [])).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_aggregate(
    rel: duckdb.DuckDBPyRelation,
    *,
    time_unit: str = "month",
    fun: str | dict = "count",
    group_by: list[str] | None = None,
    value_col: str | None = None,
    complete_dates: bool = False,
    date_col: str | None = None,
    system: str | None = None,
    lang: str = "en",
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation | pd.DataFrame:
    """Aggregate SUS health microdata into a time series.

    Mirrors ``climasus4r::sus_data_aggregate()``. Stays lazy (DuckDB)
    unless ``complete_dates=True``, which materialises to a DataFrame.

    Args:
        rel: Lazy DuckDB relation (after ``sus_data_standardize()``).
        time_unit: Temporal aggregation unit. One of:
            ``"year"``, ``"quarter"``, ``"month"`` (default), ``"week"``,
            ``"day"``, ``"5 days"``, ``"14 days"``, ``"season"``.
        fun: Aggregation function — ``"count"`` (default), ``"sum"``,
            ``"mean"``, ``"median"``, ``"min"``, ``"max"``, ``"sd"``,
            ``"q25"``, ``"q75"``, ``"q95"``, ``"q99"``.
            Or a dict for multiple functions simultaneously:
            ``{"mean_age": "mean", "max_age": "max"}``.
        group_by: Additional grouping columns, e.g. ``["sex", "age_group"]``.
        value_col: Column to aggregate. Required when ``fun != "count"``.
        complete_dates: Fill periods with no events with ``0`` (count)
            or ``NaN`` (other functions). Materialises to DataFrame.
        date_col: Date column name. Auto-detected by system if ``None``.
        system: SUS system identifier for smart column naming and geo
            detection, e.g. ``"SIM-DO"``, ``"SINAN-DENGUE"``.
        lang: Output language for column names — ``"en"`` (default),
            ``"pt"``, ``"es"``.
        verbose: Print progress messages.

    Returns:
        Lazy ``DuckDBPyRelation`` — or ``pd.DataFrame`` if
        ``complete_dates=True``. Registers ``stage="aggregate"`` in
        sus_meta when returning a relation.

    Example:
        >>> agg = cs.sus_data_aggregate(stand, time_unit="month")
        >>> agg = cs.sus_data_aggregate(stand, time_unit="month",
        ...     group_by=["sex", "age_group"], system="SIM-DO", lang="en")
        >>> agg = cs.sus_data_aggregate(stand, time_unit="month",
        ...     fun="mean", value_col="age_years")
        >>> df  = cs.sus_data_aggregate(stand, time_unit="month",
        ...     complete_dates=True)
    """
    # validations
    valid_funs = set(AGG_FUN_EXPRS.keys())
    if isinstance(fun, str) and fun not in valid_funs:
        raise ValueError(f"fun {fun!r} not valid. Options: {sorted(valid_funs)}")
    if isinstance(fun, dict) and not all(v in valid_funs for v in fun.values()):
        raise ValueError(f"All functions must be one of: {sorted(valid_funs)}")
    if isinstance(fun, str) and fun != "count" and value_col is None:
        raise ValueError(f"value_col is required when fun='{fun}'")
    if isinstance(fun, dict) and value_col is None:
        raise ValueError("value_col is required when fun is a dict")
    if time_unit not in AGG_TIME_EXPRS:
        raise ValueError(
            f"time_unit {time_unit!r} not valid. "
            f"Options: {list(AGG_TIME_EXPRS.keys())}"
        )

    # capture original rel for sus_meta
    _original_rel = rel

    conn    = get_connection()
    columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # 1. Date column
    # ------------------------------------------------------------------
    if date_col is None:
        date_col = _agg_detect_date_col(columns, system)
    if date_col is None:
        raise ValueError(
            "Date column not found. "
            "Specify date_col= or run sus_data_standardize() first."
        )
    if verbose:
        print(f"Date column: {date_col}")

    date_expr = f'TRY_CAST("{date_col}" AS DATE)'

    # ------------------------------------------------------------------
    # 2. Geographic column
    # ------------------------------------------------------------------
    geo_col = _agg_detect_geo_col(columns, system)
    if verbose and geo_col:
        print(f"Geographic column: {geo_col}")

    # ------------------------------------------------------------------
    # 3. Build GROUP BY
    # ------------------------------------------------------------------
    group_parts:  list[str] = []
    select_parts: list[str] = []

    time_expr_sql = _agg_time_expr(date_expr, time_unit)
    select_parts.append(f"({time_expr_sql}) AS date")
    group_parts.append(f"({time_expr_sql})")

    if geo_col:
        select_parts.append(f'"{geo_col}"')
        group_parts.append(f'"{geo_col}"')

    if group_by:
        missing = [c for c in group_by if c not in columns]
        if missing:
            raise ValueError(f"Columns not found in relation: {missing}")
        for c in group_by:
            select_parts.append(f'"{c}"')
            group_parts.append(f'"{c}"')

    # ------------------------------------------------------------------
    # 4. Aggregation
    # ------------------------------------------------------------------
    agg_col = _agg_smart_name(
        system, fun if isinstance(fun, str) else "count", lang
    )

    if isinstance(fun, str):
        agg_expr = _agg_fun_expr(fun, value_col)
        select_parts.append(f"{agg_expr} AS {agg_col}")
    else:
        for alias, fn in fun.items():
            select_parts.append(f"{_agg_fun_expr(fn, value_col)} AS {alias}")

    # ------------------------------------------------------------------
    # 5. Execute SQL
    # ------------------------------------------------------------------
    group_by_sql = ", ".join(group_parts)
    select_sql   = ", ".join(select_parts)

    sql = f"""
        SELECT {select_sql}
        FROM rel
        WHERE "{date_col}" IS NOT NULL
        GROUP BY {group_by_sql}
        ORDER BY {group_by_sql}
    """

    if verbose:
        label = _agg_time_label(time_unit, lang)
        print(f"Aggregating: {label} | fun={fun} | group_by={group_by}")

    rel_agg = conn.sql(sql)

    if verbose:
        n = rel_agg.count("*").fetchone()[0]
        print(f"✓ {n:,} periods generated")

    # ------------------------------------------------------------------
    # 6. complete_dates — materialise to DataFrame
    # ------------------------------------------------------------------
    if complete_dates:
        df_agg = rel_agg.df()
        df_agg = _agg_complete_dates(
            df_agg,
            time_unit,
            group_by,
            agg_col,
            fill_value=0 if (isinstance(fun, str) and fun == "count") else float("nan"),
        )
        if verbose:
            print(f"✓ complete_dates: {len(df_agg):,} periods after fill")
        return df_agg

    # ------------------------------------------------------------------
    # 7. sus_meta
    # ------------------------------------------------------------------
    fun_desc = fun if isinstance(fun, str) else str(list(fun.keys()))
    history_msg = (
        f"Aggregated by {time_unit}; fun={fun_desc}; "
        f"group_by={group_by}; lang={lang}"
    )
    rel_agg = rel_agg.set_alias("aggregate")
    rel_agg = set_stage(rel_agg, "aggregate", _inherit_from=_original_rel)
    rel_agg = add_history(rel_agg, history_msg)

    return rel_agg
