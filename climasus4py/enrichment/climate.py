"""Climate data enrichment — join health + INMET observations.

Mirrors R: climate.R

Two calling modes:

**Lazy** (preferred) — pass a ``DuckDBPyRelation`` with no ``climate`` arg.
Reads INMET parquet files from climasus-data automatically via DuckDB SQL.

**Legacy** — pass a ``climate`` DataFrame. Materialises immediately and
returns a ``pandas.DataFrame``. Kept for backward compatibility.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..core._guards import _unwrap_sus_relation
from ..core._sql import quote_ident, sql_string
from ..core._stage import set_stage
from ..utils.data import data_path, detect_date_column, detect_geo_column

_DEFAULT_VARIABLES: tuple[str, ...] = ("temp_mean", "precipitation")


def _sql_path(p: Path) -> str:
    """Return a safely-quoted SQL string for a filesystem path."""
    return sql_string(str(p).replace("\\", "/"))


def _obs_union_sql(obs_paths: list[Path]) -> str:
    """Build a UNION ALL sub-query reading all observation parquet files."""
    parts = [f"SELECT * FROM read_parquet({_sql_path(p)})" for p in obs_paths]
    return "(" + " UNION ALL ".join(parts) + ")"


def _join_idw(
    geo_col: str,
    date_col: str,
    idw_path: Path,
    obs_paths: list[Path],
    variables: list[str],
) -> str:
    """Return SQL for IDW-weighted climate join against _climate_health."""
    idw_sql = _sql_path(idw_path)
    obs_sql = _obs_union_sql(obs_paths)
    h_geo = quote_ident(geo_col)
    h_date = quote_ident(date_col)
    vars_agg = ", ".join(
        f"SUM(w.weight * o.{quote_ident(v)}) / SUM(w.weight) AS {quote_ident(v)}"
        for v in variables
    )
    return (
        f"SELECT h.*, {vars_agg} "
        f"FROM _climate_health h "
        f"LEFT JOIN read_parquet({idw_sql}) w "
        f"  ON LEFT(CAST(h.{h_geo} AS VARCHAR), 6) "
        f"   = LEFT(CAST(w.municipality_code AS VARCHAR), 6) "
        f"LEFT JOIN {obs_sql} o "
        f"  ON w.station_id = o.station_id "
        f"  AND CAST(h.{h_date} AS DATE) = CAST(o.date AS DATE) "
        f"GROUP BY ALL"
    )


def _join_direct(
    geo_col: str,
    date_col: str,
    obs_paths: list[Path],
    variables: list[str],
) -> str:
    """Return SQL for direct (no-IDW) climate join against _climate_health."""
    obs_sql = _obs_union_sql(obs_paths)
    h_geo = quote_ident(geo_col)
    h_date = quote_ident(date_col)
    vars_sel = ", ".join(f"o.{quote_ident(v)}" for v in variables)
    return (
        f"SELECT h.*, {vars_sel} "
        f"FROM _climate_health h "
        f"LEFT JOIN {obs_sql} o "
        f"  ON LEFT(CAST(h.{h_geo} AS VARCHAR), 6) "
        f"   = LEFT(CAST(o.municipality_code AS VARCHAR), 6) "
        f"  AND CAST(h.{h_date} AS DATE) = CAST(o.date AS DATE)"
    )


def sus_climate(
    rel: Any,
    climate: pd.DataFrame | None = None,
    *,
    time_window: int = 0,
    variables: Sequence[str] | None = None,
    lags: Sequence[int] | None = None,
    idw: bool = True,
    years: Sequence[int] | None = None,
) -> Any:
    """Join health data with climate observations.

    Two calling modes:

    **Lazy** (preferred) — pass a ``DuckDBPyRelation`` with no ``climate``
    arg. Reads INMET parquet files from climasus-data automatically via
    DuckDB SQL. No materialisation until ``.df()`` is called.

    **Legacy** — pass a ``climate`` DataFrame. Materialises immediately
    and returns a ``pandas.DataFrame``.

    Mirrors climasus4r::sus_climate (legacy reference).

    Args:
        rel: Lazy DuckDB relation (lazy mode) or DataFrame (legacy mode).
        climate: Legacy — ``DataFrame`` with ``municipality_code``, ``date``,
            and climate variable columns.
        time_window: Legacy — reserved, not used.
        variables: Lazy mode — climate variable columns to include, e.g.
            ``["temp_mean", "precipitation"]``. Defaults to both.
        lags: Lag offsets in days (both modes).
        idw: Whether to apply IDW station-weighting (lazy mode only).
        years: Observation years to load (lazy mode only).

    Returns:
        **Lazy mode**: ``DuckDBPyRelation`` with climate columns added.
        **Legacy mode**: ``pandas.DataFrame`` with climate columns merged.

    Raises:
        TypeError: If *rel* is not a DuckDB relation (lazy mode).
        ValueError: If date column has monthly granularity (YYYY-MM).

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_climate(rel, variables=["temp_mean"], years=[2023])
        >>> out.df()
    """
    # ---------------------------------------------------------------------------
    # Legacy eager path: climate DataFrame provided explicitly
    # ---------------------------------------------------------------------------
    if climate is not None:
        from ..core.engine import collect

        if isinstance(rel, duckdb.DuckDBPyRelation):
            health_df = collect(rel)
        elif isinstance(rel, pd.DataFrame):
            health_df = rel
        else:
            try:
                inner = object.__getattribute__(rel, "_rel")
                health_df = collect(inner)
            except AttributeError as err:
                raise TypeError(
                    f"Expected DuckDBPyRelation or DataFrame, got {type(rel).__name__}"
                ) from err

        columns = list(health_df.columns)
        geo_col = detect_geo_column(columns, level="municipality")
        date_col = detect_date_column(columns)

        if not geo_col or not date_col:
            raise ValueError(
                "Health data must have a municipality_code and date column for climate join."
            )

        health_df = health_df.copy()
        health_df[date_col] = pd.to_datetime(health_df[date_col], errors="coerce")
        climate = climate.copy()
        climate["date"] = pd.to_datetime(climate["date"], errors="coerce")

        result = health_df.merge(
            climate,
            left_on=[geo_col, date_col],
            right_on=["municipality_code", "date"],
            how="left",
            suffixes=("", "_clim"),
        )

        if lags:
            climate_cols = [
                c for c in climate.columns if c not in ("municipality_code", "date")
            ]
            for lag_days in lags:
                lagged = climate.copy()
                lagged["date"] = lagged["date"] + pd.Timedelta(days=lag_days)
                lag_suffix = f"_lag{lag_days}d"
                lagged = lagged.rename(
                    columns={c: f"{c}{lag_suffix}" for c in climate_cols}
                )
                result = result.merge(
                    lagged,
                    left_on=[geo_col, date_col],
                    right_on=["municipality_code", "date"],
                    how="left",
                    suffixes=("", lag_suffix),
                )

        return result

    # ---------------------------------------------------------------------------
    # Lazy path: read from climasus-data INMET parquets
    # ---------------------------------------------------------------------------
    rel = _unwrap_sus_relation(rel, "sus_climate")

    geo_col = (
        detect_geo_column(list(rel.columns), level="municipality") or "municipality_code"
    )
    date_col = detect_date_column(list(rel.columns)) or "date"
    vars_list: list[str] = list(variables) if variables is not None else list(_DEFAULT_VARIABLES)

    # Guard: reject monthly date granularity
    check_sql = (
        f"SELECT COUNT(*) FROM _climate_health "
        f"WHERE length(CAST({quote_ident(date_col)} AS VARCHAR)) = 7"
    )
    monthly_count = rel.query("_climate_health", check_sql).fetchone()[0]
    if monthly_count > 0:
        raise ValueError(
            f"Date column '{date_col}' has monthly granularity (YYYY-MM). "
            "sus_climate requires daily dates (YYYY-MM-DD). "
            "Disaggregate to daily granularity before calling sus_climate."
        )

    # Auto-detect years from date column when not supplied
    if years is None:
        year_sql = (
            f"SELECT DISTINCT YEAR(TRY_CAST({quote_ident(date_col)} AS DATE)) AS y "
            f"FROM _climate_health WHERE y IS NOT NULL ORDER BY y"
        )
        rows = rel.query("_climate_health", year_sql).fetchall()
        years = [row[0] for row in rows] if rows else []

    climate_dir: Path = data_path("assets/climate")
    obs_paths: list[Path] = [
        climate_dir / f"inmet_observations_{y}.parquet" for y in years
    ]
    idw_path: Path = climate_dir / "idw_weights_municipality.parquet"

    if idw:
        sql = _join_idw(geo_col, date_col, idw_path, obs_paths, vars_list)
    else:
        sql = _join_direct(geo_col, date_col, obs_paths, vars_list)

    result = rel.query("_climate_health", sql)
    return set_stage(result, "enrichment")
