"""Climate data enrichment — join health + INMET observations (lazy).

Mirrors R: climate.R

Lazy contract: takes a ``DuckDBPyRelation``, JOINs against INMET parquet
files cached in ``climasus-data``, and returns a ``DuckDBPyRelation``.
Health data is never materialised internally — the relation stays lazy
until the user calls ``.df()`` or ``cs.materialize(...)``.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import duckdb

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
    rel: duckdb.DuckDBPyRelation,
    *,
    variables: Sequence[str] | None = None,
    lags: Sequence[int] | None = None,
    idw: bool = True,
    years: Sequence[int] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Join health data with cached INMET climate observations (lazy).

    Reads INMET parquet files from ``climasus-data`` automatically and
    JOINs them via DuckDB SQL. The relation remains lazy until the user
    materialises with ``.df()`` or ``cs.materialize(...)``.

    Mirrors ``climasus4r::sus_climate`` (legacy reference).

    Args:
        rel: Lazy DuckDB relation with health data containing a
            municipality column and a daily date column.
        variables: Climate variable columns to include, e.g.
            ``["temp_mean", "precipitation"]``. Defaults to both.
        lags: Lag offsets in days. Reserved for future windowed joins.
        idw: Whether to apply IDW station-weighting (default ``True``).
        years: Observation years to load. When ``None`` (default), the
            years are auto-detected from the date column.

    Returns:
        ``DuckDBPyRelation`` with climate columns joined to the health
        data. Stays lazy.

    Raises:
        TypeError: If *rel* is not a DuckDB relation.
        ValueError: If the date column has monthly granularity
            (``YYYY-MM``); ``sus_climate`` requires daily dates.

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_climate(rel, variables=["temp_mean"], years=[2023])
        >>> out.df()
    """
    rel = _unwrap_sus_relation(rel, "sus_climate")

    geo_col = (
        detect_geo_column(list(rel.columns), level="municipality") or "municipality_code"
    )
    date_col = detect_date_column(list(rel.columns)) or "date"
    vars_list: list[str] = list(variables) if variables is not None else list(_DEFAULT_VARIABLES)
    _ = lags  # reserved for future windowed joins; preserved in signature for parity

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
