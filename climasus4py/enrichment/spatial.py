"""Spatial enrichment — join health data with Brazilian municipality geometries.

Mirrors R: spatial.R

Two calling modes:

**Lazy** (preferred) — pass a ``DuckDBPyRelation`` with no ``shapefile`` arg.
Reads a spatial parquet from climasus-data automatically via DuckDB SQL.

**Legacy** — pass a ``shapefile`` GeoDataFrame. Materialises immediately and
returns a ``gpd.GeoDataFrame``. Kept for backward compatibility.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..core._guards import _unwrap_sus_relation
from ..core._sql import quote_ident, sql_string
from ..core._stage import set_stage
from ..core.engine import get_connection
from ..utils.data import data_path, detect_geo_column

_REQUIRED_SPATIAL_COLS = {"geometry_wkt", "name"}

# shapefile column → geo_level → expected join column in shapefile
_SHAPEFILE_JOIN_COLS: dict[str, str] = {
    "municipality": "code_muni",
    "state": "abbrev_state",
}

# health data geo candidates per level
_HEALTH_GEO_CANDIDATES: dict[str, list[str]] = {
    "municipality": ["municipality_code", "CODMUNRES", "ID_MUNICIP"],
    "state": ["state", "SG_UF", "UF", "SG_UF_NOT"],
}


def _sql_path(p: Path) -> str:
    """Return a safely-quoted SQL string for a filesystem path."""
    return sql_string(str(p).replace("\\", "/"))


def sus_spatial_join(
    rel: Any,
    *,
    shapefile: Any = None,
    spatial_path: str | Path | None = None,
    geo_level: str = "municipality",
    join_type: str = "left",
) -> Any:
    """Join health data with Brazilian municipality or state spatial data.

    Two calling modes:

    **Lazy** (preferred) — pass a ``DuckDBPyRelation`` with no ``shapefile``
    arg. Reads a spatial parquet from climasus-data (or *spatial_path*)
    via SQL. No materialisation until ``.df()`` is called.

    **Legacy** — pass a ``shapefile`` GeoDataFrame. Materialises immediately
    and returns a ``gpd.GeoDataFrame``.

    Mirrors climasus4r::sus_spatial_join (legacy reference).

    Args:
        rel: Lazy DuckDB relation (lazy mode) or DataFrame (legacy mode).
        shapefile: Legacy — GeoDataFrame with geometry column and a
            recognised join column (``code_muni`` / ``abbrev_state``).
        spatial_path: Lazy mode — custom path to a spatial parquet that
            must contain ``code_muni``, ``name``, and ``geometry_wkt``
            columns. Uses the bundled municipalities parquet when ``None``.
        geo_level: Geographic level — ``"municipality"`` (default) or
            ``"state"``. Determines join column auto-detection.
        join_type: Legacy mode only — ``"left"`` (default) or ``"inner"``.

    Returns:
        **Lazy mode**: ``DuckDBPyRelation`` with ``spatial_name`` and
        ``geometry_wkt`` columns added.
        **Legacy mode**: ``gpd.GeoDataFrame`` with geometry columns merged.

    Raises:
        TypeError: If *rel* is not a DuckDB relation (lazy mode).
        ValueError: If health data has no recognised geo column, or if the
            shapefile is missing a supported join column.

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_spatial_join(rel)
        >>> out.df()["geometry_wkt"].iloc[0]
        'POINT (-46.63 -23.55)'
    """
    # ---------------------------------------------------------------------------
    # Legacy eager path: shapefile GeoDataFrame provided explicitly
    # ---------------------------------------------------------------------------
    if shapefile is not None:
        import geopandas as gpd

        from ..core.engine import collect

        if isinstance(rel, duckdb.DuckDBPyRelation):
            health_df = collect(rel)
        elif isinstance(rel, pd.DataFrame):
            health_df = rel.copy()
        else:
            try:
                inner = object.__getattribute__(rel, "_rel")
                health_df = collect(inner)
            except AttributeError as err:
                raise TypeError(
                    f"Expected DuckDBPyRelation or DataFrame, got {type(rel).__name__}"
                ) from err

        # Detect join column in health data
        health_cols = list(health_df.columns)
        candidates = _HEALTH_GEO_CANDIDATES.get(geo_level, [])
        health_join_col = next((c for c in candidates if c in health_cols), None)
        if health_join_col is None:
            raise ValueError(
                f"No {geo_level} geo column found in health data. "
                f"Expected one of: {candidates}. Got: {health_cols}"
            )

        # Detect join column in shapefile
        shp_cols = list(shapefile.columns)
        expected_shp_col = _SHAPEFILE_JOIN_COLS.get(geo_level, "code_muni")
        if expected_shp_col not in shp_cols:
            raise ValueError(
                f"Shapefile missing join column '{expected_shp_col}'. "
                f"Available columns: {shp_cols}"
            )

        how = join_type if join_type in ("left", "inner", "right", "outer") else "left"
        result = health_df.merge(
            shapefile,
            left_on=health_join_col,
            right_on=expected_shp_col,
            how=how,
        )

        if not isinstance(result, gpd.GeoDataFrame):
            result = gpd.GeoDataFrame(result, geometry="geometry", crs=shapefile.crs)

        return result

    # ---------------------------------------------------------------------------
    # Lazy path: read from climasus-data spatial parquets
    # ---------------------------------------------------------------------------
    rel = _unwrap_sus_relation(rel, "sus_spatial_join")

    if spatial_path is None:
        resolved_path: Path = data_path("assets/spatial/municipalities.parquet")
    else:
        resolved_path = Path(spatial_path)

    conn = get_connection()

    # Validate spatial parquet schema before building the join
    spatial_rel = conn.read_parquet(str(resolved_path).replace("\\", "/"))
    spatial_cols = set(spatial_rel.columns)
    missing = _REQUIRED_SPATIAL_COLS - spatial_cols
    if missing:
        raise ValueError(
            f"Spatial parquet missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(spatial_cols)}"
        )

    geo_col = (
        detect_geo_column(list(rel.columns), level="municipality") or "municipality_code"
    )

    p_sql = _sql_path(resolved_path)
    h_geo = quote_ident(geo_col)
    sql = (
        f"SELECT h.*, s.name AS spatial_name, s.geometry_wkt "
        f"FROM _spatial_health h "
        f"LEFT JOIN read_parquet({p_sql}) s "
        f"  ON LEFT(CAST(h.{h_geo} AS VARCHAR), 6) "
        f"   = LEFT(CAST(s.code_muni AS VARCHAR), 6)"
    )

    result = rel.query("_spatial_health", sql)
    return set_stage(result, "enrichment")

