"""Spatial enrichment — join health data with Brazilian municipality geometries (lazy).

Mirrors R: spatial.R

Lazy contract: takes a ``DuckDBPyRelation``, JOINs against a spatial
parquet from ``climasus-data`` (or a custom path), and returns a
``DuckDBPyRelation``. Health data is never materialised internally —
the relation stays lazy until the user calls ``.df()`` or
``cs.materialize(...)``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from ..core._guards import _unwrap_sus_relation
from ..core._sql import quote_ident, sql_string
from ..core._stage import set_stage
from ..core.engine import get_connection
from ..utils.data import data_path, detect_geo_column

_REQUIRED_SPATIAL_COLS = {"geometry_wkt", "name"}


def _sql_path(p: Path) -> str:
    """Return a safely-quoted SQL string for a filesystem path."""
    return sql_string(str(p).replace("\\", "/"))


def sus_spatial_join(
    rel: duckdb.DuckDBPyRelation,
    *,
    spatial_path: str | Path | None = None,
    geo_level: str = "municipality",
) -> duckdb.DuckDBPyRelation:
    """Join health data with Brazilian municipality or state spatial data (lazy).

    Reads a spatial parquet from ``climasus-data`` (or *spatial_path*)
    via DuckDB SQL. The relation remains lazy until the user materialises
    with ``.df()`` or ``cs.materialize(...)``.

    Mirrors ``climasus4r::sus_spatial_join`` (legacy reference).

    Args:
        rel: Lazy DuckDB relation with health data containing a
            recognised municipality column.
        spatial_path: Custom path to a spatial parquet that must contain
            ``code_muni``, ``name``, and ``geometry_wkt`` columns. Uses
            the bundled municipalities parquet when ``None``.
        geo_level: Geographic level — ``"municipality"`` (default) or
            ``"state"``.

    Returns:
        ``DuckDBPyRelation`` with ``spatial_name`` and ``geometry_wkt``
        columns joined to the health data.

    Raises:
        TypeError: If *rel* is not a DuckDB relation.
        ValueError: If health data has no recognised geo column, or if
            the spatial parquet is missing required columns.

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_spatial_join(rel)
        >>> out.df()["geometry_wkt"].iloc[0]
        'POINT (-46.63 -23.55)'
    """
    _ = geo_level  # reserved for future state-level joins; preserved for parity
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
