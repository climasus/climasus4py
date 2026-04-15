"""Spatial enrichment — join with Brazilian shapefiles.

Mirrors R: spatial.R
"""

from __future__ import annotations

import duckdb
import pandas as pd

from climasus.core.engine import collect


def sus_spatial(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    *,
    shapefile: "gpd.GeoDataFrame | None" = None,
    geo_level: str = "municipality",
    join_type: str = "left",
) -> "gpd.GeoDataFrame":
    """Join health data with Brazilian shapefiles.

    Materializes the result (returns GeoDataFrame).
    Requires: pip install climasus4py[spatial]

    Parameters
    ----------
    data : Health data
    shapefile : GeoDataFrame with geometries. Auto-loaded via geobr if None.
    geo_level : "municipality", "state", "region"
    join_type : "left" or "inner"
    """
    try:
        import geopandas as gpd
    except ImportError:
        raise ImportError("Install spatial extras: pip install climasus4py[spatial]")

    # Materialize
    df = collect(data) if isinstance(data, duckdb.DuckDBPyRelation) else data

    # Auto-load shapefile via geobr
    if shapefile is None:
        try:
            import geobr
        except ImportError:
            raise ImportError(
                "Provide a shapefile or install geobr: pip install climasus4py[spatial]"
            )

        if geo_level == "state":
            shapefile = geobr.read_state()
        elif geo_level == "region":
            shapefile = geobr.read_region()
        else:
            shapefile = geobr.read_municipality()

    # Determine join column
    from climasus.utils.data import detect_geo_column
    join_col = detect_geo_column(list(df.columns), level=geo_level)

    if not join_col:
        raise ValueError(f"No {geo_level}-level column found in data.")

    # Find matching column in shapefile
    shape_candidates = {
        "municipality": ["code_muni", "CD_MUN"],
        "state": ["abbrev_state", "SIGLA_UF"],
        "region": ["name_region", "NM_REGIAO"],
    }
    shape_col = None
    for c in shape_candidates.get(geo_level, []):
        if c in shapefile.columns:
            shape_col = c
            break

    if not shape_col:
        raise ValueError(f"Cannot find join column in shapefile for level={geo_level}")

    # Cast join columns to string for compatibility
    df[join_col] = df[join_col].astype(str)
    shapefile[shape_col] = shapefile[shape_col].astype(str)

    how = "left" if join_type == "left" else "inner"
    result = shapefile.merge(df, left_on=shape_col, right_on=join_col, how=how)

    return result
