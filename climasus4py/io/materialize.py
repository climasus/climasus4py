"""Explicit materialisation of lazy DuckDB relations.

Mirrors R: collect() / as.data.frame() — lets callers choose the target
in-memory format after all lazy pipeline steps have been applied.
"""

from __future__ import annotations

import warnings
from typing import Any

import duckdb

from ..core._sql import fetchone_scalar

_LARGE_ROW_THRESHOLD = 100_000


def _warn_size(
    rel: duckdb.DuckDBPyRelation,
    *,
    how: str,
    quiet: bool,
) -> None:
    """Emit a UserWarning if *rel* has >= 100,000 rows and *quiet* is False.

    Args:
        rel: Relation to count.
        how: Target format string (included in the warning message).
        quiet: If ``True``, suppresses the warning.
    """
    if quiet:
        return
    count = fetchone_scalar(rel.count("*"), fallback=0)
    if count >= _LARGE_ROW_THRESHOLD:
        formatted = f"{count:,}"
        warnings.warn(
            f"materialize(): collecting {formatted} rows as {how!r}. "
            "Consider using how='pyarrow' for large datasets.",
            UserWarning,
            stacklevel=3,
        )


def materialize(
    rel: duckdb.DuckDBPyRelation,
    *,
    how: str = "pandas",
    quiet: bool = False,
) -> Any:
    """Materialise a lazy DuckDB relation into an in-memory format.

    Args:
        rel: Lazy DuckDB relation to collect.
        how: Target format — one of ``"pandas"`` (default), ``"pyarrow"``,
            ``"geopandas"``, or ``"polars"``.
        quiet: Suppress the large-dataset warning.

    Returns:
        The collected data in the requested format:

        - ``"pandas"`` → ``pandas.DataFrame``
        - ``"pyarrow"`` → ``pyarrow.Table``
        - ``"geopandas"`` → ``geopandas.GeoDataFrame``
          (requires a ``geometry_wkt`` column and ``geopandas`` installed)
        - ``"polars"`` → ``polars.DataFrame``
          (requires ``polars`` installed)

    Raises:
        ValueError: If *how* is not one of the supported options, or if
            ``how='geopandas'`` is requested but the relation has no
            ``geometry_wkt`` column.
        ImportError: If the required library for *how* is not installed.

    Example:
        >>> import climasus4py as cs
        >>> df = cs.materialize(rel)
        >>> table = cs.materialize(rel, how="pyarrow")
        >>> gdf = cs.materialize(rel, how="geopandas")
    """
    _SUPPORTED = ("pandas", "pyarrow", "geopandas", "polars")
    if how not in _SUPPORTED:
        raise ValueError(
            f"how must be one of {_SUPPORTED}, got {how!r}."
        )

    if how == "pandas":
        _warn_size(rel, how="pandas", quiet=quiet)
        return rel.df()

    if how == "pyarrow":
        _warn_size(rel, how="pyarrow", quiet=quiet)
        result = rel.arrow()
        if hasattr(result, "read_all"):
            return result.read_all()
        return result

    if how == "geopandas":
        _warn_size(rel, how="geopandas", quiet=quiet)
        return _materialize_geopandas(rel)

    if how == "polars":
        import polars as pl  # noqa: F401 (let ImportError propagate naturally)

        _warn_size(rel, how="polars", quiet=quiet)
        arrow = rel.arrow()
        if hasattr(arrow, "read_all"):
            arrow = arrow.read_all()
        return pl.from_arrow(arrow)


def _materialize_geopandas(rel: duckdb.DuckDBPyRelation):
    """Shared helper that converts a relation with geometry_wkt to GeoDataFrame."""
    import geopandas as gp
    import shapely.wkt

    if "geometry_wkt" not in rel.columns:
        raise ValueError(
            "how='geopandas' requires a 'geometry_wkt' column containing "
            "WKT geometry strings. Available columns: "
            f"{rel.columns}"
        )
    df = rel.df()
    df["geometry"] = df["geometry_wkt"].apply(shapely.wkt.loads)
    return gp.GeoDataFrame(df.drop(columns=["geometry_wkt"]), geometry="geometry")

