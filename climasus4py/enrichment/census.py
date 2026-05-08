"""Census data enrichment — join health + IBGE census data lazily.

Mirrors R: census.R
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ..core._guards import _unwrap_sus_relation
from ..core._sql import quote_ident, sql_string
from ..core._stage import set_stage
from ..utils.data import data_path, detect_geo_column


def _sql_path(p: Path) -> str:
    """Return a safely-quoted SQL string for a filesystem path."""
    return sql_string(str(p).replace("\\", "/"))


def sus_census(
    rel: Any,
    census: pd.DataFrame | None = None,
    *,
    variables: Sequence[str] | None = None,
    year: int | None = None,
) -> Any:
    """Join health data with IBGE census indicators.

    Two calling modes:

    **Lazy** (preferred) — pass a ``DuckDBPyRelation``; no ``census`` arg.
    Reads census parquet files from climasus-data automatically.

    **Legacy** — pass a ``DataFrame`` or ``DuckDBPyRelation`` plus a
    ``census`` DataFrame. Materialises immediately and returns a
    ``pandas.DataFrame``. Kept for backward compatibility.

    Mirrors climasus4r::sus_census (legacy reference).

    Args:
        rel: Lazy DuckDB relation (lazy mode) or DataFrame (legacy mode).
        census: Legacy — ``DataFrame`` with ``municipality_code`` plus
            census variable columns.
        variables: Census variable columns to include. All columns when
            ``None``.
        year: Census year. When ``None``, all available census parquets
            are unioned. Legacy only: emits a ``UserWarning`` for
            ``year=2010`` (synthetic data).

    Returns:
        **Lazy mode**: ``DuckDBPyRelation`` with census columns added.
        **Legacy mode**: ``pandas.DataFrame`` with census columns merged.

    Raises:
        TypeError: If *rel* is not a DuckDB relation (lazy mode) or
            DataFrame (legacy mode).

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_census(rel, variables=["population"])
        >>> out.df()
    """
    # ---------------------------------------------------------------------------
    # Legacy eager path: census DataFrame provided explicitly
    # ---------------------------------------------------------------------------
    if census is not None:
        if year == 2010:
            warnings.warn(
                "census_2010.parquet contém dados SINTÉTICOS (seed=2010). "
                "Não use em análises ou publicações. Substitua por dados reais "
                "do IBGE antes de qualquer uso científico.",
                UserWarning,
                stacklevel=2,
            )
        # Materialise health data if lazy
        if isinstance(rel, duckdb.DuckDBPyRelation):
            from ..core.engine import collect
            df = collect(rel)
        else:
            df = rel

        join_col = detect_geo_column(list(df.columns), level="municipality")
        if not join_col:
            raise ValueError("No municipality column found in health data.")

        df = df.copy()
        df[join_col] = df[join_col].astype(str)
        census = census.copy()
        if "municipality_code" not in census.columns:
            raise ValueError("Census data must have a 'municipality_code' column.")
        census["municipality_code"] = census["municipality_code"].astype(str)

        result = df.merge(
            census,
            left_on=join_col,
            right_on="municipality_code",
            how="left",
        )

        # Filter to requested variables if specified
        if variables is not None:
            keep_cols = [c for c in df.columns] + [
                v for v in variables if v in result.columns and v not in df.columns
            ]
            result = result[keep_cols]

        return result

    # ---------------------------------------------------------------------------
    # Lazy path: read from climasus-data parquets
    # ---------------------------------------------------------------------------
    rel = _unwrap_sus_relation(rel, "sus_census")

    geo_col = (
        detect_geo_column(list(rel.columns), level="municipality") or "municipality_code"
    )

    # Resolve which census file(s) to read
    census_dir: Path = data_path("assets/census")
    if year is not None:
        census_read = f"read_parquet({_sql_path(census_dir / f'census_{year}.parquet')})"
    else:
        glob_path = _sql_path(census_dir / "census_*.parquet")
        census_read = f"read_parquet({glob_path}, union_by_name=true)"

    # Build SELECT clause for requested variables
    if variables is not None:
        var_cols = ", ".join(f"c.{quote_ident(v)}" for v in variables)
        select_vars = f", {var_cols}"
    else:
        select_vars = ", c.* EXCLUDE (municipality_code)"

    h_geo = quote_ident(geo_col)
    sql = (
        f"SELECT h.*{select_vars} "
        f"FROM _census_health h "
        f"LEFT JOIN {census_read} c "
        f"  ON LEFT(CAST(h.{h_geo} AS VARCHAR), 6) "
        f"   = LEFT(CAST(c.municipality_code AS VARCHAR), 6)"
    )

    result = rel.query("_census_health", sql)
    return set_stage(result, "enrichment")

