"""Export data to various formats.

Mirrors R: export.R — uses COPY TO for maximum throughput.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from climasus.core.engine import collect, get_connection, is_relation


def sus_export(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    path: str | Path,
    *,
    fmt: str | None = None,
    overwrite: bool = True,
    compress: str = "snappy",
) -> Path:
    """Export data to parquet, csv, or xlsx.

    Supports lazy DuckDB relations (zero-copy for parquet/csv) and DataFrames.
    Uses DuckDB COPY TO for relations — faster than write_parquet/write_csv.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if fmt is None:
        fmt = path.suffix.lstrip(".").lower()

    if not overwrite and path.exists():
        raise FileExistsError(f"File already exists: {path}")

    if fmt == "parquet":
        if is_relation(data):
            _copy_to(data, path, "PARQUET", f"COMPRESSION '{compress}'")
        else:
            pq.write_table(
                pa.Table.from_pandas(data),
                path,
                compression=compress,
            )

    elif fmt == "csv":
        if is_relation(data):
            _copy_to(data, path, "CSV", "HEADER TRUE")
        else:
            data.to_csv(path, index=False)

    elif fmt in ("xlsx", "excel"):
        df = collect(data) if is_relation(data) else data
        try:
            df.to_excel(path, index=False, engine="openpyxl")
        except ImportError:
            raise ImportError("Install openpyxl: pip install climasus4py[excel]")

    else:
        raise ValueError(f"Unsupported format: {fmt}. Use parquet, csv, or xlsx.")

    return path


def _copy_to(rel: duckdb.DuckDBPyRelation, path: Path, fmt: str, opts: str) -> None:
    """Use DuckDB COPY TO — faster than write_parquet/write_csv."""
    conn = get_connection()
    dest = str(path).replace("\\", "/")
    conn.sql(f"COPY rel TO '{dest}' (FORMAT {fmt}, {opts})")
