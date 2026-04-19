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
    """Export data to Parquet, CSV, or Excel.

    Accepts lazy DuckDB relations and ``pandas.DataFrame``. For
    relations, uses DuckDB ``COPY TO`` which avoids Python-side
    materialisation for parquet and CSV formats (significantly faster
    than ``write_parquet`` / ``write_csv``).

    Args:
        data: Data to export — a lazy ``DuckDBPyRelation`` or a
            ``pandas.DataFrame``.
        path: Destination file path. The format is inferred from the
            extension unless *fmt* is specified explicitly.
        fmt: Output format override — ``"parquet"``, ``"csv"``, or
            ``"xlsx"`` / ``"excel"``. If ``None``, inferred from
            *path*.
        overwrite: If ``False``, raise ``FileExistsError`` when *path*
            already exists. Defaults to ``True``.
        compress: Parquet compression codec — ``"snappy"`` (default),
            ``"zstd"``, ``"gzip"``, or ``"none"``.

    Returns:
        Resolved ``pathlib.Path`` of the written file.

    Raises:
        FileExistsError: If *path* exists and *overwrite* is ``False``.
        ValueError: If *fmt* (or the inferred extension) is not
            supported.
        ImportError: If Excel export is requested but ``openpyxl`` is
            not installed.

    Example:
        >>> import climasus as cs
        >>> cs.sus_export(rel, "output/mortality_2022.parquet")
        PosixPath('output/mortality_2022.parquet')
        >>> cs.sus_export(rel, "output/data.csv")
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
