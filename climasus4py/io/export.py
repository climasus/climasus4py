"""Export data to various formats.

Mirrors R: export.R — uses COPY TO for maximum throughput.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import duckdb

from ..core._sql import sql_string
from ..core.engine import collect, get_connection, is_relation


def sus_export(
    data: duckdb.DuckDBPyRelation,
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
        >>> import climasus4py as cs
        >>> cs.sus_export(rel, "output/mortality_2022.parquet")
        PosixPath('output/mortality_2022.parquet')
        >>> cs.sus_export(rel, "output/data.csv")
    """
    if not is_relation(data):
        raise TypeError(
            f"Expected DuckDBPyRelation but got {type(data).__name__}. "
            "sus_export() only accepts lazy DuckDB relations."
        )

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if fmt is None:
        fmt = path.suffix.lstrip(".").lower()

    if not overwrite and path.exists():
        raise FileExistsError(f"File already exists: {path}")

    # ``COPY TO`` is what makes this fast — no Python-side materialisation —
    # but it writes only the data, so any pipeline history the relation
    # carries is dropped. Say so instead of losing provenance in silence;
    # ``sus_meta(rel, to_parquet=...)`` keeps it, at the cost of going
    # through Arrow. Which of the two should be the default is an API
    # question, recorded as M19.
    if fmt == "parquet":
        from ..core.meta import sus_meta

        if sus_meta(data):
            warnings.warn(
                f"sus_export: {path.name} is being written without its "
                f"sus_meta — COPY TO carries data only, so the pipeline "
                f"history is lost. Use sus_meta(rel, to_parquet=...) to "
                f"embed it in the Parquet schema.",
                UserWarning,
                stacklevel=2,
            )

        _valid_compress = {"snappy", "zstd", "gzip", "none", "lz4"}
        if compress not in _valid_compress:
            raise ValueError(
                f"Invalid parquet compression {compress!r}. "
                f"Choose from: {sorted(_valid_compress)}."
            )
        _copy_to(data, path, "PARQUET", f"COMPRESSION {sql_string(compress)}")

    elif fmt == "csv":
        _copy_to(data, path, "CSV", "HEADER TRUE")

    elif fmt in ("xlsx", "excel"):
        df = collect(data)
        try:
            df.to_excel(path, index=False, engine="openpyxl")
        except ImportError as err:
            raise ImportError("Install openpyxl: pip install climasus4py[excel]") from err

    else:
        raise ValueError(f"Unsupported format: {fmt}. Use parquet, csv, or xlsx.")

    return path


def _copy_to(rel: duckdb.DuckDBPyRelation, path: Path, fmt: str, opts: str) -> None:
    """Use DuckDB COPY TO — faster than write_parquet/write_csv.

    *path* is quoted via :func:`sql_string`; *fmt* and *opts* are caller-controlled
    SQL fragments and must come from a trusted source. The relation is registered
    under a uuid-suffixed view name so the singleton connection's global namespace
    stays clean between calls.
    """
    import uuid

    conn = get_connection()
    dest = sql_string(str(path).replace("\\", "/"))
    view_name = f"_export_view_{uuid.uuid4().hex[:12]}"
    conn.register(view_name, rel)
    try:
        conn.execute(f"COPY {view_name} TO {dest} (FORMAT {fmt}, {opts})")
    finally:
        conn.unregister(view_name)
