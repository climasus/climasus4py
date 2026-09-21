"""Export data to various formats.

Mirrors R: export.R — uses COPY TO for maximum throughput.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Literal

import duckdb

from ..core._sql import sql_string
from ..core.engine import collect, get_connection, is_relation

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {"written": "sus_export: {path} gravado ({fmt}, {kb:.1f} KB)"},
    "en": {"written": "sus_export: wrote {path} ({fmt}, {kb:.1f} KB)"},
    "es": {"written": "sus_export: {path} grabado ({fmt}, {kb:.1f} KB)"},
}


def sus_export(
    data: duckdb.DuckDBPyRelation,
    path: str | Path,
    *,
    fmt: str | None = None,
    overwrite: bool = False,
    compress: str = "snappy",
    compression_level: int | None = None,
    include_metadata: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = False,
) -> Path:
    """Export data to Parquet, CSV, or Excel.

    Takes a **lazy DuckDB relation**, and uses DuckDB ``COPY TO``, which
    avoids Python-side materialisation for parquet and CSV (significantly
    faster than ``write_parquet`` / ``write_csv``). A
    ``pandas.DataFrame`` is refused: the docstring used to promise it was
    accepted while the code raised ``TypeError``, which is half of M19.
    Wrap one with :func:`sus_as_relation` — the refusal says so.

    Args:
        data: Data to export — a lazy ``DuckDBPyRelation``.
        path: Destination file path. The format is inferred from the
            extension unless *fmt* is specified explicitly.
        fmt: Output format override — ``"parquet"``, ``"csv"``, or
            ``"xlsx"`` / ``"excel"``. If ``None``, inferred from
            *path*.
        overwrite: Whether to replace *path* if it already exists.
            Defaults to ``False`` — an existing file raises
            ``FileExistsError`` instead of being destroyed, matching
            ``climasus4r::sus_data_export(overwrite = FALSE)``. The
            default used to be ``True``, so the same call that the R
            refuses silently replaced the file here. Pass ``True`` to
            replace on purpose.
        compress: Parquet compression codec — ``"snappy"`` (default),
            ``"zstd"``, ``"gzip"``, or ``"none"``. R spells this as a
            logical ``compress = TRUE`` plus a separate codec choice;
            here the codec name carries both, and ``"none"`` is the off
            switch.
        compression_level: Codec effort, 1 to 22. Only ZSTD accepts one —
            DuckDB raises ``Binder Error: Compression level is only
            supported for the ZSTD compression codec`` for the others, so
            a level passed with a different codec is dropped with a
            warning rather than turned into an error. ``None`` (default)
            leaves DuckDB's own default. R defaults this to ``6``; the
            default is not carried over here because R's default codec
            differs, and silently re-compressing every existing call's
            output would be a change nobody asked for.

            **A higher level does not mean a smaller file here.** Measured
            on DuckDB 1.5.3 over 2,000 rows: levels 1, 3, 6, 9, 12, 15,
            19 and 22 gave 2979, 3893, 2533, 2533, 5372, 4413, 5542 and
            5542 bytes — level 22 more than twice level 6. The same
            non-monotonic shape appears with random data, so it is not a
            property of one dataset. Recorded as M116; measure before
            choosing a level.
        include_metadata: Whether to carry the relation's ``sus_meta``
            into the file. Defaults to ``False``, and that default is the
            decision recorded as M19.

            ``False`` keeps the fast path: DuckDB ``COPY TO``, no
            Python-side materialisation, data only. ``True`` routes the
            write through ``sus_meta(rel, to_parquet=...)``, which embeds
            the metadata as JSON in the Parquet schema at the cost of
            going through Arrow — so it materialises, and on a large
            relation that is the difference the flag buys or spends.

            **Parquet only.** CSV and Excel have nowhere to put it, and
            asking for metadata in those formats warns rather than
            failing, because the data still writes correctly.

            R spells this ``include_metadata = TRUE`` and writes a
            separate ``<base>_metadata.txt`` beside the file. That
            sidecar is **not** written here: it would be a third
            mechanism alongside the embedded schema, and the schema is
            the one ``sus_meta(from_parquet=...)`` and
            ``sus_data_read(read_metadata=True)`` already read. A sidecar
            produced by R *is* read back, so files cross the boundary in
            that direction.
        lang: Message language — ``"pt"`` (default), ``"en"`` or
            ``"es"``.
        verbose: Whether to print what was written. Defaults to
            ``False``, unlike R's ``TRUE``: this function is called from
            inside ``sus_pipeline``, and a library that prints by default
            is harder to use than one that does not. The warning about
            lost ``sus_meta`` is **not** governed by this flag — it
            reports a loss, not progress.

    Returns:
        Resolved ``pathlib.Path`` of the written file.

    Raises:
        TypeError: If *data* is not a lazy ``DuckDBPyRelation``.
        FileExistsError: If *path* exists and *overwrite* is ``False``.
        ValueError: If *fmt* (or the inferred extension) is not
            supported, or if *compression_level* is out of range.
        ImportError: If Excel export is requested but ``openpyxl`` is
            not installed.

    Note:
        ``include_metadata`` and ``metadata``, which R has, are **not**
        implemented. R writes a ``<base>_metadata.txt`` sidecar; this
        package embeds metadata in the Parquet schema through
        ``sus_meta(rel, to_parquet=...)``. Implementing the R parameters
        would make a third mechanism, and picking one is an open API
        decision — M17 and M19, for the coordinator.

    Example:
        >>> import climasus4py as cs
        >>> cs.sus_export(rel, "output/mortality_2022.parquet")
        PosixPath('output/mortality_2022.parquet')
        >>> cs.sus_export(rel, "output/data.csv")
    """
    if not is_relation(data):
        # The old message stopped at "only accepts lazy DuckDB relations",
        # which is true and useless: the common way to get here is having
        # called .df() to inspect the data and then trying to export it
        # (M19). Name the way out.
        # Not sus_as_duckdb: that one takes a relation and materialises it
        # as a named table, so it cannot start from a DataFrame. Until
        # sus_as_relation existed (M115) this message had to quote an
        # internal call, get_connection().from_df, which was the sign that
        # a public function was missing.
        dica = (
            " Convert it first: sus_export(sus_as_relation(df), path)."
            if type(data).__name__ in ("DataFrame", "Table") else ""
        )
        raise TypeError(
            f"Expected DuckDBPyRelation but got {type(data).__name__}. "
            f"sus_export() only accepts lazy DuckDB relations.{dica}"
        )

    if lang not in _MESSAGES:
        warnings.warn(
            f"Unsupported lang {lang!r}; using 'pt'.", UserWarning,
            stacklevel=2,
        )
        lang = "pt"

    if compression_level is not None and not 1 <= compression_level <= 22:
        raise ValueError(
            f"compression_level must be between 1 and 22, got "
            f"{compression_level}."
        )

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if fmt is None:
        fmt = path.suffix.lstrip(".").lower()

    if not overwrite and path.exists():
        raise FileExistsError(
            f"File already exists and would be replaced: {path}. "
            f"Pass overwrite=True to replace it on purpose."
        )

    if include_metadata and fmt not in ("parquet",):
        warnings.warn(
            f"sus_export: include_metadata=True was ignored for {fmt!r} — "
            f"only Parquet can carry the metadata, which goes in its "
            f"schema. The data itself is written normally.",
            UserWarning,
            stacklevel=2,
        )

    # ``COPY TO`` is what makes this fast — no Python-side materialisation —
    # but it writes only the data, so any pipeline history the relation
    # carries is dropped. With include_metadata the write goes through
    # ``sus_meta(rel, to_parquet=...)`` instead, which embeds it in the
    # schema and pays for that with an Arrow materialisation. Default is
    # off so the fast path stays the fast path: the decision is M19.
    if fmt == "parquet":
        from ..core.meta import sus_meta

        if include_metadata:
            sus_meta(data, to_parquet=path)
            if verbose:
                tam = path.stat().st_size if path.is_file() else 0
                print(_MESSAGES[lang]["written"].format(
                    path=path.name, fmt=fmt, kb=tam / 1024))
            return path

        if sus_meta(data):
            warnings.warn(
                f"sus_export: {path.name} is being written without its "
                f"sus_meta — COPY TO carries data only, so the pipeline "
                f"history is lost. Pass include_metadata=True to embed it "
                f"in the Parquet schema.",
                UserWarning,
                stacklevel=2,
            )

        _valid_compress = {"snappy", "zstd", "gzip", "none", "lz4"}
        if compress not in _valid_compress:
            raise ValueError(
                f"Invalid parquet compression {compress!r}. "
                f"Choose from: {sorted(_valid_compress)}."
            )
        opts = f"COMPRESSION {sql_string(compress)}"
        if compression_level is not None:
            if compress == "zstd":
                opts += f", COMPRESSION_LEVEL {int(compression_level)}"
            else:
                warnings.warn(
                    f"sus_export: compression_level="
                    f"{compression_level} was dropped — DuckDB accepts a "
                    f"level only for zstd, and the codec here is "
                    f"{compress!r}. Pass compress='zstd' to use it.",
                    UserWarning,
                    stacklevel=2,
                )
        _copy_to(data, path, "PARQUET", opts)

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

    if verbose:
        tam = path.stat().st_size if path.is_file() else 0
        print(_MESSAGES[lang]["written"].format(
            path=path.name, fmt=fmt, kb=tam / 1024))

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
