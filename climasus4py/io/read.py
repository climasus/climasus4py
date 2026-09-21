"""Lazy Parquet and CSV reader — sus_data_read.

Mirrors R: ``climasus4r::sus_data_read()``, which the reference
documentation describes as reading "one or multiple health data files
exported by sus_data_export()". Six of its seven parameters were missing
here, so the Python side read exactly one file (M18).

Two deliberate divergences from R, both recorded:

* **A directory works with the default arguments** (M114). R builds the
  scan pattern with ``paste0("\\.", format, "$")``, so the default
  ``format = NULL`` yields ``"\\.$"`` -- a regex matching a name that
  ends in a literal dot, which no file does. Measured: a directory with
  two Parquet files returns zero and aborts with "Nenhum arquivo valido
  encontrado para leitura". Not replicated: an abort carries no
  information at all, and the documented headline feature is precisely
  reading a directory.
* **``read_metadata`` reads the embedded metadata first, and R's sidecar
  only as a fallback** (M17/M19, decided 21/09/2026). R writes and reads a
  ``<base>_metadata.txt`` beside the file; this package embeds it in the
  Parquet schema. Writing the sidecar *too* would make a third mechanism,
  so it is read and never written — which is what lets a file written in
  R keep its provenance when it is read here.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Literal

import duckdb

from ..core._sql import sql_string
from ..core.engine import get_connection

# Extensions this reader can actually open. R also detects rds, dbf, dbc,
# shapefile, gpkg and geojson; those are R-side formats (or need a
# geospatial stack) and are named in the error rather than half-read.
_SUPPORTED: dict[str, str] = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".tsv": "csv",
    ".txt": "csv",
}

# R detects these and this reader does not. Named explicitly so the error
# says why instead of "unsupported".
_R_ONLY: dict[str, str] = {
    ".rds": "rds",
    ".dbf": "dbf",
    ".dbc": "dbc",
    ".shp": "shapefile",
    ".gpkg": "gpkg",
    ".geojson": "geojson",
}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "climasus4py: Leitor de Dados",
        "found": "Encontrados {n} arquivo(s) para processar",
        "not_found": "Caminho nao encontrado: {path}",
        "parallel": "DuckDB le em paralelo por conta propria; threads={n}",
        "reading": "Lendo {n} arquivo(s)...",
        "done": "Carregados {n_cols} colunas de {n} arquivo(s)",
        "no_meta": "Nenhum arquivo carrega metadata",
    },
    "en": {
        "title": "climasus4py: Data Reader",
        "found": "Found {n} file(s) to process",
        "not_found": "Path not found: {path}",
        "parallel": "DuckDB reads in parallel on its own; threads={n}",
        "reading": "Reading {n} file(s)...",
        "done": "Loaded {n_cols} columns from {n} file(s)",
        "no_meta": "No file carries metadata",
    },
    "es": {
        "title": "climasus4py: Lector de Datos",
        "found": "Encontrados {n} archivo(s) para procesar",
        "not_found": "Ruta no encontrada: {path}",
        "parallel": "DuckDB lee en paralelo por su cuenta; threads={n}",
        "reading": "Leyendo {n} archivo(s)...",
        "done": "Cargadas {n_cols} columnas de {n} archivo(s)",
        "no_meta": "Ningun archivo lleva metadata",
    },
}


def sus_data_read(
    path: str | Path | list[str | Path],
    format: str | None = None,
    parallel: bool = False,
    workers: int = 4,
    read_metadata: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Read one or many Parquet/CSV files lazily as a single relation.

    No data is loaded into memory until the relation is materialised
    (e.g. with ``materialize()``, ``.df()`` or ``.fetchdf()``).

    Several files are combined **by column name**, matching R, which
    finishes with ``dplyr::bind_rows()``: a column missing from one file
    comes back as ``NULL`` for that file's rows rather than shifting
    values into the wrong column. Files are read through a single DuckDB
    scan, so this stays lazy no matter how many there are.

    Args:
        path: A file, a directory, a glob such as ``"cache/*.parquet"``,
            or a list mixing any of those. A directory is scanned
            recursively. Sidecar files written by R's
            ``include_metadata`` (``*_metadata.txt``) are skipped, as R
            skips them.
        format: Format override — ``"parquet"`` or ``"csv"``. When
            scanning a directory this also restricts which extensions are
            collected. ``None`` (default) detects from each extension and
            collects every supported one.
        parallel: Whether to raise DuckDB's thread count for this read.
            Defaults to ``False``. DuckDB's scan is already
            multi-threaded, so this is not what makes reading parallel —
            the parameter exists because R has it, where it switches
            ``lapply`` for ``future.apply``.
        workers: Thread count used when *parallel*. Defaults to ``4``, as
            in R. Restored afterwards.
        read_metadata: Whether to restore the pipeline metadata onto the
            relation. Defaults to ``False``, as in R.

            Looked for in two places, in order: the Parquet schema, under
            ``climasus_meta``, where ``sus_meta(rel, to_parquet=...)`` and
            ``sus_export(include_metadata=True)`` put it; then a
            ``<base>_metadata.txt`` sidecar, which is what
            ``climasus4r::sus_data_export(include_metadata = TRUE)``
            writes. The sidecar is **read and never written** — see M19.

            With several files, the metadata of the **first** that carries
            any is used, and a warning names it when more than one does.
            Metadata describes one pipeline, and merging several would
            describe none of them.
        lang: Message language — ``"pt"`` (default), ``"en"`` or
            ``"es"``.
        verbose: Whether to print progress. Defaults to ``True``, as in
            R.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` over every file read, carrying
        ``sus_meta`` when *read_metadata* found any.

    Raises:
        ValueError: If no file is found, if *format* is not supported, or
            if a file's extension is one only R reads.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_data_read("dados/cache/SIM-DO/SP_2022_all.parquet")
        >>> lote = cs.sus_data_read("dados/cache/SIM-DO", verbose=False)
        >>> anos = cs.sus_data_read(["SP_2021.parquet", "SP_2022.parquet"])
    """
    if lang not in _MESSAGES:
        warnings.warn(
            f"Unsupported lang {lang!r}; using 'pt'.", UserWarning,
            stacklevel=2,
        )
        lang = "pt"
    msg = _MESSAGES[lang]

    fmt = _normalise_format(format)
    arquivos = _collect(path, fmt, msg, verbose)
    if not arquivos:
        raise ValueError(
            f"No readable file found in: {path!r}. Supported extensions: "
            f"{', '.join(sorted(_SUPPORTED))}."
        )

    if verbose:
        print(msg["title"])
        print("  " + msg["found"].format(n=len(arquivos)))
        print("  " + msg["reading"].format(n=len(arquivos)))

    conn = get_connection()
    anterior = None
    if parallel:
        anterior = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        conn.execute(f"SET threads={int(workers)}")
        if verbose:
            print("  " + msg["parallel"].format(n=int(workers)))
    try:
        rel = _scan(conn, arquivos, fmt)
    finally:
        if anterior is not None:
            conn.execute(f"SET threads={int(anterior)}")

    if read_metadata:
        rel = _anexar_metadata(rel, arquivos, msg, verbose)

    if verbose:
        print("  " + msg["done"].format(n_cols=len(rel.columns),
                                        n=len(arquivos)))
    return rel


def _anexar_metadata(
    rel: duckdb.DuckDBPyRelation,
    arquivos: list[Path],
    msg: dict[str, str],
    verbose: bool,
) -> duckdb.DuckDBPyRelation:
    """Attach the metadata of the first file that carries any.

    Two places are looked at, in this order:

    1. the Parquet schema, under ``climasus_meta`` — where
       ``sus_meta(rel, to_parquet=...)`` and
       ``sus_export(include_metadata=True)`` put it;
    2. a ``<base>_metadata.txt`` sidecar, which is what
       ``climasus4r::sus_data_export(include_metadata = TRUE)`` writes.

    The sidecar is **read** and never written (M17/M19): writing it too
    would make a third mechanism, while reading it is what lets a file
    written in R keep its provenance when it is read here.

    The *first* file wins. Metadata describes a pipeline, and several
    files read together may come from several pipelines; merging them
    would fabricate a history that produced none of the data. When more
    than one file carries metadata that is said out loud rather than
    resolved quietly.
    """
    from ..core._stage import add_history
    from ..core.meta import _attach_meta, sus_meta

    achados: list[tuple[Path, dict]] = []
    for f in arquivos:
        meta: dict | None = None
        if f.suffix.lower() == ".parquet":
            try:
                lido = sus_meta(from_parquet=f)
                meta = sus_meta(lido) if lido is not None else None
            except Exception:
                meta = None
        if not meta:
            meta = _ler_sidecar(f)
        if meta:
            achados.append((f, meta))

    if not achados:
        if verbose:
            print("  " + msg["no_meta"])
        return rel

    if len(achados) > 1:
        warnings.warn(
            f"sus_data_read: {len(achados)} of the {len(arquivos)} files "
            f"carry metadata, and it is taken from the first, "
            f"{achados[0][0].name}. Metadata describes one pipeline, and "
            f"merging several would describe none of them; read the files "
            f"separately if their provenance matters.",
            UserWarning,
            stacklevel=3,
        )

    origem, meta = achados[0]
    rel = _attach_meta(rel, dict(meta))
    return add_history(rel, f"sus_data_read(read_metadata from {origem.name})")


def _ler_sidecar(caminho: Path) -> dict[str, str] | None:
    """Read R's ``<base>_metadata.txt``: one ``chave: valor`` per line.

    Same shape R parses on the way in — ``readLines``, then split on the
    first colon. A line with no colon is skipped rather than failing the
    read: the file is a convenience, and half of it is worth more than an
    exception.
    """
    lado = caminho.with_name(caminho.stem + "_metadata.txt")
    if not lado.is_file():
        return None
    try:
        texto = lado.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    saida: dict[str, str] = {}
    for linha in texto.splitlines():
        if ":" not in linha:
            continue
        chave, valor = linha.split(":", 1)
        chave = chave.strip()
        if chave:
            saida[chave] = valor.strip()
    return saida or None


def _normalise_format(format: str | None) -> str | None:
    """Map a *format* argument to one of the reader's two families."""
    if format is None:
        return None
    f = format.strip().lower().lstrip(".")
    if f in ("parquet", "pq"):
        return "parquet"
    if f in ("csv", "tsv", "txt"):
        return "csv"
    if f in set(_R_ONLY.values()):
        raise ValueError(
            f"format={format!r} is read by climasus4r but not here: "
            f"{f} needs an R-side or geospatial reader. This function "
            f"reads parquet and csv."
        )
    raise ValueError(
        f"Unsupported format={format!r}. Use 'parquet' or 'csv'."
    )


def _collect(
    path: str | Path | list[str | Path],
    fmt: str | None,
    msg: dict[str, str],
    verbose: bool,
) -> list[Path]:
    """Expand files, directories and globs into a de-duplicated list.

    Mirrors R's loop: a directory is scanned recursively, a missing path
    warns rather than aborting, and only an empty final list is an error.

    The list keeps the order the paths were given, and a directory scan is
    sorted, so the same call collects the same files in the same sequence
    -- a set for de-duplication would make it depend on hash seeding.
    That fixes the *scan* order, not the row order of the result: when
    both families are present the reader scans each one separately, so
    rows arrive grouped by family. A relation has no promised row order
    either way; sort explicitly if it matters.
    """
    entradas = path if isinstance(path, (list, tuple)) else [path]
    if not entradas:
        raise ValueError("path must name at least one file or directory.")

    aceitas = (set(_SUPPORTED) if fmt is None
               else {e for e, f in _SUPPORTED.items() if f == fmt})
    saida: list[Path] = []
    vistos: set[Path] = set()

    def juntar(p: Path) -> None:
        resolvido = p.resolve()
        if resolvido not in vistos:
            vistos.add(resolvido)
            saida.append(p)

    for entrada in entradas:
        p = Path(entrada)
        if p.is_dir():
            achados = sorted(
                f for f in p.rglob("*")
                if f.is_file() and f.suffix.lower() in aceitas
                and not f.name.endswith("_metadata.txt")
            )
            for f in achados:
                juntar(f)
        elif p.is_file():
            _conferir_extensao(p)
            juntar(p)
        else:
            # A glob, or a path that does not exist. Path.glob needs the
            # pattern split from its anchor, so go through the parent.
            texto = str(entrada)
            if any(c in texto for c in "*?["):
                pai = p.parent if str(p.parent) else Path(".")
                achados = sorted(
                    f for f in pai.glob(p.name)
                    if f.is_file() and f.suffix.lower() in aceitas
                    and not f.name.endswith("_metadata.txt")
                )
                for f in achados:
                    juntar(f)
            elif verbose:
                warnings.warn(msg["not_found"].format(path=texto),
                              UserWarning, stacklevel=3)
    return saida


def _conferir_extensao(p: Path) -> None:
    """Reject an explicitly named file this reader cannot open."""
    ext = p.suffix.lower()
    if ext in _SUPPORTED:
        return
    if ext in _R_ONLY:
        raise ValueError(
            f"{p.name!r} is a {_R_ONLY[ext]} file. climasus4r reads it, "
            f"this function does not: it reads parquet and csv. Convert "
            f"it first, or read it in R."
        )
    raise ValueError(
        f"Unsupported extension {ext!r} in {p.name!r}. Supported: "
        f"{', '.join(sorted(_SUPPORTED))}. Pass format= to override "
        f"detection."
    )


def _scan(
    conn: duckdb.DuckDBPyConnection, arquivos: list[Path], fmt: str | None
) -> duckdb.DuckDBPyRelation:
    """One relation over every file, combined by column name.

    A directory can hold both families, and R handles that -- it picks a
    reader per file and finishes with ``bind_rows`` -- so refusing the
    mix would be a gratuitous divergence. Each family gets one scan, and
    the two are joined with ``UNION ALL BY NAME``, which stays lazy.
    """
    porfamilia: dict[str, list[str]] = {}
    for f in arquivos:
        familia = fmt or _SUPPORTED[f.suffix.lower()]
        porfamilia.setdefault(familia, []).append(
            str(f).replace("\\", "/"))

    # union_by_name matches R's dplyr::bind_rows. Without it DuckDB unions
    # by POSITION, which would silently pour one file's column into
    # another's when the schemas differ in order -- corrupting values
    # rather than failing.
    if len(porfamilia) == 1:
        familia, caminhos = next(iter(porfamilia.items()))
        return (conn.read_parquet(caminhos, union_by_name=True)
                if familia == "parquet"
                else conn.read_csv(caminhos, union_by_name=True))

    # Built from the table functions directly, with no registered view.
    # Registering one and unregistering it on the way out would hand back
    # a lazy relation over a view that no longer exists, which fails the
    # moment the caller touches it; leaving it registered would litter the
    # singleton connection's namespace.
    partes = [
        f"SELECT * FROM {_funcao(familia)}("
        f"[{', '.join(sql_string(c) for c in caminhos)}], union_by_name=true)"
        for familia, caminhos in porfamilia.items()
    ]
    return conn.sql(" UNION ALL BY NAME ".join(partes))


def _funcao(familia: str) -> str:
    """The DuckDB table function for a format family."""
    return "read_parquet" if familia == "parquet" else "read_csv"
