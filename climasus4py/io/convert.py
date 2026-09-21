"""Conversions that carry ``sus_meta`` across a format boundary.

Mirrors R: sus_as_arrow(), sus_as_duckdb()

These exist for one reason: metadata survival. ``collect_arrow()`` gives the
data and drops the pipeline history; these give both. The R versions do the
same — ``sus_as_arrow()`` embeds the metadata as JSON in the Arrow schema and
``sus_as_duckdb()`` writes a companion table beside the data.

Not to be confused with the file writers, which already exist under a
different name: ``write_parquet_climasus()`` and ``write_duckdb_climasus()``
in R are ``sus_meta(rel, to_parquet=...)`` and ``sus_meta(rel, to_duckdb=...)``
here.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

import duckdb

from ..core._sql import quote_ident
from ..core._stage import add_history
from ..core.engine import collect_arrow, get_connection, is_relation
from ..core.meta import (
    META_SCHEMA_KEY,
    META_TABLE_SUFFIX,
    _attach_meta,
    sus_meta,
)

if TYPE_CHECKING:
    import pyarrow as pa


def sus_as_arrow(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    """Convert a relation to an Arrow table with ``sus_meta`` embedded.

    The metadata goes into the Arrow schema under the ``climasus_meta``
    key, as JSON — the same place :func:`sus_meta` writes it when saving
    Parquet, so a table produced here round-trips through
    ``sus_meta(from_parquet=...)``.

    Use :func:`collect_arrow` instead when the provenance does not matter;
    it skips the schema rewrite.

    Args:
        rel: Lazy DuckDB relation to convert.

    Returns:
        ``pyarrow.Table`` whose schema metadata carries the pipeline
        history. Relations with no metadata yield a table with no
        ``climasus_meta`` key rather than an empty one.

    Raises:
        TypeError: If *rel* is not a lazy DuckDB relation.

    Example:
        >>> import climasus4py as cs
        >>> tbl = cs.sus_as_arrow(rel)
        >>> b"climasus_meta" in tbl.schema.metadata
        True
    """
    if not is_relation(rel):
        raise TypeError(
            f"Expected DuckDBPyRelation but got {type(rel).__name__}. "
            "sus_as_arrow() only accepts lazy DuckDB relations."
        )

    table = collect_arrow(rel)
    meta = sus_meta(rel)
    if not meta:
        # Nothing to carry — hand back the plain table rather than
        # stamping an empty payload that a reader would take for real
        # provenance.
        return table

    schema_meta = dict(table.schema.metadata or {})
    schema_meta[META_SCHEMA_KEY.encode()] = json.dumps(meta).encode()
    return table.replace_schema_metadata(schema_meta)


def sus_as_duckdb(
    rel: duckdb.DuckDBPyRelation,
    con: duckdb.DuckDBPyConnection | None = None,
    *,
    name: str = "sus_data",
    overwrite: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Materialise a relation as a named table, with a metadata companion.

    The data lands in ``name`` and the pipeline history in
    ``<name>__meta``, which is where ``sus_meta(from_duckdb=...)`` looks
    for it. That makes a table written here readable back with its
    provenance intact.

    Python's pipeline is already DuckDB-backed, so unlike the R version
    this does not move data between engines — it just fixes a lazy
    relation into a real table under a known name. When *con* is the
    shared connection (the default) the write stays inside DuckDB; for a
    different connection the data is transferred through Arrow.

    Args:
        rel: Lazy DuckDB relation to materialise.
        con: Target connection. Defaults to the shared singleton from
            :func:`get_connection`.
        name: Table name to create.
        overwrite: When ``False``, raise if *name* already exists.
            Defaults to ``True``.

    Returns:
        Lazy relation over the table just created, carrying the same
        metadata as *rel*.

    Raises:
        TypeError: If *rel* is not a lazy DuckDB relation.
        ValueError: If *name* exists and *overwrite* is ``False``.

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_as_duckdb(rel, name="mortality")
        >>> cs.sus_meta(out)["stage"]
        'standardize'
    """
    if not is_relation(rel):
        raise TypeError(
            f"Expected DuckDBPyRelation but got {type(rel).__name__}. "
            "sus_as_duckdb() only accepts lazy DuckDB relations."
        )

    conn = con if con is not None else get_connection()
    ident = quote_ident(name)
    meta_ident = quote_ident(f"{name}{META_TABLE_SUFFIX}")

    if not overwrite:
        existing = conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [name]
        ).fetchone()
        if existing and existing[0]:
            raise ValueError(
                f"Table {name!r} already exists. Pass overwrite=True to replace it."
            )

    meta: dict[str, Any] | None = sus_meta(rel)
    tmp = f"_as_duckdb_{uuid.uuid4().hex[:12]}"

    try:
        conn.register(tmp, rel)
    except (duckdb.Error, TypeError):
        # A relation belongs to the connection that produced it, so it
        # cannot be registered on a different one. Fall back to an Arrow
        # hand-off, which does materialise.
        conn.register(tmp, collect_arrow(rel))

    try:
        conn.execute(f"CREATE OR REPLACE TABLE {ident} AS SELECT * FROM {tmp}")
    finally:
        conn.unregister(tmp)

    conn.execute(
        f"CREATE OR REPLACE TABLE {meta_ident} (climasus_meta VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {meta_ident} VALUES (?)", [json.dumps(meta or {})]
    )

    out = conn.sql(f"SELECT * FROM {ident}")
    if meta:
        # sus_meta(add_history=...) only annotates the dict it returns, so
        # the metadata has to be attached to the new relation first.
        _attach_meta(out, meta)
        add_history(out, f"Materialised as DuckDB table: {name}")
    return out


def sus_as_relation(
    data: Any,
    *,
    meta: dict[str, Any] | None = None,
) -> duckdb.DuckDBPyRelation:
    """Turn a DataFrame or Arrow table into a lazy DuckDB relation.

    The way *into* the package, closing a family that until now only went
    out: :func:`sus_as_arrow` and :func:`sus_as_duckdb` both take a
    relation, and nothing took one back. The only route was
    ``get_connection().from_df(df)``, from ``climasus4py.core.engine`` —
    an internal module that is not in ``__all__``, which is why
    :func:`sus_export`'s refusal had to quote an internal call to be
    useful at all (M115).

    Every entry point in this package is lazy on purpose, so a
    ``pandas.DataFrame`` is refused at the door rather than converted
    behind the caller's back. That is the right boundary; it was just
    missing a gate. The conversion here is explicit — the caller says
    "make this a relation" and then works lazily from there.

    An Arrow table carrying ``climasus_meta`` in its schema, as
    :func:`sus_as_arrow` writes it, comes back with that metadata
    attached, so ``sus_as_arrow`` and this function round-trip.

    Args:
        data: A ``pandas.DataFrame``, a ``pyarrow.Table``, or an object
            DuckDB can register directly. A relation is returned
            unchanged, so this is safe to call on input of either kind.
        meta: Metadata to attach to the result. Overrides anything read
            from an Arrow schema.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` over *data*.

    Raises:
        TypeError: If *data* is not something DuckDB can read.

    Example:
        >>> import climasus4py as cs
        >>> df = rel.df()                      # materialise to inspect
        >>> cs.sus_export(cs.sus_as_relation(df), "out.parquet")
    """
    if is_relation(data):
        return data

    conn = get_connection()
    embutida: dict[str, Any] | None = None

    tipo = type(data).__name__
    if tipo == "Table" and hasattr(data, "schema"):
        # pyarrow.Table — read the metadata sus_as_arrow left in the
        # schema before handing the table to DuckDB, which drops it.
        bruto = (data.schema.metadata or {}).get(META_SCHEMA_KEY.encode())
        if bruto:
            try:
                embutida = json.loads(bruto.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                embutida = None
        rel = conn.from_arrow(data)
    elif hasattr(data, "columns") and hasattr(data, "to_dict"):
        rel = conn.from_df(data)
    else:
        try:
            rel = conn.from_arrow(data)
        except Exception as err:
            raise TypeError(
                f"sus_as_relation() cannot read {tipo}. Pass a "
                f"pandas.DataFrame, a pyarrow.Table, or a DuckDB relation."
            ) from err

    final = meta if meta is not None else embutida
    if final:
        rel = _attach_meta(rel, final)
    return rel
