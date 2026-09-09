"""Metadata inspection and persistence for DuckDB relations.

Mirrors R: climasus4r::sus_meta()

Dispatches on which argument is supplied:
- Introspection: field, add_history
- Persistence: to_parquet, from_parquet, to_duckdb, from_duckdb
- Vocabulary:  valid_values
"""

from __future__ import annotations

import json
import uuid

import duckdb
import pandas as pd

from ._stage import CANONICAL_STAGES, format_history_entry, get_meta, _stage_map


def _append_history(history: list[str], message: str) -> list[str]:
    """Append a timestamped *message*, skipping a consecutive repeat.

    The entry is stamped by :func:`format_history_entry` so the public
    ``sus_meta(add_history=...)`` writes the same shape as the internal
    writer. The de-duplication has to compare the *message*, not the
    finished entry: two identical messages stamped a second apart are
    different strings, and comparing those would never match.
    """
    if history and history[-1].endswith(message):
        return list(history)
    return [*history, format_history_entry(message)]
from .engine import collect_arrow, get_connection

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

META_SCHEMA_KEY  = "climasus_meta"   # key embedded in Arrow/Parquet schema
META_TABLE_SUFFIX = "__meta"         # companion table suffix in DuckDB files
VALID_BACKENDS   = ["parquet", "duckdb"]

_VALID_FIELDS = {"stage", "system", "type", "history", "stages"}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _attach_meta(rel: duckdb.DuckDBPyRelation, meta: dict | None) -> duckdb.DuckDBPyRelation:
    """Re-attach a metadata dict to a relation."""
    if meta:
        _stage_map[rel] = dict(meta)
    return rel


def _resolve_valid_values(kind: str) -> list:
    """Return controlled vocabulary for a metadata field."""
    if kind == "backend":
        return list(VALID_BACKENDS)
    if kind == "stage":
        return list(CANONICAL_STAGES)
    if kind == "system":
        from .importer import _datasus_sources
        return sorted(_datasus_sources()["systems"].keys())
    raise ValueError(
        f"valid_values: unknown kind {kind!r}. "
        "Choose one of ['system', 'stage', 'backend']."
    )


def _write_parquet_with_meta(rel: duckdb.DuckDBPyRelation, path) -> str:
    """Write relation to Parquet with metadata embedded in the Arrow schema."""
    import pyarrow.parquet as pq
    tbl  = collect_arrow(rel)
    meta = sus_meta(rel) or {}
    md   = dict(tbl.schema.metadata or {})
    md[META_SCHEMA_KEY.encode()] = json.dumps(meta).encode()
    pq.write_table(tbl.replace_schema_metadata(md), str(path))
    return str(path)


def _read_parquet_with_meta(path) -> duckdb.DuckDBPyRelation:
    """Read Parquet as a lazy relation and restore its metadata."""
    import pyarrow.parquet as pq
    raw  = (pq.read_schema(str(path)).metadata or {}).get(META_SCHEMA_KEY.encode())
    meta = json.loads(raw) if raw else None
    rel  = get_connection().read_parquet(str(path))
    return _attach_meta(rel, meta)


def _write_duckdb_with_meta(rel: duckdb.DuckDBPyRelation, db_path, table: str) -> str:
    """Persist relation to DuckDB with a companion metadata table."""
    meta = sus_meta(rel) or {}
    tbl  = collect_arrow(rel)
    out  = duckdb.connect(str(db_path))
    try:
        out.register("_arrow_in", tbl)
        out.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM _arrow_in')
        out.unregister("_arrow_in")
        out.execute(
            f'CREATE OR REPLACE TABLE "{table}{META_TABLE_SUFFIX}" (climasus_meta VARCHAR)'
        )
        out.execute(
            f'INSERT INTO "{table}{META_TABLE_SUFFIX}" VALUES (?)', [json.dumps(meta)]
        )
    finally:
        out.close()
    return str(db_path)


def _read_duckdb_with_meta(db_path, table: str) -> duckdb.DuckDBPyRelation:
    """Read DuckDB table as a lazy relation and restore its metadata."""
    con   = get_connection()
    alias = f"climasus_src_{uuid.uuid4().hex[:8]}"
    safe  = str(db_path).replace("\\", "/")
    con.execute(f"ATTACH '{safe}' AS {alias} (READ_ONLY)")
    row  = con.execute(
        f'SELECT climasus_meta FROM {alias}."{table}{META_TABLE_SUFFIX}"'
    ).fetchone()
    meta = json.loads(row[0]) if row and row[0] else None
    rel  = con.sql(f'SELECT * FROM {alias}."{table}"')
    return _attach_meta(rel, meta)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_meta(
    rel=None,
    field: str | None = None,
    *,
    add_history: str | None = None,
    valid_values: str | None = None,
    to_parquet=None,
    from_parquet=None,
    to_duckdb=None,
    from_duckdb=None,
    table: str = "data",
) -> dict | str | list | None:
    """Manage metadata of a climasus DuckDB relation.

    Mirrors ``climasus4r::sus_meta()``. Dispatches on which argument
    is supplied:

    **Vocabulary** — no relation needed:

    .. code-block:: python

        cs.sus_meta(valid_values="stage")
        # ['import', 'clean', 'standardize', 'variables', 'filter', 'aggregate']

        cs.sus_meta(valid_values="system")
        # ['SIM-DO', 'SIH-RD', 'SINAN-DENGUE', ...]

    **Persistence** — write with metadata embedded:

    .. code-block:: python

        cs.sus_meta(rel, to_parquet="data/sim_se_2023.parquet")
        cs.sus_meta(rel, to_duckdb="data/sim_se_2023.duckdb", table="sim_do")

    **Restore** — read back lazy with metadata:

    .. code-block:: python

        rel = cs.sus_meta(from_parquet="data/sim_se_2023.parquet")
        rel = cs.sus_meta(from_duckdb="data/sim_se_2023.duckdb", table="sim_do")

    **Introspection** — inspect metadata of a relation:

    .. code-block:: python

        cs.sus_meta(rel)
        # {'stage': 'aggregate', 'system': 'SIM-DO', 'type': 'health',
        #  'stages': ['import', 'clean', 'standardize', 'variables', 'filter', 'aggregate'],
        #  'history': ['[2026-06-19 20:25:40] Imported...', ...]}

        cs.sus_meta(rel, "stage")      # 'aggregate'
        cs.sus_meta(rel, "system")     # 'SIM-DO'
        cs.sus_meta(rel, "stages")     # ['import', 'clean', ...]
        cs.sus_meta(rel, "history")    # list of timestamped entries

    Args:
        rel: DuckDB relation to inspect or persist.
        field: Metadata field to retrieve — ``"stage"``, ``"system"``,
            ``"type"``, ``"stages"``, ``"history"``. ``None`` returns
            the full dict.
        add_history: Append an entry to the history list in the returned
            copy without mutating stored metadata.
        valid_values: Return controlled vocabulary for a field —
            ``"stage"``, ``"system"``, ``"backend"``.
        to_parquet: Write relation to this Parquet path with metadata
            in the Arrow schema. Returns *rel* unchanged for chaining.
        from_parquet: Read a Parquet file as a lazy relation and restore
            its metadata.
        to_duckdb: Write relation to this DuckDB path with a companion
            metadata table. Returns *rel* unchanged for chaining.
        from_duckdb: Read a DuckDB table as a lazy relation and restore
            its metadata.
        table: Table name inside the DuckDB file (default ``"data"``).

    Returns:
        Depends on operation: ``list`` (vocabulary), ``DuckDBPyRelation``
        (readers/writers), ``dict`` / field value / ``None`` (introspection).
    """
    # --- vocabulary ---
    if valid_values is not None:
        return _resolve_valid_values(valid_values)

    # --- read from persistence ---
    if from_parquet is not None:
        return _read_parquet_with_meta(from_parquet)
    if from_duckdb is not None:
        return _read_duckdb_with_meta(from_duckdb, table=table)

    # --- DataFrame with attrs["sus_meta"] (e.g. sus_climate_inmet output) ---
    if isinstance(rel, pd.DataFrame):
        stored = rel.attrs.get("sus_meta")
        if stored is None:
            return None
        result = dict(stored)
        if add_history is not None:
            result["history"] = _append_history(
                list(result.get("history", [])), add_history
            )
        return result if field is None else result.get(field)

    # --- relation required from here ---
    if not isinstance(rel, duckdb.DuckDBPyRelation):
        raise TypeError(
            f"sus_meta: expected DuckDBPyRelation, got {type(rel).__name__!r}"
        )

    # --- write to persistence ---
    if to_parquet is not None:
        _write_parquet_with_meta(rel, to_parquet)
        return rel
    if to_duckdb is not None:
        _write_duckdb_with_meta(rel, to_duckdb, table=table)
        return rel

    # --- introspection ---
    if field is not None and field not in _VALID_FIELDS:
        raise ValueError(
            f"sus_meta: unknown field {field!r}. "
            f"Choose one of {sorted(_VALID_FIELDS)} or None."
        )

    stored = get_meta(rel)
    if stored is None:
        return None

    result = dict(stored)

    if add_history is not None:
        result["history"] = _append_history(
            list(result.get("history", [])), add_history
        )

    return result if field is None else result.get(field)
