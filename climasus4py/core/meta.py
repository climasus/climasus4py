"""Metadata inspection for DuckDB relations in the climasus4py pipeline.

Provides ``sus_meta`` — a lightweight introspection function that returns
pipeline metadata recorded by ``set_stage`` / ``get_meta`` without forcing
any DuckDB materialisation.

Paridade com ``climasus4r`` legacy: ``sus_meta()``.
"""

from __future__ import annotations

import duckdb

from ._stage import get_meta


def sus_meta(
    rel: duckdb.DuckDBPyRelation,
    field: str | None = None,
    *,
    add_history: str | None = None,
) -> dict | str | list | None:
    """Return pipeline metadata for a DuckDB relation.

    Metadata is keyed on the exact relation object (``WeakKeyDictionary``).
    ``sus_data_import`` and the enrichment functions (``sus_climate``,
    ``sus_spatial_join``, ``sus_census``) tag their *output* relation with
    a stage marker. **DuckDB operations such as ``.filter()`` and
    ``.project()`` create a new relation object that does not carry the
    parent's metadata** — call ``sus_meta`` on the immediate output of
    these tagged functions or apply ``set_stage`` manually after each
    transformation. The ``sus_data_clean_encoding`` /
    ``sus_data_standardize`` / ``sus_filter`` / ``sus_data_create_variables``
    / ``sus_data_aggregate`` chain is not currently instrumented (tracked
    in ``ideias-climasus4py-v2.md`` as a v2.0 refactor).

    Args:
        rel: DuckDB relation to inspect.
        field: Optional field to retrieve from the metadata dict.
            One of ``"stage"``, ``"system"``, ``"type"``, ``"history"``.
            If ``None``, returns the full dict.
        add_history: If provided, appends this string to the history list in
            the returned metadata **without** modifying the stored metadata.
            Useful for constructing annotated copies. When combined with
            *field*, ``"history"`` is the only field that includes the
            appended entry; other fields are unaffected.

    Returns:
        - Full metadata dict when *field* is ``None``.
        - The value of the requested *field* (``str``, ``list[str]``, etc.).
        - ``None`` when the relation has no recorded metadata.

    Raises:
        ValueError: If *field* is not one of the recognised keys.
        TypeError: If *rel* is not a ``DuckDBPyRelation``.

    Examples::

        import climasus4py as cs

        rel = cs.sus_data_import("SIM-DO", "SP", 2023)
        cs.sus_meta(rel)
        # {'stage': 'import', 'system': 'SIM-DO', 'type': 'health', 'history': ['import']}

        cs.sus_meta(rel, field="stage")
        # 'import'

        cs.sus_meta(rel, field="history")
        # ['import']
    """
    if not isinstance(rel, duckdb.DuckDBPyRelation):
        raise TypeError(
            f"sus_meta: expected DuckDBPyRelation, got {type(rel).__name__!r}"
        )

    _VALID_FIELDS = {"stage", "system", "type", "history"}
    if field is not None and field not in _VALID_FIELDS:
        raise ValueError(
            f"sus_meta: unknown field {field!r}. "
            f"Choose one of {sorted(_VALID_FIELDS)} or None."
        )

    meta = get_meta(rel)
    if meta is None:
        return None if field is None else None

    # Build a shallow copy to avoid mutating stored metadata
    result = dict(meta)

    if add_history is not None:
        history = list(result.get("history", []))
        if not history or history[-1] != add_history:
            history = history + [add_history]
        result["history"] = history

    if field is None:
        return result

    return result.get(field)
