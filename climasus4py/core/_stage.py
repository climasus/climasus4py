"""Pipeline stage tracking for DuckDB relations.

Tracks which pipeline stage each relation has passed through, enabling
``assert_after`` checks that enforce correct execution order.

The metadata dict stored per relation has the shape::

    {
        "stage":   str,           # e.g. "filter"
        "system":  str | None,    # e.g. "SIM-DO"
        "type":    str | None,    # e.g. "health" or "climate"
        "history": list[str],     # ordered stages visited
    }
"""

from __future__ import annotations

from weakref import WeakKeyDictionary

import duckdb

CANONICAL_STAGES: list[str] = [
    "import",
    "clean",
    "standardize",
    "filter",
    "variables",
    "aggregate",
]

# WeakKeyDictionary so relations can be garbage-collected without leaks.
_stage_map: WeakKeyDictionary[
    duckdb.DuckDBPyRelation, dict
] = WeakKeyDictionary()


def set_stage(
    rel: duckdb.DuckDBPyRelation,
    stage: str,
    *,
    system: str | None = None,
    rel_type: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Record *stage* (and optional metadata) for *rel* and return it.

    Args:
        rel: DuckDB relation to tag.
        stage: Pipeline stage name, e.g. ``"import"`` or ``"clean"``.
        system: DATASUS system identifier (e.g. ``"SIM-DO"``).
        rel_type: Relation type (e.g. ``"health"`` or ``"climate"``).

    Returns:
        The same *rel* (allows ``return set_stage(rel, "clean")``).
    """
    existing = _stage_map.get(rel, {})
    history: list[str] = list(existing.get("history", []))
    if not history or history[-1] != stage:
        history.append(stage)
    _stage_map[rel] = {
        "stage": stage,
        "system": system if system is not None else existing.get("system"),
        "type": rel_type if rel_type is not None else existing.get("type"),
        "history": history,
    }
    return rel


def get_stage(rel: duckdb.DuckDBPyRelation) -> str | None:
    """Return the recorded stage for *rel*, or ``None`` if unknown.

    Args:
        rel: DuckDB relation to look up.

    Returns:
        Stage string or ``None``.
    """
    meta = _stage_map.get(rel)
    if meta is None:
        return None
    return meta.get("stage")


def get_meta(rel: duckdb.DuckDBPyRelation) -> dict | None:
    """Return the full metadata dict for *rel*, or ``None`` if unknown.

    Keys: ``stage``, ``system``, ``type``, ``history``.

    Args:
        rel: DuckDB relation to look up.

    Returns:
        Metadata dict or ``None``.
    """
    return _stage_map.get(rel)


def assert_after(rel: duckdb.DuckDBPyRelation, stage: str) -> None:
    """Raise ValueError if *rel*'s stage is before *stage* in the pipeline.

    Args:
        rel: DuckDB relation to check.
        stage: Minimum required stage.

    Raises:
        ValueError: If the current stage precedes *stage* in
            ``CANONICAL_STAGES``.
    """
    current = get_stage(rel)
    if current is None:
        return

    try:
        current_idx = CANONICAL_STAGES.index(current)
        required_idx = CANONICAL_STAGES.index(stage)
    except ValueError:
        return

    if current_idx < required_idx:
        raise ValueError(
            f"Pipeline stage '{current}' is before required stage '{stage}'. "
            f"Expected order: {CANONICAL_STAGES}"
        )
