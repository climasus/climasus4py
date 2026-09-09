"""Pipeline stage tracking for DuckDB relations.

Tracks which pipeline stage each relation has passed through, enabling
``assert_after`` checks that enforce correct execution order.

The metadata dict stored per relation has the shape::

    {
        "stage":   str,           # e.g. "clean"
        "system":  str | None,    # e.g. "SIM-DO"
        "type":    str | None,    # e.g. "health" or "climate"
        "stages":  list[str],     # ordered stages visited (e.g. ["import", "clean"])
        "history": list[str],     # timestamped log messages (mirrors R sus_meta$history)
    }

Note: ``stages`` tracks which pipeline functions were called (for assert_after).
      ``history`` tracks human-readable timestamped messages (for quality_report).
      Mirrors climasus4r::sus_meta(add_history=) behaviour.
"""

from __future__ import annotations

from datetime import datetime
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
    _inherit_from: duckdb.DuckDBPyRelation | None = None,
) -> duckdb.DuckDBPyRelation:
    """Record *stage* (and optional metadata) for *rel* and return it.

    When a pipeline function transforms a relation into a new object
    (e.g. after ``.filter()`` or ``.project()``), the WeakKeyDictionary
    loses the original metadata. Pass ``_inherit_from=original_rel`` to
    copy the parent metadata (system, type, stages, history) into the
    new relation before recording the new stage.

    Args:
        rel: DuckDB relation to tag.
        stage: Pipeline stage name, e.g. ``"import"`` or ``"clean"``.
        system: DATASUS system identifier (e.g. ``"SIM-DO"``).
        rel_type: Relation type (e.g. ``"health"`` or ``"climate"``).
        _inherit_from: Optional parent relation whose metadata should be
            inherited. Use when *rel* is a new object derived from the
            parent (e.g. after filter/project operations).

    Returns:
        The same *rel* (allows ``return set_stage(rel, "clean")``).
    """
    # start from current meta (may be empty for a new relation object)
    existing = dict(_stage_map.get(rel, {}))

    # inherit parent metadata when the relation object changed
    if _inherit_from is not None:
        parent = _stage_map.get(_inherit_from, {})
        # parent fills in missing keys — existing has priority
        for k, v in parent.items():
            if k not in existing or not existing[k]:
                existing[k] = v

    stages: list[str] = list(existing.get("stages", []))
    if not stages or stages[-1] != stage:
        stages.append(stage)

    _stage_map[rel] = {
        "stage":   stage,
        "system":  system    if system   is not None else existing.get("system"),
        "type":    rel_type  if rel_type is not None else existing.get("type"),
        "stages":  stages,
        "history": list(existing.get("history", [])),
    }
    return rel


def format_history_entry(message: str) -> str:
    """Stamp *message* with the current time, as the history log expects.

    One place decides the format so every writer produces the same shape.
    ``sus_meta(add_history=...)`` used to append the raw string while this
    module's :func:`add_history` stamped it, which left the audit trail
    holding two different formats in one list — half the entries parseable
    by timestamp and half not.
    """
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"


def add_history(
    rel: duckdb.DuckDBPyRelation,
    message: str,
) -> duckdb.DuckDBPyRelation:
    """Append a timestamped message to the relation's history log.

    Mirrors ``climasus4r::sus_meta(add_history=...)`` — each pipeline
    function calls this after its work to build an auditable log::

        "[2023-01-01 10:01:00] Imported DATASUS SIM-DO (SE) via FTP"
        "[2023-01-01 10:02:00] Cleaned character encoding (UTF-8)..."

    Args:
        rel: DuckDB relation to update.
        message: Human-readable description of the operation performed.

    Returns:
        The same *rel*.
    """
    existing = _stage_map.get(rel, {})
    history: list[str] = list(existing.get("history", []))
    history.append(format_history_entry(message))
    _stage_map[rel] = {**existing, "history": history}
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

    Keys: ``stage``, ``system``, ``type``, ``stages``, ``history``.

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