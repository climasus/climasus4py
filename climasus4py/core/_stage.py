"""Pipeline stage tracking for DuckDB relations.

Tracks which pipeline stage each relation has passed through, enabling
``assert_after`` checks that enforce correct execution order.
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
_stage_map: WeakKeyDictionary[duckdb.DuckDBPyRelation, str] = WeakKeyDictionary()


def set_stage(
    rel: duckdb.DuckDBPyRelation,
    stage: str,
) -> duckdb.DuckDBPyRelation:
    """Record *stage* for *rel* and return it (for chaining).

    Args:
        rel: DuckDB relation to tag.
        stage: Pipeline stage name, e.g. ``"import"`` or ``"clean"``.

    Returns:
        The same *rel* (allows ``return set_stage(rel, "clean")``).
    """
    _stage_map[rel] = stage
    return rel


def get_stage(rel: duckdb.DuckDBPyRelation) -> str | None:
    """Return the recorded stage for *rel*, or ``None`` if unknown.

    Args:
        rel: DuckDB relation to look up.

    Returns:
        Stage string or ``None``.
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
