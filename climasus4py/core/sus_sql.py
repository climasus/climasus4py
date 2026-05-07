"""Arbitrary SQL over lazy relations — sus_sql.

Mirrors R: sus_sql() in climasus4r — allows users to write raw DuckDB SQL
while staying inside the lazy pipeline.
"""

from __future__ import annotations

import duckdb

from ._sql import register_relation
from .engine import get_connection, is_relation

# Placeholder token used in SQL templates
_DATA_PLACEHOLDER = "{data}"
_DATA_VIEW = "_sus_sql_data_view"


class _SusRelation:
    """Thin wrapper around DuckDBPyRelation that adds ``.pipe()`` support.

    DuckDB relations do not expose a ``.pipe()`` method. This wrapper
    delegates all attribute access to the underlying relation while
    adding the pandas-compatible ``.pipe(func, *args, **kwargs)``
    pattern used by the lazy pipeline.
    """

    def __init__(self, rel: duckdb.DuckDBPyRelation) -> None:
        object.__setattr__(self, "_rel", rel)

    def pipe(
        self,
        func,  # type: ignore[type-arg]
        *args,
        **kwargs,
    ) -> "_SusRelation":
        """Call ``func(self._rel, *args, **kwargs)`` — pandas-style pipe.

        The underlying ``DuckDBPyRelation`` (not the wrapper) is passed
        as the first argument so that ``is_relation()`` returns ``True``
        inside the called function.
        """
        return func(object.__getattribute__(self, "_rel"), *args, **kwargs)

    def __getattr__(self, name: str):  # type: ignore[override]
        return getattr(object.__getattribute__(self, "_rel"), name)

    def __repr__(self) -> str:
        return repr(object.__getattribute__(self, "_rel"))


def sus_sql(
    sql_or_rel: str | duckdb.DuckDBPyRelation,
    sql_template: str | None = None,
) -> _SusRelation:
    """Execute arbitrary DuckDB SQL, optionally over an existing relation.

    Two calling modes:

    1. **Direct** — ``sus_sql("SELECT 1 AS x")``
       Executes *sql_or_rel* as a raw SQL string and returns the result
       as a lazy DuckDB relation wrapped in ``_SusRelation`` (which
       exposes ``.pipe()``).

    2. **Pipe** — ``rel.pipe(sus_sql, "SELECT … FROM {data}")``
       When called via ``.pipe()``, the upstream relation arrives as the
       first positional argument and the SQL template as the second.
       The ``{data}`` placeholder is replaced by a registered view of the
       upstream relation before execution.

    Args:
        sql_or_rel: Either a SQL string (direct mode) or a DuckDB relation
            (pipe mode).
        sql_template: SQL template containing ``{data}`` as the reference
            to the upstream relation. Only used in pipe mode.

    Returns:
        ``_SusRelation`` wrapping a lazy DuckDB relation.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_sql("SELECT 1 AS value UNION ALL SELECT 2 AS value")
        >>> out = rel.pipe(cs.sus_sql, "SELECT SUM(value) AS total FROM {data}")
        >>> out.fetchone()[0]
        3
    """
    conn = get_connection()

    if sql_template is None:
        # Direct mode: sus_sql("SELECT …")
        if not isinstance(sql_or_rel, str):
            raise TypeError(
                "In direct mode sus_sql() expects a SQL string as its first argument."
            )
        return _SusRelation(conn.sql(sql_or_rel))

    # Pipe mode: sus_sql(rel, "SELECT … FROM {data}")
    rel = sql_or_rel
    if not is_relation(rel):
        raise TypeError(
            "In pipe mode the first argument must be a DuckDBPyRelation."
        )
    register_relation(conn, rel, _DATA_VIEW)
    sql = sql_template.replace(_DATA_PLACEHOLDER, _DATA_VIEW)
    return _SusRelation(conn.sql(sql))

