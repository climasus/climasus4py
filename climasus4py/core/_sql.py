"""SQL safety helpers — prevent SQL injection in dynamic queries.

All user-controlled values that appear in SQL must go through these
helpers before being embedded in query strings.
"""

from __future__ import annotations

import re

import duckdb

# Regex for valid SQL identifiers (view/table names used in register)
_VALID_IDENT_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def quote_ident(name: str) -> str:
    """Return *name* safely quoted as a SQL identifier.

    Doubles any embedded double-quote characters and wraps the result
    in double quotes, following ANSI SQL identifier quoting rules.

    Args:
        name: Raw identifier string (column name, table name, etc.).

    Returns:
        Double-quoted identifier safe to embed in SQL.

    Example:
        >>> quote_ident('CAUSABAS')
        '"CAUSABAS"'
        >>> quote_ident('col"name')
        '"col""name"'
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def sql_string(val: str) -> str:
    """Return *val* safely quoted as a SQL string literal.

    Escapes embedded single-quote characters and wraps the result in
    single quotes.

    Args:
        val: Raw string value to embed as a SQL literal.

    Returns:
        Single-quoted string literal safe to embed in SQL.

    Example:
        >>> sql_string("O'Brien")
        "'O''Brien'"
    """
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def register_relation(
    conn: duckdb.DuckDBPyConnection,
    rel: duckdb.DuckDBPyRelation,
    name: str,
) -> None:
    """Register *rel* as a named view in *conn* after validating *name*.

    Args:
        conn: Active DuckDB connection.
        rel: Relation to register.
        name: View name — must match ``[a-zA-Z_][a-zA-Z0-9_]*``.

    Raises:
        ValueError: If *name* contains characters outside the allowed
            identifier pattern (prevents SQL injection via view names).
    """
    if not _VALID_IDENT_RE.match(name):
        raise ValueError(
            f"Invalid relation name {name!r}. "
            "Name must match [a-zA-Z_][a-zA-Z0-9_]*"
        )
    conn.register(name, rel)


def fetchone_scalar(rel: duckdb.DuckDBPyRelation, fallback: object = 0) -> object:
    """Return the first column of the first row, or *fallback* if empty.

    Avoids ``TypeError`` when ``fetchone()`` returns ``None`` on an
    empty dataset.

    Args:
        rel: DuckDB relation (typically a COUNT or scalar query).
        fallback: Value to return when the relation is empty.

    Returns:
        ``row[0]`` if a row exists, otherwise *fallback*.
    """
    row = rel.fetchone()
    return row[0] if row is not None else fallback
