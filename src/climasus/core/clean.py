"""Data cleaning — deduplication and encoding fixes.

Mirrors R: clean.R
"""

from __future__ import annotations

import duckdb

from climasus.core.engine import get_connection, schema_columns
from climasus.utils.encoding import fix_encoding


def sus_clean(
    rel: duckdb.DuckDBPyRelation,
    *,
    fix_enc: bool = True,
    dedup: bool = True,
    dedup_cols: list[str] | None = None,
    age_range: tuple[int, int] = (0, 120),
) -> duckdb.DuckDBPyRelation:
    """Clean SUS data: deduplicate, fix encoding, validate age range.

    All operations stay lazy (DuckDB relation) until materialized.

    Parameters
    ----------
    dedup_cols : Columns for dedup. If None, uses key columns only
        (faster than full-row distinct). Pass ["*"] for full distinct.
    """
    columns = schema_columns(rel)

    if dedup:
        if dedup_cols and dedup_cols != ["*"]:
            key_cols = [c for c in dedup_cols if c in columns]
        else:
            # Use key columns for SUS data instead of full-row distinct
            key_candidates = [
                "CONTADOR", "NUMERODO", "NUMERODN", "N_AIH", "NU_NOTIFIC",
                "counter", "record_id",
            ]
            key_cols = [c for c in key_candidates if c in columns]

        if key_cols:
            # ROW_NUMBER() dedup — keeps first row per key, much faster
            conn = get_connection()
            partition = ", ".join(f'"{c}"' for c in key_cols)
            all_cols = ", ".join(f'"{c}"' for c in columns)
            rel = conn.sql(
                f"SELECT {all_cols} FROM ("
                f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition}) AS __rn"
                f"  FROM rel"
                f") WHERE __rn = 1"
            )
        else:
            # No key column found — fall back to full distinct
            rel = rel.distinct()

    columns = schema_columns(rel)

    # Age validation
    age_col = None
    for candidate in ("IDADE", "age", "age_years"):
        if candidate in columns:
            age_col = candidate
            break

    if age_col:
        lo, hi = age_range
        rel = rel.filter(
            f'TRY_CAST("{age_col}" AS INTEGER) IS NULL '
            f'OR (TRY_CAST("{age_col}" AS INTEGER) >= {lo} '
            f'AND TRY_CAST("{age_col}" AS INTEGER) <= {hi})'
        )

    # Note: encoding fixes for string columns require materialization
    # or a UDF. For now, we mark that encoding fix should happen at
    # standardization time when we collect column subsets.

    return rel
