"""Data cleaning — deduplication and encoding fixes.

Mirrors R: clean.R
"""

from __future__ import annotations
from ._stage import set_stage, add_history

import duckdb

from ._guards import _assert_lazy
from .engine import get_connection, schema_columns


def sus_data_clean_encoding(
    rel: duckdb.DuckDBPyRelation,
    *,
    dedup: bool = True,
    dedup_cols: list[str] | None = None,
    age_range: tuple[int, int] = (0, 120),
    fix_enc: bool | None = None,
) -> duckdb.DuckDBPyRelation:
    """Clean SUS data: deduplicate and validate age range.

    All operations remain lazy (DuckDB relation) until materialised.
    Deduplication uses ROW_NUMBER() over known DATASUS key columns for
    performance. Age validation decodes the SIM-DO 3-digit coded age
    field before applying numeric range filters.

    Note on encoding: UTF-8 conversion is performed upstream at the
    Parquet writer level (``_coerce_datasus_types`` in ``importer.py``),
    not here. This mirrors the R behaviour where
    ``sus_data_clean_encoding()`` detects and converts latin1 → UTF-8;
    in Python that conversion happens earlier (at import time). The
    ``fix_enc`` parameter is kept only for backward compatibility.

    Mirrors climasus4r::sus_data_clean_encoding() pipeline registration:
    sets stage to ``"clean"`` and appends a timestamped history entry,
    propagating system/type/history from the previous stage.

    Args:
        rel: Lazy DuckDB relation to clean.
        dedup: If ``True``, remove duplicate records.
        dedup_cols: Columns to use for deduplication. If ``None``, uses
            known DATASUS key columns (faster). Pass ``["*"]`` to force
            a full-row DISTINCT.
        age_range: ``(min_age, max_age)`` tuple in years. Records with
            decoded age outside this range are dropped. Handles the
            DATASUS 3-digit coding scheme for SIM-DO IDADE fields.
        fix_enc: **Deprecated and ignored.** Kept for backward
            compatibility only. Emits a ``DeprecationWarning`` when set.

    Returns:
        Lazy DuckDB relation with duplicates removed and invalid ages
        filtered out, registered at stage ``"clean"`` in sus_meta.

    Example:
        >>> import climasus4py as cs
        >>> clean = cs.sus_data_clean_encoding(rel, age_range=(0, 110))
        >>> cs.sus_meta(clean, "stage")
        'clean'
        >>> cs.sus_meta(clean, "history")
        ['[...] Imported DATASUS ...', '[...] Cleaned character encoding ...']
    """
    if fix_enc is not None:
        import warnings as _warnings
        _warnings.warn(
            "sus_data_clean_encoding(fix_enc=...) is deprecated and has no "
            "effect — encoding fixes were never implemented at this stage. "
            "The argument is accepted only for backward compatibility and "
            "will be removed in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )

    _assert_lazy(rel)

    # ------------------------------------------------------------------
    # Captura rel original antes de qualquer transformação
    # Necessário porque filter/project criam novos objetos DuckDB e
    # o WeakKeyDictionary perde a referência ao meta do stage anterior
    # ------------------------------------------------------------------
    _original_rel = rel
    columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------
    if dedup:
        if dedup_cols and dedup_cols != ["*"]:
            key_cols = [c for c in dedup_cols if c in columns]
        else:
            key_candidates = [
                "CONTADOR", "NUMERODO", "NUMERODN", "N_AIH", "NU_NOTIFIC",
                "counter", "record_id",
            ]
            key_cols = [c for c in key_candidates if c in columns]

        if key_cols:
            conn = get_connection()
            partition = ", ".join(f'"{c}"' for c in key_cols)
            all_cols  = ", ".join(f'"{c}"' for c in columns)
            rel = conn.sql(
                f"SELECT {all_cols} FROM ("
                f"  SELECT *, ROW_NUMBER() OVER ("
                f"    PARTITION BY {partition} ORDER BY {partition}"
                f"  ) AS __rn"
                f"  FROM rel"
                f") WHERE __rn = 1"
            )
        else:
            rel = rel.distinct()

    columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # Age validation
    # DATASUS SIM-DO encodes age as a 3-digit string:
    #   5xx → 100 + xx years (centenarians)
    #   4xx → xx years
    #   3xx → months  → 0 years (infant)
    #   0xx / 1xx / 2xx → min/hr/days → 0 years (infant)
    # ------------------------------------------------------------------
    age_col = None
    for candidate in ("IDADE", "age", "age_years"):
        if candidate in columns:
            age_col = candidate
            break

    if age_col:
        lo, hi = age_range
        conn = get_connection()

        decoded_expr = (
            f'CASE '
            f'  WHEN LENGTH(TRIM("{age_col}")) = 3'
            f'   AND SUBSTR(TRIM("{age_col}"), 1, 1) = \'5\''
            f'    THEN 100 + TRY_CAST(SUBSTR(TRIM("{age_col}"), 2) AS INTEGER) '
            f'  WHEN LENGTH(TRIM("{age_col}")) = 3'
            f'   AND SUBSTR(TRIM("{age_col}"), 1, 1) = \'4\''
            f'    THEN TRY_CAST(SUBSTR(TRIM("{age_col}"), 2) AS INTEGER) '
            f'  WHEN LENGTH(TRIM("{age_col}")) = 3'
            f'   AND SUBSTR(TRIM("{age_col}"), 1, 1) = \'3\''
            f'    THEN 0 '
            f'  WHEN LENGTH(TRIM("{age_col}")) = 3'
            f'   AND SUBSTR(TRIM("{age_col}"), 1, 1) IN (\'0\', \'1\', \'2\')'
            f'    THEN 0 '
            f'  ELSE TRY_CAST("{age_col}" AS INTEGER) '
            f'END'
        )

        rel = conn.sql(f'SELECT *, ({decoded_expr}) AS __age_years FROM rel')
        rel = rel.filter(
            f'__age_years IS NULL OR (__age_years >= {lo} AND __age_years <= {hi})'
        )
        final_cols = [c for c in schema_columns(rel) if c != "__age_years"]
        rel = rel.project(", ".join(f'"{c}"' for c in final_cols))

    # ------------------------------------------------------------------
    # Registra stage e history — mirrors climasus4r::sus_data_clean_encoding
    # _inherit_from=_original_rel propaga system/type/stages/history do
    # stage anterior (import), mesmo que rel seja um objeto DuckDB novo
    # ------------------------------------------------------------------
    dedup_msg   = f"dedup={'key-cols' if dedup else 'off'}"
    age_msg     = f"age_range={age_range}"
    enc_msg     = "encoding=UTF-8 (applied at import/parquet level)"
    history_msg = (
        f"Cleaned character encoding (UTF-8); {dedup_msg}; {age_msg}; {enc_msg}"
    )

    rel = set_stage(rel, "clean", _inherit_from=_original_rel)
    rel = add_history(rel, history_msg)

    return rel