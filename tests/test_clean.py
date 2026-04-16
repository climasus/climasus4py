"""Tests for sus_clean — deduplication, age validation, encoding.

Focuses on:
  - ROW_NUMBER dedup with deterministic ORDER BY
  - IDADE decoding + age_range filtering
  - Fallback to distinct when no key column
"""

import pandas as pd
import pytest

from climasus.core.engine import get_connection
from climasus.core.clean import sus_clean


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


def _count(rel) -> int:
    return rel.count("*").fetchone()[0]


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDedup:
    def test_dedup_removes_duplicates(self):
        """Duplicate rows by key column should be removed."""
        rel = _make_rel({
            "CONTADOR": [1, 1, 2, 3, 3, 3],
            "IDADE": ["420", "420", "430", "440", "440", "440"],
            "CAUSABAS": ["J189"] * 6,
        })
        cleaned = sus_clean(rel, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_dedup_keeps_first_row(self):
        """Dedup should keep one row per key."""
        rel = _make_rel({
            "CONTADOR": [1, 1],
            "IDADE": ["420", "430"],
            "CAUSABAS": ["J189", "I219"],
        })
        cleaned = sus_clean(rel, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 1

    def test_dedup_no_key_uses_distinct(self):
        """When no key column found, fall back to full distinct."""
        rel = _make_rel({
            "VALOR": [1, 1, 2, 3],
            "TEXTO": ["a", "a", "b", "c"],
        })
        cleaned = sus_clean(rel, dedup=True, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_dedup_disabled(self):
        """dedup=False should keep all rows."""
        rel = _make_rel({
            "CONTADOR": [1, 1, 2],
            "IDADE": ["420", "420", "430"],
        })
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_dedup_custom_cols(self):
        """Custom dedup columns."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "CAUSABAS": ["J189", "J189", "I219"],
            "IDADE": ["420", "430", "440"],
        })
        cleaned = sus_clean(rel, dedup_cols=["CAUSABAS"], fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 2  # J189 deduped, I219 kept


# ---------------------------------------------------------------------------
# Age validation with IDADE decoding
# ---------------------------------------------------------------------------

class TestAgeValidation:
    def test_default_range(self):
        """Default range (0-120) keeps valid ages."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3, 4],
            "IDADE": ["420", "500", "540", "301"],
            # decoded:   20,  100,  140,    0
        })
        cleaned = sus_clean(rel, dedup=False, fix_enc=False)
        # 140 filtered out
        assert _count(cleaned) == 3

    def test_custom_range(self):
        """Custom age_range filters accordingly."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3, 4],
            "IDADE": ["410", "430", "460", "490"],
            # decoded:   10,   30,   60,   90
        })
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(20, 70))
        # 10 and 90 filtered out
        assert _count(cleaned) == 2

    def test_null_ages_kept(self):
        """Rows with NULL/empty age should be kept (not filtered)."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "IDADE": ["420", None, ""],
        })
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        # 420→20 (kept), None→NULL (kept), ""→NULL (kept)
        assert _count(cleaned) == 3

    def test_no_age_column_skips_validation(self):
        """Without an age column, no filtering happens."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "VALOR": ["a", "b", "c"],
        })
        cleaned = sus_clean(rel, dedup=False, fix_enc=False)
        assert _count(cleaned) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
