"""Tests for sus_data_clean_encoding — deduplication, age validation, encoding.

Focuses on:
  - ROW_NUMBER dedup with deterministic ORDER BY
  - IDADE decoding + age_range filtering
  - Fallback to distinct when no key column
"""

import pandas as pd
import pytest

from climasus4py.core.clean import sus_data_clean_encoding
from climasus4py.core.engine import get_connection


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
        cleaned = sus_data_clean_encoding(rel, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_same_contador_different_content_are_different_records(self):
        """CONTADOR is not a key (M242).

        It numbers the rows of each yearly SIM file and restarts at 1, so the
        same value in two years is two deaths. Deduplicating on it took
        SIM-DO SP 2014-2019 from 1,764,884 deaths to 745,026.
        """
        rel = _make_rel({
            "CONTADOR": [1, 1, 254, 254],
            "DTOBITO": ["01012014", "01012015", "03022014", "07032015"],
            "IDADE": ["420", "430", "470", "481"],
            "CAUSABAS": ["J189", "I219", "C509", "I219"],
        })
        cleaned = sus_data_clean_encoding(rel, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 4

    def test_sih_aih_in_two_competence_months_is_kept(self):
        """N_AIH repeats across competence months (-114,505 on SIH-RD SP)."""
        rel = _make_rel({
            "N_AIH": ["3514100000001", "3514100000001"],
            "ANO_CMPT": ["2014", "2014"],
            "MES_CMPT": ["06", "07"],
        })
        assert _count(sus_data_clean_encoding(rel, fix_enc=False)) == 2

    def test_dedup_cols_still_defines_a_key(self):
        """A caller who knows the key can still pass it."""
        rel = _make_rel({
            "N_AIH": ["1", "1", "1"],
            "ANO_CMPT": ["2014", "2014", "2014"],
            "MES_CMPT": ["06", "06", "07"],
            "SEQUENCIA": ["1", "1", "1"],
            "VAL_TOT": [10.0, 11.0, 12.0],
        })
        cleaned = sus_data_clean_encoding(
            rel, fix_enc=False,
            dedup_cols=["N_AIH", "ANO_CMPT", "MES_CMPT", "SEQUENCIA"])
        assert _count(cleaned) == 2

    def test_dedup_no_key_uses_distinct(self):
        """When no key column found, fall back to full distinct."""
        rel = _make_rel({
            "VALOR": [1, 1, 2, 3],
            "TEXTO": ["a", "a", "b", "c"],
        })
        cleaned = sus_data_clean_encoding(rel, dedup=True, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_dedup_disabled(self):
        """dedup=False should keep all rows."""
        rel = _make_rel({
            "CONTADOR": [1, 1, 2],
            "IDADE": ["420", "420", "430"],
        })
        cleaned = sus_data_clean_encoding(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        assert _count(cleaned) == 3

    def test_dedup_custom_cols(self):
        """Custom dedup columns."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "CAUSABAS": ["J189", "J189", "I219"],
            "IDADE": ["420", "430", "440"],
        })
        cleaned = sus_data_clean_encoding(rel, dedup_cols=["CAUSABAS"], fix_enc=False, age_range=(0, 120))  # noqa: E501
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
        cleaned = sus_data_clean_encoding(rel, dedup=False, fix_enc=False)
        # 140 filtered out
        assert _count(cleaned) == 3

    def test_custom_range(self):
        """Custom age_range filters accordingly."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3, 4],
            "IDADE": ["410", "430", "460", "490"],
            # decoded:   10,   30,   60,   90
        })
        cleaned = sus_data_clean_encoding(rel, dedup=False, fix_enc=False, age_range=(20, 70))
        # 10 and 90 filtered out
        assert _count(cleaned) == 2

    def test_null_ages_kept(self):
        """Rows with NULL/empty age should be kept (not filtered)."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "IDADE": ["420", None, ""],
        })
        cleaned = sus_data_clean_encoding(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        # 420→20 (kept), None→NULL (kept), ""→NULL (kept)
        assert _count(cleaned) == 3

    def test_no_age_column_skips_validation(self):
        """Without an age column, no filtering happens."""
        rel = _make_rel({
            "CONTADOR": [1, 2, 3],
            "VALOR": ["a", "b", "c"],
        })
        cleaned = sus_data_clean_encoding(rel, dedup=False, fix_enc=False)
        assert _count(cleaned) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
