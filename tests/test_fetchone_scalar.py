"""P3 Sprint 2 — fetchone_scalar helper: guard against empty datasets."""

import pandas as pd

from climasus4py.core._sql import fetchone_scalar
from climasus4py.core.engine import get_connection


def _conn():
    return get_connection()


class TestFetchoneScalar:
    def test_returns_value_when_row_exists(self):
        """Returns first column of first row when relation has data."""
        conn = _conn()
        rel = conn.sql("SELECT 42 AS val")
        assert fetchone_scalar(rel) == 42

    def test_returns_fallback_when_empty(self):
        """Returns fallback=0 (default) when relation is empty."""
        conn = _conn()
        rel = conn.sql("SELECT 1 AS val WHERE 1=0")  # empty
        assert fetchone_scalar(rel) == 0

    def test_custom_fallback(self):
        """Accepts a custom fallback value."""
        conn = _conn()
        rel = conn.sql("SELECT 1 AS val WHERE 1=0")
        assert fetchone_scalar(rel, fallback=-1) == -1

    def test_count_on_empty_relation(self):
        """count(*) on empty df returns 0 via fetchone_scalar."""
        conn = _conn()
        df = pd.DataFrame({"x": pd.Series([], dtype="int64")})
        rel = conn.from_df(df)
        result = fetchone_scalar(rel.aggregate("count(*)"), fallback=0)
        assert result == 0

    def test_count_on_non_empty_relation(self):
        """count(*) on populated df returns correct count."""
        conn = _conn()
        df = pd.DataFrame({"x": [1, 2, 3]})
        rel = conn.from_df(df)
        result = fetchone_scalar(rel.aggregate("count(*)"), fallback=0)
        assert result == 3

    def test_none_fallback_allowed(self):
        """fallback=None is a valid sentinel for optional scalars."""
        conn = _conn()
        rel = conn.sql("SELECT 1 WHERE 1=0")
        assert fetchone_scalar(rel, fallback=None) is None
