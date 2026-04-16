"""Tests for decode_age_sql utility and data.py safety checks."""

import pytest

from climasus.core.engine import get_connection
from climasus.utils.data import decode_age_sql, update_climasus_data


class TestDecodeAgeSql:
    """Validate the decode_age_sql expression via DuckDB execution."""

    def _decode(self, value: str) -> int | None:
        conn = get_connection()
        expr = decode_age_sql("age")
        row = conn.sql(f"SELECT {expr} AS decoded FROM (SELECT '{value}' AS age)").fetchone()
        return row[0]

    def test_code_4_years(self):
        assert self._decode("420") == 20
        assert self._decode("401") == 1
        assert self._decode("499") == 99

    def test_code_5_centenarian(self):
        assert self._decode("500") == 100
        assert self._decode("520") == 120

    def test_code_3_months_is_zero(self):
        assert self._decode("301") == 0
        assert self._decode("311") == 0

    def test_code_2_days_is_zero(self):
        assert self._decode("201") == 0

    def test_code_1_hours_is_zero(self):
        assert self._decode("112") == 0

    def test_code_0_minutes_is_zero(self):
        assert self._decode("030") == 0

    def test_plain_integer_fallback(self):
        """Non-coded values should be cast as integer."""
        assert self._decode("25") == 25
        assert self._decode("0") == 0

    def test_empty_string_returns_null(self):
        assert self._decode("") is None


class TestUpdateClimaSUSDataSafety:
    def test_rmtree_rejects_non_climasus_dir(self, tmp_path):
        """rmtree should refuse to delete a directory without manifest.json."""
        target = tmp_path / "not_climasus"
        target.mkdir()
        (target / "some_file.txt").write_text("important data")

        with pytest.raises(RuntimeError, match="manifest.json"):
            update_climasus_data(target_dir=str(target))


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
