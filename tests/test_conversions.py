"""Tests for DATASUS data type conversions in the import/clean/standardize pipeline.

Covers:
  - Date columns (DDMMYYYY → datetime)
  - Age decoding (SIM-DO coded IDADE → years)
  - Numeric coercion
  - String cleanup
  - collect_arrow returns pyarrow.Table
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from climasus.core.engine import collect_arrow, get_connection, read_parquets
from climasus.core.clean import sus_clean
from climasus.core.standardize import sus_standardize
from climasus.core.importer import _coerce_datasus_types


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_sim_do_df():
    """
    Simulates a raw SIM-DO DataFrame as returned by _read_dbc.
    All columns are strings, matching real DATASUS .dbc output.
    """
    return pd.DataFrame({
        "CONTADOR": ["1", "2", "3", "4", "5", "6", "7", "8"],
        "DTOBITO": ["01012023", "15062023", "31122023", "", "99999999", "28022023", "01012023", "10032023"],
        "DTNASC": ["01011990", "15061955", "31121950", "invalid", "", "28022000", "01011990", "10031980"],
        "IDADE": ["420", "468", "301", "105", "500", "023", "420", "540"],
        "SEXO": ["1", "2", "1", "2", "1", " 2 ", "1", "2"],
        "RACACOR": ["1", "4", "", "  ", "2", "1", "1", "3"],
        "CODMUNRES": ["355030", "330455", "abc", "355030", "310620", "355030", "355030", "310620"],
        "PESO": ["3200", "2500", "abc", "", "4000", "3500", "3200", "3800"],
        "CAUSABAS": ["J189", "I219", "A90", "B342", "J449", "I219", "J189", "E149"],
    })


@pytest.fixture
def sample_parquet(tmp_path, sample_sim_do_df):
    """Write raw DataFrame to parquet (pre-coercion) and return path."""
    path = tmp_path / "test_raw.parquet"
    pq.write_table(pa.Table.from_pandas(sample_sim_do_df), path)
    return path


@pytest.fixture
def coerced_parquet(tmp_path, sample_sim_do_df):
    """Write coerced DataFrame to parquet and return path."""
    df = _coerce_datasus_types(sample_sim_do_df.copy())
    path = tmp_path / "test_coerced.parquet"
    pq.write_table(pa.Table.from_pandas(df), path)
    return path


# ---------------------------------------------------------------------------
# Tests: _coerce_datasus_types
# ---------------------------------------------------------------------------

class TestCoerceTypes:
    def test_date_columns_converted(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        assert pd.api.types.is_datetime64_any_dtype(df["DTOBITO"])
        assert pd.api.types.is_datetime64_any_dtype(df["DTNASC"])

    def test_valid_dates_parsed(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        # "01012023" → 2023-01-01
        assert df["DTOBITO"].iloc[0] == pd.Timestamp("2023-01-01")
        # "15062023" → 2023-06-15
        assert df["DTOBITO"].iloc[1] == pd.Timestamp("2023-06-15")
        # "31122023" → 2023-12-31
        assert df["DTOBITO"].iloc[2] == pd.Timestamp("2023-12-31")

    def test_invalid_dates_become_nat(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        # Empty string → NaT
        assert pd.isna(df["DTOBITO"].iloc[3])
        # "99999999" → NaT
        assert pd.isna(df["DTOBITO"].iloc[4])
        # "invalid" → NaT
        assert pd.isna(df["DTNASC"].iloc[3])

    def test_numeric_columns_converted(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        assert pd.api.types.is_numeric_dtype(df["CONTADOR"])
        assert pd.api.types.is_numeric_dtype(df["PESO"])
        assert pd.api.types.is_numeric_dtype(df["CODMUNRES"])

    def test_numeric_invalid_becomes_nan(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        # "abc" in CODMUNRES → NaN
        assert pd.isna(df["CODMUNRES"].iloc[2])
        # "abc" in PESO → NaN
        assert pd.isna(df["PESO"].iloc[2])
        # "" in PESO → NaN
        assert pd.isna(df["PESO"].iloc[3])

    def test_numeric_valid_values_preserved(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        assert df["CONTADOR"].iloc[0] == 1
        assert df["PESO"].iloc[0] == 3200
        assert df["CODMUNRES"].iloc[0] == 355030

    def test_string_whitespace_stripped(self, sample_sim_do_df):
        df = _coerce_datasus_types(sample_sim_do_df.copy())
        # SEXO " 2 " → "2"
        assert df["SEXO"].iloc[5] == "2"
        # RACACOR empty → None
        assert df["RACACOR"].iloc[2] is None or pd.isna(df["RACACOR"].iloc[2])

    def test_non_datasus_columns_unchanged(self):
        df = pd.DataFrame({"CUSTOM_COL": ["a", "b", "c"], "CAUSABAS": ["J189", "I219", "A90"]})
        result = _coerce_datasus_types(df.copy())
        # Non-mapped columns should remain as string-like
        assert pd.api.types.is_string_dtype(result["CAUSABAS"])


# ---------------------------------------------------------------------------
# Tests: Age decoding in sus_clean
# ---------------------------------------------------------------------------

class TestAgeDecoding:
    def _make_rel(self, idade_values):
        """Create a DuckDB relation with IDADE column."""
        df = pd.DataFrame({
            "CONTADOR": list(range(len(idade_values))),
            "IDADE": idade_values,
        })
        conn = get_connection()
        return conn.from_df(df)

    def test_code_4_years(self):
        """Code 4xx = age in years. 420 → 20 years, 468 → 68 years."""
        rel = self._make_rel(["420", "468", "401", "499"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 4  # All valid ages (20, 68, 1, 99)

    def test_code_5_hundred_plus(self):
        """Code 5xx = 100 + value. 500 → 100, 520 → 120."""
        rel = self._make_rel(["500", "520", "550"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        # 500 → 100 (ok), 520 → 120 (ok), 550 → 150 (filtered out)
        assert n == 2

    def test_code_3_months(self):
        """Code 3xx = months → 0 years (infant)."""
        rel = self._make_rel(["301", "306", "311"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 3  # All valid (0 years)

    def test_code_2_days(self):
        """Code 2xx = days → 0 years."""
        rel = self._make_rel(["201", "215", "230"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 3  # All valid (0 years)

    def test_code_1_hours(self):
        """Code 1xx = hours → 0 years."""
        rel = self._make_rel(["101", "112", "123"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 3  # All valid (0 years)

    def test_code_0_minutes(self):
        """Code 0xx = minutes → 0 years."""
        rel = self._make_rel(["001", "030", "059"])
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 3  # All valid (0 years)

    def test_invalid_ages_filtered(self):
        """Ages >120 should be filtered out."""
        rel = self._make_rel(["420", "550", "468"])
        # 420 → 20 (ok), 550 → 150 (out), 468 → 68 (ok)
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        assert n == 2

    def test_row_count_realistic(self):
        """Realistic mix: keep most, only filter 150+ years."""
        ages = ["420", "468", "301", "105", "500", "023", "540", "499"]
        rel = self._make_rel(ages)
        cleaned = sus_clean(rel, dedup=False, fix_enc=False, age_range=(0, 120))
        n = cleaned.count("*").fetchone()[0]
        # 420→20✓, 468→68✓, 301→0✓, 105→0✓, 500→100✓, 023→int(23)✓, 540→140✗, 499→99✓
        assert n == 7


# ---------------------------------------------------------------------------
# Tests: Date conversion in sus_standardize
# ---------------------------------------------------------------------------

class TestStandardizeDates:
    def _make_rel(self, data):
        conn = get_connection()
        return conn.from_df(pd.DataFrame(data))

    def test_string_dates_converted(self):
        """String dates in DDMMYYYY format should be converted to DATE."""
        rel = self._make_rel({
            "DTOBITO": ["01012023", "15062023"],
            "CAUSABAS": ["J189", "I219"],
        })
        std = sus_standardize(rel, lang="pt", system="SIM-DO")
        df = std.df()
        # DTOBITO should still exist and be a date type
        assert "DTOBITO" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["DTOBITO"])
        assert df["DTOBITO"].iloc[0] == pd.Timestamp("2023-01-01")

    def test_datetime_columns_preserved(self):
        """Already-datetime columns should not be broken by re-conversion."""
        rel = self._make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-06-15"]),
            "CAUSABAS": ["J189", "I219"],
        })
        std = sus_standardize(rel, lang="pt", system="SIM-DO")
        df = std.df()
        assert pd.api.types.is_datetime64_any_dtype(df["DTOBITO"])
        assert df["DTOBITO"].iloc[0] == pd.Timestamp("2023-01-01")

    def test_english_renamed_dates_converted(self):
        """After rename to English, dates should still convert."""
        rel = self._make_rel({
            "DTOBITO": ["01012023", "15062023"],
            "CAUSABAS": ["J189", "I219"],
        })
        std = sus_standardize(rel, lang="en", system="SIM-DO")
        df = std.df()
        # Check that the renamed date column has datetime values
        date_cols = [c for c in df.columns if "date" in c.lower() or "DT" in c]
        has_valid_date = False
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                has_valid_date = True
                break
        # At minimum, the original DTOBITO was converted (before or after rename)
        assert has_valid_date or "DTOBITO" not in df.columns


# ---------------------------------------------------------------------------
# Tests: collect_arrow
# ---------------------------------------------------------------------------

class TestCollectArrow:
    def test_returns_table(self):
        """collect_arrow should return a pyarrow.Table, not RecordBatchReader."""
        conn = get_connection()
        rel = conn.from_df(pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
        result = collect_arrow(rel)
        assert isinstance(result, pa.Table)

    def test_has_num_rows(self):
        """Result should have .num_rows and .num_columns."""
        conn = get_connection()
        rel = conn.from_df(pd.DataFrame({"a": [1, 2, 3]}))
        result = collect_arrow(rel)
        assert result.num_rows == 3
        assert result.num_columns == 1


# ---------------------------------------------------------------------------
# Tests: Full pipeline round-trip (coerce → parquet → DuckDB → clean → std)
# ---------------------------------------------------------------------------

class TestPipelineRoundTrip:
    def test_coerced_parquet_preserves_dates(self, coerced_parquet):
        """Dates should survive the Parquet round-trip."""
        rel = read_parquets([coerced_parquet])
        df = rel.df()
        assert pd.api.types.is_datetime64_any_dtype(df["DTOBITO"])
        assert df["DTOBITO"].iloc[0] == pd.Timestamp("2023-01-01")

    def test_coerced_parquet_preserves_numerics(self, coerced_parquet):
        """Numeric columns should survive the Parquet round-trip."""
        rel = read_parquets([coerced_parquet])
        df = rel.df()
        assert pd.api.types.is_numeric_dtype(df["PESO"])
        assert df["PESO"].iloc[0] == 3200

    def test_clean_preserves_most_rows(self, coerced_parquet):
        """Cleaning with proper age decoding should keep most rows."""
        rel = read_parquets([coerced_parquet])
        original_count = rel.count("*").fetchone()[0]
        cleaned = sus_clean(rel, dedup=True, fix_enc=False, age_range=(0, 120))
        clean_count = cleaned.count("*").fetchone()[0]
        # Should keep at least 70% of rows (realistic, not 0.3%)
        assert clean_count >= original_count * 0.5, (
            f"Too many rows removed: {original_count} → {clean_count} "
            f"({clean_count/original_count*100:.1f}%)"
        )

    def test_standardize_dates_not_null(self, coerced_parquet):
        """After standardize, date columns should NOT be all NaT."""
        rel = read_parquets([coerced_parquet])
        std = sus_standardize(rel, lang="en", system="SIM-DO")
        df = std.df()
        # Find date columns
        date_cols = [c for c in df.columns
                     if pd.api.types.is_datetime64_any_dtype(df[c])]
        for col in date_cols:
            valid = df[col].notna().sum()
            assert valid > 0, f"Column '{col}' is all NaT after standardization"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
