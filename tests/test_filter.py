"""Tests for sus_filter — disease, age, sex, geography, and date filtering.

Focuses on:
  - CID-10 filtering (groups + raw codes)
  - IDADE decoding for age_min/age_max
  - Sex, race, UF, municipality filters
  - Date range filtering
"""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.core.filter import sus_filter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


def _count(rel) -> int:
    return rel.count("*").fetchone()[0]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sim_do_rel():
    """Minimal SIM-DO-like relation for filter tests."""
    return _make_rel({
        "CONTADOR": list(range(1, 9)),
        "DTOBITO": ["01012023", "15062023", "31122023", "01032023",
                     "15092023", "20112023", "01072023", "28022023"],
        "IDADE": ["420", "468", "301", "410", "450", "500", "435", "550"],
        # decoded: 20, 68, 0, 10, 50, 100, 35, 150
        "SEXO": ["1", "2", "1", "2", "1", "2", "1", "2"],
        "RACACOR": ["1", "4", "2", "1", "3", "1", "4", "2"],
        "CODMUNRES": ["355030", "330455", "310620", "355030",
                       "330455", "310620", "355030", "330455"],
        "CAUSABAS": ["J189", "I219", "A90", "B342", "J449", "C509", "E149", "I219"],
    })


# ---------------------------------------------------------------------------
# Age filtering with IDADE decoding
# ---------------------------------------------------------------------------

class TestAgeFilter:
    def test_age_min(self, sim_do_rel):
        """age_min=35 should keep ages >= 35: 68, 50, 100, 35, 150."""
        result = sus_filter(sim_do_rel, age_min=35)
        assert _count(result) == 5

    def test_age_max(self, sim_do_rel):
        """age_max=30 should keep ages <= 30: 20, 0, 10."""
        result = sus_filter(sim_do_rel, age_max=30)
        assert _count(result) == 3

    def test_age_range(self, sim_do_rel):
        """age_min=10, age_max=68 should keep: 20, 68, 10, 50, 35."""
        result = sus_filter(sim_do_rel, age_min=10, age_max=68)
        assert _count(result) == 5

    def test_age_filter_infant(self, sim_do_rel):
        """age_min=0, age_max=0 should keep only infants (code 3xx → 0): row 3."""
        result = sus_filter(sim_do_rel, age_min=0, age_max=0)
        assert _count(result) == 1

    def test_age_centenarian(self, sim_do_rel):
        """age_min=100 should keep 500→100 and 550→150."""
        result = sus_filter(sim_do_rel, age_min=100)
        assert _count(result) == 2

    def test_no_age_filter_keeps_all(self, sim_do_rel):
        """Without age filters, all rows kept."""
        result = sus_filter(sim_do_rel)
        assert _count(result) == 8


# ---------------------------------------------------------------------------
# Sex filtering
# ---------------------------------------------------------------------------

class TestSexFilter:
    def test_filter_male(self, sim_do_rel):
        result = sus_filter(sim_do_rel, sex="1")
        assert _count(result) == 4

    def test_filter_female(self, sim_do_rel):
        result = sus_filter(sim_do_rel, sex="2")
        assert _count(result) == 4

    def test_filter_male_canonical_letter(self, sim_do_rel):
        """Canonical 'M' must match rows stored as DATASUS '1' or 'Masculino'."""
        result = sus_filter(sim_do_rel, sex="M")
        assert _count(result) == 4

    def test_filter_female_portuguese_label(self, sim_do_rel):
        """'Feminino' should match the same rows as '2'."""
        result = sus_filter(sim_do_rel, sex="Feminino")
        assert _count(result) == 4

    def test_filter_female_english_label(self, sim_do_rel):
        result = sus_filter(sim_do_rel, sex="Female")
        assert _count(result) == 4

    def test_filter_female_spanish_label(self, sim_do_rel):
        result = sus_filter(sim_do_rel, sex="Femenino")
        assert _count(result) == 4

    def test_filter_sex_list(self, sim_do_rel):
        """List input keeps rows matching any of the values."""
        result = sus_filter(sim_do_rel, sex=["Male", "Female"])
        assert _count(result) == 8

    def test_filter_unrecognised_sex_raises(self, sim_do_rel):
        with pytest.raises(ValueError, match="Unrecognised sex value"):
            sus_filter(sim_do_rel, sex="X")


# ---------------------------------------------------------------------------
# Disease / CID filtering
# ---------------------------------------------------------------------------

class TestDiseaseFilter:
    def test_single_code(self, sim_do_rel):
        result = sus_filter(sim_do_rel, codes=["J189"])
        assert _count(result) == 1

    def test_multiple_codes(self, sim_do_rel):
        result = sus_filter(sim_do_rel, codes=["J189", "I219"])
        assert _count(result) == 3  # J189 + I219 (appears twice)

    def test_no_match_returns_zero(self, sim_do_rel):
        result = sus_filter(sim_do_rel, codes=["Z999"])
        assert _count(result) == 0


# ---------------------------------------------------------------------------
# Race filtering
# ---------------------------------------------------------------------------

class TestRaceFilter:
    def test_single_race(self, sim_do_rel):
        result = sus_filter(sim_do_rel, race="1")
        assert _count(result) == 3  # rows with RACACOR="1"

    def test_multiple_races(self, sim_do_rel):
        result = sus_filter(sim_do_rel, race=["1", "4"])
        assert _count(result) == 5


# ---------------------------------------------------------------------------
# UF filtering
# ---------------------------------------------------------------------------

class TestUFFilter:
    def test_filter_uf(self):
        """Filter by UF when column exists."""
        rel = _make_rel({
            "UF": ["SP", "RJ", "MG", "SP"],
            "DTOBITO": ["01012023"] * 4,
        })
        result = sus_filter(rel, uf="SP")
        assert _count(result) == 2


# ---------------------------------------------------------------------------
# Municipality filtering
# ---------------------------------------------------------------------------

class TestMunicipalityFilter:
    def test_filter_municipality(self, sim_do_rel):
        result = sus_filter(sim_do_rel, municipality="355030")
        assert _count(result) == 3


# ---------------------------------------------------------------------------
# Combined filters
# ---------------------------------------------------------------------------

class TestCombinedFilters:
    def test_age_and_sex(self, sim_do_rel):
        """age_min=20 + sex=1 should intersect both filters."""
        result = sus_filter(sim_do_rel, age_min=20, sex="1")
        # Male AND age>=20: 420→20(M), 450→50(M), 435→35(M) = 3
        assert _count(result) == 3

    def test_disease_and_age(self, sim_do_rel):
        """Filter by CID + age range."""
        result = sus_filter(sim_do_rel, codes=["I219"], age_max=70)
        # I219 rows: idx 1(68y) and idx 7(150y) → only idx 1 passes age≤70
        assert _count(result) == 1


# ---------------------------------------------------------------------------
# Disease group lookup (codes_for_groups branch — lines 81-82)
# ---------------------------------------------------------------------------

class TestGroupsFilter:
    def test_groups_string_resolves_codes(self, sim_do_rel):
        """groups='respiratory' should resolve to J* codes and filter accordingly."""
        result = sus_filter(sim_do_rel, groups="respiratory")
        df = result.df()
        # J189 and J449 are respiratory; others are not
        assert _count(result) == 2
        assert set(df["CAUSABAS"]) == {"J189", "J449"}

    def test_groups_list_resolves_codes(self, sim_do_rel):
        """groups=['respiratory'] (list form) must also work."""
        result = sus_filter(sim_do_rel, groups=["respiratory"])
        assert _count(result) == 2

    def test_no_cause_column_with_codes_raises(self):
        """When codes= is provided but there's no CID column, raise ValueError."""
        rel = _make_rel({"value": [1, 2, 3]})
        with pytest.raises(ValueError, match="cause/CID"):
            sus_filter(rel, codes=["J189"])

    def test_large_code_list_uses_temp_table(self, sim_do_rel):
        """With >200 unique 3-char prefixes the temp-table branch must execute."""
        big_codes = (
            [f"J{i:02d}0" for i in range(100)]
            + [f"I{i:02d}0" for i in range(100)]
            + [f"A{i:02d}0" for i in range(100)]
        )
        result = sus_filter(sim_do_rel, codes=big_codes)
        # J189 (J-prefix) and I219 (I-prefix) and A90 (A-prefix) match → 4 rows
        assert _count(result) >= 3


# ---------------------------------------------------------------------------
# Error raises for missing columns
# ---------------------------------------------------------------------------

class TestMissingColumnErrors:
    def test_age_filter_no_age_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="age"):
            sus_filter(rel, age_min=18)

    def test_sex_filter_no_sex_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="sex"):
            sus_filter(rel, sex="M")

    def test_race_filter_no_race_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="race"):
            sus_filter(rel, race="1")

    def test_uf_filter_no_uf_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="UF|uf"):
            sus_filter(rel, uf="SP")

    def test_municipality_filter_no_muni_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="municipality"):
            sus_filter(rel, municipality="355030")

    def test_date_filter_no_date_column_raises(self):
        rel = _make_rel({"value": [1, 2]})
        with pytest.raises(ValueError, match="date"):
            sus_filter(rel, date_start="2023-01-01")


# ---------------------------------------------------------------------------
# Date range filtering (lines 189-201)
# ---------------------------------------------------------------------------

@pytest.fixture
def date_rel():
    """Relation with ISO-format date column (death_date) for date filter tests."""
    return _make_rel({
        "death_date": [
            "2023-01-01", "2023-06-15", "2023-12-31", "2023-03-01",
            "2023-09-15", "2023-11-20", "2023-07-01", "2023-02-28",
        ],
        "value": list(range(8)),
    })


class TestDateFilter:
    def test_date_start_filters_early_rows(self, date_rel):
        """date_start removes rows before 2023-06-01."""
        result = sus_filter(date_rel, date_start="2023-06-01")
        # ≥ 2023-06-01: Jun-15, Dec-31, Sep-15, Nov-20, Jul-01 = 5
        assert _count(result) == 5

    def test_date_end_filters_late_rows(self, date_rel):
        """date_end removes rows after 2023-03-31."""
        result = sus_filter(date_rel, date_end="2023-03-31")
        # ≤ 2023-03-31: Jan-01, Mar-01, Feb-28 = 3
        assert _count(result) == 3

    def test_date_range_combined(self, date_rel):
        """date_start + date_end together narrow to a window."""
        result = sus_filter(date_rel, date_start="2023-06-01", date_end="2023-09-30")
        # Jun-15, Sep-15, Jul-01 → 3 rows
        assert _count(result) == 3


# ---------------------------------------------------------------------------
# P5 Sprint 2 — SQL injection regression tests (OWASP A03)
# ---------------------------------------------------------------------------

class TestSQLInjection:
    """Regression tests — each case verifies that user-controlled values
    cannot inject SQL through race/sex/uf/municipality parameters."""

    def test_sex_payload_blocked_by_synonym_expansion(self, sim_do_rel):
        """SQL injection via sex= raises ValueError — never reaches DB."""
        with pytest.raises(ValueError, match="Unrecognised sex value"):
            sus_filter(sim_do_rel, sex="M' OR '1'='1")

    def test_race_payload_returns_zero_not_all_rows(self, sim_do_rel):
        """Race injection payload must not return extra rows via RACACOR."""
        result = sus_filter(sim_do_rel, race=["4'; DROP TABLE x; --"])
        # sql_string() escapes the quote → no match, 0 rows
        assert _count(result) == 0

    def test_race_double_quote_payload_safe(self, sim_do_rel):
        """Double-quote in race value must not escape the identifier boundary."""
        result = sus_filter(sim_do_rel, race=['1" OR "1"="1'])
        assert _count(result) == 0

    def test_municipality_payload_returns_zero(self, sim_do_rel):
        """SQL injection via municipality= is neutralised by sql_string()."""
        result = sus_filter(sim_do_rel, municipality="355030' OR '1'='1")
        assert _count(result) == 0

    def test_uf_payload_returns_zero(self):
        """SQL injection via uf= is neutralised by sql_string()."""
        rel = _make_rel({
            "UF": ["SP", "RJ", "MG"],
            "DTOBITO": ["01012023"] * 3,
        })
        result = sus_filter(rel, uf="SP' OR '1'='1")
        assert _count(result) == 0


# ---------------------------------------------------------------------------
# D.3 — match_type (exact vs starts_with)
# ---------------------------------------------------------------------------

class TestMatchType:
    def test_starts_with_matches_subcode(self, sim_do_rel):
        """Default starts_with matches J189 when filtering by J18."""
        result = sus_filter(sim_do_rel, codes=["J18"])
        assert _count(result) == 1

    def test_exact_requires_full_code(self, sim_do_rel):
        """exact match on J18 must NOT match J189."""
        result = sus_filter(sim_do_rel, codes=["J18"], match_type="exact")
        assert _count(result) == 0

    def test_exact_matches_full_code(self, sim_do_rel):
        """exact match on J189 MUST match J189."""
        result = sus_filter(sim_do_rel, codes=["J189"], match_type="exact")
        assert _count(result) == 1

    def test_invalid_match_type_raises(self, sim_do_rel):
        with pytest.raises(ValueError, match="Invalid match_type"):
            sus_filter(sim_do_rel, codes=["J18"], match_type="regex")

    def test_exact_multi_code(self, sim_do_rel):
        """exact with multiple codes filters precisely."""
        result = sus_filter(sim_do_rel, codes=["J189", "I219"], match_type="exact")
        assert _count(result) == 3  # J189 x1, I219 x2


# ---------------------------------------------------------------------------
# D.3 — education filter
# ---------------------------------------------------------------------------

class TestEducationFilter:
    def test_education_filters_by_esc(self):
        rel = _make_rel({
            "ESC": ["1", "2", "3", "1", "9"],
            "CAUSABAS": ["J189"] * 5,
        })
        result = sus_filter(rel, education=["1", "2"])
        assert _count(result) == 3

    def test_education_single_value(self):
        rel = _make_rel({
            "education": ["1", "2", "3"],
            "CAUSABAS": ["J189"] * 3,
        })
        result = sus_filter(rel, education="1")
        assert _count(result) == 1

    def test_education_column_priority_education_over_esc(self):
        """When both 'education' and 'ESC' exist, 'education' takes priority."""
        rel = _make_rel({
            "education": ["1", "2", "3"],
            "ESC": ["9", "9", "9"],
            "CAUSABAS": ["J189"] * 3,
        })
        result = sus_filter(rel, education="1")
        assert _count(result) == 1  # filtered on 'education', not 'ESC'

    def test_education_no_column_raises(self, sim_do_rel):
        with pytest.raises(ValueError, match="No education column"):
            sus_filter(sim_do_rel, education="1")


# ---------------------------------------------------------------------------
# D.3 — drop_ignored filter
# ---------------------------------------------------------------------------

class TestDropIgnored:
    def test_drop_ignored_removes_9_in_sex(self):
        rel = _make_rel({
            "SEXO": ["1", "2", "9", "1"],
            "CAUSABAS": ["J189"] * 4,
        })
        result = sus_filter(rel, drop_ignored=True)
        assert _count(result) == 3

    def test_drop_ignored_removes_99_in_race(self):
        rel = _make_rel({
            "RACACOR": ["1", "4", "99", "2"],
            "CAUSABAS": ["J189"] * 4,
        })
        result = sus_filter(rel, drop_ignored=True)
        assert _count(result) == 3

    def test_drop_ignored_removes_ignorado(self):
        rel = _make_rel({
            "ESC": ["1", "Ignorado", "2"],
            "CAUSABAS": ["J189"] * 3,
        })
        result = sus_filter(rel, drop_ignored=True)
        assert _count(result) == 2

    def test_drop_ignored_false_keeps_all(self):
        rel = _make_rel({
            "SEXO": ["1", "9", "2"],
            "CAUSABAS": ["J189"] * 3,
        })
        result = sus_filter(rel, drop_ignored=False)
        assert _count(result) == 3

    def test_drop_ignored_combined_with_sex_filter(self):
        rel = _make_rel({
            "SEXO": ["1", "9", "2", "1"],
            "CAUSABAS": ["J189"] * 4,
        })
        result = sus_filter(rel, sex="M", drop_ignored=True)
        # sex="M" → code "1" → 2 rows; "9" removed by drop_ignored
        assert _count(result) == 2


# ---------------------------------------------------------------------------
# D.3 — city filter (mocked — municipalities.parquet may not be present)
# ---------------------------------------------------------------------------

class TestCityFilter:
    def test_city_raises_file_not_found_when_parquet_missing(self, sim_do_rel, tmp_path, monkeypatch):  # noqa: E501
        """When municipalities.parquet is absent, raises FileNotFoundError."""
        import climasus4py.utils.data as _data
        monkeypatch.setattr(_data, "data_path", lambda rel_path: tmp_path / rel_path)
        with pytest.raises(FileNotFoundError, match="municipalities.parquet"):
            sus_filter(sim_do_rel, city="São Paulo")

    def test_city_filter_applies_resolved_codes(self, monkeypatch):
        """When expand_city_to_codes resolves correctly, filter is applied."""
        import climasus4py.core.filter as _filter_mod
        monkeypatch.setattr(
            _filter_mod, "expand_city_to_codes",
            lambda _c: ["355030"],
        )
        rel = _make_rel({
            "CODMUNRES": ["355030", "330455", "355030"],
            "CAUSABAS": ["J189"] * 3,
        })
        result = sus_filter(rel, city="São Paulo")
        assert _count(result) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
