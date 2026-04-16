"""Tests for sus_filter — disease, age, sex, geography, and date filtering.

Focuses on:
  - CID-10 filtering (groups + raw codes)
  - IDADE decoding for age_min/age_max
  - Sex, race, UF, municipality filters
  - Date range filtering
"""

import pandas as pd
import pytest

from climasus.core.engine import get_connection
from climasus.core.filter import sus_filter


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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
