"""Tests for metadata contracts used by climasus4py.

These tests must pass with both:
- newer climasus-data catalogs (with extended JSON files), and
- current PyPI climasus-data release, where some files may not exist yet.
"""

from climasus.core.variables import (
    _age_breaks_for_preset,
    _age_groups_config,
    _season_case_sql,
    _seasonal_patterns_config,
)
from climasus.utils.data import (
    detect_age_column,
    detect_cause_column,
    detect_date_column,
    detect_sex_column,
    detect_system,
    load_datasus_columns_spec,
)


class TestDatasusColumnsSpec:
    def test_spec_loads(self):
        data = load_datasus_columns_spec()
        assert "all_date_columns" in data
        assert "all_numeric_columns" in data
        assert "system_signatures" in data
        assert "role_priority" in data

    def test_date_columns_count(self):
        cols = load_datasus_columns_spec()["all_date_columns"]
        assert "DTOBITO" in cols
        assert "DTNASC" in cols
        assert len(cols) >= 18

    def test_numeric_columns_count(self):
        cols = load_datasus_columns_spec()["all_numeric_columns"]
        assert "CONTADOR" in cols
        assert "CODMUNRES" in cols
        assert len(cols) >= 23


class TestDetectSystem:
    def test_sim_do_via_causabas(self):
        assert detect_system(["CAUSABAS", "IDADE"]) == "SIM-DO"

    def test_sim_do_via_dtobito(self):
        assert detect_system(["DTOBITO", "CODMUNRES"]) == "SIM-DO"

    def test_sinasc(self):
        assert detect_system(["NUMERODN", "IDADEMAE"]) == "SINASC"

    def test_sih(self):
        assert detect_system(["DIAG_PRINC", "CODMUNRES"]) == "SIH-RD"

    def test_sinan(self):
        assert detect_system(["NU_NOTIFIC", "SEXO"]) == "SINAN-DENGUE"

    def test_unknown_returns_none(self):
        assert detect_system(["COLUNA_DESCONHECIDA"]) is None

    def test_empty_returns_none(self):
        assert detect_system([]) is None


class TestRolePriorityDetection:
    def test_detect_date_dtobito(self):
        assert detect_date_column(["DTOBITO", "CAUSABAS"]) == "DTOBITO"

    def test_detect_date_standardized(self):
        assert detect_date_column(["death_date", "DTOBITO"]) == "death_date"

    def test_detect_date_none(self):
        assert detect_date_column(["CODMUNRES"]) is None

    def test_detect_cause_causabas(self):
        assert detect_cause_column(["CAUSABAS", "IDADE"]) == "CAUSABAS"

    def test_detect_age_idade(self):
        assert detect_age_column(["IDADE", "SEXO"]) == "IDADE"

    def test_detect_sex_sexo(self):
        assert detect_sex_column(["SEXO", "IDADE"]) == "SEXO"

    def test_detect_sex_cs_sexo(self):
        assert detect_sex_column(["CS_SEXO", "NU_NOTIFIC"]) == "CS_SEXO"


class TestAgeGroupsConfig:
    def test_config_loads(self):
        data = _age_groups_config()
        assert "presets" in data
        assert "default" in data

    def test_preset_who_loaded(self):
        breaks = _age_breaks_for_preset("who")
        assert breaks[0] == 0
        assert breaks[-1] == 999

    def test_preset_decadal_loaded(self):
        breaks = _age_breaks_for_preset("decadal")
        assert 10 in breaks
        assert 20 in breaks

    def test_preset_epid_default(self):
        breaks = _age_breaks_for_preset("epidemiological_default")
        assert breaks == [0, 5, 15, 60, 999]

    def test_unknown_preset_fallback(self):
        breaks = _age_breaks_for_preset("nonexistent")
        assert breaks == [0, 18, 65, 999]


class TestSeasonalPatternsConfig:
    def test_config_loads(self):
        data = _seasonal_patterns_config()
        assert "patterns" in data
        assert "south" in data["patterns"]
        assert "north" in data["patterns"]

    def test_south_summer_months(self):
        data = _seasonal_patterns_config()
        assert 12 in data["patterns"]["south"]["summer"]
        assert 1 in data["patterns"]["south"]["summer"]
        assert 2 in data["patterns"]["south"]["summer"]

    def test_north_winter_months(self):
        data = _seasonal_patterns_config()
        assert 12 in data["patterns"]["north"]["winter"]

    def test_season_sql_south_contains_summer(self):
        sql = _season_case_sql("TRY_CAST(date AS DATE)", hemisphere="south")
        assert "Summer" in sql
        assert "12, 1, 2" in sql

    def test_season_sql_north_summer_different(self):
        sql_south = _season_case_sql("d", hemisphere="south")
        sql_north = _season_case_sql("d", hemisphere="north")
        assert sql_south != sql_north

    def test_season_sql_fallback_to_default(self):
        sql = _season_case_sql("d", hemisphere="unknown_hemisphere")
        assert "Summer" in sql
