"""Tests for metadata contracts used by climasus4py.

These tests must pass with both:
- newer climasus-data catalogs (with extended JSON files), and
- current PyPI climasus-data release, where some files may not exist yet.
"""

from climasus4py.core.variables import (
    _age_breaks_for_preset,
    _age_groups_config,
    _astronomical_season_sql,
    _seasonal_config,
)
from climasus4py.core.variables import sus_data_create_variables as cs_vars
from climasus4py.utils.data import (
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

    def test_unknown_preset_is_refused(self):
        """Preset desconhecido recusa em vez de cair no default (M53, 09/09).

        A assercao antiga era ``breaks == [0, 18, 65, 999]`` -- um valor que
        nao corresponde a nenhum dos cinco presets do config, resto de um
        AGE_BREAKS_DEFAULT anterior. Mas o defeito nao era o numero: era o
        fallback existir. O unico chamador e ``sus_pipeline(age_group=...)``,
        entao um typo em "who" devolvia as faixas epidemiologicas em silencio
        -- faixas diferentes das pedidas, sem nenhum sinal.
        """
        import pytest

        with pytest.raises(ValueError, match="Unknown age_group preset"):
            _age_breaks_for_preset("nonexistent")

    def test_every_preset_in_the_config_resolves(self):
        """Contrapartida: os nomes validos continuam todos resolvendo."""
        presets = _age_groups_config()["presets"]
        for nome in presets:
            breaks = _age_breaks_for_preset(nome)
            assert breaks, f"preset {nome} devolveu vazio"
            assert breaks == sorted(breaks), f"preset {nome} fora de ordem"
            assert all(isinstance(b, int) for b in breaks)


class TestSeasonalPatternsConfig:
    """O config de estacoes e o SQL que sai dele.

    Atualizado em 09/09/2026 (M53). Estes testes fixavam nomes e um formato que
    o pacote mudou, e o ImportError de ``_season_case_sql`` derrubava a COLETA
    da suite inteira. Tres coisas mudaram de verdade:

    - ``_seasonal_patterns_config`` virou ``_seasonal_config``, e ``patterns``
      deixou de ser chave de topo: agora vive sob ``astronomical``, ao lado de
      ``climatic``, que e a outra familia de estacoes.
    - ``_season_case_sql`` virou ``_astronomical_season_sql`` e passou a
      receber uma expressao de MES, nao de data -- quem monta o
      ``EXTRACT(MONTH FROM ...)`` agora e o chamador.
    - o hemisferio invalido nao cai mais num default silencioso. Ver
      ``test_public_function_validates_hemisphere``.
    """

    def test_config_loads(self):
        data = _seasonal_config()
        assert "patterns" in data["astronomical"]
        assert "south" in data["astronomical"]["patterns"]
        assert "north" in data["astronomical"]["patterns"]

    def test_south_summer_months(self):
        padroes = _seasonal_config()["astronomical"]["patterns"]
        assert set(padroes["south"]["summer"]) == {12, 1, 2}

    def test_north_winter_months(self):
        padroes = _seasonal_config()["astronomical"]["patterns"]
        assert set(padroes["north"]["winter"]) == {12, 1, 2}

    def test_season_sql_south_contains_summer(self):
        sql = _astronomical_season_sql("EXTRACT(MONTH FROM d)", hemisphere="south")
        assert "Summer" in sql
        # Afirmar sobre os meses, nao sobre o espacamento do SQL gerado: a
        # assercao antiga era "12, 1, 2" e quebrou quando a formatacao passou
        # a ser "12,1,2", sem que nada de substantivo tivesse mudado.
        assert "12,1,2" in sql.replace(" ", "")

    def test_season_sql_north_summer_different(self):
        sul = _astronomical_season_sql("m", hemisphere="south")
        norte = _astronomical_season_sql("m", hemisphere="north")
        assert sul != norte
        # E a diferenca tem de ser a certa: no sul dezembro e verao, no norte
        # e inverno. Sem isso, o teste passaria com qualquer divergencia.
        assert sul.split("THEN")[1].strip().startswith("'Summer'")
        assert norte.split("THEN")[1].strip().startswith("'Winter'")

    def test_public_function_validates_hemisphere(self):
        """O helper privado assume hemisferio valido; quem valida e a publica.

        Substitui o antigo ``test_season_sql_fallback_to_default``, que exigia
        que um hemisferio desconhecido caisse em 'south' em silencio. Cair num
        default e pior que recusar: um erro de digitacao viraria uma serie de
        estacoes invertidas sem nenhum sinal. A funcao exportada recusa com
        mensagem clara, e e esse o contrato que vale fixar.
        """
        import pandas as pd
        import pytest

        from climasus4py.core.engine import get_connection

        conn = get_connection()
        conn.register("_season_fix", pd.DataFrame({"date": ["2023-01-15"]}))
        rel = conn.sql("SELECT * FROM _season_fix")

        with pytest.raises(ValueError, match="hemisphere"):
            cs_vars(rel, hemisphere="hemisferio_inexistente", verbose=False)
