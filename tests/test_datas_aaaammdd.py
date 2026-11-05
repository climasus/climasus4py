"""M181 e M183 — datas AAAAMMDD e o calendário de data nula (05/11/2026).

M181: SIH, SINAN e CNES escrevem a data com o ano na frente
(``20230113``). O parser só conhecia ``DDMMYYYY`` e ``YYYYMM``: lida como
DDMMYYYY, ``20230113`` dá mês 23 e vira NULL, e ``20080710`` dá dia 20,
mês 08, ano 0710 -- uma data absurda, não nula. Medido na Etapa 1 em
seis sistemas: datas de internação, alta e nascimento do SIH-RD 0%
iguais ao climasus4r, data de notificação do SINAN nula nas 73.778
linhas, e o calendário inteiro dos dois sistemas 100% nulo.

M183: com a data nula, ``semester`` saía 2 e ``is_weekend`` saía FALSE,
porque a condição NULL caía no ELSE.
"""

from __future__ import annotations

import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.core.pipeline import _date_parse_sql
from climasus4py.core.standardize import _date_parse_expr


def _standardize(valor: str | None):
    v = "NULL" if valor is None else f"'{valor}'"
    return get_connection().sql(
        f"SELECT {_date_parse_expr('c')} FROM (SELECT {v}::VARCHAR AS c)"
    ).fetchone()[0]


def _caminho_rapido(valor: str | None):
    v = "NULL" if valor is None else f"'{valor}'"
    d = get_connection().sql(
        f"SELECT {_date_parse_sql('c')} FROM (SELECT {v}::VARCHAR AS c)"
    ).fetchone()[0]
    return None if d is None else d.date()


# Os dois parsers precisam concordar: o sus_pipeline escolhe um ou outro
# conforme os argumentos, e o total não pode depender de qual rodou.
PARSERS = pytest.mark.parametrize(
    "parse", [_standardize, _caminho_rapido], ids=["standardize", "pipeline"])


@PARSERS
class TestDataAnoNaFrente:

    def test_sih_sinan_cnes(self, parse):
        assert str(parse("20230113")) == "2023-01-13"
        assert str(parse("19991231")) == "1999-12-31"

    def test_a_data_que_virava_ano_0710(self, parse):
        """O caso que não virava nulo, virava data absurda."""
        assert str(parse("20080710")) == "2008-07-10"

    def test_dia_invalido_e_nulo_e_nao_cai_no_ddmmyyyy(self, parse):
        """``20080732`` tem a forma AAAAMMDD; dia 32 não existe.

        Se caísse no DDMMYYYY voltaria como 0732-08-20.
        """
        assert parse("20080732") is None
        assert parse("20230230") is None

    def test_o_ddmmyyyy_do_sim_nao_muda(self, parse):
        """Contrapartida. Para o DDMMYYYY ser lido como AAAAMMDD, os dígitos
        5-6 (o começo do ano: 19, 20, 21) teriam de ser mês -- nunca são."""
        assert str(parse("05012023")) == "2023-01-05"
        assert str(parse("31122022")) == "2022-12-31"
        assert str(parse("19122023")) == "2023-12-19"
        assert str(parse("20112023")) == "2023-11-20"
        assert str(parse("21012020")) == "2020-01-21"

    def test_vazio_e_nulo(self, parse):
        assert parse("") is None
        assert parse(None) is None


def test_o_mes_do_sia_continua_valendo_no_standardize():
    assert str(_standardize("202301")) == "2023-01-01"
    assert str(_standardize("201012")) == "2010-12-01"


class TestCalendarioComDataNula:
    """M183 — data faltante não pode virar 2º semestre nem dia útil."""

    def _calendario(self):
        rel = get_connection().sql(
            "SELECT * FROM (VALUES (DATE '2023-01-07'), (DATE '2023-08-02'), "
            "(NULL::DATE)) t(admission_date)")
        return cs.sus_data_create_variables(
            rel, create_age_groups=False, create_climate_vars=False,
            date_col="admission_date", lang="en", verbose=False,
        ).df()

    def test_data_nula_da_semestre_e_fim_de_semana_nulos(self):
        nula = self._calendario().iloc[2]
        assert nula["semester"] is None or str(nula["semester"]) == "<NA>"
        assert nula["is_weekend"] is None or str(nula["is_weekend"]) == "<NA>"

    def test_datas_validas_continuam_certas(self):
        df = self._calendario()
        # 07/01/2023 foi sábado; 02/08/2023, quarta
        assert df["semester"].tolist()[:2] == [1, 2]
        assert df["is_weekend"].tolist()[:2] == [True, False]
