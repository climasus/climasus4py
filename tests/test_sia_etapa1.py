"""O que a Etapa 1 em seis sistemas encontrou (27/10/2026).

Quatro defeitos saíram da mesma bateria, e o que os une é o silêncio:
nenhum deles levantava erro. O pipeline seguia em frente e devolvia
número plausível e errado.

  M162  o standardize não sabia que sistema era e devolvia os nomes
        crus do DATASUS -- 0 de 60 colunas traduzidas no SIA
  M163  o SIA não tinha coluna de idade nem de data declarada, e suas
        datas são YYYYMM
  M164  o import não filtrava por UF de residência no SIH e no SIA
  M167  a idade do SIA era decodificada com a regra do SIM

Os testes aqui não baixam nada: montam as colunas à mão e exercitam a
decisão, não o download. As contagens citadas nos comentários vêm da
comparação contra o climasus4r em SIA-PA/AC/janeiro de 2023.
"""

from __future__ import annotations

import duckdb
import pytest

from climasus4py.core.standardize import _date_parse_expr
from climasus4py.utils.data import (
    detect_age_column,
    detect_date_column,
    detect_system,
)

# --- M162: identificar o sistema ----------------------------------------


@pytest.mark.parametrize(
    "colunas, esperado",
    [
        (["PA_CODUNI", "PA_IDADE", "PA_MUNPCN"], "SIA-PA"),
        (["CNES", "VINC_SUS", "ATENDAMB"], "CNES-ST"),
        (["TP_NOT", "ID_AGRAVO", "DT_NOTIFIC"], "SINAN-DENGUE"),
        (["ORIGEM", "LOCNASC", "IDADEMAE", "APGAR5"], "SINASC"),
        (["CAUSABAS", "DTOBITO", "CODMUNRES"], "SIM-DO"),
        (["DIAG_PRINC", "ANO_CMPT", "MUNIC_RES"], "SIH-RD"),
    ],
)
def test_os_seis_sistemas_sao_reconhecidos(colunas, esperado):
    """Antes só quatro tinham assinatura, e duas delas erradas.

    SIA e CNES não tinham nenhuma; a do SINAN apontava NU_NOTIFIC, que
    não aparece em nenhuma seção dos dicionários; a do SINASC apontava
    NUMERODN, ausente do layout de 2023.
    """
    assert detect_system(colunas) == esperado


def test_o_standardize_le_o_sistema_do_metadado_antes_de_adivinhar():
    """A correção do M162 em uma linha.

    O import recebe o sistema como argumento e o grava no sus_meta; o
    clean_encoding o carrega adiante. O standardize ignorava os dois.
    """
    import inspect

    from climasus4py.core import standardize

    fonte = inspect.getsource(standardize.sus_data_standardize)
    assert 'get_meta(rel).get("system")' in fonte
    # E o alias do DuckDB não pode voltar a ser fonte: numa relação lida
    # de arquivos ele é "parquet_5957eec4d0d15e67", e adotá-lo como nome
    # de sistema é o que tornava a falha muda.
    assert "rel.alias" not in fonte
    assert "getattr(rel, 'alias'" not in fonte


# --- M163: a idade e a data do SIA --------------------------------------


def test_o_sia_tem_coluna_de_idade_declarada():
    """patient_age não estava em role_priority['age'] (nem no R)."""
    assert detect_age_column(["patient_age", "procedure_min_age"]) == "patient_age"


def test_a_idade_do_sujeito_vem_antes_da_idade_da_mae():
    """A materna é último recurso, e está registrada no M166."""
    colunas = ["mother_age", "patient_age"]
    assert detect_age_column(colunas) == "patient_age"


def test_o_sia_tem_data_declarada_e_a_competencia_vem_primeiro():
    """Competência é o mês a que a produção se refere.

    Processamento é quando o DATASUS a tratou, e pode cair num mês
    posterior ao atendimento.
    """
    assert detect_date_column(["processing_date", "competence_date"]) == \
        "competence_date"


def test_o_detector_do_aggregate_cai_na_prioridade_global():
    """templates/aggregate_config.json não tem seção do SIA.

    Sem o fallback, detect devolvia None num quadro do SIA e o
    sus_data_create_variables pulava o bloco de calendário inteiro --
    15 variáveis a menos, com aviso e sem erro.
    """
    from climasus4py.core.aggregate import _agg_detect_date_col

    assert _agg_detect_date_col(
        ["competence_date", "processing_date", "patient_age"], "SIA-PA"
    ) == "competence_date"


class TestDataYYYYMM:
    """As datas do SIA nomeiam um MÊS, não um dia."""

    def _parse(self, valor: str | None):
        conn = duckdb.connect()
        v = "NULL" if valor is None else f"'{valor}'"
        return conn.sql(
            f"SELECT {_date_parse_expr('c')} FROM (SELECT {v}::VARCHAR AS c)"
        ).fetchone()[0]

    def test_o_mes_resolve_para_o_primeiro_dia(self):
        assert str(self._parse("202301")) == "2023-01-01"
        assert str(self._parse("199901")) == "1999-01-01"

    def test_a_competencia_de_2000_a_2012_nao_e_lida_como_dia(self):
        """O motivo de a forma YYYYMM ser tentada ANTES de %d%m%Y.

        %d%m%Y não exige quatro dígitos no ano: numa string de seis lê
        os dois primeiros como dia e os DOIS ÚLTIMOS como ano, então
        201012 -- outubro de 2010 -- voltava como 20/10/0012. Pega
        exatamente as competências em que o terceiro e o quarto dígitos
        formam mês válido, ou seja, os anos 2000 a 2012.
        """
        assert str(self._parse("201012")) == "2010-12-01"
        assert str(self._parse("200112")) == "2001-12-01"

    def test_a_data_de_oito_digitos_continua_sendo_DDMMYYYY(self):
        assert str(self._parse("05012023")) == "2023-01-05"
        assert str(self._parse("31122022")) == "2022-12-31"

    def test_o_iso_e_o_nulo_sobrevivem(self):
        assert str(self._parse("2023-01-15")) == "2023-01-15"
        assert self._parse(None) is None

    def test_o_que_nao_e_data_continua_nao_sendo(self):
        assert self._parse("999999") is None
        assert self._parse("000000") is None


# --- M164: o filtro de residência ---------------------------------------


class TestFiltroDeResidencia:
    """Um arquivo do SIH do Acre traz pacientes residentes fora dele.

    Medido em SIH-RD AC+RR+AP, 2022-2023, meses 1/6/11: 69.586 linhas
    aqui contra 67.084 no climasus4r, e as 2.502 a mais eram exatamente
    as de residência fora de 12, 14 e 16.
    """

    def _expr(self, sistema, ufs, colunas):
        from climasus4py.core.importer import _residence_filter_expression

        return _residence_filter_expression(sistema, ufs, colunas)

    def test_o_sih_filtra_pela_residencia(self):
        e = self._expr("SIH-RD", ["AC", "RR", "AP"], ["MUNIC_RES", "DIAG_PRINC"])
        assert e is not None and "MUNIC_RES" in e
        for codigo in ("'12'", "'14'", "'16'"):
            assert codigo in e

    def test_o_sia_filtra_pelo_municipio_do_paciente(self):
        e = self._expr("SIA-PA", ["AC"], ["PA_MUNPCN"])
        assert e is not None and "PA_MUNPCN" in e

    @pytest.mark.parametrize("sistema, coluna",
                             [("SIM-DO", "CODMUNRES"),
                              ("SINASC", "CODMUNRES"),
                              ("CNES-ST", "CODUFMUN")])
    def test_os_ja_particionados_por_residencia_ficam_de_fora(self, sistema, coluna):
        """Medido, não suposto: em SP 2023 e Sul 2023 os dois pacotes
        devolveram a mesma contagem linha a linha."""
        assert self._expr(sistema, ["SP"], [coluna]) is None

    def test_sem_a_coluna_o_filtro_desaparece_em_vez_de_levantar(self):
        """Um layout que mudou deve perder o filtro, nunca quebrar."""
        assert self._expr("SIH-RD", ["AC"], ["DIAG_PRINC"]) is None

    def test_pedir_todas_as_ufs_nao_filtra_nada(self):
        from climasus4py.utils.data import load_uf_codes

        todas = list(load_uf_codes())
        assert self._expr("SIH-RD", todas, ["MUNIC_RES"]) is None
