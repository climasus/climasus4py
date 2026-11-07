"""D3/M52: os dois caminhos do sus_pipeline devolvem a MESMA tabela.

O `sus_pipeline` tem um fast path (uma CTE sobre o parquet) e um staged
(a cadeia completa), e QUAL RODA E DECIDIDO PELOS ARGUMENTOS, nao pelo
usuario. Enquanto as saidas diferiam, cair no fallback quebrava o codigo
a jusante -- e o aviso de fallback chegou a dizer "Results should be
equivalent but slower", que era falso.

O staged e o alvo da convergencia porque e ele que bate com o R. Medido
no climasus4r: o `.data_aggregate_tibble_internal` renomeia a data para
`date`, nomeia a contagem com `get_smart_column_name(system, "count",
lang)` -- n_obitos em pt, n_deaths em en, n_muertes em es -- e escolhe a
coluna geografica por prioridade POR SISTEMA, com
`system_priority$SIM = c("ocorrencia", "residencia", ...)`: serie de
mortalidade se agrega por local de OCORRENCIA.

Eram QUATRO divergencias e nao as tres do registro, todas medidas em
SIM-DO SP 2023:

1. esquema -- time_group/state/count contra
   date/occurrence_municipality_code/n_deaths;
2. geografia -- o fast derivava o estado da residencia, o staged usava
   ocorrencia (recortes epidemiologicos diferentes, nao grafias);
3. totais -- 334.303 contra 333.968;
4. TIPO da coluna de data -- o fast devolvia a string "2023-01" e o
   staged a DATE 2023-01-01. Nao estava no registro; apareceu depois de
   fechar as outras tres.

E a causa do item 3 NAO era a que o registro dizia. Ver
`TestOsTrezentosETrintaECinco`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import climasus4py as cs

CACHE = Path("dados/cache/SIM-DO/SP_2023_all.parquet")
pytestmark = pytest.mark.skipif(
    not CACHE.is_file(), reason="cache do SIM-DO SP 2023 ausente")


def _fast(**kw) -> pd.DataFrame:
    return cs.sus_pipeline("SIM-DO", "SP", 2023, geo="municipality",
                           verbose=False, **kw).df()


def _staged(**kw) -> pd.DataFrame:
    """Forca o staged: epi_week=True desliga o fast path."""
    return cs.sus_pipeline("SIM-DO", "SP", 2023, epi_week=True,
                           verbose=False, **kw).df()


def _ordenado(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(list(df.columns[:2])).reset_index(drop=True)


class TestAsDuasSaidasSaoIguais:

    @pytest.mark.parametrize("unidade", ["month", "year", "week", "day"])
    def test_a_tabela_inteira_coincide(self, unidade):
        """A prova que importa: nome, tipo e valor, celula a celula."""
        a = _ordenado(_fast(time=unidade))
        b = _ordenado(_staged(time=unidade))
        pd.testing.assert_frame_equal(a, b)

    def test_o_esquema_e_o_do_r(self):
        """date + coluna geografica + contagem por sistema e idioma."""
        df = _fast(time="month")
        assert list(df.columns) == [
            "date", "occurrence_municipality_code", "n_deaths"]

    def test_a_contagem_muda_de_nome_por_idioma(self):
        """get_smart_column_name do R: n_obitos / n_deaths / n_muertes."""
        esperado = {"pt": "n_obitos", "en": "n_deaths", "es": "n_muertes"}
        for lang, nome in esperado.items():
            df = _fast(time="year", lang=lang)
            assert df.columns[-1] == nome, f"lang={lang}"

    def test_a_coluna_de_data_e_DATE_e_nao_rotulo(self):
        """Quarta divergencia, a que nao estava no registro.

        O fast devolvia a string "2023-01". Aritmetica de data,
        reamostragem e grafico funcionam num tipo e nao no outro.
        """
        for unidade in ("month", "year", "week"):
            df = _fast(time=unidade)
            assert df["date"].dtype.kind == "M", (
                f"time={unidade}: {df['date'].dtype}")

    def test_a_geografia_e_a_de_OCORRENCIA_no_sim(self):
        """system_priority$SIM comeca por ocorrencia, no R.

        O fast derivava do municipio de residencia. Nao e questao de
        nome: e onde a morte foi contada.
        """
        df = _fast(time="year")
        assert "occurrence" in df.columns[1]
        assert "residence" not in df.columns[1]


class TestOsTrezentosETrintaECinco:
    """A causa do buraco de 335 linhas NAO era a que o M52 registrava.

    O registro dizia: "o staged roda sus_data_clean_encoding e portanto
    DEDUPLICA ... 335 linhas, exatamente o que a deduplicacao remove".
    Nao e verdade. Medido neste parquet: CONTADOR tem 334.303 valores
    distintos em 334.303 linhas, entao a deduplicacao nao remove NADA.

    A causa real: havia DUAS implementacoes do decodificador de idade do
    DATASUS -- a compartilhada, `decode_age_sql`, e uma copia inline
    dentro do `clean.py`, escrita antes dela. Divergiam em exatamente um
    codigo: `999`, o sentinela de IDADE DESCONHECIDA. A copia caia no
    TRY_CAST e obtinha 999 ANOS, fora de qualquer faixa plausivel, e a
    linha era descartada -- 335 obitos reais jogados fora por nao dizerem
    a idade da pessoa. A funcao compartilhada devolve NULL, que e o que o
    R devolve e o que o filtro preserva.

    Corrigido fazendo o clean.py usar a funcao compartilhada. Registrado
    como M119.
    """

    def test_o_contador_nao_tem_duplicata(self):
        """Derruba a explicacao do registro."""
        import duckdb

        n, d = duckdb.connect().sql(
            f"SELECT COUNT(*), COUNT(DISTINCT CONTADOR) "
            f"FROM read_parquet('{CACHE.as_posix()}')").fetchone()
        assert n == d == 334303, (n, d)

    def test_as_duas_decodificacoes_concordam_agora(self):
        """O clean.py passou a usar decode_age_sql."""
        import duckdb

        from climasus4py.utils.data import decode_age_sql

        expr = decode_age_sql("IDADE")
        q = (f"SELECT COUNT(*) FILTER (WHERE ({expr}) IS NULL) "
             f"FROM read_parquet('{CACHE.as_posix()}') WHERE IDADE = '999'")
        (nulos,) = duckdb.connect().sql(q).fetchone()
        assert nulos == 335, (
            "a fixture perdeu o caso: 999 tem de decodificar para NULL")

    def test_os_335_obitos_voltaram_para_a_contagem(self):
        """Antes o staged devolvia 333.968; agora os dois devolvem 334.303.

        Idade desconhecida nao e motivo para descartar um obito de uma
        contagem de mortalidade.
        """
        for df in (_fast(time="year"), _staged(time="year")):
            assert int(df[df.columns[-1]].sum()) == 334303

    def test_idade_desconhecida_sobrevive_ao_clean(self):
        from climasus4py.core.engine import get_connection

        rel = get_connection().read_parquet(str(CACHE))
        antes = rel.count("*").fetchone()[0]
        limpo = cs.sus_data_clean_encoding(rel, dedup=True,
                                           age_range=(0, 120))
        assert limpo.count("*").fetchone()[0] == antes, (
            "o clean voltou a descartar linha por idade desconhecida")

    def test_idade_implausivel_de_verdade_ainda_e_cortada(self):
        """A contrapartida: a faixa nao virou letra morta.

        Um codigo 4xx diz "xx anos", entao 4-9-9 seria 99 anos (valido) e
        5-9-9 seria 199 (invalido). O que se corta e idade fora da faixa,
        nao idade ausente.
        """
        from climasus4py.core.engine import get_connection

        rel = get_connection().from_df(pd.DataFrame({
            "CONTADOR": [1, 2, 3],
            "IDADE": ["430", "599", "999"],   # 30 anos, 199 anos, ausente
        }))
        limpo = cs.sus_data_clean_encoding(rel, dedup=False,
                                           age_range=(0, 120)).df()
        assert sorted(limpo["IDADE"]) == ["430", "999"], (
            "599 (199 anos) tinha de sair, e 999 (ausente) tinha de ficar")


class TestFallbackNaoMenteMais:

    def test_o_geo_state_segue_sem_equivalente_no_staged(self):
        """Honestidade sobre o que NAO convergiu.

        `sus_data_aggregate` nao tem `geo`, igual ao R, entao um nivel de
        estado existe so no fast path. O que foi corrigido e o CUT: o
        estado passa a vir do mesmo municipio que o staged usaria.
        """
        df = cs.sus_pipeline("SIM-DO", "SP", 2023, time="month",
                             geo="state", verbose=False).df()
        assert list(df.columns) == ["date", "state", "n_deaths"]
        assert int(df["n_deaths"].sum()) == 334303, (
            "o total tem de bater com o do outro nivel")

    def test_o_estado_vem_do_municipio_de_ocorrencia(self):
        """O digito de UF sai da MESMA coluna que o staged escolheria."""
        muni = _fast(time="year")
        estado = cs.sus_pipeline("SIM-DO", "SP", 2023, time="year",
                                 geo="state", verbose=False).df()
        prefixos = {str(v)[:2] for v in muni["occurrence_municipality_code"]}
        assert set(estado["state"]) <= prefixos
