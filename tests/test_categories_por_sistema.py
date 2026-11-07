"""M104: o categories.json ganhou a dimensao de sistema.

O problema, medido: o `categories.json` era PLANO -- uma entrada por nome
de coluna traduzido -- enquanto o R mantem um dicionario por sistema. Onde
dois sistemas alimentam a mesma coluna traduzida com livros de codigo
diferentes, o arquivo plano so cabia um, e os dois livros acabaram
MISTURADOS na mesma entrada.

O caso que forcou a mudanca, com os numeros do proprio R:

    education_level  <-  INSTRU (SIH)      1=Analfabeto, 2=1 Grau,
                                           3=2 Grau, 4=3 Grau, 9=Ignorado
    education_level  <-  CS_ESCOL_N (SINAN) 1=1 a 4 serie incompleta EF,
                                            2=4 serie completa EF, ...

O `COMMON.education_level` do arquivo antigo tem 11 codigos: os de 1 a 4
com os rotulos do SIH e os 0 e 5 a 10 com os do SINAN. Ou seja **dado de
SINAN saia rotulado com a escala do SIH**.

E nao era caso unico. O SIH codifica sexo como 1 e **3**, nao 1 e 2:
`sex` no SIH tem os codigos 0, 1, 3 e 9.

A estrutura nova espelha a do `columns.json`, que ja tinha secoes:
`_meta`, `COMMON` e uma secao por sistema. O conteudo plano de hoje virou
`COMMON` na integra, entao **nada que traduzia deixou de traduzir** -- e
ha teste para isso, comparando com o arquivo antes da mudanca.
"""

from __future__ import annotations

import json

import pytest

from climasus4py.core.standardize import _load_category_dict
from climasus4py.utils.data import load_json

LINGUAS = ("en", "es", "pt")
PASTA = {"en": "pt-en", "es": "pt-es", "pt": "pt-pt"}
SECOES_ESPERADAS = {"COMMON", "CNES", "SIA", "SIA-AD", "SIA-AM", "SIA-AQ",
                    "SIA-AR", "SIA-PS", "SIH", "SINAN", "SINASC"}


def _arquivo(lang: str) -> dict:
    return load_json(f"dictionaries/{PASTA[lang]}/categories.json")


# --------------------------------------------------------------------------
# A estrutura
# --------------------------------------------------------------------------

class TestEstrutura:

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_tem_as_onze_secoes(self, lang):
        d = _arquivo(lang)
        assert SECOES_ESPERADAS <= set(d), (
            f"{lang}: faltam secoes {SECOES_ESPERADAS - set(d)}")

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_as_secoes_sao_as_mesmas_do_columns_json(self, lang):
        """Se as duas listas divergirem, o loader nao acha a secao."""
        cats = {k for k in _arquivo(lang) if not k.startswith("_")}
        cols = {k for k in load_json(f"dictionaries/{PASTA[lang]}/columns.json")
                if not k.startswith("_")}
        # SIM so existe no columns.json: o dicionario base do R cobre SIM,
        # e no categories ele e o COMMON.
        assert cats - {"SIM"} <= cols | {"COMMON"}, (
            f"{lang}: secao em categories que columns nao tem: "
            f"{cats - cols - {'COMMON'}}")

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_meta_declara_a_estrutura(self, lang):
        meta = _arquivo(lang).get("_meta", {})
        assert meta.get("version") == "5.0"
        assert "structure" in meta, (
            "quem abrir o arquivo tem de descobrir a regra de precedencia "
            "sem ler o codigo do loader")
        assert "per code" not in meta["structure"], (
            "a fusao e por LIVRO e nao por codigo -- ver o docstring do "
            "_load_category_dict")

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_todo_livro_e_dict_de_texto(self, lang):
        for secao, chaves in _arquivo(lang).items():
            if secao.startswith("_"):
                continue
            for col, livro in chaves.items():
                assert isinstance(livro, dict), f"{lang}/{secao}/{col}"
                for k, v in livro.items():
                    assert isinstance(k, str) and isinstance(v, str), (
                        f"{lang}/{secao}/{col}: {k!r} -> {v!r}")


# --------------------------------------------------------------------------
# Nao houve regressao
# --------------------------------------------------------------------------

class TestSemRegressao:
    """O que valia antes continua valendo quando nao se sabe o sistema."""

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_sem_system_devolve_exatamente_o_common(self, lang):
        assert _load_category_dict(lang) == _arquivo(lang)["COMMON"]

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_o_common_nao_perdeu_celula(self, lang):
        """O arquivo plano tinha 761, 721 e 799 celulas; o COMMON tem as mesmas.

        Numeros medidos antes da conversao e fixados aqui: se alguem
        reconstruir o arquivo e o COMMON encolher, alguma coluna deixa de
        traduzir e ninguem veria.
        """
        esperado = {"en": (220, 761), "es": (206, 721), "pt": (213, 799)}
        chaves, celulas = esperado[lang]
        common = _arquivo(lang)["COMMON"]
        assert len(common) == chaves
        assert sum(len(v) for v in common.values()) == celulas

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_sistema_sem_secao_propria_cai_no_common(self, lang):
        """O SIM nao tem secao: o dicionario base do R e o COMMON."""
        assert _load_category_dict(lang, system="SIM-DO") == \
            _arquivo(lang)["COMMON"]

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_coluna_que_a_secao_nao_cita_continua_coberta(self, lang):
        """A substituicao e por livro, nao por secao inteira.

        Uma secao por sistema declara poucas colunas; todas as outras
        seguem vindo do COMMON. Sem isso, escolher o sistema REDUZIRIA a
        traducao, e o ganho do M7 seria perdido em silencio.
        """
        common = _arquivo(lang)["COMMON"]
        for sistema in ("SINAN-DENG", "SIH-RD", "CNES-ST"):
            mapa = _load_category_dict(lang, system=sistema)
            assert len(mapa) >= len(common), (
                f"{lang}/{sistema}: {len(mapa)} colunas contra "
                f"{len(common)} do COMMON -- escolher o sistema nao pode "
                f"tirar cobertura")


# --------------------------------------------------------------------------
# A colisao que motivou tudo
# --------------------------------------------------------------------------

class TestColisaoDeEscolaridade:

    def test_o_arquivo_antigo_misturava_os_dois_livros(self):
        """Fixa o defeito, para que a correcao nao pareca gratuita.

        O COMMON e o arquivo plano de antes, e ele ainda carrega a
        mistura: codigos 1 a 4 com rotulo do SIH e 0, 5 a 10 com rotulo
        do SINAN, na mesma entrada.
        """
        livro = _arquivo("en")["COMMON"]["education_level"]
        assert len(livro) == 11
        assert livro["1"] == "Illiterate", "1 a 4 vem do SIH"
        assert livro["5"] == "High school (incomplete)", "5 a 10 vem do SINAN"

    def test_sinan_recebe_o_livro_do_sinan(self):
        livro = _load_category_dict("en", system="SINAN-DENG")["education_level"]
        assert livro["1"] == "1-4 years of primary school (incomplete)"
        assert livro["2"] == "4 years of primary school (complete)"
        assert len(livro) == 11

    def test_sih_recebe_o_livro_do_sih_e_so_ele(self):
        """Cinco codigos, como o INSTRU do R -- e nada mais.

        O codigo 5 sai CRU no SIH, de proposito: o INSTRU para no 4 mais
        o 9. Rotular o 5 do SIH com o rotulo que o SINAN da a ele seria
        inventar significado, que e o defeito do M70 e do M79 por outro
        caminho.
        """
        livro = _load_category_dict("en", system="SIH-RD")["education_level"]
        assert sorted(livro) == ["1", "2", "3", "4", "9"]
        assert livro["1"] == "Illiterate"
        assert livro["4"] == "Higher Education"
        assert "5" not in livro

    def test_os_dois_livros_discordam_de_fato(self):
        """Sem esta, as duas acima poderiam passar com livros iguais."""
        sih = _load_category_dict("en", system="SIH-RD")["education_level"]
        sinan = _load_category_dict("en", system="SINAN-DENG")["education_level"]
        colidem = [c for c in set(sih) & set(sinan) if sih[c] != sinan[c]]
        assert colidem, "os livros deixaram de discordar: o teste perdeu o caso"
        assert set(colidem) >= {"1", "2", "3", "4"}

    def test_o_sih_codifica_feminino_como_3(self):
        """Segunda colisao real, achada ao implementar isto.

        O SIH usa 1 e 3 para sexo, nao 1 e 2. O arquivo plano tinha os
        dois pares juntos, entao um 2 em dado do SIH -- que o SIH nao
        emite -- saia como 'Female'.
        """
        sih = _load_category_dict("en", system="SIH-RD")["sex"]
        assert sorted(sih) == ["0", "1", "3", "9"]
        assert sih["1"] == "Male" and sih["3"] == "Female"
        assert "2" not in sih

    def test_sinan_usa_letra_para_sexo(self):
        sinan = _load_category_dict("en", system="SINAN-DENG")["sex"]
        assert sorted(sinan) == ["F", "I", "M"]


# --------------------------------------------------------------------------
# Precedencia
# --------------------------------------------------------------------------

class TestPrecedencia:

    def test_familia_atende_o_subsistema(self):
        """SIA-PA nao tem secao propria e cai em SIA, como no columns.json."""
        pa = _load_category_dict("en", system="SIA-PA")
        sia = _load_category_dict("en", system="SIA")
        assert pa == sia

    def test_subsistema_com_secao_propria_vence_a_familia(self):
        ad = _load_category_dict("en", system="SIA-AD")
        sia = _load_category_dict("en", system="SIA")
        assert ad != sia, "SIA-AD tem secao propria e devia diferir de SIA"

    def test_sistema_desconhecido_nao_quebra(self):
        assert _load_category_dict("en", system="INEXISTENTE-XX") == \
            _arquivo("en")["COMMON"]

    def test_lingua_inexistente_devolve_vazio(self):
        assert _load_category_dict("zz") == {}

    def test_arquivo_plano_antigo_ainda_carrega(self, tmp_path, monkeypatch):
        """Compatibilidade para tras, detectada pela AUSENCIA de COMMON.

        Nao pela versao no _meta: um arquivo editado a mao sem o bump de
        versao tem de carregar igual.
        """
        from climasus4py.core import standardize

        plano = {"_meta": {"version": "4.0"},
                 "sex": {"1": "Male", "2": "Female"}}
        monkeypatch.setattr(standardize, "load_json", lambda _p: plano)
        assert standardize._load_category_dict("en") == \
            {"sex": {"1": "Male", "2": "Female"}}
        assert standardize._load_category_dict("en", system="SINAN-DENG") == \
            {"sex": {"1": "Male", "2": "Female"}}


# --------------------------------------------------------------------------
# columns.json: as cinco secoes SIA que faltavam em es e pt
# --------------------------------------------------------------------------

class TestSecoesSiaAcrescentadas:
    """Pre-requisito da mudanca, e nao extra.

    Sem as secoes SIA-AD, SIA-AM, SIA-AQ, SIA-AR e SIA-PS no
    `columns.json`, 62 campos de valor do SIA nao tinham nome traduzido, e
    a medicao mostrou pt-es e pt-pt mapeando 72% dos campos do R contra
    99,2% do pt-en. As cinco secoes vieram dos proprios dicionarios do
    climasus4r, que as tem nas tres linguas.
    """

    CINCO = ("SIA-AD", "SIA-AM", "SIA-AQ", "SIA-AR", "SIA-PS")

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_as_cinco_secoes_existem(self, lang):
        cols = load_json(f"dictionaries/{PASTA[lang]}/columns.json")
        faltam = [s for s in self.CINCO if s not in cols]
        assert not faltam, f"{lang}: {faltam}"

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_o_tamanho_bate_com_o_do_ingles(self, lang):
        """As tres linguas descrevem os MESMOS campos."""
        esperado = {"SIA-AD": 46, "SIA-AM": 51, "SIA-AQ": 74, "SIA-AR": 74,
                    "SIA-PS": 45}
        cols = load_json(f"dictionaries/{PASTA[lang]}/columns.json")
        for secao, n in esperado.items():
            assert len(cols[secao]) == n, f"{lang}/{secao}"

    @pytest.mark.parametrize("lang", LINGUAS)
    def test_os_campos_sia_de_valor_agora_tem_destino(self, lang):
        """AP_RACACOR e companhia deixaram de ficar sem nome traduzido."""
        cats = _arquivo(lang)
        for secao in self.CINCO:
            assert cats[secao], f"{lang}/{secao} vazia"
            cruas = [k for k in cats[secao] if k.isupper() and "_" in k]
            assert not cruas, (
                f"{lang}/{secao}: ainda ha chave com nome cru do DATASUS, "
                f"logo o columns.json nao a traduz: {cruas}")

    def test_o_espanhol_traduzia_menos_que_o_ingles(self):
        """M117, e o unico ponto que NAO consertei aqui.

        A lacuna que sobra e do dicionario do R e nao deste arquivo: o
        $columns do SIH em espanhol tem 100 campos contra os 153 do ingles
        e do portugues. Os 53 que faltam nao tem traducao em NENHUMA
        secao, e dois deles -- GESTOR_TP e VINCPREV -- TEM livro de
        valores em espanhol, entao o valor sai traduzido e o nome da
        coluna sai cru. Registrado como M117; corrigir e trabalho no
        climasus4r.
        """
        cols_es = load_json("dictionaries/pt-es/columns.json")
        cols_en = load_json("dictionaries/pt-en/columns.json")
        faltando = set(cols_en["SIH"]) - set(cols_es["SIH"])
        assert len(faltando) == 53, (
            f"a lacuna do espanhol mudou: {len(faltando)} campos. Se foi "
            f"corrigida, atualize o M117 e este teste")
        assert {"GESTOR_TP", "VINCPREV"} <= faltando


# --------------------------------------------------------------------------
# Ponta a ponta
# --------------------------------------------------------------------------

class TestNoPipeline:

    @staticmethod
    def _rel(**cols):
        import pandas as pd

        from climasus4py.core.engine import get_connection
        return get_connection().from_df(pd.DataFrame(cols))

    def test_o_mesmo_codigo_sai_diferente_por_sistema(self):
        """A prova que importa: dois sistemas, mesmo codigo, rotulo certo.

        Nao basta o loader devolver o livro certo -- o
        sus_data_standardize tem de repassar o system para ele. Esta foi a
        linha de uma palavra que faltava.
        """
        import climasus4py as cs

        alvo = {}
        for sistema in ("SIH-RD", "SINAN-DENG"):
            rel = self._rel(INSTRU=["1"], CS_ESCOL_N=["1"])
            out = cs.sus_data_standardize(rel, lang="en", system=sistema).df()
            col = "education_level"
            alvo[sistema] = out[col].iloc[0] if col in out.columns else None

        assert alvo["SIH-RD"] == "Illiterate", alvo
        assert alvo["SINAN-DENG"] == \
            "1-4 years of primary school (incomplete)", alvo
        assert alvo["SIH-RD"] != alvo["SINAN-DENG"], (
            "o system nao chegou ao passo de valor: os dois sistemas "
            "receberam o mesmo rotulo para o codigo 1")
