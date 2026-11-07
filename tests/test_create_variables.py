"""M57 e M126: o `sus_data_create_variables` criava 4 variáveis de 15 em pt.

**M57** era o registrado: o R tem `date_col` e `age_col` e o Python não,
então quem tivesse a detecção errando não tinha escape. Ao implementar,
o que apareceu foi maior.

**M126**, medido antes da correção, sobre o mesmo quadro do SIM:

| idioma | variáveis criadas |
|---|---|
| inglês    | 15 |
| português | **4** — o bloco de calendário e clima pulado, em silêncio |
| espanhol  | **erro** |

Duas causas, e as duas são cópias de listas que o pacote já tinha:

1. `_detect_date_col` andava por um `DATE_COLUMN_CANDIDATES` local —
   **idêntico**, byte a byte, ao `date_candidates` do
   `aggregate_config.json`. A cópia fez a correção do M123 (expandir cada
   nome em todas as grafias da mesma coluna) chegar à agregação e não
   aqui. Sem `data_obito`, o bloco inteiro era pulado.
2. A idade usava uma tupla escrita à mão, e não o `detect_age_column` que
   o próprio arquivo já importava. A tupla tinha **`edad_codigo`**, com
   as palavras na ordem trocada contra o `codigo_edad` que o
   `sus_data_standardize` produz — um nome que nunca casou com nada.

O silêncio era o pior dos dois. O bloco pulado só avisava por um `print`
sob `verbose`, e `verbose` está desligado em qualquer pipeline: o usuário
pedia variáveis de calendário, recebia nenhuma, e nada dizia.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.core.variables import DATE_COLUMN_CANDIDATES, _detect_date_col
from climasus4py.utils.data import load_json

IDIOMAS = ("pt", "en", "es")


@pytest.fixture
def rel_sim():
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": ["310620"] * 3,
        "DTOBITO": ["01012020", "15022020", "20032020"],
        "IDADE": ["465", "470", "301"],
        "SEXO": ["1", "2", "1"],
    })
    conn.register("_cv_sim", df)
    return conn.table("_cv_sim")


def _padronizado(rel, lang: str):
    return cs.sus_data_standardize(rel, system="SIM-DO", lang=lang)


# --- M126: a mesma contagem nos três idiomas ----------------------------


@pytest.mark.parametrize("lang", IDIOMAS)
def test_cria_o_mesmo_numero_de_variaveis_em_qualquer_idioma(
    rel_sim, lang: str
) -> None:
    """A asserção que o M126 é.

    Compara contra o inglês em vez de um número fixo: se alguém
    acrescentar uma variável derivada, o teste continua medindo a coisa
    certa — que os três idiomas andam juntos — em vez de virar um número
    obsoleto.
    """
    def quantas(idioma: str) -> int:
        st = _padronizado(rel_sim, idioma)
        out = cs.sus_data_create_variables(
            st, lang=idioma, system="SIM-DO", verbose=False
        )
        return len([c for c in out.columns if c not in st.columns])

    assert quantas(lang) == quantas("en")


@pytest.mark.parametrize("lang", IDIOMAS)
def test_as_variaveis_de_calendario_sao_criadas(rel_sim, lang: str) -> None:
    """O bloco que era pulado, nomeado.

    A contagem sozinha passaria se um bloco diferente compensasse.
    """
    st = _padronizado(rel_sim, lang)
    out = cs.sus_data_create_variables(
        st, lang=lang, system="SIM-DO", verbose=False
    )
    novas = [c for c in out.columns if c not in st.columns]
    ano = {"pt": "ano", "en": "year", "es": "anio"}[lang]
    assert ano in novas, f"{lang}: sem variável de ano — {novas}"


@pytest.mark.parametrize("lang", IDIOMAS)
def test_as_variaveis_de_idade_sao_criadas(rel_sim, lang: str) -> None:
    st = _padronizado(rel_sim, lang)
    out = cs.sus_data_create_variables(
        st, lang=lang, system="SIM-DO", verbose=False
    )
    novas = [c for c in out.columns if c not in st.columns]
    faixa = {"pt": "faixa_etaria", "en": "age_group", "es": "grupo_edad"}[lang]
    assert faixa in novas, f"{lang}: sem faixa etária — {novas}"


def test_o_espanhol_nao_levanta_mais(rel_sim) -> None:
    """Era erro duro, não silêncio, e por um nome com as palavras trocadas.

    A tupla dizia `edad_codigo`; o `sus_data_standardize` produz
    `codigo_edad`. Nunca casou.
    """
    st = _padronizado(rel_sim, "es")
    assert "codigo_edad" in st.columns
    cs.sus_data_create_variables(st, lang="es", system="SIM-DO", verbose=False)


# --- a cópia eliminada --------------------------------------------------


def test_a_deteccao_de_data_delega_para_a_agregacao() -> None:
    """Uma lista a menos para divergir.

    Enquanto eram duas cópias, corrigir uma deixava a outra para trás —
    que é exatamente o que aconteceu com o M123.
    """
    assert _detect_date_col(["data_obito"], "SIM-DO") == "data_obito"
    assert _detect_date_col(["fecha_muerte"], "SIM-DO") == "fecha_muerte"
    assert _detect_date_col(["death_date"], "SIM-DO") == "death_date"


def test_a_lista_local_continua_identica_a_publicada() -> None:
    """Ela virou fallback offline; divergir dela seria voltar ao defeito.

    Não é mais consultada no caminho normal, mas se alguém acrescentar um
    nome aqui achando que resolve algo, o acréscimo não chega a lugar
    nenhum — e este teste diz isso na cara.
    """
    publicada = load_json("templates/aggregate_config.json")["date_candidates"]
    assert DATE_COLUMN_CANDIDATES == publicada


def test_o_sistema_decide_a_prioridade_da_data() -> None:
    """SIM data pelo óbito, SINAN pela notificação.

    É o que justifica passar `system` adiante em vez de usar só a lista
    comum.
    """
    colunas = ["death_date", "notification_date"]
    assert _detect_date_col(colunas, "SIM-DO") == "death_date"
    assert _detect_date_col(colunas, "SINAN-DENG") == "notification_date"


def test_o_sistema_vem_do_sus_meta_quando_nao_informado(rel_sim) -> None:
    """Chamada sem `system=` não pode perder a prioridade do sistema.

    Mesmo raciocínio do M21 no `sus_data_aggregate`: a relação carrega o
    sistema, e ignorá-lo escolhia a coluna errada sem sinal nenhum.
    """
    from climasus4py.core._stage import get_meta

    # O `sus_data_standardize` já grava o sistema; não é preciso montar
    # o metadado à mão, e montar à mão testaria outra coisa.
    st = _padronizado(rel_sim, "pt")
    assert get_meta(st).get("system") == "SIM-DO"

    out = cs.sus_data_create_variables(st, lang="pt", verbose=False)
    assert "ano" in [c for c in out.columns if c not in st.columns]


# --- M57: os parâmetros --------------------------------------------------


@pytest.mark.parametrize("lang", IDIOMAS)
def test_date_col_e_age_col_explicitos_funcionam(rel_sim, lang: str) -> None:
    st = _padronizado(rel_sim, lang)
    data = {"pt": "data_obito", "en": "death_date", "es": "fecha_muerte"}[lang]
    idade = {"pt": "codigo_idade", "en": "age_code", "es": "codigo_edad"}[lang]
    out = cs.sus_data_create_variables(
        st, lang=lang, date_col=data, age_col=idade, verbose=False
    )
    assert len([c for c in out.columns if c not in st.columns]) > 10


def test_o_explicito_vence_a_deteccao(rel_sim) -> None:
    """O ponto do M57: quando a detecção erra, tem de haver saída.

    Aqui a detecção escolheria `data_obito`; passando
    `date_col="data_nascimento"` o ano derivado passa a ser o do
    nascimento, o que prova que o argumento mandou.
    """
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": ["310620"] * 2,
        "DTOBITO": ["01012020", "15022020"],
        "DTNASC": ["01011950", "02021960"],
        "IDADE": ["465", "470"],
    })
    conn.register("_cv_dois", df)
    st = cs.sus_data_standardize(
        conn.table("_cv_dois"), system="SIM-DO", lang="pt"
    )

    padrao = cs.sus_data_create_variables(
        st, lang="pt", system="SIM-DO", verbose=False
    ).df()
    forcado = cs.sus_data_create_variables(
        st, lang="pt", system="SIM-DO", date_col="data_nascimento", verbose=False
    ).df()

    assert set(padrao["ano"]) == {2020}
    assert set(forcado["ano"]) == {1950, 1960}


@pytest.mark.parametrize("parametro", ["date_col", "age_col"])
def test_coluna_inexistente_levanta_em_vez_de_cair_na_deteccao(
    rel_sim, parametro: str
) -> None:
    """Cair de volta na detecção seria o pior dos dois mundos.

    Quem nomeia uma coluna errada receberia variáveis construídas a
    partir de outra, sem nada indicando a troca.
    """
    st = _padronizado(rel_sim, "pt")
    with pytest.raises(ValueError, match="is not a column"):
        cs.sus_data_create_variables(
            st, lang="pt", verbose=False, **{parametro: "nao_existe"}
        )


def test_a_assinatura_tem_os_parametros_do_r() -> None:
    """O M57 era exatamente a ausência destes dois nas formals."""
    import inspect

    p = inspect.signature(cs.sus_data_create_variables).parameters
    assert "date_col" in p and p["date_col"].default is None
    assert "age_col" in p and p["age_col"].default is None


# --- o silêncio ---------------------------------------------------------


def test_avisa_quando_pula_o_bloco_de_calendario() -> None:
    """Era um `print` sob `verbose`, e `verbose` é falso em pipeline.

    O usuário pediu variáveis de calendário e recebeu nenhuma.
    """
    conn = get_connection()
    conn.register("_cv_sem_data", pd.DataFrame({"IDADE": ["465", "470"]}))
    with pytest.warns(UserWarning, match="calendar and climate"):
        cs.sus_data_create_variables(conn.table("_cv_sem_data"), verbose=False)


def test_nao_avisa_quando_ha_data(rel_sim) -> None:
    st = _padronizado(rel_sim, "pt")
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        cs.sus_data_create_variables(
            st, lang="pt", system="SIM-DO", verbose=False
        )
    assert [w for w in capturados if "calendar and climate" in str(w.message)] == []


def test_nao_avisa_quando_o_calendario_foi_dispensado(rel_sim) -> None:
    """Contrapartida: quem pediu para não criar não precisa ser avisado."""
    st = _padronizado(rel_sim, "pt")
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        cs.sus_data_create_variables(
            st, lang="pt", create_calendar_vars=False,
            create_climate_vars=False, verbose=False,
        )
    assert [w for w in capturados if "calendar and climate" in str(w.message)] == []
