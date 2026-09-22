"""M123: o pipeline documentado quebrava em português e espanhol.

    sus_data_standardize(lang="pt")   →  data_obito
    sus_data_aggregate(...)           →  ValueError: Date column not found.
                                         Specify date_col= or run
                                         sus_data_standardize() first.

A mensagem mandava rodar a função que acabara de rodar. A causa eram
**duas listas de prioridade em inglês**, mantidas separadamente para o
mesmo trabalho:

- `role_priority`, em `metadata/datasus_columns.json`, que os
  `detect_*_column` leem;
- `date_candidates` / `geo_candidates`, em
  `templates/aggregate_config.json`, que o `sus_data_aggregate` lê — e
  que carrega uma prioridade **por sistema** que a outra não tem (no SIM,
  ocorrência antes de residência).

O próprio `aggregate.py` advertia, duas funções abaixo, que "duas cópias
de uma ordem de prioridade acabariam discordando". Elas discordavam.

**Medido antes:** 26 de 44 combinações de sistema × idioma × papel não
achavam a coluna — 12 em português, 12 em espanhol e 2 em inglês
(`SIH-RD/cause` e `SINAN-DENG/date`). Só o detector de município
funcionava, e funcionava porque tinha sido o único a receber tratamento
multilíngue, no M11.

**A correção não foi traduzir as listas.** Traduzir cada cópia à mão
criaria uma terceira coisa para manter em sincronia. Em vez disso, cada
nome se expande no seu **grupo de sinônimos** — `DTOBITO`, `death_date`,
`data_obito`, `fecha_muerte` são a mesma coluna —, derivado dos
dicionários. Isso tira a dimensão do idioma das listas, e cobre os 43
sistemas sem enumerar nenhum.

**Medido depois:** 0 falhas nos 43 sistemas × 3 idiomas.

A ordem é o que exige cuidado, porque `role_priority` é prioridade e o
primeiro que casa vence: se `data_nascimento` entrasse antes de
`data_obito`, uma série do SIM passaria a ser datada pelo **nascimento**,
silenciosamente. Cada tradução entra logo após o bruto de onde veio.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.aggregate import _agg_detect_date_col, _agg_geo_candidates
from climasus4py.core.engine import get_connection
from climasus4py.utils.data import (
    _FALLBACK_DATASUS_COLUMNS,
    data_path,
    detect_age_column,
    detect_cause_column,
    detect_date_column,
    detect_geo_column,
    detect_sex_column,
    expand_column_synonyms,
    load_datasus_columns_spec,
)

IDIOMAS = ("pt", "en", "es")


@pytest.fixture
def rel_sim():
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": ["310620", "355030", "261160", "310620"],
        "DTOBITO": ["01012020", "15022020", "20032020", "05042020"],
        "CAUSABAS": ["J189"] * 4,
        "IDADE": ["465"] * 4,
        "SEXO": ["1"] * 4,
    })
    conn.register("_m123_sim", df)
    return conn.table("_m123_sim")


# --- o sintoma, nos três idiomas ----------------------------------------


@pytest.mark.parametrize("lang", IDIOMAS)
def test_a_cadeia_standardize_agregate_funciona(rel_sim, lang: str) -> None:
    """A asserção que o M123 é. Antes, `pt` e `es` levantavam."""
    padronizado = cs.sus_data_standardize(rel_sim, system="SIM-DO", lang=lang)
    agregado = cs.sus_data_aggregate(
        padronizado, time_unit="month", system="SIM-DO", lang=lang,
        verbose=False,
    )
    d = agregado.df()
    assert "date" in d.columns
    assert d["date"].notna().all(), f"{lang}: data não converteu"
    assert len(d) == 4


@pytest.mark.parametrize("lang", IDIOMAS)
def test_todos_os_papeis_sao_detectados_apos_o_standardize(
    rel_sim, lang: str
) -> None:
    """Data era o sintoma; causa, idade e sexo falhavam junto."""
    colunas = list(
        cs.sus_data_standardize(rel_sim, system="SIM-DO", lang=lang).columns
    )
    assert detect_date_column(colunas) is not None, "date"
    assert detect_cause_column(colunas) is not None, "cause"
    assert detect_age_column(colunas) is not None, "age"
    assert detect_sex_column(colunas) is not None, "sex"
    assert detect_geo_column(colunas, level="municipality") is not None, "muni"


# --- a ordem, que é o risco silencioso ----------------------------------


@pytest.mark.parametrize("lang", IDIOMAS)
def test_o_sim_continua_datado_pelo_obito_e_nao_pelo_nascimento(
    lang: str
) -> None:
    """O jeito silencioso de estragar tudo ao consertar isto.

    `role_priority` é prioridade: o primeiro que casa vence. Um frame do
    SIM tem data de óbito **e** de nascimento, então pôr a tradução no
    lugar errado trocaria a série inteira por uma de datas de nascimento
    — sem erro, sem aviso, com números plausíveis e décadas errados.
    """
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": ["310620"] * 2,
        "DTOBITO": ["01012020", "15022020"],
        "DTNASC": ["01011950", "02021960"],
    })
    conn.register(f"_m123_ordem_{lang}", df)
    padronizado = cs.sus_data_standardize(
        conn.table(f"_m123_ordem_{lang}"), system="SIM-DO", lang=lang
    )
    agregado = cs.sus_data_aggregate(
        padronizado, time_unit="year", system="SIM-DO", lang=lang,
        verbose=False,
    )
    anos = {str(x)[:4] for x in agregado.df()["date"]}
    assert anos == {"2020"}, f"{lang}: datou por {anos}, esperado o óbito"


def test_a_traducao_vem_logo_apos_o_bruto_no_role_priority() -> None:
    """A invariante que sustenta o teste acima, afirmada na lista.

    Sem isto, um acréscimo futuro poderia pôr uma tradução no fim da
    lista e a ordem relativa deixaria de valer — e o teste de cima só
    pegaria o caso do SIM.
    """
    date = load_datasus_columns_spec()["role_priority"]["date"]
    assert date.index("data_obito") < date.index("DTNASC")
    assert date.index("fecha_muerte") < date.index("DTNASC")
    assert date.index("data_obito") < date.index("data_nascimento")
    assert date.index("fecha_muerte") < date.index("fecha_nacimiento")


# --- a expansão de sinônimos --------------------------------------------


def test_o_grupo_reune_as_quatro_grafias() -> None:
    grupo = expand_column_synonyms(["DTOBITO"])
    for grafia in ("DTOBITO", "death_date", "data_obito", "fecha_muerte"):
        assert grafia in grupo, grafia


def test_a_expansao_preserva_a_prioridade() -> None:
    """O grupo do primeiro candidato vem inteiro antes do grupo do segundo.

    É isto que mantém uma lista de prioridade sendo uma lista de
    prioridade depois de expandida.
    """
    saida = expand_column_synonyms(["DTOBITO", "DTNASC"])
    assert saida.index("fecha_muerte") < saida.index("birth_date")
    assert saida.index("data_obito") < saida.index("data_nascimento")


def test_a_expansao_nao_duplica() -> None:
    saida = expand_column_synonyms(["DTOBITO", "death_date", "DTOBITO"])
    assert len(saida) == len(set(saida))


def test_nome_desconhecido_atravessa_intacto() -> None:
    """Coluna que não está em dicionário nenhum não pode sumir."""
    assert expand_column_synonyms(["MINHA_COLUNA"]) == ["MINHA_COLUNA"]


def test_a_expansao_e_usada_pelo_aggregate() -> None:
    """A ligação entre o helper e o defeito, afirmada.

    Sem isto, o helper poderia existir e o `sus_data_aggregate` continuar
    lendo a lista crua — que era exatamente o estado do defeito.
    """
    assert _agg_detect_date_col(["data_obito"], "SIM-DO") == "data_obito"
    assert _agg_detect_date_col(["fecha_muerte"], "SIM-DO") == "fecha_muerte"
    assert "codigo_municipio_residencia" in _agg_geo_candidates("SIM-DO")


def test_o_aggregate_respeita_a_prioridade_por_sistema_do_config() -> None:
    """A expansão não pode atropelar a ordem que o config declara.

    O `aggregate_config` põe ocorrência antes de residência no SIM, e
    essa escolha é a diferença entre duas séries de mortalidade
    diferentes.
    """
    candidatos = _agg_geo_candidates("SIM-DO")
    assert candidatos.index("occurrence_municipality_code") < candidatos.index(
        "residence_municipality_code"
    )
    assert candidatos.index("codigo_municipio_ocorrencia") < candidatos.index(
        "codigo_municipio_residencia"
    )


# --- o metadado e o seu substituto --------------------------------------


def test_o_catalogo_publica_schema_version_4() -> None:
    publicado = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8")
    )
    assert int(publicado["schema_version"]) >= 4


def test_o_fallback_acompanha_o_publicado() -> None:
    """A cópia embutida não pode ficar para trás da publicada.

    Ela existe para instalação sem climasus-data, e uma divergência aqui
    significa comportamento diferente conforme o catálogo esteja
    presente — o tipo de diferença que ninguém procura.
    """
    publicado = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8")
    )["role_priority"]
    embutido = _FALLBACK_DATASUS_COLUMNS["role_priority"]
    assert set(publicado) == set(embutido)
    for papel in publicado:
        assert publicado[papel] == embutido[papel], papel


def test_catalogo_na_versao_3_recebe_a_lista_corrigida(monkeypatch) -> None:
    """Quem não atualizar o catálogo não pode herdar o defeito em silêncio.

    É o mesmo padrão dos ramos de versão 2 e 3: substituir a lista e
    dizer por quê, em vez de servir a publicada e deixar o pipeline
    quebrar em português.
    """
    from climasus4py.utils import data as _data

    publicado = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8")
    )
    antigo = dict(publicado)
    antigo["schema_version"] = 3
    antigo["role_priority"] = {
        "date": ["death_date", "DTOBITO"], "cause": ["underlying_cause"],
        "age": ["age"], "sex": ["sex"], "municipality": ["CODMUNRES"],
        "state": ["state"],
    }

    _data.load_datasus_columns_spec.cache_clear()
    monkeypatch.setattr(_data, "load_json", lambda rel: antigo)
    try:
        with pytest.warns(UserWarning, match="M123"):
            spec = _data.load_datasus_columns_spec()
        assert "data_obito" in spec["role_priority"]["date"]
    finally:
        _data.load_datasus_columns_spec.cache_clear()


# --- a varredura --------------------------------------------------------


def test_os_sistemas_principais_funcionam_nos_tres_idiomas() -> None:
    """A medição que fechou o M123, reduzida ao que cabe num teste.

    A varredura completa cobriu os 43 sistemas do catálogo e deu 0
    falhas; aqui ficam os três que representam famílias diferentes de
    coluna crua, para a regressão aparecer sem custar um minuto de suíte.
    """
    conn = get_connection()
    casos = {
        "SIM-DO": {"DTOBITO": ["01012020"], "CODMUNRES": ["310620"],
                   "CAUSABAS": ["J189"], "IDADE": ["465"], "SEXO": ["1"]},
        "SIH-RD": {"DT_INTER": ["01012020"], "CODMUNRES": ["310620"],
                   "DIAG_PRINC": ["J189"], "IDADE": ["70"], "SEXO": ["1"]},
        "SINAN-DENG": {"DT_NOTIFIC": ["01012020"], "ID_MUNICIP": ["310620"],
                       "NU_IDADE_N": ["4030"], "CS_SEXO": ["M"]},
    }
    falhas: list[str] = []
    for sistema, dados in casos.items():
        nome = f"_m123_{sistema.replace('-', '_')}"
        conn.register(nome, pd.DataFrame(dados))
        for lang in IDIOMAS:
            colunas = list(
                cs.sus_data_standardize(
                    conn.table(nome), system=sistema, lang=lang
                ).columns
            )
            if detect_date_column(colunas) is None:
                falhas.append(f"{sistema}/{lang}/date")
            if detect_geo_column(colunas, level="municipality") is None:
                falhas.append(f"{sistema}/{lang}/municipality")
    assert falhas == [], falhas
