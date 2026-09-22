"""M8: o `sus_spatial_join` devolvia nome e polígono, e mais nada.

Qualquer análise por estado ou por região exigia um join manual contra
outra tabela, porque a saída trazia só `spatial_name` e `geometry_wkt`.

**Duas coisas que o registro dizia não se confirmaram**, e vale dizer
quais, porque cada uma mandava procurar no lugar errado:

1. **"O R devolve 13 colunas geográficas."** A lista do registro inclui
   `date`, `year.x`, `year.y` e `astronomical_season`, que não são
   geográficas — e `year.x`/`year.y` são o *artefato* de um join com
   coluna duplicada, que a própria planilha de paridade já classificava
   como defeito do R. O que o `geobr` entrega de fato, conferido, é
   `code_muni`, `name_muni`, `code_state`, `abbrev_state`, `geometry`; o
   `sus_spatial_join` do R acrescenta `code_muni_7`. `name_region` e
   `code_region` vêm do `sus_census_join`, outra função.

2. **"O R também cria a coluna `date`."** Não cria. As únicas linhas com
   `date` no `sus_spatial_join` do R são `spatial_var$date <- NULL` (que
   *apaga*) e a leitura de `result_sf$date` para montar o metadado
   temporal. Ele **consome** `date`; quem cria é o
   `.data_aggregate_tibble_internal`, no passo de agregação — e o
   `sus_data_aggregate` do Python já produz `date` desde a convergência
   do M52.

`code_state` e `code_region` saem do próprio código IBGE do município —
os dois primeiros dígitos e o primeiro — em vez de uma tabela de
tradução que poderia divergir. Conferido que a correspondência é
um-para-um nos 27 estados.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.enrichment.spatial import (
    _case_from_map,
    _region_names,
    _state_names,
)

#: Um município por região, para cobrir as cinco.
MUNICIPIOS = {
    "150680": ("Santarém", "15", "PA", "Pará", "1", "Norte"),
    "261160": ("Recife", "26", "PE", "Pernambuco", "2", "Nordeste"),
    "310620": ("Belo Horizonte", "31", "MG", "Minas Gerais", "3", "Sudeste"),
    "431490": ("Porto Alegre", "43", "RS", "Rio Grande do Sul", "4", "Sul"),
    "520870": ("Goiânia", "52", "GO", "Goiás", "5", "Centro-Oeste"),
}

NOVAS = [
    "code_muni_7", "code_state", "abbrev_state",
    "name_state", "code_region", "name_region",
]


@pytest.fixture
def rel():
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": list(MUNICIPIOS),
        "CAUSABAS": ["J189"] * len(MUNICIPIOS),
    })
    conn.register("_m8_rel", df)
    return conn.table("_m8_rel")


# --- as colunas que faltavam --------------------------------------------


def test_o_join_devolve_estado_e_regiao(rel) -> None:
    """A asserção que o M8 é."""
    out = cs.sus_spatial_join(rel)
    for coluna in NOVAS:
        assert coluna in out.columns, coluna


def test_os_valores_estao_certos_nas_cinco_regioes(rel) -> None:
    """Uma região errada não aparece num teste de presença de coluna."""
    d = cs.sus_spatial_join(rel).df().set_index("CODMUNRES")
    for codigo, esperado in MUNICIPIOS.items():
        linha = d.loc[codigo]
        obtido = (
            linha["spatial_name"], linha["code_state"], linha["abbrev_state"],
            linha["name_state"], linha["code_region"], linha["name_region"],
        )
        assert obtido == esperado, codigo


def test_o_codigo_de_estado_e_o_prefixo_do_codigo_do_municipio(rel) -> None:
    """A derivação, afirmada como invariante em vez de suposta.

    É por isso que não há tabela de tradução aqui: a relação está no
    próprio código, e uma tabela poderia divergir dele.
    """
    d = cs.sus_spatial_join(rel).df()
    assert (d["code_state"] == d["code_muni_7"].str[:2]).all()
    assert (d["code_region"] == d["code_muni_7"].str[:1]).all()


def test_o_codigo_de_sete_digitos_convive_com_o_de_seis(rel) -> None:
    """Os dois, que é o ponto do `code_muni_7` no R.

    A coluna de saúde tem 6 dígitos e o código IBGE completo tem 7; ter
    só um dos dois é o que obriga a alinhamentos manuais (ver M1).
    """
    d = cs.sus_spatial_join(rel).df()
    assert (d["code_muni_7"].str.len() == 7).all()
    assert (d["code_muni_7"].str[:6] == d["CODMUNRES"]).all()


def test_nenhuma_coluna_nova_veio_nula(rel) -> None:
    """Coluna presente e inteira nula passaria no teste de presença."""
    d = cs.sus_spatial_join(rel).df()
    for coluna in NOVAS:
        assert d[coluna].notna().all(), coluna


# --- idioma -------------------------------------------------------------


@pytest.mark.parametrize(
    ("lang", "estado", "regiao"),
    [("pt", "Pará", "Norte"), ("en", "Pará", "North"), ("es", "Pará", "Norte")],
)
def test_os_rotulos_seguem_o_idioma(rel, lang: str, estado: str, regiao: str) -> None:
    d = cs.sus_spatial_join(rel, lang=lang).df().set_index("CODMUNRES")
    assert d.loc["150680", "name_state"] == estado
    assert d.loc["150680", "name_region"] == regiao


def test_o_default_e_pt_como_no_r(rel) -> None:
    """O `sus_spatial_join` do R tem `lang = "pt"`."""
    import inspect

    assert inspect.signature(cs.sus_spatial_join).parameters["lang"].default == "pt"


def test_idioma_desconhecido_cai_no_portugues() -> None:
    """Melhor o rótulo em português que um KeyError ou um None."""
    assert _state_names("tlh")["PA"] == "Pará"
    assert _region_names("tlh")["norte"] == "Norte"


def test_os_cinco_slugs_de_regiao_tem_rotulo() -> None:
    """Um slug sem rótulo viraria NULL em name_region, em silêncio."""
    nomes = _region_names("pt")
    for slug in ("norte", "nordeste", "sudeste", "sul", "centro_oeste"):
        assert nomes.get(slug), slug


def test_os_27_estados_tem_nome() -> None:
    nomes = _state_names("pt")
    assert len(nomes) == 27
    assert all(nomes.values())


# --- colisão de nomes ---------------------------------------------------


def test_coluna_que_ja_existe_nao_e_duplicada(rel) -> None:
    """R sufixaria a colisão; nós preservamos e avisamos.

    O `year.x`/`year.y` do R é exatamente esse comportamento, e deixa
    duas colunas onde quem chamou esperava uma — sem dizer qual é qual.
    """
    conn = get_connection()
    df = rel.df()
    df["code_state"] = "99"
    conn.register("_m8_colisao", df)

    with pytest.warns(UserWarning, match="already has code_state"):
        out = cs.sus_spatial_join(conn.table("_m8_colisao"))

    assert sum(1 for c in out.columns if c == "code_state") == 1
    assert out.df()["code_state"].tolist() == ["99"] * len(df)


def test_sem_colisao_nao_avisa(rel) -> None:
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        cs.sus_spatial_join(rel)
    assert [w for w in capturados if "already has" in str(w.message)] == []


def test_a_colisao_fica_no_historico(rel) -> None:
    """O aviso some do notebook; o metadado fica com o resultado."""
    conn = get_connection()
    df = rel.df()
    df["name_region"] = "X"
    conn.register("_m8_hist", df)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = cs.sus_spatial_join(conn.table("_m8_hist"))
    assert "skipped (already present): name_region" in str(
        cs.sus_meta(out, "history")
    )


def test_juntar_duas_vezes_nao_quebra(rel) -> None:
    """Todas as candidatas colidem, e a SQL ficaria com vírgula solta.

    Improvável, mas é o caso que a montagem da projeção tem de
    sobreviver — e "improvável" não é "impossível" num pipeline que
    alguém reexecuta.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        uma = cs.sus_spatial_join(rel)
        duas = cs.sus_spatial_join(uma)
    assert list(duas.columns) == list(uma.columns)
    assert len(duas.df()) == len(uma.df())


# --- o CASE ------------------------------------------------------------


def test_case_de_mapa_vazio_vira_null() -> None:
    """Sem isto a SQL sairia como `CASE x END`, que não compila."""
    assert _case_from_map("s.state", {}) == "NULL"


def test_case_escapa_aspas() -> None:
    """Nome de estado com apóstrofo quebraria a SQL montada por string."""
    sql = _case_from_map("s.x", {"a'b": "c'd"})
    assert "''" in sql


# --- o que o registro dizia e não era -----------------------------------


def test_o_date_vem_da_agregacao_e_nao_do_join(rel) -> None:
    """O registro atribuía `date` ao `sus_spatial_join`. Não é dele.

    No R, as únicas linhas com `date` nessa função apagam
    (`spatial_var$date <- NULL`) ou leem para o metadado temporal. Quem
    cria é o passo de agregação, nos dois pacotes.
    """
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": list(MUNICIPIOS),
        "DTOBITO": ["01012020", "15022020", "20032020", "05042020", "10052020"],
        "CAUSABAS": ["J189"] * 5,
    })
    conn.register("_m8_date", df)
    bruto = conn.table("_m8_date")

    assert "date" not in cs.sus_spatial_join(bruto).columns
    agregado = cs.sus_data_aggregate(
        bruto, time_unit="month", system="SIM-DO", lang="pt", verbose=False
    )
    assert "date" in agregado.columns


def test_o_join_preserva_o_date_de_quem_ja_o_tem() -> None:
    """E é assim que o encadeamento com o clima funciona sem remendo.

    A queixa do registro era ter de atribuir `date` à mão antes do
    `sus_climate_aggregate`. Padronizando e agregando antes de juntar,
    como o pipeline faz, o `date` atravessa.

    O `lang="en"` aqui **não** é estilo: é o único idioma em que esta
    cadeia funciona hoje. O `role_priority` do `datasus_columns.json` só
    lista os nomes padronizados em inglês, então depois de
    `sus_data_standardize(lang="pt")` o `sus_data_aggregate` levanta
    "Date column not found. [...] run sus_data_standardize() first" — a
    função que acabou de rodar. Medido em 26 de 44 combinações de
    sistema × idioma × papel. É o M123, e não é deste teste resolver;
    quando for, este teste deve passar a varrer os três idiomas.
    """
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": list(MUNICIPIOS),
        "DTOBITO": ["01012020", "15022020", "20032020", "05042020", "10052020"],
        "CAUSABAS": ["J189"] * 5,
    })
    conn.register("_m8_chain", df)
    padronizado = cs.sus_data_standardize(
        conn.table("_m8_chain"), system="SIM-DO", lang="en"
    )
    agregado = cs.sus_data_aggregate(
        padronizado, time_unit="month", system="SIM-DO", lang="en",
        verbose=False,
    )
    juntado = cs.sus_spatial_join(agregado)
    assert "date" in juntado.columns
    assert juntado.df()["date"].notna().all(), "data não converteu"
    # e as colunas do M8 chegaram junto
    for coluna in NOVAS:
        assert coluna in juntado.columns


def test_m123_a_cadeia_em_portugues_ainda_quebra() -> None:
    """Fixa o defeito **enquanto ele existe**, para que o conserto apareça.

    Sem isto, o M123 seria só uma linha num CSV. Quando o
    `role_priority` ganhar os nomes em português, este teste falha — e é
    esse o sinal de que o teste acima pode varrer os três idiomas.
    """
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": list(MUNICIPIOS),
        "DTOBITO": ["01012020", "15022020", "20032020", "05042020", "10052020"],
        "CAUSABAS": ["J189"] * 5,
    })
    conn.register("_m123", df)
    padronizado = cs.sus_data_standardize(
        conn.table("_m123"), system="SIM-DO", lang="pt"
    )
    assert "data_obito" in padronizado.columns
    with pytest.raises(ValueError, match="Date column not found"):
        cs.sus_data_aggregate(
            padronizado, time_unit="month", system="SIM-DO", lang="pt",
            verbose=False,
        )


# --- o contrato lazy ----------------------------------------------------


def test_continua_lazy(rel) -> None:
    """Nada disto pode materializar a relação.

    Os rótulos entram como `CASE` inline justamente por isso: uma tabela
    de tradução registrada prenderia o resultado a uma conexão.
    """
    import duckdb

    out = cs.sus_spatial_join(rel)
    assert isinstance(out, duckdb.DuckDBPyRelation)
