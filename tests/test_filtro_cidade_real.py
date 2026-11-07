"""M121: o filtro `city=` nunca funcionou, e os testes que havia não viam.

Três defeitos empilhados em `expand_city_to_codes`, cada um escondendo o
seguinte:

1. **Caminho errado.** Lia `spatial/municipalities.parquet`; o ativo está
   em `assets/spatial/`. Levantava `FileNotFoundError` mandando rodar
   `cs.update_climasus_data()` — que não podia ajudar, porque o arquivo
   estava lá desde sempre, sob o outro prefixo.

2. **Apelido de coluna ausente.** Corrigido o caminho, a lista de
   apelidos era `("municipality_code", "code", "codigo", "cod_mun",
   "codmun")` e a coluna do ativo é **`code_muni`**, que não casa com
   nenhum. Trocava um erro por outro: `ValueError: must have name and
   code columns`.

3. **Dígitos a mais, e este é o pior.** `code_muni` é o código IBGE de
   sete dígitos; o `sus_filter` monta `"CODMUNRES" IN (...)` como
   comparação exata de string, e `CODMUNRES` tem seis. Então o filtro
   casava **zero linhas, sem erro** — devolvia uma relação vazia, que é
   um número publicável e errado.

**Por que sobreviveu:** os dois testes que existiam mockavam o caminho
inteiro. Um trocava `data_path` por um `tmp_path` vazio para exercitar o
`FileNotFoundError`; o outro substituía `expand_city_to_codes` por
`lambda _c: ["355030"]`. Nenhum dos dois abriu o arquivo real — e o
comentário no cabeçalho deles dizia isso em voz alta: "mocked —
municipalities.parquet may not be present". O defeito estava justamente
no pedaço que o mock removia.

Estes testes usam o ativo de verdade, e pulam quando ele não está
presente, que é a única forma de a cobertura significar algo aqui.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.utils.data import expand_city_to_codes, spatial_asset_path

pytestmark = pytest.mark.skipif(
    not spatial_asset_path("municipalities", simplified=False).is_file(),
    reason="ativo de municípios ausente",
)


# --- (1) e (2): a função resolve um nome -------------------------------


@pytest.mark.parametrize(
    ("cidade", "codigo"),
    [
        ("Belo Horizonte", "310620"),
        ("São Paulo", "355030"),
        ("Recife", "261160"),
        ("Manaus", "130260"),
    ],
)
def test_resolve_nome_para_codigo(cidade: str, codigo: str) -> None:
    """Contra o arquivo real. Antes, qualquer um destes levantava."""
    assert expand_city_to_codes(cidade) == [codigo]


def test_acento_e_opcional() -> None:
    """Quem digita num teclado sem acento não deve receber erro."""
    assert expand_city_to_codes("Sao Paulo") == expand_city_to_codes("São Paulo")
    assert expand_city_to_codes("BELO HORIZONTE") == ["310620"]


def test_nome_inexistente_levanta_com_o_nome_do_arquivo() -> None:
    with pytest.raises(ValueError, match="not found"):
        expand_city_to_codes("Zzzzville")


def test_homonimos_avisam_e_devolvem_todos() -> None:
    """"Bom Jesus" existe em cinco estados, e escolher um em silêncio seria pior.

    O aviso é o ponto: o filtro vai usar os cinco códigos, e quem pediu
    "Bom Jesus" quase certamente queria um.
    """
    with pytest.warns(UserWarning, match="matches 5 municipalities"):
        codigos = expand_city_to_codes("Bom Jesus")
    assert len(codigos) == 5


def test_o_aviso_de_homonimo_nomeia_os_estados() -> None:
    """"casa com 5" não é acionável; "casa com 5 (PB, PI, RN, RS, SC)" é.

    É o que o `resolve_city_input_internal` do R faz, mostrando o
    `uf_code` de cada um.
    """
    with pytest.warns(UserWarning) as capturado:
        expand_city_to_codes("Bom Jesus")
    msg = str(capturado[0].message)
    for uf in ("PI", "RN", "PB", "SC", "RS"):
        assert uf in msg


def test_santarem_da_paraiba_virou_joca_claudino() -> None:
    """A troca de fonte mudou um resultado, e vale fixar qual.

    O `municipalities.parquet` vem do geobr de 2010 e chama 2513653 de
    "Santarém"; o `municipio_meta` -- que é a fonte que o R usa -- traz o
    nome atual, "Joca Claudino", porque o município foi renomeado. Então
    `city="Santarém"` passou a resolver só o do Pará, o que concorda com
    o R e é o nome vigente. Não é um município perdido: ele responde pelo
    nome de hoje e pelo código.
    """
    assert expand_city_to_codes("Santarém") == ["150680"]
    assert expand_city_to_codes("Joca Claudino") == ["251365"]
    assert expand_city_to_codes("2513653") == ["251365"]


def test_lista_de_cidades() -> None:
    codigos = expand_city_to_codes(["Belo Horizonte", "Recife"])
    assert codigos == ["310620", "261160"]


# --- paridade com resolve_city_input_internal do R ---------------------


@pytest.mark.parametrize("entrada", ["3106200", "310620"])
def test_aceita_codigo_de_seis_ou_sete_digitos(entrada: str) -> None:
    """O R aceita, via `grepl("^\\\\d{6,7}$")`, e agora o Python também.

    Quem tem o código na mão não deveria ser obrigado a trocar de
    parâmetro para usá-lo.
    """
    assert expand_city_to_codes(entrada) == ["310620"]


def test_codigo_inexistente_levanta_dizendo_que_e_codigo() -> None:
    """A mensagem distingue código de nome; "not found" sozinho não ajuda."""
    with pytest.raises(ValueError, match="Municipality code"):
        expand_city_to_codes("999999")


def test_grafia_errada_sugere_a_certa() -> None:
    """O R sugere via `suggest_city_matches_internal`; um "not found" seco
    deixa a pessoa sem saber se errou o nome ou se o município não existe.
    """
    with pytest.raises(ValueError, match="Did you mean.*Recife"):
        expand_city_to_codes("Recif")


def test_nome_sem_nenhuma_semelhanca_nao_inventa_sugestao() -> None:
    """Contrapartida: sugerir qualquer coisa seria pior que não sugerir."""
    with pytest.raises(ValueError) as exc:
        expand_city_to_codes("Zzzzville")
    assert "Did you mean" not in str(exc.value)


def test_le_o_municipio_meta_e_nao_o_arquivo_de_geometria() -> None:
    """495 KB em vez de 29,6 MB, e é a fonte que o R lê.

    Esta busca traduz nome em código e nunca toca geometria — abrir o
    arquivo de polígonos para isso era desperdício puro.
    """
    from climasus4py.utils.data import _municipio_meta

    df = _municipio_meta()
    assert "geometry_wkt" not in df.columns
    assert {"municipio", "name"} <= set(df.columns)


def test_municipio_meta_e_cacheado() -> None:
    """Sem cache, cada cidade de uma lista reabriria o parquet."""
    from climasus4py.utils.data import _municipio_meta

    assert _municipio_meta() is _municipio_meta()


# --- (3) o defeito silencioso: seis dígitos ---------------------------


def test_os_codigos_tem_seis_digitos() -> None:
    """O defeito que não levantava erro nenhum.

    `code_muni` no ativo tem sete dígitos. O `sus_filter` compara string
    exata contra `CODMUNRES`, que tem seis — então sete dígitos casavam
    zero linhas e o resultado saía vazio, sem aviso.
    """
    for cidade in ("Belo Horizonte", "São Paulo", "Manaus"):
        for codigo in expand_city_to_codes(cidade):
            assert len(codigo) == 6, f"{cidade} -> {codigo!r}"
            assert codigo.isdigit()


def test_o_codigo_de_seis_digitos_e_o_prefixo_do_de_sete() -> None:
    """Truncar não pode ser reescrever.

    Se o truncamento pegasse os seis dígitos errados, o filtro casaria
    outro município em vez de nenhum — o que é pior, porque parece
    funcionar.
    """
    conn = get_connection()
    p = spatial_asset_path("municipalities").as_posix()
    sete = conn.sql(f"""
        SELECT CAST(code_muni AS VARCHAR) AS c FROM read_parquet('{p}')
        WHERE lower(name) = 'belo horizonte'
    """).fetchone()[0]
    assert sete.startswith(expand_city_to_codes("Belo Horizonte")[0])


# --- de ponta a ponta: o filtro filtra -------------------------------


@pytest.fixture
def rel_com_municipios():
    conn = get_connection()
    df = pd.DataFrame({
        "CODMUNRES": ["310620", "355030", "261160", "310620", "999999"],
        "CAUSABAS": ["J189"] * 5,
    })
    conn.register("_m121", df)
    return conn.table("_m121")


@pytest.mark.parametrize(
    ("cidade", "esperado"),
    [("Belo Horizonte", 2), ("São Paulo", 1), ("Recife", 1)],
)
def test_o_filtro_devolve_as_linhas_da_cidade(
    rel_com_municipios, cidade: str, esperado: int
) -> None:
    """A asserção que o M121 é. Antes, todas devolviam zero."""
    out = cs.sus_filter(rel_com_municipios, city=cidade)
    assert len(out.df()) == esperado


def test_o_filtro_nao_devolve_vazio_em_silencio(rel_com_municipios) -> None:
    """A contrapartida do defeito (3), dita como asserção.

    Zero linhas sem erro era o estado anterior, e é o que nenhum teste
    pegava. Aqui um resultado vazio falha.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = cs.sus_filter(rel_com_municipios, city="Belo Horizonte")
    assert len(out.df()) > 0


def test_o_filtro_com_lista_soma_as_cidades(rel_com_municipios) -> None:
    out = cs.sus_filter(rel_com_municipios, city=["Belo Horizonte", "Recife"])
    assert len(out.df()) == 3
