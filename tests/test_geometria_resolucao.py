"""M9: a geometria por linha inflava o Parquet, e a causa não era a codificação.

O registro mediu 331,9 MB no Python contra 2,1 MB no R, para as mesmas
~5.196 linhas, e propôs duas correções: guardar a geometria em WKB, ou
tirá-la da tabela de fatos e servi-la numa tabela de dimensão. **Medi as
duas, e nenhuma resolve:**

| opção                                   | tamanho | vs hoje |
|---|---|---|
| WKT por linha (o que havia)             | 45,3 MB | — |
| WKB por linha (proposta do registro)    | 44,6 MB | 1,0x |
| GeoParquet (o default do R para `sf`)   | 232,3 MB | **5x pior** |
| fatos + dimensão (proposta do registro) | 31,0 MB | 1,5x, igual a só usar zstd |
| **geometria simplificada, como o R**    | **2,1 MB** | **15x** |

WKB não ganha nada porque o Parquet já dicionariza o WKT repetido, e sob
zstd o binário compacta *pior* que o texto. A tabela de dimensão só dá
1,5x porque, com 853 municípios distintos, a geometria *única* já são
31 MB — o custo é a geometria, não a repetição dela.

A causa real estava em uma palavra no builder do catálogo:

    # climasus-data/scripts/build_spatial.py
    municipalities = geobr.read_municipality(simplified=False)   # ← aqui

enquanto o climasus4r pede o contrário:

    # climasus4r
    geobr::read_municipality(code_muni = "all", simplified = TRUE)

Medido: 1.141.965 vértices no arquivo simplificado do geobr contra ~18
milhões no de resolução cheia.

**O que se perde, medido nos 5.570 municípios:** o ponto representativo
— que é a única coisa que o `_match_spatial` extrai da geometria — anda
uma mediana de 7,5 m, e 3 municípios passam de 1 km. Com a rede real do
INMET, *nenhum* município troca de estação mais próxima. Os maiores
deslocamentos não são erro de simplificação: em Moiporá/GO o centroide
cai fora do polígono cheio e dentro do simplificado, então os dois lados
usam algoritmos diferentes (centroide contra ponto interno).

Nenhum consumidor deste pacote precisa do detalhe extra: o join tira um
ponto, o coroplético desenha em escala estadual ou nacional, e a busca
por nome de município não lê geometria nenhuma.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.utils import data as _data
from climasus4py.utils.data import (
    SPATIAL_LAYERS,
    spatial_asset_path,
)


@pytest.fixture(autouse=True)
def _limpa_memoria_do_aviso():
    """O aviso do fallback é uma vez por processo; sem isto a ordem importa.

    `_AVISOU_SEM_SIMPLIFICADO` existe para não repetir o aviso a cada
    chamada, e o efeito colateral é que o primeiro teste a acioná-lo
    cala todos os seguintes. Um teste que depende de qual outro rodou
    antes não mede nada.
    """
    _data._AVISOU_SEM_SIMPLIFICADO.clear()
    yield
    _data._AVISOU_SEM_SIMPLIFICADO.clear()


# --- o resolvedor -------------------------------------------------------


def test_o_default_e_a_geometria_simplificada() -> None:
    """Que é o que o R usa. É esta linha que fecha o M9."""
    assert spatial_asset_path("municipalities").name.endswith(
        "_simplified.parquet"
    )


def test_resolucao_cheia_continua_acessivel() -> None:
    """Simplificar é o default, não uma amputação.

    O arquivo de resolução cheia continua no catálogo: quem precisa de
    fronteira exata pede, e o detalhe não se regenera offline.
    """
    p = spatial_asset_path("municipalities", simplified=False)
    assert p.name == "municipalities.parquet"
    assert p.is_file()


@pytest.mark.parametrize("layer", SPATIAL_LAYERS)
def test_as_tres_camadas_existem_nas_duas_resolucoes(layer: str) -> None:
    for simplified in (True, False):
        p = spatial_asset_path(layer, simplified=simplified)
        assert p.is_file(), f"{p.name} ausente"


def test_camada_desconhecida_levanta() -> None:
    with pytest.raises(ValueError, match="Unknown spatial layer"):
        spatial_asset_path("cep")


def test_fallback_avisa_quando_falta_o_simplificado(tmp_path, monkeypatch) -> None:
    """O silêncio aqui custaria 10x no arquivo de saída sem explicação.

    Quem atualiza o pacote e não o catálogo receberia um Parquet muito
    maior e procuraria a causa no próprio código.
    """
    (tmp_path / "assets" / "spatial").mkdir(parents=True)
    cheio = tmp_path / "assets" / "spatial" / "municipalities.parquet"
    cheio.write_bytes(b"")
    monkeypatch.setattr(_data, "data_path", lambda rel: tmp_path / rel)

    with pytest.warns(UserWarning, match="M9"):
        p = spatial_asset_path("municipalities")
    assert p == cheio


def test_o_aviso_do_fallback_sai_uma_vez_por_camada(tmp_path, monkeypatch) -> None:
    """Contrapartida: avisar a cada chamada num laço afogaria o resto."""
    (tmp_path / "assets" / "spatial").mkdir(parents=True)
    monkeypatch.setattr(_data, "data_path", lambda rel: tmp_path / rel)

    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        for _ in range(5):
            spatial_asset_path("municipalities")
    assert len(capturados) == 1


def test_sem_fallback_nao_avisa() -> None:
    """E com o ativo presente o caminho normal é silencioso."""
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        spatial_asset_path("municipalities")
    relevantes = [w for w in capturados if "M9" in str(w.message)]
    assert relevantes == []


# --- os dois ativos descrevem o mesmo país ------------------------------


@pytest.mark.parametrize("layer", SPATIAL_LAYERS)
def test_simplificado_tem_o_mesmo_esquema_e_as_mesmas_linhas(layer: str) -> None:
    """Simplificar muda a geometria, e não o conjunto de municípios.

    Se o arquivo simplificado perdesse linhas, trocar o default faria
    municípios desaparecerem silenciosamente de todo resultado — e a
    agregação é POR município, então um município ausente não reporta
    zero: ele sai do relatório.
    """
    conn = get_connection()

    def ler(simplified: bool):
        p = spatial_asset_path(layer, simplified=simplified)
        return conn.sql(f"SELECT * FROM read_parquet('{p.as_posix()}')")

    cheio, simples = ler(False), ler(True)
    assert cheio.columns == simples.columns

    codigo = cheio.columns[0]
    p_cheio = spatial_asset_path(layer, simplified=False).as_posix()
    p_simples = spatial_asset_path(layer, simplified=True).as_posix()
    faltando = conn.sql(f"""
        SELECT count(*) FROM (
            SELECT {codigo} FROM read_parquet('{p_cheio}')
            EXCEPT
            SELECT {codigo} FROM read_parquet('{p_simples}')
        )
    """).fetchone()[0]
    assert faltando == 0


def test_o_simplificado_e_menor_de_fato() -> None:
    """A asserção que dá sentido a todas as outras."""
    cheio = spatial_asset_path("municipalities", simplified=False)
    simples = spatial_asset_path("municipalities", simplified=True)
    assert simples.stat().st_size < cheio.stat().st_size / 3


def test_a_geometria_simplificada_continua_valida() -> None:
    """Simplificar pode produzir polígono inválido; medido que não produziu.

    Era o risco concreto: o `preserve_topology` do shapely existe
    exatamente porque a simplificação ingênua cria auto-interseção — e
    geometria inválida quebra o `overlay`, que é o M49.
    """
    shapely = pytest.importorskip("shapely")
    conn = get_connection()
    p = spatial_asset_path("municipalities").as_posix()
    # Uma amostra, porque validar 5.570 multipolígonos é caro num teste.
    wkts = conn.sql(f"""
        SELECT geometry_wkt FROM read_parquet('{p}')
        WHERE geometry_wkt IS NOT NULL USING SAMPLE 150 ROWS
    """).df()["geometry_wkt"]
    invalidos = [
        w[:40] for w in wkts if not shapely.from_wkt(w).is_valid
    ]
    assert invalidos == [], f"geometria inválida: {invalidos[:3]}"


# --- o efeito no sus_spatial_join ---------------------------------------


@pytest.fixture
def rel_saude():
    """5.196 linhas em municípios de MG — o cenário que o registro mediu."""
    conn = get_connection()
    p = spatial_asset_path("municipalities", simplified=False).as_posix()
    codigos = conn.sql(f"""
        SELECT DISTINCT LEFT(CAST(code_muni AS VARCHAR), 6) AS c
        FROM read_parquet('{p}') WHERE state = 'MG' ORDER BY c
    """).df()["c"].tolist()
    n = 5196
    df = pd.DataFrame({
        "CODMUNRES": [codigos[i % len(codigos)] for i in range(n)],
        "date": pd.date_range("2020-01-01", periods=n, freq="h").date,
        "idade": [(i * 7) % 90 + 1 for i in range(n)],
    })
    conn.register("_m9_saude", df)
    return conn.table("_m9_saude")


def test_o_join_simplificado_escreve_parquet_muito_menor(rel_saude, tmp_path) -> None:
    """A medição do M9, agora como teste.

    Mede o arquivo, e não a string de WKT, porque é o arquivo que
    inviabilizava salvar o resultado de um estado grande.
    """
    tamanhos = {}
    for simplified in (False, True):
        out = cs.sus_spatial_join(rel_saude, simplified=simplified)
        p = tmp_path / f"saida_{simplified}.parquet"
        out.to_parquet(str(p).replace("\\", "/"), compression="snappy")
        tamanhos[simplified] = p.stat().st_size

    assert tamanhos[True] < tamanhos[False] / 5, (
        f"simplificado {tamanhos[True]/1e6:.1f} MB contra cheio "
        f"{tamanhos[False]/1e6:.1f} MB — ganho abaixo do medido (11,9x)"
    )


def test_o_join_nao_perde_nem_duplica_linha(rel_saude) -> None:
    """A resolução não pode mudar a contagem.

    É um LEFT JOIN por código, então não deveria — mas os dois ativos são
    arquivos diferentes, e um município a mais ou a menos num deles
    apareceria aqui como linha perdida.
    """
    n_entrada = len(rel_saude.df())
    contagens = {
        s: len(cs.sus_spatial_join(rel_saude, simplified=s).df())
        for s in (False, True)
    }
    assert contagens[True] == contagens[False] == n_entrada


def test_o_join_registra_a_resolucao_no_historico(rel_saude) -> None:
    """Sem isto, um resultado salvo não diz em que resolução foi feito.

    Dois arquivos com os mesmos números e geometrias diferentes, e nada
    no metadado para distinguir, é exatamente o tipo de coisa que não se
    reconstrói meses depois.
    """
    for simplified in (False, True):
        out = cs.sus_spatial_join(rel_saude, simplified=simplified)
        hist = str(cs.sus_meta(out, "history"))
        assert f"simplified={simplified}" in hist


def test_spatial_path_explicito_ignora_o_simplified(rel_saude, tmp_path) -> None:
    """Quem nomeia um arquivo manda no arquivo.

    `simplified` escolhe entre os ativos do catálogo; não faz sentido
    reescrever um caminho que a pessoa passou.
    """
    conn = get_connection()
    p = tmp_path / "meu.parquet"
    conn.sql(f"""
        COPY (SELECT '310620' AS code_muni, 'X' AS name,
                     'POINT (-43.9 -19.9)' AS geometry_wkt)
        TO '{p.as_posix()}' (FORMAT PARQUET)
    """)
    out = cs.sus_spatial_join(rel_saude, spatial_path=p, simplified=True)
    assert "meu.parquet" in str(cs.sus_meta(out, "history"))


# --- o ponto representativo, que é o que o clima consome ---------------


def test_o_ponto_representativo_quase_nao_se_move() -> None:
    """O risco real da simplificação, medido em vez de suposto.

    O `_match_spatial` não usa a fronteira: ele reduz cada município a um
    ponto e procura a estação mais próxima. Então a pergunta não é quanto
    a fronteira mudou, é quanto o ponto andou.
    """
    shapely = pytest.importorskip("shapely")
    import numpy as np

    conn = get_connection()
    pc = spatial_asset_path("municipalities", simplified=False).as_posix()
    ps = spatial_asset_path("municipalities", simplified=True).as_posix()
    par = conn.sql(f"""
        SELECT c.code_muni, c.geometry_wkt AS cheio, s.geometry_wkt AS simples
        FROM read_parquet('{pc}') c
        JOIN read_parquet('{ps}') s USING (code_muni)
        WHERE c.geometry_wkt IS NOT NULL AND s.geometry_wkt IS NOT NULL
        USING SAMPLE 300 ROWS
    """).df()

    def ponto(w):
        g = shapely.from_wkt(w)
        c = g.centroid
        if c.is_empty or not g.contains(c):
            c = g.representative_point()
        return c.x, c.y

    dist = []
    for _, linha in par.iterrows():
        x1, y1 = ponto(linha["cheio"])
        x2, y2 = ponto(linha["simples"])
        dlat = (y2 - y1) * 111.32
        dlon = (x2 - x1) * 111.32 * np.cos(np.radians(y1))
        dist.append(float(np.hypot(dlat, dlon)))

    mediana = float(np.median(dist))
    # Medido em todos os 5.570: mediana de 7,5 m. A folga é para a
    # amostragem do teste, não para um afrouxamento do critério.
    assert mediana < 0.1, f"mediana de {mediana*1000:.0f} m, esperado ~7,5 m"
    # E a rede do INMET tem estações a dezenas de km: um deslocamento
    # desta ordem não realoca ninguém.
    assert max(dist) < 10.0, f"máximo de {max(dist):.1f} km"
