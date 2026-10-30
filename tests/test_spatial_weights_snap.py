"""M172: o `snap` do sus_mod_spatial_weights precisa valer de verdade.

Ate 30/10/2026 o parametro existia so por paridade de assinatura. O valor
resolvido chegava apenas na mensagem de progresso, que imprimia
`snap = 0.001` enquanto o codigo construia contiguidade pelo
`libpysal.weights.Queen`, que exige VERTICE COMPARTILHADO -- coordenada
identica, bit a bit. Fronteira municipal digitalizada em escalas
diferentes nao tem coordenada identica: tem coordenada PROXIMA. Cada vao
de micrograu sumia com um vizinho sem avisar.

Medido com o MESMO WKT dos dois lados, caixas envolventes batendo em
todos os municipios nos quatro cantos -- logo divergencia 100%
algoritmica e nao de geometria:

    queen      R      py antes     py depois
    SP      3.600   3.520 (-80)      3.600   exato
    MG      4.778   3.658 (-1.120)   4.778   exato
    BA      2.374   1.674 (-700)     2.376   +4/-2

MG perdia 23% dos lacos e a Bahia 29%; SP, com 2%, era o caso BENIGNO --
e foi o unico medido antes, o que fez a diferenca passar por
arredondamento no PARIDADE ("5,67 contra 5,57, praticamente identico").

O estrago tambem nao parava nas arestas perdidas: o estilo "W" divide por
cardinalidade, entao perder um vizinho reescreve o peso de TODOS os
outros daquele municipio. Em SP, 333 das 3.520 arestas que os dois lados
tinham EM COMUM saiam com peso diferente -- 9,5% da matriz. A soma total
dos pesos batia exatamente (644 dos dois lados), porque a normalizacao
estava certa; conferir a soma nao revelaria nada.

SOBRE O QUE O `snap` NAO FAZ. Ele nao fecha vao de 111 m, nem aqui nem no
R, apesar de 1e-3 grau ser o default dos dois. Medido no R com dois
quadrados e vao crescente: vao 0 e 1e-9 viram vizinhanca, 1e-7 (1 cm) em
diante nao. Em geometria esferica o candidato sai do `st_intersects()`,
com a tolerancia do s2, e o snap so CONFIRMA. Os testes abaixo amarram
isso para que ninguem "conserte" o snap para 111 m achando que esta
aproximando do R.

A MATRIZ DE CASOS e gabarito colhido do proprio climasus4r (ver
scratchpad/matriz_casos_r.R). Sintetica de proposito: quadrados com
medida exata, sem depender de parquet baixado, para a regra ficar
amarrada e nao a amostra.
"""

from __future__ import annotations

import pytest

gpd = pytest.importorskip("geopandas", reason="exige o extra [spatial]")
pytest.importorskip("libpysal", reason="exige o extra [spatial]")
shapely = pytest.importorskip("shapely")

import climasus4py as cs

_B = shapely.geometry.box

#: (nome, poligono B, queen esperado, rook esperado). O poligono A e
#: sempre o quadrado unitario. Os esperados vieram do climasus4r.
CASOS = [
    # Encostam pela lateral inteira, vertices coincidindo: o caso facil.
    ("lateral_exata",    _B(1, 0, 2, 1),          True,  True),
    # So o canto: e a definicao de queen contra rook.
    ("canto",            _B(1, 1, 2, 2),          True,  False),
    # B encosta no MEIO da lateral de A, sem nenhum vertice proximo.
    # Tocar nao basta nos dois pacotes: tem de haver ponto de fronteira
    # perto de ponto de fronteira.
    ("meio_sem_vertice", _B(1, 0.25, 2, 0.75),    False, False),
    # Vao de um nanograu: fecha.
    ("vao_1e9",          _B(1 + 1e-9, 0, 2, 1),   True,  True),
    # Vao de 1,1 m: NAO fecha, embora seja 100x menor que o snap.
    ("vao_1e5",          _B(1 + 1e-5, 0, 2, 1),   False, False),
    # Vao de 222 m, o DOBRO do snap: se um dia isto virar vizinhanca, o
    # Python se afastou do R em vez de se aproximar.
    ("vao_2e3",          _B(1 + 2e-3, 0, 2, 1),   False, False),
    # Sobrepoem de leve, com vertices proximos mas NAO identicos: e o
    # caso real da digitalizacao, e o que o libpysal puro perdia.
    ("sobrepoe_1e6",     _B(1 - 1e-6, 0, 2, 1),   True,  True),
    ("sobrepoe_1e4",     _B(1 - 1e-4, 0, 2, 1),   True,  True),
    # Sobreposicao maior que o snap: ja nao e divisa mal digitalizada.
    ("sobrepoe_1e2",     _B(1 - 1e-2, 0, 2, 1),   False, False),
    ("sobrepoe_muito",   _B(0.9, 0, 2, 1),        False, False),
]

#: Os tres casos em que o caminho de topologia exata (snap=0, o Queen
#: nativo do libpysal) discorda do R. Sao a razao do M172 existir.
PERDIDOS_PELO_CAMINHO_EXATO = {"vao_1e9", "sobrepoe_1e6", "sobrepoe_1e4"}


def _par(b) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"code_muni": ["A", "B"]}, geometry=[_B(0, 0, 1, 1), b], crs=4674
    )


def _sao_vizinhos(g, *, queen=True, snap=None) -> bool:
    w = cs.sus_mod_spatial_weights(g, queen=queen, snap=snap, verbose=False)
    return bool(w["nb"]["A"])


@pytest.mark.parametrize(
    ("nome", "b", "queen_esperado", "rook_esperado"),
    CASOS,
    ids=[c[0] for c in CASOS],
)
def test_a_matriz_de_casos_bate_com_o_r(nome, b, queen_esperado, rook_esperado):
    g = _par(b)
    assert _sao_vizinhos(g, queen=True) is queen_esperado, f"{nome}: queen"
    assert _sao_vizinhos(g, queen=False) is rook_esperado, f"{nome}: rook"


@pytest.mark.parametrize(
    ("nome", "b"), [(c[0], c[1]) for c in CASOS], ids=[c[0] for c in CASOS]
)
def test_o_caminho_exato_erra_exatamente_os_tres_conhecidos(nome, b):
    """snap=0 pede topologia exata; aqui esta o que isso custa.

    Nao e um teste do que esta certo -- e a medida do buraco, para que ele
    nao volte sem ninguem perceber.
    """
    esperado = dict((c[0], c[2]) for c in CASOS)[nome]
    exato = _sao_vizinhos(_par(b), queen=True, snap=0)
    if nome in PERDIDOS_PELO_CAMINHO_EXATO:
        assert exato is not esperado, f"{nome} deixou de ser um caso perdido"
    else:
        assert exato is esperado, f"{nome} passou a divergir no caminho exato"


class TestOPesoAcompanha:
    """Perder um vizinho reescreve o peso dos OUTROS -- o dano indireto.

    E o mecanismo que fez 333 das 3.520 arestas COMUNS de SP sairem com
    peso diferente: a aresta existe dos dois lados e mesmo assim muda de
    valor, porque o divisor mudou.
    """

    def _tres(self, *, com_c: bool) -> gpd.GeoDataFrame:
        # C sobrepoe B de leve (entra) ou fica a 222 m (nao entra).
        c = _B(2 - 1e-6, 0, 3, 1) if com_c else _B(2 + 2e-3, 0, 3, 1)
        return gpd.GeoDataFrame(
            {"code_muni": ["A", "B", "C"]},
            geometry=[_B(0, 0, 1, 1), _B(1, 0, 2, 1), c],
            crs=4674,
        )

    def test_o_peso_de_AB_muda_conforme_C_entra_ou_nao(self):
        com = cs.sus_mod_spatial_weights(self._tres(com_c=True), verbose=False)
        sem = cs.sus_mod_spatial_weights(self._tres(com_c=False), verbose=False)

        assert sorted(com["nb"]["B"]) == ["A", "C"]
        assert com["listw"].weights["B"] == pytest.approx([0.5, 0.5])

        assert sem["nb"]["B"] == ["A"]
        assert sem["listw"].weights["B"] == pytest.approx([1.0])


def test_a_soma_dos_pesos_nao_denuncia_nada():
    """Guarda contra a conferencia que PARECE suficiente e nao e.

    Em SP a soma batia exatamente (644 dos dois lados) com 80 arestas de
    diferenca, porque o estilo "W" normaliza por linha: a soma e o numero
    de municipios com vizinho, nao importa quem seja o vizinho.
    """
    com = cs.sus_mod_spatial_weights(
        gpd.GeoDataFrame(
            {"code_muni": ["A", "B", "C"]},
            geometry=[_B(0, 0, 1, 1), _B(1, 0, 2, 1), _B(2 - 1e-6, 0, 3, 1)],
            crs=4674,
        ),
        verbose=False,
    )
    sem = cs.sus_mod_spatial_weights(
        gpd.GeoDataFrame(
            {"code_muni": ["A", "B", "C"]},
            geometry=[_B(0, 0, 1, 1), _B(1, 0, 2, 1), _B(2 - 1e-6, 0, 3, 1)],
            crs=4674,
        ),
        snap=0,
        verbose=False,
    )
    soma = lambda w: sum(p for c in w["listw"].id_order
                         for p in w["listw"].weights[c])
    assert soma(com) == pytest.approx(3.0)
    assert soma(sem) == pytest.approx(2.0)
    # Os dois tem soma inteira e "redonda", e mesmo assim os grafos
    # diferem: C e vizinho de B num e ilha no outro.
    assert sorted(com["nb"]["B"]) == ["A", "C"]
    assert sem["nb"]["C"] == []
