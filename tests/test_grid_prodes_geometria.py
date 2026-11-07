"""M49: o `sus_grid_prodes` quebrava na interseção, por tipos mistos.

O achado: o download das camadas do PRODES no TerraBrasilis **funciona** —
diferente das seis grades bloqueadas do M41, aqui a fonte responde — e a
falha vem depois, no processamento geométrico:

    Erro ao processar interseção para MataAtlantica 2023:
    df1 contains mixed geometry types

e a função então aborta com "Nenhum dado foi processado com sucesso", uma
mensagem que culpa a chamada quando o problema está no dado de origem.

**Reproduzido sinteticamente**, que é o que permite testar sem depender do
serviço externo: `gpd.overlay` levanta `NotImplementedError: df1 contains
mixed geometry types` quando o frame tem mais de uma família geométrica.

E medido, no geopandas 1.1.0, o que de fato dispara:

| conteúdo após `make_valid()`    | `overlay` |
|---|---|
| `Polygon` + `MultiPolygon`      | passa |
| `GeometryCollection` + `Polygon`| passa |
| `Polygon` + geometria vazia     | passa |
| **`Polygon` + `LineString`**    | **levanta** |

O `make_valid()` que o código já chamava **não resolve, e pode causar**:
um anel auto-intersectante volta como `MultiPolygon` (inofensivo), mas uma
lasca de largura zero volta como `LineString`, e um polígono com espícula
como `GeometryCollection`.

A correção é normalizar antes da interseção: explodir as multipartes e as
coleções, descartar o que não for polígono e descartar as vazias. Uma
linha tem área zero — que é o que a função soma adiante — então nada
mensurável se perde; a alternativa era perder o bioma/ano inteiro.
"""

from __future__ import annotations

import warnings

import pytest

gpd = pytest.importorskip("geopandas")
shapely = pytest.importorskip("shapely")

from shapely.geometry import (  # noqa: E402
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
)

from climasus4py.enrichment.grid_prodes import (  # noqa: E402
    _intersect_and_aggregate,
    _polygons_only,
)

MSG = {"intersect_warn": "erro {biome} {year}: {err}"}


def _muni(*geoms, codigos=None):
    codigos = codigos or [f"1501{i:02d}" for i in range(len(geoms))]
    return gpd.GeoDataFrame({"code_muni": codigos}, geometry=list(geoms),
                            crs="EPSG:4326")


def _defor(*geoms, ano=2023):
    return gpd.GeoDataFrame(
        {"uid": list(range(len(geoms))), "year": [ano] * len(geoms)},
        geometry=list(geoms), crs="EPSG:4326")


QUADRADO = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
MENOR = Polygon([(0.2, 0.2), (1.2, 0.2), (1.2, 1.2), (0.2, 1.2)])


class TestOverlayRecusaTiposMistos:
    """O que o geopandas faz, medido — a premissa do achado."""

    def test_poligono_com_linha_levanta(self):
        d = _defor(MENOR, LineString([(0, 0), (1, 1)]))
        with pytest.raises(NotImplementedError, match="mixed geometry types"):
            gpd.overlay(d, _muni(QUADRADO), how="intersection")

    @pytest.mark.parametrize("extra", [
        MultiPolygon([Polygon([(1.5, 1.5), (1.9, 1.5), (1.9, 1.9)])]),
        GeometryCollection([Point(0.5, 0.5),
                            Polygon([(1.5, 1.5), (1.9, 1.5), (1.9, 1.9)])]),
        Polygon(),
    ])
    def test_o_que_NAO_levanta(self, extra):
        """Contrapartida: nem toda mistura dispara.

        Sem isto, a correção pareceria necessária por motivos que não são
        os verdadeiros -- e a próxima pessoa procuraria o defeito no lugar
        errado.
        """
        d = _defor(MENOR, extra)
        gpd.overlay(d, _muni(QUADRADO), how="intersection")


class TestNormalizacao:

    @pytest.mark.parametrize(("geoms", "esperado"), [
        ((MENOR, LineString([(0, 0), (1, 1)])), 1),
        ((MENOR, MultiPolygon([Polygon([(1.5, 1.5), (1.9, 1.5),
                                        (1.9, 1.9)])])), 2),
        ((MENOR, GeometryCollection([Point(0.5, 0.5),
                                     Polygon([(1.5, 1.5), (1.9, 1.5),
                                              (1.9, 1.9)])])), 2),
        ((MENOR, Polygon()), 1),
        ((MENOR, Point(0.5, 0.5)), 1),
    ])
    def test_sobra_so_poligono(self, geoms, esperado):
        limpo = _polygons_only(_defor(*geoms))
        assert set(limpo.geom_type) <= {"Polygon"}
        assert len(limpo) == esperado

    def test_camada_so_de_linha_fica_vazia(self):
        limpo = _polygons_only(_defor(LineString([(0, 0), (1, 1)])))
        assert limpo.empty

    def test_frame_vazio_passa_direto(self):
        vazio = _defor()
        assert _polygons_only(vazio).empty

    def test_nao_mexe_no_que_ja_esta_certo(self):
        d = _defor(MENOR, Polygon([(1.5, 1.5), (1.9, 1.5), (1.9, 1.9)]))
        limpo = _polygons_only(d)
        assert len(limpo) == 2
        assert list(limpo["uid"]) == [0, 1], "as colunas de atributo seguem"

    def test_a_area_nao_muda(self):
        """Explodir e filtrar não pode alterar a área que será somada."""
        d = _defor(MENOR, LineString([(0, 0), (1, 1)]))
        assert _polygons_only(d).geometry.area.sum() == pytest.approx(
            MENOR.area)


class TestIntersecaoNaoQuebraMais:

    def test_o_caso_do_achado_agrega(self):
        """Linha junto de polígono: era o erro, agora é uma linha de saída."""
        r = _intersect_and_aggregate(
            _defor(MENOR, LineString([(0, 0), (1, 1)])),
            _muni(QUADRADO, codigos=["150140"]),
            "MataAtlantica", 2023, False, MSG)
        assert r is not None
        assert list(r["code_muni"]) == ["150140"]
        assert r["n_patches"].iloc[0] == 1, "a linha não conta como polígono"
        assert r["deforested_area_km2"].iloc[0] > 0

    def test_camada_so_de_linha_devolve_None_e_nao_levanta(self):
        """None é "pula este bioma/ano", e não erro.

        O comentário do código explica por quê: devolver None aqui só
        pula o item, enquanto levantar abortaria o laço externo no
        primeiro bioma sem dado.
        """
        r = _intersect_and_aggregate(
            _defor(LineString([(0, 0), (1, 1)])),
            _muni(QUADRADO), "Pampa", 2023, False, MSG)
        assert r is None

    def test_sem_sobreposicao_devolve_None(self):
        """O caminho que já existia continua valendo."""
        longe = Polygon([(50, 50), (51, 50), (51, 51)])
        r = _intersect_and_aggregate(
            _defor(longe), _muni(QUADRADO), "Amazon", 2023, False, MSG)
        assert r is None

    def test_a_area_somada_e_a_do_poligono_valido(self):
        """Contrapartida da correção: descartar a linha não pode mudar a soma."""
        so_poligono = _intersect_and_aggregate(
            _defor(MENOR), _muni(QUADRADO, codigos=["150140"]),
            "MataAtlantica", 2023, False, MSG)
        com_linha = _intersect_and_aggregate(
            _defor(MENOR, LineString([(0, 0), (1, 1)])),
            _muni(QUADRADO, codigos=["150140"]),
            "MataAtlantica", 2023, False, MSG)
        assert com_linha["deforested_area_km2"].iloc[0] == pytest.approx(
            so_poligono["deforested_area_km2"].iloc[0])


class TestMunicipioPerdidoAvisa:
    """Descartar uma lasca é uma coisa; descartar um município é outra.

    A agregação é POR município, então um município descartado desaparece
    do resultado em vez de reportar zero — e desaparecer calado é o
    defeito que esta correção não pode introduzir.
    """

    def test_o_municipio_sem_poligono_e_nomeado(self):
        antes = _muni(QUADRADO, LineString([(5, 5), (6, 6)]),
                      codigos=["150140", "150150"])
        limpo = _polygons_only(antes)
        assert set(limpo["code_muni"]) == {"150140"}
        assert set(antes["code_muni"]) - set(limpo["code_muni"]) == {"150150"}

    def test_nenhum_municipio_valido_nao_avisa(self):
        """Sem perda, sem aviso — ruído gratuito é como um aviso real passa."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            _polygons_only(_muni(QUADRADO, codigos=["150140"]))
