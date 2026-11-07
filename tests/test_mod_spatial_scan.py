"""sus_mod_spatial_scan contra o SpatialEpi 1.2.8 rodando no R.

O R delega ao ``SpatialEpi::kulldorff()``; aqui a estatistica foi
portada direto para NumPy. As fixtures deste arquivo sao a saida do R
sobre 42 municipios sinteticos em UTM 23S, com um excesso plantado em
quatro deles, mais uma grade de 300 configuracoes de duas areas usada
para identificar o que o C++ do SpatialEpi faz com os totais (M85).

Tudo menos o p-valor e deterministico: as zonas candidatas, a
verossimilhanca de cada uma e portanto a identidade dos aglomerados
reproduzem o R exatamente. O p-valor vem de sorteio Monte Carlo e nem o
R nem esta porta aceitam semente, entao ele so concorda em distribuicao.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment import mod_spatial_scan as mss
from climasus4py.enrichment.mod_spatial_scan import (
    _all_log_lkhd,
    _llr_binomial,
    _llr_poisson,
    _zones,
)

FIXTURES = Path(__file__).parent / "fixtures" / "spatial_scan"

#: Os quatro municipios onde o excesso foi plantado ao gerar a fixture.
PLANTADOS = ["3505004", "3505005", "3506004", "3506005"]


@pytest.fixture(scope="module")
def municipios() -> gpd.GeoDataFrame:
    return gpd.read_file(FIXTURES / "scan_muni.gpkg", layer="muni")


@pytest.fixture(scope="module")
def dados() -> pd.DataFrame:
    return pd.read_parquet(FIXTURES / "scan_df.parquet")


@pytest.fixture(scope="module")
def referencia() -> dict:
    return json.loads((FIXTURES / "scan_ref.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def ordenado(municipios, dados) -> pd.DataFrame:
    """Municipios e dados unidos e ordenados como o R os ordena."""
    return municipios.merge(dados, on="code_muni").sort_values(
        "code_muni", kind="stable"
    )


@pytest.fixture(scope="module")
def centroides_r() -> pd.DataFrame:
    return pd.read_parquet(FIXTURES / "scan_centroids_r.parquet").sort_values(
        "code_muni"
    )


@pytest.fixture(scope="module")
def saida(dados, municipios) -> dict:
    return cs.sus_mod_spatial_scan(
        dados, "obitos", "pop", municipios, n_simulations=499, verbose=False
    )


class TestEnumeracaoDeZonas:
    """`zones()` do SpatialEpi: 15 linhas de R, portadas direto."""

    def test_mesmas_zonas_que_o_r(self, centroides_r, ordenado):
        ref = pd.read_parquet(FIXTURES / "scan_llr_r.parquet")
        geo = centroides_r[["x", "y"]].to_numpy()
        nb = _zones(geo, ordenado["pop"].to_numpy(float), 0.5)

        centros = np.repeat(np.arange(len(nb)), [len(v) for v in nb]) + 1
        fins = np.concatenate(nb) + 1

        assert len(fins) == len(ref) == 835
        assert np.array_equal(centros, ref["center"].to_numpy())
        assert np.array_equal(fins, ref["end"].to_numpy())

    def test_o_proprio_centro_vem_primeiro(self, centroides_r, ordenado):
        """Distancia zero a si mesmo, entao cada lista comeca no centro."""
        geo = centroides_r[["x", "y"]].to_numpy()
        nb = _zones(geo, ordenado["pop"].to_numpy(float), 0.5)

        assert [int(v[0]) for v in nb] == list(range(len(nb)))

    def test_o_teto_de_populacao_e_respeitado(self, centroides_r, ordenado):
        pop = ordenado["pop"].to_numpy(float)
        for frac in (0.1, 0.25, 0.5):
            nb = _zones(centroides_r[["x", "y"]].to_numpy(), pop, frac)
            maior = max(pop[v].sum() / pop.sum() for v in nb)
            assert maior <= frac


class TestVerossimilhanca:
    """A varredura de verossimilhanca, que no R esta em C++."""

    def test_binomial_em_todas_as_835_zonas(self, centroides_r, ordenado):
        ref = pd.read_parquet(FIXTURES / "scan_llr_r.parquet")
        geo = centroides_r[["x", "y"]].to_numpy()
        pop = ordenado["pop"].to_numpy(float)
        nb = _zones(geo, pop, 0.5)
        meu = np.concatenate(
            _all_log_lkhd(ordenado["obitos"].to_numpy(float), pop, nb, "binomial")
        )
        d = np.abs(meu - ref["llr"].to_numpy())

        # 1e-8 e o erro de acumulacao do proprio C++ do R, nao da porta:
        # avaliada em dupla precisao a expressao fecha em ~2e-10.
        assert d.max() < 1e-7
        assert (d / np.maximum(np.abs(ref["llr"]), 1.0)).max() < 1e-7

    def test_poisson_em_todas_as_835_zonas(self, centroides_r, ordenado):
        ref = pd.read_parquet(FIXTURES / "poi_llr_r.parquet")
        geo = centroides_r[["x", "y"]].to_numpy()
        pop = ordenado["pop"].to_numpy(float)
        nb = _zones(geo, pop, 0.5)
        meu = np.concatenate(
            _all_log_lkhd(
                ordenado["obitos"].to_numpy(float),
                ordenado["esperado"].to_numpy(float),
                nb,
                "poisson",
            )
        )
        d = np.abs(meu - ref["llr"].to_numpy())

        assert d.max() < 1e-9

    def test_zonas_de_baixa_taxa_zeram(self, centroides_r, ordenado):
        """A varredura e unilateral: so procura excesso."""
        ref = pd.read_parquet(FIXTURES / "poi_llr_r.parquet")
        geo = centroides_r[["x", "y"]].to_numpy()
        nb = _zones(geo, ordenado["pop"].to_numpy(float), 0.5)
        meu = np.concatenate(
            _all_log_lkhd(
                ordenado["obitos"].to_numpy(float),
                ordenado["esperado"].to_numpy(float),
                nb,
                "poisson",
            )
        )

        assert ((meu == 0) == (ref["llr"].to_numpy() == 0)).all()
        assert (meu >= 0).all()


@pytest.fixture(scope="module")
def grade() -> pd.DataFrame:
    return pd.read_parquet(FIXTURES / "grid_poi.parquet")


class TestTotaisTruncados:
    """O C++ do SpatialEpi trunca os totais do estudo (M85).

    Totais de casos, de esperados e de populacao sao truncados para
    inteiro; as somas por zona seguem em dupla precisao. Como contagem
    esperada e taxa vezes populacao, ela praticamente nunca e inteira --
    ou seja o modelo nulo e ajustado contra um total que falta ate um
    caso.

    A grade de 300 configuracoes de duas areas serve justamente para
    fixar isso: sem o truncamento o erro chega a 1,8% da estatistica.
    """

    def test_com_truncamento_bate_exato(self, grade):
        cz, ez, C, E, r = (grade[c].to_numpy() for c in
                           ["cz", "ez", "C", "E", "llr"])
        meu = _llr_poisson(cz, ez, np.trunc(C), np.trunc(E))

        assert np.abs(meu - r).max() < 1e-9

    def test_sem_truncamento_diverge(self, grade):
        """O que a estatistica correta daria -- e o quanto ela difere."""
        cz, ez, C, E, r = (grade[c].to_numpy() for c in
                           ["cz", "ez", "C", "E", "llr"])
        correto = _llr_poisson(cz, ez, C, E)
        nao_zero = r != 0
        rel = np.abs(correto - r)[nao_zero] / np.abs(r[nao_zero])

        assert rel.max() > 0.01          # ate 1,8% nesta grade
        assert np.median(rel) > 1e-5

    def test_o_desvio_nao_acompanha_o_tamanho_do_estudo(self, grade):
        """Descarta-se no maximo um caso, mas o efeito na estatistica nao
        segue o tamanho do estudo: nesta grade a correlacao entre a fracao
        descartada e o erro relativo e nula. O erro e grande onde a
        estatistica e pequena, que e justamente a regiao de decisao."""
        cz, ez, C, E, r = (grade[c].to_numpy() for c in
                           ["cz", "ez", "C", "E", "llr"])
        correto = _llr_poisson(cz, ez, C, E)
        nao_zero = (r != 0) & (correto != 0)
        rel = np.abs(correto - r)[nao_zero] / np.abs(r[nao_zero])
        descartado = ((E - np.trunc(E)) / E)[nao_zero]

        assert abs(np.corrcoef(descartado, rel)[0, 1]) < 0.15
        assert (E - np.trunc(E)).max() < 1.0
        # o erro relativo e maior justamente onde a estatistica e menor
        assert np.corrcoef(np.log(np.abs(r[nao_zero])), np.log(rel))[0, 1] < -0.5

    def test_um_estudo_pequeno_sofre_muito(self):
        """O caso concreto: total esperado 12,6 truncado para 12 desloca a
        estatistica em 18%. Numeros conferidos contra o R."""
        cz, ez, C, E = np.array([10.0]), np.array([4.3]), 19.0, 12.6
        do_r = _llr_poisson(cz, ez, np.trunc(C), np.trunc(E))[0]
        correto = _llr_poisson(cz, ez, C, E)[0]

        assert do_r == pytest.approx(1.112624681044, abs=1e-9)   # R
        assert correto == pytest.approx(1.364321126777, abs=1e-9)
        assert abs(correto - do_r) / correto > 0.18

    def test_a_chave_desliga_o_truncamento(self, grade, monkeypatch):
        cz, ez = grade["cz"].to_numpy(), grade["ez"].to_numpy()
        monkeypatch.setattr(mss, "SPATIALEPI_TRUNCATES_TOTALS", False)
        assert mss._total(np.array([4090.66245])) == pytest.approx(4090.66245)
        monkeypatch.setattr(mss, "SPATIALEPI_TRUNCATES_TOTALS", True)
        assert mss._total(np.array([4090.66245])) == 4090.0

    def test_totais_inteiros_nao_sao_afetados(self):
        """Por isso o caminho binomial com populacao inteira bate sem
        que o truncamento apareca."""
        cz = np.array([30.0])
        nz = np.array([1000.0])
        com = _llr_binomial(cz, nz, np.trunc(40.0), np.trunc(5000.0))
        sem = _llr_binomial(cz, nz, 40.0, 5000.0)

        assert com == pytest.approx(sem, abs=1e-12)


class TestParidadeDaFuncao:
    """A funcao inteira, contra o climasus4r."""

    def test_binomial(self, saida, referencia):
        r = referencia["binomial"]
        m = saida["most_likely_cluster"]

        assert list(m["location_ids"]) == list(r["ids"])
        assert m["observed"] == pytest.approx(r["observed"], abs=0)
        assert m["expected"] == pytest.approx(r["expected"], abs=1e-9)
        assert m["RR"] == pytest.approx(r["RR"], abs=1e-12)
        assert m["log_lik"] == pytest.approx(r["log_lik"], abs=1e-7)

    def test_poisson(self, dados, municipios, referencia):
        r = referencia["poisson"]
        out = cs.sus_mod_spatial_scan(
            dados, "obitos", "pop", municipios, expected="esperado",
            n_simulations=499, verbose=False,
        )
        m = out["most_likely_cluster"]

        assert list(m["location_ids"]) == list(r["ids"])
        assert m["expected"] == pytest.approx(r["expected"], abs=1e-9)
        assert m["RR"] == pytest.approx(r["RR"], abs=1e-12)
        assert m["log_lik"] == pytest.approx(r["log_lik"], abs=1e-9)

    def test_a_tabela_de_dados(self, saida):
        ref = pd.read_parquet(FIXTURES / "scan_data_r.parquet")
        got = saida["data"]

        assert list(got.columns) == ["code_muni", "cases", "population",
                                     "expected", "in_mlc"]
        assert list(got["code_muni"]) == list(ref["code_muni"])
        for col in ("cases", "population", "expected"):
            assert np.abs(got[col].to_numpy() - ref[col].to_numpy()).max() < 1e-9
        assert (got["in_mlc"].to_numpy() == ref["in_mlc"].to_numpy()).all()

    def test_encontra_o_aglomerado_plantado(self, saida):
        achados = sorted(saida["most_likely_cluster"]["location_ids"])

        assert achados == PLANTADOS
        assert saida["most_likely_cluster"]["RR"] > 2.0

    def test_os_centroides_batem_com_o_sf(self, ordenado, centroides_r):
        """st_centroid(st_transform(4326)) vs geopandas: a diferenca e de
        transformacao de datum entre PROJ e GDAL, da ordem de metros, e nao
        chega a mudar a ordenacao de vizinhos a 40 km de distancia."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = ordenado.to_crs(4326).geometry.centroid
        dx = np.abs(c.x.to_numpy() - centroides_r["x"].to_numpy())
        dy = np.abs(c.y.to_numpy() - centroides_r["y"].to_numpy())

        assert dx.max() < 1e-3
        assert dy.max() < 1e-3


class TestMonteCarlo:
    """O p-valor, unica parte nao deterministica."""

    def test_o_aglomerado_plantado_e_significativo(self, saida):
        assert saida["most_likely_cluster"]["p_value"] < 0.05
        assert saida["n_clusters"] >= 1

    def test_o_p_minimo_segue_o_numero_de_simulacoes(self, dados, municipios):
        """Com n simulacoes o menor p possivel e 1/(n+1)."""
        out = cs.sus_mod_spatial_scan(
            dados, "obitos", "pop", municipios, n_simulations=99, verbose=False
        )

        assert out["most_likely_cluster"]["p_value"] >= 1.0 / 100
        assert out["most_likely_cluster"]["p_value"] == pytest.approx(0.01)

    def test_a_verossimilhanca_nao_depende_do_sorteio(self, dados, municipios):
        """Duas chamadas: mesmo aglomerado e mesma estatistica."""
        kw = dict(cases="obitos", population="pop", municipalities=municipios,
                  n_simulations=99, verbose=False)
        a = cs.sus_mod_spatial_scan(dados, **kw)["most_likely_cluster"]
        b = cs.sus_mod_spatial_scan(dados, **kw)["most_likely_cluster"]

        assert a["location_ids"] == b["location_ids"]
        assert a["log_lik"] == pytest.approx(b["log_lik"], abs=0)


class TestEntradas:
    def test_recusa_dataframe_sem_geometria(self, dados):
        with pytest.raises(TypeError, match="GeoDataFrame"):
            cs.sus_mod_spatial_scan(dados, "obitos", "pop", dados, verbose=False)

    def test_recusa_coluna_ausente(self, dados, municipios):
        with pytest.raises(ValueError, match="nao encontrada"):
            cs.sus_mod_spatial_scan(dados, "inexistente", "pop", municipios,
                                    verbose=False)

    def test_recusa_sem_code_muni(self, dados, municipios):
        with pytest.raises(ValueError, match="code_muni"):
            cs.sus_mod_spatial_scan(dados.drop(columns="code_muni"), "obitos",
                                    "pop", municipios, verbose=False)

    def test_expected_inexistente_cai_no_binomial(self, dados, municipios, capsys):
        """Como no R: avisa e segue como se fosse None."""
        out = cs.sus_mod_spatial_scan(
            dados, "obitos", "pop", municipios, expected="nao_existe",
            n_simulations=99, verbose=True,
        )

        assert out["meta"]["expected_col"] is None
        assert "nao_existe" in capsys.readouterr().err

    def test_sem_correspondencia_nenhuma(self, dados, municipios):
        outro = dados.assign(code_muni="99" + dados["code_muni"])
        with pytest.raises(ValueError, match="Nenhuma linha"):
            cs.sus_mod_spatial_scan(outro, "obitos", "pop", municipios,
                                    verbose=False)

    def test_aceita_relacao_duckdb(self, dados, municipios):
        import duckdb

        rel = duckdb.connect().from_df(dados)
        out = cs.sus_mod_spatial_scan(rel, "obitos", "pop", municipios,
                                      n_simulations=99, verbose=False)

        assert sorted(out["most_likely_cluster"]["location_ids"]) == PLANTADOS

    def test_municipio_sem_dado_e_removido_com_aviso(self, dados, municipios, capsys):
        parcial = dados.iloc[:-3]
        out = cs.sus_mod_spatial_scan(parcial, "obitos", "pop", municipios,
                                      n_simulations=99, verbose=True)

        assert out["meta"]["n_municipalities"] == len(parcial)
        assert "3" in capsys.readouterr().err

    @pytest.mark.parametrize("lang", ["pt", "en", "es"])
    def test_as_tres_linguas(self, dados, municipios, lang, capsys):
        cs.sus_mod_spatial_scan(dados, "obitos", "pop", municipios,
                                n_simulations=99, lang=lang, verbose=True)

        assert capsys.readouterr().err.strip()


class TestMeta:
    def test_ecoa_os_argumentos(self, saida):
        m = saida["meta"]

        assert m["cases_col"] == "obitos"
        assert m["population_col"] == "pop"
        assert m["expected_col"] is None
        assert m["max_pop_frac"] == 0.5
        assert m["n_simulations"] == 499
        assert m["alpha"] == 0.05
        assert m["n_municipalities"] == 42

    def test_nao_reproduz_o_call_do_r(self, saida):
        """Mesma convencao de spatial_weights e spatial_moran."""
        assert "call" not in saida
