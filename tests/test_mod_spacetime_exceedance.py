"""sus_mod_spacetime_exceedance contra o climasus4r.

As fixtures sao um ajuste REAL: INLA 26.8.7 rodando o
sus_mod_spacetime_bayes sobre um painel de 42 municipios x 8 periodos,
BYM2 espacial, RW1 temporal, Poisson com offset de esperados, uma
covariavel climatica. O excesso plantado CRESCE no tempo em quatro
municipios.

A funcao nao depende do INLA -- le a tabela `rr` de um ajuste pronto --
entao e portavel hoje, mesmo com o sus_mod_spacetime_bayes ainda stub.
Estes testes tambem fixam a FORMA do objeto ajustado.

Dois defeitos do R ficam registrados aqui: M89 (a funcao de ajuste nao
roda no Windows) e M90 (o time_idx e indice mas e lido como ano).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

import climasus4py as cs
from climasus4py.enrichment.mod_spacetime_exceedance import (
    EXCEEDANCE_REPORT_CUTOFF,
    REQUIRED_RR_COLS,
    _sigma_log,
    _threshold_suffix,
)

FIX = Path(__file__).parent / "fixtures" / "spacetime"
FIX_GEO = Path(__file__).parent / "fixtures" / "spatial_scan"


@pytest.fixture(scope="module")
def rr() -> pd.DataFrame:
    return pd.read_parquet(FIX / "st_rr.parquet")


@pytest.fixture(scope="module")
def ajuste(rr) -> dict:
    """O objeto climasus_spacetime_bayes, reduzido ao que a funcao le."""
    return {
        "rr": rr,
        "fixed": pd.read_parquet(FIX / "st_fixed.parquet"),
        "spatial_re": pd.read_parquet(FIX / "st_spatial_re.parquet"),
        "temporal_re": pd.read_parquet(FIX / "st_temporal_re.parquet"),
        "n_areas": 42,
        "n_times": 8,
    }


@pytest.fixture(scope="module")
def municipios():
    import geopandas as gpd

    return gpd.read_file(FIX_GEO / "scan_muni.gpkg", layer="muni")


def _cmp(got: pd.DataFrame, ref: pd.DataFrame, tol: float = 1e-12) -> None:
    assert list(got.columns) == list(ref.columns)
    assert len(got) == len(ref)
    for col in ref.columns:
        if pd.api.types.is_numeric_dtype(ref[col]):
            d = np.abs(got[col].to_numpy(float) - ref[col].to_numpy(float))
            assert d.max() < tol, col
        else:
            assert (got[col].astype(str) == ref[col].astype(str)).all(), col


class TestFormaDoAjuste:
    """O contrato que um ajustador em Python tera de cumprir."""

    def test_as_colunas_de_rr(self, rr):
        assert list(rr.columns)[:5] == list(REQUIRED_RR_COLS)
        assert len(rr) == 42 * 8

    def test_o_intervalo_contem_a_media(self, rr):
        assert (rr["rr_lower95"] <= rr["rr_mean"]).all()
        assert (rr["rr_mean"] <= rr["rr_upper95"]).all()

    def test_o_intercepto_esta_presente(self, ajuste):
        """Contraste com o M88: o caminho do INLA extrai o intercepto
        corretamente; quem o perde e o caminho do CARBayes."""
        assert "(Intercept)" in set(ajuste["fixed"]["term"])

    def test_o_desvio_padrao_vem_preenchido(self, ajuste):
        """Outro contraste com o caminho do CARBayes, onde sd e sempre NA."""
        assert ajuste["fixed"]["sd"].notna().all()


class TestParidade:
    def test_padrao(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)
        _cmp(out["exceedance"], pd.read_parquet(FIX / "exc_default.parquet"))
        ref = pd.read_parquet(FIX / "exc_default_n.parquet")
        assert out["n_exceed"]["n_cells_exceed"].tolist() == \
            ref["n_cells_exceed"].tolist()

    def test_limiares_customizados(self, ajuste):
        """Repetidos e fora de ordem: o R ordena e deduplica."""
        out = cs.sus_mod_spacetime_exceedance(
            ajuste, thresholds=[2, 0.75, 1.25, 2], verbose=False
        )
        _cmp(out["exceedance"], pd.read_parquet(FIX / "exc_custom.parquet"))
        assert list(out["thresholds"]) == [0.75, 1.25, 2.0]

    def test_agregado_por_ano(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(
            ajuste, aggregate_time="year", verbose=False
        )
        _cmp(out["exceedance"], pd.read_parquet(FIX / "exc_year.parquet"))

    def test_a_ordem_das_linhas_agregadas(self, ajuste):
        """R divide por list(code_muni, time_period): o primeiro fator varia
        mais rapido, entao sai ordenado por periodo e depois municipio."""
        out = cs.sus_mod_spacetime_exceedance(
            ajuste, aggregate_time="year", verbose=False
        )
        ref = pd.read_parquet(FIX / "exc_year.parquet")

        assert out["exceedance"]["code_muni"].astype(str).tolist() == \
            ref["code_muni"].astype(str).tolist()
        assert out["exceedance"]["time_period"].tolist() == \
            ref["time_period"].tolist()


class TestTimeIdxLidoComoAno:
    """O time_idx e um INDICE mas a agregacao o le como ANO (M90).

    O sus_mod_spacetime_bayes escreve em rr$time_idx um indice de base 1
    (1, 2, 3, ...), nao o rotulo temporal original. O
    sus_mod_spacetime_exceedance, ao agregar, faz
    as.Date(paste0(as.integer(time_idx), "-01-01")) -- ou seja, trata o
    indice como ano do calendario.

    Duas consequencias, as duas verificadas aqui contra o R:

    1. Os rotulos de periodo saem "0001", "0002", ... em vez dos anos
       de verdade (a fixture foi ajustada sobre 2015-2022).
    2. A agregacao NAO AGREGA NADA: como cada indice ja e unico por
       periodo, entram 336 celulas e saem 336. O parametro nao faz o
       que promete em nenhum ajuste vindo do sus_mod_spacetime_bayes.
    """

    def test_os_rotulos_sao_anos_falsos(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(
            ajuste, aggregate_time="year", verbose=False
        )
        periodos = sorted(out["exceedance"]["time_period"].unique())

        assert periodos == ["000%d" % i for i in range(1, 9)]
        assert "2015" not in periodos          # os anos reais da fixture

    def test_a_agregacao_nao_reduz_nada(self, ajuste):
        sem = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)
        com = cs.sus_mod_spacetime_exceedance(
            ajuste, aggregate_time="year", verbose=False
        )

        assert len(com["exceedance"]) == len(sem["exceedance"]) == 336

    def test_com_time_idx_de_verdade_ela_agrega(self, rr):
        """Se o time_idx trouxesse anos reais repetidos, funcionaria --
        e a prova de que o defeito esta no que o ajuste escreve."""
        real = rr.copy()
        real["time_idx"] = 2015 + (real.groupby("code_muni").cumcount() // 2)
        out = cs.sus_mod_spacetime_exceedance(
            {"rr": real}, aggregate_time="year", verbose=False
        )

        assert sorted(out["exceedance"]["time_period"].unique()) == \
            ["2015", "2016", "2017", "2018"]
        assert len(out["exceedance"]) == 42 * 4      # agregou de 8 para 4


class TestCalculo:
    def test_a_formula_da_probabilidade(self, ajuste):
        """P(RR > t) de uma log-normal reconstruida do IC."""
        out = cs.sus_mod_spacetime_exceedance(ajuste, thresholds=[1.5],
                                              verbose=False)
        rr = ajuste["rr"]
        sigma = (np.log(rr["rr_upper95"]) - np.log(rr["rr_lower95"])) / (2 * 1.96)
        esperado = norm.sf(np.log(1.5), loc=np.log(rr["rr_mean"]), scale=sigma)

        assert np.abs(out["exceedance"]["p_gt_1_5"].to_numpy() - esperado).max() < 1e-12

    def test_probabilidade_entre_zero_e_um(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)
        for c in ("p_gt_1", "p_gt_1_5", "p_gt_2"):
            assert out["exceedance"][c].between(0, 1).all()

    def test_limiar_maior_da_probabilidade_menor(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)
        e = out["exceedance"]

        assert (e["p_gt_1"] >= e["p_gt_1_5"]).all()
        assert (e["p_gt_1_5"] >= e["p_gt_2"]).all()

    def test_intervalo_degenerado_vira_zero_ou_um(self):
        """Sem dispersao, o R cai na comparacao dura rr_mean > t."""
        rr = pd.DataFrame({
            "code_muni": ["1", "2"], "time_idx": [1, 1],
            "rr_mean": [2.5, 0.5], "rr_lower95": [2.5, 0.5],
            "rr_upper95": [2.5, 0.5],
        })
        out = cs.sus_mod_spacetime_exceedance({"rr": rr}, thresholds=[1.0],
                                              verbose=False)

        assert out["exceedance"]["p_gt_1"].tolist() == [1.0, 0.0]

    def test_celulas_invalidas_sao_removidas(self, rr):
        ruim = rr.copy()
        ruim.loc[ruim.index[:5], "rr_mean"] = np.nan
        ruim.loc[ruim.index[5:8], "rr_mean"] = 0.0
        out = cs.sus_mod_spacetime_exceedance({"rr": ruim}, verbose=False)

        assert len(out["exceedance"]) == len(rr) - 8

    def test_a_contagem_usa_o_corte_de_080(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)
        n = int((out["exceedance"]["p_gt_1"] > EXCEEDANCE_REPORT_CUTOFF).sum())

        assert out["n_exceed"].iloc[0]["n_cells_exceed"] == n == 140

    def test_o_excesso_plantado_aparece(self, ajuste):
        """O modelo separa os 4 municipios plantados por seis ordens de
        grandeza: fora deles o maior P(RR > 2) e 3,8e-08, e cada um
        deles tem alguma celula acima de 0,12.

        Note que as celulas de maior probabilidade nao sao uma por
        municipio -- o mesmo municipio ocupa varias, em periodos
        diferentes, porque o excesso plantado CRESCE no tempo.
        """
        plantados = {"3505004", "3505005", "3506004", "3506005"}
        e = cs.sus_mod_spacetime_exceedance(
            ajuste, thresholds=[2.0], verbose=False
        )["exceedance"]
        codigos = e["code_muni"].astype(str)

        assert set(codigos[e.nlargest(8, "p_gt_2").index]) == plantados
        assert set(codigos[e["p_gt_2"] > 0.8]) <= plantados
        assert e.loc[~codigos.isin(plantados), "p_gt_2"].max() < 1e-6
        assert e.loc[codigos.isin(plantados)].groupby(codigos)["p_gt_2"] \
            .max().min() > 0.1


class TestAgregacaoNaoEstreitaOIntervalo:
    """Ao agregar, o sigma e a MEDIA dos sigmas, nao o sigma da media.

    Ou seja agregar n periodos nao estreita o intervalo como estreitaria
    a media de estimativas independentes: o valor agregado carrega a
    incerteza tipica de um periodo. Com termo temporal RW1 as celulas
    sao correlacionadas, entao parte da conservacao se justifica -- o
    quanto nao esta derivado. Replicado do R como esta.
    """

    def test_o_sigma_agregado_e_a_media_dos_sigmas(self):
        rr = pd.DataFrame({
            "code_muni": ["1"] * 4,
            "time_idx": [2020, 2020, 2021, 2021],
            "rr_mean": [1.0, 1.0, 1.0, 1.0],
            "rr_lower95": [0.5, 0.8, 0.5, 0.8],
            "rr_upper95": [2.0, 1.25, 2.0, 1.25],
        })
        out = cs.sus_mod_spacetime_exceedance(
            {"rr": rr}, thresholds=[1.5], aggregate_time="year", verbose=False
        )
        sig = _sigma_log(np.array([0.5, 0.8]), np.array([2.0, 1.25])).mean()
        esperado = norm.sf(np.log(1.5), loc=0.0, scale=sig)

        assert out["exceedance"]["p_gt_1_5"].iloc[0] == pytest.approx(esperado)

    def test_nao_encolhe_com_mais_periodos(self):
        """Dois periodos identicos dao a mesma probabilidade que um."""
        base = dict(code_muni=["1"], rr_mean=[1.0], rr_lower95=[0.5],
                    rr_upper95=[2.0])
        um = pd.DataFrame({**base, "time_idx": [2020]})
        dois = pd.DataFrame({
            "code_muni": ["1", "1"], "time_idx": [2020, 2020],
            "rr_mean": [1.0, 1.0], "rr_lower95": [0.5, 0.5],
            "rr_upper95": [2.0, 2.0],
        })
        kw = dict(thresholds=[1.5], aggregate_time="year", verbose=False)
        p1 = cs.sus_mod_spacetime_exceedance({"rr": um}, **kw)
        p2 = cs.sus_mod_spacetime_exceedance({"rr": dois}, **kw)

        assert p1["exceedance"]["p_gt_1_5"].iloc[0] == \
            pytest.approx(p2["exceedance"]["p_gt_1_5"].iloc[0])


class TestNomeDasColunas:
    @pytest.mark.parametrize(("valor", "esperado"), [
        (1.0, "1"), (1.5, "1_5"), (2.0, "2"), (0.75, "0_75"), (2.25, "2_25"),
        (10.0, "10"),
    ])
    def test_sufixo_como_o_as_character_do_r(self, valor, esperado):
        assert _threshold_suffix(valor) == esperado

    def test_as_colunas_saem_na_ordem_dos_limiares(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(
            ajuste, thresholds=[3, 1, 2], verbose=False
        )

        assert list(out["exceedance"].columns) == [
            "code_muni", "time_idx", "rr_mean", "p_gt_1", "p_gt_2", "p_gt_3",
        ]


class TestGeometria:
    def test_anexa_a_geometria(self, ajuste, municipios):
        import geopandas as gpd

        out = cs.sus_mod_spacetime_exceedance(
            ajuste, municipalities=municipios, verbose=False
        )

        assert isinstance(out["exceedance"], gpd.GeoDataFrame)
        assert len(out["exceedance"]) == 336
        assert out["exceedance"].geometry.notna().all()

    def test_recusa_dataframe_sem_geometria(self, ajuste, municipios):
        plano = pd.DataFrame({"code_muni": municipios["code_muni"]})
        with pytest.raises(TypeError, match="GeoDataFrame"):
            cs.sus_mod_spacetime_exceedance(ajuste, municipalities=plano,
                                            verbose=False)


class TestEntradas:
    def test_recusa_fit_que_nao_e_dict(self, rr):
        with pytest.raises(TypeError, match="sus_mod_spacetime_bayes"):
            cs.sus_mod_spacetime_exceedance(rr, verbose=False)

    def test_recusa_sem_rr(self):
        with pytest.raises(ValueError, match="nao contem"):
            cs.sus_mod_spacetime_exceedance({"fixed": pd.DataFrame()},
                                            verbose=False)

    def test_recusa_rr_incompleto(self, rr):
        with pytest.raises(ValueError, match="rr_lower95"):
            cs.sus_mod_spacetime_exceedance(
                {"rr": rr.drop(columns="rr_lower95")}, verbose=False
            )

    @pytest.mark.parametrize("ruim", [[0], [-1], [1, np.nan], []])
    def test_recusa_limiares_invalidos(self, ajuste, ruim):
        with pytest.raises(ValueError, match="thresholds"):
            cs.sus_mod_spacetime_exceedance(ajuste, thresholds=ruim,
                                            verbose=False)

    def test_recusa_agregacao_invalida(self, ajuste):
        with pytest.raises(ValueError, match="aggregate_time"):
            cs.sus_mod_spacetime_exceedance(ajuste, aggregate_time="semana",
                                            verbose=False)

    def test_nao_reproduz_o_call_do_r(self, ajuste):
        out = cs.sus_mod_spacetime_exceedance(ajuste, verbose=False)

        assert set(out) == {"exceedance", "thresholds", "n_exceed"}

    @pytest.mark.parametrize("lang", ["pt", "en", "es"])
    def test_as_tres_linguas(self, ajuste, lang, capsys):
        cs.sus_mod_spacetime_exceedance(ajuste, lang=lang, verbose=True)

        assert capsys.readouterr().err.strip()
