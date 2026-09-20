"""sus_mod_plot_spatial_bayes: mapas e coeficientes do ajuste CAR/BYM.

As fixtures sao um ajuste REAL: CARBayes 6.1.1 rodando o
sus_mod_spatial_bayes do climasus4r sobre os 42 municipios sinteticos,
modelo BYM, Poisson, offset de esperados, uma covariavel climatica,
6000 iteracoes com 1000 de burnin e thin 5.

A funcao nao depende do CARBayes -- ela consome o objeto ja ajustado --
entao e portavel hoje, mesmo com o sus_mod_spatial_bayes ainda stub.
Estes testes tambem fixam a FORMA desse objeto, que e o contrato que um
eventual ajustador em Python vai ter de cumprir.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.viz.mod_plot_spatial_bayes import (
    _REQUIRED_FIXED,
    _REQUIRED_RR,
    VALID_TYPES,
)

FIX_BAYES = Path(__file__).parent / "fixtures" / "spatial_bayes"
FIX_SCAN = Path(__file__).parent / "fixtures" / "spatial_scan"


@pytest.fixture(scope="module")
def municipios() -> gpd.GeoDataFrame:
    return gpd.read_file(FIX_SCAN / "scan_muni.gpkg", layer="muni")


@pytest.fixture(scope="module")
def ajuste() -> dict:
    """O objeto climasus_spatial_bayes, como o R o devolve."""
    meta = pd.read_parquet(FIX_BAYES / "bayes_meta.parquet").iloc[0]
    return {
        "fixed": pd.read_parquet(FIX_BAYES / "bayes_fixed.parquet"),
        "random": pd.read_parquet(FIX_BAYES / "bayes_random.parquet"),
        "rr": pd.read_parquet(FIX_BAYES / "bayes_rr.parquet"),
        "fitted": pd.read_parquet(FIX_BAYES / "bayes_fitted.parquet")["fitted"].to_numpy(),
        "dic": float(meta["dic"]),
        "model": str(meta["model"]),
        "family": str(meta["family"]),
        "n_iter_effective": int(meta["n_iter_effective"]),
    }


class TestFormaDoObjeto:
    """O contrato que um ajustador em Python tera de cumprir."""

    def test_os_slots_do_r(self, ajuste):
        assert set(ajuste) == {
            "fixed", "random", "rr", "fitted", "dic", "model", "family",
            "n_iter_effective",
        }

    def test_as_colunas_de_rr(self, ajuste):
        assert list(ajuste["rr"].columns) == list(_REQUIRED_RR)
        assert len(ajuste["rr"]) == 42

    def test_as_colunas_de_fixed(self, ajuste):
        assert set(_REQUIRED_FIXED).issubset(ajuste["fixed"].columns)

    def test_o_intervalo_contem_a_media(self, ajuste):
        rr = ajuste["rr"]
        assert (rr["rr_lower95"] <= rr["rr_mean"]).all()
        assert (rr["rr_mean"] <= rr["rr_upper95"]).all()

    def test_o_ajuste_achou_o_excesso_plantado(self, ajuste):
        """Os 4 municipios com excesso sao os 4 com IC95 inferior > 1."""
        elevados = set(ajuste["rr"].loc[ajuste["rr"]["rr_lower95"] > 1, "code_muni"])

        assert elevados == {"3505004", "3505005", "3506004", "3506005"}


class TestTabelaDeEfeitosFixos:
    """A tabela `fixed` do R sai errada nas duas pontas (M88).

    O climasus4r filtra os rownames do summary.results do CARBayes com
    coef_pattern = "^(Intercept|[A-Za-z_][A-Za-z0-9_\\.]*)" e
    excl_pattern = "(tau2|nu2|rho|Sigma|phi|psi|delta|gamma)".

    O CARBayes devolve "(Intercept)", "temp_media", "tau2", "sigma2":

    - "(Intercept)" e DESCARTADO, porque os parenteses do padrao sao um
      grupo e nao literais, entao o padrao exige comecar com letra e a
      string real comeca com "(".
    - "sigma2" e MANTIDO, porque a exclusao lista "Sigma" maiusculo e o
      grepl e sensivel a caso.

    O autor queria o oposto nos dois casos -- o padrao comeca justamente
    com "Intercept|" e a exclusao existe justamente para tirar variancia.
    Efeito no grafico: type="coef" mostra uma variancia (sempre positiva,
    nunca cruza a linha de referencia no zero) ao lado de uma razao de
    taxas em log, e sem o intercepto.
    """

    def test_o_intercepto_sumiu(self, ajuste):
        termos = set(ajuste["fixed"]["term"])

        assert "(Intercept)" not in termos
        assert "Intercept" not in termos

    def test_a_variancia_entrou_como_efeito_fixo(self, ajuste):
        assert "sigma2" in set(ajuste["fixed"]["term"])

    def test_a_variancia_e_sempre_positiva(self, ajuste):
        """Por isso ela nunca cruza a referencia no zero do grafico."""
        s = ajuste["fixed"].set_index("term").loc["sigma2"]

        assert s["lower95"] > 0
        assert s["mean"] > 0

    def test_tau2_foi_corretamente_excluido(self, ajuste):
        """O filtro acerta neste -- e a prova de que a intencao era essa."""
        assert "tau2" not in set(ajuste["fixed"]["term"])

    def test_o_desvio_padrao_fica_sempre_nulo(self, ajuste):
        """O summary.results do CARBayes nao tem coluna SD, entao o R
        preenche com NA -- a coluna existe e nunca serve."""
        assert ajuste["fixed"]["sd"].isna().all()


class TestMapaDeRisco:
    def test_junta_todos_os_municipios(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="rr")

        assert len(p.data) == len(municipios)
        assert "rr_mean" in p.data.columns

    def test_contorna_os_quatro_elevados(self, ajuste, municipios):
        """O R contorna 4; a marca e IC95 inferior > 1."""
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="rr")

        assert int(p.data["sig_elevated"].sum()) == 4

    def test_a_marca_e_unilateral(self, ajuste, municipios):
        """Risco significativamente REDUZIDO nao recebe contorno."""
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="rr")
        reduzidos = p.data["rr_upper95"] < 1

        assert reduzidos.any()                              # existem
        assert not p.data.loc[reduzidos, "sig_elevated"].any()

    def test_a_escala_nao_e_degenerada_aqui(self, ajuste):
        """Ao contrario do plot_spatial_scan (M87), aqui todo municipio
        tem RR proprio, entao a rampa de cor existe."""
        rr = ajuste["rr"]["rr_mean"]

        assert rr.nunique() == len(rr)
        assert rr.max() - rr.min() > 0.5

    def test_titulo_e_legenda(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="rr")

        assert p.labels.title == "Risco Relativo Suavizado (BYM/CAR)"
        assert "IC95% inferior > 1" in p.labels.caption


class TestMapaDeIncerteza:
    def test_calcula_a_largura_do_intervalo(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="uncertainty")
        esperado = ajuste["rr"]["rr_upper95"] - ajuste["rr"]["rr_lower95"]

        assert np.allclose(sorted(p.data["ci_width"]), sorted(esperado))

    def test_a_largura_bate_com_o_r(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="uncertainty")

        assert p.data["ci_width"].min() == pytest.approx(0.22525, abs=1e-4)
        assert p.data["ci_width"].max() == pytest.approx(0.7483376, abs=1e-4)

    def test_titulo(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="uncertainty")

        assert "Incerteza" in p.labels.title


class TestCoeficientes:
    def test_ordena_por_media(self, ajuste):
        """R: reorder(term, mean) -- crescente."""
        p = cs.sus_mod_plot_spatial_bayes(ajuste, type="coef")
        ordem = list(p.data["term"].cat.categories)

        assert ordem == ["sigma2", "temp_media"]

    def test_os_valores_batem_com_o_r(self, ajuste):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, type="coef")
        d = p.data.set_index("term")

        assert d.loc["temp_media", "mean"] == pytest.approx(0.1397)
        assert d.loc["temp_media", "lower95"] == pytest.approx(0.0765)
        assert d.loc["temp_media", "upper95"] == pytest.approx(0.2063)

    def test_nao_precisa_de_geometria(self, ajuste):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipalities=None, type="coef")

        assert p is not None

    def test_recusa_ajuste_sem_efeitos_fixos(self, ajuste):
        vazio = dict(ajuste, fixed=pd.DataFrame(columns=list(_REQUIRED_FIXED)))
        with pytest.raises(ValueError, match="[Nn]enhum efeito fixo"):
            cs.sus_mod_plot_spatial_bayes(vazio, type="coef")


class TestAmbos:
    def test_devolve_dict_com_os_dois_mapas(self, ajuste, municipios):
        from plotnine import ggplot

        with pytest.warns(UserWarning, match="patchwork"):
            r = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="both")

        assert set(r) == {"rr", "uncertainty"}
        assert all(isinstance(v, ggplot) for v in r.values())

    def test_mesma_convencao_do_plot_spatial_moran(self, ajuste, municipios):
        """plotnine nao tem equivalente ao patchwork; o R cai nessa mesma
        lista nomeada quando o patchwork nao esta instalado."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="both")

        assert isinstance(r, dict)


class TestEntradas:
    def test_recusa_x_que_nao_e_ajuste(self, municipios):
        with pytest.raises(TypeError, match="sus_mod_spatial_bayes"):
            cs.sus_mod_plot_spatial_bayes({"nada": 1}, municipios)

    @pytest.mark.parametrize("type_", ["rr", "uncertainty", "both"])
    def test_mapa_exige_municipios(self, ajuste, type_):
        with pytest.raises(ValueError, match="municipalities"):
            cs.sus_mod_plot_spatial_bayes(ajuste, None, type=type_)

    def test_recusa_municipios_sem_geometria(self, ajuste, municipios):
        plano = pd.DataFrame({"code_muni": municipios["code_muni"]})
        with pytest.raises(TypeError, match="GeoDataFrame"):
            cs.sus_mod_plot_spatial_bayes(ajuste, plano, type="rr")

    def test_recusa_type_invalido(self, ajuste, municipios):
        with pytest.raises(ValueError, match="invalido|invalid|valido"):
            cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type="mapa")

    def test_recusa_rr_sem_as_colunas(self, ajuste, municipios):
        capenga = dict(ajuste, rr=ajuste["rr"].drop(columns="rr_lower95"))
        with pytest.raises(ValueError, match="rr_lower95"):
            cs.sus_mod_plot_spatial_bayes(capenga, municipios, type="rr")

    def test_lingua_desconhecida_avisa_e_cai_no_pt(self, ajuste, municipios):
        with pytest.warns(UserWarning, match="nao suportado"):
            p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, lang="tupi")

        assert p.labels.title == "Risco Relativo Suavizado (BYM/CAR)"

    @pytest.mark.parametrize(("lang", "esperado"), [
        ("pt", "Risco Relativo Suavizado (BYM/CAR)"),
        ("en", "Smoothed Relative Risk (BYM/CAR)"),
        ("es", "Riesgo Relativo Suavizado (BYM/CAR)"),
    ])
    def test_as_tres_linguas(self, ajuste, municipios, lang, esperado):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, lang=lang)

        assert p.labels.title == esperado

    def test_titulo_customizado(self, ajuste, municipios):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, title="Meu mapa")

        assert p.labels.title == "Meu mapa"

    def test_aceita_e_ignora_kwargs(self, ajuste, municipios):
        assert cs.sus_mod_plot_spatial_bayes(ajuste, municipios, algo=1) is not None

    def test_os_tipos_validos(self):
        assert VALID_TYPES == ("rr", "uncertainty", "coef", "both")


class TestRenderizacao:
    @pytest.mark.parametrize("type_", ["rr", "uncertainty", "coef"])
    def test_desenha_sem_erro(self, ajuste, municipios, type_, tmp_path):
        p = cs.sus_mod_plot_spatial_bayes(ajuste, municipios, type=type_)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p.save(tmp_path / f"b_{type_}.png", width=5, height=4, dpi=60,
                   verbose=False)

        assert (tmp_path / f"b_{type_}.png").exists()
