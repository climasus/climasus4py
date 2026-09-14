"""Tests para sus_mod_pool -- agrupamento multi-cidade de DLNMs.

O estagio 2 (mvmeta) ja esta verificado numero a numero contra o R em
test_mvmeta.py. O que se testa aqui e a LIGACAO: extrair o bloco certo de
coeficientes de cada cidade, montar a grade, indexar os quantis e
alimentar o crosspred. E as duas armadilhas que o R nao sinaliza -- bases
com nos diferentes, e Psi superparametrizado.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment.mod_pool import _same_basis, sus_mod_pool

pytest.importorskip("statsmodels")

LAG = 6


def _cidade(seed: int, *, temp: np.ndarray | None = None, media: float = 24.0):
    """Uma cidade sintetica pronta para sus_mod_dlnm.

    Passando ``temp`` a serie de exposicao e reaproveitada, e como os nos
    saem dos quantis dela as cidades ficam com bases IDENTICAS -- que e a
    unica situacao em que agrupar coeficientes faz sentido.
    """
    dias = pd.date_range("2018-01-01", "2021-12-31", freq="D")
    rng = np.random.default_rng(seed)
    if temp is None:
        temp = (media + 6.0 * np.sin(2 * np.pi * (dias.dayofyear - 20) / 365)
                + rng.normal(0, 2.0, len(dias)))
    risco = 18 * (1 + 0.03 * np.clip(temp - 26, 0, None)
                  + 0.02 * np.clip(18 - temp, 0, None))
    df = pd.DataFrame({"date": dias, "n_obitos": rng.poisson(risco)})
    s = pd.Series(temp)
    for lag in range(LAG + 1):
        df[f"tair_dry_bulb_c_lag{lag}"] = s.shift(lag).values
    return df.dropna().reset_index(drop=True), temp


def _ajusta(df):
    return cs.sus_mod_dlnm(df, outcome_col="n_obitos", lag_max=LAG,
                           verbose=False, lang="pt")


@pytest.fixture(scope="module")
def fits_base_comum():
    """Tres cidades com a MESMA serie de exposicao: bases identicas."""
    df0, temp = _cidade(1)
    return {
        "a": _ajusta(df0),
        "b": _ajusta(_cidade(2, temp=temp)[0]),
        "c": _ajusta(_cidade(3, temp=temp)[0]),
    }


@pytest.fixture(scope="module")
def fits_base_divergente():
    """Cidades com climas distintos: nos -- e portanto bases -- diferentes."""
    return {
        "fria": _ajusta(_cidade(11, media=19.0)[0]),
        "quente": _ajusta(_cidade(12, media=28.0)[0]),
    }


# ---------------------------------------------------------------------------
# Validacao
# ---------------------------------------------------------------------------

class TestValidacao:
    def test_fits_precisa_ser_dict(self):
        with pytest.raises(ValueError, match="dict"):
            sus_mod_pool([], verbose=False)

    def test_fits_vazio(self):
        with pytest.raises(ValueError, match="vazio"):
            sus_mod_pool({}, verbose=False)

    def test_entrada_que_nao_e_saida_de_dlnm(self, fits_base_comum):
        fits = {**fits_base_comum, "ruim": {"model": None}}
        with pytest.raises(ValueError, match="sus_mod_dlnm"):
            sus_mod_pool(fits, verbose=False)

    def test_lag_max_divergente(self, fits_base_comum):
        """Agrupar cidades com lags diferentes nao faz sentido."""
        outro = dict(fits_base_comum["a"])
        outro["meta"] = {**outro["meta"], "lag_max": LAG + 3}
        with pytest.raises(ValueError, match="lag_max"):
            sus_mod_pool({"a": fits_base_comum["a"], "x": outro}, verbose=False)

    def test_climate_col_divergente(self, fits_base_comum):
        outro = dict(fits_base_comum["a"])
        outro["meta"] = {**outro["meta"], "climate_col": "outra_variavel"}
        with pytest.raises(ValueError, match="climate_col"):
            sus_mod_pool({"a": fits_base_comum["a"], "x": outro}, verbose=False)


# ---------------------------------------------------------------------------
# As duas armadilhas
# ---------------------------------------------------------------------------

class TestBaseCompartilhada:
    """Coeficientes de crossbasis so sao comparaveis entre bases identicas.

    Os nos saem dos quantis da exposicao de CADA cidade, entao duas cidades
    de climas diferentes recebem bases diferentes por padrao -- e ai o
    mesmo coeficiente significa coisas diferentes em cada uma. O R nao
    verifica isso; aqui o resultado ainda sai (paridade), mas avisado.
    """

    def test_bases_iguais_nao_avisam(self, fits_base_comum, capsys):
        sus_mod_pool(fits_base_comum, n_grid=30, blup=False, verbose=False)
        assert "nos diferentes" not in capsys.readouterr().err

    def test_bases_diferentes_avisam(self, fits_base_divergente, capsys):
        sus_mod_pool(fits_base_divergente, n_grid=30, blup=False, verbose=False)
        assert "nos diferentes" in capsys.readouterr().err

    def test_meta_registra_se_a_base_era_compartilhada(
        self, fits_base_comum, fits_base_divergente
    ):
        igual = sus_mod_pool(fits_base_comum, n_grid=30, blup=False, verbose=False)
        difer = sus_mod_pool(fits_base_divergente, n_grid=30, blup=False, verbose=False)

        assert igual["meta"]["shared_basis"] is True
        assert difer["meta"]["shared_basis"] is False

    def test_same_basis_compara_os_nos_e_nao_so_o_df(self):
        a = {"fun": "ns", "df": 4, "knots": np.array([1.0, 2.0]),
             "boundary_knots": (0.0, 3.0)}
        assert _same_basis(a, dict(a))
        assert not _same_basis(a, {**a, "knots": np.array([1.0, 2.5])})
        assert not _same_basis(a, {**a, "boundary_knots": (0.0, 4.0)})
        assert not _same_basis(a, {**a, "df": 5})


class TestSuperparametrizacao:
    """Psi custa p(p+1)/2 e as cidades fornecem m*p numeros.

    Com poucas cidades e uma base cheia, Psi simplesmente nao e estimavel:
    a verossimilhanca achata, nenhum otimizador converge e onde cada um
    para e arbitrario. Medido nos mesmos dados, o mvmeta do R tambem nao
    converge. O aviso e o que separa 'resultado ruim' de 'resultado ruim e
    silencioso'.
    """

    def test_avisa_com_numeros_concretos(self, fits_base_comum, capsys):
        sus_mod_pool(fits_base_comum, n_grid=30, blup=False, verbose=False)
        err = capsys.readouterr().err
        p = fits_base_comum["a"]["crossbasis"].shape[1]

        assert "parametros livres" in err
        assert str(p * (p + 1) // 2) in err.replace("\n", "")

    def test_fixed_nao_estima_psi_e_nao_avisa(self, fits_base_comum, capsys):
        out = sus_mod_pool(fits_base_comum, n_grid=30, blup=False,
                           method="fixed", verbose=False)
        err = capsys.readouterr().err

        assert np.allclose(out["mvmeta_fit"].psi, 0.0)
        assert out["mvmeta_fit"].n_par_psi == 0
        assert not out["mvmeta_fit"].overparametrised
        assert "parametros livres" not in err


# ---------------------------------------------------------------------------
# Ligacao com o estagio 1
# ---------------------------------------------------------------------------

class TestExtracao:
    def test_pega_o_bloco_do_crossbasis_e_nao_o_intercepto(self, fits_base_comum):
        """O desenho e [intercepto | crossbasis | sazonalidade | covariaveis].

        Pegar a fatia errada nao levanta erro nenhum -- so produz um RR
        errado. Dai conferir contra o que o proprio ajuste da cidade usou.
        """
        from climasus4py.enrichment.mod_pool import _city_coefficients

        fit = fits_base_comum["a"]
        n_cb = fit["crossbasis"].shape[1]
        coef, vcov = _city_coefficients(fit, "a", {"err_no_coef": "x"})

        assert coef.shape == (n_cb,)
        assert vcov.shape == (n_cb, n_cb)
        esperado = np.asarray(fit["model"].params)[1:1 + n_cb]
        assert np.allclose(coef, esperado)
        # O intercepto NAO pode estar no bloco.
        assert not np.isclose(coef[0], np.asarray(fit["model"].params)[0])

    def test_uma_cidade_avisa_e_repete_o_ajuste_dela(self, fits_base_comum, capsys):
        out = sus_mod_pool({"a": fits_base_comum["a"]}, n_grid=30,
                           blup=False, verbose=False)
        assert "uma cidade" in capsys.readouterr().err
        assert out["meta"]["n_cities"] == 1
        assert out["blup_preds"] is None


# ---------------------------------------------------------------------------
# Saida
# ---------------------------------------------------------------------------

class TestSaida:
    def test_estrutura_espelha_o_climasus_pool_do_r(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=40, verbose=False)
        esperadas = {
            "mvmeta_fit", "pooled_pred", "exposure_response", "exposure_curve",
            "lag_response", "blup_preds", "city_table", "heterogeneity", "meta",
        }
        assert esperadas <= set(out)

    def test_curva_tem_n_grid_pontos_e_rr_positivo(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=40, blup=False, verbose=False)
        curva = out["exposure_curve"]

        assert len(curva) == 40
        assert (curva["rr"] > 0).all()
        assert (curva["lo"] <= curva["rr"]).all()
        assert (curva["rr"] <= curva["hi"]).all()

    def test_exposure_response_cobre_os_quantis_pedidos(self, fits_base_comum):
        pedidos = (0.50, 0.90, 0.99)
        out = sus_mod_pool(fits_base_comum, n_grid=40, pred_at=pedidos,
                           blup=False, verbose=False)

        assert list(out["exposure_response"]["pct"]) == list(pedidos)
        # Exposicao crescente com o quantil.
        assert out["exposure_response"]["exposure"].is_monotonic_increasing

    def test_lag_response_vai_de_0_a_lag_max(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=30, blup=False, verbose=False)
        assert list(out["lag_response"]["lag"]) == list(range(LAG + 1))

    def test_rr_no_valor_de_referencia_e_um(self, fits_base_comum):
        """A curva e centrada: no ponto de referencia o RR vale 1 por construcao."""
        out = sus_mod_pool(fits_base_comum, n_grid=101, blup=False, verbose=False)
        cen = out["meta"]["ref_value"]
        grade = out["meta"]["expo_grid"]
        idx = int(np.argmin(np.abs(grade - cen)))

        assert out["exposure_curve"]["rr"].iloc[idx] == pytest.approx(1.0, abs=0.02)

    def test_heterogeneidade_tem_os_quatro_campos(self, fits_base_comum):
        het = sus_mod_pool(fits_base_comum, n_grid=30, blup=False,
                           verbose=False)["heterogeneity"]

        assert list(het.columns) == ["Q", "df", "p_value", "i2"]
        assert het["Q"].iloc[0] >= 0
        assert 0 <= het["i2"].iloc[0] <= 100

    def test_city_table_uma_linha_por_cidade(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=30, verbose=False)
        tabela = out["city_table"]

        assert list(tabela["city"]) == list(fits_base_comum)
        assert tabela["rr"].notna().all()
        # Com blup=True as tres colunas de BLUP saem preenchidas.
        assert tabela["blup_rr"].notna().all()

    def test_blup_desligado_deixa_as_colunas_vazias(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=30, blup=False, verbose=False)

        assert out["blup_preds"] is None
        assert out["city_table"]["blup_rr"].isna().all()

    def test_blup_encolhe_a_cidade_na_direcao_do_agrupado(self, fits_base_comum):
        """O BLUP fica mais perto da media agrupada do que o ajuste proprio."""
        out = sus_mod_pool(fits_base_comum, n_grid=30, verbose=False)
        t = out["city_table"]
        agrupado = out["exposure_response"]
        rr_pool = float(agrupado.loc[agrupado["pct"] == 0.75, "rr"].iloc[0])

        dist_blup = np.abs(t["blup_rr"] - rr_pool).sum()
        dist_propria = np.abs(t["rr"] - rr_pool).sum()
        assert dist_blup <= dist_propria

    def test_exposure_range_manda_na_grade(self, fits_base_comum):
        out = sus_mod_pool(fits_base_comum, n_grid=25, exposure_range=(15.0, 30.0),
                           blup=False, verbose=False)
        grade = out["meta"]["expo_grid"]

        assert grade[0] == pytest.approx(15.0)
        assert grade[-1] == pytest.approx(30.0)
        assert len(grade) == 25
