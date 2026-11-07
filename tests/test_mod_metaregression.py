"""Tests para sus_mod_metaregression -- covariaveis de cidade explicando heterogeneidade.

O ponto central destes testes e a ORDEM dos coeficientes. O mvmeta devolve
o vetor em outcome-major intercalado (y1.intercepto, y1.x, y2.intercepto,
y2.x, ...), e o R fatia como se cada moderador ocupasse um bloco contiguo
no inicio. Nao ocupa. Ver M67.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment._mvmeta import mvmeta_fit
from climasus4py.enrichment.mod_metaregression import (
    _heterogeneity,
    _wald_tests,
    sus_mod_metaregression,
)

pytest.importorskip("statsmodels")

LAG = 5
N_CIDADES = 10


@pytest.fixture(scope="module")
def cenario():
    """Dez cidades com a MESMA exposicao (base compartilhada) e efeito
    crescente com o indice de pobreza."""
    dias = pd.date_range("2018-01-01", "2021-12-31", freq="D")
    base = np.random.default_rng(1)
    temp = (24 + 6 * np.sin(2 * np.pi * (dias.dayofyear - 20) / 365)
            + base.normal(0, 2.0, len(dias)))

    def cidade(seed: int, beta: float):
        r = np.random.default_rng(seed)
        risco = 18 * (1 + beta * np.clip(temp - 26, 0, None)
                      + 0.02 * np.clip(18 - temp, 0, None))
        df = pd.DataFrame({"date": dias, "n_obitos": r.poisson(risco)})
        s = pd.Series(temp)
        for lag in range(LAG + 1):
            df[f"tair_dry_bulb_c_lag{lag}"] = s.shift(lag).values
        return cs.sus_mod_dlnm(df.dropna().reset_index(drop=True),
                               outcome_col="n_obitos", lag_max=LAG, verbose=False)

    rng = np.random.default_rng(9)
    pobreza = np.round(np.linspace(0.08, 0.62, N_CIDADES), 3)
    pib = np.round(rng.normal(20, 6, N_CIDADES), 1)
    fits = {f"c{i}": cidade(50 + i, 0.008 + 0.05 * p) for i, p in enumerate(pobreza)}
    cov = pd.DataFrame({"city": list(fits), "pobreza": pobreza, "pib": pib})
    return fits, cov


@pytest.fixture(scope="module")
def resultado(cenario):
    fits, cov = cenario
    return sus_mod_metaregression(fits, cov, city_col="city",
                                  method="fixed", verbose=False)


# ---------------------------------------------------------------------------
# A ordem dos coeficientes -- o coracao do M67
# ---------------------------------------------------------------------------

class TestOrdemDosCoeficientes:
    """O vetor do mvmeta e outcome-major INTERCALADO, nao blocos contiguos.

    Com p resultados e q moderadores, o coeficiente do moderador j para o
    resultado a esta na posicao ``a*q + j`` -- passo q, nao um bloco no
    inicio. O R (sus_mod_metaregression.R linha 134) pega
    ``coef[1:n_coef]`` como se fosse o bloco do intercepto, o que devolve
    interceptos e efeitos de covariavel alternados e cobre so os primeiros
    p/q resultados.
    """

    def test_block_recupera_intercepto_e_inclinacao_conhecidos(self):
        """Dado com verdade conhecida: o block() tem de separar os dois."""
        p, m = 5, 25
        r = np.random.default_rng(7)
        intercepto = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
        inclinacao = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        x = r.normal(0, 1, m)
        S = [np.eye(p) * 0.02] * m
        y = np.array([intercepto + inclinacao * x[i] + r.normal(0, 0.14, p)
                      for i in range(m)])

        f = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), x]), method="reml")

        assert f.n_moderators == 2
        assert np.allclose(f.block(0)[0], intercepto, atol=0.08)
        assert np.allclose(f.block(1)[0], inclinacao, atol=0.08)

    def test_a_fatia_do_r_nao_e_o_bloco_do_intercepto(self):
        """Contraexemplo que define o bug: a fatia ingenua pega outra coisa."""
        p, m = 5, 25
        r = np.random.default_rng(7)
        intercepto = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
        inclinacao = np.array([0.9, 0.8, 0.7, 0.6, 0.5])
        x = r.normal(0, 1, m)
        S = [np.eye(p) * 0.02] * m
        y = np.array([intercepto + inclinacao * x[i] + r.normal(0, 0.14, p)
                      for i in range(m)])
        f = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), x]), method="reml")

        fatia_do_r = f.coef[:p]
        assert not np.allclose(fatia_do_r, intercepto, atol=0.08)
        # E o motivo: a fatia alterna intercepto e inclinacao.
        assert fatia_do_r[1] == pytest.approx(f.block(1)[0][0])

    def test_vcov_do_bloco_e_simetrica_e_do_tamanho_certo(self):
        p, m = 4, 20
        r = np.random.default_rng(3)
        x = r.normal(0, 1, m)
        S = [np.eye(p) * 0.03] * m
        y = r.normal(0, 0.5, (m, p))
        f = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), x]), method="reml")

        for j in range(f.n_moderators):
            coef_j, vcov_j = f.block(j)
            assert coef_j.shape == (p,)
            assert vcov_j.shape == (p, p)
            assert np.allclose(vcov_j, vcov_j.T)

    def test_moderador_fora_do_desenho(self):
        f = mvmeta_fit(np.zeros((5, 2)), [np.eye(2)] * 5)
        with pytest.raises(IndexError, match="outside the design"):
            f.block(1)


# ---------------------------------------------------------------------------
# Desenho degenerado
# ---------------------------------------------------------------------------

class TestDesenhoDegenerado:
    """Falhar alto em vez de devolver zeros.

    Sem estas checagens, um desenho singular fazia o GLS voltar coeficientes
    zerados, e o que chegava ao usuario era RR = 1 com intervalo [1, 1] --
    um resultado fabricado com cara de plausivel.
    """

    def test_covariaveis_colineares_levantam_erro(self, cenario):
        fits, cov = cenario
        ruim = cov.assign(copia=34 - 40 * cov["pobreza"])

        with pytest.raises(ValueError, match="rank-deficient"):
            sus_mod_metaregression(fits, ruim[["city", "pobreza", "copia"]],
                                   city_col="city", verbose=False)

    def test_menos_cidades_que_termos_cai_na_checagem_de_posto(self):
        """Nao ha checagem separada de m < q, e nao precisa haver.

        O posto e limitado pelo numero de linhas, entao duas cidades com
        tres termos ja sao detectadas como desenho deficiente.
        """
        with pytest.raises(ValueError, match="rank-deficient"):
            mvmeta_fit(np.zeros((2, 3)), [np.eye(3)] * 2,
                       X=np.array([[1.0, 0.0, 1.0], [1.0, 1.0, 2.0]]))

    def test_covariavel_constante_e_descartada_com_aviso(self, cenario, capsys):
        """Constante e colinear com o intercepto: descartar, nao quebrar."""
        fits, cov = cenario
        out = sus_mod_metaregression(fits, cov.assign(constante=1.0),
                                     covariate_cols=["pobreza", "constante"],
                                     city_col="city", method="fixed", verbose=False)

        assert "desvio-padrao zero" in capsys.readouterr().err
        assert out["meta"]["covariate_cols"] == ["pobreza"]
        assert out["mvmeta_fit"].n_moderators == 2  # intercepto + pobreza

    def test_todas_as_covariaveis_constantes(self, cenario):
        fits, cov = cenario
        with pytest.raises(ValueError, match="numerica"):
            sus_mod_metaregression(fits, cov.assign(a=1.0, b=2.0),
                                   covariate_cols=["a", "b"],
                                   city_col="city", verbose=False)


# ---------------------------------------------------------------------------
# Alinhamento das covariaveis
# ---------------------------------------------------------------------------

class TestAlinhamento:
    def test_cidade_sem_covariavel_e_excluida_com_aviso(self, cenario, capsys):
        fits, cov = cenario
        out = sus_mod_metaregression(fits, cov.iloc[:-2], city_col="city",
                                     method="fixed", verbose=False)

        assert "ausente" in capsys.readouterr().err
        assert out["meta"]["n_cities"] == N_CIDADES - 2

    def test_nenhuma_cidade_casa(self, cenario):
        fits, cov = cenario
        outra = cov.assign(city=[f"z{i}" for i in range(len(cov))])
        with pytest.raises(ValueError, match="Nenhuma cidade"):
            sus_mod_metaregression(fits, outra, city_col="city", verbose=False)

    def test_city_col_inexistente(self, cenario):
        fits, cov = cenario
        with pytest.raises(ValueError, match="nao existe"):
            sus_mod_metaregression(fits, cov, city_col="municipio", verbose=False)

    def test_covariaveis_pelo_indice_quando_city_col_e_none(self, cenario):
        fits, cov = cenario
        pelo_indice = cov.set_index("city")
        out = sus_mod_metaregression(fits, pelo_indice, method="fixed", verbose=False)

        assert out["meta"]["n_cities"] == N_CIDADES

    def test_sem_coluna_numerica(self, cenario):
        fits, _ = cenario
        so_texto = pd.DataFrame({"city": list(fits), "regiao": ["sul"] * N_CIDADES})
        with pytest.raises(ValueError, match="numerica"):
            sus_mod_metaregression(fits, so_texto, city_col="city", verbose=False)

    def test_covariavel_segue_a_cidade_e_nao_a_posicao_na_tabela(self, cenario):
        """O alinhamento e o risco de verdade aqui.

        As linhas de ``coef_mat`` e as do desenho sao montadas em passagens
        separadas; se uma seguir a ordem do dict de ajustes e a outra a da
        tabela de covariaveis, cada cidade recebe a covariavel de outra e
        nada levanta erro. A ordem final acompanha ``fits`` -- igual ao
        ``intersect()`` do R -- independentemente de como a tabela veio.
        """
        fits, cov = cenario
        embaralhado = cov.iloc[::-1].reset_index(drop=True)

        direto = sus_mod_metaregression(fits, cov, city_col="city",
                                        method="fixed", verbose=False)
        trocado = sus_mod_metaregression(fits, embaralhado, city_col="city",
                                         method="fixed", verbose=False)

        assert trocado["meta"]["city_names"] == list(fits)
        assert list(trocado["city_table"]["city"]) == list(fits)
        # Reordenar as LINHAS da tabela nao pode mudar estimativa nenhuma.
        assert np.allclose(direto["mvmeta_fit"].coef, trocado["mvmeta_fit"].coef)
        assert np.allclose(direto["pooled_curve"]["rr"], trocado["pooled_curve"]["rr"])


# ---------------------------------------------------------------------------
# Padronizacao
# ---------------------------------------------------------------------------

class TestPadronizacao:
    def test_cov_scales_guarda_media_e_desvio_originais(self, resultado, cenario):
        _, cov = cenario
        escalas = resultado["cov_scales"].set_index("covariate")

        assert escalas.loc["pobreza", "mean"] == pytest.approx(cov["pobreza"].mean())
        assert escalas.loc["pobreza", "sd"] == pytest.approx(cov["pobreza"].std(ddof=1))

    def test_intercepto_e_a_curva_da_cidade_media(self, resultado):
        """Covariaveis centradas: o moderador 0 vale para covariaveis na media.

        E por isso que o bloco do intercepto -- e nao uma fatia qualquer --
        e o que alimenta o crosspred.
        """
        curva = resultado["pooled_curve"]
        cen = resultado["meta"]["ref_value"]
        idx = int(np.argmin(np.abs(resultado["meta"]["expo_grid"] - cen)))

        assert curva["rr"].iloc[idx] == pytest.approx(1.0, abs=0.02)
        assert (curva["rr"] > 0).all()


# ---------------------------------------------------------------------------
# Testes de Wald e heterogeneidade
# ---------------------------------------------------------------------------

class TestWald:
    def test_uma_linha_por_covariavel_com_df_igual_a_p(self, resultado):
        testes = resultado["covariate_tests"]

        assert list(testes["covariate"]) == ["pobreza", "pib"]
        assert (testes["n_df"] == resultado["mvmeta_fit"].n_outcomes).all()
        assert ((testes["p_value"] >= 0) & (testes["p_value"] <= 1)).all()

    def test_estatistica_e_nao_negativa(self, resultado):
        assert (resultado["covariate_tests"]["wald_stat"] >= 0).all()

    def test_covariavel_sem_efeito_nao_e_significativa(self):
        """Moderador puro ruido: o Wald nao deve acusar efeito."""
        p, m = 3, 40
        r = np.random.default_rng(21)
        ruido = r.normal(0, 1, m)
        S = [np.eye(p) * 0.05] * m
        y = np.array([r.multivariate_normal(np.array([0.2, -0.1, 0.05]), S[0])
                      for _ in range(m)])
        f = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), ruido]), method="reml")

        assert _wald_tests(f, ["ruido"])["p_value"].iloc[0] > 0.05

    def test_sem_covariavel_a_tabela_sai_vazia_mas_com_as_colunas(self):
        f = mvmeta_fit(np.zeros((5, 2)), [np.eye(2)] * 5)
        tabela = _wald_tests(f, [])

        assert len(tabela) == 0
        assert list(tabela.columns) == ["covariate", "n_df", "wald_stat", "p_value"]


class TestHeterogeneidade:
    def test_duas_linhas_nulo_e_completo(self, resultado):
        het = resultado["heterogeneity"]

        assert list(het["model"]) == ["null", "full"]
        assert list(het.columns) == ["model", "Q", "df_het", "p_het", "i2", "r2_het"]

    def test_o_modelo_completo_gasta_mais_graus_de_liberdade(self, resultado):
        het = resultado["heterogeneity"]
        assert het.loc[1, "df_het"] < het.loc[0, "df_het"]

    def test_r2_so_existe_na_linha_completa(self, resultado):
        """O R2 e a fracao do I2 do nulo que as covariaveis explicam.

        Quando o nulo ja nao tem heterogeneidade, nao ha fracao a reportar e
        o campo sai NaN -- por isso o teste aceita as duas situacoes na
        linha completa, mas exige NaN na linha nula sempre.
        """
        het = resultado["heterogeneity"]
        r2 = het.loc[1, "r2_het"]

        assert np.isnan(het.loc[0, "r2_het"])
        assert np.isnan(r2) or 0.0 <= r2 <= 1.0
        if np.isnan(r2):
            assert het.loc[0, "i2"] == 0.0

    def test_r2_e_nan_quando_o_nulo_nao_tem_heterogeneidade(self):
        """Sem heterogeneidade a explicar, nao ha proporcao a reportar."""
        p, m = 2, 12
        r = np.random.default_rng(5)
        S = [np.eye(p) * 0.5] * m
        y = np.array([[0.2, -0.1]] * m) + r.normal(0, 1e-4, (m, p))
        x = r.normal(0, 1, m)
        nulo = mvmeta_fit(y, S, method="fixed")
        cheio = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), x]), method="fixed")

        assert np.isnan(_heterogeneity(nulo, cheio).loc[1, "r2_het"])


# ---------------------------------------------------------------------------
# Saida
# ---------------------------------------------------------------------------

class TestSaida:
    def test_estrutura_espelha_o_climasus_metaregression_do_r(self, resultado):
        esperadas = {
            "mvmeta_fit", "null_fit", "pooled_pred", "pooled_curve",
            "exposure_response", "blup_preds", "city_table", "covariate_tests",
            "heterogeneity", "cov_scales", "meta",
        }
        assert esperadas <= set(resultado)

    def test_exposure_response_tem_rr_lo_e_rr_hi(self, resultado):
        """O R nomeia rr_lo/rr_hi aqui e lo/hi no sus_mod_pool; mantida a diferenca."""
        er = resultado["exposure_response"]

        assert list(er.columns) == ["pct", "exposure", "rr", "rr_lo", "rr_hi"]
        assert (er["rr_lo"] <= er["rr"]).all()
        assert (er["rr"] <= er["rr_hi"]).all()

    def test_city_table_uma_linha_por_cidade_com_blup(self, resultado):
        t = resultado["city_table"]

        assert len(t) == N_CIDADES
        assert t["raw_rr"].notna().all()
        assert t["blup_rr"].notna().all()

    def test_null_fit_nao_tem_moderador(self, resultado):
        assert resultado["null_fit"].n_moderators == 1
        assert resultado["mvmeta_fit"].n_moderators == 3  # intercepto + 2

    def test_meta_registra_as_covariaveis_usadas(self, resultado):
        assert resultado["meta"]["covariate_cols"] == ["pobreza", "pib"]
        assert resultado["meta"]["shared_basis"] is True
