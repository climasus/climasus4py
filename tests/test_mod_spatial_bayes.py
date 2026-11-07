"""sus_mod_spatial_bayes em PyMC: forma, invariantes e paridade estatistica.

O R ajusta com os amostradores Gibbs/Metropolis do CARBayes; este porte
usa PyMC com NUTS, decidido em 17/09/2026. Paridade numerica exata NAO
existe e nunca vai existir -- amostrador diferente no mesmo posterior da
amostras diferentes. O criterio e concordancia estatistica dentro do erro
de Monte Carlo, contra a saida do R congelada em
tests/fixtures/spatial_bayes/ref_*.

A referencia foi gerada com W explicito (grade 6x7, adjacencia rook, 71
arestas) porque a fixture antiga nao guardou o W -- e sem ele a comparacao
nao e maca com maca.

Custo dos testes de paridade: com nutpie instalado (que o
climasus4py[bayes] traz, e que o PyMC escolhe sozinho) um ajuste de 42
areas leva ~28 s e os quatro testes rodam em ~2 min, entao rodam por
default. Sem nutpie, o NUTS do pytensor numa maquina sem compilador C
leva ~150 s por ajuste e eles sao pulados. Os testes de forma e invariante
rodam sempre, com poucas amostras.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pm = pytest.importorskip("pymc", reason="sus_mod_spatial_bayes precisa de PyMC")

from climasus4py.enrichment.mod_spatial_bayes import (  # noqa: E402
    sus_mod_spatial_bayes,
)

FIX = Path(__file__).parent / "fixtures" / "spatial_bayes"
def _tem_nutpie() -> bool:
    try:
        import nutpie  # noqa: F401
    except ImportError:
        return False
    return True


# O portao e o nutpie, nao uma variavel de ambiente. Com ele os quatro
# testes de paridade custam ~2 min (4 ajustes de 42 areas); sem ele, o
# NUTS do pytensor sem compilador C leva ~10 min, e o que era o teste mais
# valioso passa a atrasar a suite. CLIMASUS_SLOW_TESTS=1 forca mesmo assim.
lento = pytest.mark.skipif(
    not _tem_nutpie() and os.environ.get("CLIMASUS_SLOW_TESTS") != "1",
    reason=("paridade MCMC sem nutpie leva ~10 min; instale "
            "climasus4py[bayes] ou force com CLIMASUS_SLOW_TESTS=1"),
)


metadado_bym2 = pytest.mark.skipif(
    not (FIX / "ref_bym2_rr.parquet").is_file(),
    reason="referencia do bym2 ausente (gerada com INLA)",
)


@pytest.fixture(scope="module")
def dados():
    return pd.read_parquet(FIX / "bayes_df.parquet")


@pytest.fixture(scope="module")
def W(dados):
    """A mesma vizinhanca com que a referencia do R foi gerada."""
    n = len(dados)
    ar = pd.read_parquet(FIX / "ref_W_edges.parquet")
    m = np.zeros((n, n))
    m[ar["i"].to_numpy() - 1, ar["j"].to_numpy() - 1] = 1.0
    assert np.allclose(m, m.T)
    return m


def _ajusta(dados, W, **kw):
    """Ajuste curto, para checar forma e invariante sem gastar minutos."""
    base = dict(df=dados, outcome="obitos", W=W, covariates=["temp_media"],
                offset="esperado", n_iter=1200, burnin=200, thin=10,
                seed=42, verbose=False, chains=1)
    base.update(kw)
    return sus_mod_spatial_bayes(**base)


class TestForma:
    """O dict devolvido espelha o objeto climasus_spatial_bayes do R."""

    def test_chaves(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        for k in ("fixed", "random", "rr", "fitted", "dic", "model",
                  "family", "n_iter_effective"):
            assert k in fit, k

    def test_tabelas_na_ordem_de_code_muni(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        esperado = sorted(dados["code_muni"].tolist())
        assert fit["rr"]["code_muni"].tolist() == esperado
        assert fit["random"]["code_muni"].tolist() == esperado

    def test_colunas_das_tabelas(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        assert list(fit["rr"].columns) == [
            "code_muni", "rr_mean", "rr_lower95", "rr_upper95"]
        assert list(fit["random"].columns) == [
            "code_muni", "phi_mean", "phi_sd"]
        assert list(fit["fixed"].columns) == [
            "term", "mean", "sd", "lower95", "upper95"]

    def test_intervalo_contem_a_media(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        rr = fit["rr"]
        assert (rr["rr_lower95"] <= rr["rr_mean"]).all()
        assert (rr["rr_mean"] <= rr["rr_upper95"]).all()

    def test_n_iter_effective_conta_as_amostras_guardadas(self, dados, W):
        fit = _ajusta(dados, W, model="independent", n_iter=2200,
                      burnin=200, thin=10, chains=1)
        assert fit["n_iter_effective"] == (2200 - 200) // 10


class TestTabelaDeEfeitosFixos:
    """M88: a tabela do R perde o intercepto e ganha uma variancia.

    O regex `^(Intercept|...)` trata os parenteses como grupo, entao a
    linha chamada `(Intercept)` nunca casa; e a exclusao escreve `Sigma`
    com S maiusculo, entao `sigma2` sobrevive. Os dois erros se compensam
    na CONTAGEM de linhas, que e por isso que passou despercebido.

    Aqui a tabela e montada por este porte, entao por default ela sai
    correta, e a forma do R fica atras de uma chave de modulo -- o mesmo
    padrao das outras divergencias deliberadas.
    """

    def test_default_traz_o_intercepto(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        assert fit["fixed"]["term"].tolist() == ["(Intercept)", "temp_media"]

    def test_default_traz_sd_de_verdade(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        assert fit["fixed"]["sd"].notna().all()
        assert (fit["fixed"]["sd"] > 0).all()

    def test_default_nao_mistura_variancia(self, dados, W):
        fit = _ajusta(dados, W, model="bym")
        assert "sigma2" not in fit["fixed"]["term"].tolist(), (
            "sigma2 e uma variancia, nao um efeito fixo"
        )

    def test_a_chave_reproduz_a_forma_do_r(self, dados, W):
        from climasus4py.enrichment import mod_spatial_bayes as mod

        antes = mod.R_FIXED_TABLE_DROPS_INTERCEPT
        mod.R_FIXED_TABLE_DROPS_INTERCEPT = True
        try:
            fit = _ajusta(dados, W, model="bym")
        finally:
            mod.R_FIXED_TABLE_DROPS_INTERCEPT = antes
        termos = fit["fixed"]["term"].tolist()
        assert termos == ["temp_media", "sigma2"], termos
        assert fit["fixed"]["sd"].isna().all(), (
            "o summary.results do CARBayes nao tem coluna SD, entao o sd do "
            "R e sempre vazio"
        )


class TestModeloIndependent:
    """M109: no R este modelo SEMPRE aborta; aqui ele ajusta.

    O R faz re_slot <- switch(model, ..., independent = 'theta') e depois
    colMeans(fit$samples[['theta']]). Mas 'independent' ajusta via
    CARBayes::S.glm, que e um GLM sem efeito aleatorio -- conferido
    chamando o S.glm direto, fit$samples tem so beta, fitted e Y. Entao e
    colMeans(NULL).

    Replicar daria uma excecao, que nao carrega informacao nenhuma. Um GLM
    sem termo espacial tem efeito aleatorio identicamente zero, e e isso
    que a tabela diz.
    """

    def test_ajusta(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        assert len(fit["rr"]) == len(dados)

    def test_efeito_aleatorio_e_zero(self, dados, W):
        fit = _ajusta(dados, W, model="independent")
        assert (fit["random"]["phi_mean"] == 0).all()
        assert (fit["random"]["phi_sd"] == 0).all()


class TestGuardas:
    def test_modelo_desconhecido(self, dados, W):
        with pytest.raises(ValueError, match="model must be"):
            _ajusta(dados, W, model="inexistente")

    def test_familia_desconhecida(self, dados, W):
        with pytest.raises(ValueError, match="family must be"):
            _ajusta(dados, W, family="inexistente")

    def test_sem_code_muni(self, dados, W):
        with pytest.raises(ValueError, match="code_muni"):
            _ajusta(dados.drop(columns=["code_muni"]), W)

    def test_coluna_ausente(self, dados, W):
        with pytest.raises(ValueError, match="missing column"):
            _ajusta(dados, W, covariates=["nao_existe"])

    def test_W_de_tamanho_errado(self, dados, W):
        with pytest.raises(ValueError, match="to match df"):
            _ajusta(dados, W[:10, :10])

    def test_W_sem_arestas(self, dados, W):
        with pytest.raises(ValueError, match="no edges"):
            _ajusta(dados, np.zeros_like(W))

    def test_W_assimetrico(self, dados, W):
        ruim = W.copy()
        ruim[0, 1] = 1.0
        ruim[1, 0] = 0.0
        with pytest.raises(ValueError, match="symmetric"):
            _ajusta(dados, ruim)

    def test_iteracoes_sem_amostra(self, dados, W):
        with pytest.raises(ValueError, match="no draws"):
            _ajusta(dados, W, n_iter=300, burnin=200, thin=1000)

    def test_aceita_W_como_dict(self, dados, W):
        fit = _ajusta(dados, {"W": W}, model="independent")
        assert len(fit["rr"]) == len(dados)


class TestSemOffset:
    """Sem offset o RR nao e relativo a 1, e isso e do R.

    O R cai em expected <- rep(mean(fitted), n), entao a razao e contra a
    media dos ajustados. Replicado.
    """

    def test_rr_gira_em_torno_de_um(self, dados, W):
        fit = _ajusta(dados, W, model="independent", offset=None)
        assert fit["rr"]["rr_mean"].mean() == pytest.approx(1.0, abs=0.15)


@lento
class TestParidadeEstatisticaComR:
    """Concordancia com o R dentro do erro de Monte Carlo.

    Medido com 1200 amostras por cadeia, 2 cadeias:

      modelo    rr_mean corr   phi_mean corr   DIC py / R
      bym       1.0000         0.9998          339.08 / 340.68
      leroux    1.0000         0.9998          339.54 / 341.27

    e o coeficiente de temp_media em 0.1322 contra 0.1341 no bym, 0.1245
    contra 0.1227 no leroux.

    Estes limites sao frouxos de proposito: aperta-los transformaria
    variacao de Monte Carlo em falha intermitente.
    """

    @pytest.mark.parametrize("modelo", ["bym", "leroux", "bym2"])
    def test_rr_e_efeito_aleatorio(self, dados, W, modelo):
        fit = sus_mod_spatial_bayes(
            df=dados, outcome="obitos", W=W, covariates=["temp_media"],
            offset="esperado", model=modelo, n_iter=14000, burnin=2000,
            thin=10, seed=42, verbose=False)

        rr_r = pd.read_parquet(FIX / f"ref_{modelo}_rr.parquet")
        a = fit["rr"]["rr_mean"].to_numpy()
        b = rr_r["rr_mean"].to_numpy()
        assert np.corrcoef(a, b)[0, 1] > 0.99
        assert np.abs(a - b).mean() < 0.02

        rd_r = pd.read_parquet(FIX / f"ref_{modelo}_random.parquet")
        assert np.corrcoef(fit["random"]["phi_mean"].to_numpy(),
                           rd_r["phi_mean"].to_numpy())[0, 1] > 0.99

    @pytest.mark.parametrize("modelo", ["bym", "leroux", "bym2"])
    def test_coeficiente_e_dic(self, dados, W, modelo):
        fit = sus_mod_spatial_bayes(
            df=dados, outcome="obitos", W=W, covariates=["temp_media"],
            offset="esperado", model=modelo, n_iter=14000, burnin=2000,
            thin=10, seed=42, verbose=False)

        fx_r = pd.read_parquet(FIX / f"ref_{modelo}_fixed.parquet")
        coef_r = float(fx_r.loc[fx_r["term"] == "temp_media", "mean"].iloc[0])
        fx_p = fit["fixed"]
        coef_p = float(fx_p.loc[fx_p["term"] == "temp_media", "mean"].iloc[0])
        assert coef_p == pytest.approx(coef_r, abs=0.02)

        dic_r = float(pd.read_parquet(
            FIX / f"ref_{modelo}_meta.parquet")["dic"].iloc[0])
        assert abs(fit["dic"] - dic_r) < 6.0


@metadado_bym2
class TestBym2:
    """A reparametrizacao de Riebler et al. (2016), contra o INLA.

    O R ajusta este modelo com INLA -- aproximacao de Laplace, que nem
    amostra -- e aqui ele e NUTS. Que as duas maquinarias concordem em 3 a
    4 casas e o resultado que sustenta a decisao de usar PyMC.

    Medido com 1200 amostras por cadeia em 2 cadeias:
      rr_mean   corr 1.0000, dif media 0.0019, maxima 0.0052
      phi_mean  corr 0.9998
      intercepto -3.038 (sd 0.725) contra -3.115 (sd 0.747)
      temp_media  0.1321 (sd 0.0323) contra 0.1355 (sd 0.0334)
      DIC  339.27 contra 340.09     WAIC 331.90 contra 332.77

    Duas aproximacoes declaradas: a PC prior do parametro de mistura nao
    tem forma fechada no PyMC e foi trocada por uma Beta calibrada na MESMA
    afirmacao de probabilidade (P(rho < 0.5) = 2/3); e o WAIC e calculado
    aqui em vez de pelo ArviZ, que na versao 1.x removeu o waic.
    """

    def test_fator_de_escala_do_icar(self, W):
        from climasus4py.enrichment.mod_spatial_bayes import _icar_scale

        s = _icar_scale(W)
        # geometria conhecida: grade 6x7 rook. O valor tem de ser positivo e
        # da ordem da unidade; se virar 0 ou inf, o rho deixa de dividir
        # variancia e o modelo perde o sentido
        assert 0.1 < s < 10.0, s

    def test_taxa_da_pc_prec(self):
        from climasus4py.enrichment.mod_spatial_bayes import _pc_prec_rate

        # P(sigma > 1) = 0.01  ->  taxa = -log(0.01)/1
        assert _pc_prec_rate(1.0, 0.01) == pytest.approx(4.60517, abs=1e-5)

    def test_beta_calibrada_na_mesma_probabilidade(self):
        from scipy import stats

        from climasus4py.enrichment.mod_spatial_bayes import _PC_PHI_BETA

        # e o ponto de calibracao da PC prior do R: P(rho < 0.5) = 2/3
        assert stats.beta(*_PC_PHI_BETA).cdf(0.5) == pytest.approx(2 / 3,
                                                                   abs=1e-9)

    def test_tabela_de_efeitos_fixos_traz_o_intercepto(self, dados, W):
        fit = _ajusta(dados, W, model="bym2")
        assert fit["fixed"]["term"].tolist() == ["(Intercept)", "temp_media"]

    def test_a_chave_do_m88_nao_alcanca_o_bym2(self, dados, W):
        """M88 e defeito do caminho CARBayes, nao deste.

        O bym2 do R monta a tabela a partir do summary.fixed do INLA, que
        JA traz o intercepto e um sd de verdade. Aplicar a chave aqui faria
        divergir DO R, nao na direcao dele.
        """
        from climasus4py.enrichment import mod_spatial_bayes as mod

        antes = mod.R_FIXED_TABLE_DROPS_INTERCEPT
        mod.R_FIXED_TABLE_DROPS_INTERCEPT = True
        try:
            fit = _ajusta(dados, W, model="bym2")
        finally:
            mod.R_FIXED_TABLE_DROPS_INTERCEPT = antes
        assert fit["fixed"]["term"].tolist() == ["(Intercept)", "temp_media"]
        assert fit["fixed"]["sd"].notna().all()

    def test_dic_e_waic_tem_tipo_estavel(self, dados, W):
        """O slot dic do R e escalar em tres modelos e vetor nomeado no bym2.

        Mesmo campo, dois tipos. Aqui dic e sempre float e o waic tem chave
        propria, entao nada se perde e o tipo nao depende do modelo.
        """
        a = _ajusta(dados, W, model="bym2")
        b = _ajusta(dados, W, model="independent")
        for fit in (a, b):
            assert isinstance(fit["dic"], float)
            assert isinstance(fit["waic"], float)


@metadado_bym2
@lento
class TestBym2ParidadeComInla:
    def test_dic_e_waic_contra_o_inla(self, dados, W):
        fit = sus_mod_spatial_bayes(
            df=dados, outcome="obitos", W=W, covariates=["temp_media"],
            offset="esperado", model="bym2", n_iter=14000, burnin=2000,
            thin=10, seed=42, verbose=False)
        mt = pd.read_parquet(FIX / "ref_bym2_meta.parquet")
        assert abs(fit["dic"] - float(mt["dic"].iloc[0])) < 4.0
        assert abs(fit["waic"] - float(mt["waic"].iloc[0])) < 4.0

    def test_sd_dos_efeitos_fixos(self, dados, W):
        """O INLA da sd de verdade aqui, entao ele tambem e comparavel."""
        fit = sus_mod_spatial_bayes(
            df=dados, outcome="obitos", W=W, covariates=["temp_media"],
            offset="esperado", model="bym2", n_iter=14000, burnin=2000,
            thin=10, seed=42, verbose=False)
        fx_r = pd.read_parquet(FIX / "ref_bym2_fixed.parquet")
        for termo in ("(Intercept)", "temp_media"):
            sd_r = float(fx_r.loc[fx_r["term"] == termo, "sd"].iloc[0])
            sd_p = float(fit["fixed"].loc[
                fit["fixed"]["term"] == termo, "sd"].iloc[0])
            assert sd_p == pytest.approx(sd_r, rel=0.25), termo
