"""sus_mod_spacetime_predict: aritmetica de sumario e os contratos quebrados.

O R compoe a predicao a partir das ESTATISTICAS RESUMO do ajuste -- medias
somam, variancias somam -- e nao das amostras posteriores; a propria
documentacao dele chama o resultado de aproximado. Esta aritmetica e
seguida aqui, o que permite paridade EXATA e nao apenas estatistica: os
testes abaixo comparam contra numeros medidos no R, casa por casa.

O achado M110: o predict do R le cinco slots e colunas que o ajustador do
R nunca escreve, e quatro das falhas sao silenciosas. A mais grave e a
coluna gamma_mean em temporal_re (o ajustador escreve psi_mean), que faz o
EFEITO TEMPORAL SER DESCARTADO DE TODA PREDICAO sem erro nem aviso.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.enrichment import mod_spacetime_predict as mod
from climasus4py.enrichment.mod_spacetime_predict import (
    sus_mod_spacetime_predict,
)

FIX = Path(__file__).parent / "fixtures" / "spacetime"

# medido chamando o sus_mod_spacetime_predict do R sobre o ajuste real do
# INLA remontado das fixtures, com newdata nos tres primeiros municipios e
# time_idx = 1, temp_media = 22.0
R_COMO_ESTA = [1.0102904, 0.9972717, 0.9720201]
R_COM_NOMES_CORRIGIDOS = [0.9114105, 0.8996660, 0.8768859]


@pytest.fixture
def ajuste():
    """O ajuste do INLA, remontado das fixtures congeladas."""
    return {
        "fixed": pd.read_parquet(FIX / "st_fixed.parquet"),
        "spatial_re": pd.read_parquet(FIX / "st_spatial_re.parquet"),
        "temporal_re": pd.read_parquet(FIX / "st_temporal_re.parquet"),
        "model_spec": {"family": "poisson", "temporal_model": "rw1"},
        "n_times": 8,
    }


@pytest.fixture
def pontos(ajuste):
    nd = pd.DataFrame(
        [(m, 1) for m in ajuste["spatial_re"]["code_muni"].iloc[:3]],
        columns=["code_muni", "time_idx"])
    nd["temp_media"] = 22.0
    return nd


class TestParidadeExataComR:
    """Aritmetica de sumario permite paridade exata, e ela e exigida aqui.

    Nao e concordancia estatistica: sao as mesmas somas sobre as mesmas
    estatisticas, entao os numeros tem de coincidir em varias casas. Os
    dois lados da chave foram medidos no R.
    """

    def test_com_a_chave_ligada_reproduz_o_r_como_esta(self, ajuste, pontos):
        antes = mod.R_DROPS_TEMPORAL_EFFECT
        mod.R_DROPS_TEMPORAL_EFFECT = True
        try:
            p = sus_mod_spacetime_predict(ajuste, newdata=pontos,
                                          verbose=False)["predictions"]
        finally:
            mod.R_DROPS_TEMPORAL_EFFECT = antes
        assert p["pred_mean"].to_numpy() == pytest.approx(R_COMO_ESTA,
                                                          abs=1e-6)

    def test_o_default_reproduz_o_r_com_os_nomes_corrigidos(self, ajuste,
                                                            pontos):
        """A prova de que a diferenca e SO o efeito temporal descartado.

        Corrigindo os nomes das colunas no lado R, ele produz exatamente
        estes numeros. Aritmetica identica; a unica diferenca e o termo que
        a guarda do R pula.
        """
        p = sus_mod_spacetime_predict(ajuste, newdata=pontos,
                                      verbose=False)["predictions"]
        assert p["pred_mean"].to_numpy() == pytest.approx(
            R_COM_NOMES_CORRIGIDOS, abs=1e-6)

    def test_o_tamanho_do_erro(self, ajuste, pontos):
        """11% na primeira celula, e parece um numero perfeitamente comum."""
        erro = abs(R_COMO_ESTA[0] / R_COM_NOMES_CORRIGIDOS[0] - 1.0)
        assert erro > 0.10


class TestM110ContratosQuebrados:
    """Os nomes que o produtor escreve sao os que este consumidor le."""

    def test_le_psi_mean_nao_gamma_mean(self, ajuste, pontos):
        """O ajustador escreve psi_*; gamma_* e a tabela de INTERACAO."""
        assert "psi_mean" in ajuste["temporal_re"].columns
        assert "gamma_mean" not in ajuste["temporal_re"].columns
        p = sus_mod_spacetime_predict(ajuste, newdata=pontos,
                                      verbose=False)["predictions"]
        # se o efeito temporal tivesse sido pulado, sairia R_COMO_ESTA
        assert p["pred_mean"].iloc[0] != pytest.approx(R_COMO_ESTA[0],
                                                       abs=1e-4)

    def test_aceita_gamma_mean_se_alguem_passar(self, ajuste, pontos):
        """Compatibilidade: um ajuste montado a mao com os nomes do R funciona."""
        t = ajuste["temporal_re"].rename(
            columns={"psi_mean": "gamma_mean", "psi_sd": "gamma_sd"})
        ajuste["temporal_re"] = t
        p = sus_mod_spacetime_predict(ajuste, newdata=pontos,
                                      verbose=False)["predictions"]
        assert p["pred_mean"].to_numpy() == pytest.approx(
            R_COM_NOMES_CORRIGIDOS, abs=1e-6)

    def test_familia_e_modelo_vem_de_model_spec(self, ajuste, pontos):
        """O R le fit$family e fit$temporal_model; o ajustador nao os escreve.

        Aqui vem de model_spec, com o slot de topo aceito como alternativa.
        """
        from climasus4py.enrichment.mod_spacetime_predict import _spec

        assert _spec(ajuste, "family", "x") == "poisson"
        assert _spec(ajuste, "temporal_model", "x") == "rw1"
        sem_spec = {k: v for k, v in ajuste.items() if k != "model_spec"}
        sem_spec["family"] = "binomial"
        assert _spec(sem_spec, "family", "poisson") == "binomial"

    def test_horizonte_nao_devolve_tudo_na(self, ajuste):
        """O R devolve TODOS NA aqui, por ler um slot que nao existe.

        Medido: horizon=2 deu 84 linhas de NA. A causa e fit$data_last_obs,
        que o ajustador do R nunca escreve, entao as covariaveis caem no
        ramo NA_real_.
        """
        ajuste["data_last_obs"] = pd.DataFrame({
            "code_muni": ajuste["spatial_re"]["code_muni"],
            "temp_media": 22.0,
        })
        p = sus_mod_spacetime_predict(ajuste, horizon=2,
                                      verbose=False)["predictions"]
        assert len(p) == 42 * 2
        assert p["pred_mean"].notna().all(), (
            "e exatamente o defeito do R: previsao que sai toda NA"
        )

    def test_sem_data_last_obs_recusa_em_vez_de_devolver_na(self, ajuste):
        """Se a covariavel nao pode ser levada adiante, e melhor recusar.

        O R preenche com NA_real_ e devolve NA silenciosamente. Aqui a
        mensagem diz o que falta e como resolver.
        """
        with pytest.raises(ValueError, match="no usable values"):
            sus_mod_spacetime_predict(ajuste, horizon=2, verbose=False)


class TestExtrapolacao:
    def test_marca_o_que_e_fora_da_amostra(self, ajuste):
        ajuste["data_last_obs"] = pd.DataFrame({
            "code_muni": ajuste["spatial_re"]["code_muni"],
            "temp_media": 22.0})
        p = sus_mod_spacetime_predict(ajuste, horizon=2,
                                      verbose=False)["predictions"]
        assert not p["in_sample"].any()
        assert sorted(p["time_idx"].unique()) == [9, 10]

    def test_o_intervalo_alarga_com_o_horizonte(self, ajuste):
        """A variancia de um passeio aleatorio cresce linear no horizonte."""
        ajuste["data_last_obs"] = pd.DataFrame({
            "code_muni": ajuste["spatial_re"]["code_muni"],
            "temp_media": 22.0})
        p = sus_mod_spacetime_predict(ajuste, horizon=3,
                                      verbose=False)["predictions"]
        largura = (p.assign(l=p["pred_upper95"] - p["pred_lower95"])
                    .groupby("time_idx")["l"].mean())
        assert largura.loc[9] < largura.loc[10] < largura.loc[11]

    def test_rw2_tem_deriva_e_rw1_nao(self, ajuste):
        ajuste["data_last_obs"] = pd.DataFrame({
            "code_muni": ajuste["spatial_re"]["code_muni"],
            "temp_media": 22.0})
        p1 = sus_mod_spacetime_predict(ajuste, horizon=3,
                                       verbose=False)["predictions"]
        # RW1: sem deriva, entao a media nao muda de um periodo para o outro
        m1 = p1.groupby("time_idx")["pred_mean"].mean()
        assert m1.loc[9] == pytest.approx(m1.loc[11], rel=1e-9)

        ajuste["model_spec"]["temporal_model"] = "rw2"
        p2 = sus_mod_spacetime_predict(ajuste, horizon=3,
                                       verbose=False)["predictions"]
        m2 = p2.groupby("time_idx")["pred_mean"].mean()
        assert m2.loc[9] != pytest.approx(m2.loc[11], rel=1e-6), (
            "RW2 extrapola com a ultima inclinacao observada"
        )


class TestCenarioContrafactual:
    def test_covariates_new_muda_a_predicao(self, ajuste, pontos):
        base = sus_mod_spacetime_predict(
            ajuste, newdata=pontos, verbose=False)["predictions"]
        quente = sus_mod_spacetime_predict(
            ajuste, newdata=pontos, covariates_new={"temp_media": 26.0},
            verbose=False)["predictions"]
        # o coeficiente e negativo na fixture, entao mais quente baixa o risco
        assert (quente["pred_mean"] < base["pred_mean"]).all()


class TestGuardas:
    def test_fit_invalido(self, pontos):
        with pytest.raises(TypeError, match="fixed"):
            sus_mod_spacetime_predict({"nada": 1}, newdata=pontos)

    def test_horizonte_negativo(self, ajuste):
        with pytest.raises(ValueError, match="horizon must be"):
            sus_mod_spacetime_predict(ajuste, horizon=-1)

    def test_newdata_e_horizonte_juntos(self, ajuste, pontos):
        with pytest.raises(ValueError, match="not both"):
            sus_mod_spacetime_predict(ajuste, newdata=pontos, horizon=2)

    def test_newdata_sem_coluna(self, ajuste, pontos):
        with pytest.raises(ValueError, match="code_muni"):
            sus_mod_spacetime_predict(
                ajuste, newdata=pontos.drop(columns=["code_muni"]))

    def test_horizonte_zero_sem_newdata_devolve_vazio(self, ajuste):
        r = sus_mod_spacetime_predict(ajuste, horizon=0, verbose=False)
        assert r["n_predicted"] == 0
        assert len(r["predictions"]) == 0

    def test_municipio_desconhecido_avisa(self, ajuste, pontos):
        nd = pontos.copy()
        nd.loc[0, "code_muni"] = 9999999
        with pytest.warns(UserWarning, match="not in the fit"):
            sus_mod_spacetime_predict(ajuste, newdata=nd, verbose=False)

    def test_amostras_exigem_o_idata(self, ajuste, pontos):
        with pytest.raises(ValueError, match="fit\\['idata'\\]"):
            sus_mod_spacetime_predict(ajuste, newdata=pontos,
                                      return_samples=True, verbose=False)

    def test_sem_ic(self, ajuste, pontos):
        p = sus_mod_spacetime_predict(
            ajuste, newdata=pontos, include_ci=False,
            verbose=False)["predictions"]
        assert "pred_lower95" not in p.columns


class TestComAjusteDoPython:
    """Ponta a ponta: o produtor e o consumidor deste porte conversam."""

    @pytest.fixture(scope="class")
    @classmethod
    def ajuste_py(cls):
        pytest.importorskip("pymc")
        from climasus4py.enrichment.mod_spacetime_bayes import (
            sus_mod_spacetime_bayes,
        )

        df = pd.read_parquet(FIX / "st_df.parquet")
        n = df["code_muni"].nunique()
        nr, nc = 6, 7
        W = np.zeros((n, n))
        for r in range(nr):
            for c in range(nc):
                i = r * nc + c
                if r < nr - 1:
                    j = (r + 1) * nc + c
                    W[i, j] = W[j, i] = 1.0
                if c < nc - 1:
                    j = r * nc + c + 1
                    W[i, j] = W[j, i] = 1.0
        return sus_mod_spacetime_bayes(
            df=df, outcome="obitos", W=W, time_col="ano",
            covariates=["temp_media"], offset="esperado",
            n_samples=300, seed=42, verbose=False, chains=1)

    def test_o_ajustador_guarda_data_last_obs(self, ajuste_py):
        """O slot que o R le e nunca escreve."""
        assert "data_last_obs" in ajuste_py
        assert len(ajuste_py["data_last_obs"]) == 42
        assert "temp_media" in ajuste_py["data_last_obs"].columns

    def test_horizonte_funciona_sem_preparo(self, ajuste_py):
        p = sus_mod_spacetime_predict(ajuste_py, horizon=2,
                                      verbose=False)["predictions"]
        assert len(p) == 84
        assert p["pred_mean"].notna().all()

    def test_amostras_posteriores(self, ajuste_py):
        nd = pd.DataFrame(
            [(m, 1) for m in ajuste_py["spatial_re"]["code_muni"].iloc[:4]],
            columns=["code_muni", "time_idx"])
        nd["temp_media"] = 22.0
        r = sus_mod_spacetime_predict(ajuste_py, newdata=nd,
                                      return_samples=True, verbose=False)
        assert r["samples"].shape[1] == 4
        assert r["samples"].shape[0] == 300
        # a media das amostras tem de ficar perto da aritmetica de sumario
        assert r["samples"].mean(axis=0) == pytest.approx(
            r["predictions"]["pred_mean"].to_numpy(), rel=0.15)
