"""sus_mod_spacetime_bayes em PyMC: forma, invariantes e paridade.

O R ajusta com o INLA -- aproximacao de Laplace, que nao amostra -- e este
porte usa PyMC com NUTS, decidido em 17/09/2026. Nao ha fluxo a casar; o
criterio e concordancia estatistica contra a saida do INLA congelada em
tests/fixtures/spacetime/.

Tres achados se encontram nesta funcao e o porte decide diferente em cada:
M89 (o R nao roda no Windows -- nao se aplica aqui), M90 (o time_idx e
indice de base 1; decidido REPLICAR) e M91 (a interacao do R e
super-parametrizada; decidido CORRIGIR).
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pymc", reason="sus_mod_spacetime_bayes precisa de PyMC")

from climasus4py.enrichment._latent_gaussian import (  # noqa: E402
    null_rank,
    scale_structure,
    structure_icar,
    structure_rw,
)
from climasus4py.enrichment.mod_spacetime_bayes import (  # noqa: E402
    sus_mod_spacetime_bayes,
)

FIX = Path(__file__).parent / "fixtures" / "spacetime"


def _tem_nutpie() -> bool:
    try:
        import nutpie  # noqa: F401
    except ImportError:
        return False
    return True


lento = pytest.mark.skipif(
    not _tem_nutpie() and os.environ.get("CLIMASUS_SLOW_TESTS") != "1",
    reason="paridade MCMC sem nutpie e lenta; instale climasus4py[bayes]",
)


@pytest.fixture(scope="module")
def dados():
    return pd.read_parquet(FIX / "st_df.parquet")


@pytest.fixture(scope="module")
def W(dados):
    """A grade 6x7 rook com que a referencia do INLA foi gerada."""
    n = dados["code_muni"].nunique()
    nr, nc = 6, 7
    m = np.zeros((n, n))
    for r in range(nr):
        for c in range(nc):
            i = r * nc + c
            if r < nr - 1:
                j = (r + 1) * nc + c
                m[i, j] = m[j, i] = 1.0
            if c < nc - 1:
                j = r * nc + c + 1
                m[i, j] = m[j, i] = 1.0
    return m


def _ajusta(dados, W, **kw):
    base = dict(df=dados, outcome="obitos", W=W, time_col="ano",
                covariates=["temp_media"], offset="esperado",
                n_samples=300, seed=42, verbose=False, chains=1)
    base.update(kw)
    return sus_mod_spacetime_bayes(**base)


class TestEstruturasGaussianas:
    """Os priors estruturados, antes de qualquer ajuste.

    Sao a base de tudo -- ICAR, RW1, RW2 e os Kronecker das interacoes --
    e um erro aqui contamina todos os termos de uma vez, entao os
    invariantes sao conferidos direto.
    """

    def test_rank_nulo(self, W):
        # a constante do ICAR, o deslocamento do RW1, deslocamento e
        # inclinacao do RW2
        assert null_rank(structure_icar(W)) == 1
        assert null_rank(structure_rw(8, 1)) == 1
        assert null_rank(structure_rw(8, 2)) == 2

    def test_rank_nulo_dos_kronecker(self, W):
        R_esp, R_t = structure_icar(W), structure_rw(8, 1)
        n_a, n_t = 42, 8
        # Knorr-Held: II tem um deslocamento por area, III uma constante
        # por periodo, IV as duas coisas menos a sobreposicao
        assert null_rank(np.kron(np.eye(n_a), R_t)) == n_a
        assert null_rank(np.kron(R_esp, np.eye(n_t))) == n_t
        assert null_rank(np.kron(R_esp, R_t)) == n_a + n_t - 1

    @pytest.mark.parametrize("construtor", [
        lambda W: structure_icar(W),
        lambda W: structure_rw(8, 1),
        lambda W: structure_rw(8, 2),
        lambda W: np.kron(np.eye(42), structure_rw(8, 1)),
        lambda W: np.kron(structure_icar(W), np.eye(8)),
        lambda W: np.kron(structure_icar(W), structure_rw(8, 1)),
    ])
    def test_escala_leva_a_variancia_generalizada_para_um(self, W, construtor):
        """`scale.model=TRUE` do INLA, e o invariante que pegou meu erro.

        Eu havia DIVIDIDO pelo fator em vez de multiplicar, o que deixava o
        ICAR em 0,324 e o RW1 em 1,424. A algebra parecia certa; o
        invariante nao deixou passar.
        """
        from climasus4py.enrichment._latent_gaussian import basis

        V, lam = basis(scale_structure(construtor(W)))
        diag_inv = ((V ** 2) / lam).sum(axis=1)
        assert float(np.exp(np.mean(np.log(diag_inv)))) == pytest.approx(
            1.0, abs=1e-9)

    def test_rw_exige_pontos_suficientes(self):
        with pytest.raises(ValueError, match="more than"):
            structure_rw(2, 2)


class TestForma:
    def test_chaves(self, dados, W):
        fit = _ajusta(dados, W)
        for k in ("fixed", "rr", "spatial_re", "temporal_re",
                  "interaction_re", "fitted", "waic", "dic", "model_spec",
                  "n_areas", "n_times", "time_labels"):
            assert k in fit, k

    def test_dimensoes(self, dados, W):
        fit = _ajusta(dados, W)
        assert fit["n_areas"] == 42
        assert fit["n_times"] == 8
        assert len(fit["rr"]) == 336
        assert len(fit["spatial_re"]) == 42
        assert len(fit["temporal_re"]) == 8
        assert fit["interaction_re"] is None

    def test_intervalo_contem_a_media(self, dados, W):
        rr = _ajusta(dados, W)["rr"]
        assert (rr["rr_lower95"] <= rr["rr_mean"]).all()
        assert (rr["rr_mean"] <= rr["rr_upper95"]).all()

    def test_probabilidade_no_intervalo_unitario(self, dados, W):
        rr = _ajusta(dados, W)["rr"]
        for col in ("p_exceed", "p_exceed_empirical"):
            assert rr[col].between(0.0, 1.0).all()


class TestTimeIdxReplicaOR:
    """M90: o time_idx e indice de base 1, e esta funcao e a PRODUTORA.

    Decidido em 15/09/2026 replicar o R. E por isso que o
    sus_mod_spacetime_exceedance rotula periodos como "0001": ele le este
    indice como ano. O rotulo original nao e descartado -- vai em
    time_labels e na coluna time_label --, mas o time_idx continua sendo
    o indice, como no R.
    """

    def test_e_indice_nao_ano(self, dados, W):
        fit = _ajusta(dados, W)
        assert sorted(fit["rr"]["time_idx"].unique()) == list(range(1, 9))
        assert fit["temporal_re"]["time_idx"].tolist() == list(range(1, 9))

    def test_o_rotulo_original_sobrevive_ao_lado(self, dados, W):
        fit = _ajusta(dados, W)
        assert fit["time_labels"].tolist() == list(range(2015, 2023))
        primeira = fit["rr"].iloc[0]
        assert primeira["time_idx"] == 1
        assert primeira["time_label"] == 2015


class TestInteracaoKnorrHeld:
    """M91: a interacao do R e super-parametrizada, nao so mal rotulada.

    O R faz area.time = (area-1)*n_times + time, que e o indice de CELULA,
    e depois passa isso a f(area.time, model=..., group=.time_idx_st). No
    INLA o indice principal de um termo agrupado tem de percorrer os nos do
    grafo (42 areas); passar o indice de celula multiplica a dimensao
    latente em vez de fatora-la: 336 x 8 = 2688 valores latentes para 336
    observacoes.

    MEDIDO na referencia do R: gamma_sd de 6,34 a 7,22 com |gamma_mean|
    medio 0,139 -- razao 47,5, ou seja termo que o dado nao identifica.
    Nesta implementacao, com o indice de area e estrutura Kronecker, os
    quatro tipos dao 336 linhas e razao entre 0,9 e 1,5.
    """

    @pytest.mark.parametrize("tipo", ["I", "II", "III", "IV"])
    def test_uma_linha_por_celula(self, dados, W, tipo):
        fit = _ajusta(dados, W, interaction_type=tipo)
        ia = fit["interaction_re"]
        assert len(ia) == 336, (
            f"tipo {tipo}: {len(ia)} linhas. O R da 2688 para as mesmas 336 "
            f"celulas; replicar isso era o defeito"
        )
        assert ia["code_muni"].nunique() == 42
        assert ia["code_muni"].value_counts().max() == 8

    @pytest.mark.parametrize("tipo", ["I", "II", "III", "IV"])
    def test_o_termo_e_identificavel(self, dados, W, tipo):
        """O contraste com o R: la a razao sd/|media| e 47,5."""
        ia = _ajusta(dados, W, interaction_type=tipo)["interaction_re"]
        razao = ia["gamma_sd"].mean() / max(ia["gamma_mean"].abs().mean(),
                                            1e-12)
        assert razao < 10.0, (
            f"tipo {tipo}: razao sd/|media| = {razao:.1f}. Acima de ~10 o "
            f"termo deixou de ser identificavel, que e o estado da "
            f"referencia do R"
        )

    def test_a_chave_reproduz_a_forma_do_r(self, dados, W):
        from climasus4py.enrichment import mod_spacetime_bayes as mod

        antes = mod.R_INTERACTION_TABLE_CLAMPS_AREAS
        mod.R_INTERACTION_TABLE_CLAMPS_AREAS = True
        try:
            ia = _ajusta(dados, W, interaction_type="IV")["interaction_re"]
        finally:
            mod.R_INTERACTION_TABLE_CLAMPS_AREAS = antes
        assert len(ia) == 336 * 8
        contagem = ia["code_muni"].value_counts()
        assert contagem.max() == 2360, (
            "e o numero do achado: 2360 das 2688 linhas num municipio so"
        )


class TestGuardas:
    def test_opcao_desconhecida(self, dados, W):
        for kw in ({"family": "x"}, {"spatial_model": "x"},
                   {"temporal_model": "x"}, {"interaction_type": "x"},
                   {"time_unit": "x"}):
            with pytest.raises(ValueError, match="must be one of"):
                _ajusta(dados, W, **kw)

    def test_coluna_ausente(self, dados, W):
        with pytest.raises(ValueError, match="missing column"):
            _ajusta(dados, W, covariates=["nao_existe"])

    def test_painel_incompleto_recusa(self, dados, W):
        """Uma celula faltando quebraria a indexacao em silencio."""
        with pytest.raises(ValueError, match="one row per area-period"):
            _ajusta(dados.iloc[:-1], W)

    def test_W_de_tamanho_errado(self, dados, W):
        with pytest.raises(ValueError, match="to match the"):
            _ajusta(dados, W[:10, :10])

    def test_W_assimetrico(self, dados, W):
        ruim = W.copy()
        ruim[0, 1], ruim[1, 0] = 1.0, 0.0
        with pytest.raises(ValueError, match="symmetric"):
            _ajusta(dados, ruim)


class TestVariantesDeModelo:
    @pytest.mark.parametrize("sp", ["bym2", "bym", "besag", "iid"])
    def test_todos_os_espaciais_ajustam(self, dados, W, sp):
        fit = _ajusta(dados, W, spatial_model=sp)
        assert len(fit["spatial_re"]) == 42
        assert fit["spatial_re"]["phi_sd"].gt(0).all()

    @pytest.mark.parametrize("tp", ["rw1", "rw2", "ar1", "iid_time"])
    def test_todos_os_temporais_ajustam(self, dados, W, tp):
        fit = _ajusta(dados, W, temporal_model=tp)
        assert len(fit["temporal_re"]) == 8
        assert fit["model_spec"]["temporal_model"] == tp


@lento
class TestParidadeComInla:
    """Concordancia com o INLA no ajuste SEM interacao.

    Medido com 1000 amostras por cadeia em 2 cadeias:
      rr_mean          corr 0.9997, dif media 0.0030
      spatial_re.phi   corr 0.9997
      temporal_re.psi  corr 1.0000
      temp_media       -0.0228 contra -0.0237
      (Intercept)       0.5724 contra  0.5915

    Os tipos II a IV NAO sao comparados contra a referencia de proposito:
    la o termo de interacao e super-parametrizado (M91), e casar com ele
    exigiria reproduzir a super-parametrizacao.
    """

    def test_rr_e_efeitos_aleatorios(self, dados, W):
        fit = sus_mod_spacetime_bayes(
            df=dados, outcome="obitos", W=W, time_col="ano",
            covariates=["temp_media"], offset="esperado",
            interaction_type="none", n_samples=1000, seed=42, verbose=False)

        rr_r = pd.read_parquet(FIX / "st_rr.parquet")
        a = fit["rr"]["rr_mean"].to_numpy()
        b = rr_r["rr_mean"].to_numpy()
        assert np.corrcoef(a, b)[0, 1] > 0.995
        assert np.abs(a - b).mean() < 0.02

        sp_r = pd.read_parquet(FIX / "st_spatial_re.parquet")
        assert np.corrcoef(fit["spatial_re"]["phi_mean"].to_numpy(),
                           sp_r["phi_mean"].to_numpy())[0, 1] > 0.99
        tp_r = pd.read_parquet(FIX / "st_temporal_re.parquet")
        assert np.corrcoef(fit["temporal_re"]["psi_mean"].to_numpy(),
                           tp_r["psi_mean"].to_numpy())[0, 1] > 0.99

    def test_efeitos_fixos(self, dados, W):
        fit = sus_mod_spacetime_bayes(
            df=dados, outcome="obitos", W=W, time_col="ano",
            covariates=["temp_media"], offset="esperado",
            interaction_type="none", n_samples=1000, seed=42, verbose=False)
        fx_r = pd.read_parquet(FIX / "st_fixed.parquet")
        fx_p = fit["fixed"]
        assert fx_p["term"].tolist() == fx_r["term"].tolist()
        for termo, tol in (("(Intercept)", 0.1), ("temp_media", 0.005)):
            r_ = float(fx_r.loc[fx_r["term"] == termo, "mean"].iloc[0])
            p_ = float(fx_p.loc[fx_p["term"] == termo, "mean"].iloc[0])
            assert p_ == pytest.approx(r_, abs=tol), termo

    def test_a_aproximacao_normal_ganha_na_cauda(self, dados, W):
        """Por que p_exceed segue o R, contra a minha primeira intuicao.

        Eu havia posto a contagem empirica como default, por ser livre de
        suposicao. Nas celulas onde a probabilidade e pequena -- que e
        onde uma probabilidade de excedencia e lida -- a contagem sobre
        2000 amostras nao resolve abaixo de 1/2000 e devolve ZERO. Medido:
        15 das 43 celulas com p_R < 0.01 sairam zero exato, e o erro medio
        da empirica foi o dobro do da normal.
        """
        fit = sus_mod_spacetime_bayes(
            df=dados, outcome="obitos", W=W, time_col="ano",
            covariates=["temp_media"], offset="esperado",
            interaction_type="none", n_samples=1000, seed=42, verbose=False)
        rr_r = pd.read_parquet(FIX / "st_rr.parquet")
        p_r = rr_r["p_exceed"].to_numpy()
        cauda = p_r < 0.01
        assert cauda.sum() > 10, "a fixture perdeu as celulas de cauda"
        erro_normal = np.abs(
            fit["rr"]["p_exceed"].to_numpy()[cauda] - p_r[cauda]).mean()
        erro_empirico = np.abs(
            fit["rr"]["p_exceed_empirical"].to_numpy()[cauda]
            - p_r[cauda]).mean()
        assert erro_normal < erro_empirico, (
            f"normal {erro_normal:.6f} contra empirica {erro_empirico:.6f}"
        )
        assert (fit["rr"]["p_exceed"].to_numpy()[cauda] > 0).all(), (
            "a aproximacao normal nunca devolve zero exato, que e a sua "
            "vantagem aqui"
        )
