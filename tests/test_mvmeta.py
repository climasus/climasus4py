"""Tests para _mvmeta.py -- meta-analise multivariada de efeitos aleatorios.

O pacote R ``mvmeta`` NAO esta instalado nesta maquina, entao nao ha como
comparar contra ele. Em compensacao o alvo e definido pela verossimilhanca,
nao pela implementacao: cada teste aqui confronta o codigo com uma
referencia independente -- a forma fechada univariada, o ponto fixo REML de
Viechtbauer, o gradiente numerico, ou uma propriedade que a estimativa tem
de satisfazer por construcao.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from climasus4py.enrichment._mvmeta import (
    _n_par,
    _neg_loglik,
    _neg_loglik_grad,
    _par_to_psi,
    _psi_to_par,
    blup,
    mvmeta_fit,
    qtest,
)

optimize = pytest.importorskip("scipy.optimize")


# ---------------------------------------------------------------------------
# Referencias independentes
# ---------------------------------------------------------------------------

def reml_univariado(y: np.ndarray, v: np.ndarray) -> tuple[float, float, float]:
    """REML univariado pelo ponto fixo de Viechtbauer (2005).

    Referencia independente: nao usa nada de _mvmeta. Itera
    ``tau2 = sum w^2 [(y-mu)^2 + 1/sum(w) - v] / sum(w^2)`` ate o ponto
    fixo, com ``w = 1/(v + tau2)``.
    """
    tau2 = 0.0
    for _ in range(20000):
        w = 1.0 / (v + tau2)
        mu = (w * y).sum() / w.sum()
        novo = max(0.0, (w**2 * ((y - mu) ** 2 + 1.0 / w.sum() - v)).sum() / (w**2).sum())
        if abs(novo - tau2) < 1e-14:
            tau2 = novo
            break
        tau2 = novo
    w = 1.0 / (v + tau2)
    return tau2, (w * y).sum() / w.sum(), 1.0 / w.sum()


def estudos(p: int, m: int, seed: int = 3) -> tuple[np.ndarray, list[np.ndarray], np.ndarray]:
    """Gera m estudos p-variados com Psi e beta conhecidos."""
    r = np.random.default_rng(seed)
    A = r.normal(0, 0.3, (p, p))
    psi = A @ A.T
    beta = r.normal(0, 0.3, p)
    y, S = [], []
    for _ in range(m):
        B = r.normal(0, 0.2, (p, p))
        s = B @ B.T + np.eye(p) * 0.05
        S.append(s)
        efeito = beta + r.multivariate_normal(np.zeros(p), psi)
        y.append(r.multivariate_normal(efeito, s))
    return np.array(y), S, beta


FIXTURES = Path(__file__).parent / "fixtures" / "mvmeta"


@pytest.fixture(scope="module")
def referencia():
    """Casos de teste e a saida do mvmeta 1.0.3 do R sobre eles."""
    casos = json.loads((FIXTURES / "casos.json").read_text(encoding="utf-8"))
    ref = json.loads((FIXTURES / "referencia_r.json").read_text(encoding="utf-8"))
    return casos, ref


Y_HET = np.array([0.30, 0.12, 0.55, 0.21, 0.42, -0.05, 0.33, 0.60])
V_HET = np.array([0.02, 0.05, 0.03, 0.01, 0.04, 0.02, 0.06, 0.03])


def _uni(y: np.ndarray, v: np.ndarray) -> list[np.ndarray]:
    return [np.array([[vi]]) for vi in v]


# ---------------------------------------------------------------------------
# Caso univariado: forma fechada
# ---------------------------------------------------------------------------

class TestUnivariado:
    def test_reml_bate_com_o_ponto_fixo(self):
        """p=1 tem de reproduzir a meta-analise de efeitos aleatorios classica."""
        tau2, mu, var = reml_univariado(Y_HET, V_HET)
        f = mvmeta_fit(Y_HET[:, None], _uni(Y_HET, V_HET), method="reml")

        assert f.converged
        assert f.psi[0, 0] == pytest.approx(tau2, abs=1e-10)
        assert f.coef[0] == pytest.approx(mu, abs=1e-10)
        assert f.vcov[0, 0] == pytest.approx(var, abs=1e-10)

    def test_fixed_e_a_media_ponderada_pelo_inverso_da_variancia(self):
        """method='fixed' tem forma fechada; nao ha o que otimizar."""
        f = mvmeta_fit(Y_HET[:, None], _uni(Y_HET, V_HET), method="fixed")
        w = 1.0 / V_HET

        assert f.coef[0] == pytest.approx((w * Y_HET).sum() / w.sum(), abs=1e-12)
        assert f.vcov[0, 0] == pytest.approx(1.0 / w.sum(), abs=1e-12)
        assert np.allclose(f.psi, 0.0)

    def test_estudos_homogeneos_levam_psi_a_zero(self):
        """A solucao sem heterogeneidade fica na FRONTEIRA do espaco.

        E o motivo de a diagonal da fatoracao de Cholesky nao ser
        exponenciada em ``_par_to_psi``: com ``exp`` o zero so seria
        alcancado em menos infinito e o otimizador nunca chegaria la.
        """
        y = np.array([0.200, 0.210, 0.190, 0.205, 0.198])
        v = np.full(5, 0.02)
        f = mvmeta_fit(y[:, None], _uni(y, v), method="reml")
        w = 1.0 / v

        assert f.psi[0, 0] == pytest.approx(0.0, abs=1e-10)
        # Sem heterogeneidade, o agrupado colapsa no de efeitos fixos.
        assert f.coef[0] == pytest.approx((w * y).sum() / w.sum(), abs=1e-8)

    def test_q_de_cochran(self):
        """Q univariado e a soma dos quadrados ponderada sob efeitos fixos."""
        f = mvmeta_fit(Y_HET[:, None], _uni(Y_HET, V_HET), method="fixed")
        w = 1.0 / V_HET
        mu = (w * Y_HET).sum() / w.sum()
        het = qtest(f)

        assert het["Q"] == pytest.approx((w * (Y_HET - mu) ** 2).sum(), abs=1e-10)
        assert het["df"] == len(Y_HET) - 1
        assert 0.0 <= het["i2"] <= 100.0

    def test_i2_e_zero_quando_nao_ha_excesso_de_dispersao(self):
        """I2 tem piso em zero: Q abaixo dos graus de liberdade nao e heterogeneidade negativa."""
        y = np.array([0.200, 0.201, 0.199, 0.200])
        v = np.full(4, 0.05)
        het = qtest(mvmeta_fit(y[:, None], _uni(y, v), method="fixed"))

        assert het["Q"] < het["df"]
        assert het["i2"] == 0.0


# ---------------------------------------------------------------------------
# Gradiente analitico
# ---------------------------------------------------------------------------

class TestGradiente:
    """O gradiente fechado e o que torna o p realista viavel.

    Medido antes de existir: p=16 (136 parametros) levava 111 s e o BFGS
    parava antes da tolerancia. Com o gradiente, 2,5 s e convergencia.
    Se ele estiver errado, a estimativa fica errada em silencio -- dai a
    conferencia contra a derivada numerica.
    """

    @pytest.mark.parametrize(("p", "m", "restricted"),
                             [(2, 6, True), (3, 8, True), (4, 10, True),
                              (3, 8, False), (6, 9, False)])
    def test_bate_com_a_derivada_numerica(self, p, m, restricted):
        y, S, _ = estudos(p, m, seed=p * 10 + m)
        X = [np.eye(p) for _ in range(m)]
        r = np.random.default_rng(p)
        par = r.normal(0, 0.3, _n_par(p))

        analitico = _neg_loglik_grad(par, y, S, X, p, restricted)
        numerico = optimize.approx_fprime(par, _neg_loglik, 1e-7, y, S, X, p, restricted)

        escala = max(1e-12, float(np.abs(numerico).max()))
        assert np.abs(analitico - numerico).max() / escala < 1e-5


class TestParametrizacao:
    def test_ida_e_volta_psi(self):
        r = np.random.default_rng(2)
        A = r.normal(0, 0.4, (4, 4))
        psi = A @ A.T
        assert np.allclose(_par_to_psi(_psi_to_par(psi, 4), 4), psi, atol=1e-6)

    def test_par_zero_da_psi_zero(self):
        """par=0 -> Psi=0, que e a origem exigida pelo teste de fronteira."""
        assert np.allclose(_par_to_psi(np.zeros(_n_par(3)), 3), 0.0)

    def test_psi_estimado_e_sempre_psd(self):
        """Covariancia entre estudos negativa nao existe; a fatoracao garante isso."""
        y, S, _ = estudos(5, 10, seed=17)
        f = mvmeta_fit(y, S, method="reml")
        assert np.linalg.eigvalsh(f.psi).min() > -1e-10


# ---------------------------------------------------------------------------
# Multivariado
# ---------------------------------------------------------------------------

class TestMultivariado:
    def test_recupera_beta_dentro_do_erro_padrao(self):
        y, S, beta = estudos(3, 60, seed=11)
        f = mvmeta_fit(y, S, method="reml")
        ep = np.sqrt(np.diag(f.vcov))

        assert f.converged
        assert np.all(np.abs(f.coef - beta) / ep < 3.0)

    def test_ml_subestima_a_heterogeneidade_frente_ao_reml(self):
        """Propriedade conhecida: ML ignora os graus de liberdade gastos com beta."""
        y, S, _ = estudos(3, 25, seed=23)
        reml = mvmeta_fit(y, S, method="reml")
        ml = mvmeta_fit(y, S, method="ml")

        assert np.trace(ml.psi) < np.trace(reml.psi)

    def test_p_grande_converge_em_tempo_util(self):
        """p=16 e a dimensao de um crossbasis 4x4 -- o caso real do sus_mod_pool."""
        y, S, _ = estudos(16, 12, seed=3)
        f = mvmeta_fit(y, S, method="reml")

        assert f.converged
        assert f.psi.shape == (16, 16)
        assert np.linalg.eigvalsh(f.psi).min() > -1e-10

    def test_meta_regressao_recupera_o_moderador(self):
        """Os coeficientes saem em ordem OUTCOME-MAJOR, como no R.

        Isto e, ``[y1.intercepto, y1.inclinacao, y2.intercepto,
        y2.inclinacao]`` -- nao todos os interceptos seguidos de todas as
        inclinacoes. Trocar as duas ordens passa despercebido no
        agrupamento simples, onde ha um unico intercepto e os dois
        arranjos coincidem, mas permuta a ``vcov`` na meta-regressao: na
        comparacao contra o R os coeficientes batiam a 6e-08 enquanto a
        ``vcov`` errava por 82%.
        """
        p, m = 2, 80
        r = np.random.default_rng(31)
        x = r.normal(0, 1, m)
        intercepto, inclinacao = np.array([0.3, -0.1]), np.array([0.5, 0.2])
        S = [np.eye(p) * 0.05] * m
        y = np.array([r.multivariate_normal(intercepto + inclinacao * x[i], S[i])
                      for i in range(m)])

        f = mvmeta_fit(y, S, X=np.column_stack([np.ones(m), x]), method="reml")

        esperado = np.array([intercepto[0], inclinacao[0],
                             intercepto[1], inclinacao[1]])
        assert f.n_coef == 4  # q * p
        assert np.allclose(f.coef, esperado, atol=0.12)


# ---------------------------------------------------------------------------
# BLUP
# ---------------------------------------------------------------------------

class TestBlup:
    def test_sem_heterogeneidade_todo_estudo_colapsa_na_media(self):
        """Psi=0: nada distingue os estudos, entao a melhor previsao e a media."""
        y = np.array([[0.5, 0.1], [0.1, 0.4], [0.9, -0.2]])
        S = [np.eye(2) * 0.05] * 3
        f = mvmeta_fit(y, S, method="fixed")

        assert all(np.allclose(b["blup"], f.coef) for b in blup(f))

    def test_sem_ruido_interno_cada_estudo_mantem_o_proprio_valor(self):
        """S->0: o estudo e medido sem erro, entao encolher seria perder informacao."""
        y = np.array([[0.5, 0.1], [0.1, 0.4], [0.9, -0.2]])
        f = mvmeta_fit(y, [np.eye(2) * 1e-9] * 3, method="reml")

        for i, b in enumerate(blup(f)):
            assert np.allclose(b["blup"], y[i], atol=1e-4)

    def test_caso_intermediario_fica_entre_os_dois_extremos(self):
        y = np.array([[0.5, 0.1], [0.1, 0.4], [0.9, -0.2]])
        S = [np.eye(2) * 0.05] * 3
        f = mvmeta_fit(y, S, method="reml")
        preds = np.array([b["blup"] for b in blup(f)])

        # Encolhido em direcao a media, mas sem chegar la.
        assert np.abs(preds - y).sum() > 1e-6
        assert np.abs(preds - f.coef).sum() > 1e-6
        assert np.abs(preds - f.coef).sum() < np.abs(y - f.coef).sum()

    def test_covariancia_do_blup_e_psd(self):
        y, S, _ = estudos(4, 12, seed=41)
        f = mvmeta_fit(y, S, method="reml")

        for b in blup(f):
            assert np.linalg.eigvalsh(b["vcov"]).min() > -1e-9


# ---------------------------------------------------------------------------
# Validacao de entrada
# ---------------------------------------------------------------------------

class TestParidadeComR:
    """Compara numero a numero contra o mvmeta 1.0.3 do R.

    A referencia esta congelada em ``fixtures/mvmeta/referencia_r.json``,
    gerada rodando ``mvmeta(y, S=Slist, method="reml")`` sobre os mesmos
    sete casos de ``casos.json``. Fica no repositorio para a paridade
    continuar verificavel em maquina sem R instalado.

    O caso ``cb16`` (p=16, m=14, 136 parametros) tem a dimensao de um
    crossbasis 4x4, que e o que o ``sus_mod_pool()`` de fato agrupa. Ali a
    tolerancia e mais larga, e por um motivo que vale registrar: a
    log-verossimilhanca do Python fica 2,3e-04 ACIMA da do R, ou seja o
    otimo encontrado aqui e ligeiramente melhor -- a diferenca e o R
    parando cedo, nao erro deste codigo.
    """

    @staticmethod
    def _lista(v):
        """jsonlite serializa lista nomeada como objeto; iterar daria as chaves."""
        return list(v.values()) if isinstance(v, dict) else list(v)

    @staticmethod
    def _ajusta(caso):
        y = np.array(caso["y"], dtype=float)
        S = [np.array(s, dtype=float) for s in caso["S"]]
        X = (None if caso["x"] is None
             else np.column_stack([np.ones(caso["m"]), np.array(caso["x"], dtype=float)]))
        return mvmeta_fit(y, S, X=X, method="reml")

    @staticmethod
    def _rel(a, b):
        a = np.atleast_1d(np.asarray(a, dtype=float)).ravel()
        b = np.atleast_1d(np.asarray(b, dtype=float)).ravel()
        assert a.shape == b.shape, f"formas diferentes: {a.shape} vs {b.shape}"
        return float(np.abs(a - b).max() / max(1e-12, float(np.abs(b).max())))

    CASOS = ["uni_het", "biv_het", "tri_het", "quad_het", "cb16", "homogeneo", "metareg"]

    @pytest.mark.parametrize("nome", CASOS)
    def test_coeficientes_e_covariancia(self, referencia, nome):
        casos, ref = referencia
        caso, R = casos[nome], ref[nome]
        f = self._ajusta(caso)
        tol = 2e-3 if nome == "cb16" else 1e-4

        assert self._rel(f.coef, R["coef"]) < tol
        vcov_r = np.array(R["vcov"], dtype=float).reshape(f.vcov.shape, order="F")
        assert self._rel(f.vcov, vcov_r) < tol

    @pytest.mark.parametrize("nome", CASOS)
    def test_psi_e_verossimilhanca(self, referencia, nome):
        casos, ref = referencia
        caso, R = casos[nome], ref[nome]
        p = caso["p"]
        f = self._ajusta(caso)
        tol = 2e-3 if nome == "cb16" else 1e-4

        psi_r = np.array(R["psi"], dtype=float).reshape(p, p, order="F")
        assert self._rel(f.psi, psi_r) < tol
        # A verossimilhanca do Python nunca pode ficar ABAIXO da do R: se
        # ficar, o otimizador daqui parou antes do otimo.
        assert f.loglik >= float(R["logLik"]) - 1e-6

    @pytest.mark.parametrize("nome", CASOS)
    def test_heterogeneidade(self, referencia, nome):
        casos, ref = referencia
        caso, R = casos[nome], ref[nome]
        if "Q" not in R:
            pytest.skip("R nao reportou qtest neste caso")
        het = qtest(self._ajusta(caso))

        assert het["df"] == R["df"]
        assert self._rel(het["Q"], R["Q"]) < 1e-8

    @pytest.mark.parametrize("nome", CASOS)
    def test_blup_e_sua_covariancia(self, referencia, nome):
        casos, ref = referencia
        caso, R = casos[nome], ref[nome]
        if "blup" not in R:
            pytest.skip("R nao reportou blup neste caso")
        f = self._ajusta(caso)
        bs = blup(f)
        tol = 2e-3 if nome == "cb16" else 1e-4

        esperado = np.array([np.atleast_1d(v) for v in self._lista(R["blup"])], dtype=float)
        assert self._rel(np.array([b["blup"] for b in bs]), esperado) < tol

        if "blup_vcov" in R:
            # Formula do mvmeta: X V X' + Psi - Psi Sigma^-1 Psi. NAO e a
            # MSE de Henderson, que encolhe o primeiro termo -- medido, a
            # do mvmeta sai de 15% a 38% mais larga. Paridade vence aqui
            # porque estes sao os intervalos por cidade publicados.
            esperado_v = np.array(
                [np.atleast_1d(v) for v in self._lista(R["blup_vcov"])], dtype=float)
            obtido_v = np.array([b["vcov"].ravel(order="F") for b in bs])
            assert self._rel(obtido_v, esperado_v) < tol


class TestValidacao:
    def test_y_precisa_ser_bidimensional(self):
        with pytest.raises(ValueError, match="2-D"):
            mvmeta_fit(np.array([1.0, 2.0]), [np.eye(1)] * 2)

    def test_s_com_tamanho_diferente_de_y(self):
        with pytest.raises(ValueError, match="3 entries but y has 2"):
            mvmeta_fit(np.zeros((2, 1)), [np.eye(1)] * 3)

    def test_s_com_forma_errada(self):
        with pytest.raises(ValueError, match=r"S\[1\] has shape"):
            mvmeta_fit(np.zeros((2, 2)), [np.eye(2), np.eye(3)])

    def test_metodo_desconhecido(self):
        with pytest.raises(ValueError, match="reml"):
            mvmeta_fit(np.zeros((2, 1)), [np.eye(1)] * 2, method="bayes")

    def test_x_com_numero_de_linhas_incompativel(self):
        with pytest.raises(ValueError, match="rows but y has"):
            mvmeta_fit(np.zeros((3, 1)), [np.eye(1)] * 3, X=np.ones((2, 1)))
