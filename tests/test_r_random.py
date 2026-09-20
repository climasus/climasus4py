"""O RNG do R reimplementado: exigencia de paridade bit a bit.

Por que este modulo existe: o climasus4r sorteia o Monte Carlo do
`compute_uncertainty` sob `set.seed(2024L)` fixo, entao os intervalos de
confianca dele sao deterministicos -- e reproduzi-los exige reproduzir o
fluxo do R. O NumPy nao serve: semeia o MT19937 de outro jeito e
transforma uniforme em normal por outro metodo. `RandomState(2024)` da
uma sequencia sem relacao nenhuma.

A referencia esta congelada em tests/fixtures/r_random/referencia_r.parquet,
gerada no R 4.6.0 (Mersenne-Twister, Inversion): 4.200 valores em quatro
combinacoes de tipo e semente.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from climasus4py.utils.r_random import RRandom, qnorm

FIX = Path(__file__).parent / "fixtures" / "r_random" / "referencia_r.parquet"


@pytest.fixture(scope="module")
def ref() -> pd.DataFrame:
    return pd.read_parquet(FIX)


def _ref(df: pd.DataFrame, kind: str, seed: int) -> np.ndarray:
    sub = df[(df["kind"] == kind) & (df["seed"] == seed)].sort_values("i")
    return sub["value"].to_numpy()


class TestParidadeComR:
    """Bit a bit, nao 'proximo'.

    Arredondar o quantil a 2 casas perdoaria um ULP, mas a disciplina do
    porte e exatidao e um fluxo de RNG que divirja no bit acaba divergindo
    no valor: um empate de arredondamento em qualquer uma das 200
    simulacoes muda o quantil.
    """

    @pytest.mark.parametrize("seed,n", [(2024, 2000), (7, 700)])
    def test_runif(self, ref, seed, n):
        esperado = _ref(ref, "unif", seed)
        obtido = RRandom(seed).runif(n)
        assert np.array_equal(obtido, esperado), (
            f"maior diferenca: {np.abs(obtido - esperado).max():.3e}"
        )

    @pytest.mark.parametrize("seed,n", [(2024, 1000), (99, 500)])
    def test_rnorm(self, ref, seed, n):
        esperado = _ref(ref, "norm", seed)
        obtido = RRandom(seed).rnorm(n)
        assert np.array_equal(obtido, esperado), (
            f"maior diferenca: {np.abs(obtido - esperado).max():.3e}"
        )

    def test_atravessa_a_fronteira_do_bloco(self, ref):
        """O bloco do MT tem 624 palavras, e o twist e auto-referente.

        A primeira versao vetorizada deste modulo lia o feed `mt[i-227]` do
        estado ANTERIOR, quando o laco do C ja o havia reescrito para
        i >= 454. O sintoma foi exatamente este: as 622 primeiras palavras
        certas, a 623a errada, e tudo depois do sorteio 1246 errado. Sem
        atravessar a fronteira o defeito passa despercebido, e por isso este
        teste olha as posicoes exatas.
        """
        esperado = _ref(ref, "unif", 2024)
        obtido = RRandom(2024).runif(2000)
        for pos in (623, 624, 625, 1247, 1248, 1873, 2000):
            assert obtido[pos - 1] == esperado[pos - 1], f"posicao {pos}"

    def test_semeadura_do_numpy_nao_serve(self):
        """Fixa o motivo de o modulo existir, para ninguem 'simplificar'."""
        r = RRandom(2024).runif(5)
        np_ = np.random.RandomState(2024).random_sample(5)
        assert not np.allclose(r, np_), (
            "se estes coincidirem, o RandomState do NumPy passou a semear "
            "como o R e este modulo virou redundante -- conferir antes de "
            "apagar nada"
        )

    def test_duas_instancias_com_a_mesma_semente_coincidem(self):
        assert np.array_equal(RRandom(1).runif(50), RRandom(1).runif(50))

    def test_estado_avanca_entre_chamadas(self):
        rng = RRandom(2024)
        a, b = rng.runif(4), rng.runif(4)
        assert not np.array_equal(a, b)
        # e a concatenacao tem de bater com um sorteio unico de 8
        assert np.array_equal(np.concatenate([a, b]), RRandom(2024).runif(8))

    def test_media_e_desvio(self):
        """rnorm(n, mean, sd) escala como o do R."""
        base = RRandom(5).rnorm(100)
        escalado = RRandom(5).rnorm(100, mean=10.0, sd=0.5)
        assert np.array_equal(escalado, 10.0 + 0.5 * base)


class TestQnorm:
    """O qnorm do R e AS 241; o ndtri do scipy difere no ultimo bit.

    Medido contra o R em 35 probabilidades de 1e-10 a 1-1e-10: AS 241
    acertou as 35 exatamente, o ndtri errou por ate 1,8e-15. Parece pouco,
    mas cada normal do R sai de uma inversao, entao o desvio entra em
    TODO sorteio.
    """

    @pytest.mark.parametrize("p,esperado", [
        (0.5, 0.0),
        (0.975, 1.959963984540054),
        (0.025, -1.959963984540054),
        (0.995, 2.5758293035489004),
    ])
    def test_valores_conhecidos(self, p, esperado):
        assert qnorm(p) == pytest.approx(esperado, abs=1e-15)

    def test_simetria(self):
        for p in (0.01, 0.1, 0.3, 0.42, 0.45, 0.499):
            assert qnorm(p) == pytest.approx(-qnorm(1 - p), abs=1e-15)

    def test_bordas(self):
        assert qnorm(0.0) == -math.inf
        assert qnorm(1.0) == math.inf
        assert math.isnan(qnorm(math.nan))

    def test_monotono_atravessando_os_tres_ramos(self):
        # os ramos trocam em |p-0.5|=0.425 e em sqrt(-log(min(p,1-p)))=5
        ps = [1e-9, 1e-7, 1e-6, 1e-4, 0.001, 0.05, 0.074, 0.076, 0.3,
              0.425, 0.5, 0.575, 0.7, 0.926, 0.999, 1 - 1e-6, 1 - 1e-9]
        vals = [qnorm(p) for p in ps]
        assert all(a < b for a, b in zip(vals, vals[1:])), vals
