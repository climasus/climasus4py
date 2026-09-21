"""sus_climate_compute_spei contra o climasus4r.

Fixture: 96 meses x 5 municipios, chuva com ciclo anual e ruido lognormal,
temperatura com ciclo invertido; PET por thornthwaite. A referencia e a
saida do climasus4r sobre a MESMA entrada.

Este arquivo nasceu da correcao do M43: o extremo umido do indice vinha
espelhado do extremo seco em vez de calculado.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy import stats

import climasus4py as cs
from climasus4py.enrichment.climate_spei import _spei_transform

FIX = Path(__file__).parent / "fixtures" / "spei"
ESCALAS = ("spei_1mo", "spei_3mo", "spei_6mo", "spei_12mo")

#: A referencia do R, escala por escala. Sao os numeros do M43.
REFERENCIA = {
    "spei_1mo":  {"n": 480, "media": -0.004248, "min": -2.561682, "max": 2.153875},
    "spei_3mo":  {"n": 470, "media": -0.004350, "min": -2.554361, "max": 2.145475},
    "spei_6mo":  {"n": 455, "media": -0.004512, "min": -2.543048, "max": 2.132485},
    "spei_12mo": {"n": 425, "media": -0.004872, "min": -2.519124, "max": 2.104968},
}


@pytest.fixture(scope="module")
def entrada() -> pd.DataFrame:
    return pd.read_parquet(FIX / "spei_in.parquet")


@pytest.fixture(scope="module")
def referencia_r() -> pd.DataFrame:
    return pd.read_parquet(FIX / "spei_r.parquet")


@pytest.fixture(scope="module")
def saida(entrada) -> pd.DataFrame:
    out = cs.sus_climate_compute_spei(
        entrada, rain_var="rainfall_chirps_mm", pet_method="thornthwaite",
        temp_var="tair_dry_bulb_c", verbose=False,
    )
    return out.df() if hasattr(out, "df") else out


def _alinhado(a: pd.DataFrame, b: pd.DataFrame):
    chave = ["code_muni", "date"]
    return (a.sort_values(chave).reset_index(drop=True),
            b.sort_values(chave).reset_index(drop=True))


class TestParidade:
    @pytest.mark.parametrize("escala", ESCALAS)
    def test_bate_com_o_r(self, saida, referencia_r, escala):
        a, b = _alinhado(saida, referencia_r)
        x, y = a[escala].to_numpy(float), b[escala].to_numpy(float)
        ambos = ~np.isnan(x) & ~np.isnan(y)

        assert (np.isnan(x) == np.isnan(y)).all()      # NA nas mesmas linhas
        assert np.abs(x[ambos] - y[ambos]).max() < 1e-12

    @pytest.mark.parametrize("escala", ESCALAS)
    def test_os_resumos_do_m43(self, saida, escala):
        """Os numeros que o achado registrou, escala por escala."""
        v = saida[escala].to_numpy(float)
        v = v[~np.isnan(v)]
        esp = REFERENCIA[escala]

        assert len(v) == esp["n"]
        assert v.mean() == pytest.approx(esp["media"], abs=1e-6)
        assert v.min() == pytest.approx(esp["min"], abs=1e-6)
        assert v.max() == pytest.approx(esp["max"], abs=1e-6)

    def test_a_janela_encurta_a_serie(self, saida):
        """Cada escala perde (escala - 1) meses por municipio."""
        n = {c: int(saida[c].notna().sum()) for c in ESCALAS}

        assert n["spei_1mo"] == 480
        assert n["spei_3mo"] == 480 - 5 * 2
        assert n["spei_6mo"] == 480 - 5 * 5
        assert n["spei_12mo"] == 480 - 5 * 11


class TestExtremoUmidoNaoEspelhado:
    """O extremo umido era espelhado do seco (M43).

    O `_spei_transform` diz replicar
    `findInterval(x, sorted_calib, rightmost.closed = TRUE)`, mas
    `searchsorted(side="right")` sozinho NAO e essa funcao: elas divergem
    exatamente no ponto para o qual a flag existe. Quando x e igual ao
    maior valor de calibracao, o `rightmost.closed = TRUE` trata o ultimo
    intervalo como fechado e devolve n-1; o searchsorted devolve n.

    Com rank n, a probabilidade de Hazen do maximo vira (n-0.5)/n, que e o
    espelho exato de 0.5/n -- a do minimo. Resultado: o mes mais chuvoso de
    CADA municipio recebia o mesmo numero, -(minimo global). Numa serie de
    96 meses isso e 2.561682, contra 2.153875 do R: erro de 0.41 no indice.

    A assinatura do defeito era a media sair exatamente 0.000000 e a
    amplitude ficar perfeitamente simetrica. So o extremo superior era
    afetado; o lado seco, que e para o que um indice de seca se le, batia
    desde sempre.
    """

    N = 96

    def test_a_formula_de_hazen_nos_dois_ranks(self):
        """A aritmetica que separa 2.561682 de 2.153875."""
        espelhado = stats.norm.ppf((self.N - 0.5) / self.N)
        correto = stats.norm.ppf((self.N - 1 - 0.5) / self.N)

        assert espelhado == pytest.approx(2.561682, abs=1e-6)
        assert correto == pytest.approx(2.153875, abs=1e-6)
        assert espelhado == pytest.approx(-stats.norm.ppf(0.5 / self.N), abs=1e-12)

    def test_o_maximo_recebe_rank_n_menos_um(self):
        """O comportamento do rightmost.closed, no ponto onde ele atua."""
        calib = np.arange(1.0, self.N + 1)
        soh_o_maximo = _spei_transform(np.array([float(self.N)]), calib)[0]

        assert soh_o_maximo == pytest.approx(2.153875, abs=1e-6)

    def test_acima_do_maximo_segue_com_rank_n(self):
        """A flag atua so em x == vec[n], nao em x > vec[n]. Conferido no R."""
        calib = np.arange(1.0, self.N + 1)
        acima = _spei_transform(np.array([self.N + 5.0]), calib)[0]

        assert acima == pytest.approx(2.561682, abs=1e-6)

    def test_empate_no_maximo(self):
        """Com o maior valor repetido, o R ainda devolve n-1. vec=(1,2,3,4,4)."""
        calib = np.array([1.0, 2.0, 3.0, 4.0, 4.0])
        n = len(calib)
        got = _spei_transform(np.array([4.0]), calib)[0]

        assert got == pytest.approx(stats.norm.ppf((n - 1 - 0.5) / n), abs=1e-12)

    def test_a_media_nao_e_mais_exatamente_zero(self, saida):
        """A simetria perfeita era o sintoma, nao uma propriedade do indice."""
        for c in ESCALAS:
            v = saida[c].to_numpy(float)
            v = v[~np.isnan(v)]

            assert abs(v.mean()) > 1e-9, c
            assert abs(v.min()) != pytest.approx(v.max(), abs=1e-9), c

    def test_os_cinco_maximos_compartilham_o_valor_certo(self, saida):
        """Os cinco maximos recebem UM MESMO numero -- e isso esta correto.

        Vale registrar porque o achado original leu o compartilhamento
        como sintoma ("todas recebem o MESMO valor no Python"). Nao e: a
        posicao de plotagem depende so do rank e de n, e com 96 meses por
        municipio todo maximo cai em (n-1-0.5)/n. O R faz igual. O que
        estava errado era o VALOR compartilhado -- 2.561682 em vez de
        2.153875 -- nao o fato de ser compartilhado.
        """
        maximos = saida.groupby("code_muni")["spei_1mo"].max().to_numpy()

        assert len(maximos) == 5
        assert len(np.unique(np.round(maximos, 9))) == 1
        assert maximos[0] == pytest.approx(2.153875, abs=1e-6)
        assert maximos[0] != pytest.approx(2.561682, abs=1e-4)

    def test_o_lado_seco_ja_batia(self, saida, referencia_r):
        """Zero divergencias no lado negativo -- o que o M43 ja media."""
        a, b = _alinhado(saida, referencia_r)
        for c in ESCALAS:
            x, y = a[c].to_numpy(float), b[c].to_numpy(float)
            seco = (~np.isnan(x)) & (~np.isnan(y)) & (y < 0)

            assert seco.sum() > 200, c
            assert np.abs(x[seco] - y[seco]).max() < 1e-12, c


class TestTransformIsolado:
    def test_serie_curta_devolve_nan(self):
        assert np.isnan(_spei_transform(np.array([1.0]), np.array([1.0, 2.0]))).all()

    def test_calibracao_com_na(self):
        calib = np.array([1.0, 2.0, np.nan, 3.0, 4.0, 5.0])
        got = _spei_transform(np.array([3.0]), calib)

        assert not np.isnan(got[0])

    def test_entrada_toda_na(self):
        got = _spei_transform(np.full(3, np.nan), np.arange(1.0, 11))

        assert np.isnan(got).all()

    def test_monotonico(self):
        calib = np.arange(1.0, 51)
        got = _spei_transform(calib.copy(), calib)
        finito = got[~np.isnan(got)]

        assert (np.diff(finito) >= -1e-12).all()
