"""Paridade dos seis indicadores novos contra o climasus4r.

Graus-dia (cdd/hdd/gdd), classe de umidade de Koppen e as duas sensacoes
termicas de vento (wcet/wct). A referencia esta congelada em
fixtures/indicators/referencia_r.parquet, gerada chamando os helpers
internos do R (.compute_cdd, .compute_wcet, ...) sobre
fixtures/indicators/entrada.parquet -- 4000 linhas com T de -25 a 45 C,
RH de 5 a 100%, vento de 0 a 18 m/s e 40 nulos em cada variavel de
entrada, de proposito.

Dois achados ficam fixados aqui, e nenhum dos dois e diferenca de
arredondamento:

- O DuckDB e o R discordam sobre NULL em GREATEST/LEAST. Sem guard
  explicito, temperatura ausente virava ZERO graus-dia e vento ausente
  virava uma calma de 0,01 m/s com sensacao termica calculada. As duas
  coisas sao observacao fabricada, nao valor faltante.
- O wct_c do R converte o vento com o fator km/h -> mph aplicado a uma
  coluna em m/s. Prova-se pela saida do proprio R: wcet_c e wct_c sao a
  mesma grandeza e diferem 4,6 C. Ver M69.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate_indicators import (
    _MPH_PER_KMH,
    _MPH_PER_MS,
    _wct_correct_units,
)

FIXTURES = Path(__file__).parent / "fixtures" / "indicators"
NOVOS = ["cdd", "hdd", "gdd", "koppen_humidity", "wcet", "wct"]
NUMERICOS = ["cdd_c", "hdd_c", "gdd_c", "wcet_c", "wct_c"]


@pytest.fixture(scope="module")
def entrada() -> pd.DataFrame:
    return pd.read_parquet(FIXTURES / "entrada.parquet")


@pytest.fixture(scope="module")
def referencia_r() -> pd.DataFrame:
    return pd.read_parquet(FIXTURES / "referencia_r.parquet")


@pytest.fixture(scope="module")
def saida_wbgt(entrada) -> pd.DataFrame:
    """As duas colunas de WBGT lado a lado -- a evidencia do M71."""
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=["wbgt", "wbgt_stull"], station_col="station_code",
        date_col="datetime", verbose=False,
    ).df()


@pytest.fixture(scope="module")
def saida_py(entrada) -> pd.DataFrame:
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=NOVOS, station_col="station_code",
        date_col="datetime", verbose=False,
    ).df()


# ---------------------------------------------------------------------------
# Paridade numerica
# ---------------------------------------------------------------------------

class TestParidadeComR:
    @pytest.mark.parametrize("coluna", NUMERICOS)
    def test_valores_identicos(self, saida_py, referencia_r, coluna):
        py = saida_py[coluna].astype(float)
        r = referencia_r[coluna].astype(float)
        ambos = py.notna() & r.notna()

        assert ambos.sum() > 0
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    @pytest.mark.parametrize("coluna", NUMERICOS)
    def test_os_nulos_caem_nas_mesmas_linhas(self, saida_py, referencia_r, coluna):
        """Valor igual nao basta: o padrao de ausencia tem de coincidir.

        Foi aqui que os dois defeitos do DuckDB apareceram -- os valores
        batiam e os nulos nao.
        """
        py = saida_py[coluna].astype(float).isna()
        r = referencia_r[coluna].astype(float).isna()

        assert (py != r).sum() == 0

    def test_todas_as_seis_colunas_saem(self, saida_py):
        esperadas = {*NUMERICOS, "koppen_humidity"}
        assert esperadas <= set(saida_py.columns)


# ---------------------------------------------------------------------------
# A semantica de NULL do DuckDB
# ---------------------------------------------------------------------------

class TestNulosDoDuckDB:
    """GREATEST/LEAST do DuckDB IGNORAM NULL; o pmax/pmin do R propaga.

    Sem guard explicito cada um destes casos produziria um numero onde
    nao ha observacao. Os testes conferem a premissa E a consequencia,
    porque se um dia o DuckDB mudar de comportamento o guard fica
    redundante e vale saber.
    """

    def test_a_premissa_greatest_ignora_null(self):
        conn = get_connection()
        assert conn.sql("SELECT GREATEST(NULL::DOUBLE - 18.0, 0.0)").fetchone()[0] == 0.0
        assert conn.sql("SELECT LEAST(NULL::DOUBLE, 30.0)").fetchone()[0] == 30.0
        assert conn.sql("SELECT GREATEST(NULL::DOUBLE * 3.6, 0.01)").fetchone()[0] == 0.01

    @pytest.mark.parametrize("coluna", ["cdd_c", "hdd_c", "gdd_c"])
    def test_temperatura_ausente_nao_vira_zero_grau_dia(self, coluna):
        """Zero graus-dia le como dia ameno, nao como dia sem medicao."""
        df = pd.DataFrame({
            "station_code": ["A"] * 3,
            "datetime": pd.date_range("2020-01-01", periods=3, freq="h"),
            "tair_dry_bulb_c": [25.0, None, 12.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["cdd", "hdd", "gdd"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()

        assert pd.isna(out[coluna].iloc[1])
        assert out[coluna].notna().sum() == 2

    def test_vento_ausente_nao_vira_calma(self):
        """Vento nulo com T no dominio produzia sensacao termica inventada.

        Medido antes do guard: 20 valores fabricados em 4000 linhas.
        """
        df = pd.DataFrame({
            "station_code": ["A"] * 3,
            "datetime": pd.date_range("2020-01-01", periods=3, freq="h"),
            "tair_dry_bulb_c": [5.0, 5.0, 5.0],
            "ws_2_m_s": [4.0, None, 4.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wcet", "wct"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()

        assert pd.isna(out["wcet_c"].iloc[1])
        assert pd.isna(out["wct_c"].iloc[1])
        assert out["wcet_c"].notna().sum() == 2


# ---------------------------------------------------------------------------
# Dominio de validade
# ---------------------------------------------------------------------------

class TestDominioDeValidade:
    """A sensacao termica de vento so existe com T <= 10 C e vento > 1,3 m/s.

    Fora disso a regressao nao significa nada, e tanto o R quanto o Python
    devolvem NULL em vez de extrapolar.
    """

    @pytest.mark.parametrize(("t", "ws", "vale"), [
        (5.0, 4.0, True),
        (10.0, 4.0, True),
        (10.1, 4.0, False),
        (30.0, 4.0, False),
        (5.0, 1.3, False),
        (5.0, 1.31, True),
        (5.0, 0.0, False),
    ])
    def test_mascara(self, t, ws, vale):
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [t], "ws_2_m_s": [ws],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wcet"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()

        assert out["wcet_c"].notna().iloc[0] is np.True_ if vale else True
        assert pd.isna(out["wcet_c"].iloc[0]) != vale


# ---------------------------------------------------------------------------
# O erro de unidade do R (M69)
# ---------------------------------------------------------------------------

class TestErroDeUnidadeNoWct:
    """wcet_c e wct_c sao a MESMA grandeza em unidades diferentes.

    Environment Canada usa km/h, o NWS usa mph, e as duas regressoes
    concordam entre si a menos de ~0,03 C. O R converte o vento com
    `ws * 0.621371` -- o fator km/h -> mph -- sobre uma coluna em m/s,
    onde o correto e 2,23694. Nao precisa de referencia externa: a
    inconsistencia esta na saida do proprio R.
    """

    def test_os_dois_fatores_sao_o_que_eu_digo_que_sao(self):
        assert _MPH_PER_KMH == pytest.approx(0.621371, abs=1e-6)
        assert _MPH_PER_MS == pytest.approx(2.2369362920544, abs=1e-9)
        # E um confunde o outro pelo fator de 3,6 -- nao exato porque o
        # 0.621371 que o R escreve ja e arredondado (a razao da 3,6000011).
        assert _MPH_PER_MS / _MPH_PER_KMH == pytest.approx(3.6, abs=1e-5)

    def test_a_saida_do_r_e_internamente_inconsistente(self, referencia_r):
        ambos = referencia_r["wcet_c"].notna() & referencia_r["wct_c"].notna()
        gap = (referencia_r.loc[ambos, "wct_c"] - referencia_r.loc[ambos, "wcet_c"])

        assert ambos.sum() > 1000
        assert gap.mean() > 4.0          # medido: +4,60 C
        assert gap.min() > 1.0           # sempre na mesma direcao: mais quente

    def test_a_formula_correta_concorda_com_o_wcet(self, saida_py):
        """E a prova: com m/s -> mph correto, as duas convergem."""
        ambos = saida_py["wcet_c"].notna() & saida_py["wct_c"].notna()
        t = saida_py.loc[ambos, "tair_dry_bulb_c"].to_numpy(dtype=float)
        ws = saida_py.loc[ambos, "ws_2_m_s"].to_numpy(dtype=float)
        wcet = saida_py.loc[ambos, "wcet_c"].to_numpy(dtype=float)

        correto = _wct_correct_units(t, ws)
        como_o_r = saida_py.loc[ambos, "wct_c"].to_numpy(dtype=float)

        assert np.abs(correto - wcet).max() < 0.1      # medido: 0,04
        assert np.abs(como_o_r - wcet).mean() > 4.0    # medido: 4,60

    def test_o_erro_subestima_o_frio(self, saida_py):
        """A direcao importa: subestimar vento subestima a sensacao de frio.

        Para analise de onda de frio e o lado perigoso -- reporta menos
        risco do que existe.
        """
        ambos = saida_py["wcet_c"].notna() & saida_py["wct_c"].notna()
        assert (saida_py.loc[ambos, "wct_c"] > saida_py.loc[ambos, "wcet_c"]).all()


# ---------------------------------------------------------------------------
# Koppen
# ---------------------------------------------------------------------------

class TestKoppen:
    def test_as_quatro_faixas(self):
        df = pd.DataFrame({
            "station_code": ["A"] * 6,
            "datetime": pd.date_range("2020-01-01", periods=6, freq="h"),
            "rh_mean_porc": [10.0, 29.9, 30.0, 49.9, 69.9, 70.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["koppen_humidity"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()

        assert list(out["koppen_humidity"]) == [
            "Arid", "Arid", "Semi-arid", "Semi-arid", "Humid", "Perhumid",
        ]

    def test_concorda_com_o_r_onde_a_umidade_existe(self, entrada, saida_py, referencia_r):
        presente = entrada["rh_mean_porc"].notna()
        assert (saida_py.loc[presente, "koppen_humidity"].to_numpy()
                == referencia_r.loc[presente, "koppen_humidity"].to_numpy()).all()

    def test_umidade_ausente_e_classificada_como_perhumid_nos_dois(
        self, entrada, saida_py, referencia_r
    ):
        """PARIDADE COM UM DEFEITO DO R, POR DECISAO (M70).

        O case_when do R termina em `TRUE ~ "Perhumid"`, e esse ramo
        captura tambem o NA: umidade ausente e rotulada com a faixa MAIS
        UMIDA das quatro. Uma contagem por classe soma as ausencias ali,
        sem nada sinalizar.

        O Python devolvia NULL, que e o comportamento defensavel, e passou
        a acompanhar o R por decisao do Andrey em 14/09/2026 -- para a
        apresentacao ao coordenador ficar consistente, com o tratamento
        definitivo a decidir depois.

        Este teste existe para que a escolha continue VISIVEL: se alguem
        no futuro fizer o Python devolver NULL aqui, o teste quebra e a
        pessoa le por que o comportamento era esse.
        """
        ausente = entrada["rh_mean_porc"].isna()

        assert ausente.sum() == 40
        assert (referencia_r.loc[ausente, "koppen_humidity"] == "Perhumid").all()
        assert (saida_py.loc[ausente, "koppen_humidity"] == "Perhumid").all()


# ---------------------------------------------------------------------------
# WBGT (M71)
# ---------------------------------------------------------------------------

class TestWbgt:
    """`wbgt_c` replica o R; `wbgt_stull_c` e a alternativa validada.

    As duas saem na mesma tabela de proposito: a divergencia entre elas e
    a evidencia do M71 em forma de dado, nao de afirmacao.
    """

    def test_wbgt_c_reproduz_o_r_exatamente(self, saida_wbgt, referencia_r):
        py = saida_wbgt["wbgt_c"].astype(float)
        r = referencia_r["wbgt_c"].astype(float)
        ambos = py.notna() & r.notna()

        assert (py.isna() != r.isna()).sum() == 0
        assert ambos.sum() > 3000
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    def test_so_temperatura_e_umidade_propagam_nulo(self, entrada, saida_wbgt):
        """O dispatcher do R zera radiacao e vento ausentes antes de calcular.

        Entao o wbgt_c sai preenchido mesmo sem radiacao nem vento; so T e
        RH ausentes anulam. Replicado, e conferido aqui porque e a parte
        que um port descuidado erraria.
        """
        sem_t_ou_rh = entrada["tair_dry_bulb_c"].isna() | entrada["rh_mean_porc"].isna()
        assert saida_wbgt["wbgt_c"].isna().to_numpy().tolist() == sem_t_ou_rh.to_numpy().tolist()

    def test_stull_reproduz_a_tabela_psicrometrica(self):
        """O ponto fixo que nao depende de distribuicao nenhuma.

        A T=30 C e RH=60% o bulbo umido psicrometrico e ~23,9 C. O
        wbgt_stull_c e 0.67*Twb + 0.33*T, logo Twb = (w - 0.33*30)/0.67.
        """
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [30.0], "rh_mean_porc": [60.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wbgt_stull"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()
        twb = (float(out["wbgt_stull_c"].iloc[0]) - 0.33 * 30.0) / 0.67

        assert twb == pytest.approx(23.9, abs=0.3)

    def test_o_bulbo_umido_do_r_fica_5_graus_abaixo_da_tabela(self):
        """O mesmo ponto fixo, pelo lado do R -- reconstruindo o tnw dele.

        Nao e diferenca de arredondamento: sao 5 C num bulbo umido, e a
        causa aparente e o tnw1 receber pressao de vapor em kPa (~2,5)
        onde a forma de Stull espera RH em porcentagem (~60).
        """
        t, rh = 30.0, 60.0
        es = 0.6108 * np.exp(17.27 * t / (t + 237.3))
        e_a = (rh / 100.0) * es
        tnw1 = t * np.arctan(0.16 * np.sqrt(max(e_a, 0.01) + 0.1)) + 3.0
        tnw2 = t + 0.33 * (rh / 100.0) * np.exp(0.0514 * t) - 4.0
        tnw = (tnw1 + tnw2) / 2.0

        assert e_a == pytest.approx(2.55, abs=0.05)   # kPa, nao porcentagem
        assert tnw == pytest.approx(18.78, abs=0.05)
        assert abs(tnw - 23.9) > 4.5

    def test_o_termo_de_globo_do_r_quase_nao_responde_ao_sol(self, entrada):
        """Um WBGT externo existe para capturar carga solar.

        O termo de globo do R varia menos de 1 C enquanto a radiacao vai
        de zero a sol pleno. Na literatura um globo em 1000 W/m2 passa de
        45 C.
        """
        def tg(sr_kj, ws=1.5, t=30.0):
            return t + 0.0144 * max(sr_kj / 3.6, 0.0) ** 0.6 / max(ws, 0.1) ** 0.2 - 2.0

        assert tg(0) == pytest.approx(28.00, abs=0.05)
        assert tg(3600) == pytest.approx(28.84, abs=0.05)
        assert tg(3600) - tg(0) < 1.0

    def test_as_duas_colunas_divergem_nos_dois_sentidos(self, saida_wbgt):
        """Nao e viés constante -- e o motivo de um numero medio enganar.

        Uma medicao minha anterior dizia 'media 3,35 C', amostrada com
        vento de 0,2 a 4 m/s. Aqui o vento vai a 18 e o termo de globo do
        R e dividido por ws^0.2, entao a media cai para -0,36 C enquanto a
        faixa vai de -8 a +8. O que sobrevive a qualquer distribuicao e a
        consequencia no limiar, no teste seguinte.
        """
        m = saida_wbgt["wbgt_c"].notna() & saida_wbgt["wbgt_stull_c"].notna()
        gap = saida_wbgt.loc[m, "wbgt_stull_c"] - saida_wbgt.loc[m, "wbgt_c"]

        assert gap.min() < -5.0
        assert gap.max() > 5.0

    def test_o_limiar_iso_7243_marca_mais_que_o_dobro(self, saida_wbgt):
        """A consequencia que nao depende de distribuicao.

        31 C e o limiar de calor extremo do ISO 7243, e sao os limiares do
        WBGT EXTERNO que o R aplica (31/28/25).
        """
        m = saida_wbgt["wbgt_c"].notna() & saida_wbgt["wbgt_stull_c"].notna()
        como_o_r = int((saida_wbgt.loc[m, "wbgt_c"] > 31.0).sum())
        validado = int((saida_wbgt.loc[m, "wbgt_stull_c"] > 31.0).sum())

        assert como_o_r == 207
        assert validado == 449
        assert validado > 2 * como_o_r

    def test_stull_funciona_sem_radiacao_nem_vento(self):
        """Consequencia pratica da paridade: o wbgt_c agora exige 4 insumos.

        O wbgt_stull_c exige so T e RH, entao e o que ainda serve em dado
        sem radiacao solar -- que e a maioria das series curtas do INMET.
        """
        df = pd.DataFrame({
            "station_code": ["A", "A"],
            "datetime": pd.date_range("2020-01-01", periods=2, freq="h"),
            "tair_dry_bulb_c": [30.0, 25.0], "rh_mean_porc": [60.0, 80.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wbgt_stull"],
            station_col="station_code", date_col="datetime", verbose=False,
        ).df()

        assert out["wbgt_stull_c"].notna().all()
