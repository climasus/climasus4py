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
    ALL_INDICATORS,
    CORRECTED_INDICATORS,
    _INDICATOR_DEFS,
    _MPH_PER_KMH,
    _MPH_PER_MS,
    _wct_correct_units,
    flag_threshold,
    has_flags,
    resolve_indicator,
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


# ---------------------------------------------------------------------------
# Flags de confianca (M72)
# ---------------------------------------------------------------------------

INDS_DA_FIXTURE = [
    "wbgt", "wbgt_stull", "hi", "thi", "wcet", "wct",
    "cdd", "hdd", "gdd", "vapor_pressure", "koppen_humidity",
]


@pytest.fixture(scope="module")
def com_flags(entrada) -> pd.DataFrame:
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=INDS_DA_FIXTURE, station_col="station_code",
        date_col="datetime", verbose=False,
    ).df()


class TestFlagsEstrutura:
    """Tres booleanos por indicador que declare limiar, como no R.

    O R guarda com ``length(reg$thresholds) > 0``, entao indicador sem
    limiar nao ganha coluna nenhuma -- e nao tres colunas FALSE.
    """

    def test_saem_por_padrao(self, com_flags):
        """R usa region != "none" com region="auto", logo TRUE por padrao."""
        assert any("_flag_" in c for c in com_flags.columns)

    def test_podem_ser_desligadas(self, entrada):
        rel = get_connection().from_df(entrada)
        sem = cs.sus_climate_compute_indicators(
            rel, indicators=INDS_DA_FIXTURE, station_col="station_code",
            date_col="datetime", confidence_flags=False, verbose=False,
        ).df()
        assert not any("_flag_" in c for c in sem.columns)

    def test_tres_colunas_por_indicador_com_limiar(self, com_flags):
        com_limiar = [i for i in INDS_DA_FIXTURE if has_flags(resolve_indicator(i))]
        flags = [c for c in com_flags.columns if "_flag_" in c]

        assert len(flags) == 3 * len(com_limiar)

    @pytest.mark.parametrize("ind", ["cdd", "hdd", "gdd", "koppen_humidity"])
    def test_indicador_sem_limiar_nao_ganha_flag(self, com_flags, ind):
        out_col = _INDICATOR_DEFS[ind][0]

        assert not has_flags(ind)
        assert not [c for c in com_flags.columns if c.startswith(out_col + "_flag_")]

    def test_a_flag_nunca_e_nula(self, com_flags):
        """R faz !is.na(vals) & vals > thr, logo ausencia vira FALSE, nao NA.

        Importa porque uma flag nula se propagaria em qualquer filtro ou
        soma a jusante.
        """
        for c in [c for c in com_flags.columns if "_flag_" in c]:
            assert com_flags[c].notna().all()
            assert com_flags[c].dtype == bool

    def test_onde_o_indicador_e_nulo_a_flag_e_false(self, com_flags):
        nulo = com_flags["wcet_c"].isna()

        assert nulo.sum() > 0
        assert not com_flags.loc[nulo, "wcet_c_flag_extreme"].any()


class TestFlagsLimiares:
    """Qual limiar declarado alimenta cada flag -- a cadeia de prioridade do R.

    Conferido contra a saida real do ``.add_confidence_flags`` numa grade
    de -40 a 60, nao contra a minha leitura do codigo dele.
    """

    @pytest.mark.parametrize(("ind", "flag", "esperado"), [
        ("wbgt", "extreme", 31.0), ("wbgt", "high", 28.0), ("wbgt", "low", 15.0),
        ("heat_index", "extreme", 54.0), ("heat_index", "high", 41.0),
        ("thi", "high", 28.0),
        ("wcet", "extreme", -35.0), ("wct", "extreme", -35.0),
    ])
    def test_limiar_que_de_fato_alimenta_a_flag(self, ind, flag, esperado):
        assert flag_threshold(ind, flag) == pytest.approx(esperado)

    @pytest.mark.parametrize(("ind", "flag"), [
        ("heat_index", "low"), ("thi", "extreme"), ("thi", "low"),
        ("wcet", "high"), ("wcet", "low"), ("wct", "high"), ("wct", "low"),
        ("diurnal_range", "extreme"), ("diurnal_range", "high"),
        ("diurnal_range", "low"),
        ("vapor_pressure", "extreme"), ("vapor_pressure", "high"),
        ("vapor_pressure", "low"),
    ])
    def test_nomes_que_a_cadeia_nunca_le(self, ind, flag):
        """16 das 30 colunas de flag do R sao constante FALSE.

        Nao por falta de limiar declarado, mas porque o NOME declarado nao
        esta na cadeia de prioridade. O diurnal_range declara
        high/moderate/low e a cadeia procura high_stress/warning_low; o pet
        declara slight_cold onde a cadeia quer slight_cold_stress -- quase
        acerto que custa duas colunas.
        """
        assert flag_threshold(ind, flag) is None

    def test_as_colunas_constantes_saem_mesmo_assim(self, com_flags):
        """O R emite a coluna cheia de FALSE em vez de omiti-la."""
        for c in ("vapor_pressure_kpa_flag_extreme",
                  "vapor_pressure_kpa_flag_high",
                  "vapor_pressure_kpa_flag_low",
                  "thi_c_flag_extreme", "thi_c_flag_low"):
            assert c in com_flags.columns
            assert not com_flags[c].any()

    def test_a_conta_das_colunas_que_nao_disparam(self):
        """Dos 7 indicadores com limiar ja portados, 13 das 21 flags sao mortas.

        Por indicador: wbgt 0, heat_index 1 (low), thi 2 (extreme e low),
        wcet 2 (high e low), wct 2, diurnal_range 3, vapor_pressure 3.

        No conjunto completo do R -- que inclui et, utci e pet, ainda nao
        portados -- sao 16 de 30.
        """
        portados = ["wbgt", "heat_index", "thi", "wcet", "wct",
                    "diurnal_range", "vapor_pressure"]
        por_indicador = {
            i: sum(1 for f in ("extreme", "high", "low")
                   if flag_threshold(i, f) is None)
            for i in portados
        }

        assert por_indicador == {
            "wbgt": 0, "heat_index": 1, "thi": 2, "wcet": 2, "wct": 2,
            "diurnal_range": 3, "vapor_pressure": 3,
        }
        assert sum(por_indicador.values()) == 13
        assert all(has_flags(i) for i in portados)

    def test_a_conta_completa_do_r(self):
        """16 das 30 colunas do conjunto completo do R nunca disparam."""
        todos_do_r = ["wbgt", "heat_index", "thi", "wcet", "wct", "et",
                      "utci", "pet", "diurnal_range", "vapor_pressure"]
        mortas = sum(1 for i in todos_do_r for f in ("extreme", "high", "low")
                     if flag_threshold(i, f) is None)

        assert len(todos_do_r) * 3 == 30
        assert mortas == 16


class TestFlagInvertidaNoFrio:
    """A flag extreme de wcet/wct marca o lado AMENO (M72).

    Os dois declaram high_risk = -35, que cai na cadeia do extreme, e essa
    flag dispara em valor > limiar. Para sensacao termica de frio, mais
    frio e pior -- entao a flag e TRUE em quase todo lugar e FALSE
    exatamente nos casos perigosos.
    """

    def test_o_high_risk_cai_na_cadeia_do_extreme(self):
        assert flag_threshold("wcet", "extreme") == -35.0
        assert flag_threshold("wcet", "high") is None

    def test_dispara_no_ameno_e_nao_no_perigoso(self, com_flags):
        com_valor = com_flags[com_flags["wcet_c"].notna()]
        mais_frio = com_valor.loc[com_valor["wcet_c"].idxmin()]

        assert mais_frio["wcet_c"] < -40          # risco de vida
        assert not bool(mais_frio["wcet_c_flag_extreme"])
        # E TRUE na maioria esmagadora, que e o outro sintoma.
        assert com_valor["wcet_c_flag_extreme"].mean() > 0.8

    def test_o_mesmo_no_wct(self, com_flags):
        com_valor = com_flags[com_flags["wct_c"].notna()]
        mais_frio = com_valor.loc[com_valor["wct_c"].idxmin()]

        assert not bool(mais_frio["wct_c_flag_extreme"])


class TestAliasDoCodigoR:
    """R documenta o codigo como 'hi'; este pacote usa 'heat_index' (M73).

    A coluna de saida sempre foi a mesma (hi_c); o codigo nao. Uma chamada
    transcrita da documentacao do R levantava Unknown indicator code.
    """

    def test_resolve(self):
        assert resolve_indicator("hi") == "heat_index"
        assert resolve_indicator("heat_index") == "heat_index"
        assert resolve_indicator("wbgt") == "wbgt"

    def test_o_codigo_do_r_e_aceito(self, entrada):
        rel = get_connection().from_df(entrada)
        out = cs.sus_climate_compute_indicators(
            rel, indicators=["hi"], station_col="station_code",
            date_col="datetime", verbose=False,
        ).df()

        assert "hi_c" in out.columns
        assert "hi_c_flag_extreme" in out.columns

    def test_codigo_inexistente_ainda_levanta(self, entrada):
        rel = get_connection().from_df(entrada)
        with pytest.raises(ValueError, match="Unknown indicator"):
            cs.sus_climate_compute_indicators(
                rel, indicators=["nao_existe"], station_col="station_code",
                date_col="datetime", verbose=False,
            )


class TestFlagsComoEvidencia:
    """As flags tornam o M71 contavel em vez de argumentavel.

    ``wbgt_stull`` recebe os limiares do ``wbgt`` de proposito -- e a mesma
    grandeza fisica, entao as bandas do ISO 7243 valem igual, e com o mesmo
    limiar nas duas colunas a diferenca vira uma contagem.
    """

    def test_o_stull_marca_mais_que_o_dobro_de_calor_extremo(self, com_flags):
        como_o_r = int(com_flags["wbgt_c_flag_extreme"].sum())
        validado = int(com_flags["wbgt_stull_c_flag_extreme"].sum())

        assert como_o_r == 207
        assert validado == 449
        assert validado > 2 * como_o_r

    def test_o_stull_usa_os_mesmos_limiares(self):
        for f in ("extreme", "high", "low"):
            assert flag_threshold("wbgt_stull", f) == flag_threshold("wbgt", f)


# ---------------------------------------------------------------------------
# Os tres indicadores que JA existiam e nunca tinham sido conferidos
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def saida_tri(entrada) -> pd.DataFrame:
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=["hi", "thi", "vapor_pressure"],
        station_col="station_code", date_col="datetime",
        confidence_flags=False, verbose=False,
    ).df()


class TestIndiceDeCalor:
    """O hi_c chegava a 151,6 C antes desta correcao (M75).

    Os dois polinomios -- o do R em Fahrenheit e o daqui em Celsius -- sao a
    MESMA regressao de Rothfusz: sem o teto eles concordam com mediana
    0,000 C e media 0,0217. Faltavam tres pecas, e a terceira produzia
    absurdo.
    """

    def test_paridade_exata_com_o_r(self, saida_tri, referencia_r):
        py = saida_tri["hi_c"].astype(float)
        r = referencia_r["hi_c"].astype(float)
        ambos = py.notna() & r.notna()

        assert (py.isna() != r.isna()).sum() == 0
        assert ambos.sum() > 500
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    def test_o_teto_de_60_graus(self, saida_tri):
        """Rothfusz e um ajuste de ~27-43 C; alem disso o polinomio foge.

        Sem o teto esta coluna chegava a 151,6 C -- um indice de calor
        metade mais quente que qualquer temperatura ja registrada na Terra.
        O teto dispara em 32,6% do dominio valido, entao nao era canto.
        """
        valores = saida_tri["hi_c"].dropna()

        assert valores.max() <= 60.0
        assert (valores >= 59.999).sum() > 0     # o teto de fato atua

    def test_o_limiar_de_validade_e_26_7_e_nao_27(self):
        """26,7 C sao 80 F, a borda real do dominio de Rothfusz.

        O valor 27,0 que estava aqui divergia do R em 716 linhas de 200 mil
        -- pequeno, mas so na mascara, sem nada a ver com a formula.
        """
        df = pd.DataFrame({
            "station_code": ["A"] * 4,
            "datetime": pd.date_range("2020-01-01", periods=4, freq="h"),
            "tair_dry_bulb_c": [26.6, 26.7, 26.8, 27.0],
            "rh_mean_porc": [50.0] * 4,
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["hi"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()

        assert pd.isna(out["hi_c"].iloc[0])      # 26,6 fora
        assert out["hi_c"].iloc[1:].notna().all()  # 26,7 em diante dentro

    def test_o_ajuste_de_umidade_alta_do_r(self):
        """R soma um ajuste acima de 85% de RH na faixa de 80-87 F.

        Vale 0,264 C em media nas 5,5% de linhas onde incide -- pequeno,
        mas e parte da formula publicada e estava faltando.
        """
        # 27,5 C = 81,5 F, dentro da faixa; RH 95 > 85
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [27.5], "rh_mean_porc": [95.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["hi"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()
        t_f = 27.5 * 9 / 5 + 32
        ajuste = (95.0 - 85.0) / 10.0 * ((87.0 - t_f) / 5.0)

        assert 80.0 <= t_f <= 87.0
        assert abs(ajuste) > 0.1                 # o ajuste nao e nulo aqui
        assert out["hi_c"].notna().iloc[0]

    def test_fora_do_dominio_devolve_nulo_e_nao_extrapola(self):
        df = pd.DataFrame({
            "station_code": ["A", "A"],
            "datetime": pd.date_range("2020-01-01", periods=2, freq="h"),
            "tair_dry_bulb_c": [15.0, 35.0], "rh_mean_porc": [90.0, 20.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["hi"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()

        assert out["hi_c"].isna().all()   # frio demais / seco demais


class TestThi:
    """R e este pacote usavam variantes DIFERENTES do indice de Thom (M74).

    R  : T - (1 - RH/100) * (T - 14.4) / 2   ==  T - (0.5 - 0.005 RH)(T - 14.4)
    Antes daqui:                                 T - (0.55 - 0.0055 RH)(T - 14.5)

    A segunda e a forma normalmente citada como indice de desconforto de
    Thom. Nenhuma das duas esta errada -- sao coeficientes publicados
    distintos -- entao o alinhamento seguiu a regra de replicar o R.
    """

    def test_paridade_com_o_r_a_menos_do_desempate(self, saida_tri, referencia_r):
        """99,3% exatas; o residuo e arredondamento de empate, nao formula.

        27 de 3.920 linhas diferem por exatamente 0,01, e todas sao casos
        de meio exato na terceira casa (23,585; 9,315; 28,245). O round()
        do R 4.x desempata conforme a representacao binaria -- a propria
        documentacao dele avisa -- e nao ha modo do DuckDB que reproduza
        isso: o ROUND_EVEN acerta 3.893 e o ROUND 3.884.
        """
        py = saida_tri["thi_c"].astype(float)
        r = referencia_r["thi_c"].astype(float)
        ambos = py.notna() & r.notna()
        d = np.abs(py[ambos].to_numpy() - r[ambos].to_numpy())

        assert (py.isna() != r.isna()).sum() == 0
        # 0.01 mais uma folga de ponto flutuante: a subtracao de dois
        # valores ja arredondados nao devolve 0,01 exato.
        assert d.max() <= 0.0101
        assert (d < 1e-9).sum() / len(d) > 0.99
        assert d.mean() < 1e-4

    def test_usa_a_variante_do_r(self):
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [30.0], "rh_mean_porc": [60.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["thi"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()
        do_r = 30.0 - ((1 - 60.0 / 100) * (30.0 - 14.4)) / 2
        a_classica = 30.0 - (0.55 - 0.0055 * 60.0) * (30.0 - 14.5)

        assert float(out["thi_c"].iloc[0]) == pytest.approx(do_r, abs=0.005)
        # E as duas variantes de fato diferem neste ponto.
        assert abs(do_r - a_classica) > 0.05


class TestPressaoDeVapor:
    def test_paridade_exata_com_o_r(self, saida_tri, referencia_r):
        """A formula sempre foi a mesma; faltava so o arredondamento em 3."""
        py = saida_tri["vapor_pressure_kpa"].astype(float)
        r = referencia_r["vapor_pressure_kpa"].astype(float)
        ambos = py.notna() & r.notna()

        assert (py.isna() != r.isna()).sum() == 0
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    def test_arredonda_em_tres_casas(self, saida_tri):
        valores = saida_tri["vapor_pressure_kpa"].dropna()
        assert (np.abs(valores * 1000 - np.round(valores * 1000)) < 1e-9).all()


# ---------------------------------------------------------------------------
# Variantes corretas (M77)
# ---------------------------------------------------------------------------

class TestVariantesCorretas:
    """A regra e replicar o R e anotar; isso perde trabalho sem um lugar
    para o codigo correto.

    Cada divergencia em que este pacote acredita que o R erra tem uma
    variante correta: indicador de verdade, com coluna propria e teste.
    Ficam FORA de indicators="all", para o padrao entregar exatamente o
    conjunto de colunas do R, e sao pedidas por nome.

    Antes disso o arranjo era ad-hoc -- uma funcao auxiliar para a
    sensacao termica, uma coluna para o WBGT, e nada para as outras duas.
    """

    def test_cada_variante_aponta_o_que_corrige(self):
        assert CORRECTED_INDICATORS == {
            "thi_classic": "thi",
            "koppen_humidity_strict": "koppen_humidity",
            "diurnal_range_station": "diurnal_range",
            "wct_ms": "wct",
            "wbgt_stull": "wbgt",
            "et_calm": "et",
            "heat_stress_risk_strict": "heat_stress_risk",
        }
        for variante, base in CORRECTED_INDICATORS.items():
            assert variante in _INDICATOR_DEFS
            assert base in ALL_INDICATORS

    def test_ficam_fora_do_conjunto_padrao(self):
        """indicators="all" tem de render o conjunto de colunas do R."""
        for variante in CORRECTED_INDICATORS:
            assert variante not in ALL_INDICATORS

    def test_mas_sao_aceitas_por_nome(self, entrada):
        rel = get_connection().from_df(entrada)
        out = cs.sus_climate_compute_indicators(
            rel, indicators=list(CORRECTED_INDICATORS),
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()

        for variante in CORRECTED_INDICATORS:
            assert _INDICATOR_DEFS[variante][0] in out.columns

    def test_herdam_os_limiares_do_que_corrigem(self):
        """Mesma grandeza fisica, mesmas bandas -- e assim a comparacao
        entre as duas colunas vira uma CONTAGEM."""
        for variante, base in CORRECTED_INDICATORS.items():
            for f in ("extreme", "high", "low"):
                assert flag_threshold(variante, f) == flag_threshold(base, f)

    def test_o_erro_de_codigo_desconhecido_menciona_as_variantes(self, entrada):
        rel = get_connection().from_df(entrada)
        with pytest.raises(ValueError, match="Corrected variants"):
            cs.sus_climate_compute_indicators(
                rel, indicators=["nao_existe"], station_col="station_code",
                date_col="datetime", verbose=False,
            )


@pytest.fixture(scope="module")
def par(entrada) -> pd.DataFrame:
    """Variantes e bases na MESMA chamada, para a comparacao nao depender
    de a ordem das linhas coincidir entre execucoes."""
    rel = get_connection().from_df(entrada)
    pedidos = [*CORRECTED_INDICATORS, *CORRECTED_INDICATORS.values()]
    return cs.sus_climate_compute_indicators(
        rel, indicators=pedidos, station_col="station_code",
        date_col="datetime", confidence_flags=False, verbose=False,
    ).df()


class TestVarianteVsBase:
    """Cada variante tem de DIFERIR da que corrige, senao nao ha o que
    preservar. Comparadas na MESMA chamada, para nao depender de a ordem
    das linhas coincidir entre execucoes.
    """

    @pytest.mark.parametrize(("variante", "base"), [
        ("thi_classic_c", "thi_c"),
        ("diurnal_range_station_c", "diurnal_range_c"),
        ("wct_ms_c", "wct_c"),
        ("wbgt_stull_c", "wbgt_c"),
    ])
    def test_as_numericas_diferem(self, par, variante, base):
        a, b = par[variante].astype(float), par[base].astype(float)
        m = a.notna() & b.notna()

        assert m.sum() > 500
        # 0.7 e nao 0.9: o diurnal_range so difere onde o dia tem mais de
        # uma estacao, e na fixture isso e 72% das linhas.
        assert (np.abs(a[m] - b[m]) > 1e-9).mean() > 0.7

    def test_koppen_difere_SO_onde_a_umidade_falta(self, par, entrada):
        """A unica variante cuja diferenca e restrita: o R rotula ausencia
        como "Perhumid" e a estrita devolve nulo."""
        a, b = par["koppen_humidity_strict"], par["koppen_humidity"]
        dif = (a != b) & ~(a.isna() & b.isna())
        sem_rh = par["rh_mean_porc"].isna()

        assert int(dif.sum()) == int(sem_rh.sum()) == 40
        assert (dif == sem_rh).all()
        assert (b[sem_rh] == "Perhumid").all()
        assert a[sem_rh].isna().all()

    def test_o_wct_corrigido_converge_com_o_wcet(self, entrada):
        """A prova do M69, agora entre duas COLUNAS em vez de uma funcao.

        wcet_c (Environment Canada, km/h, conversao correta) e wct_ms_c
        (NWS, m/s -> mph correto) sao a mesma grandeza e convergem; o
        wct_c do R, nao.
        """
        rel = get_connection().from_df(entrada)
        out = cs.sus_climate_compute_indicators(
            rel, indicators=["wcet", "wct", "wct_ms"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()
        m = out["wcet_c"].notna() & out["wct_c"].notna() & out["wct_ms_c"].notna()

        assert np.abs(out.loc[m, "wct_ms_c"] - out.loc[m, "wcet_c"]).max() < 0.1
        assert np.abs(out.loc[m, "wct_c"] - out.loc[m, "wcet_c"]).mean() > 4.0

    def test_a_ordem_das_linhas_segue_a_entrada(self, entrada):
        """Premissa de toda comparacao contra fixture neste arquivo.

        Se a saida reordenasse, comparar posicao a posicao com a
        referencia do R nao significaria nada.
        """
        rel = get_connection().from_df(entrada)
        out = cs.sus_climate_compute_indicators(
            rel, indicators=["thi"], station_col="station_code",
            date_col="datetime", confidence_flags=False, verbose=False,
        ).df()

        assert np.array_equal(out["rh_mean_porc"].to_numpy(),
                              entrada["rh_mean_porc"].to_numpy(), equal_nan=True)
        assert list(out["station_code"]) == list(entrada["station_code"])


class TestDiurnalRangeSegueOR:
    """O diurnal_range_c passou a agrupar por DIA apenas, como o R (M76).

    ATENCAO AO METODO: a funcao de JANELA reordena as linhas de saida --
    verificado, ao contrario dos indicadores escalares, que preservam a
    ordem da entrada. Comparar por POSICAO contra a entrada nao vale aqui,
    e foi assim que uma medicao minha anterior chegou a "3936 de 4000
    linhas diferem" comparando arrays desalinhados. O numero correto,
    junto pela chave (estacao, datetime), e 2896 de 4000.
    """

    CHAVE = ["station_code", "datetime"]

    def _com_esperado(self, entrada, extras):
        """Saida do pacote junta com o valor esperado, POR CHAVE."""
        rel = get_connection().from_df(entrada)
        out = cs.sus_climate_compute_indicators(
            rel, indicators=extras, station_col="station_code",
            date_col="datetime", confidence_flags=False, verbose=False,
        ).df()
        d = entrada.copy()
        d["dia"] = pd.to_datetime(d["datetime"]).dt.date
        por_dia = d.groupby("dia")["tair_dry_bulb_c"].agg(["max", "min"])
        d["esperado_r"] = (por_dia["max"] - por_dia["min"]).reindex(d["dia"]).to_numpy()
        por_est = d.groupby(["station_code", "dia"])["tair_dry_bulb_c"].agg(["max", "min"])
        d["esperado_estacao"] = (por_est["max"] - por_est["min"]).reindex(
            pd.MultiIndex.from_arrays([d["station_code"], d["dia"]])).to_numpy()
        return out.merge(d[[*self.CHAVE, "esperado_r", "esperado_estacao"]],
                         on=self.CHAVE, how="left", validate="one_to_one")

    def test_a_chave_identifica_a_linha(self, entrada):
        """Premissa da juncao: sem isso o merge nao significaria nada."""
        assert not entrada.duplicated(self.CHAVE).any()

    def test_a_janela_reordena_e_o_escalar_nao(self, entrada):
        """Registra a diferenca de comportamento que invalidou a medicao.

        Vale saber porque qualquer teste futuro que compare um indicador
        de janela posicionalmente cai na mesma armadilha.
        """
        rel = get_connection().from_df(entrada)
        def ordem(inds):
            o = cs.sus_climate_compute_indicators(
                rel, indicators=inds, station_col="station_code",
                date_col="datetime", confidence_flags=False, verbose=False).df()
            return np.array_equal(o["rh_mean_porc"].to_numpy(),
                                  entrada["rh_mean_porc"].to_numpy(), equal_nan=True)

        assert ordem(["thi"]) is True              # escalar: preserva
        assert ordem(["diurnal_range"]) is False   # janela: reordena

    def test_agrupa_sem_estacao_como_o_r(self, entrada):
        j = self._com_esperado(entrada, ["diurnal_range"])
        assert np.allclose(j["diurnal_range_c"], j["esperado_r"], equal_nan=True)

    def test_a_variante_agrupa_por_estacao_e_dia(self, entrada):
        j = self._com_esperado(entrada, ["diurnal_range_station"])
        assert np.allclose(j["diurnal_range_station_c"], j["esperado_estacao"],
                           equal_nan=True)

    def test_as_duas_divergem_onde_o_dia_tem_mais_de_uma_estacao(self, entrada):
        """Agrupar sem estacao dobra a amplitude com a diferenca ENTRE elas."""
        j = self._com_esperado(entrada, ["diurnal_range", "diurnal_range_station"])
        dif = np.abs(j["diurnal_range_c"] - j["diurnal_range_station_c"]) > 1e-9

        assert entrada["station_code"].nunique() == 2
        assert int(dif.sum()) == 2896
        # E sempre na mesma direcao: misturar estacoes so pode AUMENTAR.
        assert (j.loc[dif, "diurnal_range_c"]
                > j.loc[dif, "diurnal_range_station_c"]).all()


# ---------------------------------------------------------------------------
# et e heat_stress_risk (M78, M79)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def saida_et(entrada) -> pd.DataFrame:
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=["et", "heat_stress_risk", "et_calm",
                         "heat_stress_risk_strict", "wbgt"],
        station_col="station_code", date_col="datetime",
        confidence_flags=False, verbose=False,
    ).df()


class TestTemperaturaEfetiva:
    """O et_c do R e NA para vento abaixo de 0,2 m/s (M78).

    R poe um piso de 0,04 m/s com pmax(ws, 0.04) e em seguida tira
    (ws_safe - 0.2)^0.5 -- o piso e anulado pela subtracao que vem depois,
    e abaixo de 0,2 o radicando fica negativo. Verificado no R: 0, 0.04,
    0.10 e 0.19 dao NA; 0.20 da valor.
    """

    def test_paridade_exata_com_o_r(self, saida_et, referencia_r):
        py = saida_et["et_c"].astype(float)
        r = referencia_r["et_c"].astype(float)
        ambos = py.notna() & r.notna()

        assert (py.isna() != r.isna()).sum() == 0
        assert ambos.sum() > 3000
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    def test_calma_nao_produz_valor(self, entrada, saida_et):
        """76 linhas da fixture tem vento abaixo de 0,2 m/s.

        Noite de calma e exatamente quando uma temperatura efetiva
        importaria, entao isto nao e canto raro.
        """
        calmo = entrada["ws_2_m_s"].fillna(0) < 0.2

        assert int(calmo.sum()) == 76
        assert saida_et.loc[calmo, "et_c"].isna().all()

    def test_a_variante_recupera_a_calma(self, entrada, saida_et):
        """O et_calm poe o piso no RADICANDO, nao no vento."""
        calmo = entrada["ws_2_m_s"].fillna(0) < 0.2
        com_dado = calmo & entrada["tair_dry_bulb_c"].notna() & entrada["rh_mean_porc"].notna()

        assert saida_et.loc[com_dado, "et_calm_c"].notna().all()
        assert int(com_dado.sum()) == 74

    def test_as_duas_batem_onde_ha_vento(self, entrada, saida_et):
        """Acima de 0,2 m/s a variante nao muda nada."""
        com_vento = entrada["ws_2_m_s"].fillna(0) >= 0.2
        a = saida_et.loc[com_vento, "et_c"]
        b = saida_et.loc[com_vento, "et_calm_c"]
        m = a.notna() & b.notna()

        assert m.sum() > 3000
        assert np.abs(a[m] - b[m]).max() == 0.0


class TestRiscoDeEstresseTermico:
    """O heat_stress_risk do R rotula dado ausente como "None" (M79).

    Mesma forma do koppen -- o case_when termina em TRUE ~ "None" -- mas
    pior, porque "None" e tambem a resposta legitima para tempo frio: as
    duas situacoes ficam indistinguiveis.
    """

    def test_paridade_exata_com_o_r(self, saida_et, referencia_r):
        py, r = saida_et["heat_stress_risk"], referencia_r["heat_stress_risk"]
        assert ((py == r) | (py.isna() & r.isna())).all()

    def test_classifica_o_wbgt_ARREDONDADO(self, saida_et):
        """Tres linhas dependiam disso, e foi defeito meu.

        O .compute_heat_stress_risk do R classifica o que o
        .compute_wbgt DEVOLVE, que ja vem arredondado em duas casas.
        Substituir a expressao crua punha tres linhas da fixture uma faixa
        acima -- cada uma delas exatamente sobre um limiar (20,00, 28,00 e
        30,00).
        """
        no_limiar = saida_et["wbgt_c"].isin([20.0, 28.0, 30.0])

        assert int(no_limiar.sum()) >= 3
        # Sobre o limiar, o `>` e falso: a faixa e a de BAIXO.
        assert (saida_et.loc[saida_et["wbgt_c"] == 20.0,
                             "heat_stress_risk"] == "None").all()
        assert (saida_et.loc[saida_et["wbgt_c"] == 28.0,
                             "heat_stress_risk"] == "Moderate").all()

    def test_dado_ausente_vira_None_como_no_r(self, entrada, saida_et):
        sem_dado = entrada["tair_dry_bulb_c"].isna() | entrada["rh_mean_porc"].isna()

        assert int(sem_dado.sum()) == 80
        assert (saida_et.loc[sem_dado, "heat_stress_risk"] == "None").all()

    def test_a_variante_estrita_devolve_nulo(self, entrada, saida_et):
        """Separa "sem risco" de "sem dado", que o R funde."""
        sem_dado = entrada["tair_dry_bulb_c"].isna() | entrada["rh_mean_porc"].isna()

        assert saida_et.loc[sem_dado, "heat_stress_risk_strict"].isna().all()
        # E fora dessas linhas as duas concordam.
        resto = ~sem_dado
        assert (saida_et.loc[resto, "heat_stress_risk"]
                == saida_et.loc[resto, "heat_stress_risk_strict"]).all()

    def test_quantos_None_o_r_conta_a_mais(self, saida_et):
        """A medida do problema: 80 linhas sem dado escondidas no "None"."""
        do_r = int((saida_et["heat_stress_risk"] == "None").sum())
        estrito = int((saida_et["heat_stress_risk_strict"] == "None").sum())

        assert do_r - estrito == 80

    def test_as_seis_faixas(self, saida_et):
        assert set(saida_et["heat_stress_risk"].unique()) <= {
            "Extreme", "Very High", "High", "Moderate", "Low", "None"}


# ---------------------------------------------------------------------------
# utci e pet (M80, M81) -- os dois ultimos indicadores
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def saida_up(entrada) -> pd.DataFrame:
    rel = get_connection().from_df(entrada)
    return cs.sus_climate_compute_indicators(
        rel, indicators=["utci", "pet"], station_col="station_code",
        date_col="datetime", confidence_flags=False, verbose=False,
    ).df()


class TestUtci:
    """Aproximacao de seis termos, nao o polinomio publicado (M80).

    A ajuda do R chama de "Fiala-polynomial-inspired multi-term
    regression" e cita Brode et al. (2012), o artigo do UTCI. O hedge e
    justo: o UTCI publicado e um polinomio de sexta ordem com 210 termos,
    e este tem seis. Registrado para quem le a coluna saber, nao como
    defeito.
    """

    def test_paridade_exata_com_o_r(self, saida_up, referencia_r):
        py = saida_up["utci_c"].astype(float)
        r = referencia_r["utci_c"].astype(float)
        ambos = py.notna() & r.notna()

        assert (py.isna() != r.isna()).sum() == 0
        assert ambos.sum() > 3000
        assert np.abs(py[ambos].to_numpy() - r[ambos].to_numpy()).max() == 0.0

    def test_o_teto_de_50_graus_atua_em_dado_real(self, saida_up):
        """Nao e limite defensivo distante: ele TRUNCA a fixture."""
        assert saida_up["utci_c"].max() == 50.0
        assert int((saida_up["utci_c"] >= 50.0).sum()) == 2

    @pytest.mark.parametrize(("sr_kj", "esperado"), [
        (0, 30.39), (1800, 31.87), (3600, 35.36),
    ])
    def test_resposta_ao_sol_e_modesta(self, sr_kj, esperado):
        """4,97 C de 0 a 1000 W/m2, onde o UTCI publicado passa de 10.

        Os valores vem de rodar o .compute_utci do R com T=30, RH=60 e
        ws=2 -- nao da minha leitura da formula.
        """
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [30.0], "rh_mean_porc": [60.0],
            "ws_2_m_s": [2.0], "sr_kj_m2": [float(sr_kj)],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["utci"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()

        assert float(out["utci_c"].iloc[0]) == pytest.approx(esperado, abs=0.01)


class TestPet:
    """O ajuste sazonal de vestuario nao se aplica sem coluna `date` (M81).

    R le o `date` por esse nome FIXO, e nao o datetime_col que a funcao
    aceita e detecta. Sem uma coluna chamada literalmente `date`, o
    inherits() falha e o mes vira 6 para todas as linhas, entao o ajuste
    documentado fica inerte. Replicado, coluna `date` e tudo.
    """

    def test_paridade_com_o_r_a_menos_do_desempate(self, saida_up, referencia_r):
        """99,82% exatas; o residuo e o mesmo desempate do thi_c.

        7 de 3.920 linhas diferem por 0,01, todas casos de meio exato
        (34,645; 34,455; -21,515; 24,755). O round() do R 4.x desempata
        conforme a representacao binaria.
        """
        py = saida_up["pet_c"].astype(float)
        r = referencia_r["pet_c"].astype(float)
        ambos = py.notna() & r.notna()
        d = np.abs(py[ambos].to_numpy() - r[ambos].to_numpy())

        assert (py.isna() != r.isna()).sum() == 0
        assert d.max() <= 0.0101
        assert (d < 1e-9).sum() / len(d) > 0.998

    def test_sem_coluna_date_o_mes_e_6(self, entrada):
        """A fixture chama a coluna de `datetime`, entao cai no fallback."""
        from climasus4py.enrichment.climate_indicators import (
            PET_MONTH_FALLBACK,
            _pet_month_expr,
        )

        assert "date" not in entrada.columns
        assert _pet_month_expr(list(entrada.columns)) == PET_MONTH_FALLBACK

    def test_com_coluna_date_o_ajuste_se_aplica(self):
        """Janeiro e julho passam a diferir -- so com a coluna certa.

        Diferenca de 0,12 C: o que se perde e um recurso documentado, nao
        muita exatidao.
        """
        from climasus4py.enrichment.climate_indicators import _pet_month_expr

        df = pd.DataFrame({
            "station_code": ["A", "A"],
            "datetime": [pd.Timestamp("2020-01-15"), pd.Timestamp("2020-07-15")],
            "date": [pd.Timestamp("2020-01-15").date(),
                     pd.Timestamp("2020-07-15").date()],
            "tair_dry_bulb_c": [30.0, 30.0], "rh_mean_porc": [60.0, 60.0],
            "ws_2_m_s": [2.0, 2.0], "sr_kj_m2": [1800.0, 1800.0],
        })
        assert _pet_month_expr(list(df.columns)) != "6"
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["pet"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()
        jan, jul = float(out["pet_c"].iloc[0]), float(out["pet_c"].iloc[1])

        assert jan == pytest.approx(31.25, abs=0.01)
        assert jul == pytest.approx(31.13, abs=0.01)
        assert abs(jan - jul) == pytest.approx(0.12, abs=0.01)

    def test_pet_nao_tem_teto_ao_contrario_do_utci(self, saida_up):
        """R limita o UTCI em [-60, 50] e nao limita o PET."""
        assert saida_up["utci_c"].max() <= 50.0
        df = pd.DataFrame({
            "station_code": ["A"], "datetime": [pd.Timestamp("2020-01-01")],
            "tair_dry_bulb_c": [60.0], "rh_mean_porc": [100.0],
            "ws_2_m_s": [0.1], "sr_kj_m2": [3600.0],
        })
        out = cs.sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["pet"],
            station_col="station_code", date_col="datetime",
            confidence_flags=False, verbose=False,
        ).df()

        assert float(out["pet_c"].iloc[0]) > 50.0


class TestTodosOsQuinzeIndicadoresDoR:
    """Fecha o M12: os 15 indicadores do R existem no Python."""

    COLUNA_DO_R = {
        "wbgt": "wbgt_c", "hi": "hi_c", "thi": "thi_c", "wcet": "wcet_c",
        "wct": "wct_c", "et": "et_c", "utci": "utci_c", "pet": "pet_c",
        "cdd": "cdd_c", "hdd": "hdd_c", "gdd": "gdd_c",
        "diurnal_range": "diurnal_range_c",
        "vapor_pressure": "vapor_pressure_kpa",
        "heat_stress_risk": "heat_stress_risk",
        "koppen_humidity": "koppen_humidity",
    }

    def test_sao_quinze(self):
        assert len(self.COLUNA_DO_R) == 15

    @pytest.mark.parametrize("codigo", list(COLUNA_DO_R))
    def test_portado_com_a_coluna_do_r(self, codigo):
        canonico = resolve_indicator(codigo)

        assert canonico in _INDICATOR_DEFS
        assert _INDICATOR_DEFS[canonico][0] == self.COLUNA_DO_R[codigo]
        assert canonico in ALL_INDICATORS
