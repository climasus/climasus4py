"""M46: os plots de onda de calor e de frio, e a simetria entre os irmaos.

O achado registrado era que o `sus_climate_plot_coldwaves` quebrava com
"TypeError: Continuous value supplied to discrete scale" enquanto o
`sus_climate_plot_heatwaves`, chamado com a mesma forma, gerava o
grafico. Verificado em 07/09/2026.

**O sintoma NAO reproduz no codigo atual.** Tentei 24 combinacoes -- os
quatro tipos (timeline, calendar, intensity, trend) x tres filtros de
metodo (nenhum, EHF, UTCI) x as duas funcoes -- mais `intensity_class`
inteira nula e `is_cw` em bool, int, float e float-com-NaN. Todas passam.
O arquivo do plot nao muda desde 04/09, ou seja e o mesmo codigo que
falhou, e nenhum teste cobria essas funcoes -- entao o mais provavel e
que a versao do plotnine tenha mudado (0.15.7 aqui).

**O que REPRODUZIU foi a assimetria que o registro supunha ser a causa**,
e ela era real e medivel: para um metodo que nao pode ser calculado por
falta de coluna de entrada, o coldwaves escrevia `np.nan` e o heatwaves
`pd.NA`. `np.nan` forca a coluna inteira para float64; `pd.NA` a deixa
object. Sao BANDEIRAS booleanas -- "houve onda por este metodo" -- e uma
bandeira que sai numerica e exatamente o que vai para uma escala continua
num grafico que declara escala discreta.

Medido antes: `cw_utci`, `cw_wbgt` e `cw_hi` em float64 contra
`hw_utci`, `hw_wbgt` e `hw_hi` em object. Corrigido usando `pd.NA` nos
tres, o que exigiu alinhar tambem o calculo do `cw_any` -- que fazia
`astype(float)` e levanta com `pd.NA`, e cujo irmao ja usava `.eq(True)`
desde o M61.

Estes testes existem porque nao havia nenhum: sem eles, nem a quebra
original nem a correcao dela seriam notadas.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection

TIPOS = ("timeline", "calendar", "intensity", "trend")
METODOS = ("who", "wmo", "inmet", "ehf", "utci", "wbgt", "hi")


@pytest.fixture(scope="module")
def serie() -> pd.DataFrame:
    """Serie sintetica com ciclo anual, longa o bastante para o baseline."""
    rng = np.random.default_rng(3)
    n = 900
    return pd.DataFrame({
        "station_code": ["A"] * n,
        "date": pd.date_range("2021-01-01", periods=n),
        "tair_dry_bulb_c": (20 + 12 * np.sin(np.arange(n) / 58)
                            + rng.normal(0, 3, n)),
    })


@pytest.fixture(scope="module")
def frio(serie):
    return cs.sus_climate_compute_coldwaves(
        get_connection().from_df(serie), verbose=False)


@pytest.fixture(scope="module")
def calor(serie):
    return cs.sus_climate_compute_heatwaves(
        get_connection().from_df(serie), verbose=False)


class TestSimetriaEntreOsIrmaos:
    """A causa que o M46 supunha, e que era real."""

    @pytest.mark.parametrize("metodo", METODOS)
    def test_o_dtype_da_bandeira_e_o_mesmo_nos_dois(self, frio, calor,
                                                    metodo):
        f = frio["daily"].get(f"cw_{metodo}")
        c = calor["daily"].get(f"hw_{metodo}")
        assert f is not None and c is not None, metodo
        assert f.dtype == c.dtype, (
            f"cw_{metodo} e {f.dtype} e hw_{metodo} e {c.dtype} -- "
            f"bandeira numerica num e categorica no outro")

    @pytest.mark.parametrize("metodo", ("utci", "wbgt", "hi"))
    def test_metodo_sem_entrada_nao_vira_float(self, frio, metodo):
        """np.nan forcaria float64; pd.NA deixa object.

        Sao os tres metodos que a serie sintetica nao alimenta: nao ha
        utci_min, wbgt_min nem hi_min.
        """
        col = frio["daily"][f"cw_{metodo}"]
        assert col.isna().all(), "a fixture perdeu o caso"
        assert col.dtype == object, (
            f"cw_{metodo} voltou a ser {col.dtype}: uma bandeira booleana "
            f"nao pode sair numerica")

    def test_o_any_e_booleano_nos_dois(self, frio, calor):
        assert frio["daily"]["cw_any"].dtype == bool
        assert calor["daily"]["hw_any"].dtype == bool

    def test_o_any_sobrevive_a_bandeira_nula(self, frio):
        """`astype(float)` levantava com pd.NA; `.eq(True)` nao.

        Contrapartida do teste acima: trocar o sentinela sem trocar o
        calculo do cw_any quebrava o computo inteiro com "float()
        argument must be a string or a real number, not 'NAType'".
        """
        assert frio["daily"]["cw_any"].any(), (
            "a serie tem de produzir pelo menos uma onda de frio")
        assert not frio["daily"]["cw_any"].isna().any()


class TestOsPlotsGeram:
    """A cobertura que nao existia: nenhum teste chamava estas funcoes."""

    @pytest.mark.parametrize("tipo", TIPOS)
    @pytest.mark.parametrize("metodo", [None, "EHF"])
    def test_coldwaves(self, frio, tipo, metodo):
        g = cs.sus_climate_plot_coldwaves(frio, type=tipo, method=metodo)
        assert g is not None

    @pytest.mark.parametrize("tipo", TIPOS)
    @pytest.mark.parametrize("metodo", [None, "EHF"])
    def test_heatwaves(self, calor, tipo, metodo):
        g = cs.sus_climate_plot_heatwaves(calor, type=tipo, method=metodo)
        assert g is not None

    @pytest.mark.parametrize("tipo", TIPOS)
    def test_metodo_sem_dado_avisa_e_devolve_None(self, frio, calor, tipo):
        """UTCI nao tem dado nesta serie, e os dois se comportam igual.

        A primeira versao deste teste cobrava `is not None` tambem aqui e
        falhou -- o teste estava errado, nao o codigo. Devolver None com
        aviso e a resposta certa para "filtrei por um metodo que nao
        existe no dado"; o que importa e que as duas funcoes facam o
        MESMO, que e o assunto do M46.
        """
        import warnings as w

        for fn, dados in ((cs.sus_climate_plot_coldwaves, frio),
                          (cs.sus_climate_plot_heatwaves, calor)):
            with w.catch_warnings(record=True) as capturados:
                w.simplefilter("always")
                g = fn(dados, type=tipo, method="UTCI")
            assert g is None, f"{fn.__name__}/{tipo}"
            assert len(capturados) == 1, (
                f"{fn.__name__}/{tipo}: devolver None sem avisar seria "
                f"silencio")

    def test_o_default_de_lang_difere_entre_os_irmaos_como_no_R(self):
        """Assimetria do R, replicada de proposito (M120).

        `plot_coldwaves` tem lang='pt' e `plot_heatwaves` lang='en' --
        conferido nas formals do climasus4r, onde e exatamente assim. Quem
        chamar as duas sem passar lang recebe uma figura em portugues e
        outra em ingles. E defeito do R, nao do porte, e por isso esta
        fixado aqui em vez de corrigido: se alguem "arrumar" o Python, a
        paridade quebra sem que ninguem perceba.
        """
        import inspect

        pf = inspect.signature(cs.sus_climate_plot_coldwaves).parameters
        pc = inspect.signature(cs.sus_climate_plot_heatwaves).parameters
        assert pf["lang"].default == "pt"
        assert pc["lang"].default == "en"
        # E as funcoes de CALCULO usam 'pt' nas duas, tambem como no R.
        assert inspect.signature(
            cs.sus_climate_compute_coldwaves).parameters["lang"].default == "pt"
        assert inspect.signature(
            cs.sus_climate_compute_heatwaves).parameters["lang"].default == "pt"

    def test_o_numero_de_eventos_nao_mudou(self, frio, calor):
        """Guarda contra a correcao de dtype mexer no que se conta.

        Trocar o sentinela de uma bandeira podia, em principio, mudar
        quantos eventos o `cw_any` reconhece. Os numeros sao os medidos
        antes da mudanca.
        """
        assert len(frio["events"]) == 8
        assert len(calor["events"]) == 9


class TestOSintomaOriginalNaoReproduz:
    """Honestidade sobre o que nao foi reproduzido.

    Se voltar a acontecer, e aqui que se documenta o gatilho. Estes
    testes cobrem as formas de dado que eu suspeitei e descartei, para
    ninguem repetir a busca.
    """

    @staticmethod
    def _minimo(intensity_class):
        ev = pd.DataFrame({
            "station_code": ["A"], "method": ["INMET"],
            "start_date": pd.to_datetime(["2021-06-01"]),
            "end_date": pd.to_datetime(["2021-06-04"]),
            "duration_days": [4], "temp_mean": [8.0], "temp_peak": [5.0],
            "anomaly_mean": [-3.0], "anomaly_cumulative": [-12.0],
            "severity_index": [0.0], "year": [2021],
            "intensity_class": intensity_class,
        })
        dias = pd.date_range("2021-06-01", periods=10)
        daily = pd.DataFrame({
            "station_code": ["A"] * 10, "date": dias,
            "method": ["INMET"] * 10,
            "cw_any": [True] * 4 + [False] * 6,
            "cw_inmet": [True] * 4 + [False] * 6,
        })
        resumo = pd.DataFrame({"station_code": ["A"], "total_events": [1]})
        return {"events": ev, "daily": daily, "summary": resumo}

    def test_intensity_class_toda_nula_nao_quebra(self):
        """float64 por ser toda NaN era a suspeita mais direta."""
        cs.sus_climate_plot_coldwaves(self._minimo([np.nan]), type="timeline")

    def test_intensity_class_de_texto_nao_quebra(self):
        cs.sus_climate_plot_coldwaves(
            self._minimo(["Severe (SCW)"]), type="timeline")
