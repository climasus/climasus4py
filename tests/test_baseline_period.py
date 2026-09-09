"""Data parcial no baseline de ondas de calor/frio (M14).

``pd.Timestamp`` le data parcial como o PRIMEIRO INSTANTE do periodo, entao
``baseline_start="2023"`` e ``baseline_end="2023"`` -- que qualquer pessoa
escreve querendo dizer "o ano de 2023" -- viravam os dois 2023-01-01, uma
janela de UM DIA. Nao ficava vazia, entao a checagem de "baseline vazio" nao
disparava, e o percentil saia de um unico dia. No INMET SP 2023 isso dava 135
eventos contra 245 do ano cheio: 45% de diferenca sem nada indicando.

O aviso de baseline curto existia, mas reportava ANOS DISTINTOS -- uma janela
de 2 dias aparecia como "1 ano", errando a severidade por um fator de ~365.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.utils.data import period_bound


# ---------------------------------------------------------------------------
# period_bound
# ---------------------------------------------------------------------------

class TestPeriodBound:
    @pytest.mark.parametrize(
        ("valor", "inicio", "fim"),
        [
            ("2023", "2023-01-01", "2023-12-31"),
            ("2023-06", "2023-06-01", "2023-06-30"),
            ("2024-02", "2024-02-01", "2024-02-29"),   # bissexto
            ("2023-06-15", "2023-06-15", "2023-06-15"),  # data cheia: 1 dia
        ],
    )
    def test_expands_to_the_period_it_names(self, valor, inicio, fim):
        assert str(period_bound(valor).date()) == inicio
        assert str(period_bound(valor, end=True).date()) == fim

    def test_complete_date_is_unchanged_either_way(self):
        """Data cheia nomeia um instante: nao ha o que expandir."""
        assert period_bound("2023-06-15") == period_bound("2023-06-15", end=True)

    def test_value_with_time_falls_back_to_timestamp(self):
        """Formato que Period recusa e Timestamp aceita nao pode levantar."""
        assert str(period_bound("2023-01-01 10:30:00").date()) == "2023-01-01"

    def test_result_is_normalised_to_midnight(self):
        """A borda final de um periodo e 23:59:59.999...; comparacao com
        date_day exige meia-noite, senao o ultimo dia entraria por acidente
        de fracao de segundo."""
        assert period_bound("2023", end=True).time() == pd.Timestamp("00:00:00").time()


# ---------------------------------------------------------------------------
# Efeito nas ondas de calor / frio
# ---------------------------------------------------------------------------

def _serie_sintetica() -> pd.DataFrame:
    """Quatro anos, com um surto de calor SO em 2023.

    O surto tem de ficar FORA da janela de baseline: o percentil vem de uma
    janela circular de dia-do-ano sobre o proprio baseline, entao uma onda
    dentro dele eleva o proprio limiar e nao e detectada. E por isso que a
    funcao recomenda 20 anos.
    """
    rng = np.random.default_rng(3)
    dias = pd.date_range("2020-01-01", "2023-12-31", freq="D")
    frames = []
    for estacao in ("A001", "A002"):
        base = 24 + 8 * np.sin(2 * np.pi * dias.dayofyear / 365)
        serie = pd.Series(base + rng.normal(0, 1.0, len(dias)), index=dias)
        serie.loc["2023-01-10":"2023-01-20"] += 12
        frames.append(pd.DataFrame({
            "station_code": estacao,
            "date": dias,
            "tair_dry_bulb_c": serie.values,
            "tair_max_c": serie.values + 4,
            "tair_min_c": serie.values - 4,
        }))
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def clima():
    return _serie_sintetica()


@pytest.fixture(scope="module")
def clima_com_lacunas():
    """Lacuna que realmente esvazia uma fatia do baseline. Custou duas tentativas.

    O gatilho e especifico: UMA coluna de temperatura ausente por um bloco
    CONTIGUO maior que a janela do baseline (31 dias, +/-15 em torno do
    dia-do-ano), com as OUTRAS colunas presentes. As outras presentes e que
    fazem a linha sobreviver ate o baseline; se faltasse toda temperatura, a
    linha seria descartada antes e a fatia sairia vazia (``size == 0``), caso
    que o codigo antigo ja tratava.

    Duas versoes anteriores deste fixture passavam com e sem a correcao, e
    portanto nao provavam nada: lacuna ALEATORIA de 35% (a chance de 31 dias
    seguidos serem todos NA e 0,35^31, ou seja nenhuma) e lacuna em TODAS as
    colunas (linha descartada antes). Medido: com o padrao abaixo sao 91
    RuntimeWarning sem a correcao e 0 com ela.
    """
    df = _serie_sintetica()
    sem_maxima = (df["station_code"] == "A002") & df["date"].dt.month.isin([3, 4, 5, 6])
    df.loc[sem_maxima, "tair_max_c"] = np.nan
    return df


def _eventos(df, inicio, fim, funcao=cs.sus_climate_compute_heatwaves):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return funcao(
            df, method=["WHO", "WMO"],
            baseline_start=inicio, baseline_end=fim, verbose=False,
        )["events"]


class TestBaselineParcialEquivaleAoPeriodoInteiro:
    def test_year_only_equals_the_full_year_dates(self, clima):
        parcial = _eventos(clima, "2020", "2022")
        completa = _eventos(clima, "2020-01-01", "2022-12-31")
        assert not parcial.empty, "fixture nao gerou evento; o teste nao provaria nada"
        assert parcial.equals(completa)

    def test_and_differs_from_the_truncated_window_the_bug_produced(self, clima):
        """A contrapartida: sem ela, o teste acima passaria com o bug intacto.

        O defeito transformava ("2020", "2022") em 2020-01-01..2022-01-01 --
        dois anos de baseline em vez de tres.
        """
        parcial = _eventos(clima, "2020", "2022")
        truncada = _eventos(clima, "2020-01-01", "2022-01-01")
        assert not parcial.equals(truncada)

    def test_coldwaves_too(self, clima):
        """As duas funcoes parseiam baseline do mesmo jeito e foram corrigidas juntas."""
        parcial = _eventos(clima, "2020", "2022", cs.sus_climate_compute_coldwaves)
        completa = _eventos(clima, "2020-01-01", "2022-12-31",
                            cs.sus_climate_compute_coldwaves)
        assert parcial.equals(completa)


class TestHigieneDeAvisos:
    """Os avisos uteis nao podem ficar soterrados em ruido do numpy (M61).

    Uma chamada sobre o INMET SP 2023 emitia 618 avisos, dos quais 608 eram
    ``RuntimeWarning: Mean of empty slice``. Os 10 restantes eram os que o
    usuario precisa ler. Isso importa mais do que parece: as correcoes de
    M21, M52, M58 e M14 tornaram defeitos silenciosos visiveis ACRESCENTANDO
    UserWarning -- e aviso util enterrado em 608 linhas de aviso inutil e
    aviso que ninguem le.
    """

    def test_no_runtime_warnings_from_numpy(self, clima_com_lacunas):
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_climate_compute_heatwaves(
                clima_com_lacunas, method=["WHO", "WMO", "INMET", "EHF"], verbose=False
            )
        ruido = [w for w in capturados if issubclass(w.category, RuntimeWarning)]
        assert not ruido, (
            f"{len(ruido)} RuntimeWarning; a primeira: {ruido[0].message}"
        )

    def test_no_pandas_deprecation_warnings(self, clima_com_lacunas):
        """As mascaras booleanas usavam fillna sobre object dtype, que o pandas
        deprecou -- o infer_objects() logo apos nao evitava o aviso porque ele
        nasce no proprio fillna. Trocado por .eq(True).

        SEM passar ``method``: e a lista default de sete que dispara isso,
        porque UTCI/WBGT/HI sao pedidos e suas colunas nao existem aqui, e sao
        essas colunas ausentes que produzem o dtype object. Com um subconjunto
        de metodos o aviso nao aparece nem antes da correcao, e o teste
        passaria sem provar nada.
        """
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_climate_compute_heatwaves(clima_com_lacunas, verbose=False)
        futuros = [w for w in capturados if issubclass(w.category, FutureWarning)]
        assert not futuros, [str(w.message)[:70] for w in futuros]

    def test_the_useful_warnings_survive(self, clima_com_lacunas):
        """Contrapartida: silenciar o ruido nao pode ter silenciado o sinal.

        Sem esta, a correcao passaria por um catch_warnings global que
        engolisse tudo -- que e exatamente o remedio errado.
        """
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_climate_compute_heatwaves(
                clima_com_lacunas, method=["WHO", "UTCI"], verbose=False
            )
        uteis = [str(w.message) for w in capturados
                 if issubclass(w.category, UserWarning)]
        assert any("NA" in m for m in uteis), uteis
        assert any("UTCI" in m for m in uteis), uteis

    def test_results_unchanged_by_the_silencing(self, clima):
        """O conserto e de ruido, nao de calculo: a contagem nao pode mudar.

        _nanmean passou a checar all-NaN antes de chamar nanmean -- que
        devolvia NaN de todo jeito, so avisando. Mesmo resultado.
        """
        eventos = _eventos(clima, "2020", "2022")
        assert eventos.shape[0] == 6, eventos.shape


class TestAvisoDeBaselineCurto:
    @pytest.mark.parametrize("lang", ["pt", "en"])
    def test_reports_days_not_only_years(self, clima, lang):
        """Janela de 2 dias tem de aparecer como 2 dias, nao como "1 ano"."""
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_climate_compute_heatwaves(
                clima, method=["WHO"], baseline_start="2020-06-01",
                baseline_end="2020-06-02", lang=lang, verbose=False,
            )
        curtos = [
            str(w.message) for w in capturados
            if "baseline curto" in str(w.message) or "Short baseline" in str(w.message)
        ]
        assert curtos, "nenhum aviso de baseline curto"
        assert "2 dia" in curtos[0] or "2 day" in curtos[0], curtos[0]

    def test_full_year_reports_365_days(self, clima):
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_climate_compute_heatwaves(
                clima, method=["WHO"], baseline_start="2020", baseline_end="2020",
                lang="pt", verbose=False,
            )
        curtos = [s for s in (str(w.message) for w in capturados) if "baseline curto" in s]
        assert curtos and "366 dia" in curtos[0], curtos
