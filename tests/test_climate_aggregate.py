"""sus_climate_aggregate — integracao clima+saude por estrategia temporal.

REESCRITO em 15/09/2026. Os testes anteriores exercitavam uma funcao de
FORMA DIFERENTE: um agregador so de clima, com `time_resolution=` e
`stats=`, recebendo uma relacao so. A funcao atual e a do R -- recebe
`health_data` E `climate_data` e integra as duas por uma de dez
estrategias temporais. Nao havia renome possivel; os 14 testes foram
substituidos.

A reescrita estava registrada no M53 e ja anotada em test_coverage_boost
como pendente. Ela expos o M96: o validador aceitava nomes de coluna que
o detector nao resolvia.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate_aggregate import (
    _DATE_CANDIDATES,
    _MUNI_CANDIDATES,
)
from climasus4py.utils.data import detect_date_column, detect_geo_column

#: Quadrado simples em torno da estacao sintetica.
WKT = "POLYGON((-47 -23,-46 -23,-46 -22,-47 -22,-47 -23))"

#: As dez estrategias que a funcao aceita, e o que cada uma exige a mais.
ESTRATEGIAS = {
    "exact": {},
    "moving_window": {"window_days": 7},
    "discrete_lag": {"lag_days": [0, 7]},
    "distributed_lag": {"lag_days": [0, 7]},
    "offset_window": {"offset_days": [3, 10]},
    "degree_days": {"window_days": 7, "temp_base": 18.0},
    "threshold_exceedance": {"window_days": 7, "threshold_value": 25.0},
    "cold_wave_exceedance": {"window_days": 7, "threshold_value": 15.0},
    "weighted_window": {"window_days": 7},
    "seasonal": {"min_days": 1},
}


def _rel(df: pd.DataFrame):
    return get_connection().from_df(df)


@pytest.fixture(scope="module")
def clima():
    """Quatro meses de serie diaria numa estacao, com ciclo suave."""
    dias = pd.date_range("2022-12-01", "2023-03-31", freq="D")
    return _rel(pd.DataFrame({
        "station_code": ["A701"] * len(dias),
        "latitude": [-22.5] * len(dias),
        "longitude": [-46.5] * len(dias),
        "date": dias,
        "tair_dry_bulb_c": 20 + 5 * np.sin(np.arange(len(dias)) / 10),
        "tair_max_c": 25 + 5 * np.sin(np.arange(len(dias)) / 10),
        "tair_min_c": 15 + 5 * np.sin(np.arange(len(dias)) / 10),
        "rh_mean_porc": [70.0] * len(dias),
    }))


@pytest.fixture(scope="module")
def saude():
    """Saude com geometria, como sai do sus_spatial_join."""
    return _rel(pd.DataFrame({
        "CODMUNRES": ["3550308"] * 5,
        "DTOBITO": pd.to_datetime([
            "2023-01-05", "2023-01-12", "2023-01-20",
            "2023-02-03", "2023-02-14",
        ]),
        "geometry_wkt": [WKT] * 5,
    }))


class TestEstrategiasTemporais:
    """As dez estrategias rodam e preservam as linhas de saude."""

    @pytest.mark.parametrize("estrategia", sorted(ESTRATEGIAS))
    def test_cada_estrategia_roda(self, saude, clima, estrategia):
        out = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy=estrategia,
            verbose=False, **ESTRATEGIAS[estrategia],
        )
        df = out.df()

        assert len(df) == 5
        assert "CODMUNRES" in df.columns
        assert "DTOBITO" in df.columns

    def test_exact_traz_o_valor_do_proprio_dia(self, saude, clima):
        """A estrategia 'exact' casa data com data, sem janela."""
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="exact", verbose=False
        ).df()
        serie = clima.df().set_index(clima.df()["date"].dt.date)

        for _, linha in df.iterrows():
            esperado = serie.loc[linha["DTOBITO"].date(), "tair_dry_bulb_c"]
            assert linha["tair_dry_bulb_c"] == pytest.approx(esperado)

    def test_a_janela_movel_suaviza(self, saude, clima):
        """A media de 7 dias nao pode cair fora do intervalo da serie."""
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="moving_window",
            window_days=7, verbose=False,
        ).df()
        col = next(c for c in df.columns if "tair_dry_bulb_c" in c)
        bruto = clima.df()["tair_dry_bulb_c"]

        assert df[col].min() >= bruto.min()
        assert df[col].max() <= bruto.max()

    def test_defasagem_discreta_cria_uma_coluna_por_lag(self, saude, clima):
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="discrete_lag",
            lag_days=[0, 7, 14], verbose=False,
        ).df()

        assert any("lag0" in c or "lag_0" in c for c in df.columns)
        assert any("lag14" in c or "lag_14" in c for c in df.columns)

    def test_graus_dia_nunca_e_negativo(self, saude, clima):
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="degree_days",
            window_days=7, temp_base=18.0, verbose=False,
        ).df()
        col = next((c for c in df.columns if "degree" in c or "gdd" in c), None)

        assert col is not None
        assert (df[col].dropna() >= 0).all()

    def test_excedencia_conta_dias_e_proporcao(self, saude, clima):
        """A funcao devolve `nexcN_gtXpY_<var>` (contagem) e `pexc...`
        (proporcao). O limiar 22 fica no meio da serie, que vai de 15 a
        25 -- com 25 a contagem seria zero em toda linha e o teste nao
        diria nada.
        """
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="threshold_exceedance",
            window_days=7, threshold_value=22.0,
            climate_vars=["tair_dry_bulb_c"], verbose=False,
        ).df()
        n_col = "nexc7_gt22p0_tair_dry_bulb_c"
        p_col = "pexc7_gt22p0_tair_dry_bulb_c"

        assert n_col in df.columns and p_col in df.columns
        assert df[n_col].dropna().between(0, 8).all()
        assert df[p_col].dropna().between(0, 1).all()
        assert df[n_col].max() > 0            # o limiar de fato e cruzado

    def test_a_proporcao_e_a_contagem_sobre_a_janela(self, saude, clima):
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="threshold_exceedance",
            window_days=7, threshold_value=22.0,
            climate_vars=["tair_dry_bulb_c"], verbose=False,
        ).df()
        n = df["nexc7_gt22p0_tair_dry_bulb_c"]
        p = df["pexc7_gt22p0_tair_dry_bulb_c"]
        valido = n.notna() & p.notna() & (p > 0)

        assert (n[valido] / p[valido]).round(6).nunique() == 1


class TestValidacao:
    def test_recusa_saude_que_nao_e_relacao(self, clima):
        with pytest.raises(TypeError, match="health_data"):
            cs.sus_climate_aggregate(pd.DataFrame({"a": [1]}), clima, verbose=False)

    def test_recusa_saude_sem_geometria(self, clima):
        rel = _rel(pd.DataFrame({
            "CODMUNRES": ["3550308"], "DTOBITO": pd.to_datetime(["2023-01-05"]),
        }))
        with pytest.raises(ValueError, match="geometry_wkt"):
            cs.sus_climate_aggregate(rel, clima, verbose=False)

    def test_recusa_saude_sem_municipio(self, clima):
        rel = _rel(pd.DataFrame({
            "DTOBITO": pd.to_datetime(["2023-01-05"]), "geometry_wkt": [WKT],
        }))
        with pytest.raises(ValueError, match="municipality"):
            cs.sus_climate_aggregate(rel, clima, verbose=False)

    def test_recusa_clima_vazio(self, saude):
        vazio = _rel(pd.DataFrame({
            "station_code": [], "latitude": [], "longitude": [],
            "date": pd.to_datetime([]), "tair_dry_bulb_c": [],
        }))
        with pytest.raises(ValueError, match="empty"):
            cs.sus_climate_aggregate(saude, vazio, verbose=False)

    def test_recusa_clima_sem_estacao(self, saude):
        sem = _rel(pd.DataFrame({
            "latitude": [-22.5], "longitude": [-46.5],
            "date": pd.to_datetime(["2023-01-05"]), "tair_dry_bulb_c": [22.0],
        }))
        with pytest.raises(ValueError, match="station"):
            cs.sus_climate_aggregate(saude, sem, verbose=False)

    def test_recusa_estrategia_invalida(self, saude, clima):
        with pytest.raises(ValueError, match="temporal_strategy|strategy"):
            cs.sus_climate_aggregate(
                saude, clima, temporal_strategy="mensal", verbose=False
            )

    @pytest.mark.parametrize(("estrategia", "faltando"), [
        ("moving_window", "window_days"),
        ("discrete_lag", "lag_days"),
        ("offset_window", "offset_days"),
    ])
    def test_estrategia_sem_o_parametro_obrigatorio(
        self, saude, clima, estrategia, faltando
    ):
        with pytest.raises(ValueError, match=faltando):
            cs.sus_climate_aggregate(
                saude, clima, temporal_strategy=estrategia, verbose=False
            )


class TestDetectorBateComOValidador:
    """O validador aceitava nomes que o detector nao resolvia (M96).

    O `_validate_health_data` conferia a coluna de municipio contra
    `_MUNI_CANDIDATES` e a de data contra `_DATE_CANDIDATES`, mas o join
    pedia a coluna ao `detect_geo_column` / `detect_date_column`, cujas
    listas eram OUTRAS. Tres nomes de municipio (`code_muni`,
    `notification_municipality_code`, `MUNI_RES`) e dois de data
    (`DT_NOTIFIC`, `DT_INTER`) passavam na validacao e devolviam None na
    deteccao -- o None ia direto para o SQL e a funcao morria com
    `Binder Error: Values list "h" does not have a column named "None"`,
    que nao nomeia a coluna culpada nem sugere o que fazer.

    Importava de verdade: `DT_NOTIFIC` e a data de notificacao do SINAN,
    que e o caminho de dengue -- o caso de uso principal de uma
    biblioteca de clima e saude. `DT_INTER` e a de internacao do SIH.

    Corrigido acrescentando os nomes no FIM de cada lista, sem mexer na
    precedencia existente. Os de data moraram sempre no climasus-data
    (`role_priority.date`); os de municipio estao hardcoded no
    `detect_geo_column`, que ignora o `role_priority.municipality` -- duas
    fontes de verdade para a mesma coisa, o que continua registrado.
    """

    @pytest.mark.parametrize("coluna", _MUNI_CANDIDATES)
    def test_todo_municipio_aceito_e_detectavel(self, coluna):
        assert detect_geo_column([coluna], level="municipality") == coluna

    @pytest.mark.parametrize("coluna", _DATE_CANDIDATES)
    def test_toda_data_aceita_e_detectavel(self, coluna):
        assert detect_date_column([coluna]) == coluna

    @pytest.mark.parametrize(("muni", "data"), [
        ("CODMUNRES", "DTOBITO"),                        # ja funcionava
        ("CODMUNRES", "DT_NOTIFIC"),                     # SINAN
        ("MUNI_RES", "DT_INTER"),                        # SIH
        ("code_muni", "DTOBITO"),
        ("notification_municipality_code", "DT_NOTIFIC"),
    ])
    def test_as_combinacoes_que_quebravam(self, clima, muni, data):
        rel = _rel(pd.DataFrame({
            muni: ["3550308"] * 3,
            data: pd.to_datetime(["2023-01-05", "2023-01-12", "2023-02-03"]),
            "geometry_wkt": [WKT] * 3,
        }))
        df = cs.sus_climate_aggregate(
            rel, clima, temporal_strategy="exact", verbose=False
        ).df()

        assert len(df) == 3
        assert df["tair_dry_bulb_c"].notna().all()


class TestSaida:
    def test_devolve_relacao_lazy(self, saude, clima):
        import duckdb

        out = cs.sus_climate_aggregate(saude, clima, verbose=False)

        assert isinstance(out, duckdb.DuckDBPyRelation)

    def test_preserva_as_colunas_de_saude(self, saude, clima):
        antes = set(saude.columns)
        depois = set(cs.sus_climate_aggregate(saude, clima, verbose=False).columns)

        assert antes <= depois

    def test_climate_vars_restringe(self, saude, clima):
        df = cs.sus_climate_aggregate(
            saude, clima, temporal_strategy="exact",
            climate_vars=["tair_dry_bulb_c"], verbose=False,
        ).df()

        assert "tair_dry_bulb_c" in df.columns
        assert "rh_mean_porc" not in df.columns

    def test_verbose_nao_levanta(self, saude, clima, capsys):
        cs.sus_climate_aggregate(saude, clima, verbose=True)

        assert capsys.readouterr().out or capsys.readouterr().err

    def test_sus_climate_info_le_o_resultado(self, saude, clima):
        out = cs.sus_climate_aggregate(saude, clima, verbose=False)
        cs.sus_climate_info(out)   # nao deve levantar
