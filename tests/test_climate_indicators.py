"""Tests for sus_climate_compute_indicators — bioclimatic indicators (B.2)."""

from __future__ import annotations

import math
import warnings
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.climate_indicators import (
    sus_climate_compute_indicators,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_full_inmet_rel(n: int = 20) -> duckdb.DuckDBPyRelation:
    """Synthetic INMET relation with all 17 canonical variables."""
    import numpy as np

    rng = np.random.default_rng(0)
    dates = pd.date_range("2023-06-01", periods=n, freq="D")
    df = pd.DataFrame(
        {
            "station_code": ["A701"] * n,
            "date": dates,
            # Temperature
            "tair_dry_bulb_c": rng.uniform(25.0, 32.0, n),
            "tair_max_c": rng.uniform(33.0, 38.0, n),
            "tair_min_c": rng.uniform(18.0, 24.0, n),
            # Humidity
            "rh_mean_porc": rng.uniform(55.0, 85.0, n),
            # Dew point
            "dew_tmean_c": rng.uniform(16.0, 22.0, n),
            # Wind
            "ws_2_m_s": rng.uniform(0.5, 4.0, n),
            # Solar
            "sr_kj_m2": rng.uniform(500.0, 2200.0, n),
        }
    )
    return get_connection().from_df(df)


@pytest.fixture
def full_rel():
    return _make_full_inmet_rel()


@pytest.fixture
def full_df():
    """Same data as a DataFrame."""
    return _make_full_inmet_rel().df()


# ---------------------------------------------------------------------------
# Core: returns DuckDBPyRelation (lazy)
# ---------------------------------------------------------------------------


def test_returns_duckdb_relation(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


def test_accepts_dataframe_input(full_df):
    result = sus_climate_compute_indicators(full_df, verbose=False)
    assert type(result).__name__ == "DuckDBPyRelation"


# ---------------------------------------------------------------------------
# Individual indicators
# ---------------------------------------------------------------------------


def test_heat_index_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["heat_index"], verbose=False
    )
    df = result.df()
    assert "hi_c" in df.columns


def test_thi_column_present(full_rel):
    result = sus_climate_compute_indicators(full_rel, indicators=["thi"], verbose=False)
    df = result.df()
    assert "thi_c" in df.columns


def test_apparent_temperature_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["apparent_temperature"], verbose=False
    )
    df = result.df()
    assert "at_c" in df.columns


def test_vapor_pressure_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["vapor_pressure"], verbose=False
    )
    df = result.df()
    assert "vapor_pressure_kpa" in df.columns
    # All values must be positive (vapor pressure is always > 0)
    assert (result.df()["vapor_pressure_kpa"] > 0).all()


def test_dew_point_depression_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["dew_point_depression"], verbose=False
    )
    df = result.df()
    assert "dpd_c" in df.columns


def test_diurnal_range_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["diurnal_range"], verbose=False
    )
    df = result.df()
    assert "diurnal_range_c" in df.columns
    # DTR must be ≥ 0 (max − min of same series)
    assert (df["diurnal_range_c"] >= 0).all()


def test_consecutive_hot_days_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["consecutive_hot_days"], verbose=False
    )
    df = result.df()
    assert "consecutive_hot_days" in df.columns
    # CHD is a true run length (not a 7-day rolling count) — values are
    # non-negative integers, no upper bound from the window.
    assert (df["consecutive_hot_days"] >= 0).all()
    assert df["consecutive_hot_days"].dtype.kind in ("i", "u")


def test_heat_wave_column_present(full_rel):
    result = sus_climate_compute_indicators(
        full_rel, indicators=["heat_wave"], verbose=False
    )
    df = result.df()
    assert "heat_wave" in df.columns
    # Binary: 0 or 1
    assert df["heat_wave"].isin([0, 1]).all()


# ---------------------------------------------------------------------------
# BUG-01 regression: CHD must report TRUE run lengths (not 7-day rolling sum)
# ---------------------------------------------------------------------------


def test_chd_returns_true_run_length_alternating_pattern():
    """Sequence Q-F-Q-Q-Q-Q-Q (Q = Tmax > 32, F = Tmax <= 32):

    CHD on the last hot day must be 5 (length of the trailing run),
    not 6 (count of hot days within the 7-day window).
    """
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    days = pd.date_range("2023-01-01", periods=7, freq="D")
    # Q F Q Q Q Q Q
    tmax = [33.0, 25.0, 33.0, 33.0, 33.0, 33.0, 33.0]
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 7,
        "date": days,
        "tair_max_c": tmax,
        "tair_dry_bulb_c": tmax,
        "rh_mean_porc": [50.0] * 7,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["consecutive_hot_days"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    # day 0 (Q) → run length 1
    # day 1 (F) → 0
    # days 2..6 (Q×5) → 1, 2, 3, 4, 5
    expected = [1, 0, 1, 2, 3, 4, 5]
    assert out["consecutive_hot_days"].tolist() == expected


def test_chd_zero_when_not_hot():
    """Tmax <= 32 → CHD = 0."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 3,
        "date": pd.date_range("2023-01-01", periods=3, freq="D"),
        "tair_max_c": [25.0, 30.0, 32.0],  # all <= 32
        "tair_dry_bulb_c": [25.0, 30.0, 32.0],
        "rh_mean_porc": [50.0] * 3,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["consecutive_hot_days"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    assert out["consecutive_hot_days"].tolist() == [0, 0, 0]


# ---------------------------------------------------------------------------
# BUG-02 regression: heat_wave must flag ALL days of a run (not skip the
# first two days of the episode)
# ---------------------------------------------------------------------------


def test_heat_wave_flags_all_three_days_of_episode():
    """A 3-day run of Tmax > 35 must produce heat_wave = 1 on ALL THREE days."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 5,
        "date": pd.date_range("2023-01-01", periods=5, freq="D"),
        # cool, hot, hot, hot, cool — the 3 hot days are an episode
        "tair_max_c": [30.0, 36.0, 36.0, 36.0, 30.0],
        "tair_dry_bulb_c": [30.0, 36.0, 36.0, 36.0, 30.0],
        "rh_mean_porc": [50.0] * 5,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_wave"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    # Days 1, 2, 3 are all part of the 3-day run → all flagged
    assert out["heat_wave"].tolist() == [0, 1, 1, 1, 0]


def test_heat_wave_does_not_flag_two_day_run():
    """Run of only 2 hot days → no heat_wave."""
    import pandas as pd

    from climasus4py.core.engine import get_connection

    conn = get_connection()
    rel = conn.from_df(pd.DataFrame({
        "station_code": ["A"] * 4,
        "date": pd.date_range("2023-01-01", periods=4, freq="D"),
        "tair_max_c": [30.0, 36.0, 36.0, 30.0],
        "tair_dry_bulb_c": [30.0, 36.0, 36.0, 30.0],
        "rh_mean_porc": [50.0] * 4,
    }))
    out = sus_climate_compute_indicators(
        rel, indicators=["heat_wave"], verbose=False
    ).df().sort_values("date").reset_index(drop=True)
    assert out["heat_wave"].tolist() == [0, 0, 0, 0]


# ---------------------------------------------------------------------------
# BUG-04 regression: heat_index returns NULL outside the Rothfusz domain
# ---------------------------------------------------------------------------


def _rel_hi(temps, rhs):
    import pandas as pd

    from climasus4py.core.engine import get_connection

    return get_connection().from_df(pd.DataFrame({
        "station_code": ["A"] * len(temps),
        "date": pd.date_range("2023-01-01", periods=len(temps), freq="D"),
        "tair_dry_bulb_c": temps,
        "rh_mean_porc": rhs,
        "tair_max_c": [t + 5 for t in temps],
    }))


def test_heat_index_masked_outside_the_domain_when_region_is_none():
    """Fora do dominio de Rothfusz, hi_c = NULL -- com region="none".

    A mascara de validade so atua nessa configuracao, porque o R computa
    `apply_validity_mask && !use_region` e qualquer regiao diferente de
    "none" a desliga (M83).
    """
    import pandas as pd

    out = sus_climate_compute_indicators(
        _rel_hi([20.0, 35.0, 35.0], [60.0, 25.0, 60.0]),
        indicators=["heat_index"], region="none", verbose=False,
    ).df()

    assert pd.isna(out["hi_c"].iloc[0])   # 20 C: frio demais
    assert pd.isna(out["hi_c"].iloc[1])   # 25% RH: seco demais
    assert pd.notna(out["hi_c"].iloc[2])  # 35 C / 60%: dentro


def test_heat_index_extrapolates_only_on_the_r_path():
    """No caminho do R a mascara fica DESLIGADA e o indice extrapola (M83).

    O resultado e um indice de CALOR abaixo da temperatura do ar -- 18,48 C
    para T=20 e RH=90. E o que o R devolve, porque ele computa
    `apply_validity_mask && !use_region` e o default `region="auto"` zera o
    segundo termo.

    Desde 15/09/2026 o Python NAO replica esse acoplamento por default: os
    valores nao tem significado fisico, entao entregar isso sem pedir seria
    um default sabidamente ruim. A chave de modulo continua reproduzindo o
    R exato, e e o que este teste usa.
    """
    from climasus4py.enrichment import climate_indicators as mod

    anterior = mod.R_COUPLES_MASK_TO_REGION
    mod.R_COUPLES_MASK_TO_REGION = True
    try:
        out = sus_climate_compute_indicators(
            _rel_hi([20.0, 35.0], [90.0, 25.0]),
            indicators=["heat_index"], verbose=False,
        ).df()
    finally:
        mod.R_COUPLES_MASK_TO_REGION = anterior

    assert out["hi_c"].notna().all()
    assert float(out["hi_c"].iloc[0]) == pytest.approx(18.48, abs=0.01)
    assert float(out["hi_c"].iloc[0]) < 20.0   # "calor" abaixo do ar


def test_heat_index_e_mascarado_no_default_do_python():
    """A contrapartida: sem pedir nada, o valor fora de dominio vira NULL."""
    out = sus_climate_compute_indicators(
        _rel_hi([20.0, 35.0], [90.0, 25.0]),
        indicators=["heat_index"], verbose=False,
    ).df()

    assert pd.isna(out["hi_c"].iloc[0])    # T=20: frio demais para o HI
    assert pd.isna(out["hi_c"].iloc[1])    # RH=25: seco demais


def test_wbgt_indicator_available_and_returns_column(full_rel):
    """WBGT documented in module — must exist and produce wbgt_c."""
    out = sus_climate_compute_indicators(
        full_rel, indicators=["wbgt"], verbose=False
    ).df()
    assert "wbgt_c" in out.columns
    # WBGT (simplified outdoor) should fall in a plausible thermal range
    valid = out["wbgt_c"].dropna()
    assert (valid > -10).all() and (valid < 50).all()


# ---------------------------------------------------------------------------
# "all" indicators (default)
# ---------------------------------------------------------------------------


def test_all_indicators_default(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    df = result.df()
    expected_output_cols = [
        "hi_c", "thi_c", "at_c", "vapor_pressure_kpa", "dpd_c",
        "diurnal_range_c", "consecutive_hot_days", "heat_wave",
    ]
    for col in expected_output_cols:
        assert col in df.columns, f"Missing output column: {col}"


def test_all_indicators_preserves_original_columns(full_rel):
    result = sus_climate_compute_indicators(full_rel, verbose=False)
    df = result.df()
    assert "tair_dry_bulb_c" in df.columns
    assert "station_code" in df.columns


# ---------------------------------------------------------------------------
# Combinations
# ---------------------------------------------------------------------------


def test_subset_of_indicators(full_rel):
    result = sus_climate_compute_indicators(
        full_rel,
        indicators=["heat_index", "vapor_pressure"],
        verbose=False,
    )
    df = result.df()
    assert "hi_c" in df.columns
    assert "vapor_pressure_kpa" in df.columns
    # Other indicators NOT present
    assert "thi_c" not in df.columns
    assert "at_c" not in df.columns


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_unknown_indicator_raises(full_rel):
    with pytest.raises(ValueError, match="Unknown indicator code"):
        sus_climate_compute_indicators(full_rel, indicators=["mode"], verbose=False)


def test_missing_required_column_raises():
    """Requesting 'apparent_temperature' without ws_2_m_s → clear error."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"] * 5,
            "date": pd.date_range("2023-01-01", periods=5),
            "tair_dry_bulb_c": [28.0] * 5,
            "rh_mean_porc": [70.0] * 5,
            # ws_2_m_s is intentionally missing
        }
    )
    rel = conn.from_df(df)
    with pytest.raises(ValueError, match="ws_2_m_s"):
        sus_climate_compute_indicators(
            rel, indicators=["apparent_temperature"], verbose=False
        )


# ---------------------------------------------------------------------------
# Numeric sanity checks
# ---------------------------------------------------------------------------


def test_heat_index_value_sanity():
    """HI at T=30, RH=80 should be roughly 35-40°C (hot and humid)."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"],
            "date": pd.to_datetime(["2023-07-15"]),
            "tair_dry_bulb_c": [30.0],
            "rh_mean_porc": [80.0],
        }
    )
    rel = conn.from_df(df)
    result = sus_climate_compute_indicators(
        rel, indicators=["heat_index"], verbose=False
    )
    hi = result.df()["hi_c"].iloc[0]
    assert 30.0 < hi < 50.0, f"Unexpected HI value: {hi}"


def test_vapor_pressure_sanity():
    """At T=25°C, RH=60% → e ≈ 1.90 kPa (within 10% tolerance)."""
    conn = get_connection()
    df = pd.DataFrame(
        {
            "station_code": ["A701"],
            "date": pd.to_datetime(["2023-01-01"]),
            "tair_dry_bulb_c": [25.0],
            "rh_mean_porc": [60.0],
        }
    )
    rel = conn.from_df(df)
    result = sus_climate_compute_indicators(
        rel, indicators=["vapor_pressure"], verbose=False
    )
    vp = result.df()["vapor_pressure_kpa"].iloc[0]
    expected = 0.60 * 0.6108 * math.exp(17.27 * 25 / (25 + 237.3))
    assert abs(vp - expected) / expected < 0.01, f"VP={vp}, expected~{expected:.4f}"


# ---------------------------------------------------------------------------
# Verbose output
# ---------------------------------------------------------------------------


def test_verbose_does_not_raise(full_rel, capsys):
    sus_climate_compute_indicators(full_rel, indicators=["heat_index"], verbose=True, lang="en")
    out = capsys.readouterr().out
    assert "sus_climate_compute_indicators" in out


# ---------------------------------------------------------------------------
# verify_physics (M12)
# ---------------------------------------------------------------------------


def _rel_sr(sr_values, latitude=-23.5, start="2020-01-01"):
    """Relation with a solar-radiation column and an hourly timestamp."""
    n = len(sr_values)
    df = pd.DataFrame({
        "station_code": ["A701"] * n,
        "date": pd.date_range(start, periods=n, freq="h"),
        "tair_dry_bulb_c": [28.0] * n,
        "rh_mean_porc": [60.0] * n,
        "ws_2_m_s": [2.0] * n,
        "sr_kj_m2": sr_values,
        "latitude": [latitude] * n,
    })
    return get_connection().from_df(df)


class TestVerifyPhysics:
    """`verify_physics` mirrors R's `.verify_solar_radiation`.

    The helper does two things and the first one EDITS the data - which the
    M28 note originally got wrong when it said R "never edits the data":

    1. negative `sr_kj_m2` clamped to 0, nulls preserved
    2. a count of values above 110% of the extraterrestrial irradiance,
       warned about and otherwise left alone

    Parity measured against R on a 4.000-row input carrying 39 negatives, 25
    nulls and 30 impossible readings: clamping identical (0 negatives, 39
    zeros, 25 nulls on both sides) and the warning count identical at 2.347.

    Pinning the timezone was necessary to measure it at all: R's
    `arrow::read_parquet` returns POSIXct with `tzone=NULL`, which localises
    to America/Sao_Paulo, while DuckDB keeps the naive timestamp. That 3-hour
    shift alone moved the count from 2.339 to 2.347.
    """

    def test_negativo_vira_zero(self):
        rel = _rel_sr([-100.0, 500.0, -0.5, 1200.0])
        out = sus_climate_compute_indicators(
            rel, indicators=["wbgt"], verbose=False).df()
        assert out["sr_kj_m2"].tolist() == [0.0, 500.0, 0.0, 1200.0]

    def test_nulo_e_preservado(self):
        rel = _rel_sr([-100.0, None, 500.0])
        out = sus_climate_compute_indicators(
            rel, indicators=["wbgt"], verbose=False).df()
        sr = out["sr_kj_m2"]
        assert sr.iloc[0] == 0.0
        assert pd.isna(sr.iloc[1]), "nulo nao pode virar zero: e ausencia, nao medicao"
        assert sr.iloc[2] == 500.0

    def test_desligado_preserva_o_negativo(self):
        rel = _rel_sr([-100.0, 500.0])
        out = sus_climate_compute_indicators(
            rel, indicators=["wbgt"], verify_physics=False, verbose=False).df()
        assert out["sr_kj_m2"].tolist() == [-100.0, 500.0]

    def test_avisa_acima_de_110_por_cento(self):
        # 9000 kJ/m2 excede a irradiancia extraterrestre em qualquer hora
        rel = _rel_sr([9000.0] * 6)
        with pytest.warns(UserWarning, match="110"):
            sus_climate_compute_indicators(
                rel, indicators=["wbgt"], verbose=True).df()

    def test_nao_avisa_com_valor_plausivel_de_dia(self):
        # meio-dia UTC de janeiro em -23.5: G0 e alto, 1500 nao excede
        rel = _rel_sr([1500.0], start="2020-01-15 12:00")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sus_climate_compute_indicators(
                rel, indicators=["wbgt"], verbose=True).df()

    def test_silencioso_quando_verbose_false(self):
        rel = _rel_sr([9000.0] * 6)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sus_climate_compute_indicators(
                rel, indicators=["wbgt"], verbose=False).df()

    def test_latitude_nula_nao_conta_excedencia(self):
        """A armadilha do GREATEST, que ja custou dois defeitos neste modulo.

        R faz `pmax(sin_elev, 0)` e propaga NA; o GREATEST do DuckDB IGNORA
        NULL e devolveria 0, o que poria G0 em zero e faria QUALQUER leitura
        positiva contar como excedencia. Com latitude nula o R nao conta
        nenhuma, e e isso que este teste exige.
        """
        rel = _rel_sr([9000.0] * 6, latitude=None)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sus_climate_compute_indicators(
                rel, indicators=["wbgt"], verbose=True).df()

    def test_sem_coluna_de_radiacao_nao_quebra(self):
        df = pd.DataFrame({
            "station_code": ["A701"] * 3,
            "date": pd.date_range("2020-01-01", periods=3, freq="h"),
            "tair_dry_bulb_c": [28.0, 29.0, 30.0],
            "rh_mean_porc": [60.0, 61.0, 62.0],
        })
        rel = get_connection().from_df(df)
        out = sus_climate_compute_indicators(
            rel, indicators=["heat_index"], verbose=True).df()
        assert len(out) == 3

    def test_grampeamento_alimenta_o_indicador(self):
        """O grampeamento tem de chegar ao calculo, nao so a coluna de saida."""
        neg = _rel_sr([-500.0])
        zero = _rel_sr([0.0])
        a = sus_climate_compute_indicators(
            neg, indicators=["wbgt"], verbose=False).df()["wbgt_c"].iloc[0]
        b = sus_climate_compute_indicators(
            zero, indicators=["wbgt"], verbose=False).df()["wbgt_c"].iloc[0]
        assert a == b, "com sr grampeado em 0 o indicador tem de bater com sr=0"


# ---------------------------------------------------------------------------
# station metadata join (M106) and the row multiplication R does (M107)
# ---------------------------------------------------------------------------

_META_COLS = (
    "region", "federal_unit", "station_name", "latitude", "longitude",
    "altitude", "foundation_date", "id_link", "zona_climatica",
    "tipo_umidade", "distr_umidade", "temperatura_id", "descricao",
)


def _rel_estacao(codigo: str, n: int = 24, extra: dict | None = None):
    df = pd.DataFrame({
        "station_code": [codigo] * n,
        "date": pd.date_range("2020-01-01", periods=n, freq="h"),
        "tair_dry_bulb_c": [28.0] * n,
        "rh_mean_porc": [60.0] * n,
        "ws_2_m_s": [2.0] * n,
        "sr_kj_m2": [800.0] * n,
    })
    for k, v in (extra or {}).items():
        df[k] = v
    return get_connection().from_df(df)


def _tem_metadado() -> bool:
    from climasus4py.utils.data import data_path
    return data_path("assets/climate/inmet_station_meta.parquet").is_file()


metadado = pytest.mark.skipif(
    not _tem_metadado(),
    reason="climasus-data sem assets/climate/inmet_station_meta.parquet",
)


@metadado
class TestMetadadoDeEstacao:
    """R fecha a funcao com um left join na tabela de estacoes.

    Sao 13 das 60 colunas que ele devolve por padrao. O join e
    INCONDICIONAL -- use_cache e cache_dir so decidem se o download e
    guardado, nunca se o join acontece -- e a chave e o nome literal
    station_code, nao o argumento station_col.

    Os 13 valores foram conferidos contra o R para a estacao A701
    (SAO PAULO - MIRANTE): identicos, inclusive os acentuados.
    """

    def test_acrescenta_as_treze_colunas(self):
        out = sus_climate_compute_indicators(
            _rel_estacao("A701"), indicators=["wbgt"], verbose=False).df()
        faltando = [c for c in _META_COLS if c not in out.columns]
        assert not faltando, faltando

    def test_valores_conferidos_contra_o_r(self):
        out = sus_climate_compute_indicators(
            _rel_estacao("A701"), indicators=["wbgt"], verbose=False).df()
        linha = out.iloc[0]
        assert linha["region"] == "SE"
        assert linha["federal_unit"] == "SP"
        assert linha["station_name"] == "SAO PAULO - MIRANTE"
        assert linha["latitude"] == pytest.approx(-23.48333333)
        assert linha["longitude"] == pytest.approx(-46.61666666)
        assert linha["zona_climatica"] == "Tropical Brasil Central"
        assert linha["tipo_umidade"] == "super-úmido"
        assert linha["distr_umidade"] == "subseca"

    def test_estacao_desconhecida_nao_perde_linha(self):
        out = sus_climate_compute_indicators(
            _rel_estacao("ZZZZ", n=20), indicators=["wbgt"], verbose=False).df()
        assert len(out) == 20, "left join, nao inner: a observacao tem de ficar"
        assert out["zona_climatica"].isna().all()

    def test_nao_multiplica_linha_onde_o_r_multiplica(self):
        """M107: o R dobra as linhas de 26 das 636 estacoes.

        A tabela de origem do R tem 636 linhas para 609 estacoes, e nenhum
        dos 26 codigos repetidos e duplicata de verdade: cada par discorda
        da classificacao climatica, porque o ponto da estacao caiu na
        fronteira dos poligonos. Medido no R: 100 linhas de entrada para a
        A134 voltam 200, e toda contagem, media ou soma sobre essas
        estacoes sai dobrada, em silencio.

        O climasus-data entrega a tabela colapsada em uma linha por
        estacao, entao este join nao pode multiplicar.
        """
        out = sus_climate_compute_indicators(
            _rel_estacao("A134", n=100), indicators=["wbgt"], verbose=False).df()
        assert len(out) == 100, (
            "o join multiplicou linhas de observacao -- e o defeito do R que "
            "este porte recusa de proposito"
        )

    def test_campo_contraditorio_vira_nulo_e_identidade_sobrevive(self):
        """Onde a fonte se contradiz, nulo; onde concorda, o valor fica.

        Arbitrar um lado seria afirmar que uma estacao de terra esta dentro
        de um corpo de agua -- ou, no caso do arquipelago de Sao Pedro e Sao
        Paulo, que nao esta.
        """
        out = sus_climate_compute_indicators(
            _rel_estacao("A134"), indicators=["wbgt"], verbose=False).df()
        assert out["zona_climatica"].isna().all()
        assert out["station_name"].iloc[0] == "S. G. DA CACHOEIRA"
        assert out["federal_unit"].iloc[0] is not None

    def test_nao_duplica_coluna_que_a_entrada_ja_tem(self):
        """A entrada do sus_climate_inmet ja traz latitude e mais quatro.

        No R o `result` so tem id e indicadores nesse ponto, entao as 13
        chegam limpas. Aqui as colunas da entrada sobrevivem (M105), e o
        valor do proprio arquivo do INMET fica.
        """
        out = sus_climate_compute_indicators(
            _rel_estacao("A701", n=20, extra={"latitude": -99.0}),
            indicators=["wbgt"], verbose=False).df()
        assert [c for c in out.columns if "latitude" in c] == ["latitude"]
        assert out["latitude"].unique().tolist() == [-99.0]

    def test_sem_station_code_nao_tenta_o_join(self):
        df = pd.DataFrame({
            "date": pd.date_range("2020-01-01", periods=5, freq="h"),
            "tair_dry_bulb_c": [28.0] * 5,
            "rh_mean_porc": [60.0] * 5,
            "ws_2_m_s": [2.0] * 5,
            "sr_kj_m2": [800.0] * 5,
        })
        out = sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wbgt"],
            verbose=False).df()
        assert len(out) == 5
        assert "zona_climatica" not in out.columns

    def test_metadado_tem_uma_linha_por_estacao(self):
        """O invariante que impede o defeito de voltar pela porta dos fundos."""
        from climasus4py.utils.data import data_path

        meta = pd.read_parquet(
            data_path("assets/climate/inmet_station_meta.parquet"))
        assert meta["station_code"].is_unique, (
            "a tabela voltou a ter codigo repetido; o join passa a "
            "multiplicar linhas de observacao"
        )
        for c in ("region", "federal_unit", "station_name", "latitude",
                  "longitude"):
            assert not meta[c].isna().any(), f"{c} nao devia ter nulo"

    def test_o_join_preserva_a_ordem_das_linhas(self):
        """O hash join do DuckDB embaralha; o left_join do dplyr nao.

        Defeito real encontrado ao escrever estes testes: uma entrada de 4
        linhas voltou como [1200, 0, 500, 0] em vez de [0, 500, 0, 1200].
        Para uma serie horaria isso e pior que coluna faltando -- e por isso
        o join carrega um row_number e ordena por ele.
        """
        n = 50
        df = pd.DataFrame({
            "station_code": ["A701"] * n,
            "date": pd.date_range("2020-03-01", periods=n, freq="h"),
            "tair_dry_bulb_c": [20.0 + i for i in range(n)],
            "rh_mean_porc": [60.0] * n,
            "ws_2_m_s": [2.0] * n,
            "sr_kj_m2": [800.0] * n,
        })
        out = sus_climate_compute_indicators(
            get_connection().from_df(df), indicators=["wbgt"],
            verbose=False).df()
        assert out["tair_dry_bulb_c"].tolist() == df["tair_dry_bulb_c"].tolist()
        assert out["date"].tolist() == df["date"].tolist()


class TestKeepSourceVars:
    """M105: a forma padrao da saida difere do R, e o parametro da a escolha.

    O R monta `result` so com as colunas de identificacao e traz as
    variaveis de origem de volta apenas com keep_source_vars=TRUE. Aqui o
    SELECT sempre abriu com '*', entao o default deste porte corresponde ao
    keep_source_vars=TRUE do R. Mudar o default deixaria de devolver
    colunas que quem chama ja recebe, entao ele fica -- e o False e a opcao
    que reproduz a forma do R.

    Medido contra o R sobre a mesma entrada, um indicador:
      keep_source_vars=False -> 19 colunas nos DOIS, na mesma ordem
      keep_source_vars=True  -> R 23, Python 24
    A diferenca no True e conhecida: o R traz de volta so as variaveis que
    os indicadores pedidos consomem (all_required), e o Python preserva
    todas as colunas da entrada. Aqui o Python e superconjunto.
    """

    @staticmethod
    def _rel(n: int = 10):
        df = pd.DataFrame({
            "station_code": ["A701"] * n,
            "date": pd.date_range("2020-01-01", periods=n, freq="h"),
            "tair_dry_bulb_c": [28.0] * n,
            "rh_mean_porc": [60.0] * n,
            "ws_2_m_s": [2.0] * n,
            "sr_kj_m2": [800.0] * n,
            "rainfall_mm": [0.0] * n,
        })
        return get_connection().from_df(df)

    def test_false_reproduz_a_forma_do_r(self):
        out = sus_climate_compute_indicators(
            self._rel(), indicators=["wbgt"], keep_source_vars=False,
            verbose=False).df()
        esperado = [
            "date", "station_code", "wbgt_c", "wbgt_c_flag_extreme",
            "wbgt_c_flag_high", "wbgt_c_flag_low", "region", "federal_unit",
            "station_name", "latitude", "longitude", "altitude",
            "foundation_date", "id_link", "zona_climatica", "tipo_umidade",
            "distr_umidade", "temperatura_id", "descricao",
        ]
        assert list(out.columns) == esperado

    def test_default_preserva_as_colunas_da_entrada(self):
        out = sus_climate_compute_indicators(
            self._rel(), indicators=["wbgt"], verbose=False).df()
        for c in ("tair_dry_bulb_c", "rh_mean_porc", "ws_2_m_s", "sr_kj_m2",
                  "rainfall_mm"):
            assert c in out.columns, f"{c} desapareceu do default"

    def test_o_indicador_nao_muda_com_a_forma(self):
        a = sus_climate_compute_indicators(
            self._rel(), indicators=["wbgt"], verbose=False).df()
        b = sus_climate_compute_indicators(
            self._rel(), indicators=["wbgt"], keep_source_vars=False,
            verbose=False).df()
        assert a["wbgt_c"].tolist() == b["wbgt_c"].tolist()
        assert len(a) == len(b)

    def test_false_nao_perde_o_metadado_de_estacao(self):
        """O metadado vem do join, nao da entrada, entao tem de sobreviver."""
        out = sus_climate_compute_indicators(
            self._rel(), indicators=["wbgt"], keep_source_vars=False,
            verbose=False).df()
        assert out["station_name"].iloc[0] == "SAO PAULO - MIRANTE"


class TestComputeUncertainty:
    """M12: o Monte Carlo sobre erro de medicao, contra o R.

    Paridade EXATA e nao apenas estatistica, porque o
    climasus4py.utils.r_random reproduz o fluxo do R bit a bit:
    set.seed(2024) por indicador, 200 avaliacoes perturbadas, percentis
    2,5% e 97,5% por linha arredondados a duas casas.

    A referencia foi gerada chamando o proprio sus_climate_compute_indicators
    do R com compute_uncertainty=TRUE e esta em
    tests/fixtures/indicators/ref_uncertainty.parquet. Medido: 99,4% a
    99,8% das linhas identicas, e cada linha residual difere por exatamente
    0,01 -- empate de arredondamento, a mesma classe dos residuos de thi e
    pet. Fechar isso exigiria reimplementar o fround do R.
    """

    REF = (Path(__file__).parent / "fixtures" / "indicators"
           / "ref_uncertainty.parquet")

    @staticmethod
    def _entrada():
        ent = pd.read_parquet(Path(__file__).parent / "fixtures"
                              / "indicators" / "entrada.parquet")
        return get_connection().from_df(
            ent.rename(columns={"datetime": "date"}))

    def test_default_nao_acrescenta_coluna(self):
        out = sus_climate_compute_indicators(
            self._entrada(), indicators=["cdd"], region="none",
            verbose=False).df()
        assert not [c for c in out.columns
                    if c.endswith(("_ci_low", "_ci_high"))]

    def test_acrescenta_duas_colunas_por_indicador(self):
        out = sus_climate_compute_indicators(
            self._entrada(), indicators=["cdd", "vapor_pressure"],
            region="none", compute_uncertainty=True, confidence_flags=False,
            verbose=False).df()
        for col in ("cdd_c", "vapor_pressure_kpa"):
            assert f"{col}_ci_low" in out.columns
            assert f"{col}_ci_high" in out.columns

    def test_classificacoes_nao_ganham_intervalo(self):
        """26 colunas e nao 30: duas das quinze nao declaram incerteza."""
        from climasus4py.enrichment.climate_indicators import _UNCERTAINTY_SD

        assert "heat_stress_risk" not in _UNCERTAINTY_SD
        assert "koppen_humidity" not in _UNCERTAINTY_SD
        assert len(_UNCERTAINTY_SD) == 13
        assert len(_UNCERTAINTY_SD) * 2 == 26

    def test_avisa_quando_nenhum_indicador_tem_incerteza(self):
        with pytest.warns(UserWarning, match="declares input uncertainty"):
            sus_climate_compute_indicators(
                self._entrada(), indicators=["koppen_humidity"],
                region="none", compute_uncertainty=True, verbose=True).df()

    def test_o_intervalo_contem_a_estimativa_onde_as_duas_existem(self):
        out = sus_climate_compute_indicators(
            self._entrada(), indicators=["cdd"], region="none",
            compute_uncertainty=True, confidence_flags=False,
            verbose=False).df()
        ok = out[["cdd_c", "cdd_c_ci_low", "cdd_c_ci_high"]].dropna()
        assert (ok["cdd_c_ci_low"] <= ok["cdd_c"] + 1e-9).all()
        assert (ok["cdd_c"] <= ok["cdd_c_ci_high"] + 1e-9).all()

    @pytest.mark.parametrize("ind,col", [
        ("cdd", "cdd_c"),
        ("vapor_pressure", "vapor_pressure_kpa"),
        ("wbgt", "wbgt_c"),
    ])
    def test_paridade_com_o_r(self, ind, col):
        """Mais de 99% das linhas identicas, residuo so em empate de 0,01."""
        import numpy as np

        if not self.REF.is_file():
            pytest.skip("referencia de incerteza ausente")

        from climasus4py.enrichment.climate_indicators import (
            _mc_interval,
            _pet_month_expr,
            resolve_region_params,
        )

        rel = self._entrada()
        _, params = resolve_region_params("none", rel, None)
        mes = _pet_month_expr(rel.limit(0).df().columns.tolist())
        baixo, alto = _mc_interval(rel, ind, "station_code", "date", mes,
                                   params)
        ref = pd.read_parquet(self.REF)
        for suf, meu in (("_ci_low", baixo), ("_ci_high", alto)):
            b = ref[col + suf].to_numpy()
            comparavel = ~(np.isnan(meu) | np.isnan(b))
            d = np.abs(meu[comparavel] - b[comparavel])
            fracao = float((d < 1e-9).mean())
            assert fracao > 0.99, f"{col}{suf}: so {fracao:.4f} identico"
            residuo = d[d >= 1e-9]
            if residuo.size:
                assert np.allclose(residuo, 0.01), (
                    f"{col}{suf}: residuo fora do empate de arredondamento: "
                    f"{np.unique(residuo)[:5]}"
                )

    def test_m111_o_intervalo_existe_onde_a_estimativa_e_nula(self):
        """A mascara de validade vale para o ponto e nao para o Monte Carlo.

        O R despacha o MC com apply_mask=FALSE enquanto a estimativa usa a
        mascara, entao ha linha com hi_c nulo e intervalo preenchido -- na
        referencia, a linha 2 tem hi_c nulo e intervalo de 38,10 a 46,76. Um
        grafico da faixa mostraria banda onde nao existe estimativa.
        Replicado de proposito; registrado como M111.
        """
        if not self.REF.is_file():
            pytest.skip("referencia de incerteza ausente")
        ref = pd.read_parquet(self.REF)
        assert (ref["hi_c"].isna() & ref["hi_c_ci_low"].notna()).sum() > 0, \
            "a fixture perdeu o caso"

        out = sus_climate_compute_indicators(
            self._entrada(), indicators=["heat_index"], region="none",
            compute_uncertainty=True, confidence_flags=False,
            verbose=False).df()
        meu = (out["hi_c"].isna() & out["hi_c_ci_low"].notna()).sum()
        assert meu > 0, "o porte deixou de replicar o comportamento do R"
