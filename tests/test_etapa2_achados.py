"""O que a Etapa 2 (os 43 sistemas da página do climasus4r) encontrou.

  M149  a semana epidemiológica errava a virada do ano: '00/2022', '53/2023'
  M188  o importador apagava a data de nascimento do SIA-PS e do SIA-SAD
  M189  um .dbc de um registro era lido como 0 linhas

Nada aqui baixa dado: os dois .dbc em fixtures/dbc são os arquivos reais do
DATASUS (SIA-ACF e CNES-EF, AC, janeiro de 2023), de um registro cada.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.core.importer import (
    _cache_antigo,
    _coerce_datasus_types,
    _read_dbc,
    _write_parquet_atomic,
)
from climasus4py.core.variables import _epi_week_sql

FIXTURES = Path(__file__).parent / "fixtures" / "dbc"


# --- M149: semana epidemiológica ------------------------------------------


@pytest.mark.parametrize("data, esperado", [
    ("2022-01-01", "52/2021"),   # sábado: era '00/2022'
    ("2023-12-31", "01/2024"),   # domingo: era '53/2023'
    ("2020-12-31", "53/2020"),   # 2020 TEM SE 53
    ("2021-01-02", "53/2020"),
    ("2021-01-03", "01/2021"),
    ("2023-01-01", "01/2023"),
    ("2024-12-29", "01/2025"),
    ("2023-07-15", "28/2023"),
])
def test_semana_epidemiologica_na_virada_do_ano(data, esperado):
    """A semana é do ano da sua QUARTA-FEIRA: a SE 1 é a primeira semana
    (domingo a sábado) com ao menos quatro dias no ano novo."""
    obtido = get_connection().sql(
        f"SELECT {_epi_week_sql('d')} FROM (SELECT DATE '{data}' AS d)").fetchone()[0]
    assert obtido == esperado


def test_semana_epidemiologica_de_data_nula_e_nula():
    assert get_connection().sql(
        f"SELECT {_epi_week_sql('d')} FROM (SELECT NULL::DATE AS d)").fetchone()[0] is None


def test_o_numero_bate_com_o_r_o_ano_fica():
    """O R devolve só o número (52); o Python mantém 'SS/AAAA' de propósito,
    para um groupby de vários anos não juntar semanas de anos diferentes."""
    obtido = get_connection().sql(
        f"SELECT {_epi_week_sql('d')} FROM (SELECT DATE '2022-01-01' AS d)").fetchone()[0]
    assert int(obtido.split("/")[0]) == 52


# --- M188: datas do importador ---------------------------------------------


def _bruto():
    return pd.DataFrame({"DTNASC": ["19690303", "20020904", None],
                         "DTOBITO": ["05012023", "31122022", None]})


@pytest.mark.parametrize("system", ["SIA-PS", "SIA-SAD", "SIH-RD", "SINAN-DENGUE", "CNES-ST"])
def test_ano_na_frente_fica_texto_no_cache(system):
    """Lido como DDMMAAAA, '19690303' virava NaT -- 1.461 de 1.461 no SIA-PS."""
    df = _coerce_datasus_types(_bruto(), system)
    assert df["DTNASC"].tolist()[:2] == ["19690303", "20020904"]
    assert df["DTNASC"].isna().iloc[2]


@pytest.mark.parametrize("system", ["SIM-DO", "SINASC", None])
def test_sim_e_sinasc_continuam_convertendo_na_importacao(system):
    df = _coerce_datasus_types(_bruto(), system)
    assert str(df["DTOBITO"].iloc[0].date()) == "2023-01-05"


def _grava(tmp_path, df, marcar=True):
    alvo = tmp_path / "AC_2023_01.parquet"
    tabela = pa.Table.from_pandas(df)
    if marcar:
        _write_parquet_atomic(tabela, alvo)
    else:
        pq.write_table(tabela, alvo)
    return alvo


def test_cache_antigo_com_data_destruida_e_baixado_de_novo(tmp_path):
    df = pd.DataFrame({"DTNASC": pd.Series([pd.NaT] * 10), "PA_IDADE": ["063"] * 10})
    assert _cache_antigo("SIA-PS", _grava(tmp_path, df, marcar=False))


def test_cache_novo_e_confiado(tmp_path):
    df = pd.DataFrame({"DTNASC": ["19690303"] * 10})
    assert not _cache_antigo("SIA-PS", _grava(tmp_path, df, marcar=True))


def test_cache_antigo_do_sim_nao_e_baixado_de_novo(tmp_path):
    """O SIM sempre foi DDMMAAAA: o cache dele estava certo."""
    df = pd.DataFrame({"DTNASC": pd.to_datetime(["2023-01-05"] * 10)})
    assert not _cache_antigo("SIM-DO", _grava(tmp_path, df, marcar=False))


def test_cache_antigo_do_sia_sem_coluna_de_data_da_lista_fica(tmp_path):
    """SIA-PA não tem DTNASC: os 72.654 do cache não precisam de novo download."""
    df = pd.DataFrame({"PA_CMP": ["202301"] * 10})
    assert not _cache_antigo("SIA-PA", _grava(tmp_path, df, marcar=False))


# --- M189: .dbc de um registro ----------------------------------------------


@pytest.mark.parametrize("arquivo, colunas, primeiro", [
    ("ACFAC2301.dbc", 56, ["202301", "EP", "120000", "2001586"]),
    ("EFAC2301.dbc", 31, ["2002078", "120040", "001"]),
])
def test_dbc_de_um_registro_traz_o_registro(arquivo, colunas, primeiro):
    """O leitor tomava o .dbc por um DBF sem compressão (tamanho a menos de
    512 bytes do anunciado) e devolvia 0 linhas. O climasus4r lê a linha."""
    df = _read_dbc(FIXTURES / arquivo)
    assert df.shape == (1, colunas)
    assert df.iloc[0, : len(primeiro)].tolist() == primeiro


def test_cache_vazio_sem_marca_e_baixado_de_novo(tmp_path):
    df = pd.DataFrame({"CNES": pd.Series([], dtype=object)})
    assert _cache_antigo("CNES-EF", _grava(tmp_path, df, marcar=False))


def test_cache_pequeno_legitimo_sem_marca_e_confiado(tmp_path):
    """Uma linha de verdade não pode custar novo download -- nem quebrar quem
    está sem internet."""
    df = pd.DataFrame({"CNES": ["2002078"]})
    assert not _cache_antigo("CNES-EF", _grava(tmp_path, df, marcar=False))
