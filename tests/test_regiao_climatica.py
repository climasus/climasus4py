"""M141: ``climate_region`` inválido levantava nada e devolvia coluna nula.

**O defeito.** ``_climatic_season_sql`` fazia ``regions.get(r, {})``. Uma
região que o config não conhece dava dicionário vazio, as três listas de
mês saíam vazias, e a função devolvia o literal ``"NULL"``. As duas
colunas sazonais eram criadas assim mesmo — e vinham **100% nulas**.

Medido com ``climate_region="subtropical"`` no SIM-DO/SP/2023:
``climatic_season`` e ``dry_rainy_season`` com **334.303 nulos de
334.303**, e **zero avisos**.

**Por que levantar, e não avisar nem substituir.** É o pior dos três
desfechos: o esquema afirma que a informação está lá e ela não está, então
um ``groupby`` na coluna devolve quadro vazio sem nada que explique.
Avisar e deixar nula mantém a coluna nula para contaminar a análise, e o
aviso se perde em notebook de saída longa. Substituir por uma região
padrão inventa sazonalidade que ninguém pediu — e o usuário nunca fica
sabendo que a região dele foi ignorada.

**Alinhamento raro com o R:** ele também levanta aqui, com ``índice fora
dos limites``. Comportamento certo, mensagem que não diz a causa. A nossa
nomeia os valores válidos e a saída.
"""

from __future__ import annotations

import warnings

import duckdb
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.core.variables import (
    regioes_climaticas,
    valida_regiao_climatica,
)

VALIDAS = ("norte", "nordeste", "centro-oeste", "sudeste", "sul")

#: Valores que já apareceram em uso real e **não** são aceitos.
#: ``subtropical`` estava no notebook de demonstração até 08/10/2026;
#: ``southeast`` é a tradução que um falante de inglês tentaria.
INVALIDAS = ("subtropical", "southeast", "sudest", "SP", "", "xyz")


@pytest.fixture(scope="module")
def quadro() -> duckdb.DuckDBPyRelation:
    conn = get_connection()
    conn.register("_m141", pd.DataFrame({
        "death_date": pd.to_datetime(
            ["2023-01-15", "2023-04-20", "2023-07-10", "2023-10-05"]),
        "age_code": ["440", "450", "460", "470"],
        "sex": ["Male"] * 4,
    }))
    return conn.sql("SELECT * FROM _m141")


# ---------------------------------------------------------------------------
# A asserção que a correção é
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("valor", INVALIDAS)
def test_regiao_invalida_levanta(valor: str) -> None:
    with pytest.raises(ValueError, match="is not recognised"):
        valida_regiao_climatica(valor)


@pytest.mark.parametrize("valor", INVALIDAS)
def test_create_variables_levanta_antes_de_trabalhar(quadro, valor) -> None:
    """Levanta na entrada da função pública, não no meio do SQL."""
    with pytest.raises(ValueError, match="is not recognised"):
        cs.sus_data_create_variables(quadro, lang="en",
                                     climate_region=valor, verbose=False)


def test_a_mensagem_nomeia_os_validos_e_a_saida() -> None:
    """A mensagem é a correção tanto quanto a recusa.

    A do R (``índice fora dos limites``) recusa certo e não diz por quê.
    """
    with pytest.raises(ValueError) as exc:
        valida_regiao_climatica("subtropical")
    msg = str(exc.value)
    assert "subtropical" in msg
    for r in VALIDAS:
        assert r in msg
    assert "astronomical" in msg.lower()


# ---------------------------------------------------------------------------
# A contrapartida: o que funcionava tem de continuar funcionando
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("regiao", VALIDAS)
def test_as_cinco_regioes_continuam_aceitas(regiao: str) -> None:
    assert valida_regiao_climatica(regiao) == regiao


@pytest.mark.parametrize("grafia", ["SUDESTE", "Sudeste", " sudeste "])
def test_caixa_e_espaco_nao_atrapalham(grafia: str) -> None:
    assert valida_regiao_climatica(grafia) == "sudeste"


def test_as_validas_vem_do_config_compartilhado() -> None:
    """Lida do ``climasus-data``, não copiada — se o config ganhar uma
    região, a validação a aceita sem tocar no código."""
    assert set(regioes_climaticas()) == set(VALIDAS)


# ---------------------------------------------------------------------------
# O que o defeito produzia, e que não pode voltar
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("regiao", VALIDAS)
def test_regiao_valida_nao_devolve_coluna_nula(quadro, regiao: str) -> None:
    """O sintoma do defeito, virado do avesso.

    Antes, só valores inválidos davam coluna nula; esta asserção fixa que
    nenhum valor **válido** a produza — que é o que garantiria que a
    validação não virou um teatro.
    """
    v = cs.sus_data_create_variables(quadro, lang="en",
                                     climate_region=regiao, verbose=False)
    df = v.df()
    for col in ("climatic_season", "dry_rainy_season"):
        assert col in df.columns
        assert df[col].notna().all(), f"{regiao}: {col} veio com nulo"


def test_sem_regiao_usa_a_astronomica_e_nao_levanta(quadro) -> None:
    """A saída que a mensagem de erro oferece tem de existir de fato."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        v = cs.sus_data_create_variables(quadro, lang="en", verbose=False)
    df = v.df()
    assert "astronomical_season" in df.columns
    assert df["astronomical_season"].notna().all()
    assert "climatic_season" not in df.columns
