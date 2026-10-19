"""M137/M138: qual município a geometria e a contagem usam.

**O defeito.** ``sus_spatial_join`` anexava o polígono do município de
**residência** e ``sus_data_aggregate`` contava por município de
**ocorrência** — para SIM, e só para SIM. Num choropleth isso desenha a
contagem de um município sobre o polígono de outro.

Não era raro: medido no SIM-DO/SP/2023, residência e ocorrência
discordam em **1.059 de 5.196 registros (20,4%)**, e a discordância
cruza fronteira de estado — um residente de Planalto/SP que morre em
Boa Vista/RR recebe um polígono ou o outro conforme a etapa.

**A causa eram duas listas de prioridade que discordavam**, o mesmo
padrão do M119 (dois decodificadores de idade):

- ``detect_geo_column``, do ``climasus-data``, atende ``sus_spatial_join``,
  ``sus_census``, ``sus_climate`` e ``plot_aggregate_map`` — residência
  primeiro;
- ``_agg_geo_candidates``, lista própria do ``sus_data_aggregate``, tinha
  ``geo_candidates["SIM"]`` com ocorrência primeiro. Os outros cinco
  sistemas já listavam residência.

**A correção** tem duas partes: alinhar o ``SIM`` aos demais, e expor
``join_col=`` nas duas funções — o ``detect_geo_column`` já aceitava
``basis`` e nenhuma função pública o repassava, então quem precisava de
ocorrência não tinha como pedir.

**Por que residência é o default.** Este é um pacote de saúde *e clima*:
o que importa é o clima a que a pessoa esteve exposta, não o município
onde ela veio a falecer. O ``climasus4r`` escolhe o oposto no
``sus_spatial_join`` (seu ``priority_order`` põe ocorrência nas três
primeiras posições) e o mesmo que nós no ``sus_data_aggregate`` — ou
seja, ele é incoerente na direção inversa. Ver M137 e M138.
"""

from __future__ import annotations

import warnings

import duckdb
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.aggregate import _agg_geo_candidates
from climasus4py.core.engine import get_connection
from climasus4py.enrichment.spatial import (
    _bases_validas,
    _colunas_da_base,
    resolve_join_col,
)

BASES = ("residence", "occurrence", "notification", "unspecified")


@pytest.fixture(scope="module")
def quadro() -> duckdb.DuckDBPyRelation:
    """Dois municípios de residência, um de ocorrência.

    Construído para que as duas bases **não** coincidam: se o código
    voltar a trocá-las, a contagem de municípios distintos muda.

    Registrado na conexão compartilhada, e não com ``duckdb.from_df``:
    uma relação criada noutra conexão não serve de replacement scan
    para o SQL que o pacote monta.
    """
    conn = get_connection()
    conn.register("_m138_base", pd.DataFrame({
        "death_date": pd.to_datetime(
            ["2023-01-15", "2023-01-20", "2023-02-10", "2023-02-11"]),
        "residence_municipality_code":  ["355030", "355030", "354990", "354990"],
        "occurrence_municipality_code": ["355030", "355030", "355030", "355030"],
        "sex": ["Male"] * 4,
    }))
    return conn.sql("SELECT * FROM _m138_base")


# ---------------------------------------------------------------------------
# A asserção que a correção é: o SIM alinhado aos demais sistemas
# ---------------------------------------------------------------------------

def test_o_sim_agrega_por_OCORRENCIA_como_o_r() -> None:
    """Série de mortalidade se agrega por local de ocorrência.

    Não é escolha nossa: o ``system_priority`` do
    ``.data_aggregate_tibble_internal`` do ``climasus4r`` é
    ``SIM = c("ocorrencia", "residencia", "estabelecimento",
    "generico")``, e o ``resolve_geo_col`` repete a mesma tabela.

    Esta asserção já existia em ``test_pipeline_convergencia.py`` e em
    ``test_deteccao_multilingue.py``; está repetida aqui porque em
    19/10/2026 eu a inverti — tinha lido só os ``geo_groups``, que
    listam residência primeiro, sem ver que o ``system_priority``
    sobrepõe para o SIM. Os guardas pegaram.
    """
    assert _agg_geo_candidates("SIM-DO")[0] == "CODMUNOCOR"


@pytest.mark.parametrize("system", ["SIH-RD", "SINAN-DENGUE",
                                    "SIA-PA", "SINASC"])
def test_os_demais_sistemas_agregam_por_residencia(system: str) -> None:
    """O SIM é a exceção, e os outros têm de continuar em residência."""
    cands = _agg_geo_candidates(system)
    res = next(i for i, c in enumerate(cands)
               if c in _colunas_da_base("residence"))
    ocor = next((i for i, c in enumerate(cands)
                 if c in _colunas_da_base("occurrence")), len(cands))
    assert res < ocor, f"{system}: ocorrência veio antes de residência"


def test_residencia_continua_alcancavel_no_sim() -> None:
    """Priorizar ocorrência não é descartar residência."""
    cands = _agg_geo_candidates("SIM-DO")
    assert any(c in _colunas_da_base("residence") for c in cands)


# ---------------------------------------------------------------------------
# O parâmetro novo
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("basis", BASES)
def test_as_quatro_bases_sao_aceitas(basis: str) -> None:
    assert basis in _bases_validas()


def test_aggregate_default_agrupa_por_ocorrencia_no_sim(quadro) -> None:
    d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              verbose=False).df()
    assert "occurrence_municipality_code" in d.columns
    assert d["occurrence_municipality_code"].nunique() == 1


def test_aggregate_com_residencia_agrupa_por_residencia(quadro) -> None:
    d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col="residencia", verbose=False).df()
    assert "residence_municipality_code" in d.columns
    assert d["residence_municipality_code"].nunique() == 2


def test_as_duas_bases_dao_resultados_DIFERENTES(quadro) -> None:
    """O teste que falharia se o parâmetro fosse ignorado em silêncio.

    Compara os **municípios**, não o número de linhas: o agrupamento é
    por (mês, município) e aqui há dois meses dos dois jeitos, então a
    contagem de linhas coincide por acidente. É a geografia que muda —
    por residência fevereiro cai em 354990, por ocorrência em 355030.
    """
    res = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                                join_col="residence", verbose=False).df()
    ocor = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                                 join_col="occurrence", verbose=False).df()
    assert set(res["residence_municipality_code"]) == {"355030", "354990"}
    assert set(ocor["occurrence_municipality_code"]) == {"355030"}


def test_o_total_nao_muda_entre_as_bases(quadro) -> None:
    """Mudar a geografia redistribui a contagem; não cria nem perde caso."""
    tot = []
    for b in ("residence", "occurrence"):
        d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                                  join_col=b, verbose=False).df()
        tot.append(int(d[[c for c in d.columns if c.startswith("n")][0]].sum()))
    assert tot[0] == tot[1] == 4


# ---------------------------------------------------------------------------
# Recusa, que é o ponto: cair para outra geografia em silêncio é o defeito
# ---------------------------------------------------------------------------

def test_base_invalida_e_recusada_no_aggregate(quadro) -> None:
    with pytest.raises(ValueError, match="neither a basis nor a column"):
        cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col="xyz", verbose=False)


def test_base_ausente_no_dado_e_recusada_no_aggregate(quadro) -> None:
    """Não há coluna de notificação aqui — tem de levantar, não cair para
    residência."""
    with pytest.raises(ValueError, match="has no column of it"):
        cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col="notification", verbose=False)


def test_base_invalida_e_recusada_no_spatial_join(quadro) -> None:
    with pytest.raises(ValueError, match="neither a basis nor a column"):
        cs.sus_spatial_join(quadro, join_col="xyz")


def test_base_ausente_avisa_no_spatial_join(quadro) -> None:
    """O join avisa em vez de levantar: diferente do aggregate, ele ainda
    produz geometria utilizável — só não a que foi pedida. O aviso é o que
    impede isso de passar despercebido."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        cs.sus_spatial_join(quadro, join_col="notification")
    assert any("basis" in str(x.message) for x in w)


# ---------------------------------------------------------------------------
# Coerência entre as duas etapas, que é o que o M138 cobra
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("basis", ["residence", "occurrence"])
def test_join_e_aggregate_usam_a_mesma_coluna(quadro, basis: str) -> None:
    g = cs.sus_spatial_join(quadro, join_col=basis)
    a = cs.sus_data_aggregate(g, time_unit="month", system="SIM-DO",
                              join_col=basis, verbose=False).df()
    col = next(c for c in a.columns if "municipality_code" in c)
    assert col in _colunas_da_base(basis)


def test_pedindo_ocorrencia_as_duas_etapas_concordam(quadro) -> None:
    """A divergencia real: o aggregate do Python ja batia com o R em
    ocorrencia, e e o spatial_join que usa residencia por default --
    entao a coerencia exige pedir ocorrencia ao join."""
    g = cs.sus_spatial_join(quadro, join_col="occurrence")
    a = cs.sus_data_aggregate(g, time_unit="month", system="SIM-DO",
                              verbose=False).df()
    col = next(c for c in a.columns if "municipality_code" in c)
    assert col in _colunas_da_base("occurrence")


# ---------------------------------------------------------------------------
# join_col aceita três formas — a do R entre elas
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("grafia", [
    "residence", "residencia", "residência", "RESIDENCIA",
])
def test_a_base_e_aceita_em_qualquer_grafia(quadro, grafia: str) -> None:
    """Acento e caixa não importam, e as três línguas valem.

    O pacote é trilíngue e o ``sus_filter`` já aceitava
    ``sex="Masculino"``; este parâmetro tinha de aceitar também.
    """
    d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col=grafia, verbose=False).df()
    assert "residence_municipality_code" in d.columns


@pytest.mark.parametrize("grafia", ["ocorrencia", "ocorrência", "ocurrencia"])
def test_ocorrencia_tambem_nas_tres_linguas(quadro, grafia: str) -> None:
    d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col=grafia, verbose=False).df()
    assert "occurrence_municipality_code" in d.columns


def test_generico_resolve_para_unspecified() -> None:
    """``generico`` é como o ``climasus4r`` chama esta base internamente."""
    assert resolve_join_col("generico", [])[1] == "unspecified"


# --- a forma que o R usa ---------------------------------------------------

@pytest.mark.parametrize("coluna", [
    "residence_municipality_code", "occurrence_municipality_code",
])
def test_o_nome_da_coluna_funciona_como_no_r(quadro, coluna: str) -> None:
    """``join_col="residence_municipality_code"`` é o que o R recebe.

    Era o ponto cego do nome anterior: o valor que funciona no R era
    recusado aqui, porque ``basis`` só aceitava a base.
    """
    d = cs.sus_data_aggregate(quadro, time_unit="month", system="SIM-DO",
                              join_col=coluna, verbose=False).df()
    assert coluna in d.columns


def test_a_coluna_resolve_para_a_sua_base() -> None:
    col, base = resolve_join_col("CODMUNRES", ["CODMUNRES"])
    assert (col, base) == ("CODMUNRES", "residence")


def test_qualquer_coluna_presente_e_aceita() -> None:
    """Mesma permissividade do R, que só confere ``join_col %in% names(df)``.

    A base volta ``None`` porque nenhuma a reivindica — e isso é
    informação, não erro.
    """
    assert resolve_join_col("minha_col", ["minha_col"]) == ("minha_col", None)


def test_coluna_ausente_e_recusada() -> None:
    with pytest.raises(ValueError, match="neither a basis nor a column"):
        resolve_join_col("nao_existe", ["outra"])
