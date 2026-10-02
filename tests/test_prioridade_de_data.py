"""M125: a lista de datas estava incompleta, e em dois lugares ninguém a lia.

Duas coisas, e a segunda só apareceu ao conferir quem consome a lista.

**O que o registro dizia** — quatro nomes que o `aggregate_config.json`
trata como candidatos a data e o `role_priority` não declarava:
`first_symptom_date` e `DT_SIN_PRI` (SINAN), `update_date` e `DT_COMPET`
(CNES). Inconsistência entre dois arquivos do mesmo catálogo: o
`sus_data_aggregate` achava essas colunas e os `detect_*_column` não.

**O que apareceu ao olhar os consumidores** — a correção do M127 não
tinha alcançado dois lugares que também precisam da data do **evento**:

- `core/pipeline.py`, o caminho rápido do `sus_pipeline`, usava a lista
  **por sistema** para o município e a **genérica** para a data, no mesmo
  bloco. O comentário ali já dizia "a MESMA prioridade por sistema que o
  caminho staged usa" — e valia só para o município.
- `enrichment/climate.py`, o `sus_climate`, juntava saúde e clima pela
  data genérica.

Nos dois, um arquivo do SINAN — que traz `DT_NASC` ao lado de
`DT_NOTIFIC` — seria datado pelo **nascimento** do paciente.

E a `role_priority["date"]` foi reordenada **por categoria**, como o
`aggregate_config.common` já tinha sido: `birth_date` saiu da posição 6
para a 18, depois de óbito, notificação, internação e competência. Data
de nascimento não é data de evento.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.aggregate import _agg_config, _agg_detect_date_col
from climasus4py.core.engine import get_connection
from climasus4py.utils.data import (
    _FALLBACK_DATASUS_COLUMNS,
    data_path,
    detect_date_column,
    load_datasus_columns_spec,
)

#: Os quatro que o registro nomeava.
QUATRO = ("first_symptom_date", "DT_SIN_PRI", "update_date", "DT_COMPET")


def _date_priority() -> list[str]:
    return load_datasus_columns_spec()["role_priority"]["date"]


# --- os quatro nomes ----------------------------------------------------


@pytest.mark.parametrize("nome", QUATRO)
def test_os_quatro_nomes_entraram(nome: str) -> None:
    assert nome in _date_priority()


def test_nenhum_candidato_do_aggregate_config_ficou_de_fora() -> None:
    """A asserção geral, que sobrevive a novos sistemas.

    Testar os quatro por nome pega o caso de hoje; isto pega o próximo.
    """
    declarados = set(_date_priority())
    citados = {n for lst in _agg_config()["date_candidates"].values()
               for n in lst}
    faltando = citados - declarados
    assert not faltando, (
        f"o aggregate_config trata como data mas o role_priority não "
        f"declara: {sorted(faltando)}")


# --- a ordem por categoria ----------------------------------------------


def test_o_nascimento_vem_depois_das_datas_de_evento() -> None:
    """A invariante, e não só o caso do SINAN.

    Uma lista de prioridade é uma ordem; o que o M127 ensinou é que ali
    a ordem codifica uma **categoria**.
    """
    d = _date_priority()
    for evento in ("death_date", "notification_date", "admission_date",
                   "update_date"):
        assert d.index(evento) < d.index("birth_date"), evento


def test_o_detector_generico_prefere_o_evento() -> None:
    """O efeito da reordenação, medido onde ele importa.

    Antes de 02/10/2026 esta chamada devolvia `birth_date`.
    """
    assert detect_date_column(["birth_date", "notification_date"]) == \
        "notification_date"
    assert detect_date_column(["birth_date", "admission_date"]) == \
        "admission_date"


def test_o_nascimento_ainda_e_achado_quando_e_a_unica() -> None:
    """Rebaixar não é remover: o SINASC continua precisando dele."""
    assert detect_date_column(["birth_date"]) == "birth_date"
    assert _agg_detect_date_col(["birth_date", "death_date"], "SINASC") == \
        "birth_date"


# --- os dois consumidores que faltavam ----------------------------------


def test_o_caminho_rapido_do_pipeline_usa_a_lista_por_sistema() -> None:
    """Município por sistema e data pela genérica, no mesmo bloco.

    A inconsistência estava a duas linhas de distância, com um comentário
    afirmando que as duas seguiam a mesma prioridade.
    """
    import inspect

    from climasus4py.core import pipeline

    src = inspect.getsource(pipeline)
    codigo = "\n".join(l for l in src.splitlines()
                       if not l.lstrip().startswith("#"))
    assert "_agg_detect_date_col(" in codigo
    assert "detect_date_column(columns)" not in codigo


def test_o_sus_climate_usa_a_lista_por_sistema() -> None:
    import inspect

    from climasus4py.enrichment import climate

    src = inspect.getsource(climate)
    codigo = "\n".join(l for l in src.splitlines()
                       if not l.lstrip().startswith("#"))
    assert "_agg_detect_date_col(" in codigo
    assert "detect_date_column(list(rel.columns))" not in codigo


def test_o_sus_climate_le_o_sistema_do_metadado() -> None:
    """Sem isso, a lista por sistema não teria sistema para consultar."""
    import inspect

    from climasus4py.enrichment import climate

    assert "get_meta(rel)" in inspect.getsource(climate)


# --- o metadado e o seu substituto --------------------------------------


def test_o_catalogo_publica_schema_version_5() -> None:
    publicado = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8"))
    assert int(publicado["schema_version"]) >= 5


def test_o_fallback_acompanha_o_publicado() -> None:
    """Divergir aqui é comportar-se diferente conforme o catálogo exista."""
    publicado = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8")
    )["role_priority"]["date"]
    assert publicado == _FALLBACK_DATASUS_COLUMNS["role_priority"]["date"]


def test_catalogo_na_versao_4_recebe_a_lista_corrigida(monkeypatch) -> None:
    """Quem não atualizar o catálogo não herda o defeito em silêncio."""
    from climasus4py.utils import data as _data

    antigo = json.loads(
        data_path("metadata/datasus_columns.json").read_text(encoding="utf-8"))
    antigo["schema_version"] = 4
    antigo["role_priority"] = dict(antigo["role_priority"])
    antigo["role_priority"]["date"] = ["death_date", "birth_date",
                                       "notification_date"]

    _data.load_datasus_columns_spec.cache_clear()
    monkeypatch.setattr(_data, "load_json", lambda rel: antigo)
    try:
        with pytest.warns(UserWarning, match="M127"):
            spec = _data.load_datasus_columns_spec()
        d = spec["role_priority"]["date"]
        assert d.index("notification_date") < d.index("birth_date")
    finally:
        _data.load_datasus_columns_spec.cache_clear()


# --- de ponta a ponta ---------------------------------------------------


def test_o_sus_climate_nao_junta_pelo_nascimento() -> None:
    """O caso do SINAN, na função que ficou de fora do M127."""
    conn = get_connection()
    conn.register("_m125", pd.DataFrame({
        "CODMUNRES": ["310620", "310620"],
        "notification_date": pd.to_datetime(["2023-03-20", "2023-03-25"]),
        "birth_date": pd.to_datetime(["1990-01-10", "1990-01-15"]),
    }))
    from climasus4py.core._stage import set_stage

    rel = set_stage(conn.table("_m125"), "standardize", system="SINAN-DENG")
    assert _agg_detect_date_col(list(rel.columns), "SINAN-DENG") == \
        "notification_date"
