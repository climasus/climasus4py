"""`sus_data_plot_demographics(type="bar", var="region")` no SIM.

A chamada levantava ``ValueError: Column for 'region' not found.`` em
qualquer arquivo do SIM, em **qualquer** ponto do pipeline.

**A causa.** O padrão de ``region`` em ``climasus-data/viz/viz_config.json``
listava cinco colunas, e as cinco são de outros sistemas:

| coluna | sistema |
|---|---|
| ``UF_ZI``, ``manager_uf``, ``uf_gestor`` | SIH |
| ``SG_UF_NOT``, ``notification_uf`` | SINAN |

O SIM não tem nenhuma. E o reflexo de rodar depois do
``sus_spatial_join`` não resolvia: ele **cria** exatamente a informação
que falta — ``abbrev_state``, ``name_state``, ``name_region``,
``code_state``, ``code_region`` — mas sob nomes que a lista não conhecia.
O pacote produzia a coluna com uma função sua e o detector da própria
biblioteca não a reconhecia.

**A correção**, de 07/10/2026: os cinco nomes do ``sus_spatial_join``
foram **acrescentados ao fim** da lista. Ao fim, e não ao início, porque
``_find_col`` devolve o primeiro que casa: pôr os novos na frente mudaria
calado o significado de ``var="region"`` para quem usa SIH e SINAN, que
hoje resolve para a UF do gestor ou da notificação. Assim nada que
funcionava muda, e só o caso que falhava passa a funcionar.

A ordem entre os novos segue o que serve de **rótulo de gráfico**: sigla,
depois nome, e os códigos numéricos por último. O rótulo da chave é
``State/Region`` nos três idiomas, então aceitar as duas granularidades é
o que a configuração já dizia pretender.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.viz.plot_demographics import _detect_demo_cols, _load_config

#: O que o ``sus_spatial_join`` acrescenta, na ordem em que foi inserido.
DO_SPATIAL_JOIN = [
    "abbrev_state", "name_state", "name_region", "code_state", "code_region",
]

#: O que já estava lá, e que não pode ter mudado de posição.
DE_SIH_E_SINAN = [
    "manager_uf", "uf_gestor", "UF_ZI", "SG_UF_NOT", "notification_uf",
]


def _padroes() -> list[str]:
    return _load_config()["demo_column_patterns"]["region"]


# ---------------------------------------------------------------------------
# A asserção que a correção é
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("coluna", DO_SPATIAL_JOIN)
def test_cada_coluna_do_spatial_join_esta_na_lista(coluna: str) -> None:
    assert coluna in _padroes()


@pytest.mark.parametrize("coluna", DO_SPATIAL_JOIN)
def test_cada_uma_sozinha_resolve_region(coluna: str) -> None:
    """Uma de cada vez: nenhuma depende da presença das outras."""
    df = pd.DataFrame({"sex": ["Male"], coluna: ["SP"]})
    assert _detect_demo_cols(df)["region"] == coluna


def test_o_caso_real_do_sim_resolve_para_a_sigla() -> None:
    """Depois do ``sus_spatial_join`` as cinco chegam juntas.

    Medido no SIM-DO/SP/2023: ``region`` saía ``None`` e passou a sair
    ``abbrev_state``, que é a que vira rótulo legível de barra.
    """
    df = pd.DataFrame({c: ["SP"] for c in DO_SPATIAL_JOIN})
    assert _detect_demo_cols(df)["region"] == "abbrev_state"


# ---------------------------------------------------------------------------
# A contrapartida: o que já funcionava não pode ter mudado
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("coluna", DE_SIH_E_SINAN)
def test_sih_e_sinan_continuam_resolvendo_para_si(coluna: str) -> None:
    assert _detect_demo_cols(pd.DataFrame({coluna: ["SP"]}))["region"] == coluna


def test_sih_vence_o_spatial_join_quando_os_dois_estao_presentes() -> None:
    """A razão de os novos terem ido para o **fim** da lista.

    Um SIH que passou pelo ``sus_spatial_join`` carrega ``UF_ZI`` e
    ``abbrev_state`` ao mesmo tempo. Quem ganha tem de continuar sendo o
    ``UF_ZI``, ou ``var="region"`` mudaria de sentido sem avisar para
    quem já usa.
    """
    df = pd.DataFrame({"abbrev_state": ["SP"], "UF_ZI": ["35"]})
    assert _detect_demo_cols(df)["region"] == "UF_ZI"


def test_os_antigos_vem_todos_antes_dos_novos() -> None:
    padroes = _padroes()
    assert max(padroes.index(c) for c in DE_SIH_E_SINAN) \
        < min(padroes.index(c) for c in DO_SPATIAL_JOIN)


def test_a_sigla_vem_antes_dos_codigos_numericos() -> None:
    """``code_state`` é ``"35"``: serve de último recurso, não de rótulo."""
    padroes = _padroes()
    assert padroes.index("abbrev_state") < padroes.index("code_state")
    assert padroes.index("name_region") < padroes.index("code_region")


# ---------------------------------------------------------------------------
# O que continua sendo None, e deve continuar
# ---------------------------------------------------------------------------

def test_sim_antes_do_spatial_join_continua_none() -> None:
    """Não é regressão: as colunas de fato ainda não existem.

    O quadro abaixo é o que ``sus_data_create_variables`` devolve para o
    SIM — nenhuma coluna de UF ou região.
    """
    df = pd.DataFrame({
        "sex": ["Male"], "race": ["White"], "age_years": [60],
        "ibge_age_group": ["60-64"], "education": ["High"],
        "climate_risk_group": ["Standard Risk (5-64)"],
        "residence_municipality_code": ["355030"],
    })
    assert _detect_demo_cols(df)["region"] is None


def test_as_outras_chaves_nao_foram_afetadas() -> None:
    """A edição mexeu numa chave só; esta é a guarda contra dano colateral."""
    df = pd.DataFrame({
        "sex": ["Male"], "race": ["White"], "age_years": [60],
        "ibge_age_group": ["60-64"], "education": ["High"],
        "climate_risk_group": ["Standard Risk (5-64)"],
        "residence_municipality_code": ["355030"],
        "abbrev_state": ["SP"],
    })
    assert _detect_demo_cols(df) == {
        "sex": "sex",
        "race": "race",
        "age": "age_years",
        "age_group": "ibge_age_group",
        "ibge_age_group": "ibge_age_group",
        "education": "education",
        "climate_risk": "climate_risk_group",
        "region": "abbrev_state",
        "municipality": "residence_municipality_code",
    }


def test_o_plot_sai_mesmo_com_a_coluna_resolvida() -> None:
    """Resolver o nome não bastaria se ``_vd_bar`` quebrasse adiante."""
    import climasus4py as cs

    df = pd.DataFrame({"abbrev_state": ["SP"] * 7 + ["RJ"] * 3 + ["MG"] * 5})
    p = cs.sus_data_plot_demographics(df, type="bar", var="region",
                                      lang="en", verbose=False)
    assert p is not None
