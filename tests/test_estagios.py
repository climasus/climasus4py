"""M34: o vocabulário de estágios divergia do R, e em três frentes.

O registro apontava a contagem — 10 estágios no R contra 6 aqui — mas o
que importava eram as consequências, e elas são duas:

1. **Três enriquecimentos, um nome só.** O `sus_spatial_join`, o
   `sus_climate_join` e o `sus_census_join` gravavam todos a string
   `"enrichment"`. Depois de qualquer um deles, o metadado não dizia
   qual havia rodado — enquanto o `climasus4r` separa `spatial`,
   `climate` e `census`.

2. **O `assert_after` não cobria nada depois da agregação.** Ele retorna
   em silêncio quando o estágio não está em `CANONICAL_STAGES`, e
   `enrichment` e `climate` eram escritos pelo pacote e não estavam na
   lista. Toda checagem de ordem a jusante da agregação passava sem
   verificar coisa alguma.

**O que eu NÃO fiz, e é decisão em aberto:** renomear `standardize` para
`stand`, `variables` para `derive` e fundir `filter` em `filter_cid` +
`filter_demo`, que era a proposta literal do registro ("adotar a lista
do R como referência e alinhar os nomes"). As grafias do R não são
melhores, e renomear quebraria todo código que lê o metadado. No lugar
disso há o `R_STAGE_EQUIVALENTS`, que torna a comparação entre os dois
pacotes possível — que era o que faltava de fato. Se a decisão for
renomear mesmo assim, o mapa é o lugar por onde começar.
"""

from __future__ import annotations

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core._stage import (
    CANONICAL_STAGES,
    LEGACY_STAGE_ALIASES,
    R_STAGE_EQUIVALENTS,
    assert_after,
    canonical_stage,
    get_stage,
    set_stage,
)
from climasus4py.core.engine import get_connection


@pytest.fixture
def rel():
    conn = get_connection()
    conn.register(
        "_est",
        pd.DataFrame({"CODMUNRES": ["310620"], "DTOBITO": ["01012020"]}),
    )
    return conn.table("_est")


# --- os três enriquecimentos se distinguem -----------------------------


def test_o_join_espacial_grava_spatial(rel) -> None:
    """Era `"enrichment"`, igual aos outros dois."""
    assert get_stage(cs.sus_spatial_join(rel)) == "spatial"


def test_os_tres_enriquecimentos_estao_na_lista_canonica() -> None:
    for estagio in ("spatial", "climate", "census"):
        assert estagio in CANONICAL_STAGES, estagio


def test_os_enriquecimentos_vem_depois_da_agregacao() -> None:
    """A ordem é a do R, e é o que dá sentido ao `assert_after`."""
    for estagio in ("spatial", "climate", "census"):
        assert CANONICAL_STAGES.index(estagio) > CANONICAL_STAGES.index(
            "aggregate"
        )


def test_a_ordem_dos_tres_segue_o_r() -> None:
    """`spatial`, `climate`, `census` — a ordem de `sus_meta(valid_values=)`.

    Não é cosmético: `assert_after` compara índices, então trocar a ordem
    muda quais combinações levantam.
    """
    tres = [e for e in CANONICAL_STAGES if e in ("spatial", "climate", "census")]
    assert tres == ["spatial", "climate", "census"]


# --- o assert_after passou a verificar ----------------------------------


def test_assert_after_cobre_os_enriquecimentos(rel) -> None:
    """A regressão que o registro descreveu: checagem que não checava.

    Com o estágio fora da lista, `assert_after` retornava em silêncio —
    então exigir um estágio posterior passava, o que é o oposto do que a
    função existe para fazer.
    """
    juntado = cs.sus_spatial_join(rel)
    assert_after(juntado, "aggregate")   # spatial vem depois: passa
    assert_after(juntado, "spatial")     # o próprio: passa
    with pytest.raises(ValueError, match="before required stage"):
        assert_after(juntado, "climate")
    with pytest.raises(ValueError, match="before required stage"):
        assert_after(juntado, "census")


def test_estagio_desconhecido_continua_passando(rel) -> None:
    """Contrapartida deliberada: um estágio inventado não vira erro duro.

    Era o contrato anterior e não é este achado que deve mudá-lo — o que
    mudou é que os três enriquecimentos deixaram de cair aqui.
    """
    marcado = set_stage(rel, "meu_estagio_proprio")
    assert_after(marcado, "aggregate")


# --- o nome antigo -------------------------------------------------------


def test_o_nome_antigo_ainda_e_entendido() -> None:
    """Metadado gravado antes do M34 carrega `"enrichment"`.

    Um Parquet salvo semana passada não pode virar ilegível.
    """
    assert canonical_stage("enrichment") == "spatial"
    assert "enrichment" in LEGACY_STAGE_ALIASES


def test_o_nome_antigo_resolve_para_o_mais_CEDO_dos_tres() -> None:
    """Conservador de propósito.

    `"enrichment"` podia ser qualquer um dos três, e não há como saber
    qual. Resolver para o mais cedo faz `assert_after` recusar o que não
    pode confirmar, em vez de deixar passar um estágio que a relação
    talvez não tenha alcançado.
    """
    alvo = LEGACY_STAGE_ALIASES["enrichment"]
    indices = [CANONICAL_STAGES.index(e) for e in ("spatial", "climate", "census")]
    assert CANONICAL_STAGES.index(alvo) == min(indices)


def test_assert_after_entende_o_nome_antigo(rel) -> None:
    antigo = set_stage(rel, "enrichment")
    assert_after(antigo, "aggregate")
    with pytest.raises(ValueError):
        assert_after(antigo, "census")


# --- o mapa para o R ------------------------------------------------------


def test_todo_estagio_canonico_tem_equivalente_no_r() -> None:
    """Um estágio sem equivalente torna a comparação impossível de novo."""
    assert set(R_STAGE_EQUIVALENTS) == set(CANONICAL_STAGES)
    for py, r in R_STAGE_EQUIVALENTS.items():
        assert r, f"{py} sem equivalente"


def test_o_mapa_cobre_os_dez_estagios_do_r() -> None:
    """Os dez do `sus_meta(valid_values="stage")`, nenhum sobrando.

    Conferido no pacote R instalado: import, clean, stand, filter_cid,
    filter_demo, derive, aggregate, spatial, climate, census.
    """
    do_r = {
        "import", "clean", "stand", "filter_cid", "filter_demo",
        "derive", "aggregate", "spatial", "climate", "census",
    }
    mapeados = {nome for grupo in R_STAGE_EQUIVALENTS.values() for nome in grupo}
    assert mapeados == do_r


def test_o_filtro_e_um_para_dois() -> None:
    """A única divergência de granularidade real entre os dois pacotes.

    O R separa filtro de CID e filtro demográfico; aqui há um
    `sus_filter` só. Afirmar isso evita que alguém "conserte" o mapa
    para um-para-um e perca metade da correspondência.
    """
    assert R_STAGE_EQUIVALENTS["filter"] == ("filter_cid", "filter_demo")


def test_nenhum_nome_do_r_aparece_em_dois_estagios() -> None:
    vistos: dict[str, str] = {}
    for py, grupo in R_STAGE_EQUIVALENTS.items():
        for nome in grupo:
            assert nome not in vistos, f"{nome} em {py} e em {vistos[nome]}"
            vistos[nome] = py
