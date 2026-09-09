"""Regressao: o resumo do sus_mod_burden tem de seguir o component pedido.

O bloco de ``total_burden`` filtrava pelo literal ``"total"``, entao com
``component="heat"`` ou ``"cold"`` ele selecionava zero linha. O efeito visivel
era um ``IndexError`` no ``top_an``, mas o dano silencioso vinha antes:
``an_total`` somava um frame vazio e dava 0, e ``af_pct_avg`` dava NaN. Sem as
duas ultimas linhas do bloco, a funcao devolveria zeros sem reclamar.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_burden import sus_mod_burden

# heat + cold == total em cada cidade, o que da uma checagem de consistencia
# independente do resultado. n_cases e o mesmo dentro de cada cidade e af_pct
# sai de an/n_cases, como no af de verdade -- fixture incoerente esconde erro.
_CIDADES = {
    "Sao Paulo": (75_000, [("total", 3000.0), ("heat", 1800.0), ("cold", 1200.0)]),
    "Campinas": (30_000, [("total", 900.0), ("heat", 500.0), ("cold", 400.0)]),
    "Santos": (15_000, [("total", 300.0), ("heat", 200.0), ("cold", 100.0)]),
}


@pytest.fixture
def fits():
    """Reproduz o conjunto de colunas que sus_mod_af devolve em ``total``.

    Vale notar: ``_classify_fit`` aceita um frame com apenas ``{an, af_pct}``,
    mas ``_burden_from_af`` exige tambem ``an_lo``/``an_hi`` e os pares de
    ``af_pct``, e falha com KeyError cru se faltarem. A validacao e mais
    frouxa que o calculo -- ver M55.
    """
    saida = {}
    for cidade, (casos, linhas) in _CIDADES.items():
        an = [a for _, a in linhas]
        pct = [round(a / casos * 100, 4) for _, a in linhas]
        saida[cidade] = pd.DataFrame(
            {
                "component": [c for c, _ in linhas],
                "threshold": [None] * len(linhas),
                "n_cases": [casos] * len(linhas),
                "an": an,
                "an_lo": [a * 0.9 for a in an],
                "an_hi": [a * 1.1 for a in an],
                "af": [p / 100 for p in pct],
                "af_lo": [p / 100 * 0.9 for p in pct],
                "af_hi": [p / 100 * 1.1 for p in pct],
                "af_pct": pct,
                "af_pct_lo": [p * 0.9 for p in pct],
                "af_pct_hi": [p * 1.1 for p in pct],
            }
        )
    return saida


@pytest.mark.parametrize(
    ("component", "an_esperado"),
    [("total", 4200), ("heat", 2500), ("cold", 1700)],
)
def test_summary_follows_the_requested_component(fits, component, an_esperado):
    out = sus_mod_burden(fits, component=component, verbose=False)
    assert out["total_burden"]["an_total"] == an_esperado


def test_heat_and_cold_add_up_to_total(fits):
    """Checagem independente: as tres somas tem de fechar entre si."""
    somas = {
        c: sus_mod_burden(fits, component=c, verbose=False)["total_burden"]["an_total"]
        for c in ("total", "heat", "cold")
    }
    assert somas["heat"] + somas["cold"] == somas["total"]


@pytest.mark.parametrize("component", ["total", "heat", "cold", "all"])
def test_no_crash_and_no_empty_summary(fits, component):
    """Antes: IndexError em heat/cold/all. E o zero silencioso, que e pior."""
    tb = sus_mod_burden(fits, component=component, verbose=False)["total_burden"]
    assert tb["an_total"] > 0, "resumo vazio -> soma de frame vazio"
    assert tb["af_pct_avg"] == tb["af_pct_avg"], "af_pct_avg saiu NaN"
    assert tb["top_city"] == "Sao Paulo"
    assert tb["top_city_an"] > 0


def test_all_summarises_totals(fits):
    """``all`` resume os totais -- e o que _burden_rank tambem usa para ranquear."""
    todos = sus_mod_burden(fits, component="all", verbose=False)["total_burden"]
    total = sus_mod_burden(fits, component="total", verbose=False)["total_burden"]
    assert todos["an_total"] == total["an_total"]


def test_top_city_an_matches_the_component(fits):
    quente = sus_mod_burden(fits, component="heat", verbose=False)["total_burden"]
    assert quente["top_city_an"] == 1800, "pegou o an do total, nao o do heat"
