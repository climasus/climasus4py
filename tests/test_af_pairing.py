"""O RR diario tem de ficar pareado com o dia certo (M24).

ESTE ARQUIVO EXISTE PARA IMPEDIR UMA "CORRECAO" ERRADA. O M24 foi registrado
como divergencia do Python contra o R na fracao atribuivel -- af total +8%,
heat -29%, cold +85%, com a ordem heat/cold invertida. Investigado em
09/09/2026, o defeito e do R:

``dlnm::crosspred(cb, model, at = x)`` devolve ``allRRfit`` na ordem CRESCENTE
de x, nao na ordem em que x foi passado. O ``.saf_component`` do climasus4r
pareia esse vetor POSICIONALMENTE com ``cases`` e com ``in_range``, que estao
em ordem de DATA. Como todos os x sao distintos, os comprimentos coincidem
(1812 = 1812) e o R nao emite aviso nenhum: cada dia recebe silenciosamente o
RR de outro dia.

Prova: embaralhando o RR do Python da mesma forma -- ``rr[argsort(x)]`` --
reproduzem-se os numeros do R ate a sexta casa decimal (0.054892 / 0.032898 /
0.021994 contra 0.056669 / -0.007568 / 0.064237 do pareamento correto).

Portanto o Python esta CERTO e nao deve ser alinhado ao R aqui. Se uma rodada
futura de comparacao apontar esta divergencia, a resposta e o bug do R, nao
uma correcao no Python. O lado R esta registrado no IDEIAS.md.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment.mod_dlnm import _allrr_fit

LAG = 14


@pytest.fixture(scope="module")
def ajuste():
    """Cinco anos de serie diaria com colunas de lag -- ajuste estavel.

    Um ano so nao serve: com 15 colunas de lag o ajuste fica instavel e a AF
    sai com sinal erratico, o que mistura ruido de estimacao com o efeito que
    este teste quer medir.
    """
    rng = np.random.default_rng(2024)
    dias = pd.date_range("2019-01-01", "2023-12-31", freq="D")
    temp = (
        22
        + 7 * np.sin(2 * np.pi * (dias.dayofyear - 20) / 365)
        + rng.normal(0, 2.0, len(dias))
    )
    risco = 14 * (
        1 + 0.03 * np.clip(temp - 26, 0, None) + 0.02 * np.clip(18 - temp, 0, None)
    )
    df = pd.DataFrame({"date": dias, "n_obitos": rng.poisson(risco)})
    serie = pd.Series(temp)
    for lag in range(LAG + 1):
        df[f"tair_dry_bulb_c_lag{lag}"] = serie.shift(lag).values
    df = df.dropna().reset_index(drop=True)
    return cs.sus_mod_dlnm(df, climate_col="tair_dry_bulb_c", verbose=False)


def _componentes(fit, rr):
    """AF total/heat/cold a partir de um vetor de RR, na formula de Gasparrini."""
    daily = fit["data_daily"]
    cen = fit["meta"]["ref_value"]
    x = daily[f"{fit['meta']['climate_col']}_lag0"].to_numpy(float)
    cases = daily["y"].to_numpy(float)
    n = cases.sum()
    an = cases * (1 - 1 / rr)
    return {
        "total": float(np.nansum(an) / n),
        "heat": float(np.nansum(np.where(x >= cen, an, 0)) / n),
        "cold": float(np.nansum(np.where(x < cen, an, 0)) / n),
    }


def _rr(fit):
    m = fit["meta"]
    daily = fit["data_daily"]
    x = daily[f"{m['climate_col']}_lag0"].to_numpy(float)
    coef = np.asarray(fit["model"].params[1 : 1 + fit["crossbasis"].shape[1]])
    return _allrr_fit(m["var_meta"], m["lag_meta"], coef, x,
                      m["lag_max"], m["ref_value"], "pt")


def test_sus_mod_af_matches_the_correctly_paired_formula(ajuste):
    """A funcao publica tem de dar o mesmo que a formula pareada na mao."""
    esperado = _componentes(ajuste, _rr(ajuste))
    obtido = cs.sus_mod_af(ajuste, nsim=0, verbose=False)["total"]
    por_componente = dict(zip(obtido["component"], obtido["af"]))
    for nome, valor in esperado.items():
        assert por_componente[nome] == pytest.approx(valor, abs=1e-9), nome


def test_scrambling_the_rr_changes_the_split(ajuste):
    """Guarda contra alinhar o Python ao R aqui.

    Pareamento embaralhado -- o que o R faz -- da resultado DIFERENTE, e em
    especial troca o sinal do componente de calor. Se este teste comecar a
    falhar porque os dois passaram a coincidir, alguem alinhou o Python ao
    comportamento defeituoso do R.
    """
    rr = _rr(ajuste)
    x = ajuste["data_daily"][
        f"{ajuste['meta']['climate_col']}_lag0"
    ].to_numpy(float)

    certo = _componentes(ajuste, rr)
    embaralhado = _componentes(ajuste, rr[np.argsort(x, kind="stable")])

    assert certo["heat"] != pytest.approx(embaralhado["heat"], abs=1e-6)
    assert certo["cold"] != pytest.approx(embaralhado["cold"], abs=1e-6)


def test_heat_and_cold_add_up_to_total(ajuste):
    """A decomposicao e exaustiva: todo dia cai em um lado ou no outro."""
    tot = cs.sus_mod_af(ajuste, nsim=0, verbose=False)["total"]
    por = dict(zip(tot["component"], tot["af"]))
    assert por["heat"] + por["cold"] == pytest.approx(por["total"], abs=1e-9)


def test_rr_is_aligned_with_the_exposure_day(ajuste):
    """O teste direto: o RR do dia i tem de ser o RR da exposicao do dia i.

    E o invariante que o R quebra. Recalculo o RR de cada dia isoladamente e
    exijo que case com o vetor devolvido para a serie inteira.
    """
    m = ajuste["meta"]
    x = ajuste["data_daily"][f"{m['climate_col']}_lag0"].to_numpy(float)
    coef = np.asarray(
        ajuste["model"].params[1 : 1 + ajuste["crossbasis"].shape[1]]
    )
    rr_serie = _rr(ajuste)

    for i in (0, 1, 17, 500, len(x) - 1):
        rr_isolado = _allrr_fit(
            m["var_meta"], m["lag_meta"], coef, np.array([x[i]]),
            m["lag_max"], m["ref_value"], "pt",
        )[0]
        assert rr_serie[i] == pytest.approx(rr_isolado, rel=1e-12), i
