"""M127: o clima era casado com a data de NASCIMENTO num arquivo do SINAN.

O `sus_climate_aggregate` detectava a coluna de data pela lista **global**
`role_priority["date"]`, que está ordenada:

    death_date, date, DTOBITO, data_obito, fecha_muerte, DTNASC,
    birth_date, ..., notification_date, ...

`birth_date` na posição 6, `notification_date` na 11. Um arquivo do SINAN
traz `DT_NASC` ao lado de `DT_NOTIFIC`, e depois do `sus_data_standardize`
isso vira `birth_date` e `notification_date` — então a função escolhia o
**nascimento**.

**Medido antes da correção**, com clima sintético em rampa (18 °C em
dezembro a 28 °C em abril) e dois eventos notificados em 20 e 25/03,
nascidos em 10 e 15/01:

| janela | temperatura |
|---|---|
| março (notificação) | ~25,4 °C |
| janeiro (nascimento) | ~20,8 °C |
| **resultado obtido** | **[20,53; 20,20]** |

Sem erro e sem aviso. Um estudo de dengue teria a exposição climática
casada à data de nascimento do paciente, e nada no resultado indicaria.

**O erro de origem não é a ordem, é a categoria.** Uma data de
nascimento não é uma data de evento. O `climasus4r` acerta nisso: o
`detect_event_date_column_lazy` tem uma lista que **omite `birth_date`** —
embora nada no pacote R a chame, é código morto.

**E o R não tem o defeito** por outro motivo: o `.join_moving_window`
usa a coluna literalmente chamada `date`, sem detector nenhum, e quem
chama tem de criá-la. Mais trabalho e mais seguro — a escolha é
explícita.

A correção tem quatro partes: a lista `common` reordenada por categoria,
o `system` lido do `sus_meta`, o `date_col` como escape, e o aviso quando
há mais de uma candidata.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.enrichment import climate_aggregate as ca

#: Rampa de 18 a 28 °C entre dez/2022 e abr/2023, numa estação em BH.
#: A janela de março vale ~25,4 e a de janeiro ~20,8 — bem separadas, para
#: que qual coluna comandou a agregação fique evidente no valor.
_DIAS = pd.date_range("2022-12-01", "2023-04-30", freq="D")


@pytest.fixture(autouse=True)
def _limpa_aviso():
    """O aviso é uma vez por processo; sem isto a ordem dos testes importa."""
    ca._AVISOU_DATA.clear()
    yield
    ca._AVISOU_DATA.clear()


@pytest.fixture
def clima():
    conn = get_connection()
    conn.register("_m127_clima", pd.DataFrame({
        "station_code": ["A"] * len(_DIAS),
        "date": _DIAS,
        "latitude": [-19.9] * len(_DIAS),
        "longitude": [-43.9] * len(_DIAS),
        "tair_dry_bulb_c": np.linspace(18, 28, len(_DIAS)),
    }))
    return conn.table("_m127_clima")


def _saude(nome: str, **colunas):
    """Relação de saúde com geometria, para as datas dadas."""
    conn = get_connection()
    dados = {"CODMUNRES": ["310620", "310620"]}
    dados.update({k: pd.to_datetime(v) for k, v in colunas.items()})
    conn.register(nome, pd.DataFrame(dados))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return cs.sus_spatial_join(conn.table(nome))


def _valor(saude, clima, **kw):
    """O valor climático agregado, que denuncia qual data comandou."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = cs.sus_climate_aggregate(
            health_data=saude, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False, **kw)
    d = r.df()
    col = next(c for c in d.columns if "tair" in c)
    return float(d[col].mean())


MARCO, JANEIRO = 25.0, 20.6   # as duas janelas, arredondadas


# --- o defeito, nos dois sistemas que o expõem -------------------------


def test_sinan_casa_o_clima_pela_notificacao(clima) -> None:
    """A asserção que o M127 é.

    Antes da correção este valor era ~20,5 — a janela do nascimento.
    """
    s = _saude("_m127_sinan",
               notification_date=["2023-03-20", "2023-03-25"],
               birth_date=["2023-01-10", "2023-01-15"])
    assert _valor(s, clima) == pytest.approx(MARCO, abs=1.0)


def test_sim_continua_casando_pelo_obito(clima) -> None:
    """Contrapartida: o SIM já estava certo e não pode ter regredido.

    `death_date` é o primeiro da lista em qualquer ordenação, então este
    teste falha se a correção tiver mexido em algo que não devia.
    """
    s = _saude("_m127_sim",
               death_date=["2023-03-20", "2023-03-25"],
               birth_date=["2023-01-10", "2023-01-15"],
               receipt_date=["2023-02-01", "2023-02-05"])
    assert _valor(s, clima) == pytest.approx(MARCO, abs=1.0)


def test_sinasc_casa_pelo_nascimento(clima) -> None:
    """No SINASC o nascimento **é** o evento, e aí é o certo.

    É o teste que impede a correção de virar "nunca use birth_date",
    que seria errado — a categoria depende do sistema.
    """
    s = _saude("_m127_sinasc", birth_date=["2023-01-10", "2023-01-15"])
    assert _valor(s, clima, system="SINASC") == pytest.approx(JANEIRO, abs=1.0)


# --- as quatro partes da correção --------------------------------------


def test_a_lista_common_poe_evento_antes_de_nascimento() -> None:
    """Sem `system`, é a `common` que decide — e ela estava errada.

    Ordenada por categoria, não por frequência: nascimento por último.
    """
    from climasus4py.core.aggregate import _agg_config

    common = _agg_config()["date_candidates"]["common"]
    assert common.index("notification_date") < common.index("birth_date")
    assert common.index("admission_date") < common.index("birth_date")
    assert common.index("death_date") < common.index("birth_date")


def test_o_sistema_vem_do_sus_meta(clima) -> None:
    """Chamada sem `system=` não pode perder a prioridade do sistema.

    Mesmo raciocínio do M21 no `sus_data_aggregate`.
    """
    from climasus4py.core._stage import get_meta, set_stage

    s = _saude("_m127_meta", birth_date=["2023-01-10", "2023-01-15"])
    com_meta = set_stage(s, "spatial", system="SINASC")
    assert get_meta(com_meta).get("system") == "SINASC"
    assert _valor(com_meta, clima) == pytest.approx(JANEIRO, abs=1.0)


def test_date_col_explicito_vence_a_deteccao(clima) -> None:
    """O escape que faltava: quando a detecção erra, tem de haver saída."""
    s = _saude("_m127_esc",
               notification_date=["2023-03-20", "2023-03-25"],
               birth_date=["2023-01-10", "2023-01-15"])
    assert _valor(s, clima, date_col="birth_date") == pytest.approx(
        JANEIRO, abs=1.0)
    assert _valor(s, clima, date_col="notification_date") == pytest.approx(
        MARCO, abs=1.0)


def test_date_col_inexistente_levanta(clima) -> None:
    """Cair de volta na detecção seria o pior dos dois mundos."""
    s = _saude("_m127_ruim", notification_date=["2023-03-20", "2023-03-25"])
    with pytest.raises(ValueError, match="is not a column of health_data"):
        cs.sus_climate_aggregate(
            health_data=s, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False,
            date_col="nao_existe")


def test_avisa_quando_ha_mais_de_uma_candidata(clima) -> None:
    """A escolha era silenciosa, e é isso que torna o defeito perigoso."""
    s = _saude("_m127_av",
               notification_date=["2023-03-20", "2023-03-25"],
               birth_date=["2023-01-10", "2023-01-15"])
    with pytest.warns(UserWarning, match="more than one date column"):
        cs.sus_climate_aggregate(
            health_data=s, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False)


def test_nao_avisa_com_uma_so_candidata(clima) -> None:
    """Contrapartida: aviso em caso sem ambiguidade vira ruído."""
    s = _saude("_m127_uma", death_date=["2023-03-20", "2023-03-25"])
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        _valor(s, clima)
    assert [w for w in capturados
            if "more than one date column" in str(w.message)] == []


def test_o_aviso_sai_uma_vez_por_processo(clima) -> None:
    """Nove estratégias detectam a data; nove avisos iguais seriam ruído."""
    s = _saude("_m127_umavez",
               notification_date=["2023-03-20", "2023-03-25"],
               birth_date=["2023-01-10", "2023-01-15"])

    # Sem o `_valor`, que silencia os avisos para medir so o numero.
    def chamar():
        cs.sus_climate_aggregate(
            health_data=s, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False)

    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        chamar()
        chamar()
    relevantes = [w for w in capturados
                  if "more than one date column" in str(w.message)]
    assert len(relevantes) == 1


# --- o contrato interno -------------------------------------------------


def test_as_estrategias_usam_o_detector_unico() -> None:
    """Nenhuma pode voltar a detectar por conta própria.

    Eram nove chamadas independentes a `detect_date_column`, e é por isso
    que o `system` não chegava a lugar nenhum. Se alguém reintroduzir uma,
    o defeito volta para aquela estratégia só — o pior caso, porque as
    outras continuariam certas.
    """
    import inspect

    src = inspect.getsource(ca)
    assert "detect_date_column(list(health_data.columns))" not in src
    assert src.count("_detect_date(") >= 9


def test_o_detector_respeita_o_sistema_em_forca() -> None:
    """Direto no detector, sem passar pela agregação inteira."""
    cols = ["notification_date", "birth_date", "death_date"]
    tok = ca._SYSTEM.set("SINAN-DENG")
    try:
        assert ca._detect_date(cols) == "notification_date"
    finally:
        ca._SYSTEM.reset(tok)

    tok = ca._SYSTEM.set("SINASC")
    try:
        assert ca._detect_date(cols) == "birth_date"
    finally:
        ca._SYSTEM.reset(tok)
