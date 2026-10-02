"""M130 e M129: a distância entre município e estação, e o que fazer com ela.

**M130 — a distância era medida em graus.** O `_match_spatial` montava um
`cKDTree` sobre pares `(lon, lat)` crus e convertia com
`distances_deg * 111.0`. Dois erros no mesmo lugar:

| | reportado antes | real |
|---|---|---|
| 1 grau a leste, em SP | 111 km | **102,4 km** |
| 5 graus a leste | 555 km | **511,8 km** |
| 1 grau ao norte | 111 km | 111,2 km |

Um grau de longitude não vale um grau de latitude, e o fator único de 111
só serve para a latitude. Mas o número reportado era o menor problema: a
escolha mudava. Duas estações que **empatam** no espaço de graus — uma a
um grau ao norte, outra a um grau a leste — estão de fato a 111,2 e
102,4 km, e o `cKDTree` desempatava pela ordem do índice, podendo
escolher a que está 8,8 km mais longe.

A correção não é escalar por `cos(lat)`, que seria aproximado: é
converter para cartesianas na esfera unitária. A distância de **corda** é
monotônica com a geodésica, então o vizinho mais próximo fica exato, e a
corda vira arco sem aproximação — `d = 2R·asin(corda/2)`. Continua uma
árvore KD, com o mesmo custo. Medido: erro **zero** contra a geodésica.

Mesma família do M84, que registramos no R — só que aqui é o R que está
certo: o `sf::st_nearest_feature` dele já trabalha sobre o elipsoide.

**M129 — e nada obrigava a estação a estar perto.** Casar com a mais
próxima é só isso: a mais próxima. Medido sobre SIM-DO SP 2023 filtrado,
29 das 5.196 linhas são de residentes de outros estados — entre elas
Pará e Ceará —, e o município do Pará ficava casado com uma estação de
São Paulo a ~2.800 km. Nenhum dos dois pacotes recusava nem sinalizava;
os dois imprimiam o máximo num log que passa batido, e o número seguia
adiante como se fosse exposição.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest
from scipy.spatial import cKDTree

import climasus4py as cs
from climasus4py.core.engine import get_connection

_R_KM = 6371.0


def _geodesica(lat1, lon1, lat2, lon2):
    """Haversine, a referência contra a qual a implementação é medida."""
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp, dl = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * _R_KM * np.arcsin(np.sqrt(a))


def _esfera(lonlat):
    """A conversão que o módulo passou a usar."""
    arr = np.asarray(lonlat, dtype=float)
    lon, lat = np.radians(arr[:, 0]), np.radians(arr[:, 1])
    return np.column_stack([np.cos(lat) * np.cos(lon),
                            np.cos(lat) * np.sin(lon),
                            np.sin(lat)])


def _dist(lon1, lat1, lon2, lat2):
    """Distância pelo caminho do módulo: corda na esfera, depois arco."""
    t = cKDTree(_esfera([[lon2, lat2]]))
    corda, _ = t.query(_esfera([[lon1, lat1]]), k=1)
    return float(2 * _R_KM * np.arcsin(np.clip(corda[0] / 2, 0, 1)))


# --- M130: a distância -------------------------------------------------


@pytest.mark.parametrize(
    ("dlon", "dlat", "rotulo"),
    [(1, 0, "1 grau a leste"), (0, 1, "1 grau ao norte"),
     (5, 0, "5 graus a leste"), (0, 5, "5 graus ao norte"),
     (2, 20, "Pará, ao norte"), (-7, -2, "sudoeste")],
)
def test_a_distancia_bate_com_a_geodesica(dlon, dlat, rotulo) -> None:
    """Erro zero, não "próximo o suficiente".

    A conversão corda→arco é exata, então não há tolerância a conceder.
    """
    lon0, lat0 = -46.0, -23.0
    obtida = _dist(lon0, lat0, lon0 + dlon, lat0 + dlat)
    esperada = _geodesica(lat0, lon0, lat0 + dlat, lon0 + dlon)
    assert obtida == pytest.approx(esperada, abs=1e-6), rotulo


def test_um_grau_de_longitude_nao_vale_um_de_latitude() -> None:
    """A premissa que o fator único de 111 violava.

    Sem esta, o teste acima poderia passar por coincidência num caso em
    que as duas medidas calham de coincidir.
    """
    leste = _dist(-46.0, -23.0, -45.0, -23.0)
    norte = _dist(-46.0, -23.0, -46.0, -22.0)
    assert leste == pytest.approx(102.4, abs=0.5)
    assert norte == pytest.approx(111.2, abs=0.5)
    assert norte - leste > 8.0


def test_a_escolha_da_estacao_deixou_de_depender_da_ordem() -> None:
    """O defeito que mudava resultado, e não só o número impresso.

    Em graus as duas estações empatam. Na esfera, a do leste está 8,8 km
    mais perto — e é ela que tem de ser escolhida.
    """
    muni = [[-46.0, -23.0]]
    estacoes = [[-46.0, -22.0],    # 1 grau ao norte
                [-45.0, -23.0]]    # 1 grau a leste
    reais = [_geodesica(-23, -46, la, lo) for lo, la in estacoes]
    certa = int(np.argmin(reais))

    _, escolhida = cKDTree(_esfera(estacoes)).query(_esfera(muni), k=1)
    assert int(escolhida[0]) == certa

    # contraprova: a árvore antiga, em graus, escolhia a outra
    _, antiga = cKDTree(np.asarray(estacoes)).query(np.asarray(muni), k=1)
    assert int(antiga[0]) != certa


def test_o_modulo_nao_usa_mais_o_fator_de_111() -> None:
    """Se alguém reintroduzir a conversão por fator único, o resto cai.

    Varre só o **código**: a conversão antiga aparece no comentário que
    explica o defeito, e esse comentário tem de continuar lá.
    """
    import inspect

    from climasus4py.enrichment import climate_aggregate as ca

    linhas = inspect.getsource(ca).splitlines()
    codigo = "\n".join(l for l in linhas if not l.lstrip().startswith("#"))

    assert "distances_deg * 111.0" not in codigo
    assert "* 111.0" not in codigo
    assert "_esfera(" in codigo
    # e o comentário que explica o porquê continua no arquivo
    assert "M130" in "\n".join(linhas)


# --- M129: o guarda de distância ---------------------------------------


@pytest.fixture
def cenario():
    """São Paulo e Belém/PA, com estação só em São Paulo.

    Belém fica a ~2.480 km da estação — a situação real medida no SIM-DO,
    onde residentes de outros estados morrem em SP.
    """
    conn = get_connection()
    conn.register("_md_saude", pd.DataFrame({
        "CODMUNRES": ["355030", "150140"],
        "death_date": pd.to_datetime(["2023-03-20", "2023-03-21"]),
    }))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        geo = cs.sus_spatial_join(conn.table("_md_saude"))

    dias = pd.date_range("2023-02-01", "2023-04-30", freq="D")
    conn.register("_md_clima", pd.DataFrame({
        "station_code": ["SP1"] * len(dias), "date": dias,
        "latitude": [-23.5] * len(dias), "longitude": [-46.6] * len(dias),
        "tair_dry_bulb_c": np.linspace(20, 26, len(dias)),
    }))
    return geo, conn.table("_md_clima")


def _rodar(geo, clima, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = cs.sus_climate_aggregate(
            health_data=geo, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False, **kw)
    d = r.df()
    return d[next(c for c in d.columns if "tair" in c)]


def test_sem_o_guarda_o_municipio_distante_recebe_valor(cenario) -> None:
    """O comportamento anterior, fixado para o contraste ficar visível.

    Belém recebe a temperatura de São Paulo. É isso que o guarda existe
    para impedir — e é o default, porque mudá-lo calado alteraria
    resultados de quem já usa.
    """
    valores = _rodar(*cenario)
    assert valores.notna().all()


def test_com_o_guarda_o_municipio_distante_vira_nulo(cenario) -> None:
    valores = _rodar(*cenario, max_distance_km=300)
    assert valores.isna().sum() == 1
    assert valores.notna().sum() == 1


def test_o_municipio_proximo_nao_e_afetado(cenario) -> None:
    """A contrapartida: o guarda não pode derrubar quem está perto."""
    sem = _rodar(*cenario)
    com = _rodar(*cenario, max_distance_km=300)
    assert com.dropna().iloc[0] == pytest.approx(sem.iloc[1], abs=1e-6)


def test_o_aviso_nomeia_o_municipio_e_a_distancia(cenario) -> None:
    """"1 município descartado" não é acionável; "150140 a 2480 km" é."""
    geo, clima = cenario
    with pytest.warns(UserWarning, match="max_distance_km") as capturado:
        cs.sus_climate_aggregate(
            health_data=geo, climate_data=clima,
            temporal_strategy="moving_window", window_days=14,
            climate_vars=["tair_dry_bulb_c"], verbose=False,
            max_distance_km=300)
    msg = str(capturado[0].message)
    assert "150140" in msg
    assert "2480" in msg or "2479" in msg or "2481" in msg


def test_limite_generoso_nao_descarta_nem_avisa(cenario) -> None:
    """Guarda que dispara quando não devia vira ruído e acaba ignorado."""
    geo, clima = cenario
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        valores = _rodar(geo, clima, max_distance_km=5000)
    assert valores.notna().all()
    assert [w for w in capturados if "max_distance_km" in str(w.message)] == []


def test_o_parametro_existe_e_e_opcional() -> None:
    import inspect

    p = inspect.signature(cs.sus_climate_aggregate).parameters
    assert "max_distance_km" in p
    assert p["max_distance_km"].default is None
