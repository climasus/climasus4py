"""O nome canonico da coluna de estacao e ``station_code`` (M10).

O ``sus_climate_inmet`` emitia ``wmo_code`` enquanto heatwaves, coldwaves e os
seis helpers exigiam ``station_code`` -- oito funcoes inutilizaveis com a saida
do proprio pacote, e o erro era so um nome.

O nome antigo veio do arquivo do INMET, que rotula o campo como
``CODIGO (WMO):`` e coloca dentro dele o codigo do INMET, com prefixo de letra
(A701, A806, A828). Codigo WMO e numerico de cinco digitos -- Sao Paulo e
83781. Conferido nos fixtures de 2008, 2015 e 2023: o rotulo esta errado na
fonte nos tres, e o parser o transcreveu literalmente. O ``climasus4r`` guarda
os mesmos valores sob ``station_code``.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.core.climate_inmet import _cast_inmet_types
from climasus4py.core.engine import get_connection
from climasus4py.utils.inmet_parser import _METADATA_KEYS


def _rel(dados: dict):
    return get_connection().from_df(pd.DataFrame(dados))


class TestParserEmiteStationCode:
    @pytest.mark.parametrize("rotulo", ["codigo estacao", "codigo (wmo)"])
    def test_both_inmet_headers_map_to_station_code(self, rotulo):
        """Os dois rotulos que o INMET usa apontam para a mesma coluna.

        Nao e conflacao de identificadores diferentes: ambos carregam o codigo
        da estacao, verificado nos fixtures.
        """
        assert _METADATA_KEYS[rotulo] == "station_code"

    def test_no_metadata_key_maps_to_wmo_code(self):
        assert "wmo_code" not in set(_METADATA_KEYS.values())


class TestCacheAntigoEhNormalizado:
    """Parquet gravado antes da correcao ainda tem wmo_code em disco.

    Sem normalizar na saida, uma relacao lida do cache carregaria o nome antigo
    para fora do pacote e o bloqueio persistiria -- exatamente como antes, mas
    so para quem ja tinha cache. Nao da para mudar arquivo em disco nem pedir
    redownload.
    """

    def test_legacy_wmo_code_is_renamed(self):
        rel = _rel({"wmo_code": ["A701"], "tair_dry_bulb_c": [25.0]})
        saida = _cast_inmet_types(rel)
        assert "station_code" in saida.columns
        assert "wmo_code" not in saida.columns
        assert saida.df()["station_code"].tolist() == ["A701"]

    def test_station_code_is_left_alone(self):
        rel = _rel({"station_code": ["A701"], "tair_dry_bulb_c": [25.0]})
        saida = _cast_inmet_types(rel)
        assert "station_code" in saida.columns
        assert saida.df()["station_code"].tolist() == ["A701"]

    def test_station_code_wins_when_both_are_present(self):
        """Cache hibrido: nao inventar coluna duplicada nem sobrescrever."""
        rel = _rel({
            "station_code": ["A701"], "wmo_code": ["XXX"],
            "tair_dry_bulb_c": [25.0],
        })
        saida = _cast_inmet_types(rel)
        assert saida.df()["station_code"].tolist() == ["A701"]

    def test_other_columns_survive_the_rename(self):
        """A renomeacao reprojeta todas as colunas -- nenhuma pode cair."""
        rel = _rel({
            "wmo_code": ["A701"], "station_name": ["X"],
            "tair_dry_bulb_c": [25.0], "latitude": [-23.5],
        })
        saida = _cast_inmet_types(rel)
        assert set(saida.columns) == {
            "station_code", "station_name", "tair_dry_bulb_c", "latitude",
        }


def test_the_value_is_an_inmet_code_not_a_wmo_code():
    """Documenta o fato que decidiu o nome, para nao se perder.

    Se algum dia a coluna passar a trazer codigo WMO de verdade (numerico de
    cinco digitos), este teste falha e a discussao se reabre com evidencia.
    """
    from pathlib import Path

    fixture = Path("tests/fixtures/inmet/inmet_2023_SC_A806_FLORIANOPOLIS_stub.CSV")
    if not fixture.exists():
        pytest.skip("fixture do INMET ausente")
    linhas = fixture.read_text(encoding="latin-1").splitlines()[:5]
    linha = next(ln for ln in linhas if "CODIGO" in ln.upper())
    assert "WMO" in linha.upper(), "o rotulo da fonte mudou; reveja o mapeamento"
    valor = linha.split(";")[1].strip()
    assert valor.startswith("A"), valor
    assert not valor.isdigit(), f"{valor} parece codigo WMO de verdade"
