"""sus_filter: municipality=, city=, uf= and region= on every column layout (M241).

Three defects in the same family, all measured on SIM-DO/SP/2023 and
SINAN-DENGUE before the fix:

- municipality="3550308" -- the full 7-digit IBGE code -- returned 0 rows
  without a warning; DATASUS records the 6-digit code ("355030").
- after sus_data_standardize(lang="pt") the municipality filter raised "No
  municipality column found": only raw and English names were looked for.
- uf="MT" on a standardized SINAN frame returned 0 rows without a warning:
  its notification UF holds the IBGE code "51", not the abbreviation.
"""

from __future__ import annotations

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection


def _rel(**cols):
    return get_connection().from_df(pd.DataFrame(cols))


@pytest.mark.parametrize("coluna", [
    "CODMUNRES",                      # raw SIM
    "residence_municipality_code",    # lang="en"
    "codigo_municipio_residencia",    # lang="pt" and lang="es"
])
@pytest.mark.parametrize("codigo", ["355030", "3550308"])
def test_municipality_accepts_6_or_7_digits_in_any_language(coluna, codigo):
    rel = _rel(**{coluna: ["355030", "355030", "330455"]})
    assert cs.sus_filter(rel, municipality=codigo).count("*").fetchone()[0] == 2


def test_municipality_matches_a_7_digit_column_too():
    rel = _rel(codigo_municipio_residencia=["3550308", "3304557"])
    assert cs.sus_filter(rel, municipality="355030").count("*").fetchone()[0] == 1


def test_a_name_in_municipality_points_to_city():
    rel = _rel(CODMUNRES=["355030"])
    with pytest.raises(ValueError, match="city="):
        cs.sus_filter(rel, municipality="São Paulo")


def test_city_uses_the_same_column_detection(monkeypatch):
    from climasus4py.core import filter as filtro
    monkeypatch.setattr(filtro, "expand_city_to_codes", lambda city: ["355030"])
    rel = _rel(codigo_municipio_residencia=["355030", "330455"])
    assert cs.sus_filter(rel, city="São Paulo").count("*").fetchone()[0] == 1


@pytest.mark.parametrize("pedido", ["MT", "mt", "51"])
def test_uf_on_a_column_of_ibge_codes(pedido):
    """SINAN keeps the IBGE code in notification_uf after standardization."""
    rel = _rel(notification_uf=["51", "51", "50"])
    assert cs.sus_filter(rel, uf=pedido).count("*").fetchone()[0] == 2


@pytest.mark.parametrize("pedido", ["SP", "35"])
def test_uf_on_a_column_of_abbreviations(pedido):
    rel = _rel(UF_ZI=["SP", "RJ"])
    assert cs.sus_filter(rel, uf=pedido).count("*").fetchone()[0] == 1


def test_uf_in_portuguese_resolves_the_notification_column():
    rel = _rel(uf_notificacao=["51", "50"], codigo_municipio_residencia=["500270", "500270"])
    # Notification UF, as in English, not the residence municipality.
    assert cs.sus_filter(rel, uf="MT").count("*").fetchone()[0] == 1


def test_uf_derived_from_municipality_accepts_a_code():
    rel = _rel(codigo_municipio_residencia=["355030", "330455"])
    assert cs.sus_filter(rel, uf="35").count("*").fetchone()[0] == 1


def test_unknown_uf_raises():
    rel = _rel(UF_ZI=["SP"])
    with pytest.raises(ValueError, match="Unknown UF"):
        cs.sus_filter(rel, uf="XX")


def test_region_on_a_column_of_ibge_codes():
    rel = _rel(notification_uf=["51", "50", "35"])
    assert cs.sus_filter(rel, region="centro_oeste").count("*").fetchone()[0] == 2


def test_region_without_uf_column_derives_from_municipality():
    """It used to skip the filter with a warning, returning everything."""
    rel = _rel(codigo_municipio_residencia=["355030", "330455", "530010"])
    assert cs.sus_filter(rel, region="sudeste").count("*").fetchone()[0] == 2
