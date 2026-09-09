"""Tests for DATASUS source metadata consumed from climasus-data."""


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from climasus4py.core.importer import (
    _build_urls,
    _raw_cache_path,
    _state_filter_expression,
    sus_data_import,
)
from climasus4py.utils.data import load_json


def test_datasus_sources_catalog_contains_sinan_dengue():
    catalog = load_json("metadata/datasus_systems.json")

    sinan = catalog["systems"]["SINAN-DENGUE"]

    # A chave "disease_code" == "DENG" saiu do catalogo (M53, 09/09/2026). O
    # metadado vem do climasus-data e nao e nosso para alterar (principio 2 do
    # CLAUDE.md), entao o teste passa a afirmar o que o catalogo de fato
    # publica -- e o que o importador de fato consome: familia, escopo e a
    # coluna de particao por UF. O nome do arquivo DENGBR ficou nos
    # url_templates, que e onde a informacao vive agora.
    assert sinan["family"] == "SINAN"
    assert sinan["geographic_scope"] == "national"
    assert sinan["is_national"] is True
    assert sinan["partition_filter"]["state_column"] == "SG_UF_NOT"
    assert any(
        "DENGBR" in t["path_template"] for t in sinan["url_templates"]
    ), sinan["url_templates"]


def test_build_urls_for_sinan_dengue_from_catalog():
    urls = _build_urls("SINAN-DENGUE", "SP", 2024)

    assert urls == [
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/DENGBR24.dbc",
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/PRELIM/DENGBR24.dbc",
    ]


def test_build_urls_for_existing_state_system_from_catalog():
    urls = _build_urls("SIM-DO", "SP", 2023)

    assert urls == [
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/PRELIM/DORES/DOSP2023.dbc",
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DOSP2023.dbc",
    ]


def test_sinan_state_partition_filter_uses_ibge_uf_code():
    expression = _state_filter_expression("SINAN-DENGUE", ["SP"])

    assert expression == 'TRY_CAST("SG_UF_NOT" AS INTEGER) IN (35)'


def test_raw_cache_path_preserves_source_url_structure(tmp_path):
    raw_dir = tmp_path / "raw"
    path = _raw_cache_path(
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/DENGBR24.dbc",
        raw_dir,
    )

    expected = (
        raw_dir / "ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/DENGBR24.dbc"
    ).resolve()
    assert path == expected


def test_sus_import_filters_cached_national_sinan_by_uf(tmp_path):
    cache_file = tmp_path / "SINAN-DENGUE" / "BR_2024_all.parquet"
    cache_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "NU_NOTIFIC": ["1", "2", "3"],
                    "SG_UF_NOT": [35, 33, 35],
                }
            )
        ),
        cache_file,
    )

    rel = sus_data_import("SINAN-DENGUE", "SP", 2024, cache_dir=tmp_path)

    df = rel.df()
    assert df["NU_NOTIFIC"].tolist() == ["1", "3"]
