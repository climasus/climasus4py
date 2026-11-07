"""sus_climate_aggregate keeps a DuckDB climate relation in DuckDB (M232).

The hourly climate table used to be copied whole to pandas at the top of
sus_climate_aggregate, only to be validated, and then copied again to Arrow
for the daily pre-aggregation. Validation and climate-region detection now run
as SQL on the relation; the only copy left is the Arrow one the daily step
needs. On SP 2023 (350,400 hourly rows) the pandas copy was 109 MB, and all
ten temporal strategies give identical output either way.

The relation route also exposed a latent bug: from DuckDB 1.4 on,
``relation.arrow()`` returns a one-shot RecordBatchReader, and the daily step
queries its table more than once. It now uses ``fetch_arrow_table()``.
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment import climate_aggregate as ca


def _clima(n_dias: int = 3) -> pd.DataFrame:
    horas = pd.date_range("2023-01-01", periods=24 * n_dias, freq="h")
    linhas = []
    for est, lat in (("A001", -15.0), ("A002", -25.0)):
        for i, h in enumerate(horas):
            linhas.append({"station_code": est, "date": h, "latitude": lat,
                           "longitude": -47.0, "tair_dry_bulb_c": 20.0 + (i % 24) / 2,
                           "rainfall_mm": float(i % 5 == 0)})
    return pd.DataFrame(linhas)


def test_validation_of_a_relation_does_not_need_pandas():
    rel = get_connection().from_df(_clima())
    ca._validate_climate_data(rel)                       # no error
    vazio = get_connection().from_df(_clima().iloc[:0])
    with pytest.raises(ValueError, match="empty"):
        ca._validate_climate_data(vazio)
    sem_data = get_connection().from_df(_clima().drop(columns="date"))
    with pytest.raises(ValueError, match="date"):
        ca._validate_climate_data(sem_data)


def test_date_overlap_on_a_relation_matches_the_pandas_path():
    clima = _clima()
    saude = get_connection().from_df(pd.DataFrame({
        "date": pd.to_datetime(["2023-01-02", "2023-01-05"]), "code_muni": ["355030", "355030"]}))
    for entrada in (get_connection().from_df(clima), clima):
        with pytest.warns(UserWarning, match="ends 2023-01-05 but climate_data ends 2023-01-03"):
            ca._validate_date_overlap(saude, entrada)


def test_climate_region_from_a_relation_is_the_same_mean_latitude():
    clima = _clima()
    rel = get_connection().from_df(clima)
    assert ca._detect_climate_region(rel) == ca._detect_climate_region(clima) == "subtropical"


def test_daily_preaggregation_reads_a_relation_more_than_once():
    """relation.arrow() is a one-shot reader in DuckDB >= 1.4; the daily step
    must get a table, or its second query sees no rows."""
    clima = _clima()
    pela_relacao = ca._aggregate_meteo_daily(get_connection().from_df(clima))
    pelo_pandas = ca._aggregate_meteo_daily(clima)
    assert isinstance(pela_relacao, pa.Table)
    assert pela_relacao.num_rows == pelo_pandas.num_rows == 2 * 3
    ordem = [("station_code", "ascending"), ("date", "ascending")]
    assert pela_relacao.sort_by(ordem).equals(pelo_pandas.sort_by(ordem))
