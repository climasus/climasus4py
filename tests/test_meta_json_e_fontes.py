"""sus_meta in DataFrame.attrs must survive to_parquet(); grid sources moved.

1. pandas writes ``DataFrame.attrs`` into the Parquet schema as JSON. The
   grid/climate functions stored ``pd.Timestamp`` in ``sus_meta["temporal"]``,
   so ``sus_grid_chirps(...).to_parquet(...)`` failed with "Object of type
   Timestamp is not JSON serializable" (found 07/11/2026). Every
   ``attrs["sus_meta"]`` assignment now goes through ``json_safe_meta``.
2. In Nov/2026 CHIRPS moved from /products/CHIRPS-2.0/ to
   /products/CHIRPS/v2.0/ (old path: 404) and the TerraClimate host
   climate.northwestknowledge.net stopped accepting connections; the
   project's THREDDS server serves the same files.
"""

from __future__ import annotations

import ast
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

import climasus4py
from climasus4py.core._meta_json import json_safe_meta
from climasus4py.enrichment import grid_chirps, grid_pdsi

PKG = Path(climasus4py.__file__).parent


def test_json_safe_meta_converts_every_awkward_type():
    meta = {
        "temporal": {
            "start": pd.Timestamp("2023-01-01"), "end": dt.date(2023, 1, 31),
            "t64": np.datetime64("2023-02-01"), "nat": pd.NaT,
        },
        "n": np.int64(5), "x": np.float32(1.5), "flag": np.bool_(True),
        "years": (2022, 2023), "set": {1}, "arr": np.array([1, 2]),
        "path": Path("a") / "b", "na": pd.NA, "lag": pd.Timedelta(days=2),
        "ok": ["a", 1, 2.5, None, True],
    }
    out = json_safe_meta(meta)
    json.dumps(out)                                          # must not raise
    assert out["temporal"]["start"] == "2023-01-01T00:00:00"
    assert out["temporal"]["end"] == "2023-01-31"
    assert out["temporal"]["nat"] is None and out["na"] is None
    assert out["n"] == 5 and isinstance(out["n"], int)
    assert out["years"] == [2022, 2023] and out["arr"] == [1, 2]
    assert out["ok"] == ["a", 1, 2.5, None, True]


def test_frame_with_meta_round_trips_through_parquet(tmp_path):
    df = pd.DataFrame(
        {"code_muni": ["3550308"], "date": pd.to_datetime(["2023-01-01"]), "v": [1.0]})
    df.attrs["sus_meta"] = json_safe_meta(
        {"temporal": {"start": df["date"].min()}, "years": [2023]})
    df.to_parquet(tmp_path / "x.parquet")
    back = pd.read_parquet(tmp_path / "x.parquet")
    assert back.attrs["sus_meta"]["temporal"]["start"] == "2023-01-01T00:00:00"


def test_every_sus_meta_assignment_is_wrapped():
    """Guard: a new ``X.attrs["sus_meta"] = {...}`` must go through json_safe_meta."""
    soltos = []
    for p in PKG.rglob("*.py"):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                t = node.targets[0]
                if (isinstance(t, ast.Subscript) and isinstance(t.value, ast.Attribute)
                        and t.value.attr == "attrs" and isinstance(t.slice, ast.Constant)
                        and t.slice.value == "sus_meta"):
                    v = node.value
                    embrulhado = (isinstance(v, ast.Call)
                                  and getattr(v.func, "id", None) == "json_safe_meta")
                    if not embrulhado:
                        soltos.append(f"{p.relative_to(PKG)}:{node.lineno}")
    assert soltos == []


def test_chirps_uses_the_new_chc_tree():
    _, url = grid_chirps._chirps_file_info("monthly", 2023, 1, None)
    assert url == ("https://data.chc.ucsb.edu/products/CHIRPS/v2.0/global_monthly/tifs/"
                   "chirps-v2.0.2023.01.tif.gz")


def test_terraclimate_uses_thredds():
    _, url = grid_pdsi._pdsi_file_info("terraclimate", 2023)
    assert url == ("https://thredds.northwestknowledge.net/thredds/fileServer/"
                   "TERRACLIMATE_ALL/data/TerraClimate_PDSI_2023.nc")


def test_smvi_reads_the_wide_hydroshare_layout():
    """HydroShare ships one row per cell with up to six events side by side."""
    from climasus4py.enrichment.grid_smvi import _EVENTS_CSV_PATTERN, _events_long

    nome = "SMVI_GLDAS_Summ_table_SMVI_2021_n864000.csv"
    assert _EVENTS_CSV_PATTERN.search(nome).group(1) == "2021"
    assert _EVENTS_CSV_PATTERN.search("SMVI_GLDAS_2021.csv").group(1) == "2021"
    largo = pd.DataFrame({
        "cell_id": [1, 2],
        "fstdate1": ["2021-03-23", None], "lstdate1": ["2021-04-12", None],
        "fstdate2": ["2021-05-17", None], "lstdate2": ["2021-06-06", None],
        "sv1": [4.9, None], "sv2": [5.3, None],
    })
    longo = _events_long(largo)
    assert longo["cell_id"].tolist() == [1, 1]
    assert longo["fstdate"].tolist() == ["2021-03-23", "2021-05-17"]
    assert longo["sv"].tolist() == [4.9, 5.3]
    plano = pd.DataFrame({"cell_id": [1], "fstdate": ["2021-01-01"], "lstdate": ["2021-01-09"]})
    pd.testing.assert_frame_equal(_events_long(plano), plano)
