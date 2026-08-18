"""Tests for sus_mod_swot."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_swot import sus_mod_swot


@pytest.fixture
def vi_result():
    vt = pd.DataFrame(
        {
            "city": ["fortaleza", "curitiba"],
            "vi_score": [0.72, 0.31],
            "exposure_score": [80.0, 30.0],
            "sensitivity_score": [65.0, 40.0],
            "adaptive_capacity_score": [40.0, 75.0],
            "vi_rank": [1, 2],
            "vi_percentile": [90.0, 20.0],
        }
    )
    return {"vi_table": vt, "meta": {"city_col": "city"}}


@pytest.fixture
def af_result():
    total = pd.DataFrame(
        {"component": ["total", "heat", "cold"], "af_pct": [5.0, 3.0, 2.0]}
    )
    return {"total": total}


@pytest.fixture
def burden_result():
    bt = pd.DataFrame({"city": ["fortaleza", "curitiba"], "rank": [1, 2], "an": [50.0, 20.0]})
    return {"burden_table": bt}


@pytest.fixture
def dlnm_result():
    er = pd.DataFrame({"pct": [0.75, 0.95], "rr": [1.2, 1.8]})
    return {"exposure_response": er}


@pytest.fixture
def sensitivity_result():
    comp = pd.DataFrame({"hot_rr": [1.5, 1.1]})
    return {"comparison": comp}


def test_swot_all_inputs(vi_result, af_result, burden_result, dlnm_result, sensitivity_result):
    swot = sus_mod_swot(
        vulnerability=vi_result,
        af=af_result,
        burden=burden_result,
        dlnm=dlnm_result,
        sensitivity=sensitivity_result,
        lang="en",
        verbose=False,
    )
    assert set(swot["scores"].columns) >= {
        "entity", "S_score", "W_score", "O_score", "T_score", "S_cat", "W_cat", "O_cat", "T_cat",
    }
    assert set(swot["scores"]["entity"]) == {"fortaleza", "curitiba"}
    assert swot["meta"]["inputs_used"] == [
        "climasus_vi", "climasus_af", "climasus_burden", "climasus_dlnm", "climasus_sensitivity",
    ]
    assert len(swot["indicators"]) == swot["meta"]["n_indicators"]


def test_swot_numeric_only_has_no_cat_columns(vi_result):
    swot = sus_mod_swot(vulnerability=vi_result, score_type="numeric", verbose=False)
    assert "S_cat" not in swot["scores"].columns


def test_swot_no_input_raises():
    with pytest.raises(ValueError):
        sus_mod_swot()


def test_swot_bad_breaks_raises(vi_result):
    with pytest.raises(ValueError):
        sus_mod_swot(vulnerability=vi_result, breaks=(90, 10))


def test_swot_bad_labels_raises(vi_result):
    with pytest.raises(ValueError):
        sus_mod_swot(vulnerability=vi_result, labels=["only_one"])


def test_swot_bad_af_type_raises():
    with pytest.raises(TypeError):
        sus_mod_swot(af={"not": "shaped right"})


def test_swot_entities_fallback_overall(af_result):
    swot = sus_mod_swot(af=af_result, verbose=False)
    assert swot["scores"]["entity"].tolist() == ["overall"]
