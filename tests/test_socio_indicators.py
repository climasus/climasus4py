"""Tests for sus_socio_compute_indicators / sus_socio_list_indicators."""

from __future__ import annotations

import math

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.enrichment.socio_indicators import (
    sus_socio_compute_indicators,
    sus_socio_list_indicators,
)

# ---------------------------------------------------------------------------
# sus_socio_list_indicators
# ---------------------------------------------------------------------------


def test_list_indicators_default_pt():
    out = sus_socio_list_indicators()
    assert isinstance(out, pd.DataFrame)
    expected_cols = {
        "id",
        "name",
        "category",
        "required_cols",
        "formula",
        "multiplier",
        "unit",
        "uncertainty_method",
        "source",
    }
    assert expected_cols.issubset(out.columns)
    assert "dependency_ratio" in out["id"].tolist()
    # PT name for gini_index
    row = out[out["id"] == "gini_index"].iloc[0]
    assert row["name"] == "Índice de Gini"
    assert row["formula"] == "gini_value"


def test_list_indicators_lang_en():
    out = sus_socio_list_indicators(lang="en")
    row = out[out["id"] == "homicide_rate"].iloc[0]
    assert row["name"] == "Homicide Rate"


def test_list_indicators_filter_by_category():
    out = sus_socio_list_indicators(category="mortality")
    assert set(out["category"]) == {"mortality"}
    assert "infant_mortality_rate" in out["id"].tolist()
    assert "dependency_ratio" not in out["id"].tolist()


def test_list_indicators_filter_by_category_list():
    out = sus_socio_list_indicators(category=["mortality", "demographic"])
    assert set(out["category"]) == {"mortality", "demographic"}


# ---------------------------------------------------------------------------
# sus_socio_compute_indicators — known-value formula checks
# ---------------------------------------------------------------------------


def _base_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "pop_young": [200.0],
            "pop_elderly": [100.0],
            "pop_working": [700.0],
            "pop_urban": [80.0],
            "pop_total": [100.0],
            "deaths_infant": [5.0],
            "live_births": [1000.0],
            "gini_value": [0.55],
        }
    )


def test_dependency_ratio_known_value():
    df = _base_df()
    out = sus_socio_compute_indicators(
        df, indicators=["dependency_ratio"], verbose=False, add_ci=False
    )
    # (200 + 100) / 700 * 100 = 42.857142...
    expected = (200.0 + 100.0) / 700.0 * 100
    assert math.isclose(out["ind_dependency_ratio"].iloc[0], expected, rel_tol=1e-9)


def test_gini_index_passthrough_no_multiplier():
    df = _base_df()
    out = sus_socio_compute_indicators(
        df, indicators=["gini_index"], verbose=False, add_ci=False
    )
    assert out["ind_gini_index"].iloc[0] == pytest.approx(0.55)


def test_urbanization_rate_with_binomial_ci():
    df = _base_df()
    out = sus_socio_compute_indicators(
        df,
        indicators=["urbanization_rate"],
        verbose=False,
        add_ci=True,
        confidence_level=0.95,
    )
    expected = 80.0 / 100.0 * 100
    assert out["ind_urbanization_rate"].iloc[0] == pytest.approx(expected)
    # Wilson-score CI must bracket the point estimate and stay within [0, 100]
    low = out["ind_urbanization_rate_low"].iloc[0]
    high = out["ind_urbanization_rate_high"].iloc[0]
    assert 0 <= low <= expected <= high <= 100


def test_infant_mortality_rate_with_poisson_ci():
    df = _base_df()
    out = sus_socio_compute_indicators(
        df,
        indicators=["infant_mortality_rate"],
        verbose=False,
        add_ci=True,
        confidence_level=0.95,
    )
    expected = 5.0 / 1000.0 * 1000  # per 1,000 live births
    assert out["ind_infant_mortality_rate"].iloc[0] == pytest.approx(expected)
    low = out["ind_infant_mortality_rate_low"].iloc[0]
    high = out["ind_infant_mortality_rate_high"].iloc[0]
    assert low <= expected <= high


def test_poisson_ci_zero_events_gives_zero_low_not_nan():
    """R special-cases k == 0 with ifelse(k == 0, 0, qgamma(...)) so the
    lower bound is exactly 0 instead of NaN (qgamma with shape=0 is
    undefined). Verify the vectorised Python port keeps that special case.
    """
    df = pd.DataFrame({"deaths_infant": [0.0], "live_births": [1000.0]})
    out = sus_socio_compute_indicators(
        df, indicators=["infant_mortality_rate"], verbose=False, add_ci=True
    )
    assert out["ind_infant_mortality_rate"].iloc[0] == pytest.approx(0.0)
    assert out["ind_infant_mortality_rate_low"].iloc[0] == pytest.approx(0.0)
    assert not math.isnan(out["ind_infant_mortality_rate_high"].iloc[0])


def test_col_mapping_overrides_formula_names():
    df = pd.DataFrame({"V003_sum": [500.0], "V111_sum": [400.0]})
    out = sus_socio_compute_indicators(
        df,
        indicators=["water_connection_rate"],
        col_mapping={"total_hh": "V003_sum", "hh_water": "V111_sum"},
        verbose=False,
        add_ci=False,
    )
    expected = 400.0 / 500.0 * 100
    assert out["ind_water_connection_rate"].iloc[0] == pytest.approx(expected)


def test_auto_detect_indicators_when_none_given():
    df = _base_df()
    out = sus_socio_compute_indicators(df, verbose=False, add_ci=False)
    # dependency_ratio, urbanization_rate, gini_index are all computable
    assert "ind_dependency_ratio" in out.columns
    assert "ind_gini_index" in out.columns
    assert "ind_urbanization_rate" in out.columns
    # infant_mortality_rate needs deaths_infant/live_births — present too
    assert "ind_infant_mortality_rate" in out.columns


def test_missing_columns_skips_with_warning_not_abort():
    df = pd.DataFrame({"pop_total": [100.0]})  # missing cnes_beds
    with (
        pytest.warns(UserWarning, match="beds_per_capita"),
        pytest.raises(ValueError, match="Nenhum indicador"),
    ):
        sus_socio_compute_indicators(df, indicators=["beds_per_capita"], verbose=False)


def test_unknown_indicator_id_warns_and_falls_back():
    df = _base_df()
    with pytest.warns(UserWarning, match="unknown_ind_xyz|Unknown"):
        out = sus_socio_compute_indicators(
            df,
            indicators=["unknown_ind_xyz", "gini_index"],
            verbose=False,
            add_ci=False,
        )
    assert "ind_gini_index" in out.columns


def test_no_valid_indicators_raises():
    df = _base_df()
    with pytest.raises(ValueError):
        sus_socio_compute_indicators(df, indicators=["totally_unknown"], verbose=False)


def test_invalid_confidence_level_raises():
    df = _base_df()
    with pytest.raises(ValueError):
        sus_socio_compute_indicators(
            df, indicators=["gini_index"], confidence_level=1.5, verbose=False
        )


def test_lazy_relation_input_materializes_with_warning():
    rel = get_connection().from_df(_base_df())
    with pytest.warns(UserWarning, match="materializada|materialised|materializando"):
        out = sus_socio_compute_indicators(
            rel, indicators=["gini_index"], verbose=False, add_ci=False
        )
    assert isinstance(out, pd.DataFrame)
    assert out["ind_gini_index"].iloc[0] == pytest.approx(0.55)


def test_sus_meta_attrs_recorded():
    df = _base_df()
    out = sus_socio_compute_indicators(
        df, indicators=["gini_index"], verbose=False, add_ci=False
    )
    meta = out.attrs["sus_meta"]
    assert meta["stage"] == "census"
    assert meta["type"] == "indicators"
    assert len(meta["history"]) == 1
    # Preserved R bug: the R source's own glue::glue("[%s] ...") step string
    # never substitutes "%s" with a timestamp (glue only expands {}
    # placeholders); sus_meta()'s generic history helper then prepends a
    # REAL timestamp bracket around that already-broken step string. Net
    # result: a genuine "[<timestamp>] " prefix followed by the literal,
    # never-substituted "[%s]" baked into the step text. See IDEIAS.md.
    entry = meta["history"][0]
    assert "] [%s] Computed 1 indicator(s):" in entry
    assert entry.startswith("[20")  # real leading timestamp, e.g. "[2026-..."


def test_history_lists_first_requested_not_first_successful_id():
    """Preserved R bug: the history message names the first N *requested*
    indicators, not the ones that actually succeeded — because R slices
    ``indicators_to_compute[seq_len(n_computed)]`` instead of tracking
    which ids were actually computed. Here ``beds_per_capita`` is skipped
    (missing column) and ``gini_index`` succeeds, but the history entry
    still names ``beds_per_capita`` because it was first in the request.
    """
    df = pd.DataFrame({"pop_total": [100.0], "gini_value": [0.4]})
    with pytest.warns(UserWarning, match="beds_per_capita"):
        out = sus_socio_compute_indicators(
            df,
            indicators=["beds_per_capita", "gini_index"],
            verbose=False,
            add_ci=False,
        )
    assert "ind_gini_index" in out.columns
    assert "ind_beds_per_capita" not in out.columns
    history = out.attrs["sus_meta"]["history"][0]
    assert "Computed 1 indicator(s): beds_per_capita" in history


def test_invalid_type_raises_type_error():
    with pytest.raises(TypeError):
        sus_socio_compute_indicators([1, 2, 3], verbose=False)
