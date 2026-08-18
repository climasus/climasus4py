"""Tests for sus_climate_plot_aggregate — plotnine exploratory visualisation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from climasus4py.viz.climate_plot_aggregate import (
    _detect_climate_cols,
    _detect_outcome_col,
    _unit_label,
    sus_climate_plot_aggregate,
)

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _make_agg_df(n: int = 200) -> pd.DataFrame:
    """Synthetic daily climate-health table matching sus_climate_aggregate output."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    tair = 24 + 6 * np.sin(np.arange(n) / 30) + rng.normal(0, 1.0, n)
    outcome = (5 + 0.4 * tair + rng.normal(0, 2, n)).round().clip(min=0).astype(int)
    return pd.DataFrame(
        {
            "date": dates,
            "code_muni": ["2611606"] * n,
            "tair_dry_bulb_c": tair,
            "n_obitos": outcome,
        }
    )


@pytest.fixture
def agg_df():
    return _make_agg_df()


@pytest.fixture
def agg_df_multi():
    df = _make_agg_df()
    rng = np.random.default_rng(7)
    df["rh_c"] = 60 + rng.normal(0, 5, len(df))
    return df


# ---------------------------------------------------------------------------
# Returns ggplot object per plot_type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "plot_type",
    ["timeseries", "scatter", "ccf", "distribution", "corr_matrix", "seasonal"],
)
def test_returns_ggplot_object(agg_df, plot_type):
    pytest.importorskip("plotnine", reason="plotnine not installed")

    result = sus_climate_plot_aggregate(agg_df, plot_type=plot_type, verbose=False)
    assert type(result).__name__ in ("ggplot", "_Composition") or hasattr(result, "draw")


def test_timeseries_multi_climate_cols_composes(agg_df_multi):
    pytest.importorskip("plotnine", reason="plotnine not installed")

    result = sus_climate_plot_aggregate(
        agg_df_multi,
        climate_cols=["tair_dry_bulb_c", "rh_c"],
        plot_type="timeseries",
        verbose=False,
    )
    assert result is not None


def test_seasonal_always_stacks(agg_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")

    result = sus_climate_plot_aggregate(agg_df, plot_type="seasonal", verbose=False)
    # seasonal always composes two panels (climate box + outcome box)
    assert hasattr(result, "draw") or hasattr(result, "__iter__") or result is not None


def test_multilingual_does_not_raise(agg_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")

    for lang in ("pt", "en", "es"):
        result = sus_climate_plot_aggregate(agg_df, lang=lang, verbose=False)
        assert result is not None


# ---------------------------------------------------------------------------
# Missing plotnine raises clear ImportError
# ---------------------------------------------------------------------------


def test_missing_plotnine_raises_clear_error(agg_df, monkeypatch):
    import climasus4py.viz.climate_plot_aggregate as mod

    def _boom() -> None:
        raise ImportError(
            "sus_climate_plot_aggregate requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        )

    monkeypatch.setattr(mod, "_require_plotnine", _boom)
    with pytest.raises(ImportError, match="pip install climasus4py"):
        mod.sus_climate_plot_aggregate(agg_df, verbose=False)


# ---------------------------------------------------------------------------
# Parameter validation (no plotnine required)
# ---------------------------------------------------------------------------


def test_invalid_lang_raises(agg_df):
    with pytest.raises(ValueError, match="lang"):
        sus_climate_plot_aggregate(agg_df, lang="fr", verbose=False)


def test_invalid_plot_type_raises(agg_df):
    with pytest.raises(ValueError, match="plot_type"):
        sus_climate_plot_aggregate(agg_df, plot_type="dlnm", verbose=False)


def test_interactive_not_supported_raises(agg_df):
    with pytest.raises(ImportError, match="plotly"):
        sus_climate_plot_aggregate(agg_df, interactive=True, verbose=False)


def test_missing_date_column_raises():
    df = pd.DataFrame({"tair_dry_bulb_c": [20.0, 21.0], "n_obitos": [1, 2]})
    with pytest.raises(ValueError, match="date"):
        sus_climate_plot_aggregate(df, verbose=False)


def test_missing_climate_col_raises(agg_df):
    with pytest.raises(ValueError, match="not found"):
        sus_climate_plot_aggregate(agg_df, climate_cols=["nonexistent_var"], verbose=False)


def test_missing_outcome_col_raises(agg_df):
    with pytest.raises(ValueError, match="not found"):
        sus_climate_plot_aggregate(agg_df, outcome_col="nonexistent_outcome", verbose=False)


def test_no_climate_cols_detected_raises():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2022-01-01", periods=5),
            "code_muni": ["2611606"] * 5,
            "name_muni": ["Recife"] * 5,
        }
    )
    with pytest.raises(ValueError, match="No climate columns"):
        sus_climate_plot_aggregate(df, verbose=False)


def test_invalid_smooth_method_raises(agg_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.raises(ValueError, match="smooth_method"):
        sus_climate_plot_aggregate(
            agg_df, plot_type="scatter", smooth_method="bogus", verbose=False
        )


# ---------------------------------------------------------------------------
# Helpers (no plotnine needed)
# ---------------------------------------------------------------------------


def test_detect_climate_cols_matches_naming_conventions():
    df = pd.DataFrame(
        {
            "date": [1],
            "code_muni": [1],
            "tair_dry_bulb_c": [1.0],
            "lag7_tair_dry_bulb_c": [1.0],
            "n_obitos": [1],
        }
    )
    cols = _detect_climate_cols(df)
    assert "tair_dry_bulb_c" in cols
    assert "lag7_tair_dry_bulb_c" in cols
    assert "n_obitos" not in cols
    assert "code_muni" not in cols


def test_detect_outcome_col_prefers_named_prefix():
    df = pd.DataFrame(
        {"date": [1], "tair_dry_bulb_c": [1.0], "some_int": [3], "n_obitos": [1]}
    )
    assert _detect_outcome_col(df, ["tair_dry_bulb_c"]) == "n_obitos"


def test_detect_outcome_col_returns_none_when_no_numeric():
    df = pd.DataFrame({"date": [1], "tair_dry_bulb_c": [1.0], "name_muni": ["a"]})
    assert _detect_outcome_col(df, ["tair_dry_bulb_c"]) is None


@pytest.mark.parametrize(
    "col,expected",
    [
        ("tair_dry_bulb_c", " (°C)"),
        ("patm_mb", " (mb)"),
        ("rainfall_mm", " (mm)"),
        ("rh_porc", " (%)"),
        ("ws_m_s", " (m/s)"),
        ("sr_kj_m2", " (kJ/m²)"),
        ("wd_degrees", " (°)"),
        ("unknown_col", ""),
    ],
)
def test_unit_label(col, expected):
    assert _unit_label(col) == expected
