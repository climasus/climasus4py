"""Tests for sus_mod_plot_af."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import pandas as pd
import pytest

pytest.importorskip("plotnine")

from climasus4py.viz.mod_plot_af import sus_mod_plot_af  # noqa: E402


@pytest.fixture
def af_fit():
    total = pd.DataFrame(
        {
            "component": ["total", "heat", "cold"],
            "threshold": [None, 25.0, 25.0],
            "af": [0.05, 0.03, 0.02],
            "af_lo": [0.03, 0.02, 0.01],
            "af_hi": [0.07, 0.04, 0.03],
            "af_pct": [5.0, 3.0, 2.0],
            "af_pct_lo": [3.0, 2.0, 1.0],
            "af_pct_hi": [7.0, 4.0, 3.0],
            "an": [50.0, 30.0, 20.0],
            "an_lo": [30.0, 20.0, 10.0],
            "an_hi": [70.0, 40.0, 30.0],
            "n_cases": [1000, 1000, 1000],
        }
    )
    by_quantile = pd.DataFrame(
        {
            "component": ["hot", "cold"],
            "quantile_prob": [0.95, 0.05],
            "quantile_label": ["Above P95", "Below P5"],
            "threshold_val": [28.0, 15.0],
            "af": [0.02, 0.01],
            "af_lo": [0.01, 0.005],
            "af_hi": [0.03, 0.015],
            "af_pct": [2.0, 1.0],
            "an": [20.0, 10.0],
            "an_lo": [10.0, 5.0],
            "an_hi": [30.0, 15.0],
        }
    )
    meta = {"outcome_col": "n_obitos", "climate_col": "tmean", "ref_value": 22.0}
    return {"total": total, "by_quantile": by_quantile, "meta": meta}


def test_bar_plot(af_fit):
    from plotnine import ggplot

    p = sus_mod_plot_af(af_fit, type="bar", lang="pt")
    assert isinstance(p, ggplot)
    p.draw()


def test_forest_plot(af_fit):
    from plotnine import ggplot

    p = sus_mod_plot_af(af_fit, type="forest", lang="en")
    assert isinstance(p, ggplot)
    p.draw()


def test_quantile_plot(af_fit):
    from plotnine import ggplot

    p = sus_mod_plot_af(af_fit, type="quantile", lang="es")
    assert isinstance(p, ggplot)
    p.draw()


def test_quantile_plot_empty_raises(af_fit):
    af_fit["by_quantile"] = pd.DataFrame()
    with pytest.raises(ValueError):
        sus_mod_plot_af(af_fit, type="quantile")


def test_output_type_table(af_fit):
    tbl = sus_mod_plot_af(af_fit, type="bar", output_type="table")
    assert isinstance(tbl, pd.DataFrame)


def test_output_type_all(af_fit):
    out = sus_mod_plot_af(af_fit, type="forest", output_type="all")
    assert set(out.keys()) == {"plot", "table", "data"}
    assert isinstance(out["table"], pd.DataFrame)


def test_interactive_not_supported(af_fit):
    with pytest.raises(ImportError):
        sus_mod_plot_af(af_fit, type="bar", interactive=True)


def test_bad_input_type():
    with pytest.raises(TypeError):
        sus_mod_plot_af({"not": "an af dict"})
