"""Tests for sus_mod_plot_sensitivity."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("plotnine")

from climasus4py.viz.mod_plot_sensitivity import sus_mod_plot_sensitivity  # noqa: E402


@pytest.fixture
def sensitivity_fit():
    exposure = np.linspace(10, 35, 20)
    stratum_curves = pd.concat(
        [
            pd.DataFrame(
                {
                    "stratum": "elderly",
                    "label": "elderly",
                    "exposure": exposure,
                    "rr": 1.0 + 0.01 * (exposure - 22),
                    "rr_lo": 0.95 + 0.01 * (exposure - 22),
                    "rr_hi": 1.05 + 0.01 * (exposure - 22),
                }
            ),
            pd.DataFrame(
                {
                    "stratum": "adults",
                    "label": "adults",
                    "exposure": exposure,
                    "rr": 1.0 + 0.005 * (exposure - 22),
                    "rr_lo": 0.97 + 0.005 * (exposure - 22),
                    "rr_hi": 1.03 + 0.005 * (exposure - 22),
                }
            ),
        ],
        ignore_index=True,
    )

    rr_table = pd.DataFrame(
        {
            "stratum": ["elderly", "elderly", "adults", "adults"],
            "label": ["elderly", "elderly", "adults", "adults"],
            "component": ["hot", "cold", "hot", "cold"],
            "quantile_prob": [0.99, 0.01, 0.99, 0.01],
            "exposure": [33.0, 11.0, 33.0, 11.0],
            "rr": [1.15, 1.10, 1.06, 1.03],
            "rr_lo": [1.05, 1.02, 1.01, 0.99],
            "rr_hi": [1.25, 1.18, 1.11, 1.07],
            "ref_exposure": [22.0, 22.0, 22.0, 22.0],
        }
    )

    comparison = pd.DataFrame(
        {
            "stratum": ["elderly", "adults"],
            "label": ["elderly", "adults"],
            "hot_rr": [1.15, 1.06],
            "hot_rr_lo": [1.05, 1.01],
            "hot_rr_hi": [1.25, 1.11],
            "cold_rr": [1.10, 1.03],
            "cold_rr_lo": [1.02, 0.99],
            "cold_rr_hi": [1.18, 1.07],
            "sensitivity_index": [0.234, 0.087],
            "hot_rank": [1, 2],
            "cold_rank": [1, 2],
        }
    )

    meta = {"climate_col": "tmean", "n_strata": 2}
    return {
        "rr_table": rr_table,
        "comparison": comparison,
        "stratum_curves": stratum_curves,
        "meta": meta,
    }


def test_curves_plot(sensitivity_fit):
    from plotnine import ggplot

    p = sus_mod_plot_sensitivity(sensitivity_fit, type="curves", lang="pt")
    assert isinstance(p, ggplot)
    p.draw()


def test_scatter_plot(sensitivity_fit):
    from plotnine import ggplot

    p = sus_mod_plot_sensitivity(sensitivity_fit, type="scatter", lang="en")
    assert isinstance(p, ggplot)
    p.draw()


def test_bar_plot(sensitivity_fit):
    from plotnine import ggplot

    p = sus_mod_plot_sensitivity(sensitivity_fit, type="bar", lang="es")
    assert isinstance(p, ggplot)
    p.draw()


def test_output_type_table(sensitivity_fit):
    tbl = sus_mod_plot_sensitivity(sensitivity_fit, type="scatter", output_type="table")
    assert isinstance(tbl, pd.DataFrame)


def test_output_type_all(sensitivity_fit):
    out = sus_mod_plot_sensitivity(sensitivity_fit, type="bar", output_type="all")
    assert set(out.keys()) == {"plot", "table", "data"}
    assert isinstance(out["table"], pd.DataFrame)


def test_interactive_not_supported(sensitivity_fit):
    with pytest.raises(ImportError):
        sus_mod_plot_sensitivity(sensitivity_fit, type="curves", interactive=True)


def test_bad_input_type():
    with pytest.raises(TypeError):
        sus_mod_plot_sensitivity({"not": "a sensitivity dict"})
