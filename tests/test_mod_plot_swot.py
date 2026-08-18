"""Tests for sus_mod_plot_swot."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import pandas as pd
import pytest

pytest.importorskip("plotnine")

from climasus4py.enrichment.mod_swot import sus_mod_swot
from climasus4py.viz.mod_plot_swot import sus_mod_plot_swot  # noqa: E402


@pytest.fixture
def swot_result():
    vt = pd.DataFrame(
        {
            "city": ["fortaleza", "curitiba"],
            "vi_score": [0.72, 0.31],
            "exposure_score": [80.0, 30.0],
            "sensitivity_score": [65.0, 40.0],
            "adaptive_capacity_score": [40.0, 75.0],
            "vi_percentile": [90.0, 20.0],
        }
    )
    vulnerability = {"vi_table": vt, "meta": {"city_col": "city"}}
    return sus_mod_swot(vulnerability=vulnerability, lang="en", verbose=False)


def test_radar_plot(swot_result):
    from plotnine import ggplot

    p = sus_mod_plot_swot(swot_result, type="radar", lang="pt")
    assert isinstance(p, ggplot)
    p.draw()


def test_matrix_plot_single_entity(swot_result):
    from plotnine import ggplot

    p = sus_mod_plot_swot(swot_result, type="matrix", entities=["fortaleza"], lang="en")
    assert isinstance(p, ggplot)
    p.draw()


def test_matrix_plot_multi_entity_warns(swot_result):
    with pytest.warns(UserWarning):
        sus_mod_plot_swot(swot_result, type="matrix", lang="en")


def test_bar_plot(swot_result):
    from plotnine import ggplot

    p = sus_mod_plot_swot(swot_result, type="bar", lang="es")
    assert isinstance(p, ggplot)
    p.draw()


def test_output_type_table(swot_result):
    tbl = sus_mod_plot_swot(swot_result, type="radar", output_type="table")
    assert isinstance(tbl, pd.DataFrame)


def test_output_type_all(swot_result):
    out = sus_mod_plot_swot(swot_result, type="bar", output_type="all")
    assert set(out.keys()) == {"plot", "table"}


def test_interactive_not_supported(swot_result):
    with pytest.raises(ImportError):
        sus_mod_plot_swot(swot_result, interactive=True)


def test_bad_input_type():
    with pytest.raises(TypeError):
        sus_mod_plot_swot({"not": "a swot dict"})
