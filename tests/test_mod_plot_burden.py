"""Tests for sus_mod_plot_burden."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_burden import sus_mod_burden

pytest.importorskip("plotnine")

from climasus4py.viz.mod_plot_burden import sus_mod_plot_burden  # noqa: E402


def _af_table_all(n_cases, an_total, an_heat, an_cold) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "component": ["total", "heat", "cold"],
            "n_cases": [n_cases] * 3,
            "an": [an_total, an_heat, an_cold],
            "an_lo": [an_total * 0.8, an_heat * 0.8, an_cold * 0.8],
            "an_hi": [an_total * 1.2, an_heat * 1.2, an_cold * 1.2],
            "af_pct": [
                an_total / n_cases * 100,
                an_heat / n_cases * 100,
                an_cold / n_cases * 100,
            ],
        }
    )


def _excess_table(
    n_days, observed, expected, excess, excess_lo, excess_hi, excess_pct
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "n_days": [n_days],
            "observed": [observed],
            "expected": [expected],
            "excess": [excess],
            "excess_lo": [excess_lo],
            "excess_hi": [excess_hi],
            "excess_pct": [excess_pct],
        }
    )


@pytest.fixture
def af_burden_all():
    fits = {
        "A": _af_table_all(1000, 50.0, 30.0, 20.0),
        "B": _af_table_all(2000, 100.0, 60.0, 40.0),
    }
    return sus_mod_burden(fits, component="all", lang="en", verbose=False)


@pytest.fixture
def excess_burden():
    fits = {
        "A": _excess_table(365, 1200, 1150.0, 50.0, 40.0, 60.0, 4.3),
        "B": _excess_table(365, 2400, 2300.0, 100.0, 80.0, 120.0, 4.3),
    }
    return sus_mod_burden(fits, lang="en", verbose=False)


def test_lollipop_plot_af(af_burden_all):
    from plotnine import ggplot

    p = sus_mod_plot_burden(af_burden_all, type="lollipop", lang="en")
    assert isinstance(p, ggplot)


def test_lollipop_plot_excess(excess_burden):
    from plotnine import ggplot

    p = sus_mod_plot_burden(excess_burden, type="lollipop", lang="pt")
    assert isinstance(p, ggplot)


def test_lorenz_plot(af_burden_all):
    from plotnine import ggplot

    p = sus_mod_plot_burden(af_burden_all, type="lorenz", lang="es")
    assert isinstance(p, ggplot)


def test_stacked_plot_requires_af_all(excess_burden):
    with pytest.raises(ValueError):
        sus_mod_plot_burden(excess_burden, type="stacked")


def test_stacked_plot_af_all(af_burden_all):
    from plotnine import ggplot

    p = sus_mod_plot_burden(af_burden_all, type="stacked")
    assert isinstance(p, ggplot)


def test_output_type_table(af_burden_all):
    tbl = sus_mod_plot_burden(af_burden_all, type="lollipop", output_type="table")
    assert isinstance(tbl, pd.DataFrame)
    assert "component" not in tbl.columns or (tbl["component"] == "total").all()


def test_output_type_all(af_burden_all):
    out = sus_mod_plot_burden(af_burden_all, type="lorenz", output_type="all")
    assert set(out.keys()) == {"plot", "table", "data"}
    assert isinstance(out["table"], pd.DataFrame)


def test_interactive_not_supported(af_burden_all):
    with pytest.raises(ImportError):
        sus_mod_plot_burden(af_burden_all, type="lollipop", interactive=True)


def test_bad_input_type():
    with pytest.raises(TypeError):
        sus_mod_plot_burden({"not": "a burden dict"})
