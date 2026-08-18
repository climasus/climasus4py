"""Tests for sus_mod_plot_dlnm."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("plotnine")

from climasus4py.viz.mod_plot_dlnm import sus_mod_plot_dlnm  # noqa: E402


@pytest.fixture
def dlnm_fit():
    n_exp = 30
    lag_max = 5
    n_lag = lag_max + 1
    rng = np.random.default_rng(42)

    predvar = np.linspace(10, 35, n_exp)
    base = 1 + 0.01 * (predvar - 22) ** 2
    mat_rr = np.tile(base.reshape(-1, 1), (1, n_lag)) * np.linspace(1.0, 0.8, n_lag)
    pred = {
        "predvar": predvar,
        "lag": np.array([0, lag_max]),
        "allRRfit": base,
        "allRRlow": base * 0.9,
        "allRRhigh": base * 1.1,
        "matRRfit": mat_rr,
        "matRRlow": mat_rr * 0.9,
        "matRRhigh": mat_rr * 1.1,
    }

    n_days = 200
    dates = pd.date_range("2020-01-01", periods=n_days, freq="D")
    exposure = 22 + 6 * np.sin(np.linspace(0, 4 * np.pi, n_days)) + rng.normal(0, 1, n_days)
    y = rng.poisson(5, n_days).astype(float)
    data_daily = pd.DataFrame({"date": dates, "y": y, "tmean_lag0": exposure})

    exposure_response = pd.DataFrame(
        {
            "pct": [0.5, 0.75, 0.95],
            "exposure": [22.0, 28.0, 33.0],
            "rr": [1.0, 1.3, 1.8],
            "lo": [0.95, 1.2, 1.6],
            "hi": [1.05, 1.4, 2.0],
        }
    )
    lag_response = pd.DataFrame(
        {
            "lag": np.arange(n_lag),
            "rr": np.linspace(1.3, 1.05, n_lag),
            "lo": np.linspace(1.1, 0.95, n_lag),
            "hi": np.linspace(1.5, 1.15, n_lag),
            "rr_cum": np.cumprod(np.linspace(1.3, 1.05, n_lag)),
        }
    )
    diagnostics = {
        "disp_ratio": 1.2,
        "disp_category": "ok",
        "autocorr_pval": 0.3,
        "has_autocorr": False,
        "aic_poisson": 1234.5,
        "deviance": 200.0,
    }
    meta = {
        "climate_col": "tmean",
        "outcome_col": "n_obitos",
        "family": "quasipoisson",
        "lag_max": lag_max,
        "ref_value": 22.0,
        "ns_df": 4,
        "n": n_days,
    }

    return {
        "pred": pred,
        "meta": meta,
        "data_daily": data_daily,
        "exposure_response": exposure_response,
        "lag_response": lag_response,
        "diagnostics": diagnostics,
    }


@pytest.mark.parametrize("plot_type", ["overall", "lag", "surface", "contour", "slice", "distribution", "series"])
def test_all_plot_types_draw(dlnm_fit, plot_type):
    from plotnine import ggplot

    p = sus_mod_plot_dlnm(dlnm_fit, type=plot_type, lang="pt")
    assert isinstance(p, ggplot)
    p.draw()


def test_output_type_table(dlnm_fit):
    tbl = sus_mod_plot_dlnm(dlnm_fit, output_type="table")
    assert set(tbl.keys()) == {"model_spec", "exposure_response", "diagnostics"}
    for df in tbl.values():
        assert isinstance(df, pd.DataFrame)


def test_output_type_all(dlnm_fit):
    out = sus_mod_plot_dlnm(dlnm_fit, output_type="all")
    assert set(out.keys()) == {"plot", "table", "data"}


def test_interactive_not_supported(dlnm_fit):
    with pytest.raises(ImportError):
        sus_mod_plot_dlnm(dlnm_fit, interactive=True)


def test_bad_input_type():
    with pytest.raises(TypeError):
        sus_mod_plot_dlnm({"not": "a dlnm dict"})
