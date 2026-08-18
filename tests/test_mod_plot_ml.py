"""Tests for sus_mod_plot_ml."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("plotnine", reason="plotnine not installed")
pytest.importorskip("xgboost", reason="xgboost not installed")

from climasus4py.enrichment.mod_ml import sus_mod_ml  # noqa: E402
from climasus4py.viz.mod_plot_ml import sus_mod_plot_ml  # noqa: E402


def _make_ml_result():
    rng = np.random.default_rng(0)
    n = 200
    df = pd.DataFrame({
        "tmax": rng.uniform(20, 40, n),
        "pop": rng.uniform(1000, 100000, n),
        "decoy": rng.normal(0, 1, n),
    })
    df["n_obitos"] = (df["tmax"] * 2 + rng.normal(0, 5, n)).clip(lower=0)
    return sus_mod_ml(
        df, outcome_col="n_obitos", feature_cols=["tmax", "pop", "decoy"],
        nfold=3, verbose=False,
    )


class TestPlotTypes:
    def test_importance_returns_ggplot(self):
        ml = _make_ml_result()
        p = sus_mod_plot_ml(ml, type="importance")
        assert type(p).__name__ == "ggplot"

    def test_fit_returns_ggplot(self):
        ml = _make_ml_result()
        p = sus_mod_plot_ml(ml, type="fit")
        assert type(p).__name__ == "ggplot"

    def test_cv_log_returns_ggplot(self):
        ml = _make_ml_result()
        p = sus_mod_plot_ml(ml, type="cv_log")
        assert type(p).__name__ == "ggplot"

    def test_output_type_table(self):
        ml = _make_ml_result()
        tbl = sus_mod_plot_ml(ml, type="importance", output_type="table")
        assert isinstance(tbl, pd.DataFrame)
        assert "Gain" in tbl.columns

    def test_output_type_all(self):
        ml = _make_ml_result()
        out = sus_mod_plot_ml(ml, output_type="all")
        assert set(out.keys()) == {"plot", "table", "data"}

    def test_n_top_limits_importance_rows(self):
        ml = _make_ml_result()
        tbl = sus_mod_plot_ml(ml, type="importance", output_type="table", n_top=1)
        assert len(tbl) == 1

    def test_interactive_warns_and_returns_static_plot(self):
        ml = _make_ml_result()
        with pytest.warns(UserWarning, match="interactive"):
            p = sus_mod_plot_ml(ml, interactive=True)
        assert type(p).__name__ == "ggplot"


class TestValidation:
    def test_invalid_type_raises(self):
        ml = _make_ml_result()
        with pytest.raises(ValueError, match="type"):
            sus_mod_plot_ml(ml, type="bogus")

    def test_invalid_output_type_raises(self):
        ml = _make_ml_result()
        with pytest.raises(ValueError, match="output_type"):
            sus_mod_plot_ml(ml, output_type="bogus")

    def test_non_dict_input_raises_type_error(self):
        with pytest.raises(TypeError):
            sus_mod_plot_ml("not a dict")

    def test_missing_keys_raises_type_error(self):
        with pytest.raises(TypeError):
            sus_mod_plot_ml({"predictions": None})
