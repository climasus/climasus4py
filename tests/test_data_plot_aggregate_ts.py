"""Tests for sus_data_plot_aggregate_ts — plotnine time-series visualisation."""

from __future__ import annotations

import warnings

import matplotlib
import numpy as np
import pandas as pd
import pytest

matplotlib.use("Agg")

pytest.importorskip("plotnine", reason="plotnine not installed")

from climasus4py.viz.data_plot_aggregate_ts import sus_data_plot_aggregate_ts

# ---------------------------------------------------------------------------
# Fixture — mirrors the *R contract* the function is implemented against
# (a literal "date" column + an outcome column such as n_obitos), which is
# documented in the module/function docstring as NOT the current shape of
# climasus4py.sus_data_aggregate()'s real output (time_group/count/sum_*).
# ---------------------------------------------------------------------------


def _make_ts_df(n: int = 48, freq: str = "MS") -> pd.DataFrame:
    rng = np.random.default_rng(0)
    dates = pd.date_range("2019-01-01", periods=n, freq=freq)
    months = dates.month.to_numpy()
    seasonal = 5 + 3 * np.sin(2 * np.pi * months / 12)
    trend = np.arange(n) * 0.05
    n_obitos = (seasonal + trend + rng.normal(0, 1, n)).round().clip(min=0).astype(int)
    return pd.DataFrame({"date": dates, "n_obitos": n_obitos})


@pytest.fixture
def ts_df():
    return _make_ts_df()


@pytest.fixture
def ts_df_grouped():
    df1 = _make_ts_df().assign(group="A")
    df2 = _make_ts_df().assign(group="B")
    df2["n_obitos"] = (df2["n_obitos"] * 1.5).round().astype(int)
    return pd.concat([df1, df2], ignore_index=True)


# ---------------------------------------------------------------------------
# One .draw() per plot_type branch
# ---------------------------------------------------------------------------


def test_epidemic_draws(ts_df):
    p = sus_data_plot_aggregate_ts(ts_df, plot_type="epidemic", lang="en", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_seasonal_draws(ts_df):
    p = sus_data_plot_aggregate_ts(ts_df, plot_type="seasonal", lang="en", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_heatmap_draws(ts_df):
    p = sus_data_plot_aggregate_ts(ts_df, plot_type="heatmap", lang="en", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_trend_draws(ts_df):
    p = sus_data_plot_aggregate_ts(ts_df, plot_type="trend", lang="en", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


# ---------------------------------------------------------------------------
# Default plot_type is the FULL vector (R's actual match.arg default, not
# the docstring's "epidemic") — a real quirk, tested explicitly.
# ---------------------------------------------------------------------------


def test_default_plot_type_is_all_four_and_draws(ts_df):
    p = sus_data_plot_aggregate_ts(ts_df, lang="en", verbose=False)
    p.draw()


# ---------------------------------------------------------------------------
# Options that touch aesthetics/colors — must render, not just construct
# ---------------------------------------------------------------------------


def test_epidemic_log_transform_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type="epidemic", log_transform=True, lang="en", verbose=False
    )
    p.draw()


def test_epidemic_smooth_lm_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type="epidemic", smooth_method="lm", lang="en", verbose=False
    )
    p.draw()


def test_epidemic_smooth_none_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type="epidemic", smooth_method="none", lang="en", verbose=False
    )
    p.draw()


def test_epidemic_smooth_gam_falls_back_to_loess_with_warning(ts_df):
    with pytest.warns(UserWarning, match="GAM"):
        p = sus_data_plot_aggregate_ts(
            ts_df, plot_type="epidemic", smooth_method="gam", lang="en", verbose=False
        )
    p.draw()


def test_epidemic_with_group_col_draws(ts_df_grouped):
    p = sus_data_plot_aggregate_ts(
        ts_df_grouped, plot_type="epidemic", group_col="group", lang="en", verbose=False
    )
    p.draw()


def test_seasonal_with_group_col_draws(ts_df_grouped):
    p = sus_data_plot_aggregate_ts(
        ts_df_grouped, plot_type="seasonal", group_col="group", lang="en", verbose=False
    )
    p.draw()


def test_seasonal_log_transform_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type="seasonal", log_transform=True, lang="en", verbose=False
    )
    p.draw()


def test_heatmap_log_transform_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type="heatmap", log_transform=True, lang="en", verbose=False
    )
    p.draw()


def test_facet_col_draws(ts_df_grouped):
    p = sus_data_plot_aggregate_ts(
        ts_df_grouped,
        plot_type="trend",
        facet_col="group",
        facet_ncol=1,
        lang="en",
        verbose=False,
    )
    p.draw()


def test_multi_plot_type_composed_draws(ts_df):
    p = sus_data_plot_aggregate_ts(
        ts_df, plot_type=["epidemic", "trend"], lang="en", verbose=False
    )
    p.draw()


# ---------------------------------------------------------------------------
# lang variants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lang", ["pt", "en", "es"])
def test_lang_variants_draw(ts_df, lang):
    p = sus_data_plot_aggregate_ts(ts_df, plot_type="epidemic", lang=lang, verbose=False)
    p.draw()


def test_unsupported_lang_falls_back_to_pt(ts_df):
    with pytest.warns(UserWarning, match="pt"):
        p = sus_data_plot_aggregate_ts(
            ts_df, plot_type="epidemic", lang="de", verbose=False
        )
    p.draw()


# ---------------------------------------------------------------------------
# Validation / error paths
# ---------------------------------------------------------------------------


def test_empty_df_raises(ts_df):
    with pytest.raises(ValueError, match="0 rows"):
        sus_data_plot_aggregate_ts(ts_df.iloc[0:0], verbose=False)


def test_no_date_column_raises():
    df = pd.DataFrame({"n_obitos": [1, 2, 3]})
    with pytest.raises(ValueError, match="[Dd]ate"):
        sus_data_plot_aggregate_ts(df, verbose=False)


def test_no_outcome_column_raises():
    df = pd.DataFrame({"date": pd.date_range("2020-01-01", periods=5)})
    with pytest.raises(ValueError):
        sus_data_plot_aggregate_ts(df, verbose=False)


def test_unknown_value_col_raises(ts_df):
    with pytest.raises(ValueError, match="value_col"):
        sus_data_plot_aggregate_ts(ts_df, value_col="nope", verbose=False)


def test_unknown_plot_type_raises(ts_df):
    with pytest.raises(ValueError, match="plot_type"):
        sus_data_plot_aggregate_ts(ts_df, plot_type="unknown", verbose=False)


def test_missing_group_col_warns_and_ignores(ts_df):
    with pytest.warns(UserWarning, match="group_col"):
        p = sus_data_plot_aggregate_ts(
            ts_df, plot_type="epidemic", group_col="nope", verbose=False
        )
    p.draw()


def test_interactive_raises_importerror(ts_df):
    with pytest.raises(ImportError, match="plotly"):
        sus_data_plot_aggregate_ts(ts_df, interactive=True, verbose=False)


def test_bad_df_type_raises_typeerror():
    with pytest.raises(TypeError):
        sus_data_plot_aggregate_ts([1, 2, 3], verbose=False)


def test_relation_input_materialised(ts_df):
    duckdb = pytest.importorskip("duckdb")
    rel = duckdb.sql("SELECT * FROM ts_df")
    p = sus_data_plot_aggregate_ts(rel, plot_type="epidemic", lang="en", verbose=False)
    p.draw()


def test_explicit_value_col_used():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=24, freq="MS"),
            "custom_outcome": np.arange(24),
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        p = sus_data_plot_aggregate_ts(
            df, plot_type="trend", value_col="custom_outcome", lang="en", verbose=False
        )
    p.draw()
