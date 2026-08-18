"""Tests for sus_data_plot_demographics -- plotnine demographic visualisation."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

from climasus4py.viz.data_plot_demographics import (
    _detect_demo_cols,
    _numeric_leading_order,
    sus_data_plot_demographics,
)

# ---------------------------------------------------------------------------
# Fixture: synthetic table matching the real climasus4py pipeline shape
# (sus_data_standardize -> sus_data_create_variables -> sus_filter output):
# "sex"/"race"/"education" carry raw DATASUS codes (see core/filter.py),
# "age_group" is the label produced by sus_data_create_variables().
# ---------------------------------------------------------------------------


def _make_demo_df(n: int = 300) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    age_groups = ["0-4", "5-14", "15-59", "60+"]
    sexes = ["1", "2"]  # raw DATASUS codes, mirrors core/filter.py
    races = ["1", "2", "3", "4", "5"]
    educations = ["1", "2", "3", "4", "5"]
    dates = pd.date_range("2022-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "sex": rng.choice(sexes, n),
            "race": rng.choice(races, n),
            "education": rng.choice(educations, n),
            "age_group": rng.choice(age_groups, n),
            "month": dates.month.astype(str),
            "quarter": ("Q" + ((dates.month - 1) // 3 + 1).astype(str)),
            "epi_week": dates.strftime("%U/%Y"),
            "season": rng.choice(["Summer", "Autumn", "Winter", "Spring"], n),
        }
    )


@pytest.fixture
def demo_df():
    return _make_demo_df()


# ---------------------------------------------------------------------------
# Column detection helpers
# ---------------------------------------------------------------------------


def test_detect_demo_cols(demo_df):
    detected = _detect_demo_cols(list(demo_df.columns))
    assert detected["sex"] == "sex"
    assert detected["race"] == "race"
    assert detected["age_group"] == "age_group"
    assert detected["education"] == "education"
    assert detected["climate_risk"] is None


def test_numeric_leading_order():
    assert _numeric_leading_order(["15-59", "0-4", "60+", "5-14"]) == [
        "0-4", "5-14", "15-59", "60+",
    ]


def test_numeric_leading_order_na_last():
    assert _numeric_leading_order(["b", "0-4", "a"]) == ["0-4", "b", "a"]


# ---------------------------------------------------------------------------
# type="table" (returns a DataFrame, not a plotnine object)
# ---------------------------------------------------------------------------


def test_table_all_dimensions(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    out = sus_data_plot_demographics(demo_df, type="table", verbose=False)
    assert isinstance(out, pd.DataFrame)
    assert {"dimension", "category", "n", "pct"} <= set(out.columns)


def test_table_single_var(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    out = sus_data_plot_demographics(demo_df, type="table", var="sex", verbose=False)
    assert isinstance(out, pd.DataFrame)
    assert {"category", "n", "pct"} <= set(out.columns)
    assert out["n"].sum() == len(demo_df)


# ---------------------------------------------------------------------------
# Every plotnine plot_type must actually draw (not just construct)
# ---------------------------------------------------------------------------


def test_bar_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    p = sus_data_plot_demographics(demo_df, type="bar", var="race", lang="en", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_pyramid_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    p = sus_data_plot_demographics(demo_df, type="pyramid", lang="pt", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_heatmap_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    p = sus_data_plot_demographics(
        demo_df,
        type="heatmap",
        heatmap_row="age_group",
        heatmap_col="race",
        fill_metric="pct_row",
        lang="pt",
        verbose=False,
    )
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_heatmap_count_metric_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    p = sus_data_plot_demographics(
        demo_df,
        type="heatmap",
        heatmap_row="education",
        heatmap_col="sex",
        fill_metric="count",
        palette="nejm",
        verbose=False,
    )
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_temporal_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    p = sus_data_plot_demographics(
        demo_df, type="temporal", time_unit="epi_week", lang="pt", verbose=False
    )
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_temporal_with_ci_and_fill_var_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    pytest.importorskip("scipy", reason="scipy not installed")
    p = sus_data_plot_demographics(
        demo_df,
        type="temporal",
        time_unit="quarter",
        fill_var="sex",
        show_ci=True,
        verbose=False,
    )
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_climate_bar_only_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    # No month/season columns -> only the bar panel is produced (a plain
    # ggplot), matching the R source's fallback when the season heatmap
    # panel's columns are unavailable.
    df = demo_df.drop(columns=["month", "season"]).copy()
    df["climate_risk_group"] = np.random.default_rng(1).choice(["Low", "High"], len(df))
    p = sus_data_plot_demographics(df, type="climate", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_climate_with_season_heatmap_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    df = demo_df.copy()
    df["climate_risk_group"] = np.random.default_rng(1).choice(["Low", "High"], len(df))
    p = sus_data_plot_demographics(df, type="climate", verbose=False)
    assert p is not None
    p.draw()


def test_race_equity_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    df = demo_df.copy()
    df["race"] = np.random.default_rng(2).choice(
        ["Branca", "Preta", "Parda", "Amarela", "Indigena"], len(df)
    )
    p = sus_data_plot_demographics(df, type="race_equity", lang="pt", verbose=False)
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_race_equity_custom_benchmark_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    df = demo_df.copy()
    df["race"] = np.random.default_rng(2).choice(["A", "B"], len(df))
    p = sus_data_plot_demographics(
        df, type="race_equity", benchmark={"A": 50.0, "B": 50.0}, verbose=False
    )
    assert type(p).__name__ == "ggplot"
    p.draw()


def test_dashboard_draws(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    df = demo_df.copy()
    p = sus_data_plot_demographics(df, type="dashboard", lang="en", verbose=False)
    assert p is not None
    p.draw()


# ---------------------------------------------------------------------------
# Validation / error paths
# ---------------------------------------------------------------------------


def test_invalid_type_raises(demo_df):
    with pytest.raises(ValueError):
        sus_data_plot_demographics(demo_df, type="bogus", verbose=False)


def test_invalid_time_unit_raises(demo_df):
    with pytest.raises(ValueError):
        sus_data_plot_demographics(
            demo_df, type="temporal", time_unit="bogus", verbose=False
        )


def test_interactive_not_supported_raises(demo_df):
    with pytest.raises(ImportError):
        sus_data_plot_demographics(demo_df, interactive=True, verbose=False)


def test_bar_without_var_raises(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.raises(ValueError):
        sus_data_plot_demographics(demo_df, type="bar", verbose=False)


def test_missing_plotnine_raises_clear_error(demo_df, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "plotnine":
            raise ImportError("no plotnine")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="climasus4py\\[plot\\]"):
        sus_data_plot_demographics(demo_df, type="bar", var="sex", verbose=False)


def test_pyramid_missing_columns_raises():
    pytest.importorskip("plotnine", reason="plotnine not installed")
    df = pd.DataFrame({"foo": [1, 2, 3]})
    with pytest.raises(ValueError):
        sus_data_plot_demographics(df, type="pyramid", verbose=False)


def test_climate_missing_column_raises(demo_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.raises(ValueError):
        sus_data_plot_demographics(demo_df, type="climate", verbose=False)
