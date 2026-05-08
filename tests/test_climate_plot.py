"""Tests for sus_climate_plot_fill — plotnine visualisation (B.4)."""

from __future__ import annotations

import warnings
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from climasus4py.viz.climate_plot import sus_climate_plot_fill

# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


def _make_filled_df(n: int = 20) -> pd.DataFrame:
    """Synthetic filled DataFrame with is_imputed flag."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2023-01-01", periods=n, freq="D")
    values = rng.uniform(22.0, 32.0, n)
    imputed = [False] * n
    imputed[5] = True
    imputed[12] = True
    return pd.DataFrame(
        {
            "station_code": ["A701"] * n,
            "date": dates,
            "tair_dry_bulb_c": values,
            "is_imputed_tair_dry_bulb_c": imputed,
        }
    )


@pytest.fixture
def filled_df():
    return _make_filled_df()


# ---------------------------------------------------------------------------
# B.4.1 — Returns ggplot object
# ---------------------------------------------------------------------------


def test_returns_ggplot_object(filled_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_fill(filled_df, target_var="tair_dry_bulb_c")
    assert isinstance(result, ggplot)


# ---------------------------------------------------------------------------
# B.4.2 — Missing plotnine raises clear ImportError
# ---------------------------------------------------------------------------


def test_missing_plotnine_raises_clear_error(filled_df):
    """Without plotnine, ImportError should instruct user to install [plot]."""
    with patch(
        "climasus4py.viz.climate_plot._require_plotnine",
        side_effect=ImportError(
            "sus_climate_plot_fill requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ),
    ), pytest.raises(ImportError, match="pip install climasus4py"):
        sus_climate_plot_fill(filled_df, target_var="tair_dry_bulb_c")


# ---------------------------------------------------------------------------
# B.4.3 — Empty DataFrame does not crash
# ---------------------------------------------------------------------------


def test_handles_empty_dataframe():
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    df = pd.DataFrame(
        {
            "station_code": pd.Series([], dtype=str),
            "date": pd.Series([], dtype="datetime64[ns]"),
            "tair_dry_bulb_c": pd.Series([], dtype=float),
            "is_imputed_tair_dry_bulb_c": pd.Series([], dtype=bool),
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = sus_climate_plot_fill(df, target_var="tair_dry_bulb_c")
    assert isinstance(result, ggplot)


# ---------------------------------------------------------------------------
# B.4.4 — output_type="all" returns dict
# ---------------------------------------------------------------------------


def test_output_type_all_returns_dict(filled_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_fill(
        filled_df, target_var="tair_dry_bulb_c", output_type="all"
    )
    assert isinstance(result, dict)
    assert "plot" in result
    assert "data" in result
    assert isinstance(result["plot"], ggplot)


# ---------------------------------------------------------------------------
# B.4.5 — Missing target_var raises ValueError
# ---------------------------------------------------------------------------


def test_missing_target_var_raises(filled_df):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    with pytest.raises(ValueError, match="not found in df_filled"):
        sus_climate_plot_fill(filled_df, target_var="nonexistent_var")


# ---------------------------------------------------------------------------
# B.4.6 — Multilingual labels (smoke)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lang", ["pt", "en", "es"])
def test_multilingual_does_not_raise(filled_df, lang):
    pytest.importorskip("plotnine", reason="plotnine not installed")
    from plotnine import ggplot

    result = sus_climate_plot_fill(
        filled_df, target_var="tair_dry_bulb_c", lang=lang
    )
    assert isinstance(result, ggplot)


# ---------------------------------------------------------------------------
# B.4.7 — Helper _detect_datetime_col (no plotnine needed)
# ---------------------------------------------------------------------------


def test_detect_datetime_col_finds_date_col(filled_df):
    from climasus4py.viz.climate_plot import _detect_datetime_col

    assert _detect_datetime_col(filled_df) == "date"


def test_detect_datetime_col_finds_time_col():
    from climasus4py.viz.climate_plot import _detect_datetime_col

    df = pd.DataFrame({"timestamp": pd.date_range("2023-01", periods=3), "val": [1, 2, 3]})
    assert _detect_datetime_col(df) == "timestamp"


def test_detect_datetime_col_raises_when_missing():
    from climasus4py.viz.climate_plot import _detect_datetime_col

    df = pd.DataFrame({"value": [1, 2, 3]})
    with pytest.raises(ValueError, match="auto-detect"):
        _detect_datetime_col(df)


# ---------------------------------------------------------------------------
# B.4.8 — Helper _detect_station_col (no plotnine needed)
# ---------------------------------------------------------------------------


def test_detect_station_col_finds_station(filled_df):
    from climasus4py.viz.climate_plot import _detect_station_col

    assert _detect_station_col(filled_df) == "station_code"


def test_detect_station_col_returns_none_when_missing():
    from climasus4py.viz.climate_plot import _detect_station_col

    df = pd.DataFrame({"date": pd.date_range("2023-01", periods=2), "val": [1, 2]})
    assert _detect_station_col(df) is None
