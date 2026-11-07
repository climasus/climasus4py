"""Plot content checked against climasus4r on SP 2023 (07/11/2026).

What each side DRAWS was compared layer by layer (ggplot_build in R, the
plotnine build here), with the same input table on both sides.

1. Heat-wave timeline/intensity colour for an event with no intensity class.
   Only EHF grades intensity; WHO and INMET events carry intensity_class =
   NA. R leaves them to ggplot's na.value (grey50); this package painted them
   with the *severe* colour, so 110 of 245 SP events (45%) showed as "Severe
   (SHW)" with no grading behind them. Cold waves used a lighter grey; both
   now use grey50, as R.
2. Vulnerability "pillars": R writes aes(x = score, y = city) + geom_col() and
   ggplot2 lays the bars down because y is discrete. plotnine has no
   orientation detection, so the same call drew one vertical hairline per row,
   as tall as the city's factor position. Now x = city + coord_flip(); on the
   645 SP municipalities every bar edge matches R's to 2e-16.
3. climate_plot_aggregate(plot_type="scatter") above 37,833 points: loess with
   a confidence band overflows its workspace. R refuses and keeps the points
   without the curve; scikit-misc crashed the Python process. Now as in R.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

p9 = pytest.importorskip("plotnine")


def _cores(p) -> list[str]:
    p._build()
    return list(p.layers[0].data["color"])


def test_heatwave_event_without_class_is_grey_not_severe():
    from climasus4py.viz.climate_plot_heatwaves import _intensity_color_scale

    cores = {"low": "#4DBBD5FF", "severe": "#3C5488FF", "extreme": "#E64B35FF",
             "neutral": "#E0E0E0"}
    rot = {"low": "Low Intensity (LIHW)", "severe": "Severe (SHW)", "extreme": "Extreme (EHW)",
           "unknown": "Unknown"}
    ev = pd.DataFrame({"x": [1, 2, 3], "y": [1, 1, 1],
                       "intensity_class": ["Low Intensity (LIHW)", np.nan, "Severe (SHW)"]})
    p = (p9.ggplot(ev, p9.aes("x", "y", color="intensity_class")) + p9.geom_point()
         + _intensity_color_scale(cores, rot))
    assert _cores(p) == ["#4DBBD5FF", "grey50", "#3C5488FF"]


def test_coldwave_event_without_class_is_grey50():
    from climasus4py.viz import climate_plot_coldwaves as m

    assert m._NA_COLOR == "grey50"
    ev = pd.DataFrame({"x": [1, 2], "y": [1, 1], "c": ["a", np.nan]})
    p = (p9.ggplot(ev, p9.aes("x", "y", color="c")) + p9.geom_point()
         + p9.scale_color_manual(values={"a": "#4DBBD5FF"}, na_value=m._NA_COLOR))
    assert _cores(p) == ["#4DBBD5FF", "grey50"]


def test_vulnerability_pillars_are_horizontal_bars_one_per_city():
    import climasus4py as cs

    rng = np.random.default_rng(7)
    cod = [f"35{i:05d}" for i in range(8)]
    vi = cs.sus_mod_vulnerability_index(
        exposure_df=pd.DataFrame({"code_muni": cod, "t": rng.normal(25, 2, 8)}),
        sensitivity_df=pd.DataFrame({"code_muni": cod, "r": rng.uniform(100, 900, 8)}),
        adaptive_capacity_df=pd.DataFrame({"code_muni": cod, "renda": rng.uniform(300, 3000, 8)}),
        city_col="code_muni", lang="en", verbose=False)
    p = cs.sus_mod_plot_vulnerability(vi, type="pillars", lang="en")
    assert any(type(c).__name__ == "coord_flip" for c in [p.coordinates])
    p._build()
    d = p.layers[0].data
    # one stacked bar per city: x is the city's position, the length is the score
    assert sorted(d["x"].unique()) == list(range(1, 9))
    ps = vi["pillar_scores"].dropna(subset=["score"])
    np.testing.assert_allclose(np.sort((d["ymax"] - d["ymin"]).to_numpy()),
                               np.sort(ps["score"].to_numpy()), atol=1e-12)
    # each city's bar ends at the sum of its three pillars
    soma = ps.groupby("code_muni")["score"].sum().sort_values().to_numpy()
    np.testing.assert_allclose(np.sort(d.groupby("x")["ymax"].max().to_numpy()), soma, atol=1e-12)


def test_ml_importance_bars_are_horizontal_one_per_feature():
    """Same orientation defect as the pillars (item 2), in sus_mod_plot_ml."""
    from climasus4py.viz import mod_plot_ml as m

    imp = pd.DataFrame({"Feature": ["a", "b", "c"], "Gain": [0.5, 0.3, 0.2]})
    p = m._plot_importance({"importance": imp, "meta": {"outcome_col": "y"}}, 20, m._I18N["en"], 12)
    assert type(p.coordinates).__name__ == "coord_flip"
    p._build()
    d = p.layers[0].data.sort_values("x")
    # highest gain at the top: "a" is the last level, so x = 3
    assert d["x"].tolist() == [1, 2, 3]
    np.testing.assert_allclose(d["ymax"], [0.2, 0.3, 0.5])
    np.testing.assert_allclose(d["ymin"], 0.0)


def test_swot_bar_dodges_quadrants_within_each_entity_not_along_the_score():
    """Same orientation defect, in sus_mod_plot_swot(type="bar"): the dodge ran
    along the score axis. On SP the bar positions now match R's exactly."""
    from climasus4py.viz import mod_plot_swot as m

    sc = pd.DataFrame({"entity": ["A", "B", "C"], "S_score": [90.0, 50, 10], "W_score": [5.0, 6, 7],
                       "O_score": [np.nan] * 3, "T_score": [55.0, 30, 20]})
    p = m._plot_bar({"scores": sc}, m._I18N["en"], 12, None)
    assert type(p.coordinates).__name__ == "coord_flip"
    p._build()
    d = p.layers[0].data.dropna(subset=["ymax"])
    # bar length is the score, every bar starts at zero
    esperado = np.sort(sc[["S_score", "W_score", "T_score"]].to_numpy().ravel())
    np.testing.assert_allclose(np.sort(d["ymax"]), esperado)
    np.testing.assert_allclose(d["ymin"], 0.0)
    # dodged positions stay within half a unit of the entity they belong to
    assert ((d["x"] - d["x"].round()).abs() < 0.5).all() and set(d["x"].round()) == {1.0, 2.0, 3.0}


def _scatter_layers(n: int) -> list[str]:
    import climasus4py as cs

    rng = np.random.default_rng(1)
    df = pd.DataFrame({"date": pd.date_range("2023-01-01", periods=n, freq="min"),
                       "n_obitos": rng.poisson(3, n), "tair_dry_bulb_c": rng.normal(22, 4, n)})
    p = cs.sus_climate_plot_aggregate(df, outcome_col="n_obitos", climate_cols=["tair_dry_bulb_c"],
                                      plot_type="scatter", lang="en", verbose=False)
    return [type(layer.geom).__name__ for layer in p.layers]


def test_scatter_keeps_the_loess_curve_up_to_the_limit():
    from climasus4py.viz.climate_plot_aggregate import _LOESS_SE_MAX_N

    assert _LOESS_SE_MAX_N == 37_833
    assert _scatter_layers(500) == ["geom_point", "geom_smooth"]


def test_scatter_drops_the_curve_above_the_limit_as_r_does():
    from climasus4py.viz.climate_plot_aggregate import _LOESS_SE_MAX_N

    with pytest.warns(UserWarning, match="loess curve was left out"):
        camadas = _scatter_layers(_LOESS_SE_MAX_N + 1)
    assert camadas == ["geom_point"]
