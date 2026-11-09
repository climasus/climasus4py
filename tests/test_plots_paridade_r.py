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
    # grey50 of R, written as hex: plotnine/mizani does not know the R name (M264).
    assert _cores(p) == ["#4DBBD5FF", "#7F7F7F", "#3C5488FF"]


def test_coldwave_event_without_class_is_grey50():
    from climasus4py.viz import climate_plot_coldwaves as m

    assert m._NA_COLOR == "#7F7F7F"  # R's grey50 (M264)
    ev = pd.DataFrame({"x": [1, 2], "y": [1, 1], "c": ["a", np.nan]})
    p = (p9.ggplot(ev, p9.aes("x", "y", color="c")) + p9.geom_point()
         + p9.scale_color_manual(values={"a": "#4DBBD5FF"}, na_value=m._NA_COLOR))
    assert _cores(p) == ["#4DBBD5FF", "#7F7F7F"]


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


# --- 08/11/2026: trend (M245) and map palette (M247) ----------------------------


def _serie_diaria():
    datas = pd.date_range("2014-01-01", "2016-12-31", freq="D")
    n = np.where(datas.year == 2014, 1, np.where(datas.year == 2015, 2, 1))
    return pd.DataFrame({"date": datas, "codigo_municipio_ocorrencia": "355030",
                         "n_obitos": n})


def test_trend_is_annual_bars_labelled_with_the_change_on_the_year_before():
    """climasus4r's .ts_trend: one bar per year, (Y / Y_prev - 1) * 100 on top.

    This package drew the raw series with a fitted straight line instead.
    """
    import climasus4py as cs

    g = cs.sus_data_plot_aggregate_ts(_serie_diaria(), plot_type="trend", lang="pt",
                                      verbose=False)
    g._build()
    barras, textos = g.layers[0].data, g.layers[1].data
    assert len(barras) == 3                                   # 2014, 2015, 2016
    assert list(barras["y"]) == [365, 730, 366]               # 2016 is a leap year
    assert list(textos["label"]) == ["", "+100%", "-49.9%"]          # R prints "+100%"
    # red when it rises, blue when it falls, as in R
    assert list(textos["color"])[1:] == ["#C0392B", "#2196F3"]
    assert g.labels.title == "Tendência anual de eventos de saúde notificados"


def test_trend_change_is_computed_within_each_facet():
    import climasus4py as cs

    a = _serie_diaria().assign(faixa="60-69")
    b = _serie_diaria().assign(faixa="80+", n_obitos=lambda d: d["n_obitos"] * 10)
    g = cs.sus_data_plot_aggregate_ts(pd.concat([a, b]), plot_type="trend",
                                      facet_col="faixa", lang="pt", verbose=False)
    g._build()
    rotulos = g.layers[1].data.sort_values(["PANEL", "x"])["label"].tolist()
    assert rotulos == ["", "+100%", "-49.9%"] * 2


def test_map_accepts_r_palette_names():
    """'YlOrRd', climasus4r's default palette, was replaced by 'lancet' silently."""
    from matplotlib import colormaps

    from climasus4py.viz.plot_aggregate_map import _map_palette

    cmap = _map_palette("YlOrRd")
    assert np.allclose(cmap(0.0), colormaps["YlOrRd"](0.0))
    assert np.allclose(cmap(1.0), colormaps["YlOrRd"](1.0))
    with pytest.warns(UserWarning, match="not a known palette"):
        _map_palette("nao_existe")


def _mapa_log(map_type: str, lang: str = "pt"):
    import matplotlib

    matplotlib.use("Agg")
    from climasus4py.viz.plot_aggregate_map import sus_data_plot_aggregate_map

    df = pd.DataFrame({
        "date": pd.to_datetime(["2014-01-01"] * 3),
        "codigo_municipio_residencia": ["355030", "351880", "350950"],
        "n_obitos": [52118, 4878, 1060],
    })
    return sus_data_plot_aggregate_map(df, map_type=map_type, log_scale=True,
                                       palette="YlOrRd", lang=lang, verbose=False)


@pytest.mark.parametrize("map_type", ["choropleth", "bubble"])
def test_log_colour_bar_is_labelled_in_plain_counts(map_type):
    """The log bar read 2x10^3, 3x10^3, ... on top of each other (choropleth)
    and 7.0 ... 10.5, the log1p of the counts (bubble)."""
    import matplotlib.pyplot as plt

    fig = _mapa_log(map_type)
    fig.canvas.draw()
    rotulos = [t.get_text() for t in fig.axes[-1].get_xticklabels() if t.get_text()]
    plt.close(fig)
    assert rotulos == ["2.000", "5.000", "10.000", "20.000", "50.000"]


def test_log_colour_bar_uses_the_english_separator():
    import matplotlib.pyplot as plt

    fig = _mapa_log("choropleth", lang="en")
    fig.canvas.draw()
    rotulos = [t.get_text() for t in fig.axes[-1].get_xticklabels() if t.get_text()]
    plt.close(fig)
    assert rotulos[0] == "2,000"


def test_map_caption_is_drawn_below_the_colour_bar():
    """The caption sat at a fixed spot under the map, on top of the bar."""
    import matplotlib.pyplot as plt

    fig = _mapa_log("choropleth")
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    legenda = [t for t in fig.texts if "climasus4py" in t.get_text()][0]
    barra = fig.axes[-1].get_tightbbox(renderer)
    caixa_legenda = legenda.get_window_extent(renderer)
    plt.close(fig)
    assert caixa_legenda.y1 <= barra.y0


# --- 08/11/2026: epidemic smoothing (M248) and seasonal boxplot (M249) -------------


def test_epidemic_loess_is_a_curve_with_a_band_not_a_straight_line():
    """loess/gam were swapped for lm with se=False. Now loess with R's default
    span (0.25) and a confidence band; the curve matched stats::loess to 1e-13
    on the tutorial-4 series."""
    import climasus4py as cs

    d = _serie_diaria()
    d["n_obitos"] = (10 + 5 * np.sin(np.arange(len(d)) * 2 * np.pi / 365)).round()
    g = cs.sus_data_plot_aggregate_ts(d, plot_type="epidemic", smooth_method="loess",
                                      lang="pt", verbose=False)
    g._build()
    assert [type(c.geom).__name__ for c in g.layers] == ["geom_area", "geom_line",
                                                         "geom_smooth"]
    curva = g.layers[2].data
    assert {"ymin", "ymax"} <= set(curva.columns)            # the band
    assert curva["y"].max() - curva["y"].min() > 5           # follows the seasons
    assert g.layers[2].stat.params["span"] == 0.25


def test_epidemic_gam_falls_back_to_loess_with_a_warning():
    import climasus4py as cs

    with pytest.warns(UserWarning, match="no GAM smoother"):
        g = cs.sus_data_plot_aggregate_ts(_serie_diaria(), plot_type="epidemic",
                                          smooth_method="gam", lang="pt", verbose=False)
    assert g.layers[2].stat.params["method"] == "loess"


def test_seasonal_is_a_boxplot_per_month_with_the_medians_joined():
    """It drew one bar per month with the mean when there was no group_col."""
    import climasus4py as cs

    g = cs.sus_data_plot_aggregate_ts(_serie_diaria(), plot_type="seasonal", lang="pt",
                                      verbose=False)
    g._build()
    assert [type(c.geom).__name__ for c in g.layers] == ["geom_boxplot", "geom_line",
                                                         "geom_point"]
    assert len(g.layers[0].data) == 12                        # one box per month
    # each box holds that month's total in each of the three years
    assert g.layers[0].data["ymax"].max() == 2 * 31         # 2015, two deaths a day


def test_seasonal_facets_by_the_facet_column():
    """facet_col was not kept in the monthly sums, so facet_wrap failed."""
    import climasus4py as cs

    a = _serie_diaria().assign(faixa="60-69")
    b = _serie_diaria().assign(faixa="80+")
    g = cs.sus_data_plot_aggregate_ts(pd.concat([a, b]), plot_type="seasonal",
                                      facet_col="faixa", lang="pt", verbose=False)
    g._build()
    assert g.layers[0].data["PANEL"].nunique() == 2
