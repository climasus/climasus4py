"""Tests for sus_data_plot_aggregate_map."""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("matplotlib", reason="matplotlib not installed")
pytest.importorskip("plotnine", reason="plotnine not installed")
pytest.importorskip("geopandas", reason="geopandas not installed")

import matplotlib  # noqa: E402

matplotlib.use("Agg")

import geopandas as gpd  # noqa: E402
from shapely.geometry import Point, box  # noqa: E402

from climasus4py.viz.data_plot_aggregate_map import sus_data_plot_aggregate_map  # noqa: E402


def _make_agg_df() -> pd.DataFrame:
    """Synthetic sus_data_aggregate(geo='municipality') output shape."""
    return pd.DataFrame(
        {
            "time_group": ["2022-01", "2022-02", "2022-01", "2022-02", "2022-01"],
            "municipality_code": ["355030", "355030", "330455", "330455", "520870"],
            "count": [10, 12, 5, 6, 3],
            "sum_deaths": [4.0, 5.0, 2.0, 1.0, 0.0],
            "mean_deaths": [0.4, 0.42, 0.4, 0.17, 0.0],
        }
    )


def _make_munis() -> gpd.GeoDataFrame:
    coords = {
        "355030": (-46.6, -23.5),
        "330455": (-43.2, -22.9),
        "520870": (-49.3, -16.7),
    }
    rows = []
    for i, (code, (lon, lat)) in enumerate(coords.items()):
        rows.append(
            {
                "code_muni": code,
                "name": f"City{i}",
                "lon": lon,
                "lat": lat,
                "pop": 1_000_000.0 * (i + 1),
                "is_capital": i == 0,
                "code_state": code[:2],
                "geometry": box(lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5),
            }
        )
    return gpd.GeoDataFrame(rows, geometry="geometry")


class TestMapTypes:
    def test_bubble_draws(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(), municipalities=_make_munis(), map_type="bubble", verbose=False
        )
        assert type(p).__name__ == "ggplot"
        p.draw()

    def test_choropleth_draws(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(), municipalities=_make_munis(), map_type="choropleth", verbose=False
        )
        p.draw()

    def test_quantile_choropleth_draws(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(),
            municipalities=_make_munis(),
            map_type="quantile_choropleth",
            verbose=False,
        )
        p.draw()

    def test_rate_per_100k_draws(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(),
            municipalities=_make_munis(),
            map_type="bubble",
            rate_per_100k=True,
            verbose=False,
        )
        p.draw()

    def test_top_n_labels_draws(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(),
            municipalities=_make_munis(),
            map_type="bubble",
            top_n=2,
            verbose=False,
        )
        p.draw()

    def test_choropleth_without_geometry_falls_back_to_bubble(self):
        munis = _make_munis().drop(columns=["geometry"])
        munis = pd.DataFrame(munis)
        with pytest.warns(UserWarning, match="(?i)bubble|bolhas|geometry|geometria"):
            p = sus_data_plot_aggregate_map(
                _make_agg_df(), municipalities=munis, map_type="choropleth", verbose=False
            )
        p.draw()


class TestValidation:
    def test_missing_municipalities_raises(self):
        with pytest.raises(ValueError, match="municipalities"):
            sus_data_plot_aggregate_map(_make_agg_df(), verbose=False)

    def test_explicit_value_col(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(),
            value_col="sum_deaths",
            municipalities=_make_munis(),
            map_type="bubble",
            verbose=False,
        )
        p.draw()

    def test_bad_value_col_raises(self):
        with pytest.raises(ValueError):
            sus_data_plot_aggregate_map(
                _make_agg_df(),
                value_col="nope",
                municipalities=_make_munis(),
                verbose=False,
            )

    def test_period_warns_but_still_plots(self):
        with pytest.warns(UserWarning, match="(?i)period"):
            p = sus_data_plot_aggregate_map(
                _make_agg_df(),
                municipalities=_make_munis(),
                period="2022-01-01",
                verbose=False,
            )
        p.draw()

    def test_interactive_raises_import_error(self):
        with pytest.raises(ImportError):
            sus_data_plot_aggregate_map(
                _make_agg_df(), municipalities=_make_munis(), interactive=True
            )

    def test_city_filter(self):
        p = sus_data_plot_aggregate_map(
            _make_agg_df(),
            municipalities=_make_munis(),
            city="355030",
            verbose=False,
        )
        p.draw()


def test_bubble_derives_centroid_from_polygon_only_municipalities():
    """No lon/lat in municipalities -> derive bubble coords from centroids."""
    munis = _make_munis().drop(columns=["lon", "lat"])
    p = sus_data_plot_aggregate_map(
        _make_agg_df(), municipalities=munis, map_type="bubble", verbose=False
    )
    p.draw()


def test_bubble_without_lonlat_or_geometry_raises():
    munis = pd.DataFrame(
        {"code_muni": ["355030", "330455"], "name": ["A", "B"]}
    )
    df = pd.DataFrame({"municipality_code": ["355030", "330455"], "count": [10, 5]})
    with pytest.raises(ValueError, match="(?i)lon|lat|geometry"):
        sus_data_plot_aggregate_map(df, municipalities=munis, map_type="bubble", verbose=False)


def test_outcome_autodetect_skips_non_numeric_muni_code():
    """Auto-detection must not pick the municipality-code column as value_col."""
    df = pd.DataFrame(
        {
            "municipality_code": ["355030", "330455"],
            "total": [10, 5],
        }
    )
    p = sus_data_plot_aggregate_map(df, municipalities=_make_munis(), verbose=False)
    p.draw()


def test_synthetic_point_only_municipalities_bubble():
    """Municipalities supplied as a plain point GeoDataFrame (no polygons)."""
    munis = gpd.GeoDataFrame(
        {
            "code_muni": ["355030", "330455"],
            "name": ["A", "B"],
            "lon": [-46.6, -43.2],
            "lat": [-23.5, -22.9],
        },
        geometry=[Point(-46.6, -23.5), Point(-43.2, -22.9)],
    )
    df = pd.DataFrame(
        {
            "municipality_code": ["355030", "330455"],
            "count": [10, 5],
        }
    )
    p = sus_data_plot_aggregate_map(df, municipalities=munis, verbose=False)
    p.draw()
