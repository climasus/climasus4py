"""Tests for sus_mod_plot_spatial_moran."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("plotnine", reason="plotnine not installed")
pytest.importorskip("geopandas", reason="geopandas not installed")
pytest.importorskip("libpysal", reason="libpysal not installed")
pytest.importorskip("esda", reason="esda not installed")

import geopandas as gpd  # noqa: E402
from shapely.geometry import box  # noqa: E402

from climasus4py.enrichment.mod_spatial_moran import sus_mod_spatial_moran  # noqa: E402
from climasus4py.enrichment.mod_spatial_weights import sus_mod_spatial_weights  # noqa: E402
from climasus4py.viz.mod_plot_spatial_moran import sus_mod_plot_spatial_moran  # noqa: E402


def _make_grid(n_side: int = 4) -> gpd.GeoDataFrame:
    polys, ids = [], []
    for i in range(n_side):
        for j in range(n_side):
            polys.append(box(i, j, i + 1, j + 1))
            ids.append(f"{i}{j}")
    return gpd.GeoDataFrame({"code_muni": ids, "geometry": polys})


def _make_moran_result(seed: int = 0):
    gdf = _make_grid()
    rng = np.random.default_rng(seed)
    w = sus_mod_spatial_weights(gdf, verbose=False)
    vals = 10 + 5 * ((np.arange(len(gdf)) // 4) % 2) + rng.normal(0, 0.5, len(gdf))
    df = pd.DataFrame({"code_muni": gdf["code_muni"], "val": vals})
    result = sus_mod_spatial_moran(df, outcome="val", W=w, permutations=99, verbose=False)
    return result, gdf


class TestPlotTypes:
    def test_scatter_draws(self):
        result, _ = _make_moran_result()
        p = sus_mod_plot_spatial_moran(result, type="scatter")
        p.draw()

    def test_map_draws_with_municipalities(self):
        result, gdf = _make_moran_result()
        p = sus_mod_plot_spatial_moran(result, municipalities=gdf, type="map")
        p.draw()

    def test_both_returns_dict_of_two_plots(self):
        result, gdf = _make_moran_result()
        out = sus_mod_plot_spatial_moran(result, municipalities=gdf, type="both")
        assert isinstance(out, dict)
        assert set(out.keys()) == {"map", "scatter"}
        for p in out.values():
            p.draw()

    def test_map_without_municipalities_raises(self):
        result, _ = _make_moran_result()
        with pytest.raises((ValueError, TypeError)):
            sus_mod_plot_spatial_moran(result, type="map")


class TestValidation:
    def test_unsupported_lang_warns_and_falls_back(self):
        result, _ = _make_moran_result()
        with pytest.warns(UserWarning, match="(?i)lang|idioma|language"):
            p = sus_mod_plot_spatial_moran(result, type="scatter", lang="fr")
        p.draw()
