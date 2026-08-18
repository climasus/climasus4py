"""Tests for sus_mod_spatial_weights (spatial contiguity weights).

Requires geopandas + libpysal, which are optional dependencies not in the
base install. Skips cleanly when unavailable.
"""

from __future__ import annotations

import pytest

gpd = pytest.importorskip("geopandas")
pytest.importorskip("libpysal")

import numpy as np  # noqa: E402
from shapely.geometry import Polygon  # noqa: E402

from climasus4py.enrichment.mod_spatial_weights import (  # noqa: E402
    sus_mod_spatial_weights,
)


def _grid_3x3_gdf(with_island: bool = False) -> gpd.GeoDataFrame:
    """A 3x3 grid of unit squares (ids '1'..'9'), optionally + a far island."""
    squares = []
    codes = []
    n = 1
    for row in range(3):
        for col in range(3):
            squares.append(
                Polygon(
                    [
                        (col, row),
                        (col + 1, row),
                        (col + 1, row + 1),
                        (col, row + 1),
                    ]
                )
            )
            codes.append(str(n))
            n += 1

    if with_island:
        squares.append(Polygon([(100, 100), (101, 100), (101, 101), (100, 101)]))
        codes.append(str(n))

    return gpd.GeoDataFrame({"code_muni": codes}, geometry=squares, crs="EPSG:4326")


# Expected Queen cardinalities for a 3x3 grid, cell ids 1..9 (row-major):
# corners (1,3,7,9): 3 neighbours; edges (2,4,6,8): 5 neighbours; center (5): 8.
_QUEEN_CARD = {
    "1": 3, "2": 5, "3": 3,
    "4": 5, "5": 8, "6": 5,
    "7": 3, "8": 5, "9": 3,
}
_ROOK_CARD = {
    "1": 2, "2": 3, "3": 2,
    "4": 3, "5": 4, "6": 3,
    "7": 2, "8": 3, "9": 2,
}


def test_queen_cardinalities_and_no_islands():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, verbose=False)
    assert out["n_regions"] == 9
    assert out["n_islands"] == 0
    assert out["island_ids"] == []
    for rid, expected in _QUEEN_CARD.items():
        assert len(out["nb"][rid]) == expected


def test_rook_cardinalities():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, queen=False, verbose=False)
    for rid, expected in _ROOK_CARD.items():
        assert len(out["nb"][rid]) == expected


def test_islands_detected_with_zero_policy_true():
    gdf = _grid_3x3_gdf(with_island=True)
    out = sus_mod_spatial_weights(gdf, zero_policy=True, verbose=False)
    assert out["n_regions"] == 10
    assert out["n_islands"] == 1
    assert out["island_ids"] == ["10"]


def test_islands_raise_with_zero_policy_false():
    gdf = _grid_3x3_gdf(with_island=True)
    with pytest.raises(ValueError):
        sus_mod_spatial_weights(gdf, zero_policy=False, verbose=False)


def test_style_w_row_standardised():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, style="W", return_matrix=True, verbose=False)
    W = out["W"]
    row_sums = W.sum(axis=1)
    np.testing.assert_allclose(row_sums, 1.0)
    # corner "1" is region index 0 in id_order; its 3 neighbours should be 1/3 each
    idx = out["listw"].id_order.index("1")
    nonzero = W[idx][W[idx] > 0]
    np.testing.assert_allclose(nonzero, 1 / 3)


def test_style_b_binary():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, style="B", return_matrix=True, verbose=False)
    W = out["W"]
    nonzero_vals = W[W > 0]
    np.testing.assert_allclose(nonzero_vals, 1.0)
    assert W.sum() == sum(_QUEEN_CARD.values())


def test_style_c_globally_standardised():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, style="C", return_matrix=True, verbose=False)
    W = out["W"]
    n = out["n_regions"]
    total_links = sum(_QUEEN_CARD.values())
    expected_edge = n / total_links
    nonzero_vals = W[W > 0]
    np.testing.assert_allclose(nonzero_vals, expected_edge)
    # "sums over all links to n"
    np.testing.assert_allclose(W.sum(), n)


def test_style_u_is_c_over_n():
    gdf = _grid_3x3_gdf()
    out_c = sus_mod_spatial_weights(gdf, style="C", return_matrix=True, verbose=False)
    out_u = sus_mod_spatial_weights(gdf, style="U", return_matrix=True, verbose=False)
    np.testing.assert_allclose(out_u["W"], out_c["W"] / out_c["n_regions"])
    # "sums over all links to unity"
    np.testing.assert_allclose(out_u["W"].sum(), 1.0)


def test_style_minmax():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, style="minmax", return_matrix=True, verbose=False)
    W = out["W"]
    max_card = max(_QUEEN_CARD.values())  # 8, the center cell
    nonzero_vals = W[W > 0]
    np.testing.assert_allclose(nonzero_vals, 1 / max_card)


def test_style_s_variance_stabilising_matches_libpysal_v_transform():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, style="S", return_matrix=True, verbose=False)
    W = out["W"]
    assert not np.isnan(W).any()
    assert (W >= 0).all()
    # sums over all links to n, per spdep's documented S-style property
    np.testing.assert_allclose(W.sum(), out["n_regions"])


def test_invalid_style_raises():
    gdf = _grid_3x3_gdf()
    with pytest.raises(ValueError):
        sus_mod_spatial_weights(gdf, style="bogus", verbose=False)


def test_not_geodataframe_raises_typeerror():
    with pytest.raises(TypeError):
        sus_mod_spatial_weights({"not": "a geodataframe"}, verbose=False)


def test_empty_geodataframe_raises_valueerror():
    empty = gpd.GeoDataFrame({"code_muni": []}, geometry=[], crs="EPSG:4326")
    with pytest.raises(ValueError):
        sus_mod_spatial_weights(empty, verbose=False)


def test_meta_shape():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, verbose=False)
    assert out["meta"]["stage"] == "mod"
    assert out["meta"]["type"] == "spatial_weights"
    assert isinstance(out["meta"]["history"], list)


def test_return_matrix_default_none():
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, verbose=False)
    assert out["W"] is None


def test_w_row_col_order_matches_input_geodataframe():
    """listw.id_order (and hence W's row/col order) must follow the input
    GeoDataFrame's row order -- this is the contract downstream
    sus_mod_spatial_moran / sus_mod_spatial_reg rely on to align a data
    vector with the weights matrix.
    """
    gdf = _grid_3x3_gdf()
    out = sus_mod_spatial_weights(gdf, verbose=False)
    assert out["listw"].id_order == list(gdf["code_muni"])


def test_style_c_u_minmax_with_island_use_total_region_count():
    """Pins down the (unverifiable-without-spdep) assumption that C/U's
    ``n`` includes zero-neighbour islands under zero_policy=True, i.e.
    ``n = total regions`` not ``n = regions with >=1 neighbour``.
    Flagged in IDEIAS.md -- if this diverges from spdep's actual
    zero.policy handling of "n", this is the one test that will catch it.
    """
    gdf = _grid_3x3_gdf(with_island=True)
    total_links = sum(_QUEEN_CARD.values())  # island contributes 0 links
    n_total = 10  # 9 grid cells + 1 island

    out_c = sus_mod_spatial_weights(gdf, style="C", return_matrix=True, verbose=False)
    expected_edge_c = n_total / total_links
    nonzero_c = out_c["W"][out_c["W"] > 0]
    np.testing.assert_allclose(nonzero_c, expected_edge_c)
    np.testing.assert_allclose(out_c["W"].sum(), n_total)

    out_u = sus_mod_spatial_weights(gdf, style="U", return_matrix=True, verbose=False)
    nonzero_u = out_u["W"][out_u["W"] > 0]
    np.testing.assert_allclose(nonzero_u, 1.0 / total_links)
    np.testing.assert_allclose(out_u["W"].sum(), 1.0)

    out_mm = sus_mod_spatial_weights(
        gdf, style="minmax", return_matrix=True, verbose=False
    )
    max_card = max(_QUEEN_CARD.values())
    nonzero_mm = out_mm["W"][out_mm["W"] > 0]
    np.testing.assert_allclose(nonzero_mm, 1.0 / max_card)
