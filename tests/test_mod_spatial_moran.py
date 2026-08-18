"""Tests for sus_mod_spatial_moran (global Moran's I + LISA).

Requires geopandas + libpysal + esda + scipy, optional dependencies not in
the base install. Skips cleanly when unavailable.
"""

from __future__ import annotations

import pytest

gpd = pytest.importorskip("geopandas")
pytest.importorskip("libpysal")
pytest.importorskip("esda")
pytest.importorskip("scipy")

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from shapely.geometry import Polygon  # noqa: E402

from climasus4py.enrichment.mod_spatial_moran import sus_mod_spatial_moran  # noqa: E402
from climasus4py.enrichment.mod_spatial_weights import (  # noqa: E402
    sus_mod_spatial_weights,
)


def _grid_4x4_gdf() -> gpd.GeoDataFrame:
    """A 4x4 grid of unit squares, ids '1'..'16', row-major order."""
    squares = []
    codes = []
    n = 1
    for row in range(4):
        for col in range(4):
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
    return gpd.GeoDataFrame({"code_muni": codes}, geometry=squares, crs="EPSG:4326")


@pytest.fixture(scope="module")
def w4x4() -> dict:
    gdf = _grid_4x4_gdf()
    return sus_mod_spatial_weights(gdf, queen=False, verbose=False)


def test_block_pattern_positive_autocorrelation(w4x4):
    """Left 2 columns high, right 2 columns low -> strong positive Moran's I."""
    codes = [str(n) for n in range(1, 17)]
    # row-major 4x4: columns 0,1 (left) vs 2,3 (right) within each row of 4
    values = []
    for n in range(16):
        col = n % 4
        values.append(100.0 if col < 2 else 0.0)
    df = pd.DataFrame({"code_muni": codes, "outcome": values})

    res = sus_mod_spatial_moran(
        df, outcome="outcome", W=w4x4, permutations=499, lang="en", verbose=False
    )

    assert set(res.keys()) == {
        "global",
        "local",
        "n_HH",
        "n_LL",
        "n_HL",
        "n_LH",
        "outcome_name",
    }
    assert res["global"]["I"].iloc[0] > 0.3
    assert res["global"]["p_simulated"].iloc[0] < 0.05

    # FDR correction on 16 tests is conservative for such a small grid, so
    # check LISA sign-consistency with a relaxed alpha and no p-adjustment:
    # every significant unit must fall on the "correct" side (no HH/HL in
    # the low block or LL/LH in the high block), and column-0/column-3
    # units (whose rook-neighbours are entirely within their own block)
    # should be the ones that reach significance.
    res_relaxed = sus_mod_spatial_moran(
        df, outcome="outcome", W=w4x4, permutations=499,
        adjust_p="none", alpha=0.1, verbose=False,
    )
    local = res_relaxed["local"].set_index("code_muni")
    assert local.loc["5", "quadrant"] == "HH"
    assert local.loc["9", "quadrant"] == "HH"
    assert local.loc["4", "quadrant"] == "LL"
    assert local.loc["8", "quadrant"] == "LL"
    assert local.loc["12", "quadrant"] == "LL"
    assert not (local["quadrant"] == "HL").any()
    assert not (local["quadrant"] == "LH").any()


def test_checkerboard_negative_autocorrelation(w4x4):
    """Perfect checkerboard -> negative Moran's I; no HH/LL clusters."""
    codes = [str(n) for n in range(1, 17)]
    values = []
    for n in range(16):
        row, col = divmod(n, 4)
        values.append(100.0 if (row + col) % 2 == 0 else 0.0)
    df = pd.DataFrame({"code_muni": codes, "outcome": values})

    res = sus_mod_spatial_moran(
        df, outcome="outcome", W=w4x4, permutations=499, lang="pt", verbose=False
    )

    assert res["global"]["I"].iloc[0] < 0
    assert res["n_HH"] == 0
    assert res["n_LL"] == 0


def test_municipalities_filter_and_reorder(w4x4):
    codes = [str(n) for n in range(1, 17)]
    values = [float(n) for n in range(16)]
    df = pd.DataFrame({"code_muni": codes, "outcome": values})
    # shuffled subset order -- function must realign to W's internal order
    shuffled = codes[::-1]
    res = sus_mod_spatial_moran(
        df, outcome="outcome", W=w4x4, municipalities=shuffled,
        permutations=99, verbose=False,
    )
    assert list(res["local"]["code_muni"]) == list(w4x4["listw"].id_order)


def test_id_mismatch_raises(w4x4):
    df = pd.DataFrame({"code_muni": [str(n) for n in range(1, 16)], "outcome": range(15)})
    with pytest.raises(ValueError):
        sus_mod_spatial_moran(df, outcome="outcome", W=w4x4, verbose=False)


def test_not_a_weights_dict_raises():
    df = pd.DataFrame({"code_muni": ["1", "2"], "outcome": [1.0, 2.0]})
    with pytest.raises(TypeError):
        sus_mod_spatial_moran(df, outcome="outcome", W=object(), verbose=False)


def test_p_adjust_matches_r_reference():
    """Cross-check _p_adjust against R's stats::p.adjust() (values captured
    from an actual R session, incl. NA handling and the n=non-NA count quirk).
    """
    from climasus4py.enrichment.mod_spatial_moran import _p_adjust

    p = np.array([0.01, 0.5, 0.2, np.nan, 0.3, 0.04, 0.001])
    fdr = _p_adjust(p, "fdr")
    bonf = _p_adjust(p, "bonferroni")

    expected_fdr = [0.03, 0.5, 0.3, np.nan, 0.36, 0.08, 0.006]
    expected_bonf = [0.06, 1.0, 1.0, np.nan, 1.0, 0.24, 0.006]

    np.testing.assert_allclose(fdr, expected_fdr, rtol=1e-6, equal_nan=True)
    np.testing.assert_allclose(bonf, expected_bonf, rtol=1e-6, equal_nan=True)
