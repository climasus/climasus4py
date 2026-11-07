"""Spatial branch fixes found comparing with climasus4r on SP 2023 (07/11/2026).

1. Moran with islands (M219). spdep's default ``adjust.n = TRUE`` takes regions
   with no neighbour out of n in I, E(I), Var(I) and the local Ii; esda keeps
   them. On the 645 SP municipalities (one island, Ilhabela) every statistic
   came out scaled by 644/645 -- the "~3% diff" the control sheet carried. The
   local Ii differed on every map: spdep divides by sum(z^2) / n, esda by
   sum(z^2) / (n - 1).
   After the fix: I 0.18196, E.I -0.001555, Z 7.545147, identical to R.
2. spatial/spacetime bayes and the default weights object (M220). The docs
   promised R's behaviour -- build the matrix from the neighbour list when
   ``W["W"]`` is absent --, the code did not: with the default
   ``return_matrix=False`` the call failed.
3. spatial bayes with a covariate on its natural scale (M218). Income in
   reais next to counts in the tens of thousands left NUTS at its starting
   point -- posterior sd 0, DIC 7e291, no error. Covariates are now centred
   and scaled for sampling and reported back on the original scale.
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import box

import climasus4py as cs
from climasus4py.enrichment.mod_spatial_bayes import _weights_matrix


def _grade(nx: int, ny: int, ilha: bool = False) -> gpd.GeoDataFrame:
    """nx x ny squares, plus optionally one square far from everything."""
    polys, codigos = [], []
    for i in range(nx):
        for j in range(ny):
            polys.append(box(i, j, i + 1, j + 1))
            codigos.append(f"35{len(codigos):05d}")
    if ilha:
        polys.append(box(50, 50, 51, 51))
        codigos.append(f"35{len(codigos):05d}")
    return gpd.GeoDataFrame({"code_muni": codigos}, geometry=polys, crs=4674)


def _moran_spdep(x, W_dense, n_isl):
    """Global I and E(I) by spdep's formulas with adjust.n = TRUE."""
    z = (x - x.mean()) / x.std(ddof=1)
    lz = W_dense @ z
    n_adj = len(x) - n_isl
    return (n_adj / W_dense.sum()) * (z @ lz) / (z @ z), -1.0 / (n_adj - 1)


@pytest.mark.parametrize("ilha", [False, True])
def test_moran_global_follows_spdep_adjust_n(ilha):
    muni = _grade(5, 5, ilha=ilha)
    rng = np.random.default_rng(3)
    df = pd.DataFrame({"code_muni": muni.code_muni, "y": rng.normal(size=len(muni))})
    W = cs.sus_mod_spatial_weights(muni, style="W", verbose=False)
    res = cs.sus_mod_spatial_moran(df, outcome="y", W=W, permutations=99, verbose=False)
    dense = W["listw"].full()[0]
    i_ref, ei_ref = _moran_spdep(df.y.to_numpy(), dense, int(ilha))
    assert res["global"]["I"].iloc[0] == pytest.approx(i_ref, rel=1e-12)
    assert res["global"]["E.I"].iloc[0] == pytest.approx(ei_ref, rel=1e-12)


@pytest.mark.parametrize("ilha", [False, True])
def test_moran_local_follows_spdep(ilha):
    """spdep: m2 = sum(z^2) / n; esda used n - 1 (differs even without islands)."""
    muni = _grade(5, 5, ilha=ilha)
    rng = np.random.default_rng(4)
    df = pd.DataFrame({"code_muni": muni.code_muni, "y": rng.normal(size=len(muni))})
    W = cs.sus_mod_spatial_weights(muni, style="W", verbose=False)
    res = cs.sus_mod_spatial_moran(df, outcome="y", W=W, permutations=99, verbose=False)
    x = df.y.to_numpy()
    z = (x - x.mean()) / x.std(ddof=1)
    lz = W["listw"].full()[0] @ z
    m2 = (z @ z) / len(z)
    np.testing.assert_allclose(res["local"]["Ii"], z * lz / m2, atol=1e-12)


def test_bayes_builds_matrix_from_neighbour_list():
    muni = _grade(4, 4)
    W = cs.sus_mod_spatial_weights(muni, style="W", verbose=False)   # W["W"] is None
    assert W["W"] is None
    codigos = sorted(muni.code_muni, reverse=True)                   # a different order
    mat = _weights_matrix(W, len(codigos), codes=codigos)
    ref = W["listw"].full()[0]
    ordem = [list(muni.code_muni).index(c) for c in codigos]
    np.testing.assert_array_equal(mat, (ref[np.ix_(ordem, ordem)] != 0).astype(float))


def test_bayes_unscaled_covariate_does_not_freeze_the_sampler():
    pytest.importorskip("pymc")
    muni = _grade(6, 6)
    rng = np.random.default_rng(5)
    renda = rng.uniform(200, 2000, len(muni))                     # reais, as in SP
    esperado = rng.uniform(50, 5000, len(muni))
    obs = rng.poisson(esperado * np.exp(0.0002 * (renda - renda.mean())))
    df = pd.DataFrame({"code_muni": muni.code_muni, "y": obs, "E": esperado, "renda": renda})
    W = cs.sus_mod_spatial_weights(muni, style="W", verbose=False)
    res = cs.sus_mod_spatial_bayes(df, outcome="y", W=W, covariates=["renda"], offset="E",
                                   n_iter=1500, burnin=500, thin=1, chains=1, verbose=False)
    fixo = res["fixed"].set_index("term")
    assert np.isfinite(res["dic"]) and res["dic"] < 1e6
    assert fixo.loc["renda", "sd"] > 0
    assert fixo.loc["renda", "lower95"] < 0.0002 < fixo.loc["renda", "upper95"]
