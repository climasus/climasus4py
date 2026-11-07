"""M212: the dlnm basis honours fixed knots. M211: sus_mod_af takes a seed.

M212 -- ``argvar={"fun": "ns", "knots": [...]}`` used to be accepted and
dropped: ``_crossbasis`` read only ``fun`` and ``df``, so the fit silently
used the default quantile knots. Fixed knots shared by every city are the
standard remedy for pooling DLNMs across cities (M65): with them the fixed
pool of six SP cities came out IDENTICAL to climasus4r (P99 1.620504
[1.406224; 1.867436], Q 81.99), where with per-city knots it differed
(1.761 x 1.684). Unknown keys now raise instead of vanishing.

M211 -- the Monte Carlo CI of ``sus_mod_af`` drew from an unseeded
generator, so the interval changed on every call and could not be
reproduced. ``seed=`` fixes the draws.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment.mod_dlnm import _basis_spec

LAG = 7


@pytest.fixture(scope="module")
def serie():
    rng = np.random.default_rng(11)
    dias = pd.date_range("2019-01-01", "2022-12-31", freq="D")
    temp = 22 + 7 * np.sin(2 * np.pi * (dias.dayofyear - 20) / 365) + rng.normal(0, 2.0, len(dias))
    risco = 14 * (1 + 0.03 * np.clip(temp - 26, 0, None) + 0.02 * np.clip(18 - temp, 0, None))
    df = pd.DataFrame({"date": dias, "n_obitos": rng.poisson(risco)})
    s = pd.Series(temp)
    for lag in range(LAG + 1):
        df[f"tair_dry_bulb_c_lag{lag}"] = s.shift(lag).values
    return df.dropna().reset_index(drop=True)


def _fit(df, argvar=None, arglag=None):
    return cs.sus_mod_dlnm(df, climate_col="tair_dry_bulb_c", lag_max=LAG,
                           argvar=argvar, arglag=arglag, verbose=False)


# ---------------------------------------------------------------------------
# M212 -- knots
# ---------------------------------------------------------------------------


def test_fixed_knots_are_used(serie):
    fit = _fit(serie, argvar={"fun": "ns", "knots": [18, 21, 24]})
    meta = fit["meta"]["var_meta"]
    np.testing.assert_allclose(meta["knots"], [18, 21, 24])
    assert meta["df"] == 4          # splines::ns: 3 interior knots + 1


def test_boundary_knots_in_both_spellings(serie):
    for chave in ("boundary_knots", "Boundary.knots"):
        fit = _fit(serie, argvar={"fun": "ns", "knots": [18, 24], chave: [5, 40]})
        assert fit["meta"]["var_meta"]["boundary_knots"] == (5.0, 40.0)


def test_unsorted_knots_are_sorted(serie):
    fit = _fit(serie, argvar={"fun": "ns", "knots": [24, 18, 21]})
    np.testing.assert_allclose(fit["meta"]["var_meta"]["knots"], [18, 21, 24])


def test_default_still_takes_knots_from_quantiles(serie):
    fit = _fit(serie)
    x = serie[[f"tair_dry_bulb_c_lag{k}" for k in range(LAG + 1)]].to_numpy().ravel(order="F")
    np.testing.assert_allclose(fit["meta"]["var_meta"]["knots"], np.quantile(x, [0.25, 0.5, 0.75]))


def test_knots_change_the_fit(serie):
    base = _fit(serie)
    fixo = _fit(serie, argvar={"fun": "ns", "knots": [15, 20, 28]})
    assert not np.isclose(float(base["models"]["rr"].iloc[0]), float(fixo["models"]["rr"].iloc[0]))


def test_lag_basis_takes_knots_too(serie):
    fit = _fit(serie, arglag={"fun": "ns", "knots": [2]})
    np.testing.assert_allclose(fit["meta"]["lag_meta"]["knots"], [2])


@pytest.mark.parametrize("spec", [
    {"fun": "ns", "kn0ts": [18]},          # typo used to vanish in silence
    {"fun": "ns", "df": 4, "intercept": True},
])
def test_unknown_key_raises(spec):
    with pytest.raises(ValueError, match="argvar"):
        _basis_spec(spec, "argvar", "en")


def test_knots_with_non_ns_basis_raises():
    with pytest.raises(ValueError, match="only apply"):
        _basis_spec({"fun": "lin", "knots": [20]}, "argvar", "en")


def test_poly_takes_r_degree():
    assert _basis_spec({"fun": "poly", "degree": 3}, "argvar", "en")["df"] == 3


# ---------------------------------------------------------------------------
# M211 -- seed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ajuste(serie):
    return _fit(serie)


def test_same_seed_same_interval(ajuste):
    a = cs.sus_mod_af(ajuste, nsim=200, seed=7, verbose=False)["total"]
    b = cs.sus_mod_af(ajuste, nsim=200, seed=7, verbose=False)["total"]
    pd.testing.assert_frame_equal(a, b)


def test_other_seed_other_interval_same_point(ajuste):
    a = cs.sus_mod_af(ajuste, nsim=200, seed=7, verbose=False)["total"]
    b = cs.sus_mod_af(ajuste, nsim=200, seed=8, verbose=False)["total"]
    np.testing.assert_allclose(a["af"], b["af"])               # point: no randomness
    assert not np.allclose(a["af_lo"], b["af_lo"])


def test_seed_is_recorded_and_default_still_runs(ajuste):
    af = cs.sus_mod_af(ajuste, nsim=50, verbose=False)
    assert af["meta"]["seed"] is None
    assert cs.sus_mod_af(ajuste, nsim=50, seed=3, verbose=False)["meta"]["seed"] == 3
