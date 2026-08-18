"""Tests for sus_mod_spatial_reg (SAR/SEM/SDM spatial regression).

Requires spreg + libpysal + esda, optional dependencies not in the base
install. Skips cleanly when unavailable.

Synthetic data is generated WITH a known spatial process (y = rho * W @ y +
X @ beta + eps for the lag model, solved analytically), so the tests verify
that the fitted model recovers rho/beta close to their true values -- not
just "doesn't crash".
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("spreg")
libpysal = pytest.importorskip("libpysal")
pytest.importorskip("esda")

from climasus4py.enrichment.mod_spatial_reg import sus_mod_spatial_reg  # noqa: E402


def _weights_dict(n_side: int = 10) -> dict:
    """Build a climasus_weights-style dict from a regular lattice.

    Uses libpysal's rook lattice weights directly -- this is a standalone
    test fixture, not a call into sus_mod_spatial_weights (which needs real
    polygons); the contract (dict with a "listw" W object whose id_order
    matches df row order) is exactly what sus_mod_spatial_reg consumes.
    """
    w = libpysal.weights.lat2W(n_side, n_side, rook=True, id_type="string")
    w.transform = "r"
    return {
        "listw": w,
        "nb": {rid: list(neigh) for rid, neigh in w.neighbors.items()},
        "n_regions": w.n,
        "n_islands": 0,
        "island_ids": [],
        "W": None,
        "style": "W",
        "meta": {"stage": "mod", "type": "spatial_weights", "history": []},
    }


def _simulate_lag_data(w_dict: dict, rho: float, beta: np.ndarray, seed: int = 0):
    """y = (I - rho*W)^-1 @ (X @ beta + eps), the textbook SAR DGP."""
    rng = np.random.default_rng(seed)
    listw = w_dict["listw"]
    n = listw.n
    ids = list(listw.id_order)
    w_dense, _ = listw.full()

    x1 = rng.normal(size=n)
    x2 = rng.normal(size=n)
    x_mat = np.column_stack([np.ones(n), x1, x2])
    eps = rng.normal(scale=0.5, size=n)

    identity = np.eye(n)
    y = np.linalg.solve(identity - rho * w_dense, x_mat @ beta + eps)

    df = pd.DataFrame({"code_muni": ids, "y": y, "x1": x1, "x2": x2})
    return df


def test_sus_mod_spatial_reg_lag_recovers_rho_and_beta():
    w_dict = _weights_dict(n_side=10)
    true_beta = np.array([1.0, 2.0, -1.5])  # intercept, x1, x2
    true_rho = 0.5
    df = _simulate_lag_data(w_dict, true_rho, true_beta, seed=42)

    result = sus_mod_spatial_reg(
        df=df,
        formula="y ~ x1 + x2",
        W=w_dict,
        model="lag",
        verbose=False,
    )

    assert result["model"] == "lag"
    assert result["lambda"] is None
    assert result["rho"] is not None
    assert abs(result["rho"] - true_rho) < 0.1

    coef = result["coefficients"].set_index("term")["estimate"]
    assert abs(coef["CONSTANT"] - true_beta[0]) < 0.3
    assert abs(coef["x1"] - true_beta[1]) < 0.3
    assert abs(coef["x2"] - true_beta[2]) < 0.3

    assert result["impacts"] is not None
    assert set(result["impacts"]["term"]) == {"x1", "x2"}
    assert result["aic"] is not None
    assert len(result["fitted"]) == len(df)
    assert len(result["residuals"]) == len(df)
    assert set(result["moran_residuals"].keys()) == {"I", "p_value", "z"}


def test_sus_mod_spatial_reg_error_recovers_lambda_and_beta():
    w_dict = _weights_dict(n_side=10)
    listw = w_dict["listw"]
    n = listw.n
    ids = list(listw.id_order)
    w_dense, _ = listw.full()

    rng = np.random.default_rng(7)
    true_lambda = 0.6
    true_beta = np.array([0.5, 1.5, -2.0])
    x1 = rng.normal(size=n)
    x2 = rng.normal(size=n)
    x_mat = np.column_stack([np.ones(n), x1, x2])
    eps = rng.normal(scale=0.5, size=n)
    identity = np.eye(n)
    u = np.linalg.solve(identity - true_lambda * w_dense, eps)
    y = x_mat @ true_beta + u

    df = pd.DataFrame({"code_muni": ids, "y": y, "x1": x1, "x2": x2})

    result = sus_mod_spatial_reg(
        df=df, formula="y ~ x1 + x2", W=w_dict, model="error", verbose=False
    )

    assert result["model"] == "error"
    assert result["rho"] is None
    assert result["lambda"] is not None
    assert abs(result["lambda"] - true_lambda) < 0.15
    assert result["impacts"] is None

    coef = result["coefficients"].set_index("term")["estimate"]
    assert abs(coef["CONSTANT"] - true_beta[0]) < 0.3
    assert abs(coef["x1"] - true_beta[1]) < 0.3
    assert abs(coef["x2"] - true_beta[2]) < 0.3


def test_sus_mod_spatial_reg_error_method_lu():
    """method="LU" is only valid with model="error" -- must not raise."""
    w_dict = _weights_dict(n_side=8)
    listw = w_dict["listw"]
    n = listw.n
    ids = list(listw.id_order)
    w_dense, _ = listw.full()

    rng = np.random.default_rng(11)
    true_lambda = 0.4
    true_beta = np.array([0.5, 1.0])
    x1 = rng.normal(size=n)
    x_mat = np.column_stack([np.ones(n), x1])
    eps = rng.normal(scale=0.5, size=n)
    u = np.linalg.solve(np.eye(n) - true_lambda * w_dense, eps)
    y = x_mat @ true_beta + u
    df = pd.DataFrame({"code_muni": ids, "y": y, "x1": x1})

    result = sus_mod_spatial_reg(
        df=df, formula="y ~ x1", W=w_dict, model="error", method="LU", verbose=False
    )
    # Smoke test for the LU branch specifically (recovery accuracy with a
    # single small seed is already covered by the "eigen" method test
    # above); just check the fit ran and returned a plausible lambda.
    assert result["lambda"] is not None
    assert -1.0 < result["lambda"] < 1.0


def test_sus_mod_spatial_reg_durbin_runs_and_has_lagged_terms():
    w_dict = _weights_dict(n_side=8)
    true_beta = np.array([1.0, 1.0, -1.0])
    df = _simulate_lag_data(w_dict, rho=0.3, beta=true_beta, seed=3)

    result = sus_mod_spatial_reg(
        df=df, formula="y ~ x1 + x2", W=w_dict, model="durbin", verbose=False
    )

    assert result["model"] == "durbin"
    terms = set(result["coefficients"]["term"])
    assert {"x1", "x2", "W_x1", "W_x2"}.issubset(terms)
    assert result["rho"] is not None


def test_sus_mod_spatial_reg_sac_not_implemented():
    w_dict = _weights_dict(n_side=5)
    df = pd.DataFrame(
        {
            "code_muni": list(w_dict["listw"].id_order),
            "y": np.random.default_rng(0).normal(size=w_dict["listw"].n),
            "x1": np.random.default_rng(1).normal(size=w_dict["listw"].n),
        }
    )
    with pytest.raises(NotImplementedError):
        sus_mod_spatial_reg(df=df, formula="y ~ x1", W=w_dict, model="sac", verbose=False)


def test_sus_mod_spatial_reg_method_chebyshev_not_implemented():
    w_dict = _weights_dict(n_side=5)
    df = pd.DataFrame(
        {
            "code_muni": list(w_dict["listw"].id_order),
            "y": np.random.default_rng(0).normal(size=w_dict["listw"].n),
            "x1": np.random.default_rng(1).normal(size=w_dict["listw"].n),
        }
    )
    with pytest.raises(NotImplementedError):
        sus_mod_spatial_reg(
            df=df, formula="y ~ x1", W=w_dict, model="lag", method="Chebyshev", verbose=False
        )


def test_sus_mod_spatial_reg_invalid_formula():
    w_dict = _weights_dict(n_side=5)
    df = pd.DataFrame(
        {
            "code_muni": list(w_dict["listw"].id_order),
            "y": np.random.default_rng(0).normal(size=w_dict["listw"].n),
        }
    )
    with pytest.raises(ValueError):
        sus_mod_spatial_reg(df=df, formula="no tilde here", W=w_dict, model="lag", verbose=False)


def test_sus_mod_spatial_reg_row_mismatch():
    w_dict = _weights_dict(n_side=5)
    df = pd.DataFrame({"y": [1.0, 2.0], "x1": [1.0, 2.0]})  # wrong length
    with pytest.raises(ValueError):
        sus_mod_spatial_reg(df=df, formula="y ~ x1", W=w_dict, model="lag", verbose=False)
