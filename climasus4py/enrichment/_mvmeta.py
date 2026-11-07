"""Multivariate random-effects meta-analysis (the ``mvmeta`` core).

Port of the estimation engine behind R's ``mvmeta`` package (Gasparrini,
Armstrong & Kenward 2012, *Stat Med* 31:3821-3839), which ``sus_mod_pool()``
and ``sus_mod_metaregression()`` use to combine city-specific DLNM
coefficients.

The model. Study *i* contributes a ``p``-vector of estimates ``y_i`` with a
*known* within-study covariance ``S_i``; the estimates scatter around a
common mean with an unknown *between*-study covariance ``Psi``::

    y_i ~ N(X_i @ beta, S_i + Psi)

For plain pooling ``X_i`` is the identity and ``beta`` is the pooled
coefficient vector. With ``q`` study-level moderators (meta-regression)
``X_i = kron(x_i, I_p)`` and ``beta`` has length ``q * p``.

Why this can be ported faithfully without the R package installed: the
REML estimate is defined by the restricted likelihood, not by how the
optimiser walks to it. ``mvmeta`` parametrises ``Psi`` through a Cholesky
factor purely to keep the search unconstrained; any parametrisation
covering the positive-semidefinite cone reaches the same optimum. So the
numbers here are determined by the model, and an installed ``mvmeta``
would be a *check* on this code rather than its specification.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np

Method = Literal["reml", "ml", "fixed"]

_LOG_2PI = float(np.log(2.0 * np.pi))


def _require_scipy() -> Any:
    try:
        from scipy import optimize  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - depends on install
        raise ImportError(
            "scipy is required for multivariate meta-analysis "
            "(sus_mod_pool / sus_mod_metaregression). Install with "
            "'pip install scipy'."
        ) from exc
    return optimize


# ---------------------------------------------------------------------------
# Psi <-> unconstrained parameter vector
# ---------------------------------------------------------------------------

def _n_par(p: int) -> int:
    """Number of free parameters in a ``p x p`` Cholesky factor."""
    return p * (p + 1) // 2


def _par_to_psi(par: np.ndarray, p: int) -> np.ndarray:
    """Build ``Psi = L @ L.T`` from the lower triangle of ``L``.

    The diagonal is *not* exponentiated. That keeps ``Psi = 0`` reachable
    at ``par = 0``, which matters: the no-heterogeneity solution sits on
    the boundary of the parameter space and is a genuine REML optimum for
    homogeneous studies. Exponentiating the diagonal would push it to
    minus infinity and the optimiser would never arrive.
    """
    L = np.zeros((p, p), dtype=float)
    L[np.tril_indices(p)] = par
    return L @ L.T


def _psi_to_par(psi: np.ndarray, p: int) -> np.ndarray:
    """Inverse of :func:`_par_to_psi`, used to seed the optimiser."""
    # Nudge the diagonal so a singular starting Psi still factorises.
    eye = np.eye(p) * (1e-8 * max(1.0, float(np.trace(psi)) / p))
    try:
        L = np.linalg.cholesky(psi + eye)
    except np.linalg.LinAlgError:
        L = np.diag(np.sqrt(np.maximum(np.diag(psi), 1e-8)))
    return L[np.tril_indices(p)]


# ---------------------------------------------------------------------------
# Generalised least squares at a fixed Psi
# ---------------------------------------------------------------------------

@dataclass
class _GLS:
    beta: np.ndarray
    vcov_beta: np.ndarray
    logdet_sigma: float
    logdet_info: float
    quad: float
    ok: bool


def _gls(
    y: np.ndarray,
    S: list[np.ndarray],
    X: list[np.ndarray],
    psi: np.ndarray,
) -> _GLS:
    """Solve the GLS problem with ``Psi`` held fixed.

    Returns the pieces every likelihood below needs: the estimate, its
    covariance, and the three scalars that make up the log-likelihood
    (``log|Sigma_i|`` summed, ``log|information|``, and the weighted sum
    of squares).
    """
    m = len(S)
    nb = X[0].shape[1]
    info = np.zeros((nb, nb))
    rhs = np.zeros(nb)
    logdet_sigma = 0.0
    chols: list[np.ndarray] = []

    for i in range(m):
        sigma = S[i] + psi
        try:
            chol = np.linalg.cholesky(sigma)
        except np.linalg.LinAlgError:
            return _GLS(np.zeros(nb), np.zeros((nb, nb)), np.inf, np.inf, np.inf, False)
        chols.append(chol)
        logdet_sigma += 2.0 * float(np.sum(np.log(np.diag(chol))))
        # Sigma^-1 @ [X_i, y_i] via the factor, never forming the inverse.
        rhs_i = np.column_stack([X[i], y[i]])
        sol = _chol_solve(chol, rhs_i)
        wx, wy = sol[:, :-1], sol[:, -1]
        info += X[i].T @ wx
        rhs += X[i].T @ wy

    try:
        chol_info = np.linalg.cholesky(info)
    except np.linalg.LinAlgError:
        return _GLS(np.zeros(nb), np.zeros((nb, nb)), np.inf, np.inf, np.inf, False)
    logdet_info = 2.0 * float(np.sum(np.log(np.diag(chol_info))))
    beta = _chol_solve(chol_info, rhs)
    vcov_beta = _chol_inv(chol_info)

    quad = 0.0
    for i in range(m):
        res = y[i] - X[i] @ beta
        quad += float(res @ _chol_solve(chols[i], res))

    return _GLS(beta, vcov_beta, logdet_sigma, logdet_info, quad, True)


def _chol_solve(chol: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Solve ``A @ x = b`` given the lower Cholesky factor of ``A``."""
    from scipy.linalg import solve_triangular  # noqa: PLC0415

    z = solve_triangular(chol, b, lower=True)
    return solve_triangular(chol.T, z, lower=False)


def _chol_inv(chol: np.ndarray) -> np.ndarray:
    """Invert ``A`` given its lower Cholesky factor."""
    return _chol_solve(chol, np.eye(chol.shape[0]))


# ---------------------------------------------------------------------------
# Log-likelihoods
# ---------------------------------------------------------------------------

def _neg_loglik(
    par: np.ndarray,
    y: np.ndarray,
    S: list[np.ndarray],
    X: list[np.ndarray],
    p: int,
    restricted: bool,
) -> float:
    """Negative (restricted) log-likelihood at a Cholesky parameter vector."""
    gls = _gls(y, S, X, _par_to_psi(par, p))
    if not gls.ok:
        return 1e10
    n_obs = sum(len(v) for v in y)
    nb = X[0].shape[1]
    const = -0.5 * (n_obs - (nb if restricted else 0)) * _LOG_2PI
    value = const - 0.5 * gls.logdet_sigma - 0.5 * gls.quad
    if restricted:
        value -= 0.5 * gls.logdet_info
    return -float(value)


def _neg_loglik_grad(
    par: np.ndarray,
    y: np.ndarray,
    S: list[np.ndarray],
    X: list[np.ndarray],
    p: int,
    restricted: bool,
) -> np.ndarray:
    """Analytic gradient of :func:`_neg_loglik` in the Cholesky parameters.

    Without this the optimiser needs ``n_par + 1`` likelihood evaluations
    per step, and ``n_par`` grows as ``p(p+1)/2`` — for a crossbasis with
    16 coefficients that is 136 parameters and 137 evaluations, each one a
    loop over every city. Measured before this existed: 111 s for
    ``p=16, m=12``, and BFGS still stopped short of its tolerance.

    Writing ``Sigma_i = S_i + Psi``, ``W_i = Sigma_i^-1`` and
    ``res_i = y_i - X_i beta``, the derivative of the log-likelihood with
    respect to ``Psi`` is::

        G = 0.5 * sum_i [ -W_i + W_i res_i res_i' W_i
                          (+ W_i X_i A^-1 X_i' W_i   for REML) ]

    ``beta`` contributes nothing at first order because it sits at its
    GLS optimum, where ``sum X_i' W_i res_i = 0``. The extra REML term is
    the derivative of ``log|A|``, the piece that distinguishes restricted
    from ordinary likelihood.

    The chain rule through ``Psi = L L'`` gives ``dL = 2 G L``, taken on
    the lower triangle.
    """
    L = np.zeros((p, p))
    L[np.tril_indices(p)] = par
    psi = L @ L.T
    gls = _gls(y, S, X, psi)
    if not gls.ok:
        return np.zeros_like(par)

    grad_psi = np.zeros((p, p))
    for i in range(len(S)):
        chol = np.linalg.cholesky(S[i] + psi)
        w = _chol_inv(chol)
        res = y[i] - X[i] @ gls.beta
        wr = w @ res
        grad_psi += -w + np.outer(wr, wr)
        if restricted:
            wx = w @ X[i]
            grad_psi += wx @ gls.vcov_beta @ wx.T
    grad_psi *= 0.5

    # d(-logL)/dL = -2 G L, keeping only the free lower-triangular entries.
    return (-2.0 * grad_psi @ L)[np.tril_indices(p)]


# ---------------------------------------------------------------------------
# Public fit
# ---------------------------------------------------------------------------

@dataclass
class MvmetaFit:
    """Result of a multivariate random-effects meta-analysis."""

    coef: np.ndarray
    vcov: np.ndarray
    psi: np.ndarray
    method: str
    converged: bool
    loglik: float
    n_studies: int
    n_outcomes: int
    n_coef: int
    y: np.ndarray = field(repr=False)
    S: list[np.ndarray] = field(repr=False)
    X: list[np.ndarray] = field(repr=False)

    @property
    def n_moderators(self) -> int:
        """Columns of the study-level design, including the intercept."""
        return self.n_coef // self.n_outcomes

    def block(self, moderator: int = 0) -> tuple[np.ndarray, np.ndarray]:
        """Coefficients and covariance for one moderator, across outcomes.

        The coefficient vector is *outcome-major*: outcome 1's full row of
        moderators, then outcome 2's, and so on — exactly how R's
        ``mvmeta`` names them (``y1.(Intercept)``, ``y1.x``,
        ``y2.(Intercept)``, ``y2.x``, ...). So one moderator's effects sit
        at a stride of ``n_moderators``, not in a contiguous leading
        slice.

        Getting that wrong is silent, which is why this is a method rather
        than a slice at each call site: see the R bug recorded as M67,
        where ``sus_mod_metaregression()`` takes the first ``p`` entries as
        the intercept block and so feeds ``crosspred`` a vector that is
        half intercepts and half covariate slopes, interleaved.

        Args:
            moderator: Column index in the study-level design; ``0`` is
                the intercept, which is the pooled effect with every
                covariate at its centring value.

        Returns:
            ``(coef, vcov)`` for that moderator, each of length/shape
            ``n_outcomes``.

        Raises:
            IndexError: If *moderator* is outside the design.
        """
        q = self.n_moderators
        if not 0 <= moderator < q:
            raise IndexError(
                f"moderator {moderator} outside the design (0..{q - 1})."
            )
        idx = np.arange(moderator, self.n_coef, q)
        return self.coef[idx], self.vcov[np.ix_(idx, idx)]

    @property
    def n_par_psi(self) -> int:
        """Free parameters in the unstructured between-study covariance."""
        return 0 if self.method == "fixed" else _n_par(self.n_outcomes)

    @property
    def overparametrised(self) -> bool:
        """Whether ``Psi`` has more free parameters than there are data.

        With ``m`` studies of ``p`` outcomes there are ``m * p`` numbers to
        learn from, while an unstructured ``Psi`` costs ``p(p+1)/2``. Pooling
        a default DLNM cross-basis is exactly where this bites: a 4x3 basis
        gives ``p = 12``, so ``Psi`` wants 78 parameters, and three cities
        supply 36 numbers.

        The consequence is not a failure but something quieter: the
        restricted likelihood goes nearly flat, no optimiser converges, and
        where each one stops is arbitrary. Measured on three synthetic
        cities, this code reached a log-likelihood of 178.7 after 300
        iterations, 180.1 after 3000 and 181.2 after 20000, still climbing;
        R's ``mvmeta`` stopped at 181.3, also reporting non-convergence.
        Both answers are points on a plateau, not estimates.
        """
        return self.n_par_psi > self.n_studies * self.n_outcomes


def mvmeta_fit(
    y: np.ndarray,
    S: list[np.ndarray],
    X: np.ndarray | None = None,
    method: Method = "reml",
    max_iter: int = 300,
) -> MvmetaFit:
    """Fit a multivariate random-effects meta-analysis.

    Args:
        y: ``(m, p)`` array of study estimates, one row per study.
        S: Length-``m`` list of ``(p, p)`` within-study covariances.
        X: ``(m, q)`` study-level design matrix for meta-regression, or
            ``None`` for plain pooling (an intercept-only design).
        method: ``"reml"`` (default), ``"ml"``, or ``"fixed"``. Under
            ``"fixed"`` the between-study covariance is held at zero.
        max_iter: Maximum optimiser iterations.

    Returns:
        A :class:`MvmetaFit`. ``coef`` has length ``q * p``, ordered
        outcome-within-moderator to match ``kron(x_i, I_p)``.

    Raises:
        ValueError: If the shapes disagree or fewer than one study is
            supplied.
    """
    y = np.asarray(y, dtype=float)
    if y.ndim != 2:
        raise ValueError(f"y must be a 2-D (m, p) array, got shape {y.shape}.")
    m, p = y.shape
    if m < 1:
        raise ValueError("At least one study is required.")
    if len(S) != m:
        raise ValueError(f"S has {len(S)} entries but y has {m} studies.")
    for i, s in enumerate(S):
        if np.shape(s) != (p, p):
            raise ValueError(f"S[{i}] has shape {np.shape(s)}, expected ({p}, {p}).")

    S = [np.asarray(s, dtype=float) for s in S]
    xmat = np.ones((m, 1)) if X is None else np.asarray(X, dtype=float)
    if xmat.ndim == 1:
        xmat = xmat[:, None]
    if xmat.shape[0] != m:
        raise ValueError(f"X has {xmat.shape[0]} rows but y has {m} studies.")
    # A rank-deficient design makes the GLS information matrix singular, and
    # the failure is otherwise silent: every coefficient comes back zero, so
    # a caller sees RR = 1 with a [1, 1] interval and nothing announcing
    # that no model was fitted. Two collinear covariates are enough.
    rank = int(np.linalg.matrix_rank(xmat))
    if rank < xmat.shape[1]:
        raise ValueError(
            f"X is rank-deficient: {xmat.shape[1]} columns but rank {rank}. "
            "Some study-level covariate is a linear combination of the "
            "others (or of the intercept), so their separate effects are "
            "not identifiable. Drop the redundant column."
        )
    # No separate "fewer studies than terms" check: rank is bounded by the
    # row count, so m < q always surfaces above as rank deficiency.
    # kron(I_p, x_i'): outcome-major ordering, matching how R's mvmeta names
    # and orders its coefficients ("y1.(Intercept)", "y1.x", "y2.(Intercept)",
    # ...). Getting this backwards is invisible in `coef` for plain pooling —
    # with a single intercept both layouts coincide — but silently permutes
    # `vcov` under meta-regression. Measured against R before the ordering was
    # fixed: coefficients agreed to 6e-08 while vcov was off by 82%.
    design = [np.kron(np.eye(p), xmat[i][None, :]) for i in range(m)]

    if method == "fixed":
        psi = np.zeros((p, p))
        gls = _gls(y, S, design, psi)
        if not gls.ok:
            raise ValueError(
                "Fixed-effects fit failed: a within-study covariance is not "
                "positive definite."
            )
        loglik = -_neg_loglik(np.zeros(_n_par(p)), y, S, design, p, restricted=False)
        return MvmetaFit(
            coef=gls.beta, vcov=gls.vcov_beta, psi=psi, method=method,
            converged=True, loglik=loglik, n_studies=m, n_outcomes=p,
            n_coef=design[0].shape[1], y=y, S=S, X=design,
        )

    if method not in ("reml", "ml"):
        raise ValueError(f"method must be 'reml', 'ml' or 'fixed', got {method!r}.")

    optimize = _require_scipy()
    restricted = method == "reml"
    par0 = _psi_to_par(_initial_psi(y, S, design), p)

    args = (y, S, design, p, restricted)
    result = optimize.minimize(
        _neg_loglik, par0, args=args, jac=_neg_loglik_grad,
        method="L-BFGS-B",
        options={"maxiter": max_iter, "maxfun": max_iter * 4,
                 "ftol": 1e-12, "gtol": 1e-8},
    )
    psi = _par_to_psi(np.asarray(result.x, dtype=float), p)
    gls = _gls(y, S, design, psi)
    if not gls.ok:  # optimiser landed outside the cone; fall back to Psi = 0
        psi = np.zeros((p, p))
        gls = _gls(y, S, design, psi)
    if not gls.ok:
        raise ValueError(
            "The generalised least squares solution is singular even with "
            "Psi = 0. Either a within-study covariance is not positive "
            "definite, or the study-level design does not identify the "
            "coefficients. Returning zeros here would look like a fitted "
            "model with RR = 1 everywhere."
        )

    return MvmetaFit(
        coef=gls.beta, vcov=gls.vcov_beta, psi=psi, method=method,
        converged=bool(result.success), loglik=-float(result.fun),
        n_studies=m, n_outcomes=p, n_coef=design[0].shape[1],
        y=y, S=S, X=design,
    )


def _initial_psi(
    y: np.ndarray,
    S: list[np.ndarray],
    X: list[np.ndarray],
) -> np.ndarray:
    """Method-of-moments starting value for ``Psi``.

    Take the residual scatter around the fixed-effects fit and subtract
    the average within-study covariance; clamp to the PSD cone. A decent
    seed matters because the restricted likelihood is flat near the
    boundary, where a poor start can strand BFGS at ``Psi = 0``.
    """
    p = y.shape[1]
    gls = _gls(y, S, X, np.zeros((p, p)))
    if not gls.ok:
        return np.eye(p) * 1e-4
    res = np.array([y[i] - X[i] @ gls.beta for i in range(len(S))])
    scatter = res.T @ res / max(1, len(S) - X[0].shape[1] // p)
    psi = scatter - np.mean(S, axis=0)
    # Project onto the PSD cone by clipping the eigenvalues.
    vals, vecs = np.linalg.eigh((psi + psi.T) / 2.0)
    vals = np.clip(vals, 0.0, None)
    psi = vecs @ np.diag(vals) @ vecs.T
    if not np.any(vals > 0):
        psi = np.eye(p) * 1e-4
    return psi


# ---------------------------------------------------------------------------
# Heterogeneity and BLUP
# ---------------------------------------------------------------------------

def qtest(fit: MvmetaFit) -> dict[str, float]:
    """Cochran's multivariate Q test for residual heterogeneity.

    ``Q`` is the weighted residual sum of squares under the *fixed*-effects
    fit, so it measures scatter beyond what the within-study covariances
    explain. ``I2`` is the share of total variation attributable to
    heterogeneity, floored at zero.
    """
    from scipy import stats  # noqa: PLC0415

    p, m = fit.n_outcomes, fit.n_studies
    gls = _gls(fit.y, fit.S, fit.X, np.zeros((p, p)))
    q = float(gls.quad)
    df = p * m - fit.n_coef
    if df <= 0:
        return {"Q": q, "df": df, "p_value": float("nan"), "i2": float("nan")}
    p_value = float(stats.chi2.sf(q, df))
    i2 = max(0.0, (q - df) / q * 100.0) if q > 0 else 0.0
    return {"Q": q, "df": df, "p_value": p_value, "i2": i2}


def blup(fit: MvmetaFit, with_vcov: bool = True) -> list[dict[str, np.ndarray]]:
    """Best linear unbiased predictions, one per study.

    Each study's own estimate is shrunk toward the pooled mean in
    proportion to how much of its total variance is within-study noise::

        blup_i = X_i @ beta + Psi @ Sigma_i^-1 @ (y_i - X_i @ beta)

    With ``Psi = 0`` every study collapses onto the pooled estimate; with
    ``Psi`` large relative to ``S_i`` the study keeps its own value.

    The reported covariance follows ``mvmeta``::

        V_i = X_i Var(beta) X_i' + Psi - Psi Sigma_i^-1 Psi

    which adds the uncertainty in the pooled mean to the conditional
    variance of the random effect. Note this is *not* Henderson's
    prediction MSE, which sandwiches the first term as
    ``(I - Psi W_i) X_i Var(beta) X_i' (I - Psi W_i)'`` to account for the
    covariance between the two pieces. ``mvmeta``'s version is the wider,
    more conservative one — measured across seven test cases it runs 15%
    to 38% larger than Henderson's.

    Parity wins here rather than the textbook formula: this is the
    reference implementation behind the published multi-city literature,
    and ``sus_mod_pool()`` feeds these covariances straight into the
    city-specific intervals of ``city_table``. A narrower interval in
    Python than in R for identical data would be a divergence in a
    reported number, not an improvement.
    """
    out: list[dict[str, np.ndarray]] = []
    for i in range(fit.n_studies):
        sigma = fit.S[i] + fit.psi
        try:
            chol = np.linalg.cholesky(sigma)
        except np.linalg.LinAlgError:  # pragma: no cover - degenerate study
            out.append({"blup": fit.X[i] @ fit.coef, "vcov": fit.psi.copy()})
            continue
        mean_i = fit.X[i] @ fit.coef
        shrink = fit.psi @ _chol_inv(chol)          # Psi @ Sigma^-1
        pred = mean_i + shrink @ (fit.y[i] - mean_i)
        entry: dict[str, np.ndarray] = {"blup": pred}
        if with_vcov:
            entry["vcov"] = (
                fit.X[i] @ fit.vcov @ fit.X[i].T + fit.psi - shrink @ fit.psi
            )
        out.append(entry)
    return out
