"""Structured Gaussian priors for the spatiotemporal model, PyMC-side.

Every structured term INLA offers here — ICAR (``besag``), RW1, RW2, and the
Kronecker products behind Knorr-Held interaction types II to IV — is the
same object: a Gaussian prior whose precision matrix ``R`` is **singular**.
The null space is what carries the identifiability constraint (an ICAR is
defined up to a constant; an RW2 up to a line).

Rather than four hand-rolled constructions, there is one: eigendecompose
``R``, drop the null space, and put independent standard normals on the
surviving coefficients. That buys three things at once.

1. The constraint is exact rather than penalised. INLA imposes it with a
   hard linear constraint; PyMC's ``ICAR`` does it with a soft sum-to-zero
   penalty. Dropping the null vectors is the exact version.
2. The parameterisation is **non-centred** by construction, which is not a
   stylistic point — writing the BYM in centred form let ``tau2`` drift to
   103 against 0.053, moved the covariate from 0.132 to 0.159, and reported
   **zero divergences** while doing it. Only the comparison against R
   caught it.
3. ``scale.model=TRUE``, which R passes on every structured term, becomes a
   division: scaling ``R`` so the generalised variance is 1 is scaling the
   eigenvalues.
"""

from __future__ import annotations

import numpy as np

__all__ = ["structure_icar", "structure_rw", "scale_structure",
           "null_rank", "basis"]

# Eigenvalues below this fraction of the largest are treated as the null
# space. The gap is huge for these matrices (exact zeros up to rounding),
# so the threshold is not a tuning knob.
_TOL = 1e-10


def structure_icar(W: np.ndarray) -> np.ndarray:
    """ICAR / Besag structure matrix ``R = D - W``.

    Rank ``n - 1`` for a connected graph: the null space is the constant
    vector, which is the sum-to-zero constraint.
    """
    return np.diag(W.sum(axis=1)) - W


def structure_rw(n: int, order: int) -> np.ndarray:
    """RW1 (``order=1``) or RW2 (``order=2``) structure matrix.

    Built as ``Dif' Dif`` from the difference operator, which is how the
    random walk's precision is defined. Rank ``n - order``: RW1 is
    invariant to a shift, RW2 to a shift and a slope.
    """
    if n <= order:
        raise ValueError(
            f"a random walk of order {order} needs more than {order} time "
            f"points; got {n}."
        )
    dif = np.zeros((n - order, n))
    coef = {1: (-1.0, 1.0), 2: (1.0, -2.0, 1.0)}[order]
    for i in range(n - order):
        dif[i, i:i + order + 1] = coef
    return dif.T @ dif


def null_rank(R: np.ndarray) -> int:
    """Dimension of the null space of a structure matrix."""
    vals = np.linalg.eigvalsh(R)
    return int(np.sum(vals <= _TOL * max(vals.max(), 1.0)))


def scale_structure(R: np.ndarray) -> np.ndarray:
    """Scale ``R`` to unit generalised variance — INLA's ``scale.model=TRUE``.

    The generalised variance is the geometric mean of the diagonal of the
    generalised inverse. Scaling ``R`` by that factor makes the term's
    variance parameter mean the same thing regardless of graph size or
    number of time points, which is the whole reason INLA offers the option
    and the reason a PC prior on it is interpretable at all.
    """
    vals, vecs = np.linalg.eigh(R)
    corte = _TOL * max(vals.max(), 1.0)
    manter = vals > corte
    # diag of the generalised inverse, without forming the matrix
    diag_inv = ((vecs[:, manter] ** 2) / vals[manter]).sum(axis=1)
    fator = float(np.exp(np.mean(np.log(diag_inv))))
    # MULTIPLY. R' = c R gives R'^- = R^-/c, so to send the geometric mean
    # of diag(R^-) to 1 the factor goes on R, not under it. Dividing left
    # the ICAR at 0.324 and the RW1 at 1.424 - caught by asserting the
    # invariant instead of trusting the algebra.
    return R * fator


def basis(R: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Non-null eigenbasis of ``R``: ``(vectors, eigenvalues)``.

    A draw is then ``vectors @ (z / sqrt(eigenvalues))`` with ``z``
    standard normal — a Gaussian with precision ``R`` restricted to the
    complement of the null space, with the constraint exact rather than
    penalised.
    """
    vals, vecs = np.linalg.eigh(R)
    corte = _TOL * max(vals.max(), 1.0)
    manter = vals > corte
    return vecs[:, manter], vals[manter]
