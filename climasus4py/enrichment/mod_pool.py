"""Two-stage multi-city/multi-region pooling of DLNM estimates.

Mirrors R: sus_mod_pool.R

NOT YET IMPLEMENTED: this model requires the R packages ``dlnm``
(``dlnm::crosspred()``, for the pooled/BLUP prediction) and ``mvmeta``
(``mvmeta::mvmeta()``/``blup()``/``qtest()``, for the multivariate
meta-analysis pooling itself), neither of which has a Python equivalent.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_pool(
    fits: dict[str, Any],
    exposure_range: tuple[float, float] | None = None,
    n_grid: int = 100,
    pred_at: tuple[float, ...] = (0.75, 0.90, 0.95, 0.99),
    blup: bool = True,
    method: Literal["reml", "ml", "fixed"] = "reml",
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Two-stage multi-city pooling of DLNM estimates.

    R's ``sus_mod_pool()`` performs a two-stage multivariate meta-analysis
    to pool Distributed Lag Non-linear Model (DLNM) estimates across
    multiple cities or regions: stage 1 city-specific coefficients and
    variance-covariance matrices (from ``dlnm``-fitted models) are
    combined via ``mvmeta::mvmeta()``, and the pooled coefficients are
    fed back into ``dlnm::crosspred()`` for prediction. There is no
    faithful Python port of this model — see module docstring and
    ``Raises`` below.

    Args:
        fits: A named dict of ``climasus_dlnm``-shaped fits, one per
            city or region. All fits must share ``climate_col``,
            ``argvar``, ``arglag``, and ``lag_max``.
        exposure_range: ``(low, high)`` exposure grid range for the
            pooled prediction, or ``None`` to use the combined range
            across all city datasets.
        n_grid: Number of exposure grid points for the pooled
            prediction curve. Defaults to ``100``.
        pred_at: Quantile probabilities (0-1) for the pooled
            exposure-response summary table. Default ``(0.75, 0.90,
            0.95, 0.99)``.
        blup: Whether to compute BLUP city-specific predictions.
            Defaults to ``True``.
        method: ``mvmeta`` estimation method: ``"reml"`` (default,
            recommended), ``"ml"``, or ``"fixed"`` (no heterogeneity).
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to
            ``True``.

    Returns:
        A dict mirroring R's ``climasus_pool`` object (never reached —
        this function always raises).

    Raises:
        NotImplementedError: Always. Neither ``dlnm`` nor ``mvmeta`` has
            a Python equivalent.
    """
    raise NotImplementedError(
        "sus_mod_pool() has no faithful Python port: it depends on R's "
        "dlnm::crosspred() (for pooled/BLUP prediction) and mvmeta::mvmeta() "
        "(for the multivariate meta-analysis pooling itself) — neither has a "
        "Python equivalent. See IDEIAS.md for details. Flagged for coordinator "
        "review before any approximate implementation is attempted."
    )
