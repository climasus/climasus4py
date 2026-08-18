"""Meta-regression of pooled DLNM estimates with city-level covariates.

Mirrors R: sus_mod_metaregression.R

NOT YET IMPLEMENTED: this model requires the R packages ``dlnm``
(``dlnm::crosspred()``, for the pooled/BLUP prediction) and ``mvmeta``
(``mvmeta::mvmeta()``/``blup()``/``qtest()``, for the multivariate
meta-regression itself), neither of which has a Python equivalent.
"""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd


def sus_mod_metaregression(
    fits: dict[str, Any],
    covariates: pd.DataFrame,
    covariate_cols: list[str] | None = None,
    city_col: str | None = None,
    pred_at: tuple[float, ...] = (0.75, 0.90, 0.95, 0.99),
    blup: bool = True,
    method: Literal["reml", "ml", "fixed"] = "reml",
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Meta-regression of pooled DLNM estimates with city-level covariates.

    R's ``sus_mod_metaregression()`` extends two-stage multivariate
    meta-analysis (``sus_mod_pool()``) by including city-level covariates
    (e.g. mean temperature, poverty index) to explain between-city
    heterogeneity in climate-health associations, via
    ``mvmeta::mvmeta()`` fit on cross-basis coefficients extracted from
    ``dlnm``-fitted models, then predicted back through
    ``dlnm::crosspred()``. There is no faithful Python port of this
    model — see module docstring and ``Raises`` below.

    Args:
        fits: A named dict of ``climasus_dlnm``-shaped fits, one per
            city or region. All fits must use the same ``climate_col``,
            ``lag_max``, ``argvar``, and ``arglag``.
        covariates: Table with one row per city and city-level
            predictors, indexed or keyed by city identifier.
        covariate_cols: Column names to use as meta-regression
            predictors, or ``None`` to use all numeric columns.
        city_col: Name of the column in *covariates* holding city
            identifiers, or ``None`` to use the index.
        pred_at: Quantile probabilities (0-1) for the exposure-response
            summary table. Default ``(0.75, 0.90, 0.95, 0.99)``.
        blup: Whether to compute BLUP city-specific predictions.
            Defaults to ``True``.
        method: ``mvmeta`` estimation method: ``"reml"`` (default),
            ``"ml"``, or ``"fixed"``.
        alpha: Significance level for confidence intervals. Defaults to
            ``0.05``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to
            ``True``.

    Returns:
        A dict mirroring R's ``climasus_metaregression`` object (never
        reached — this function always raises).

    Raises:
        NotImplementedError: Always. Neither ``dlnm`` nor ``mvmeta`` has
            a Python equivalent.
    """
    raise NotImplementedError(
        "sus_mod_metaregression() has no faithful Python port: it depends on "
        "R's dlnm::crosspred() (for pooled/BLUP prediction) and mvmeta::mvmeta() "
        "(for the multivariate meta-regression itself) — neither has a Python "
        "equivalent. See IDEIAS.md for details. Flagged for coordinator review "
        "before any approximate implementation is attempted."
    )
