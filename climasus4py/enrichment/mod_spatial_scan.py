"""Kulldorff circular scan statistic for spatial cluster detection.

Mirrors R: sus_mod_spatial_scan.R

NOT YET IMPLEMENTED: this computation is delegated to R's
``SpatialEpi::kulldorff()`` (Kulldorff & Nagarwalla, 1995; Kulldorff,
1997), which has no maintained Python equivalent — ``SaTScan`` is a
separate standalone tool, not an importable library.
"""

from __future__ import annotations

from typing import Any, Literal


def sus_mod_spatial_scan(
    df: Any,
    cases: str,
    population: str,
    municipalities: Any,
    expected: str | None = None,
    max_pop_frac: float = 0.5,
    n_simulations: int = 999,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Kulldorff circular scan statistic for spatial cluster detection.

    R's ``sus_mod_spatial_scan()`` applies the Kulldorff circular spatial
    scan statistic to detect geographic clusters of disease excess,
    using Monte Carlo hypothesis testing for the most-likely cluster and
    any significant secondary clusters. Computations are delegated to
    ``SpatialEpi::kulldorff()``, which has no maintained Python
    equivalent — see module docstring and ``Raises`` below.

    Args:
        df: Table (or ``climasus_df``) with at least a ``code_muni``
            column and the columns named by *cases*, *population*, and
            optionally *expected*.
        cases: Name of the column in *df* with case counts.
        population: Name of the column in *df* with the at-risk
            population denominator.
        municipalities: A ``geopandas.GeoDataFrame`` with POLYGON/
            MULTIPOLYGON geometry and a ``code_muni`` column (the Python
            analog of R's ``sf`` object).
        expected: Name of the column in *df* with the expected number
            of cases under the null model, or ``None`` to let
            ``SpatialEpi::kulldorff()`` compute it internally.
        max_pop_frac: Maximum fraction of the total population a single
            cluster window may contain. Defaults to ``0.5``.
        n_simulations: Number of Monte Carlo simulations for hypothesis
            testing. Defaults to ``999``.
        alpha: Significance level for secondary cluster filtering.
            Defaults to ``0.05``.
        lang: Output language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress messages. Defaults to
            ``True``.

    Returns:
        A dict mirroring R's ``climasus_spatial_scan`` object (never
        reached — this function always raises).

    Raises:
        NotImplementedError: Always. ``SpatialEpi::kulldorff()`` has no
            Python equivalent.
    """
    raise NotImplementedError(
        "sus_mod_spatial_scan() has no faithful Python port: it delegates to "
        "R's SpatialEpi::kulldorff() (Kulldorff circular scan statistic), which "
        "has no maintained Python equivalent (SaTScan is a separate standalone "
        "tool, not an importable library). See IDEIAS.md for details. Flagged "
        "for coordinator review before any approximate implementation is "
        "attempted."
    )
