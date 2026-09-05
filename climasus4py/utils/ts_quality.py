"""Time-series quality control for daily municipal health counts.

Mirrors R: sus_data_ts_quality.R

Not a lazy pipeline stage: the algorithm needs per-municipality run-length
encoding (temporal gaps) and monthly quantiles (Tukey outlier fences), which
are not expressible as a single relational query. This module therefore
follows the same "materialize at the documented API edge" contract as
``utils/quality.py`` — a lazy ``DuckDBPyRelation`` is collected once via
``.df()``; the return value is a plain ``dict`` report, not a relation.
"""

from __future__ import annotations

import warnings
from itertools import groupby

import duckdb
import numpy as np
import pandas as pd

from ..core.engine import is_relation

__all__ = ["sus_data_ts_quality"]

# ---------------------------------------------------------------------------
# i18n — progress/warning copy only (not domain metadata; see CLAUDE.md §3.2
# and the precedent set by enrichment/climate_fill.py's inline `_msg` dicts).
# ---------------------------------------------------------------------------

_MSG_VALIDATE = {
    "pt": "Validando entradas ({n_mun} municipios, {n_days} obs)...",
    "en": "Validating inputs ({n_mun} municipalities, {n_days} obs)...",
    "es": "Validando entradas ({n_mun} municipios, {n_days} obs)...",
}
_MSG_DONE = {
    "pt": "Concluido. Recomendados para inclusao: {n_inc}/{n_total} municipios ({pct}%).",
    "en": "Done. Recommended for inclusion: {n_inc}/{n_total} municipalities ({pct}%).",
    "es": "Listo. Recomendados para inclusion: {n_inc}/{n_total} municipios ({pct}%).",
}
_WARN_NO_BREAK_TEST = (
    "Structural break test (strucchange::sctest OLS-CUSUM in R) has no "
    "adopted Python port in climasus4py; skipping it. has_break/break_pval "
    "are always None. See IDEIAS.md for the statsmodels follow-up."
)


def _resolve_lang(lang: str) -> str:
    if lang not in ("pt", "en", "es"):
        warnings.warn(f"Unsupported language {lang!r}. Using 'pt'.", UserWarning, stacklevel=3)
        return "pt"
    return lang


def _monthly_outlier_count(sub: pd.DataFrame) -> int:
    """Count months whose summed outcome exceeds the Tukey fence (Q3 + 1.5*IQR)."""
    month_key = sub["_date"].dt.strftime("%Y-%m")
    # R's tapply(..., sum, na.rm=TRUE) sums non-NaN values per group, and
    # returns 0 for an all-NaN group (sum of zero terms) — pandas .sum()
    # with the default skipna=True matches this exactly.
    monthly = sub.groupby(month_key)["_y"].sum()
    q1 = np.percentile(monthly, 25)
    q3 = np.percentile(monthly, 75)
    fence = q3 + 1.5 * (q3 - q1)
    return int((monthly > fence).sum())


def _count_long_gaps(y: pd.Series, max_gap: int) -> int:
    """Count runs of consecutive zero/missing days longer than *max_gap*.

    Mirrors R's ``rle()`` on ``is.na(y) | (y == 0)``.
    """
    in_gap = y.isna() | (y == 0)
    n_long = 0
    for value, run in groupby(in_gap.tolist()):
        if value and len(list(run)) > max_gap:
            n_long += 1
    return n_long


def sus_data_ts_quality(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    outcome_col: str = "n_obitos",
    muni_col: str = "code_muni",
    date_col: str = "date",
    min_completeness: float = 0.90,
    max_gap: int = 7,
    break_alpha: float = 0.05,
    max_outlier_months: int = 3,
    lang: str = "pt",
    verbose: bool = True,
) -> dict:
    """Evaluate the quality of daily health-event time series per municipality.

    Computes, per municipality:

    - **Completeness**: fraction of expected days with non-missing records.
    - **Structural breaks**: always ``None`` in this port — the R OLS-CUSUM
      test (``strucchange::sctest``) has no adopted Python equivalent here.
      See the module warning and ``IDEIAS.md``.
    - **Monthly outliers**: months whose summed outcome exceeds
      Q3 + 1.5 * IQR (Tukey fence) of monthly sums.
    - **Temporal gaps**: runs of consecutive days with zero or missing
      counts longer than *max_gap*.

    A composite inclusion score (0-100) and a binary ``include``
    recommendation are computed from these four criteria.

    Args:
        data: A lazy ``DuckDBPyRelation`` or ``pandas.DataFrame`` with a
            municipality identifier column, a date column, and a daily
            count column. Relations are collected via ``.df()`` once.
        outcome_col: Name of the daily count column. Default: ``"n_obitos"``.
        muni_col: Name of the municipality identifier column.
            Default: ``"code_muni"``.
        date_col: Name of the date column. Default: ``"date"``.
        min_completeness: Minimum completeness (0-1) to recommend inclusion.
            Default: ``0.90``.
        max_gap: Maximum tolerated consecutive zero/missing days before
            flagging a temporal gap. Default: ``7``.
        break_alpha: Significance level for the structural break test.
            Currently unused — kept for signature parity with R. Default: ``0.05``.
        max_outlier_months: Maximum number of outlier months allowed before
            exclusion. Default: ``3``.
        lang: Language for progress/warning messages: ``"pt"``, ``"en"``,
            ``"es"``. Default: ``"pt"``.
        verbose: Print progress messages. Default: ``True``.

    Returns:
        Dictionary with:

        - `flags` (``pd.DataFrame``): one row per municipality with columns
          *muni_col*, ``n_obs``, ``n_expected``, ``completeness``,
          ``has_break``, ``break_pval``, ``n_outlier_months``, ``n_gaps``,
          ``score``, ``include``.
        - `recommend_include` (``list``): municipality codes recommended
          for inclusion.
        - `recommend_exclude` (``dict``): municipality code -> PT-BR reason
          string (the R source hardcodes exclusion reasons in Portuguese
          regardless of *lang* — preserved here, see ``IDEIAS.md``).
        - `params` (``dict``): QC parameters used.

    Raises:
        ValueError: If *muni_col*, *date_col*, or *outcome_col* is not
            found in *data*.

    Example:
        >>> import climasus4py as cs
        >>> qc = cs.sus_data_ts_quality(df_agg, outcome_col="n_obitos")
        >>> qc["flags"].columns.tolist()[:3]
        ['code_muni', 'n_obs', 'n_expected']
    """
    lang = _resolve_lang(lang)

    if verbose:
        print("climasus4py -- TS Quality")

    if is_relation(data):
        df = data.df()
        columns = list(data.columns)
    else:
        df = data
        columns = list(data.columns)

    missing = [c for c in (muni_col, date_col, outcome_col) if c not in columns]
    if missing:
        raise ValueError(
            f"Required column(s) not found: {missing}. "
            f"Available columns: {sorted(columns)}"
        )

    work = pd.DataFrame({
        "_muni": df[muni_col].astype(str),
        "_date": pd.to_datetime(df[date_col]),
        "_y": pd.to_numeric(df[outcome_col], errors="coerce"),
    })

    munis = work["_muni"].unique().tolist()
    n_mun = len(munis)
    n_days = len(work)

    if verbose:
        print(_MSG_VALIDATE[lang].format(n_mun=n_mun, n_days=n_days))

    # strucchange has no adopted Python port in climasus4py (flagged, not
    # added as a new dependency) — structural break is always skipped.
    warnings.warn(_WARN_NO_BREAK_TEST, UserWarning, stacklevel=2)

    rows = []
    for m in munis:
        sub = work[work["_muni"] == m].sort_values("_date")

        d_min, d_max = sub["_date"].min(), sub["_date"].max()
        n_expected = int((d_max - d_min).days) + 1
        n_obs = len(sub)
        completeness = n_obs / n_expected

        has_break = None
        break_pval = None

        n_outlier = _monthly_outlier_count(sub)
        n_long_gaps = _count_long_gaps(sub["_y"], max_gap)

        score = 100
        if pd.notna(completeness) and completeness < min_completeness:
            score -= int(round((1 - completeness) * 60))
        if has_break is True:
            score -= 25
        if pd.notna(n_outlier) and n_outlier > max_outlier_months:
            score -= 10
        if n_long_gaps > 0:
            score -= 5 * min(n_long_gaps, 2)
        score = max(score, 0)

        include = (
            completeness >= min_completeness
            and has_break is not True
            and n_outlier <= max_outlier_months
            and n_long_gaps == 0
        )

        rows.append({
            muni_col: m,
            "n_obs": n_obs,
            "n_expected": n_expected,
            "completeness": round(completeness, 4),
            "has_break": has_break,
            "break_pval": break_pval,
            "n_outlier_months": n_outlier,
            "n_gaps": n_long_gaps,
            "score": score,
            "include": include,
        })

    flags = pd.DataFrame(rows)

    n_inc = int(flags["include"].sum())
    n_total = len(flags)
    pct = round(100 * n_inc / n_total, 1) if n_total else 0.0

    if verbose:
        print(_MSG_DONE[lang].format(n_inc=n_inc, n_total=n_total, pct=pct))

    recommend_include = flags.loc[flags["include"], muni_col].tolist()

    recommend_exclude: dict = {}
    for _, r in flags.loc[~flags["include"]].iterrows():
        why = []
        if r["completeness"] < min_completeness:
            why.append(f"completude={round(r['completeness'] * 100)}%")
        if r["has_break"] is True:
            why.append(f"quebra estrutural (p={round(r['break_pval'], 3)})")
        if r["n_outlier_months"] > max_outlier_months:
            why.append(f"outliers_mensais={r['n_outlier_months']}")
        if r["n_gaps"] > 0:
            why.append(f"lacunas>{max_gap}d={r['n_gaps']}")
        recommend_exclude[r[muni_col]] = "; ".join(why)

    return {
        "flags": flags,
        "recommend_include": recommend_include,
        "recommend_exclude": recommend_exclude,
        "params": {
            "outcome_col": outcome_col,
            "muni_col": muni_col,
            "date_col": date_col,
            "min_completeness": min_completeness,
            "max_gap": max_gap,
            "break_alpha": break_alpha,
            "max_outlier_months": max_outlier_months,
        },
    }
