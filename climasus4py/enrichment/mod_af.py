"""Attributable Fraction and Attributable Number from a DLNM fit.

Mirrors R: sus_mod_af.R

Theory: Gasparrini & Armstrong (2013, Epidemiology); Gasparrini et al.
(2017, Lancet Planet Health).

Input: the dict returned by ``sus_mod_dlnm()`` (Python analog of R's
``climasus_dlnm`` S3 object). Not lazy — operates entirely on the
already-fitted model and its pre-aggregated daily data.

Methodology:
  Point estimates: cumulative-over-lag RR (``_allrr_fit`` from
  ``mod_dlnm.py``) at observed exposure values.
  CI: Monte Carlo simulation drawing ``nsim`` coefficient vectors from
  N(coef_cb, vcov_cb) (``numpy.random.Generator.multivariate_normal``,
  the direct analog of R's ``MASS::mvrnorm``) and re-computing the
  cumulative RR for each draw. ``nsim=0`` uses delta-method bounds from
  the crossbasis prediction instead (matching R's ``MASS``-unavailable
  fallback) — unlike R, this path is never forced by a missing package
  since ``numpy`` (a hard dependency) always provides the MVN sampler.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

from .mod_dlnm import _allrr_fit, _crosspred

console = Console(stderr=True)

_VALID_BY: tuple[str, ...] = ("month", "year", "season")
_DELTA_CI_LEVEL = 0.95  # dlnm::crosspred()'s own default, ignored by R's alpha too.

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_check": "Verificando entradas...",
        "step_total": "Calculando FA total ({nsim} simulacoes MC)...",
        "step_components": "Calculando componentes calor/frio (limiar = {thr})...",
        "step_quantiles": "Calculando FA por faixa de percentil...",
        "step_period": "Calculando FA por {by}...",
        "done": "FA total: {af_pct}% [{lo_pct}%, {hi_pct}%] | NA: {an_val} [{an_lo}, {an_hi}]",
        "err_bad_by": "'by' deve ser None, 'month', 'year' ou 'season'.",
    },
    "en": {
        "step_check": "Checking inputs...",
        "step_total": "Computing total AF ({nsim} MC simulations)...",
        "step_components": "Computing heat/cold components (threshold = {thr})...",
        "step_quantiles": "Computing AF by percentile range...",
        "step_period": "Computing AF by {by}...",
        "done": "Total AF: {af_pct}% [{lo_pct}%, {hi_pct}%] | AN: {an_val} [{an_lo}, {an_hi}]",
        "err_bad_by": "'by' must be None, 'month', 'year', or 'season'.",
    },
    "es": {
        "step_check": "Verificando entradas...",
        "step_total": "Calculando FA total ({nsim} simulaciones MC)...",
        "step_components": "Calculando componentes calor/frio (umbral = {thr})...",
        "step_quantiles": "Calculando FA por rango de percentil...",
        "step_period": "Calculando FA por {by}...",
        "done": "FA total: {af_pct}% [{lo_pct}%, {hi_pct}%] | NA: {an_val} [{an_lo}, {an_hi}]",
        "err_bad_by": "'by' debe ser None, 'month', 'year' o 'season'.",
    },
}


def _component_af(
    x: np.ndarray,
    cases: np.ndarray,
    n_cases: float,
    range_: tuple[float, float] | None,
    rr_obs: np.ndarray,
    rr_obs_lo: np.ndarray,
    rr_obs_hi: np.ndarray,
    mc_draws: np.ndarray | None,
    var_meta: dict[str, Any],
    lag_meta: dict[str, Any],
    lag_max: int,
    cen: float,
    alpha: float,
    lang: str,
) -> dict[str, float]:
    """AF/AN and CI for one exposure range — matches R's ``.saf_component``."""
    if range_ is None:
        in_range = np.ones(len(x), dtype=bool)
    else:
        in_range = (x >= range_[0]) & (x <= range_[1])

    an_i = cases * (1 - 1 / rr_obs)
    an_i = np.where(in_range, an_i, 0.0)
    an_pt = float(np.nansum(an_i))
    af_pt = an_pt / n_cases

    if mc_draws is not None:
        af_sim = np.empty(mc_draws.shape[0])
        for s in range(mc_draws.shape[0]):
            rr_s = _allrr_fit(var_meta, lag_meta, mc_draws[s, :], x, lag_max, cen, lang)
            an_s = cases * (1 - 1 / rr_s)
            an_s = np.where(in_range, an_s, 0.0)
            af_sim[s] = np.nansum(an_s) / n_cases
        af_lo = float(np.nanquantile(af_sim, alpha / 2))
        af_hi = float(np.nanquantile(af_sim, 1 - alpha / 2))
        an_lo = af_lo * n_cases
        an_hi = af_hi * n_cases
    else:
        an_lo_i = np.where(in_range, cases * (1 - 1 / rr_obs_hi), 0.0)
        an_hi_i = np.where(in_range, cases * (1 - 1 / rr_obs_lo), 0.0)
        an_lo_raw = float(np.nansum(an_lo_i))
        an_hi_raw = float(np.nansum(an_hi_i))
        an_lo, an_hi = min(an_lo_raw, an_hi_raw), max(an_lo_raw, an_hi_raw)
        af_lo, af_hi = an_lo / n_cases, an_hi / n_cases

    return {"af": af_pt, "lo": af_lo, "hi": af_hi, "an": an_pt, "an_lo": an_lo, "an_hi": an_hi}


def _by_period(daily: pd.DataFrame, by: str) -> pd.DataFrame:
    """Temporal period breakdown of AN/AF (point estimates) — matches R's ``.saf_by_period``."""
    dt = daily.copy()
    dt["year"] = dt["date"].dt.year
    dt["month_num"] = dt["date"].dt.month
    season_map = {
        12: "DJF", 1: "DJF", 2: "DJF",
        3: "MAM", 4: "MAM", 5: "MAM",
        6: "JJA", 7: "JJA", 8: "JJA",
        9: "SON", 10: "SON", 11: "SON",
    }
    dt["season"] = dt["month_num"].map(season_map)

    group_cols_by_key = {
        "year": ["year"],
        "month": ["year", "month_num"],
        "season": ["year", "season"],
    }
    group_cols = group_cols_by_key[by]

    grouped = dt.groupby(group_cols, as_index=False).agg(cases=("cases", "sum"), an=("an", "sum"))
    grouped["an"] = grouped["an"].round(1)
    grouped["af"] = np.where(grouped["cases"] > 0, grouped["an"] / grouped["cases"], np.nan)
    grouped["af_pct"] = (grouped["af"] * 100).round(2)
    return grouped.sort_values(group_cols).reset_index(drop=True)


def sus_mod_af(
    fit: dict[str, Any],
    threshold: float | None = None,
    range: tuple[float, float] | None = None,
    pred_at: tuple[float, ...] = (0.75, 0.90, 0.95, 0.99),
    by: Literal["month", "year", "season"] | None = None,
    nsim: int = 1000,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Attributable Fraction and Number from a DLNM fit.

    Computes population attributable fractions (AF) and attributable
    numbers (AN) from a ``climasus_dlnm``-shaped dict returned by
    ``sus_mod_dlnm()``. Decomposes the total burden into heat and cold
    components, reports AF at user-defined exposure percentile ranges, and
    optionally breaks the burden down by month, year, or season.

    Args:
        fit: The dict returned by ``sus_mod_dlnm()``.
        threshold: Exposure value splitting heat (above) and cold (below)
            components. ``None`` (default) uses ``fit["meta"]["ref_value"]``.
        range: ``(low, high)`` tuple. When provided, AF is also computed
            for this custom exposure range.
        pred_at: Quantiles defining the percentile-band table. For each
            ``q``, the AF above the ``q``-th quantile (hot) and below the
            ``(1-q)``-th quantile (cold) is reported.
        by: Temporal grouping for the period table: ``"month"``,
            ``"year"``, or ``"season"``. ``None`` skips the breakdown.
        nsim: Number of Monte Carlo simulations for CI. ``0`` uses
            delta-method bounds instead (always at the 95% level, matching
            ``dlnm::crosspred()``'s own default — a quirk preserved from
            R, see IDEIAS.md).
        alpha: Significance level for the Monte Carlo CI.
        lang: Message language.
        verbose: Print progress messages.

    Returns:
        A dict (Python analog of R's ``climasus_af`` S3 object):
        ``total`` (DataFrame, one row per total/heat/cold component),
        ``by_quantile`` (DataFrame), ``by_period`` (DataFrame or ``None``),
        ``daily`` (DataFrame), ``custom`` (DataFrame or ``None``), ``meta``.

    Raises:
        ValueError: If ``by`` is not one of ``"month"``/``"year"``/``"season"``.

    Examples::

        import climasus4py as cs

        fit = cs.sus_mod_dlnm(df, outcome_col="n_obitos", lag_max=14)
        af = cs.sus_mod_af(fit, lang="pt")
        af["total"]
        af["by_quantile"]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msgs = _MESSAGES[lang]

    if by is not None and by not in _VALID_BY:
        raise ValueError(msgs["err_bad_by"])

    if verbose:
        console.print(msgs["step_check"])

    meta = fit["meta"]
    var_meta = meta["var_meta"]
    lag_meta = meta["lag_meta"]
    coef = np.asarray(fit["model"].params[1 : 1 + fit["crossbasis"].shape[1]])
    vcov = np.asarray(fit["model"].cov_params())[
        1 : 1 + fit["crossbasis"].shape[1], 1 : 1 + fit["crossbasis"].shape[1]
    ]
    lag_max = meta["lag_max"]
    cen = threshold if threshold is not None else meta["ref_value"]
    lag0_col = f"{meta['climate_col']}_lag0"
    df_agg = fit["data_daily"]
    x = df_agg[lag0_col].to_numpy(dtype=float)
    cases = df_agg["y"].to_numpy(dtype=float)
    n_cases = float(np.nansum(cases))

    rr_obs = _allrr_fit(var_meta, lag_meta, coef, x, lag_max, cen, lang)
    delta_alpha = 1 - _DELTA_CI_LEVEL
    delta_pred = _crosspred(var_meta, lag_meta, coef, vcov, x, lag_max, cen, delta_alpha, lang)
    rr_obs_lo = delta_pred["allRRlow"]
    rr_obs_hi = delta_pred["allRRhigh"]

    mc_draws = None
    if nsim > 0:
        rng = np.random.default_rng()
        mc_draws = rng.multivariate_normal(coef, vcov, size=nsim)

    if verbose:
        console.print(msgs["step_total"].format(nsim=nsim))

    tot = _component_af(
        x, cases, n_cases, None, rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
        var_meta, lag_meta, lag_max, cen, alpha, lang,
    )

    if verbose:
        console.print(msgs["step_components"].format(thr=round(cen, 2)))

    heat = _component_af(
        x, cases, n_cases, (cen, np.inf), rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
        var_meta, lag_meta, lag_max, cen, alpha, lang,
    )
    cold = _component_af(
        x, cases, n_cases, (-np.inf, cen), rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
        var_meta, lag_meta, lag_max, cen, alpha, lang,
    )

    def _pct(comp: dict[str, float], key: str) -> float:
        return round(comp[key] * 100, 2)

    total_tbl = pd.DataFrame(
        {
            "component": ["total", "heat", "cold"],
            "threshold": [np.nan, cen, cen],
            "af": [tot["af"], heat["af"], cold["af"]],
            "af_lo": [tot["lo"], heat["lo"], cold["lo"]],
            "af_hi": [tot["hi"], heat["hi"], cold["hi"]],
            "af_pct": [_pct(tot, "af"), _pct(heat, "af"), _pct(cold, "af")],
            "af_pct_lo": [_pct(tot, "lo"), _pct(heat, "lo"), _pct(cold, "lo")],
            "af_pct_hi": [_pct(tot, "hi"), _pct(heat, "hi"), _pct(cold, "hi")],
            "an": [tot["an"], heat["an"], cold["an"]],
            "an_lo": [tot["an_lo"], heat["an_lo"], cold["an_lo"]],
            "an_hi": [tot["an_hi"], heat["an_hi"], cold["an_hi"]],
            "n_cases": n_cases,
        }
    )

    if verbose:
        console.print(msgs["step_quantiles"])

    qtl_hot = np.quantile(x, pred_at)
    qtl_cold = np.quantile(x, [1 - q for q in pred_at])
    qtl_rows = []
    for q, qh, qc in zip(pred_at, qtl_hot, qtl_cold, strict=True):
        hot_c = _component_af(
            x, cases, n_cases, (qh, np.inf), rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
            var_meta, lag_meta, lag_max, cen, alpha, lang,
        )
        cld_c = _component_af(
            x, cases, n_cases, (-np.inf, qc), rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
            var_meta, lag_meta, lag_max, cen, alpha, lang,
        )
        qtl_rows.append(
            {
                "component": "hot", "quantile_prob": q,
                "quantile_label": f"Above P{round(q * 100)}",
                "threshold_val": round(float(qh), 3),
                "af": hot_c["af"], "af_lo": hot_c["lo"], "af_hi": hot_c["hi"],
                "af_pct": round(hot_c["af"] * 100, 2),
                "an": hot_c["an"], "an_lo": hot_c["an_lo"], "an_hi": hot_c["an_hi"],
            }
        )
        qtl_rows.append(
            {
                "component": "cold", "quantile_prob": 1 - q,
                "quantile_label": f"Below P{round((1 - q) * 100)}",
                "threshold_val": round(float(qc), 3),
                "af": cld_c["af"], "af_lo": cld_c["lo"], "af_hi": cld_c["hi"],
                "af_pct": round(cld_c["af"] * 100, 2),
                "an": cld_c["an"], "an_lo": cld_c["an_lo"], "an_hi": cld_c["an_hi"],
            }
        )
    by_qtl = pd.DataFrame(qtl_rows)

    custom_tbl = None
    if range is not None:
        cust = _component_af(
            x, cases, n_cases, range, rr_obs, rr_obs_lo, rr_obs_hi, mc_draws,
            var_meta, lag_meta, lag_max, cen, alpha, lang,
        )
        custom_tbl = pd.DataFrame(
            [
                {
                    "range_lo": range[0], "range_hi": range[1],
                    "af": cust["af"], "af_lo": cust["lo"], "af_hi": cust["hi"],
                    "af_pct": round(cust["af"] * 100, 2),
                    "an": cust["an"], "an_lo": cust["an_lo"], "an_hi": cust["an_hi"],
                }
            ]
        )

    an_daily = cases * (1 - 1 / rr_obs)
    af_daily = np.where(cases == 0, np.nan, an_daily / cases)
    daily_tbl = pd.DataFrame(
        {"date": df_agg["date"], "exposure": x, "cases": cases, "an": an_daily, "af": af_daily}
    )

    period_tbl = None
    if by is not None:
        if verbose:
            console.print(msgs["step_period"].format(by=by))
        period_tbl = _by_period(daily_tbl, by)

    if verbose:
        console.print(
            msgs["done"].format(
                af_pct=round(tot["af"] * 100, 2),
                lo_pct=round(tot["lo"] * 100, 2),
                hi_pct=round(tot["hi"] * 100, 2),
                an_val=round(tot["an"]),
                an_lo=round(tot["an_lo"]),
                an_hi=round(tot["an_hi"]),
            )
        )

    return {
        "total": total_tbl,
        "by_quantile": by_qtl,
        "by_period": period_tbl,
        "daily": daily_tbl,
        "custom": custom_tbl,
        "meta": {
            "climate_col": meta["climate_col"],
            "outcome_col": meta["outcome_col"],
            "family": meta["family"],
            "lag_max": lag_max,
            "ref_value": meta["ref_value"],
            "threshold": cen,
            "n_cases": n_cases,
            "n_obs": len(df_agg),
            "pred_at": pred_at,
            "nsim": nsim,
            "alpha": alpha,
            "by": by,
            "ci_method": "monte_carlo" if mc_draws is not None else "delta",
            "call_time": datetime.now(),
        },
    }
