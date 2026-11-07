"""Excess mortality/morbidity estimation for climate-health time series.

Mirrors R: sus_mod_excess.R

Theory:
  Spline baseline: Serfling (1963); Viboud et al. (2004, PNAS);
    Gasparrini et al. (2017, Nat Commun).
  Serfling model: Serfling (1963, Public Health Reports).
  DLNM counterfactual: Gasparrini & Armstrong (2013, Epidemiology).

Input: the dict returned by ``sus_mod_dlnm()`` (for ``method="from_dlnm"``)
or a ``pandas.DataFrame`` with a date column and an outcome count column.
Not lazy — operates on already-materialised daily time series.

Methods:
  ``"from_dlnm"`` — counterfactual baseline from a fitted DLNM: divides
    fitted values by the cumulative RR at the observed exposure to
    recover expected counts at the reference exposure level.
  ``"spline"`` — quasi-Poisson GLM with a natural spline over calendar
    time, fitted on the control period, predicting expected counts for
    the study period.
  ``"serfling"`` — Serfling periodic regression (linear + quadratic time
    trend plus harmonic terms) fit on the control period.

R's ``.sex_baseline_dlnm()`` calls ``dlnm::crosspred(cb, model, at=x,
cen=cen)`` with ``x`` in chronological (unsorted) order — the same
silent-misalignment bug documented for ``sus_mod_af`` in ``mod_af.py``
(``dlnm::crosspred``'s ``at`` argument is internally sorted+deduplicated,
so a positional multiply against a chronological vector pairs each day
with a different day's RR). This port reuses ``mod_dlnm._allrr_fit`` /
``_crosspred``, which are already correctly row-aligned, so the bug is
not reproduced here either — see IDEIAS.md.
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

from .mod_dlnm import _allrr_fit, _crosspred, _onebasis, _require_statsmodels

console = Console(stderr=True)

_VALID_METHODS: tuple[str, ...] = ("from_dlnm", "spline", "serfling")
_VALID_BY: tuple[str, ...] = ("year", "month", "season")
_DLNM_CI_LEVEL = 0.95  # dlnm::crosspred()'s own default, ignored by the user's alpha.

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_validate": "Validando entradas...",
        "step_baseline": "Estimando baseline ({method}) com {n_ctrl} dias de controle...",
        "step_excess": "Calculando excesso para {n_study} dias em estudo...",
        "done": "Concluido. Excesso total: {exc} [{exc_lo}, {exc_hi}] | Pico: {pk} em {pk_date}",
        "err_bad_input": "'data' deve ser um dict de sus_mod_dlnm() ou um pandas.DataFrame.",
        "err_not_dlnm": "Para method='from_dlnm', 'data' deve ser o dict de sus_mod_dlnm().",
        "err_no_date": "Coluna de data '{date_col}' nao encontrada.",
        "err_no_outcome": "Coluna de desfecho '{outcome_col}' nao encontrada. Disponivel: {avail}.",
        "err_no_control": "Nenhum dado no periodo de controle. Verifique 'control_period'.",
        "err_bad_method": "'method' deve ser um de: 'from_dlnm', 'spline', 'serfling'.",
        "err_bad_by": "'by' deve ser None, 'year', 'month' ou 'season'.",
        "err_short_series": "Serie muito curta ({n} dias) para estimar baseline com {dof} gl/ano.",
    },
    "en": {
        "step_validate": "Validating inputs...",
        "step_baseline": "Estimating baseline ({method}) using {n_ctrl} control days...",
        "step_excess": "Computing excess for {n_study} study days...",
        "done": "Done. Total excess: {exc} [{exc_lo}, {exc_hi}] | Peak: {pk} on {pk_date}",
        "err_bad_input": "'data' must be a dict from sus_mod_dlnm() or a pandas.DataFrame.",
        "err_not_dlnm": "For method='from_dlnm', 'data' must be the dict from sus_mod_dlnm().",
        "err_no_date": "Date column '{date_col}' not found.",
        "err_no_outcome": "Outcome column '{outcome_col}' not found. Available: {avail}.",
        "err_no_control": "No data in control period. Check 'control_period'.",
        "err_bad_method": "'method' must be one of: 'from_dlnm', 'spline', 'serfling'.",
        "err_bad_by": "'by' must be None, 'year', 'month', or 'season'.",
        "err_short_series": "Series too short ({n} days) to estimate baseline with {dof} df/year.",
    },
    "es": {
        "step_validate": "Validando entradas...",
        "step_baseline": "Estimando linea base ({method}) con {n_ctrl} dias de control...",
        "step_excess": "Calculando exceso para {n_study} dias en estudio...",
        "done": "Listo. Exceso total: {exc} [{exc_lo}, {exc_hi}] | Pico: {pk} en {pk_date}",
        "err_bad_input": "'data' debe ser un dict de sus_mod_dlnm() o un pandas.DataFrame.",
        "err_not_dlnm": "Para method='from_dlnm', 'data' debe ser el dict de sus_mod_dlnm().",
        "err_no_date": "Columna de fecha '{date_col}' no encontrada.",
        "err_no_outcome": (
            "Columna de resultado '{outcome_col}' no encontrada. Disponibles: {avail}."
        ),
        "err_no_control": "Sin datos en el periodo de control. Verifique 'control_period'.",
        "err_bad_method": "'method' debe ser uno de: 'from_dlnm', 'spline', 'serfling'.",
        "err_bad_by": "'by' debe ser None, 'year', 'month' o 'season'.",
        "err_short_series": (
            "Serie muy corta ({n} dias) para estimar la linea base con {dof} gl/ano."
        ),
    },
}


def _baseline_spline(
    df_ctrl: pd.DataFrame,
    df_stdy: pd.DataFrame,
    ns_df: int,
    family: str,
    alpha: float,
    lang: str,
) -> dict[str, Any]:
    sm = _require_statsmodels()
    t_ref = df_ctrl["date"].min()
    t_num_ctrl = (df_ctrl["date"] - t_ref).dt.days.to_numpy(dtype=float)
    t_num_study = (df_stdy["date"] - t_ref).dt.days.to_numpy(dtype=float)

    basis_ctrl, basis_meta = _onebasis(t_num_ctrl, fun="ns", df=ns_df, intercept=False, lang=lang)
    x_design = np.column_stack([np.ones(len(t_num_ctrl)), basis_ctrl])
    y = df_ctrl["observed"].to_numpy(dtype=float)

    model = sm.GLM(y, x_design, family=sm.families.Poisson())
    result = model.fit(scale="X2" if family == "quasipoisson" else 1.0)

    basis_study, _ = _onebasis(
        t_num_study,
        fun="ns",
        df=ns_df,
        intercept=False,
        lang=lang,
        knots=basis_meta.get("knots"),
        boundary_knots=basis_meta.get("boundary_knots"),
    )
    x_pred = np.column_stack([np.ones(len(t_num_study)), basis_study])
    return _predict_expected(result, x_pred, alpha)


def _baseline_serfling(
    df_ctrl: pd.DataFrame,
    df_stdy: pd.DataFrame,
    harmonics: int,
    family: str,
    alpha: float,
) -> dict[str, Any]:
    sm = _require_statsmodels()
    t_ref = df_ctrl["date"].min()
    t_num_ctrl = (df_ctrl["date"] - t_ref).dt.days.to_numpy(dtype=float)
    t_num_study = (df_stdy["date"] - t_ref).dt.days.to_numpy(dtype=float)

    def make_harmonics(t: np.ndarray) -> np.ndarray:
        cols = [np.ones(len(t)), t, t**2]
        for k in range(1, harmonics + 1):
            cols.append(np.sin(2 * np.pi * k * t / 365.25))
            cols.append(np.cos(2 * np.pi * k * t / 365.25))
        return np.column_stack(cols)

    x_design = make_harmonics(t_num_ctrl)
    y = df_ctrl["observed"].to_numpy(dtype=float)

    model = sm.GLM(y, x_design, family=sm.families.Poisson())
    result = model.fit(scale="X2" if family == "quasipoisson" else 1.0)

    x_pred = make_harmonics(t_num_study)
    return _predict_expected(result, x_pred, alpha)


def _predict_expected(result: Any, x_pred: np.ndarray, alpha: float) -> dict[str, Any]:
    from scipy import stats as sp_stats

    linpred = x_pred @ np.asarray(result.params)
    vcov = np.asarray(result.cov_params())
    se = np.sqrt(np.maximum(0.0, np.sum((x_pred @ vcov) * x_pred, axis=1)))
    z_crit = float(sp_stats.norm.ppf(1 - alpha / 2))
    return {
        "expected": np.exp(linpred),
        "expected_lo": np.exp(linpred - z_crit * se),
        "expected_hi": np.exp(linpred + z_crit * se),
        "model": result,
    }


def _baseline_from_dlnm(
    fit: dict[str, Any], stdy_mask: np.ndarray, lang: str
) -> dict[str, Any]:
    meta = fit["meta"]
    var_meta = meta["var_meta"]
    lag_meta = meta["lag_meta"]
    n_cb = fit["crossbasis"].shape[1]
    coef = np.asarray(fit["model"].params[1 : 1 + n_cb])
    vcov = np.asarray(fit["model"].cov_params())[1 : 1 + n_cb, 1 : 1 + n_cb]
    cen = meta["ref_value"]
    lag_max = meta["lag_max"]

    df_agg = fit["data_daily"]
    x = df_agg[f"{meta['climate_col']}_lag0"].to_numpy(dtype=float)

    rr_obs = _allrr_fit(var_meta, lag_meta, coef, x, lag_max, cen, lang)
    delta_alpha = 1 - _DLNM_CI_LEVEL
    delta_pred = _crosspred(var_meta, lag_meta, coef, vcov, x, lag_max, cen, delta_alpha, lang)
    rr_lo = delta_pred["allRRlow"]
    rr_hi = delta_pred["allRRhigh"]

    fitted_vals = np.asarray(fit["model"].fittedvalues)
    expected = fitted_vals / rr_obs
    expected_lo = fitted_vals / rr_hi
    expected_hi = fitted_vals / rr_lo

    return {
        "expected": expected[stdy_mask],
        "expected_lo": expected_lo[stdy_mask],
        "expected_hi": expected_hi[stdy_mask],
        "model": None,
    }


def _by_period(daily: pd.DataFrame, by: str) -> pd.DataFrame:
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

    grouped = dt.groupby(group_cols, as_index=False).agg(
        n_days=("date", "size"),
        observed=("observed", "sum"),
        expected=("expected", "sum"),
        excess=("excess", "sum"),
        excess_lo=("excess_lo", "sum"),
        excess_hi=("excess_hi", "sum"),
        n_excess_days=("is_excess", "sum"),
    )
    for col in ("expected", "excess", "excess_lo", "excess_hi"):
        grouped[col] = grouped[col].round(1)
    grouped["excess_pct"] = np.where(
        grouped["expected"] > 0,
        (grouped["excess"] / grouped["expected"] * 100).round(2),
        np.nan,
    )
    return grouped.sort_values(group_cols).reset_index(drop=True)


def sus_mod_excess(
    data: dict[str, Any] | pd.DataFrame,
    outcome_col: str | None = None,
    date_col: str = "date",
    control_period: tuple[date_type, date_type] | None = None,
    study_period: tuple[date_type, date_type] | None = None,
    method: Literal["from_dlnm", "spline", "serfling"] | None = None,
    dof_per_year: int = 8,
    harmonics: int = 2,
    family: Literal["quasipoisson", "poisson"] = "quasipoisson",
    threshold_z: float = 1.96,
    by: Literal["year", "month", "season"] | None = None,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Excess mortality/morbidity from a climate-health time series.

    Estimates excess counts (observed minus expected baseline) for a daily
    health-outcome series. Supports three baseline methods: counterfactual
    extraction from a fitted DLNM (``"from_dlnm"``), quasi-Poisson GLM
    with a natural spline over calendar time (``"spline"``), and Serfling
    sinusoidal regression (``"serfling"``). A z-score flag identifies
    statistically significant excess days.

    Args:
        data: Either the dict returned by ``sus_mod_dlnm()`` (required for
            ``method="from_dlnm"``) or a ``pandas.DataFrame`` with a date
            column and an outcome count column.
        outcome_col: Name of the outcome count column. Required when
            ``data`` is a DataFrame; inferred from
            ``data["meta"]["outcome_col"]`` when ``data`` is a DLNM dict.
        date_col: Name of the date column.
        control_period: ``(start, end)`` reference period used to fit the
            baseline model. ``None`` uses all data before ``study_period``,
            or all data when both are ``None``.
        study_period: ``(start, end)`` period for which excess is reported.
            ``None`` uses the full data range.
        method: ``"from_dlnm"``, ``"spline"`` (default for DataFrames), or
            ``"serfling"``. ``None`` picks the default for the input type.
        dof_per_year: Degrees of freedom per year for the spline baseline.
        harmonics: Number of sinusoidal harmonics for the Serfling model.
        family: ``"quasipoisson"`` (default) or ``"poisson"``. Ignored for
            ``method="from_dlnm"``.
        threshold_z: Z-score threshold for flagging excess events.
        by: Temporal breakdown for the period summary: ``"year"``,
            ``"month"``, or ``"season"``. ``None`` skips the breakdown.
        alpha: Significance level for confidence intervals (spline/Serfling
            baselines only — the ``"from_dlnm"`` baseline always reports a
            95% CI regardless of ``alpha``, matching ``dlnm::crosspred()``'s
            own default; a quirk preserved from R — see IDEIAS.md).
        lang: Message language.
        verbose: Print progress messages.

    Returns:
        A dict (Python analog of R's ``climasus_excess`` S3 object):
        ``daily`` (per-day observed/expected/excess + CI + z-score),
        ``total`` (one-row summary), ``by_period`` (or ``None``), ``model``
        (fitted baseline GLM, ``None`` for ``"from_dlnm"``), ``meta``.

    Raises:
        ValueError: On an unsupported ``method``/``by``, missing columns,
            an empty control period, or a control period too short for
            the requested spline complexity.

    Examples::

        import climasus4py as cs

        fit = cs.sus_mod_dlnm(df, outcome_col="n_obitos", lag_max=14)
        exc = cs.sus_mod_excess(fit, method="from_dlnm", lang="en")
        exc["total"]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msgs = _MESSAGES[lang]

    is_dlnm = isinstance(data, dict)
    is_df = isinstance(data, pd.DataFrame)
    if not is_dlnm and not is_df:
        raise ValueError(msgs["err_bad_input"])

    if method is None:
        method = "from_dlnm" if is_dlnm else "spline"
    if method not in _VALID_METHODS:
        raise ValueError(msgs["err_bad_method"])
    if method == "from_dlnm" and not is_dlnm:
        raise ValueError(msgs["err_not_dlnm"])
    if by is not None and by not in _VALID_BY:
        raise ValueError(msgs["err_bad_by"])

    if verbose:
        console.print(msgs["step_validate"])

    if is_dlnm:
        df_ts = data["data_daily"]
        outcome_col = outcome_col or data["meta"]["outcome_col"]
        date_col = "date"
        has_y_fallback = "y" in df_ts.columns and outcome_col not in df_ts.columns
        outcome_col_ts = "y" if has_y_fallback else outcome_col
    else:
        df_ts = data
        outcome_col_ts = outcome_col

    if date_col not in df_ts.columns:
        raise ValueError(msgs["err_no_date"].format(date_col=date_col))
    if outcome_col_ts is None or outcome_col_ts not in df_ts.columns:
        avail = ", ".join(df_ts.columns)
        raise ValueError(
            msgs["err_no_outcome"].format(outcome_col=outcome_col_ts or "None", avail=avail)
        )

    df_ts = df_ts.rename(columns={date_col: "date"}).copy()
    df_ts["date"] = pd.to_datetime(df_ts["date"])
    df_ts["observed"] = df_ts[outcome_col_ts].astype(float)
    df_ts = df_ts.sort_values("date").reset_index(drop=True)

    date_min, date_max = df_ts["date"].min(), df_ts["date"].max()

    if control_period is not None:
        ctrl_from, ctrl_to = pd.Timestamp(control_period[0]), pd.Timestamp(control_period[1])
    elif study_period is not None:
        ctrl_from, ctrl_to = date_min, pd.Timestamp(study_period[0]) - pd.Timedelta(days=1)
    else:
        ctrl_from, ctrl_to = date_min, date_max

    if study_period is not None:
        stdy_from, stdy_to = pd.Timestamp(study_period[0]), pd.Timestamp(study_period[1])
    else:
        stdy_from, stdy_to = date_min, date_max

    ctrl_mask = ((df_ts["date"] >= ctrl_from) & (df_ts["date"] <= ctrl_to)).to_numpy()
    stdy_mask = ((df_ts["date"] >= stdy_from) & (df_ts["date"] <= stdy_to)).to_numpy()
    n_ctrl, n_study = int(ctrl_mask.sum()), int(stdy_mask.sum())

    if n_ctrl == 0:
        raise ValueError(msgs["err_no_control"])

    if method == "from_dlnm":
        baseline_out = _baseline_from_dlnm(data, stdy_mask, lang)
    else:
        n_yrs = (date_max - date_min).days / 365.25
        ns_df = max(dof_per_year, round(dof_per_year * n_yrs))
        if n_ctrl < ns_df * 2:
            raise ValueError(msgs["err_short_series"].format(n=n_ctrl, dof=dof_per_year))

        if verbose:
            console.print(msgs["step_baseline"].format(method=method, n_ctrl=n_ctrl))

        df_ctrl, df_stdy = df_ts[ctrl_mask], df_ts[stdy_mask]
        if method == "spline":
            baseline_out = _baseline_spline(df_ctrl, df_stdy, ns_df, family, alpha, lang)
        else:
            baseline_out = _baseline_serfling(df_ctrl, df_stdy, harmonics, family, alpha)

    if verbose:
        console.print(msgs["step_excess"].format(n_study=n_study))

    daily_tbl = df_ts[stdy_mask][["date", "observed"]].copy().reset_index(drop=True)
    daily_tbl["expected"] = baseline_out["expected"]
    daily_tbl["expected_lo"] = baseline_out["expected_lo"]
    daily_tbl["expected_hi"] = baseline_out["expected_hi"]
    daily_tbl["excess"] = daily_tbl["observed"] - daily_tbl["expected"]
    daily_tbl["excess_lo"] = daily_tbl["observed"] - daily_tbl["expected_hi"]
    daily_tbl["excess_hi"] = daily_tbl["observed"] - daily_tbl["expected_lo"]
    daily_tbl["z_score"] = np.where(
        daily_tbl["expected"] > 0,
        (daily_tbl["observed"] - daily_tbl["expected"]) / np.sqrt(daily_tbl["expected"]),
        np.nan,
    )
    daily_tbl["is_excess"] = daily_tbl["z_score"].notna() & (daily_tbl["z_score"] > threshold_z)
    daily_tbl["cum_excess"] = daily_tbl["excess"].cumsum()

    obs_tot = daily_tbl["observed"].sum()
    exp_tot = daily_tbl["expected"].sum()
    exp_lo_tot = daily_tbl["expected_lo"].sum()
    exp_hi_tot = daily_tbl["expected_hi"].sum()
    exc_tot = obs_tot - exp_tot
    exc_lo = obs_tot - exp_hi_tot
    exc_hi = obs_tot - exp_lo_tot
    exc_pct = round(exc_tot / exp_tot * 100, 2) if exp_tot > 0 else np.nan
    n_events = int(daily_tbl["is_excess"].sum())
    peak_idx = int(daily_tbl["excess"].idxmax())
    peak_exc = round(daily_tbl["excess"].iloc[peak_idx])
    peak_date = daily_tbl["date"].iloc[peak_idx]

    total_tbl = pd.DataFrame(
        [
            {
                "n_days": n_study,
                "observed": obs_tot,
                "expected": round(exp_tot, 1),
                "expected_lo": round(exp_lo_tot, 1),
                "expected_hi": round(exp_hi_tot, 1),
                "excess": round(exc_tot, 1),
                "excess_lo": round(exc_lo, 1),
                "excess_hi": round(exc_hi, 1),
                "excess_pct": exc_pct,
                "n_excess_days": n_events,
                "peak_excess": peak_exc,
                "peak_date": peak_date,
            }
        ]
    )

    period_tbl = _by_period(daily_tbl, by) if by is not None else None

    if verbose:
        console.print(
            msgs["done"].format(
                exc=round(exc_tot), exc_lo=round(exc_lo), exc_hi=round(exc_hi),
                pk=peak_exc, pk_date=peak_date.strftime("%Y-%m-%d"),
            )
        )

    return {
        "daily": daily_tbl,
        "total": total_tbl,
        "by_period": period_tbl,
        "model": baseline_out["model"],
        "meta": {
            "method": method,
            "outcome_col": outcome_col or outcome_col_ts,
            "date_col": date_col,
            "control_period": (ctrl_from, ctrl_to),
            "study_period": (stdy_from, stdy_to),
            "family": "from_dlnm" if method == "from_dlnm" else family,
            "dof_per_year": dof_per_year,
            "harmonics": harmonics,
            "threshold_z": threshold_z,
            "n_obs": len(df_ts),
            "n_study": n_study,
            "n_control": n_ctrl,
            "call_time": datetime.now(),
        },
    }
