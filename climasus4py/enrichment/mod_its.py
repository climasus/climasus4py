"""mod_its.py — Interrupted Time Series (ITS) analysis for climate-health interventions.

Mirrors R: sus_mod_its.R

Not lazy — fits a segmented (quasi-)Poisson GLM per interruption date, which
is inherently a materialised statsmodels/scipy computation, not a SQL
expression. Accepts a ``DuckDBPyRelation`` or ``pd.DataFrame`` (materialised
with a ``UserWarning`` if a relation is passed) and returns a
:class:`ClimasusITS` result object (Python analogue of R's ``climasus_its``
S3 list) rather than a bare DataFrame, since the R return value bundles a
fitted model plus three result tables — flattening that into one DataFrame
would lose information the R API exposes.

Statistical method: mirrors R's ``stats::glm(family = quasipoisson()/poisson())``
segmented regression exactly — a step (level-change) + "hockey stick"
slope-change term per interruption date, plus optional Fourier harmonics for
seasonality. There is **no autocorrelation correction** in the R source (no
``gls``/``arima``/Prais-Winsten anywhere in ``sus_mod_its.R``) — the ported
Python code intentionally does not add one either, to avoid diverging from
the R statistical model. ``statsmodels.GLM(family=Poisson()).fit(scale="X2")``
reproduces R's ``quasipoisson`` family: point estimates are identical to a
plain Poisson GLM, and standard errors are scaled by the Pearson
chi-square/df dispersion ratio exactly as R's ``summary.glm`` does for
``quasipoisson``.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "climasus4py — ITS",
        "step_validate": "Validando entradas...",
        "step_build": "Construindo dataset ITS ({n_obs} dias, {n_int} intervencao(oes))...",
        "step_fit": "Ajustando modelo segmentado ({family})...",
        "step_cf": "Calculando contrafactual (tendencia pre-intervencao projetada)...",
        "done": "Concluido. Efeito imediato: RR = {rr} [{rr_lo}, {rr_hi}] na intervencao 1",
        "missing_col": "Coluna obrigatória '{col}' não encontrada.",
        "err_no_date": "Coluna de data '{date_col}' não encontrada nos dados.",
        "err_no_outcome": (
            "Coluna de desfecho '{outcome_col}' não encontrada. Disponíveis: {avail}."
        ),
        "err_bad_covariates": "Covariáveis não encontradas nos dados: {bad_cov}.",
        "err_no_interruptions": "'interruption_dates' deve conter pelo menos uma data.",
        "err_int_outside": (
            "Todas as datas de intervenção devem estar dentro do intervalo de "
            "dados ({d_min} a {d_max})."
        ),
        "err_bad_family": "'family' deve ser 'quasipoisson' ou 'poisson'.",
        "warn_short_pre": (
            "Período pré-intervenção curto ({n} dias). Recomenda-se >= 365 dias "
            "para tendência estável."
        ),
        "materialize_warning": (
            "sus_mod_its: a DuckDBPyRelation de entrada está sendo materializada "
            "para o ajuste do GLM com statsmodels — este cálculo não é "
            "expressável em SQL lazy."
        ),
    },
    "en": {
        "title": "climasus4py — ITS",
        "step_validate": "Validating inputs...",
        "step_build": "Building ITS dataset ({n_obs} days, {n_int} interruption(s))...",
        "step_fit": "Fitting segmented model ({family})...",
        "step_cf": "Computing counterfactual (pre-interruption trend projected forward)...",
        "done": "Done. Immediate effect: RR = {rr} [{rr_lo}, {rr_hi}] at interruption 1",
        "missing_col": "Required column '{col}' not found.",
        "err_no_date": "Date column '{date_col}' not found in data.",
        "err_no_outcome": "Outcome column '{outcome_col}' not found. Available: {avail}.",
        "err_bad_covariates": "Covariates not found in data: {bad_cov}.",
        "err_no_interruptions": "'interruption_dates' must contain at least one date.",
        "err_int_outside": (
            "All interruption dates must be within the data range ({d_min} to {d_max})."
        ),
        "err_bad_family": "'family' must be 'quasipoisson' or 'poisson'.",
        "warn_short_pre": (
            "Short pre-interruption period ({n} days). At least 365 days "
            "recommended for a stable trend."
        ),
        "materialize_warning": (
            "sus_mod_its: the input DuckDBPyRelation is being materialised for "
            "the statsmodels GLM fit — this cannot be expressed as lazy SQL."
        ),
    },
    "es": {
        "title": "climasus4py — ITS",
        "step_validate": "Validando entradas...",
        "step_build": "Construyendo dataset ITS ({n_obs} dias, {n_int} interrupcion(es))...",
        "step_fit": "Ajustando modelo segmentado ({family})...",
        "step_cf": "Calculando contrafactual (tendencia pre-intervencion proyectada)...",
        "done": "Listo. Efecto inmediato: RR = {rr} [{rr_lo}, {rr_hi}] en la interrupcion 1",
        "missing_col": "Columna requerida '{col}' no encontrada.",
        "err_no_date": "Columna de fecha '{date_col}' no encontrada en los datos.",
        "err_no_outcome": (
            "Columna de resultado '{outcome_col}' no encontrada. Disponibles: {avail}."
        ),
        "err_bad_covariates": "Covariables no encontradas en los datos: {bad_cov}.",
        "err_no_interruptions": "'interruption_dates' debe contener al menos una fecha.",
        "err_int_outside": (
            "Todas las fechas de interrupción deben estar dentro del rango de "
            "datos ({d_min} a {d_max})."
        ),
        "err_bad_family": "'family' debe ser 'quasipoisson' o 'poisson'.",
        "warn_short_pre": (
            "Período pre-intervención corto ({n} dias). Se recomiendan >= 365 "
            "dias para una tendencia estable."
        ),
        "materialize_warning": (
            "sus_mod_its: la DuckDBPyRelation de entrada se esta materializando "
            "para el ajuste del GLM con statsmodels — no es expresable en SQL lazy."
        ),
    },
}


@dataclass
class ClimasusITS:
    """Result of :func:`sus_mod_its` — Python analogue of R's ``climasus_its``.

    Attributes:
        model: The fitted ``statsmodels`` GLM results object.
        effects: One row per interruption: ``label``, ``interruption_date``,
            ``level_ratio``, ``level_ci_lo``, ``level_ci_hi``, ``level_p``,
            ``slope_daily_log``, ``slope_ratio_annual``, ``slope_p``.
        counterfactual: Daily ``date``, ``observed``, ``predicted``,
            ``counterfactual``, ``cf_lo``, ``cf_hi``, ``ratio_to_cf``,
            ``prevented``. ``None`` when ``counterfactual=False`` was passed.
        segments: One row per segment (pre-interruption and each
            post-interruption interval).
        data: The analysis dataset with all model covariates.
        meta: Analysis parameters and diagnostics.
    """

    model: Any
    effects: pd.DataFrame
    counterfactual: pd.DataFrame | None
    segments: pd.DataFrame
    data: pd.DataFrame
    meta: dict[str, Any] = field(default_factory=dict)

    def tidy(self) -> pd.DataFrame:
        """Return the effects table with analysis metadata columns prepended.

        Mirrors R's ``tidy.climasus_its()``.
        """
        prefix = pd.DataFrame(
            [
                {
                    "outcome_col": self.meta.get("outcome_col"),
                    "family": self.meta.get("family"),
                    "harmonics": self.meta.get("harmonics"),
                    "n_obs": self.meta.get("n_obs"),
                    "n_pre": self.meta.get("n_pre"),
                    "n_interruptions": self.meta.get("n_interruptions"),
                }
            ]
        )
        prefix = pd.concat([prefix] * len(self.effects), ignore_index=True)
        return pd.concat(
            [prefix.reset_index(drop=True), self.effects.reset_index(drop=True)], axis=1
        )

    def __repr__(self) -> str:  # mirrors print.climasus_its (abridged)
        m = self.meta
        lines = [
            "ClimasusITS",
            f"  Outcome       : {m.get('outcome_col')}",
            f"  Family        : {m.get('family')}",
            f"  Harmonics     : {m.get('harmonics')}",
            f"  N obs         : {m.get('n_obs')} ({m.get('n_pre')} pre-interruption)",
            f"  Interruptions : {m.get('n_interruptions')}",
        ]
        if m.get("disp_ratio") is not None and not (
            isinstance(m.get("disp_ratio"), float) and np.isnan(m["disp_ratio"])
        ):
            lines.append(f"  Dispersion    : {m['disp_ratio']}")
        for _, r in self.effects.iterrows():
            lines.append(f"  {r['label']} ({r['interruption_date']:%Y-%m-%d}):")
            lines.append(
                f"    Level change : RR = {r['level_ratio']:.4f} "
                f"[{r['level_ci_lo']:.4f}, {r['level_ci_hi']:.4f}]  p = {r['level_p']:.3g}"
            )
            if pd.notna(r["slope_ratio_annual"]):
                lines.append(
                    f"    Slope change : {r['slope_ratio_annual']:.4f} per year  "
                    f"p = {r['slope_p']:.3g}"
                )
        return "\n".join(lines)


def sus_mod_its(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    outcome_col: str = "n_obitos",
    date_col: str = "date",
    interruption_dates: str | list[str] | None = None,
    harmonics: int = 2,
    family: Literal["quasipoisson", "poisson"] = "quasipoisson",
    covariates: list[str] | None = None,
    alpha: float = 0.05,
    counterfactual: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> ClimasusITS:
    """Fit an interrupted time series (ITS) segmented regression.

    Fits a segmented quasi-Poisson (or Poisson) GLM to estimate the
    immediate and sustained effects of one or more interruptions (policies,
    extreme events, pandemics) on a daily health outcome count series. The
    model decomposes each interruption into a level change (immediate step)
    and a slope change (sustained trend shift), controlling for the
    underlying time trend and seasonal variation via harmonic terms. A
    counterfactual projection shows what would have been expected had the
    interruption not occurred.

    Statistical framework (mirrors R's ``sus_mod_its.R`` exactly): for K
    interruption dates, the model is
    ``log(E[Y_t]) = b0 + b1*t + sum_j(step_j*b_step_j + slope_j*b_slope_j)
    + sum_k(sin/cos harmonic terms) + covariates``, where ``step_j(t) =
    1(t >= T_j)`` and ``slope_j(t) = max(0, t - T_j)``. The immediate rate
    ratio at interruption j is ``exp(b_step_j)``; the sustained trend shift
    is ``exp(365.25 * b_slope_j)`` per year. There is no autocorrelation
    correction (no GLS/AR terms) — this matches the R source, which fits a
    plain ``stats::glm()``.

    The counterfactual is the model prediction with all step/slope terms
    set to zero, i.e. the pre-interruption baseline trend projected forward.

    Args:
        data: A `DuckDBPyRelation` or `pd.DataFrame` with a date column and
            an integer outcome count column. A lazy relation is materialised
            (with a `UserWarning`) since the GLM fit is not expressible in
            SQL.
        outcome_col: Name of the daily health outcome count column.
            Default `"n_obitos"`.
        date_col: Name of the date column. Default `"date"`.
        interruption_dates: One or more interruption dates (`"YYYY-MM-DD"`),
            as a string or list of strings. Must lie strictly within the
            data range. Required (no default in R; `None` here raises the
            same validation error).
        harmonics: Number of sinusoidal harmonic pairs for seasonal control.
            `0` suppresses seasonal terms. Default `2`.
        family: GLM family: `"quasipoisson"` (default, robust to
            overdispersion via Pearson-chi2 dispersion scaling of standard
            errors) or `"poisson"`.
        covariates: Names of additional columns to include as linear
            confounders. Must be numeric or dummy-coded. Default `None`.
        alpha: Significance level for confidence intervals. Default `0.05`.
        counterfactual: Compute and return counterfactual predictions.
            Default `True`.
        lang: Message language: `"pt"` (default), `"en"`, `"es"`.
        verbose: Print progress messages. Default `True`.

    Returns:
        A `ClimasusITS` with `model`, `effects`, `counterfactual`,
        `segments`, `data`, and `meta` attributes (Python analogue of R's
        `climasus_its` list).

    Raises:
        ValueError: If required columns are missing, `family` is invalid,
            `interruption_dates` is missing/empty, or any interruption date
            falls outside the data range.
        ImportError: If `statsmodels` or `scipy` is not installed.

    Examples::

        import climasus4py as cs

        its = cs.sus_mod_its(
            df_daily,
            outcome_col="n_obitos",
            interruption_dates="2020-03-17",
            harmonics=2,
            lang="pt",
        )
        its.effects
        its.tidy()
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    if family not in ("quasipoisson", "poisson"):
        raise ValueError(msg["err_bad_family"])

    if isinstance(data, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        df = data.df()
    else:
        df = data.copy()

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_validate"])

    if date_col not in df.columns:
        raise ValueError(msg["err_no_date"].format(date_col=date_col))
    if outcome_col not in df.columns:
        raise ValueError(
            msg["err_no_outcome"].format(outcome_col=outcome_col, avail=", ".join(df.columns))
        )
    if covariates:
        bad_cov = [c for c in covariates if c not in df.columns]
        if bad_cov:
            raise ValueError(msg["err_bad_covariates"].format(bad_cov=bad_cov))

    if interruption_dates is None or (
        not isinstance(interruption_dates, str) and len(interruption_dates) == 0
    ):
        raise ValueError(msg["err_no_interruptions"])

    if isinstance(interruption_dates, str):
        interruption_dates = [interruption_dates]
    int_dates = sorted(pd.Timestamp(d) for d in interruption_dates)
    n_int = len(int_dates)

    extra = covariates or []
    df2 = df[[date_col, outcome_col, *extra]].rename(
        columns={date_col: "date", outcome_col: "outcome_val"}
    )
    df2["date"] = pd.to_datetime(df2["date"])
    df2["outcome_val"] = pd.to_numeric(df2["outcome_val"])
    df2 = df2.sort_values("date").dropna(subset=["date", "outcome_val"]).reset_index(drop=True)

    d_min, d_max = df2["date"].min(), df2["date"].max()
    if any(d <= d_min or d >= d_max for d in int_dates):
        raise ValueError(
            msg["err_int_outside"].format(
                d_min=d_min.strftime("%Y-%m-%d"), d_max=d_max.strftime("%Y-%m-%d")
            )
        )

    n_pre = int((df2["date"] < int_dates[0]).sum())
    if n_pre < 90:
        warnings.warn(msg["warn_short_pre"].format(n=n_pre), UserWarning, stacklevel=2)

    n_obs = len(df2)
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_build"].format(n_obs=n_obs, n_int=n_int)
        )

    df_model = _its_build_dataset(df2, int_dates, harmonics, extra)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_fit"].format(family=family))

    model, term_cols = _its_fit(df_model, n_int, harmonics, extra, family)

    try:
        from scipy import stats as _stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required to compute the z-critical value for confidence "
            "intervals. Install it with: pip install scipy"
        ) from exc

    z_crit = float(_stats.norm.ppf(1 - alpha / 2))
    effects = _its_extract_effects(model, int_dates, n_int, z_crit)

    cf_tbl = None
    if counterfactual:
        if verbose:
            console.print("[cyan]INFO[/]  " + msg["step_cf"])
        cf_tbl = _its_predict_counterfactual(model, df_model, term_cols, n_int, z_crit)

    seg_tbl = _its_segment_summary(df_model, model, cf_tbl, int_dates)

    # Mirrors R's summary.glm()$dispersion: fixed at 1 for the "poisson"
    # family (no dispersion parameter estimated); the Pearson chi2/df ratio
    # only for "quasipoisson" (which does estimate one).
    if family == "poisson":
        disp_ratio = 1.0
    else:
        try:
            disp_ratio = round(float(model.pearson_chi2 / model.df_resid), 3)
        except (AttributeError, ZeroDivisionError):
            disp_ratio = float("nan")

    if verbose and len(effects) > 0:
        r = effects.iloc[0]
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(
                rr=round(r["level_ratio"], 3),
                rr_lo=round(r["level_ci_lo"], 3),
                rr_hi=round(r["level_ci_hi"], 3),
            )
        )

    meta = {
        "outcome_col": outcome_col,
        "date_col": date_col,
        "interruption_dates": int_dates,
        "n_interruptions": n_int,
        "harmonics": int(harmonics),
        "family": family,
        "covariates": covariates,
        "alpha": alpha,
        "n_obs": n_obs,
        "n_pre": n_pre,
        "disp_ratio": disp_ratio,
        "call_time": datetime.now(),
    }

    return ClimasusITS(
        model=model,
        effects=effects,
        counterfactual=cf_tbl,
        segments=seg_tbl,
        data=df_model,
        meta=meta,
    )


# =============================================================================
# INTERNAL HELPERS (mirror the dot-prefixed R helpers of the same purpose)
# =============================================================================


def _its_build_dataset(
    df2: pd.DataFrame, int_dates: list[pd.Timestamp], harmonics: int, covariate_cols: list[str]
) -> pd.DataFrame:
    """Add time, step/slope, and harmonic terms. Mirrors R's `.its_build_dataset`."""
    df2 = df2.copy()
    t0 = df2["date"].min()
    df2["t_num"] = (df2["date"] - t0).dt.days.astype(float)

    for j, t_j_date in enumerate(int_dates, start=1):
        t_j = float((t_j_date - t0).days)
        df2[f"step_{j}"] = (df2["t_num"] >= t_j).astype(float)
        df2[f"slope_{j}"] = np.maximum(0.0, df2["t_num"] - t_j)

    if harmonics > 0:
        doy = df2["date"].dt.dayofyear.astype(float)
        for k in range(1, harmonics + 1):
            df2[f"sin{k}"] = np.sin(2 * np.pi * k * doy / 365.25)
            df2[f"cos{k}"] = np.cos(2 * np.pi * k * doy / 365.25)

    return df2


def _its_fit(
    df_model: pd.DataFrame,
    n_int: int,
    harmonics: int,
    covariate_cols: list[str],
    family: str,
) -> tuple[Any, list[str]]:
    """Fit the segmented (quasi-)Poisson GLM. Mirrors R's `.its_fit`.

    Uses `statsmodels.GLM` with `family=Poisson()`. R's `quasipoisson`
    produces identical point estimates to `poisson` and only scales the
    standard errors by the Pearson chi2/df dispersion ratio — reproduced
    here via `fit(scale="X2")`, which is statsmodels' documented equivalent.
    ``use_t=True`` mirrors R's ``summary.glm()``, which reports
    ``Pr(>|t|)`` (t-distribution) for families that estimate a dispersion
    parameter (quasipoisson) and ``Pr(>|z|)`` (normal) for families that
    fix it at 1 (poisson) — see R's `.its_extract_effects`, which picks
    the p-value column by name for exactly this reason.
    """
    try:
        import statsmodels.api as sm
    except ImportError as exc:
        raise ImportError(
            "statsmodels is required to fit the ITS segmented GLM. "
            "Install it with: pip install statsmodels"
        ) from exc

    int_terms = [t for j in range(1, n_int + 1) for t in (f"step_{j}", f"slope_{j}")]
    harm_terms = [t for k in range(1, harmonics + 1) for t in (f"sin{k}", f"cos{k}")]
    term_cols = ["t_num", *int_terms, *harm_terms, *covariate_cols]

    exog = sm.add_constant(df_model[term_cols], prepend=True)
    endog = df_model["outcome_val"]

    try:
        model = sm.GLM(endog, exog, family=sm.families.Poisson()).fit(
            scale="X2" if family == "quasipoisson" else 1.0,
            use_t=family == "quasipoisson",
        )
    except Exception as exc:  # pragma: no cover - mirrors R's tryCatch abort
        raise ValueError(f"ITS GLM fitting failed: {exc}") from exc

    return model, term_cols


def _its_extract_effects(
    model: Any, int_dates: list[pd.Timestamp], n_int: int, z_crit: float
) -> pd.DataFrame:
    """Extract per-interruption level/slope effects. Mirrors R's `.its_extract_effects`."""
    params, bse, pvalues = model.params, model.bse, model.pvalues

    rows = []
    for j in range(1, n_int + 1):
        step_nm, slope_nm = f"step_{j}", f"slope_{j}"

        b_step = params.get(step_nm, np.nan)
        se_step = bse.get(step_nm, np.nan)
        p_step = pvalues.get(step_nm, np.nan)

        b_slope = params.get(slope_nm, np.nan)
        p_slope = pvalues.get(slope_nm, np.nan)

        rows.append(
            {
                "label": f"Interruption {j}",
                "interruption_date": int_dates[j - 1],
                "level_ratio": np.exp(b_step),
                "level_ci_lo": np.exp(b_step - z_crit * se_step),
                "level_ci_hi": np.exp(b_step + z_crit * se_step),
                "level_p": p_step,
                "slope_daily_log": b_slope,
                "slope_ratio_annual": np.exp(365.25 * b_slope),
                "slope_p": p_slope,
            }
        )
    return pd.DataFrame(rows)


def _its_predict_counterfactual(
    model: Any, df_model: pd.DataFrame, term_cols: list[str], n_int: int, z_crit: float
) -> pd.DataFrame:
    """Predict fitted and counterfactual (no-intervention) values.

    Mirrors R's `.its_predict_counterfactual`, which uses
    `predict(model, newdata=df_cf, type="link", se.fit=TRUE)`. Here the
    linear predictor and its standard error are computed directly from the
    design matrix and the model's covariance matrix — the same quantity
    `predict.glm(type="link", se.fit=TRUE)` returns.
    """
    exog_cf = df_model[term_cols].copy()
    for j in range(1, n_int + 1):
        exog_cf[f"step_{j}"] = 0.0
        exog_cf[f"slope_{j}"] = 0.0
    exog_cf.insert(0, "const", 1.0)

    fitted_vals = np.asarray(model.fittedvalues, dtype=float)

    exog_mat = exog_cf.to_numpy(dtype=float)
    params = np.asarray(model.params, dtype=float)
    cov = np.asarray(model.cov_params(), dtype=float)

    linpred = exog_mat @ params
    se_linpred = np.sqrt(np.einsum("ij,jk,ik->i", exog_mat, cov, exog_mat))

    cf_vals = np.exp(linpred)
    cf_lo = np.exp(linpred - z_crit * se_linpred)
    cf_hi = np.exp(linpred + z_crit * se_linpred)

    observed = df_model["outcome_val"].to_numpy(dtype=float)
    ratio_to_cf = np.where(cf_vals > 0, observed / cf_vals, np.nan)

    return pd.DataFrame(
        {
            "date": df_model["date"].to_numpy(),
            "observed": observed,
            "predicted": fitted_vals,
            "counterfactual": cf_vals,
            "cf_lo": cf_lo,
            "cf_hi": cf_hi,
            "ratio_to_cf": ratio_to_cf,
            "prevented": cf_vals - observed,
        }
    )


def _its_segment_summary(
    df_model: pd.DataFrame,
    model: Any,
    cf_tbl: pd.DataFrame | None,
    int_dates: list[pd.Timestamp],
) -> pd.DataFrame:
    """Summarise observed/predicted/counterfactual means per segment.

    Mirrors R's `.its_segment_summary`.
    """
    fitted_vals = np.asarray(model.fittedvalues, dtype=float)
    cf_vals = (
        cf_tbl["counterfactual"].to_numpy()
        if cf_tbl is not None
        else np.full(len(df_model), np.nan)
    )

    d_min, d_max = df_model["date"].min(), df_model["date"].max()
    breaks = [d_min, *int_dates, d_max + pd.Timedelta(days=1)]
    seg_labels = [
        "Pre-interruption",
        *(
            ["Post-interruption"]
            if len(int_dates) == 1
            else [f"Post-interruption {i + 1}" for i in range(len(int_dates))]
        ),
    ]

    rows = []
    with warnings.catch_warnings():
        # cf_vals is all-NaN when counterfactual=False; nanmean over an
        # all-NaN slice is expected here (mirrors R's mean(NA, na.rm=TRUE)).
        warnings.filterwarnings("ignore", message="Mean of empty slice")
        for s, label in enumerate(seg_labels):
            s_from = breaks[s]
            s_to = breaks[s + 1] - pd.Timedelta(days=1)
            mask = (df_model["date"] >= s_from) & (df_model["date"] <= s_to)
            rows.append(
                {
                    "segment": label,
                    "start_date": s_from,
                    "end_date": s_to,
                    "n_days": int(mask.sum()),
                    "mean_observed": round(float(df_model.loc[mask, "outcome_val"].mean()), 2),
                    "mean_predicted": round(float(np.nanmean(fitted_vals[mask.to_numpy()])), 2),
                    "mean_counterfactual": round(float(np.nanmean(cf_vals[mask.to_numpy()])), 2),
                }
            )
    return pd.DataFrame(rows)
