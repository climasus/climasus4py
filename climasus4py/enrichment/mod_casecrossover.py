"""mod_casecrossover.py — Time-stratified case-crossover for climate-health data.

Mirrors R: sus_mod_casecrossover.R

Theory: Conditional Poisson (Whitaker et al., 2006; Armstrong et al., 2014);
case-crossover design (Maclure, 1991; Levy et al., 2001).

Not lazy — model fitting (GLM / conditional logistic regression) is
irreducibly row-order-dependent Python/statistics work with no SQL
expression, exactly like ``climate_spi.py``. Accepts a ``DuckDBPyRelation``
or ``pd.DataFrame``; a relation is materialised with a ``UserWarning``.
Returns a :class:`CaseCrossoverResult` (mirrors the R
``climasus_casecrossover`` list: ``model``, ``or_table``, ``data``,
``diagnostics``, ``meta``).

Statistical mapping (see IDEIAS.md for the full reasoning):

- ``method="conditional_poisson"`` (R: ``stats::glm(family = quasipoisson)``
  with ``factor(stratum_id)`` fixed effects) -> ``statsmodels`` GLM with a
  ``C(stratum_id)`` formula term and ``scale="X2"`` (Pearson-chi2 dispersion
  scaling), which is statsmodels' direct equivalent of R's quasipoisson
  variance inflation.
- ``method="clogit"`` (R: ``survival::clogit(..., method = "efron")``) ->
  ``lifelines`` ``CoxPHFitter`` using the classical Breslow/Day (1980)
  data-augmentation trick that R's own ``clogit()`` uses internally: cases
  get duration 1 (event observed), controls get duration 2 (censored),
  stratified by ``stratum_id``, fit with Efron tie-handling (lifelines'
  default, matching R's default). This is not an approximation of
  ``clogit`` — it is the same conditional-likelihood computation R's
  ``survival`` package performs under the hood, run through a different
  Cox implementation.

Both ``statsmodels`` and ``lifelines`` are new, undeclared hard
dependencies for climasus4py (lazy-imported inside the function body with
a friendly ``ImportError``, same pattern as ``scipy`` in ``climate_spi.py``)
— see IDEIAS.md for the coordinator-approval flag.
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
        "title": "climasus4py — Case-Crossover",
        "step_validate": "Validando entradas...",
        "step_data": "Preparando dataset ({n_obs} obs, lag(s): {lags_str}, {n_strata} estratos)...",
        "step_fit": "Ajustando case-crossover ({method})...",
        "done": "Concluído. OR = {or_} [{or_lo}, {or_hi}] (p = {pval})",
        "no_or": "Nenhum coeficiente de exposição extraído.",
        "err_no_date": "Coluna 'date' não encontrada nos dados.",
        "err_no_outcome": (
            "Coluna de desfecho '{outcome_col}' não encontrada. Disponíveis: {avail}."
        ),
        "err_no_exposure": (
            "Coluna de exposição '{exposure_col}' não encontrada. Disponíveis: {avail}."
        ),
        "err_bad_method": "'method' deve ser 'conditional_poisson' ou 'clogit'.",
        "err_bad_stratum": (
            "'stratum' deve ser 'month', 'week', ou o nome de uma coluna existente "
            "('{stratum}')."
        ),
        "err_bad_covariates": "Covariáveis não encontradas nos dados: {bad}.",
        "err_no_cases": "Nenhum caso encontrado ({outcome_col} > 0) após remoção de missings.",
        "err_statsmodels": (
            "O pacote 'statsmodels' é necessário para method='conditional_poisson'. "
            "Instale com: pip install statsmodels"
        ),
        "err_lifelines": (
            "O pacote 'lifelines' é necessário para method='clogit'. "
            "Instale com: pip install lifelines"
        ),
        "warn_lang": "Idioma '{lang}' não suportado. Usando 'pt'.",
        "warn_few_strata": (
            "Apenas {n_strata} estrato(s). Recomenda-se >= 12 estratos para "
            "estimativas estáveis."
        ),
        "warn_clogit_binary": (
            "method='clogit' trata o desfecho como binário (is_case = count > 0). "
            "Para dados de contagem, considere method='conditional_poisson'."
        ),
        "materialize_warning": (
            "sus_mod_casecrossover: a DuckDBPyRelation de entrada está sendo "
            "materializada — o ajuste do modelo (GLM/clogit) não é expressável "
            "em SQL lazy."
        ),
    },
    "en": {
        "title": "climasus4py — Case-Crossover",
        "step_validate": "Validating inputs...",
        "step_data": "Preparing dataset ({n_obs} obs, lag(s): {lags_str}, {n_strata} strata)...",
        "step_fit": "Fitting case-crossover ({method})...",
        "done": "Done. OR = {or_} [{or_lo}, {or_hi}] (p = {pval})",
        "no_or": "No exposure coefficient extracted.",
        "err_no_date": "Column 'date' not found in data.",
        "err_no_outcome": "Outcome column '{outcome_col}' not found. Available: {avail}.",
        "err_no_exposure": "Exposure column '{exposure_col}' not found. Available: {avail}.",
        "err_bad_method": "'method' must be 'conditional_poisson' or 'clogit'.",
        "err_bad_stratum": (
            "'stratum' must be 'month', 'week', or an existing column name ('{stratum}')."
        ),
        "err_bad_covariates": "Covariates not found in data: {bad}.",
        "err_no_cases": "No cases found ({outcome_col} > 0) after removing missing values.",
        "err_statsmodels": (
            "Package 'statsmodels' is required for method='conditional_poisson'. "
            "Install with: pip install statsmodels"
        ),
        "err_lifelines": (
            "Package 'lifelines' is required for method='clogit'. "
            "Install with: pip install lifelines"
        ),
        "warn_lang": "Unsupported language '{lang}'. Using 'pt'.",
        "warn_few_strata": "Only {n_strata} strata. At least 12 recommended for stable estimates.",
        "warn_clogit_binary": (
            "method='clogit' treats outcome as binary (is_case = count > 0). "
            "For count data, consider method='conditional_poisson'."
        ),
        "materialize_warning": (
            "sus_mod_casecrossover: the input DuckDBPyRelation is being "
            "materialised — model fitting (GLM/clogit) cannot be expressed "
            "as lazy SQL."
        ),
    },
    "es": {
        "title": "climasus4py — Case-Crossover",
        "step_validate": "Validando entradas...",
        "step_data": "Preparando dataset ({n_obs} obs, lag(s): {lags_str}, {n_strata} estratos)...",
        "step_fit": "Ajustando case-crossover ({method})...",
        "done": "Listo. OR = {or_} [{or_lo}, {or_hi}] (p = {pval})",
        "no_or": "Ningún coeficiente de exposición extraído.",
        "err_no_date": "Columna 'date' no encontrada en los datos.",
        "err_no_outcome": (
            "Columna de resultado '{outcome_col}' no encontrada. Disponibles: {avail}."
        ),
        "err_no_exposure": (
            "Columna de exposición '{exposure_col}' no encontrada. Disponibles: {avail}."
        ),
        "err_bad_method": "'method' debe ser 'conditional_poisson' o 'clogit'.",
        "err_bad_stratum": (
            "'stratum' debe ser 'month', 'week', o el nombre de una columna existente "
            "('{stratum}')."
        ),
        "err_bad_covariates": "Covariables no encontradas en los datos: {bad}.",
        "err_no_cases": (
            "Ningún caso encontrado ({outcome_col} > 0) tras eliminar valores faltantes."
        ),
        "err_statsmodels": (
            "El paquete 'statsmodels' es necesario para method='conditional_poisson'. "
            "Instale con: pip install statsmodels"
        ),
        "err_lifelines": (
            "El paquete 'lifelines' es necesario para method='clogit'. "
            "Instale con: pip install lifelines"
        ),
        "warn_lang": "Idioma '{lang}' no soportado. Usando 'pt'.",
        "warn_few_strata": (
            "Solo {n_strata} estratos. Se recomiendan >= 12 para estimaciones estables."
        ),
        "warn_clogit_binary": (
            "method='clogit' trata el resultado como binario (is_case = count > 0). "
            "Para datos de conteo, considere method='conditional_poisson'."
        ),
        "materialize_warning": (
            "sus_mod_casecrossover: la DuckDBPyRelation de entrada se está "
            "materializando — el ajuste del modelo (GLM/clogit) no es "
            "expresable en SQL lazy."
        ),
    },
}


@dataclass
class CaseCrossoverResult:
    """Result of :func:`sus_mod_casecrossover` (mirrors R's ``climasus_casecrossover``).

    Attributes:
        model: The fitted model object — a ``statsmodels`` GLM results
            object for ``method="conditional_poisson"``, or a fitted
            ``lifelines.CoxPHFitter`` for ``method="clogit"``.
        or_table: One row per exposure term: ``term``, ``lag_spec``,
            ``estimate`` (log-OR/log-RR), ``or``, ``or_lo``, ``or_hi``,
            ``p_value``. Empty if the exposure term could not be extracted
            (e.g. dropped for collinearity).
        data: The analysis dataset after lag creation and NA removal:
            ``date``, ``outcome_val``, ``exposure_val``, ``stratum_id``,
            plus any covariates.
        diagnostics: ``n_obs``, ``n_cases``, ``n_strata``, ``disp_ratio``
            (for ``"conditional_poisson"``), ``method``, ``family``.
        meta: Analysis parameters: ``outcome_col``, ``exposure_col``,
            ``covariates``, ``stratum``, ``lag``, ``method``, ``family``,
            ``alpha``, ``call_time``.
    """

    model: Any
    or_table: pd.DataFrame
    data: pd.DataFrame
    diagnostics: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        m, dg = self.meta, self.diagnostics
        head = (
            f"<CaseCrossoverResult method={m.get('method')} "
            f"outcome={m.get('outcome_col')} exposure={m.get('exposure_col')} "
            f"n_obs={dg.get('n_obs')} n_cases={dg.get('n_cases')} "
            f"n_strata={dg.get('n_strata')}>"
        )
        if len(self.or_table) > 0:
            r = self.or_table.iloc[0]
            head += f" OR={r['or']:.4f} [{r['or_lo']:.4f}, {r['or_hi']:.4f}] p={r['p_value']:.3g}"
        return head


def sus_mod_casecrossover(
    data: duckdb.DuckDBPyRelation | pd.DataFrame,
    outcome_col: str = "n_obitos",
    exposure_col: str | None = None,
    covariates: list[str] | None = None,
    stratum: str = "month",
    lag: int | list[int] = 0,
    method: Literal["conditional_poisson", "clogit"] = "conditional_poisson",
    family: Literal["quasipoisson", "poisson"] = "quasipoisson",
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> CaseCrossoverResult:
    """Fit a time-stratified case-crossover model for a climate exposure.

    Each time stratum (month or week, by default) acts as its own control,
    removing long-term trends and seasonal confounding by design. Two
    fitting methods are supported: ``"conditional_poisson"`` (GLM with
    stratum fixed effects, default) for aggregate count data, and
    ``"clogit"`` (conditional logistic regression) for binary outcomes or
    rare events.

    Statistical framework:
        ``"conditional_poisson"`` (Whitaker et al., 2006):
        ``log(E[Y_t]) = alpha_s + beta * X_t + z_t' * gamma``, where
        ``alpha_s`` is a stratum-specific intercept, ``X_t`` is the
        (possibly lagged/averaged) exposure, and ``z_t`` are optional
        covariates. Rate ratio ``RR = exp(beta)``.

        ``"clogit"`` (Maclure, 1991; Levy et al., 2001): binary indicator
        ``D_t = 1(Y_t > 0)`` as outcome; each case day is matched to all
        other days within the same stratum as controls.

    Lag specification:
        A single integer (e.g. ``lag=0``) uses the exposure exactly that
        many days before the outcome day. An integer sequence (e.g.
        ``lag=range(7)``) uses the arithmetic mean of exposures at all
        specified lags (a moving average, e.g. "lag 0-6" for temperature).

    Note (preserved from R, see IDEIAS.md): lag shifting is a *positional*
    shift over rows sorted by date, not a calendar-date join — gaps in the
    daily series will silently misalign the lag. This matches the R
    implementation and is not "fixed" here.

    Args:
        data: A ``DuckDBPyRelation`` or ``pd.DataFrame`` with at least a
            ``date`` column, an outcome count column, and an exposure
            column. A lazy relation is materialised (with a
            ``UserWarning``) since model fitting is not expressible in SQL.
        outcome_col: Name of the daily health outcome count column.
            Default ``"n_obitos"``.
        exposure_col: Name of the exposure column. Required; no
            auto-detection is performed.
        covariates: Names of additional columns to include as linear
            confounders. Default ``None``.
        stratum: ``"month"`` (default, year-month strata), ``"week"``
            (ISO year-week strata), or the name of an existing column to
            use as stratum ID.
        lag: Lag(s) to apply to the exposure before fitting. Default ``0``
            (same-day exposure).
        method: ``"conditional_poisson"`` (default, GLM with stratum fixed
            effects via ``statsmodels``) or ``"clogit"`` (conditional
            logistic regression via ``lifelines``, requires the
            ``lifelines`` package). ``"clogit"`` treats the outcome as
            binary (``is_case = count > 0``).
        family: GLM family for ``method="conditional_poisson"``:
            ``"quasipoisson"`` (default) or ``"poisson"``. Ignored for
            ``method="clogit"``. Note (preserved from R): an unrecognized
            value silently falls back to ``"quasipoisson"`` rather than
            raising an error.
        alpha: Significance level for confidence intervals. Default
            ``0.05`` (95% CI).
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A :class:`CaseCrossoverResult` with ``model``, ``or_table``,
        ``data``, ``diagnostics``, ``meta``.

    Raises:
        ValueError: If required columns are missing, ``method``/``stratum``
            is invalid, or no cases are found.
        ImportError: If the required stats package (``statsmodels`` or
            ``lifelines``) is not installed.

    Examples::

        import climasus4py as cs

        cc = cs.sus_mod_casecrossover(
            df_daily,
            outcome_col="n_obitos",
            exposure_col="tair_dry_bulb_c",
            stratum="month",
            lag=0,
        )
        cc.or_table
    """
    if lang not in ("pt", "en", "es"):
        warnings.warn(_MESSAGES["pt"]["warn_lang"].format(lang=lang), UserWarning, stacklevel=2)
        lang = "pt"
    msg = _MESSAGES[lang]

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    if method not in ("conditional_poisson", "clogit"):
        raise ValueError(msg["err_bad_method"])

    if method == "clogit":
        try:
            import lifelines  # noqa: F401
        except ImportError as exc:
            raise ImportError(msg["err_lifelines"]) from exc
    else:
        try:
            import statsmodels.api  # noqa: F401
        except ImportError as exc:
            raise ImportError(msg["err_statsmodels"]) from exc

    if isinstance(data, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        df = data.df()
    else:
        df = data.copy()

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_validate"])

    if "date" not in df.columns:
        raise ValueError(msg["err_no_date"])

    if outcome_col not in df.columns:
        avail = ", ".join(df.columns)
        raise ValueError(msg["err_no_outcome"].format(outcome_col=outcome_col, avail=avail))

    if exposure_col is None or exposure_col not in df.columns:
        avail = ", ".join(df.columns)
        raise ValueError(
            msg["err_no_exposure"].format(
                exposure_col=exposure_col if exposure_col is not None else "(missing)",
                avail=avail,
            )
        )

    if stratum not in ("month", "week") and stratum not in df.columns:
        raise ValueError(msg["err_bad_stratum"].format(stratum=stratum))

    if covariates:
        bad_cov = [c for c in covariates if c not in df.columns]
        if bad_cov:
            raise ValueError(msg["err_bad_covariates"].format(bad=", ".join(bad_cov)))

    lag_list = [int(lag)] if isinstance(lag, int | np.integer) else [int(x) for x in lag]

    if method == "clogit":
        # Unconditional warning (fires regardless of `verbose`) — matches R.
        warnings.warn(msg["warn_clogit_binary"], UserWarning, stacklevel=2)

    df_analysis = _scc_build_dataset(df, outcome_col, exposure_col, covariates, stratum, lag_list)

    n_obs = len(df_analysis)
    n_strata = df_analysis["stratum_id"].nunique()
    n_cases = int((df_analysis["outcome_val"] > 0).sum())

    if n_cases == 0:
        raise ValueError(msg["err_no_cases"].format(outcome_col=outcome_col))

    if n_strata < 12:
        # Unconditional warning — matches R.
        warnings.warn(msg["warn_few_strata"].format(n_strata=n_strata), UserWarning, stacklevel=2)

    lags_str = str(lag_list[0]) if len(lag_list) == 1 else f"{min(lag_list)}-{max(lag_list)}"

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_data"].format(n_obs=n_obs, lags_str=lags_str, n_strata=n_strata)
        )
        console.print("[cyan]INFO[/]  " + msg["step_fit"].format(method=method))

    model = _scc_fit(df_analysis, covariates, method, family)

    from scipy import stats as scipy_stats

    z_crit = float(scipy_stats.norm.ppf(1 - alpha / 2))
    or_tbl = _scc_extract_or(model, method, lag_list, z_crit)

    disp_ratio = np.nan
    if method == "conditional_poisson":
        try:
            disp_ratio = round(float(model.scale), 3)
        except Exception:
            disp_ratio = np.nan

    diagnostics = {
        "n_obs": n_obs,
        "n_cases": n_cases,
        "n_strata": n_strata,
        "disp_ratio": disp_ratio,
        "method": method,
        "family": family if method == "conditional_poisson" else "binomial",
    }

    meta = {
        "outcome_col": outcome_col,
        "exposure_col": exposure_col,
        "covariates": covariates,
        "stratum": stratum,
        "lag": lag_list if len(lag_list) > 1 else lag_list[0],
        "method": method,
        "family": family,
        "alpha": alpha,
        "call_time": datetime.now(),
    }

    if verbose and len(or_tbl) > 0:
        r = or_tbl.iloc[0]
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(
                or_=round(r["or"], 3),
                or_lo=round(r["or_lo"], 3),
                or_hi=round(r["or_hi"], 3),
                pval=r["p_value"],
            )
        )
    elif verbose:
        console.print("[yellow]WARN[/]  " + msg["no_or"])

    return CaseCrossoverResult(
        model=model, or_table=or_tbl, data=df_analysis, diagnostics=diagnostics, meta=meta
    )


def _scc_build_dataset(
    df: pd.DataFrame,
    outcome_col: str,
    exposure_col: str,
    covariates: list[str] | None,
    stratum: str,
    lag_list: list[int],
) -> pd.DataFrame:
    """Build the analysis dataset: lag creation + stratum ID + NA removal.

    Note (preserved from R): the lag is a positional shift over rows
    sorted by ``date`` (not a calendar-date join) — see the public
    function's docstring.
    """
    extra_cols = list(covariates or [])
    if stratum not in ("month", "week"):
        extra_cols = [*extra_cols, stratum]

    d = (
        df[["date", outcome_col, exposure_col, *dict.fromkeys(extra_cols)]]
        .rename(columns={outcome_col: "outcome_val", exposure_col: "exposure_raw"})
        .assign(
            date=lambda x: pd.to_datetime(x["date"]),
            outcome_val=lambda x: pd.to_numeric(x["outcome_val"]),
            exposure_raw=lambda x: pd.to_numeric(x["exposure_raw"]),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )

    x = d["exposure_raw"]
    if len(lag_list) == 1:
        d["exposure_val"] = x.shift(lag_list[0])
    else:
        lag_mat = np.column_stack([x.shift(k).to_numpy() for k in lag_list])
        d["exposure_val"] = np.nanmean(lag_mat, axis=1)
        # nanmean silently drops individual-lag NAs, unlike R's rowMeans(na.rm
        # = FALSE) which propagates any NA to the whole mean. Preserve R's
        # stricter behaviour: any NA among the requested lags -> NA.
        d.loc[np.isnan(lag_mat).any(axis=1), "exposure_val"] = np.nan

    if stratum == "month":
        stratum_id = d["date"].dt.strftime("%Y-%m")
    elif stratum == "week":
        iso = d["date"].dt.isocalendar()
        stratum_id = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    else:
        stratum_id = d[stratum].astype(str)
    d["stratum_id"] = stratum_id

    keep = ["date", "outcome_val", "exposure_val", "stratum_id", *list(covariates or [])]
    return d[keep].dropna().reset_index(drop=True)


def _scc_fit(
    df_analysis: pd.DataFrame,
    covariates: list[str] | None,
    method: Literal["conditional_poisson", "clogit"],
    family: str,
) -> Any:
    """Fit the case-crossover model with the chosen method."""
    cov_terms = " + " + " + ".join(covariates) if covariates else ""

    if method == "conditional_poisson":
        import statsmodels.api as sm
        import statsmodels.formula.api as smf

        # Preserved R quirk: an unrecognized `family` value silently falls
        # back to quasipoisson instead of raising an error.
        use_quasi = family != "poisson"
        glm_family = sm.families.Poisson()
        formula = f"outcome_val ~ exposure_val{cov_terms} + C(stratum_id)"
        try:
            fit_kwargs = {"scale": "X2"} if use_quasi else {}
            return smf.glm(formula, data=df_analysis, family=glm_family).fit(**fit_kwargs)
        except Exception as exc:
            raise ValueError(f"GLM fitting failed: {exc}") from exc

    # clogit -- binary outcome via the Breslow/Day Cox-model trick.
    from lifelines import CoxPHFitter

    cc = df_analysis.copy()
    cc["is_case"] = (cc["outcome_val"] > 0).astype(int)
    cc["_cc_duration"] = 2 - cc["is_case"]

    cols = ["exposure_val", *list(covariates or [])]
    fit_df = cc[[*cols, "_cc_duration", "is_case", "stratum_id"]]
    formula = " + ".join(cols)
    try:
        cph = CoxPHFitter()
        cph.fit(
            fit_df,
            duration_col="_cc_duration",
            event_col="is_case",
            strata=["stratum_id"],
            formula=formula,
        )
        return cph
    except Exception as exc:
        raise ValueError(f"clogit fitting failed: {exc}") from exc


def _scc_extract_or(
    model: Any,
    method: Literal["conditional_poisson", "clogit"],
    lag_list: list[int],
    z_crit: float,
) -> pd.DataFrame:
    """Extract the exposure term's odds/rate ratio + CI from the fitted model."""
    lag_spec = str(lag_list[0]) if len(lag_list) == 1 else f"{min(lag_list)}-{max(lag_list)}"
    empty_cols = ["term", "lag_spec", "estimate", "or", "or_lo", "or_hi", "p_value"]

    if method == "conditional_poisson":
        if "exposure_val" not in model.params.index:
            return pd.DataFrame(columns=empty_cols)
        est = float(model.params["exposure_val"])
        se = float(model.bse["exposure_val"])
        p_val = float(model.pvalues["exposure_val"])
    else:
        summary = model.summary
        if "exposure_val" not in summary.index:
            return pd.DataFrame(columns=empty_cols)
        est = float(summary.loc["exposure_val", "coef"])
        se = float(summary.loc["exposure_val", "se(coef)"])
        p_val = float(summary.loc["exposure_val", "p"])

    return pd.DataFrame(
        {
            "term": ["exposure_val"],
            "lag_spec": [lag_spec],
            "estimate": [est],
            "or": [np.exp(est)],
            "or_lo": [np.exp(est - z_crit * se)],
            "or_hi": [np.exp(est + z_crit * se)],
            "p_value": [p_val],
        }
    )
