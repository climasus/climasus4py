"""Posterior exceedance probabilities from a spatio-temporal Bayesian fit.

Mirrors R: sus_mod_spacetime_exceedance.R

Answers, for every municipality-by-period cell, "what is the probability
that the relative risk is above *t*?" - the quantity a surveillance team
actually acts on, as opposed to the point estimate.

This function does **not** depend on INLA. It reads the ``rr`` table of
an already fitted object, so it is portable today even though
``sus_mod_spacetime_bayes()`` is still a stub in this package. The shape
it expects is documented under *Args*.

Method:
    The fit supplies, per cell, a posterior mean relative risk and a 95%
    credible interval. R reconstructs a **log-normal** posterior from
    those three numbers - taking the interval as symmetric on the log
    scale, so ``sigma = (log(upper) - log(lower)) / (2 * 1.96))`` - and
    reads the exceedance probability off that normal. It is an
    approximation to the posterior, not a draw from it: the fitted
    object carries summaries rather than samples.
"""

from __future__ import annotations

from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console
from scipy.stats import norm

console = Console(stderr=True)

#: Columns the fit's ``rr`` table must carry.
REQUIRED_RR_COLS: tuple[str, ...] = (
    "code_muni", "time_idx", "rr_mean", "rr_lower95", "rr_upper95",
)

#: R reports a cell as exceeding when the probability clears this.
EXCEEDANCE_REPORT_CUTOFF: float = 0.8

#: Half-width of a 95% interval in standard deviations, as R spells it.
_Z95: float = 1.96

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_validate": "Validando objeto de entrada...",
        "step_aggregate": "Agregando dimensao temporal por {period}...",
        "step_compute": (
            "Calculando P(RR > limiar) para {n_thresh} limiar(es) em "
            "{n_cells} celulas..."
        ),
        "step_summarise": "Sumarizando contagens de exceedencia por limiar...",
        "step_spatial_join": "Adicionando geometria espacial ao resultado...",
        "done": (
            "Concluido. {n_cells} celulas avaliadas | {n_thresh} limiares | "
            "{pct_exceed}% das celulas excedem RR > 1 (P > 0.80)"
        ),
        "err_not_spacetime": (
            "'fit' deve ser a saida de sus_mod_spacetime_bayes()."
        ),
        "err_no_rr": (
            "O objeto 'fit' nao contem 'rr'. Verifique se o modelo foi "
            "ajustado corretamente."
        ),
        "err_rr_cols": (
            "'fit[\"rr\"]' deve conter as colunas: {required}. "
            "Colunas encontradas: {found}."
        ),
        "err_bad_thresholds": (
            "'thresholds' deve ser uma sequencia numerica positiva."
        ),
        "err_bad_aggregate": (
            "'aggregate_time' deve ser 'year' ou 'month', ou None para sem "
            "agregacao."
        ),
        "err_no_sf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "err_no_time_col": (
            "'rr' contem 'time_idx' mas a agregacao por {period} requer um "
            "formato de data valido (YYYY-MM-DD ou YYYY-MM)."
        ),
        "warn_na_rr": (
            "{n_na} celulas com RR nulo ou zero removidas antes do calculo."
        ),
        "warn_sigma_zero": (
            "{n_zero} celulas com intervalo de credibilidade zero. "
            "P(RR > t) sera 0 ou 1 para essas celulas."
        ),
    },
    "en": {
        "step_validate": "Validating input object...",
        "step_aggregate": "Aggregating temporal dimension by {period}...",
        "step_compute": (
            "Computing P(RR > threshold) for {n_thresh} threshold(s) across "
            "{n_cells} cells..."
        ),
        "step_summarise": "Summarising exceedance counts per threshold...",
        "step_spatial_join": "Adding spatial geometry to the result...",
        "done": (
            "Done. {n_cells} cells evaluated | {n_thresh} thresholds | "
            "{pct_exceed}% of cells exceed RR > 1 (P > 0.80)"
        ),
        "err_not_spacetime": (
            "'fit' must be the output of sus_mod_spacetime_bayes()."
        ),
        "err_no_rr": (
            "The 'fit' object does not contain 'rr'. Check that the model was "
            "fitted correctly."
        ),
        "err_rr_cols": (
            "'fit[\"rr\"]' must contain columns: {required}. "
            "Columns found: {found}."
        ),
        "err_bad_thresholds": "'thresholds' must be a positive numeric sequence.",
        "err_bad_aggregate": (
            "'aggregate_time' must be 'year' or 'month', or None for no "
            "aggregation."
        ),
        "err_no_sf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "err_no_time_col": (
            "'rr' contains 'time_idx' but aggregation by {period} requires a "
            "valid date format (YYYY-MM-DD or YYYY-MM)."
        ),
        "warn_na_rr": (
            "{n_na} cells with NA or zero RR removed before exceedance "
            "computation."
        ),
        "warn_sigma_zero": (
            "{n_zero} cells with zero credible interval. P(RR > t) will be "
            "0 or 1 for those cells."
        ),
    },
    "es": {
        "step_validate": "Validando objeto de entrada...",
        "step_aggregate": "Agregando dimension temporal por {period}...",
        "step_compute": (
            "Calculando P(RR > umbral) para {n_thresh} umbral(es) en "
            "{n_cells} celdas..."
        ),
        "step_summarise": "Resumiendo conteos de excedencia por umbral...",
        "step_spatial_join": "Agregando geometria espacial al resultado...",
        "done": (
            "Listo. {n_cells} celdas evaluadas | {n_thresh} umbrales | "
            "{pct_exceed}% de celdas superan RR > 1 (P > 0.80)"
        ),
        "err_not_spacetime": (
            "'fit' debe ser la salida de sus_mod_spacetime_bayes()."
        ),
        "err_no_rr": (
            "El objeto 'fit' no contiene 'rr'. Verifique que el modelo fue "
            "ajustado correctamente."
        ),
        "err_rr_cols": (
            "'fit[\"rr\"]' debe contener las columnas: {required}. "
            "Columnas encontradas: {found}."
        ),
        "err_bad_thresholds": (
            "'thresholds' debe ser una secuencia numerica positiva."
        ),
        "err_bad_aggregate": (
            "'aggregate_time' debe ser 'year' o 'month', o None para sin "
            "agregacion."
        ),
        "err_no_sf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "err_no_time_col": (
            "'rr' contiene 'time_idx' pero la agregacion por {period} requiere "
            "un formato de fecha valido (YYYY-MM-DD o YYYY-MM)."
        ),
        "warn_na_rr": (
            "{n_na} celdas con RR NA o cero eliminadas antes del calculo."
        ),
        "warn_sigma_zero": (
            "{n_zero} celdas con intervalo de credibilidad cero. P(RR > t) "
            "sera 0 o 1 para esas celdas."
        ),
    },
}


def _msg(key: str, lang: str, **kwargs: Any) -> str:
    entry = _MESSAGES.get(lang, _MESSAGES["pt"])
    return entry.get(key, _MESSAGES["pt"].get(key, key)).format(**kwargs)


def _threshold_suffix(value: float) -> str:
    """Column suffix for a threshold, matching R's ``as.character()``.

    R writes ``1`` not ``1.0``, then swaps the decimal point for an
    underscore, so the columns come out ``p_gt_1``, ``p_gt_1_5``.
    """
    return f"{value:.15g}".replace(".", "_").replace("-", "neg")


def _sigma_log(lower: np.ndarray, upper: np.ndarray) -> np.ndarray:
    """Log-scale SD implied by a 95% interval, floored at zero as in R."""
    with np.errstate(divide="ignore", invalid="ignore"):
        sigma = (np.log(upper) - np.log(lower)) / (2 * _Z95)
    return np.where(np.isfinite(sigma) & (sigma > 0), sigma, 0.0)


def _time_period(values: pd.Series, period: str, lang: str) -> pd.Series:
    """Period label for each cell, as R's date formatting produces it.

    A **numeric** ``time_idx`` is read by R as a calendar year: it builds
    ``as.Date(paste0(as.integer(tv), "-01-01"))`` and formats that. The
    year is therefore whatever integer the column holds -- see the note
    on **M90** in :func:`sus_mod_spacetime_exceedance`.

    The numeric branch is formatted arithmetically rather than through a
    timestamp because R's dates reach year 1 and pandas' do not (the
    ``datetime64[ns]`` floor is 1677), and year 1 is exactly what a
    1-based index produces.
    """
    if pd.api.types.is_numeric_dtype(values):
        years = values.astype(int)
        suffix = "" if period == "year" else "-01"
        return years.map(lambda y: f"{y:04d}{suffix}")

    text = values.astype(str)
    parsed = pd.to_datetime(text, format="%Y-%m-%d", errors="coerce")
    blank = parsed.isna()
    if blank.any():
        parsed.loc[blank] = pd.to_datetime(
            text[blank] + "-01", format="%Y-%m-%d", errors="coerce"
        )
    if parsed.isna().all():
        raise ValueError(_msg("err_no_time_col", lang, period=period))
    return parsed.dt.strftime("%Y" if period == "year" else "%Y-%m")


def sus_mod_spacetime_exceedance(
    fit: dict[str, Any],
    thresholds: Any = (1.0, 1.5, 2.0),
    aggregate_time: Literal["year", "month"] | None = None,
    municipalities: Any = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Posterior probability that the relative risk exceeds each threshold.

    For every municipality-by-period cell of a fitted spatio-temporal
    model, computes ``P(RR > t)`` for each *t* in *thresholds*, from the
    log-normal posterior implied by the cell's mean and 95% credible
    interval (see the module docstring).

    Cells with a degenerate interval (``rr_lower95 == rr_upper95``) have
    zero implied spread; R falls back to the hard comparison
    ``rr_mean > t``, giving exactly 0 or 1, and warns. Cells with a
    missing or non-positive mean are dropped first, also with a warning.

    Temporal aggregation:
        With *aggregate_time*, cells are pooled within each municipality
        and period: the relative risk is the **geometric** mean (the
        arithmetic mean of ``log(rr_mean)``, exponentiated), and the
        log-scale spread is the **arithmetic mean of the per-cell
        spreads**.

        Note what that second choice implies: averaging *n* periods does
        not narrow the interval the way averaging independent estimates
        would. The pooled figure keeps the typical single-period
        uncertainty, so exceedance probabilities after aggregating stay
        nearer 0.5 than an independence assumption would put them. For
        a model with a random-walk temporal term the cells are strongly
        correlated, so some of that conservatism is warranted; the
        amount is not derived. Replicated from R as-is.

    A numeric ``time_idx`` is read as a calendar year (M90):
        R builds the period label with
        ``as.Date(paste0(as.integer(time_idx), "-01-01"))`` - it treats
        the column as a **year**. But ``sus_mod_spacetime_bayes()``
        writes a **1-based index** there (1, 2, 3, ...), not the original
        temporal label. The two functions disagree about what the column
        means, and nothing reconciles them.

        Measured on a real INLA fit (42 municipalities x 8 years,
        2015-2022): the labels come out ``"0001"`` through ``"0008"``
        instead of ``"2015"`` through ``"2022"``, and **the aggregation
        aggregates nothing** - each index is already unique within a
        municipality, so 336 cells go in and 336 come out. With a fit
        from ``sus_mod_spacetime_bayes()``, ``aggregate_time`` therefore
        never does what it promises.

        This port replicates that behaviour deliberately, rather than
        raising or guessing a calendar origin: matching R is the
        contract, and the defect belongs to the producer. Pass a
        ``time_idx`` holding real years (or parseable dates) and
        aggregation works normally - verified reducing 8 periods to 4.
        The fix belongs upstream: the fitter has to preserve the
        original temporal label.

    Args:
        fit: A ``dict`` shaped like R's ``climasus_spacetime_bayes``
            object. ``sus_mod_spacetime_bayes()`` is still a stub in this
            package, so until a Python fitter exists this is produced in
            R and carried over. Only one slot is read:

            - ``"rr"``: ``pandas.DataFrame`` with columns
              ``"code_muni"``, ``"time_idx"``, ``"rr_mean"``,
              ``"rr_lower95"``, ``"rr_upper95"`` - one row per
              municipality-period cell.

            The full R object also carries ``"fixed"``, ``"spatial_re"``,
            ``"temporal_re"``, ``"interaction_re"``, ``"fitted"``,
            ``"waic"``, ``"dic"``, ``"model_spec"``, ``"n_areas"`` and
            ``"n_times"``; none is read here.
        thresholds: Relative-risk thresholds to test. Must be positive.
            Sorted and de-duplicated before use. Defaults to
            ``(1.0, 1.5, 2.0)``.
        aggregate_time: ``"year"``, ``"month"`` or ``None`` (default,
            one row per original cell). Aggregating requires
            ``time_idx`` to be a year number or a parseable date.
        municipalities: Optional ``geopandas.GeoDataFrame`` with a
            ``code_muni`` column. When given, the result's exceedance
            table is returned as a ``GeoDataFrame`` with geometry
            attached.
        lang: Message language: ``"pt"`` (default), ``"en"`` or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A ``dict`` (the Python analog of R's
        ``climasus_spacetime_exceedance`` object) with keys:

        - ``"exceedance"``: ``pandas.DataFrame`` (or ``GeoDataFrame``
          when *municipalities* is given) with ``"code_muni"``, the time
          column (``"time_idx"``, or ``"time_period"`` when aggregated),
          ``"rr_mean"``, and one ``p_gt_<threshold>`` column per
          threshold - the decimal point written as an underscore, so
          ``1.5`` becomes ``"p_gt_1_5"``.
        - ``"thresholds"``: the sorted, de-duplicated thresholds used.
        - ``"n_exceed"``: ``pandas.DataFrame`` with ``"threshold"`` and
          ``"n_cells_exceed"`` - cells whose probability clears
          ``0.8``.

        R's ``$call`` slot has no meaningful Python analog and is not
        reproduced, matching the other ported spatial functions.

    Raises:
        TypeError: If *fit* is not a dict, or *municipalities* is not a
            ``geopandas.GeoDataFrame``.
        ValueError: If ``rr`` is missing or lacks required columns, if
            *thresholds* is empty or non-positive, if *aggregate_time*
            is invalid, or if aggregation is asked of an unparseable
            ``time_idx``.

    Examples::

        import climasus4py as cs

        exc = cs.sus_mod_spacetime_exceedance(fit, thresholds=[1, 2])
        alto = exc["exceedance"].query("p_gt_2 > 0.95")
    """
    strings_lang = lang if lang in _MESSAGES else "pt"
    if verbose:
        console.print(_msg("step_validate", strings_lang))

    if not isinstance(fit, dict):
        raise TypeError(_msg("err_not_spacetime", strings_lang))
    rr_raw = fit.get("rr")
    if rr_raw is None or not isinstance(rr_raw, pd.DataFrame):
        raise ValueError(_msg("err_no_rr", strings_lang))
    missing = [c for c in REQUIRED_RR_COLS if c not in rr_raw.columns]
    if missing:
        raise ValueError(
            _msg(
                "err_rr_cols", strings_lang,
                required=", ".join(REQUIRED_RR_COLS),
                found=", ".join(rr_raw.columns),
            )
        )

    thresh = np.asarray(thresholds, dtype=float).ravel()
    if thresh.size == 0 or np.isnan(thresh).any() or (thresh <= 0).any():
        raise ValueError(_msg("err_bad_thresholds", strings_lang))
    thresh = np.unique(thresh)

    if aggregate_time is not None and aggregate_time not in ("year", "month"):
        raise ValueError(_msg("err_bad_aggregate", strings_lang))

    if municipalities is not None:
        import geopandas as gpd

        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise TypeError(_msg("err_no_sf", strings_lang))

    rr_df = rr_raw.copy()
    bad = (
        rr_df["rr_mean"].isna()
        | (rr_df["rr_mean"] <= 0)
        | rr_df["rr_lower95"].isna()
        | rr_df["rr_upper95"].isna()
    )
    n_na = int(bad.sum())
    if n_na > 0 and verbose:
        console.print(f"[yellow]{_msg('warn_na_rr', strings_lang, n_na=n_na)}[/yellow]")
    rr_df = rr_df[~bad].reset_index(drop=True)

    if aggregate_time is not None:
        if verbose:
            console.print(
                _msg("step_aggregate", strings_lang, period=aggregate_time)
            )
        rr_df = rr_df.assign(
            time_period=_time_period(
                rr_df["time_idx"], aggregate_time, strings_lang
            ),
            log_rr_mean=np.log(rr_df["rr_mean"].to_numpy(float)),
            sigma_log=_sigma_log(
                rr_df["rr_lower95"].to_numpy(float),
                rr_df["rr_upper95"].to_numpy(float),
            ),
        )
        # R splits on list(code_muni, time_period), whose interaction levels
        # vary the first factor fastest -- so rows come out ordered by
        # period, then municipality.
        work = (
            rr_df.groupby(["time_period", "code_muni"], sort=True, as_index=False)
            .agg(log_rr_mean=("log_rr_mean", "mean"), sigma_log=("sigma_log", "mean"))
        )
        work["rr_mean"] = np.exp(work["log_rr_mean"].to_numpy(float))
        work = work[["code_muni", "time_period", "rr_mean", "sigma_log"]]
        time_col = "time_period"
    else:
        work = rr_df[list(REQUIRED_RR_COLS)].copy()
        work["sigma_log"] = _sigma_log(
            work["rr_lower95"].to_numpy(float), work["rr_upper95"].to_numpy(float)
        )
        time_col = "time_idx"

    n_zero_sigma = int((work["sigma_log"] == 0).sum())
    if n_zero_sigma > 0 and verbose:
        console.print(
            f"[yellow]{_msg('warn_sigma_zero', strings_lang, n_zero=n_zero_sigma)}"
            "[/yellow]"
        )

    n_cells = len(work)
    if verbose:
        console.print(
            _msg(
                "step_compute", strings_lang,
                n_thresh=len(thresh), n_cells=n_cells,
            )
        )

    rr_mean = work["rr_mean"].to_numpy(float)
    sigma = work["sigma_log"].to_numpy(float)
    log_rr = np.log(rr_mean)

    exc = pd.DataFrame({"code_muni": work["code_muni"].to_numpy()})
    exc[time_col] = work[time_col].to_numpy()
    exc["rr_mean"] = rr_mean

    thresh_cols: list[str] = []
    for t in thresh:
        col = f"p_gt_{_threshold_suffix(t)}"
        thresh_cols.append(col)
        with np.errstate(divide="ignore", invalid="ignore"):
            prob = norm.sf(np.log(t), loc=log_rr, scale=np.where(sigma > 0, sigma, 1.0))
        exc[col] = np.where(sigma == 0, (rr_mean > t).astype(float), prob)

    if verbose:
        console.print(_msg("step_summarise", strings_lang))
    n_exceed = pd.DataFrame(
        {
            "threshold": thresh,
            "n_cells_exceed": [
                int((exc[c] > EXCEEDANCE_REPORT_CUTOFF).sum()) for c in thresh_cols
            ],
        }
    )

    if municipalities is not None:
        if verbose:
            console.print(_msg("step_spatial_join", strings_lang))
        geo = municipalities[["code_muni", municipalities.geometry.name]].copy()
        geo["code_muni"] = geo["code_muni"].astype(str)
        exc = exc.assign(code_muni=exc["code_muni"].astype(str)).merge(
            geo, on="code_muni", how="left"
        )
        import geopandas as gpd

        exc = gpd.GeoDataFrame(exc, geometry=municipalities.geometry.name)

    if verbose:
        first_col = thresh_cols[int(np.argmin(thresh))]
        n_above = int((exc[first_col] > EXCEEDANCE_REPORT_CUTOFF).sum())
        pct = 100.0 * n_above / max(n_cells, 1)
        console.print(
            f"[green]"
            f"{_msg('done', strings_lang, n_cells=n_cells, n_thresh=len(thresh), pct_exceed=f'{pct:.1f}')}"
            f"[/green]"
        )

    return {"exceedance": exc, "thresholds": thresh, "n_exceed": n_exceed}
