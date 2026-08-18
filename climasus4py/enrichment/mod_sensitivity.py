"""Stratified climate-health sensitivity analysis from DLNM fits.

Mirrors R: sus_mod_sensitivity.R

Theory: Baccini et al. (2011, Epidemiology); Benmarhnia et al. (2015,
Environ Health); Armstrong et al. (2017, Environ Epidemiol).

Input: a named dict (or list, auto-named "Stratum 1", "Stratum 2", ...)
of dicts returned by ``sus_mod_dlnm()`` — one per stratum/subgroup.
Not lazy — pure comparison/ranking logic over already-fitted models.

Unlike ``sus_mod_af``/``sus_mod_excess``, this function never re-invokes
crosspred-style prediction machinery: it only reads each fit's
pre-computed 100-point exposure grid (``fit["pred"]["predvar"]``/
``"allRRfit"``/``"allRRlow"``/``"allRRhigh"``, already correctly ordered
by construction) and linearly interpolates the RR at hot/cold exposure
percentiles — so the crosspred sort/dedup misalignment bug documented in
``mod_af.py``/``mod_excess.py`` does not apply here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_validate": "Validando {n_strata} estrato(s)...",
        "step_extract": "Extraindo curvas de exposicao-resposta por estrato...",
        "step_compare": (
            "Calculando metricas de sensibilidade (p{hot_pct} quente, p{cold_pct} frio)..."
        ),
        "done": (
            "Concluido. Estrato mais sensivel ao calor: {top_hot} (RR = {rr_hot}); "
            "frio: {top_cold} (RR = {rr_cold})"
        ),
        "err_empty": "'fits' deve conter pelo menos 2 fits de sus_mod_dlnm().",
        "err_not_dlnm": "'fits' deve conter apenas dicts de sus_mod_dlnm(). Invalido(s): {bad}.",
        "err_diff_clim": (
            "Todos os ajustes devem usar a mesma variavel climatica. Encontrado: {vars}."
        ),
        "err_bad_quantile": (
            "'hot_percentile' e 'cold_percentile' devem estar em (0,1) com hot > cold."
        ),
        "warn_extrapolate": (
            "Estrato '{stratum}': percentil {q} ({exp_val}) fora da grade crosspred "
            "[{grid_min}, {grid_max}]. RR interpolado com extrapolacao."
        ),
    },
    "en": {
        "step_validate": "Validating {n_strata} stratum/strata...",
        "step_extract": "Extracting exposure-response curves per stratum...",
        "step_compare": "Computing sensitivity metrics (p{hot_pct} hot, p{cold_pct} cold)...",
        "done": (
            "Done. Most heat-sensitive stratum: {top_hot} (RR = {rr_hot}); "
            "cold: {top_cold} (RR = {rr_cold})"
        ),
        "err_empty": "'fits' must contain at least 2 sus_mod_dlnm() fits.",
        "err_not_dlnm": "'fits' must contain only sus_mod_dlnm() dicts. Invalid: {bad}.",
        "err_diff_clim": "All fits must use the same climate variable. Found: {vars}.",
        "err_bad_quantile": (
            "'hot_percentile' and 'cold_percentile' must be in (0,1) with hot > cold."
        ),
        "warn_extrapolate": (
            "Stratum '{stratum}': percentile {q} ({exp_val}) outside crosspred grid "
            "[{grid_min}, {grid_max}]. RR extrapolated."
        ),
    },
    "es": {
        "step_validate": "Validando {n_strata} estrato(s)...",
        "step_extract": "Extrayendo curvas de exposicion-respuesta por estrato...",
        "step_compare": (
            "Calculando metricas de sensibilidad (p{hot_pct} calor, p{cold_pct} frio)..."
        ),
        "done": (
            "Listo. Estrato mas sensible al calor: {top_hot} (RR = {rr_hot}); "
            "frio: {top_cold} (RR = {rr_cold})"
        ),
        "err_empty": "'fits' debe contener al menos 2 ajustes de sus_mod_dlnm().",
        "err_not_dlnm": "'fits' debe contener solo dicts de sus_mod_dlnm(). Invalido(s): {bad}.",
        "err_diff_clim": (
            "Todos los ajustes deben usar la misma variable climatica. Encontradas: {vars}."
        ),
        "err_bad_quantile": (
            "'hot_percentile' y 'cold_percentile' deben estar en (0,1) con hot > cold."
        ),
        "warn_extrapolate": (
            "Estrato '{stratum}': percentil {q} ({exp_val}) fuera de la grilla crosspred "
            "[{grid_min}, {grid_max}]. RR extrapolado."
        ),
    },
}


def _is_dlnm_fit(obj: Any) -> bool:
    return isinstance(obj, dict) and "meta" in obj and "climate_col" in obj.get("meta", {})


def _interp(
    x_at: float,
    grid_x: np.ndarray,
    grid_rr: np.ndarray,
    grid_lo: np.ndarray,
    grid_hi: np.ndarray,
    stratum: str,
    q_label: int,
    lang: str,
    verbose: bool,
) -> tuple[float, float, float]:
    if verbose and (x_at < grid_x.min() or x_at > grid_x.max()):
        msg = _MESSAGES[lang]["warn_extrapolate"].format(
            stratum=stratum,
            q=q_label,
            exp_val=round(x_at, 2),
            grid_min=round(float(grid_x.min()), 2),
            grid_max=round(float(grid_x.max()), 2),
        )
        console.print(f"[yellow]{msg}[/yellow]")
    # np.interp clamps to the boundary value outside [grid_x.min(), grid_x.max()],
    # matching R's stats::approx(..., rule=2).
    rr_val = float(np.interp(x_at, grid_x, grid_rr))
    lo_val = float(np.interp(x_at, grid_x, grid_lo))
    hi_val = float(np.interp(x_at, grid_x, grid_hi))
    return rr_val, lo_val, hi_val


def sus_mod_sensitivity(
    fits: dict[str, dict[str, Any]] | list[dict[str, Any]],
    hot_percentile: float = 0.99,
    cold_percentile: float = 0.01,
    stratum_labels: dict[str, str] | None = None,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Compare climate-health sensitivity across strata from DLNM fits.

    Compares exposure-response curves and cumulative relative risks across
    population strata (e.g. age groups, sexes, municipalities) to identify
    which subgroups are most sensitive to a climate exposure. Extracts the
    cumulative RR at user-specified hot and cold exposure percentiles for
    each stratum's fitted DLNM, producing a ranked comparison table plus
    the full per-stratum exposure-response curves for plotting.

    Args:
        fits: A named dict (or a plain list, auto-labelled "Stratum 1",
            "Stratum 2", ...) of dicts returned by ``sus_mod_dlnm()``, one
            per stratum. All fits must share the same ``climate_col``.
        hot_percentile: Quantile of the observed exposure distribution
            used as the "hot" comparison point. Must exceed
            ``cold_percentile``.
        cold_percentile: Quantile used as the "cold" comparison point.
        stratum_labels: Optional dict mapping stratum keys to display
            labels (e.g. ``{"age_65plus": "65+ years"}``). Keys not
            present default to the stratum key itself.
        alpha: Significance level (currently informational only — CIs are
            read directly from each fit's crosspred grid, already computed
            at that fit's own ``alpha``).
        lang: Message language.
        verbose: Print progress messages and extrapolation warnings.

    Returns:
        A dict (Python analog of R's ``climasus_sensitivity`` S3 object):
        ``rr_table`` (one row per stratum x hot/cold component),
        ``comparison`` (one row per stratum, ranked by sensitivity index),
        ``stratum_curves`` (full exposure-response grid per stratum),
        ``meta``.

    Raises:
        ValueError: If fewer than 2 fits are given, any fit isn't a
            ``sus_mod_dlnm()`` dict, fits use different climate variables,
            or the percentiles are invalid.

    Examples::

        import climasus4py as cs

        fits = {
            "elderly": cs.sus_mod_dlnm(df_65plus, lag_max=14),
            "adults": cs.sus_mod_dlnm(df_1864, lag_max=14),
        }
        sens = cs.sus_mod_sensitivity(fits, lang="pt")
        sens["comparison"]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msgs = _MESSAGES[lang]

    if isinstance(fits, list):
        fits = {f"Stratum {i + 1}": f for i, f in enumerate(fits)}

    if len(fits) < 2:
        raise ValueError(msgs["err_empty"])

    bad = [name for name, f in fits.items() if not _is_dlnm_fit(f)]
    if bad:
        raise ValueError(msgs["err_not_dlnm"].format(bad=", ".join(bad)))

    stratum_names = list(fits.keys())
    n_strata = len(fits)

    clim_cols = {name: f["meta"]["climate_col"] for name, f in fits.items()}
    unique_cols = set(clim_cols.values())
    if len(unique_cols) > 1:
        raise ValueError(msgs["err_diff_clim"].format(vars=", ".join(sorted(unique_cols))))
    climate_col = next(iter(unique_cols))

    valid_pcts = 0 < hot_percentile < 1 and 0 < cold_percentile < 1
    if not (valid_pcts and hot_percentile > cold_percentile):
        raise ValueError(msgs["err_bad_quantile"])

    labels = {name: name for name in stratum_names}
    if stratum_labels:
        for name, label in stratum_labels.items():
            if name in labels:
                labels[name] = label

    if verbose:
        console.print(msgs["step_validate"].format(n_strata=n_strata))
        console.print(msgs["step_extract"])

    curve_rows = []
    for name, fit in fits.items():
        pred = fit["pred"]
        curve_rows.append(
            pd.DataFrame(
                {
                    "stratum": name,
                    "label": labels[name],
                    "exposure": np.asarray(pred["predvar"], dtype=float),
                    "rr": np.asarray(pred["allRRfit"], dtype=float),
                    "rr_lo": np.asarray(pred["allRRlow"], dtype=float),
                    "rr_hi": np.asarray(pred["allRRhigh"], dtype=float),
                }
            )
        )
    stratum_curves = pd.concat(curve_rows, ignore_index=True)

    hot_pct_label = round(hot_percentile * 100)
    cold_pct_label = round(cold_percentile * 100)
    if verbose:
        console.print(msgs["step_compare"].format(hot_pct=hot_pct_label, cold_pct=cold_pct_label))

    rr_row_list = []
    for name, fit in fits.items():
        lag0_col = f"{climate_col}_lag0"
        expo_vals = fit["data_daily"][lag0_col].to_numpy(dtype=float)
        hot_x = float(np.quantile(expo_vals, hot_percentile))
        cold_x = float(np.quantile(expo_vals, cold_percentile))
        ref_x = float(fit["meta"]["ref_value"])

        pred = fit["pred"]
        grid_x = np.asarray(pred["predvar"], dtype=float)
        grid_rr = np.asarray(pred["allRRfit"], dtype=float)
        grid_lo = np.asarray(pred["allRRlow"], dtype=float)
        grid_hi = np.asarray(pred["allRRhigh"], dtype=float)

        for comp, x_at, q_label, q_prob in (
            ("hot", hot_x, hot_pct_label, hot_percentile),
            ("cold", cold_x, cold_pct_label, cold_percentile),
        ):
            rr_val, lo_val, hi_val = _interp(
                x_at, grid_x, grid_rr, grid_lo, grid_hi, name, q_label, lang, verbose
            )
            rr_row_list.append(
                {
                    "stratum": name,
                    "label": labels[name],
                    "component": comp,
                    "quantile_prob": q_prob,
                    "exposure": x_at,
                    "rr": rr_val,
                    "rr_lo": lo_val,
                    "rr_hi": hi_val,
                    "ref_exposure": ref_x,
                }
            )
    rr_table = pd.DataFrame(rr_row_list)

    comparison = _build_comparison(rr_table, stratum_names, labels)

    if verbose and len(comparison) > 0:
        top_h = comparison.iloc[0]["label"]
        rr_h = round(comparison.iloc[0]["hot_rr"], 3)
        cold_ord = comparison.sort_values("cold_rr", ascending=False)
        top_c = cold_ord.iloc[0]["label"]
        rr_c = round(cold_ord.iloc[0]["cold_rr"], 3)
        console.print(msgs["done"].format(top_hot=top_h, rr_hot=rr_h, top_cold=top_c, rr_cold=rr_c))

    return {
        "rr_table": rr_table,
        "comparison": comparison,
        "stratum_curves": stratum_curves,
        "meta": {
            "climate_col": climate_col,
            "n_strata": n_strata,
            "stratum_names": stratum_names,
            "stratum_labels": labels,
            "hot_percentile": hot_percentile,
            "cold_percentile": cold_percentile,
            "alpha": alpha,
            "call_time": datetime.now(),
        },
    }


def _build_comparison(
    rr_table: pd.DataFrame, stratum_names: list[str], labels: dict[str, str]
) -> pd.DataFrame:
    hot_rows = rr_table[rr_table["component"] == "hot"]
    cold_rows = rr_table[rr_table["component"] == "cold"]

    rows = []
    for name in stratum_names:
        h = hot_rows[hot_rows["stratum"] == name]
        c = cold_rows[cold_rows["stratum"] == name]

        h_rr = float(h["rr"].iloc[0]) if len(h) else np.nan
        h_lo = float(h["rr_lo"].iloc[0]) if len(h) else np.nan
        h_hi = float(h["rr_hi"].iloc[0]) if len(h) else np.nan
        c_rr = float(c["rr"].iloc[0]) if len(c) else np.nan
        c_lo = float(c["rr_lo"].iloc[0]) if len(c) else np.nan
        c_hi = float(c["rr_hi"].iloc[0]) if len(c) else np.nan

        hot_log = np.log(h_rr) if not np.isnan(h_rr) and h_rr > 0 else np.nan
        cold_log = np.log(c_rr) if not np.isnan(c_rr) and c_rr > 0 else np.nan
        si = hot_log + cold_log if not np.isnan(hot_log) and not np.isnan(cold_log) else np.nan

        rows.append(
            {
                "stratum": name,
                "label": labels[name],
                "hot_rr": h_rr,
                "hot_rr_lo": h_lo,
                "hot_rr_hi": h_hi,
                "cold_rr": c_rr,
                "cold_rr_lo": c_lo,
                "cold_rr_hi": c_hi,
                "sensitivity_index": si,
            }
        )
    tbl = pd.DataFrame(rows)
    tbl["hot_rank"] = tbl["hot_rr"].rank(ascending=False, method="min", na_option="keep")
    tbl["cold_rank"] = tbl["cold_rr"].rank(ascending=False, method="min", na_option="keep")
    return tbl.sort_values("sensitivity_index", ascending=False).reset_index(drop=True)
