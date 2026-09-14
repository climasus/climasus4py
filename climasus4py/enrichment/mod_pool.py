"""Two-stage multi-city/multi-region pooling of DLNM estimates.

Mirrors R: sus_mod_pool.R

Stage 1 is the city-specific DLNM (``sus_mod_dlnm()``), which yields a
vector of cross-basis coefficients and its covariance. Stage 2 combines
those across cities by multivariate random-effects meta-analysis
(:mod:`climasus4py.enrichment._mvmeta`, a port of R's ``mvmeta``), and the
pooled coefficients are fed back through the same cross-basis to produce a
pooled exposure-response curve.

Both halves the R version delegates to packages are already ported and
checked against R: ``dlnm::crosspred`` lives in
:mod:`climasus4py.enrichment.mod_dlnm`, and ``mvmeta`` in
:mod:`climasus4py.enrichment._mvmeta` (verified against mvmeta 1.0.3 on
seven cases, p=1 to p=16).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

from climasus4py.enrichment._mvmeta import MvmetaFit, mvmeta_fit, qtest
from climasus4py.enrichment._mvmeta import blup as _mvmeta_blup
from climasus4py.enrichment.mod_dlnm import _crosspred

console = Console(stderr=True)


_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "header": "climasus4py — DLNM agrupado ({n} cidades)",
        "step_validate": "Validando {n} ajustes...",
        "step_extract": "Extraindo coeficientes do crossbasis...",
        "step_pool": "Agrupando por meta-analise multivariada ({method})...",
        "step_pred": "Predizendo a curva agrupada ({n} pontos)...",
        "step_blup": "Calculando BLUP por cidade...",
        "done": "RR agrupado no p75: {rr} [{lo}, {hi}]; I2 = {i2}%",
        "warn_lang": "Idioma nao reconhecido; usando 'pt'.",
        "warn_single_city": (
            "Apenas uma cidade: nao ha heterogeneidade a estimar e o "
            "resultado agrupado repete o ajuste dela."
        ),
        "warn_no_converge": (
            "A meta-analise nao convergiu. Trate Psi e os intervalos com "
            "cautela."
        ),
        "warn_overparam": (
            "Psi tem {n_par} parametros livres e as {m} cidades fornecem "
            "apenas {n_obs} numeros ({m} x {p}). A covariancia entre estudos "
            "nao e estimavel assim: a verossimilhanca fica achatada, nenhum "
            "otimizador converge e onde cada um para e arbitrario -- o mesmo "
            "acontece no R. Saidas: agrupar mais cidades, reduzir a base "
            "(argvar/arglag com menos df) ou usar method='fixed', que nao "
            "estima Psi."
        ),
        "warn_basis_differs": (
            "As cidades {cities} usam nos diferentes dos de {ref} na base de "
            "exposicao. Os coeficientes do crossbasis so sao comparaveis "
            "entre bases IDENTICAS: com nos diferentes, o mesmo coeficiente "
            "significa coisas diferentes em cada cidade, e agrupa-los produz "
            "um numero sem sentido -- nao um numero aproximado. Os nos saem "
            "dos quantis dos dados de cada cidade, entao isso acontece por "
            "padrao sempre que as cidades tem climas distintos. O R nao "
            "verifica isso."
        ),
        "err_not_dict": "'fits' deve ser um dict de {{nome_cidade: ajuste}}.",
        "err_empty": "'fits' esta vazio.",
        "err_not_dlnm": (
            "Estes elementos de 'fits' nao parecem saida de sus_mod_dlnm(): "
            "{bad}. Faltam as chaves {missing}."
        ),
        "err_incompatible": (
            "Os ajustes divergem em '{field}': {values}. So e possivel "
            "agrupar cidades com o mesmo {field}."
        ),
        "err_no_coef": "A cidade '{city}' nao tem coeficientes de crossbasis.",
    },
    "en": {
        "header": "climasus4py — Pooled DLNM ({n} cities)",
        "step_validate": "Validating {n} fits...",
        "step_extract": "Extracting cross-basis coefficients...",
        "step_pool": "Pooling by multivariate meta-analysis ({method})...",
        "step_pred": "Predicting the pooled curve ({n} points)...",
        "step_blup": "Computing per-city BLUPs...",
        "done": "Pooled RR at p75: {rr} [{lo}, {hi}]; I2 = {i2}%",
        "warn_lang": "Language not recognised; falling back to 'pt'.",
        "warn_single_city": (
            "Only one city: there is no heterogeneity to estimate and the "
            "pooled result simply repeats that city's fit."
        ),
        "warn_no_converge": (
            "The meta-analysis did not converge. Treat Psi and the intervals "
            "with caution."
        ),
        "warn_overparam": (
            "Psi has {n_par} free parameters while the {m} cities supply only "
            "{n_obs} numbers ({m} x {p}). The between-study covariance is not "
            "estimable this way: the likelihood goes flat, no optimiser "
            "converges, and where each one stops is arbitrary — the same "
            "happens in R. Ways out: pool more cities, reduce the basis "
            "(fewer df in argvar/arglag), or use method='fixed', which does "
            "not estimate Psi."
        ),
        "warn_basis_differs": (
            "Cities {cities} use different knots from {ref} in the exposure "
            "basis. Cross-basis coefficients are comparable only across "
            "IDENTICAL bases: with different knots the same coefficient means "
            "different things in each city, and pooling them yields a "
            "meaningless number — not an approximate one. Knots come from "
            "each city's own data quantiles, so this happens by default "
            "whenever cities have different climates. R does not check this."
        ),
        "err_not_dict": "'fits' must be a dict of {{city_name: fit}}.",
        "err_empty": "'fits' is empty.",
        "err_not_dlnm": (
            "These entries of 'fits' do not look like sus_mod_dlnm() output: "
            "{bad}. Missing keys {missing}."
        ),
        "err_incompatible": (
            "Fits disagree on '{field}': {values}. Only cities sharing the "
            "same {field} can be pooled."
        ),
        "err_no_coef": "City '{city}' has no cross-basis coefficients.",
    },
    "es": {
        "header": "climasus4py — DLNM agrupado ({n} ciudades)",
        "step_validate": "Validando {n} ajustes...",
        "step_extract": "Extrayendo coeficientes del crossbasis...",
        "step_pool": "Agrupando por metaanalisis multivariante ({method})...",
        "step_pred": "Prediciendo la curva agrupada ({n} puntos)...",
        "step_blup": "Calculando BLUP por ciudad...",
        "done": "RR agrupado en p75: {rr} [{lo}, {hi}]; I2 = {i2}%",
        "warn_lang": "Idioma no reconocido; usando 'pt'.",
        "warn_single_city": (
            "Solo una ciudad: no hay heterogeneidad que estimar y el "
            "resultado agrupado repite su ajuste."
        ),
        "warn_no_converge": (
            "El metaanalisis no convergio. Trate Psi y los intervalos con "
            "cautela."
        ),
        "warn_overparam": (
            "Psi tiene {n_par} parametros libres y las {m} ciudades aportan "
            "solo {n_obs} numeros ({m} x {p}). La covarianza entre estudios no "
            "es estimable asi: la verosimilitud se aplana, ningun optimizador "
            "converge y donde para cada uno es arbitrario — lo mismo ocurre en "
            "R. Salidas: agrupar mas ciudades, reducir la base (menos df en "
            "argvar/arglag) o usar method='fixed', que no estima Psi."
        ),
        "warn_basis_differs": (
            "Las ciudades {cities} usan nudos distintos de {ref} en la base "
            "de exposicion. Los coeficientes del crossbasis solo son "
            "comparables entre bases IDENTICAS: con nudos distintos el mismo "
            "coeficiente significa cosas diferentes en cada ciudad, y "
            "agruparlos produce un numero sin sentido, no uno aproximado. "
            "Los nudos salen de los cuantiles de cada ciudad, asi que esto "
            "ocurre por defecto cuando los climas difieren. R no lo verifica."
        ),
        "err_not_dict": "'fits' debe ser un dict de {{nombre_ciudad: ajuste}}.",
        "err_empty": "'fits' esta vacio.",
        "err_not_dlnm": (
            "Estos elementos de 'fits' no parecen salida de sus_mod_dlnm(): "
            "{bad}. Faltan las claves {missing}."
        ),
        "err_incompatible": (
            "Los ajustes difieren en '{field}': {values}. Solo se pueden "
            "agrupar ciudades con el mismo {field}."
        ),
        "err_no_coef": "La ciudad '{city}' no tiene coeficientes de crossbasis.",
    },
}

_REQUIRED_KEYS = ("model", "crossbasis", "meta", "data_daily")


def _msgs(lang: str) -> dict[str, str]:
    if lang not in _MESSAGES:
        console.print(f"[yellow]{_MESSAGES['pt']['warn_lang']}[/yellow]")
        return _MESSAGES["pt"]
    return _MESSAGES[lang]


def _city_coefficients(fit: dict[str, Any], city: str, msgs: dict[str, str]):
    """Pull the cross-basis block out of a fitted city model.

    R finds it by name (``grep("^cb", names(coef))``); the Python
    ``sus_mod_dlnm`` builds its design as ``[intercept | crossbasis |
    seasonality | covariates]``, so the block is the contiguous slice
    right after the intercept.
    """
    n_cb = int(np.asarray(fit["crossbasis"]).shape[1])
    if n_cb == 0:
        raise ValueError(msgs["err_no_coef"].format(city=city))
    params = np.asarray(fit["model"].params, dtype=float)
    vcov = np.asarray(fit["model"].cov_params(), dtype=float)
    return params[1 : 1 + n_cb], vcov[1 : 1 + n_cb, 1 : 1 + n_cb]


def _city_exposure(fit: dict[str, Any]) -> np.ndarray:
    """The city's observed exposure series (lag-0 column when present)."""
    climate_col = fit["meta"]["climate_col"]
    daily = fit["data_daily"]
    col = f"{climate_col}_lag0"
    values = daily[col] if col in daily.columns else daily[climate_col]
    values = np.asarray(values, dtype=float)
    return values[np.isfinite(values)]


def _same_basis(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Whether two exposure bases are numerically identical."""
    if a.get("fun") != b.get("fun") or a.get("df") != b.get("df"):
        return False
    for key in ("knots", "boundary_knots"):
        va, vb = np.atleast_1d(a.get(key, [])), np.atleast_1d(b.get(key, []))
        if va.shape != vb.shape or not np.allclose(va, vb, rtol=1e-9, atol=1e-12):
            return False
    return True


def _check_shared(fits: dict[str, Any], field: str, msgs: dict[str, str]) -> Any:
    values = {name: fit["meta"][field] for name, fit in fits.items()}
    distinct = {repr(v) for v in values.values()}
    if len(distinct) > 1:
        raise ValueError(
            msgs["err_incompatible"].format(field=field, values=sorted(distinct))
        )
    return next(iter(values.values()))


def sus_mod_pool(
    fits: dict[str, Any],
    exposure_range: tuple[float, float] | None = None,
    n_grid: int = 100,
    pred_at: tuple[float, ...] = (0.75, 0.90, 0.95, 0.99),
    blup: bool = True,  # noqa: A002 - name kept for parity with R
    method: Literal["reml", "ml", "fixed"] = "reml",
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Two-stage multi-city pooling of DLNM estimates.

    Combines city-specific cross-basis coefficients through a multivariate
    random-effects meta-analysis, then predicts a pooled exposure-response
    curve from the pooled coefficients.

    Args:
        fits: A dict of ``{city_name: fit}``, where each fit is what
            ``sus_mod_dlnm()`` returns. All fits must share
            ``climate_col`` and ``lag_max``.
        exposure_range: ``(low, high)`` for the prediction grid, or
            ``None`` to span the combined range across all cities.
        n_grid: Number of grid points on the pooled curve. Default 100.
        pred_at: Quantile probabilities for the summary table. Default
            ``(0.75, 0.90, 0.95, 0.99)``.
        blup: Whether to compute per-city BLUP curves. Default ``True``.
        method: ``"reml"`` (default), ``"ml"``, or ``"fixed"``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: Whether to print progress. Default ``True``.

    Returns:
        A dict mirroring R's ``climasus_pool`` object, with keys
        ``mvmeta_fit``, ``pooled_pred``, ``exposure_response``,
        ``exposure_curve``, ``lag_response``, ``blup_preds``,
        ``city_table``, ``heterogeneity`` and ``meta``.

    Raises:
        ValueError: If ``fits`` is empty, not a dict, contains entries
            that are not ``sus_mod_dlnm()`` output, or if the fits
            disagree on ``climate_col`` or ``lag_max``.

    Warns:
        Prints a warning when the cities' exposure bases use different
        knots. Pooling is still performed, matching R, but the pooled
        coefficients are then not interpretable — see the note in the
        module's tests and ``IDEIAS.md``.
    """
    msgs = _msgs(lang)

    # ---- validation -------------------------------------------------
    if not isinstance(fits, dict):
        raise ValueError(msgs["err_not_dict"])
    if len(fits) == 0:
        raise ValueError(msgs["err_empty"])

    bad: dict[str, list[str]] = {}
    for name, fit in fits.items():
        if not isinstance(fit, dict):
            bad[name] = list(_REQUIRED_KEYS)
            continue
        missing = [k for k in _REQUIRED_KEYS if k not in fit]
        if missing:
            bad[name] = missing
    if bad:
        raise ValueError(
            msgs["err_not_dlnm"].format(
                bad=sorted(bad), missing=sorted({k for v in bad.values() for k in v})
            )
        )

    city_names = list(fits.keys())
    n_cities = len(city_names)
    if verbose:
        console.print(f"[bold cyan]{msgs['header'].format(n=n_cities)}[/bold cyan]")
        console.print(msgs["step_validate"].format(n=n_cities))
    if n_cities == 1:
        console.print(f"[yellow]{msgs['warn_single_city']}[/yellow]")

    lag_max = int(_check_shared(fits, "lag_max", msgs))
    climate_col = _check_shared(fits, "climate_col", msgs)

    # The bases must be identical for the coefficients to mean the same
    # thing city to city. R checks lag_max and climate_col but not this.
    ref_name = city_names[0]
    ref_var_meta = fits[ref_name]["meta"]["var_meta"]
    divergent = [
        n for n in city_names[1:]
        if not _same_basis(fits[n]["meta"]["var_meta"], ref_var_meta)
    ]
    if divergent:
        console.print(
            f"[yellow]{msgs['warn_basis_differs'].format(cities=divergent, ref=ref_name)}[/yellow]"
        )

    # ---- stage 1: extract ------------------------------------------
    if verbose:
        console.print(msgs["step_extract"])
    coefs, vcovs = [], []
    for name in city_names:
        coef_i, vcov_i = _city_coefficients(fits[name], name, msgs)
        coefs.append(coef_i)
        vcovs.append(vcov_i)
    widths = {len(c) for c in coefs}
    if len(widths) > 1:
        raise ValueError(
            msgs["err_incompatible"].format(field="n_coef", values=sorted(widths))
        )
    coef_mat = np.vstack(coefs)

    # ---- stage 2: pool ---------------------------------------------
    if verbose:
        console.print(msgs["step_pool"].format(method=method))
    pool_fit: MvmetaFit = mvmeta_fit(coef_mat, vcovs, method=method)
    if pool_fit.overparametrised:
        console.print(f"[yellow]{msgs['warn_overparam'].format(
            n_par=pool_fit.n_par_psi, m=n_cities, p=pool_fit.n_outcomes,
            n_obs=n_cities * pool_fit.n_outcomes)}[/yellow]")
    elif not pool_fit.converged:
        console.print(f"[yellow]{msgs['warn_no_converge']}[/yellow]")

    # ---- prediction grid -------------------------------------------
    all_expo = np.concatenate([_city_exposure(fits[n]) for n in city_names])
    lo = float(np.min(all_expo)) if exposure_range is None else float(exposure_range[0])
    hi = float(np.max(all_expo)) if exposure_range is None else float(exposure_range[1])
    expo_grid = np.linspace(lo, hi, int(n_grid))
    ref_value = float(np.median(all_expo))

    if verbose:
        console.print(msgs["step_pred"].format(n=len(expo_grid)))
    meta_ref = fits[ref_name]["meta"]
    alpha = float(meta_ref.get("alpha", 0.05))
    pooled_pred = _crosspred(
        meta_ref["var_meta"], meta_ref["lag_meta"],
        pool_fit.coef, pool_fit.vcov,
        expo_grid, lag_max, ref_value, alpha, lang,
    )

    # ---- summary tables --------------------------------------------
    quantis = np.quantile(all_expo, pred_at)
    exposure_response = pd.DataFrame([
        {
            "pct": pct,
            "exposure": round(float(val), 3),
            "rr": float(pooled_pred["allRRfit"][idx]),
            "lo": float(pooled_pred["allRRlow"][idx]),
            "hi": float(pooled_pred["allRRhigh"][idx]),
        }
        for pct, val, idx in (
            (p, v, int(np.argmin(np.abs(expo_grid - v))))
            for p, v in zip(pred_at, quantis, strict=True)
        )
    ])

    exposure_curve = pd.DataFrame({
        "exposure": pooled_pred["predvar"],
        "rr": pooled_pred["allRRfit"],
        "lo": pooled_pred["allRRlow"],
        "hi": pooled_pred["allRRhigh"],
    })

    p75_val = float(np.quantile(all_expo, 0.75))
    p75_idx = int(np.argmin(np.abs(expo_grid - p75_val)))
    lag_response = pd.DataFrame({
        "lag": np.arange(0, lag_max + 1),
        "rr_lag": pooled_pred["matRRfit"][p75_idx, :],
        "lo": pooled_pred["matRRlow"][p75_idx, :],
        "hi": pooled_pred["matRRhigh"][p75_idx, :],
    })

    het = qtest(pool_fit)
    heterogeneity = pd.DataFrame([{
        "Q": round(het["Q"], 2),
        "df": het["df"],
        "p_value": round(het["p_value"], 4),
        "i2": round(het["i2"], 1),
    }])

    # ---- per-city table and BLUPs ----------------------------------
    rows = []
    for name in city_names:
        models = fits[name].get("models")
        first = models.iloc[0] if models is not None and len(models) else {}
        rows.append({
            "city": name,
            "n": fits[name]["meta"].get("n"),
            "rr": float(first["rr"]) if "rr" in first else np.nan,
            "lo": float(first["lo"]) if "lo" in first else np.nan,
            "hi": float(first["hi"]) if "hi" in first else np.nan,
            "blup_rr": np.nan, "blup_lo": np.nan, "blup_hi": np.nan,
        })
    city_table = pd.DataFrame(rows)

    blup_preds: dict[str, dict[str, Any]] | None = None
    if blup and n_cities >= 2:
        if verbose:
            console.print(msgs["step_blup"])
        blup_preds = {}
        blups = _mvmeta_blup(pool_fit)
        for i, name in enumerate(city_names):
            pred_b = _crosspred(
                meta_ref["var_meta"], meta_ref["lag_meta"],
                blups[i]["blup"], blups[i]["vcov"],
                expo_grid, lag_max, ref_value, alpha, lang,
            )
            blup_preds[name] = pred_b
            city_table.loc[i, "blup_rr"] = float(pred_b["allRRfit"][p75_idx])
            city_table.loc[i, "blup_lo"] = float(pred_b["allRRlow"][p75_idx])
            city_table.loc[i, "blup_hi"] = float(pred_b["allRRhigh"][p75_idx])

    if verbose:
        p75_row = exposure_response.iloc[
            int(np.argmin(np.abs(exposure_response["pct"] - 0.75)))
        ]
        console.print(msgs["done"].format(
            rr=round(float(p75_row["rr"]), 4),
            lo=round(float(p75_row["lo"]), 4),
            hi=round(float(p75_row["hi"]), 4),
            i2=round(het["i2"], 1),
        ))

    return {
        "mvmeta_fit": pool_fit,
        "pooled_pred": pooled_pred,
        "exposure_response": exposure_response,
        "exposure_curve": exposure_curve,
        "lag_response": lag_response,
        "blup_preds": blup_preds,
        "city_table": city_table,
        "heterogeneity": heterogeneity,
        "meta": {
            "climate_col": climate_col,
            "outcome_col": meta_ref.get("outcome_col"),
            "lag_max": lag_max,
            "n_cities": n_cities,
            "city_names": city_names,
            "method": method,
            "argvar": meta_ref.get("argvar"),
            "arglag": meta_ref.get("arglag"),
            "ref_value": ref_value,
            "expo_grid": expo_grid,
            "pred_at": pred_at,
            "shared_basis": not divergent,
            "call_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }
