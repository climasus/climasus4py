"""SWOT analysis for climate-health surveillance.

Mirrors R: sus_mod_swot.R

Theory: IPCC AR6 (2022) — Vulnerability = f(Exposure, Sensitivity, Adaptive
Capacity); Andrews (1971) SWOT framework adapted for climate-health public
policy; Gasparrini et al. (2017, Lancet Planet Health) — multi-city
attribution.

Not lazy — synthesizes the dicts already returned by
``sus_mod_vulnerability_index()``, ``sus_mod_af()``, ``sus_mod_burden()``,
``sus_mod_dlnm()``, ``sus_mod_sensitivity()`` (all pure in-memory
pandas/NumPy analyses, same as the R source). Despite what the R
`no-port-deps.md`/comment trail for this ecosystem might suggest, this
function itself never calls ``dlnm``/``mvmeta`` — it only reads fields
(``vi_table``, ``total``, ``burden_table``, ``exposure_response``,
``comparison``) off objects already producible in climasus4py.
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
        "title": "climasus4py — Análise SWOT Climático-Saúde",
        "step_extract": "Extraindo indicadores de {n_inputs} fonte(s)...",
        "step_score": "Calculando pontuações SWOT para {n_entities} entidade(s)...",
        "done": "Concluído. Entidades: {n_ent}; Indicadores: {n_ind}; Fontes: {sources}",
        "err_no_input": (
            "Ao menos um objeto climasus deve ser fornecido "
            "(vulnerability, af, burden, dlnm ou sensitivity)."
        ),
        "err_bad_breaks": (
            "'breaks' deve ser um vetor numérico crescente com valores "
            "estritamente entre 0 e 100."
        ),
        "err_bad_labels": "'labels' deve ter comprimento {n_cats} (len(breaks) + 1).",
        "err_not_vi": "'vulnerability' deve ser o dict retornado por sus_mod_vulnerability_index().",
        "err_not_af": "'af' deve ser o dict retornado por sus_mod_af().",
        "err_not_burden": "'burden' deve ser o dict retornado por sus_mod_burden().",
        "err_not_dlnm": "'dlnm' deve ser o dict retornado por sus_mod_dlnm().",
        "err_not_sensitivity": "'sensitivity' deve ser o dict retornado por sus_mod_sensitivity().",
    },
    "en": {
        "title": "climasus4py — Climate-Health SWOT Analysis",
        "step_extract": "Extracting indicators from {n_inputs} source(s)...",
        "step_score": "Computing SWOT scores for {n_entities} entity/entities...",
        "done": "Done. Entities: {n_ent}; Indicators: {n_ind}; Sources: {sources}",
        "err_no_input": (
            "At least one climasus object must be provided "
            "(vulnerability, af, burden, dlnm, or sensitivity)."
        ),
        "err_bad_breaks": (
            "'breaks' must be a strictly increasing numeric vector with "
            "values between 0 and 100 (exclusive)."
        ),
        "err_bad_labels": "'labels' must have length {n_cats} (len(breaks) + 1).",
        "err_not_vi": "'vulnerability' must be the dict returned by sus_mod_vulnerability_index().",
        "err_not_af": "'af' must be the dict returned by sus_mod_af().",
        "err_not_burden": "'burden' must be the dict returned by sus_mod_burden().",
        "err_not_dlnm": "'dlnm' must be the dict returned by sus_mod_dlnm().",
        "err_not_sensitivity": "'sensitivity' must be the dict returned by sus_mod_sensitivity().",
    },
    "es": {
        "title": "climasus4py — Análisis SWOT Clima-Salud",
        "step_extract": "Extrayendo indicadores de {n_inputs} fuente(s)...",
        "step_score": "Calculando puntuaciones SWOT para {n_entities} entidad(es)...",
        "done": "Listo. Entidades: {n_ent}; Indicadores: {n_ind}; Fuentes: {sources}",
        "err_no_input": (
            "Debe proporcionarse al menos un objeto climasus "
            "(vulnerability, af, burden, dlnm o sensitivity)."
        ),
        "err_bad_breaks": (
            "'breaks' debe ser un vector numérico estrictamente creciente con "
            "valores entre 0 y 100 (exclusivos)."
        ),
        "err_bad_labels": "'labels' debe tener longitud {n_cats} (len(breaks) + 1).",
        "err_not_vi": "'vulnerability' debe ser el dict retornado por sus_mod_vulnerability_index().",
        "err_not_af": "'af' debe ser el dict retornado por sus_mod_af().",
        "err_not_burden": "'burden' debe ser el dict retornado por sus_mod_burden().",
        "err_not_dlnm": "'dlnm' debe ser el dict retornado por sus_mod_dlnm().",
        "err_not_sensitivity": "'sensitivity' debe ser el dict retornado por sus_mod_sensitivity().",
    },
}

_IND_LABELS: dict[str, dict[str, str]] = {
    "adaptive_capacity_vi": {
        "pt": "Capacidade Adaptativa (IV)",
        "en": "Adaptive Capacity (VI)",
        "es": "Capacidad Adaptativa (IV)",
    },
    "sensitivity_vi": {
        "pt": "Sensibilidade Populacional (IV)",
        "en": "Population Sensitivity (VI)",
        "es": "Sensibilidad Poblacional (IV)",
    },
    "af_total_inv": {
        "pt": "Baixa Carga Atribuível (FA inv.)",
        "en": "Low Attributable Burden (AF inv.)",
        "es": "Baja Carga Atribuible (FA inv.)",
    },
    "burden_rank_inv": {
        "pt": "Posto de Carga Baixo (inv.)",
        "en": "Low Burden Rank (inv.)",
        "es": "Bajo Rango de Carga (inv.)",
    },
    "heat_af_pct": {"pt": "FA ao Calor (%)", "en": "Heat AF (%)", "es": "FA por Calor (%)"},
    "cold_af_pct": {"pt": "FA ao Frio (%)", "en": "Cold AF (%)", "es": "FA por Frío (%)"},
    "stratum_inequality": {
        "pt": "Desigualdade entre Estratos",
        "en": "Stratum Inequality",
        "es": "Desigualdad entre Estratos",
    },
    "vi_percentile_inv": {
        "pt": "Janela de Intervenção (Percentil IV inv.)",
        "en": "Intervention Window (VI Percentile inv.)",
        "es": "Ventana de Intervención (Percentil IV inv.)",
    },
    "exposure_inv": {
        "pt": "Exposição Atual Moderada (Exposição inv.)",
        "en": "Moderate Current Exposure (Exposure inv.)",
        "es": "Exposición Actual Moderada (Exposición inv.)",
    },
    "exposure_vi": {
        "pt": "Exposição Climática (IV)",
        "en": "Climate Exposure (VI)",
        "es": "Exposición Climática (IV)",
    },
    "vi_score_threat": {
        "pt": "Índice de Vulnerabilidade Global",
        "en": "Overall Vulnerability Index",
        "es": "Índice de Vulnerabilidad Global",
    },
    "burden_an": {
        "pt": "Número Atribuível (Carga)",
        "en": "Attributable Number (Burden)",
        "es": "Número Atribuible (Carga)",
    },
    "heat_rr_p95": {
        "pt": "RR ao Calor (P95) — DLNM",
        "en": "Heat RR (P95) — DLNM",
        "es": "RR por Calor (P95) — DLNM",
    },
    "max_stratum_rr": {
        "pt": "RR Máximo por Estrato",
        "en": "Maximum Stratum RR",
        "es": "RR Máximo por Estrato",
    },
}

_DEFAULT_LABELS: dict[str, list[str]] = {
    "pt": ["Baixo", "Médio", "Alto"],
    "en": ["Low", "Medium", "High"],
    "es": ["Bajo", "Medio", "Alto"],
}


def _ind_label(code: str, lang: str) -> str:
    entry = _IND_LABELS[code]
    return entry.get(lang, entry["pt"])


def _norm01(x: np.ndarray) -> np.ndarray:
    """Min-max normalize to [0, 1]; returns 0.5 for constant/degenerate input."""
    x = np.asarray(x, dtype=float)
    finite = x[np.isfinite(x)]
    if finite.size == 0:
        return np.full_like(x, 0.5)
    lo, hi = float(np.min(finite)), float(np.max(finite))
    if hi - lo < np.finfo(float).eps:
        return np.full_like(x, 0.5)
    return (x - lo) / (hi - lo)


def _ind_rows(
    entities: list[str],
    quadrant: str,
    ind_code: str,
    ind_label: str,
    norm_scores: np.ndarray | float,
    direction: str = "positive",
    raw: np.ndarray | float | None = None,
) -> pd.DataFrame:
    """Build a set of indicator rows, broadcasting scalars to every entity."""
    n = len(entities)
    norm_arr = np.broadcast_to(np.atleast_1d(np.asarray(norm_scores, dtype=float)), (n,)).copy()
    raw_arr = norm_arr.copy() if raw is None else np.broadcast_to(
        np.atleast_1d(np.asarray(raw, dtype=float)), (n,)
    ).copy()
    return pd.DataFrame(
        {
            "entity": [str(e) for e in entities],
            "quadrant": quadrant,
            "ind_code": ind_code,
            "indicator": ind_label,
            "raw_value": raw_arr,
            "norm_score": np.clip(norm_arr, 0.0, 100.0),
            "direction": direction,
        }
    )


def _resolve_entities(
    vulnerability: dict[str, Any] | None, burden: dict[str, Any] | None, city_col: str
) -> list[str]:
    if vulnerability is not None:
        vt = vulnerability["vi_table"]
        col = vulnerability["meta"].get("city_col") or city_col
        if col not in vt.columns:
            chr_cols = [c for c in vt.columns if vt[c].dtype == object]
            col = chr_cols[0] if chr_cols else vt.columns[0]
        return [str(v) for v in vt[col].tolist()]
    if burden is not None:
        bt = burden["burden_table"]
        if "component" in bt.columns:
            bt = bt[bt["component"] == "total"]
        return [str(v) for v in bt["city"].tolist()]
    return ["overall"]


def sus_mod_swot(
    vulnerability: dict[str, Any] | None = None,
    af: dict[str, Any] | None = None,
    burden: dict[str, Any] | None = None,
    dlnm: dict[str, Any] | None = None,
    sensitivity: dict[str, Any] | None = None,
    score_type: Literal["both", "numeric", "categorical"] = "both",
    breaks: tuple[float, ...] = (33, 66),
    labels: list[str] | None = None,
    city_col: str = "city",
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """SWOT analysis for climate-health surveillance.

    Synthesizes pre-computed climasus model dicts into a Strengths,
    Weaknesses, Opportunities, and Threats (SWOT) framework for
    climate-health risk communication and territorial planning. Each
    quadrant aggregates normalized indicators (0-100 scale) extracted
    from the supplied inputs; quadrant scores are returned both as
    continuous values and as user-defined categorical labels.

    SWOT quadrant indicators:
        - **S** (Strengths — current protective factors): adaptive
          capacity score (VI), low total AF% (af/burden), low burden
          rank.
        - **W** (Weaknesses — current vulnerabilities): sensitivity
          score (VI), heat/cold AF% (af), stratum RR inequality
          (sensitivity).
        - **O** (Opportunities — intervention windows): low VI
          percentile (VI), low current exposure (VI).
        - **T** (Threats — climate-health risks): exposure score + VI
          score (VI), heat RR at P95 (dlnm), attributable number
          (burden), max stratum RR (sensitivity).

    All indicators are normalized to 0-100 within the supplied data. For
    Strength/Opportunity indicators the raw value is oriented so higher
    = better; for Weakness/Threat indicators, higher means worse. The
    quadrant score is the mean of its available indicator scores.

    When *vulnerability* and/or *burden* provide multi-city data, the
    SWOT is computed per city. Single-city inputs (*af*, *dlnm*,
    *sensitivity*) are broadcast to all detected entities and treated
    as shared context.

    Args:
        vulnerability: Dict from ``sus_mod_vulnerability_index()``, or
            ``None``. Provides per-city VI scores, exposure,
            sensitivity, and adaptive-capacity indicators.
        af: Dict from ``sus_mod_af()``, or ``None``. Provides total,
            heat, and cold attributable-fraction percentages.
        burden: Dict from ``sus_mod_burden()``, or ``None``. Provides
            city-level ranked attributable numbers and fractions.
        dlnm: Dict from ``sus_mod_dlnm()``, or ``None``. Provides the
            heat exposure-response RR at the highest available
            percentile (a proxy for P95).
        sensitivity: Dict from ``sus_mod_sensitivity()``, or ``None``.
            Provides stratum-level RR inequality and the maximum hot RR
            across strata.
        score_type: Which score types to compute: ``"numeric"``,
            ``"categorical"``, or ``"both"`` (default).
        breaks: Strictly increasing cut-points in (0, 100) used to
            convert numeric scores into categories. Default ``(33,
            66)`` produces three categories.
        labels: Category labels with length ``len(breaks) + 1``. ``None``
            (default) uses language-appropriate defaults.
        city_col: Column name of the city/entity identifier in
            ``vulnerability["vi_table"]``. Defaults to ``"city"``.
            Overridden by ``vulnerability["meta"]["city_col"]`` when
            available.
        lang: Language for labels: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        Dict (Python analog of R's ``climasus_swot`` object):

        - ``scores`` (``pd.DataFrame``): one row per entity with
          ``entity``, ``S_score``/``W_score``/``O_score``/``T_score``
          (0-100, ``NaN`` when no indicators are available for a
          quadrant), ``n_S``/``n_W``/``n_O``/``n_T`` (indicator
          counts), plus ``S_cat``/``W_cat``/``O_cat``/``T_cat`` when
          *score_type* is ``"categorical"``/``"both"``.
        - ``indicators`` (``pd.DataFrame``, long format): one row per
          entity x quadrant x indicator: ``entity``, ``quadrant``,
          ``ind_code``, ``indicator``, ``raw_value``, ``norm_score``,
          ``direction``.
        - ``meta`` (dict): ``n_entities``, ``n_indicators``,
          ``inputs_used``, ``score_type``, ``breaks``, ``labels``,
          ``lang``, ``call_time``.

    Raises:
        ValueError: If no input is provided, *breaks* is not a strictly
            increasing sequence in (0, 100), or *labels* has the wrong
            length.
        TypeError: If any provided input is not a dict shaped like the
            corresponding ``sus_mod_*()`` return value.

    Examples::

        import climasus4py as cs

        swot = cs.sus_mod_swot(
            vulnerability=vi_result,
            af=af_result,
            burden=burden_result,
            dlnm=dlnm_result,
            score_type="both",
            lang="pt",
        )
        swot["scores"]
        cs.sus_mod_plot_swot(swot, type="matrix", lang="pt")
    """
    if lang not in _MESSAGES:
        lang = "pt"
    msg = _MESSAGES[lang]

    if score_type not in ("both", "numeric", "categorical"):
        raise ValueError("score_type must be one of 'both', 'numeric', 'categorical'.")

    inputs_used: list[str] = []
    if vulnerability is not None:
        if not isinstance(vulnerability, dict) or "vi_table" not in vulnerability:
            raise TypeError(msg["err_not_vi"])
        inputs_used.append("climasus_vi")
    if af is not None:
        if not isinstance(af, dict) or "total" not in af:
            raise TypeError(msg["err_not_af"])
        inputs_used.append("climasus_af")
    if burden is not None:
        if not isinstance(burden, dict) or "burden_table" not in burden:
            raise TypeError(msg["err_not_burden"])
        inputs_used.append("climasus_burden")
    if dlnm is not None:
        if not isinstance(dlnm, dict) or "exposure_response" not in dlnm:
            raise TypeError(msg["err_not_dlnm"])
        inputs_used.append("climasus_dlnm")
    if sensitivity is not None:
        if not isinstance(sensitivity, dict) or "comparison" not in sensitivity:
            raise TypeError(msg["err_not_sensitivity"])
        inputs_used.append("climasus_sensitivity")
    if not inputs_used:
        raise ValueError(msg["err_no_input"])

    breaks_arr = np.asarray(breaks, dtype=float)
    bounded = np.concatenate(([0.0], breaks_arr, [100.0]))
    if breaks_arr.size < 1 or np.any(np.diff(bounded) <= 0):
        raise ValueError(msg["err_bad_breaks"])

    n_cats = breaks_arr.size + 1
    if labels is None:
        base_lbl = _DEFAULT_LABELS.get(lang, _DEFAULT_LABELS["pt"])
        labels = base_lbl if len(base_lbl) == n_cats else [f"Cat{i + 1}" for i in range(n_cats)]
    elif len(labels) != n_cats:
        raise ValueError(msg["err_bad_labels"].format(n_cats=n_cats))

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    entities = _resolve_entities(vulnerability, burden, city_col)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_extract"].format(n_inputs=len(inputs_used)))

    ind_rows: list[pd.DataFrame] = []

    if vulnerability is not None:
        vt = vulnerability["vi_table"]
        vi_col = vulnerability["meta"].get("city_col") or city_col
        city_nm = [str(v) for v in vt[vi_col].tolist()] if vi_col in vt.columns else entities

        ind_rows.append(
            _ind_rows(
                city_nm, "S", "adaptive_capacity_vi", _ind_label("adaptive_capacity_vi", lang),
                vt["adaptive_capacity_score"].to_numpy(dtype=float), "positive",
            )
        )
        ind_rows.append(
            _ind_rows(
                city_nm, "W", "sensitivity_vi", _ind_label("sensitivity_vi", lang),
                vt["sensitivity_score"].to_numpy(dtype=float), "negative",
            )
        )
        vi_pct = vt["vi_percentile"].to_numpy(dtype=float)
        ind_rows.append(
            _ind_rows(
                city_nm, "O", "vi_percentile_inv", _ind_label("vi_percentile_inv", lang),
                100 - vi_pct, "positive", raw=vi_pct,
            )
        )
        exp_score = vt["exposure_score"].to_numpy(dtype=float)
        ind_rows.append(
            _ind_rows(
                city_nm, "O", "exposure_inv", _ind_label("exposure_inv", lang),
                100 - exp_score, "positive", raw=exp_score,
            )
        )
        ind_rows.append(
            _ind_rows(
                city_nm, "T", "exposure_vi", _ind_label("exposure_vi", lang),
                exp_score, "negative",
            )
        )
        ind_rows.append(
            _ind_rows(
                city_nm, "T", "vi_score_threat", _ind_label("vi_score_threat", lang),
                vt["vi_score"].to_numpy(dtype=float), "negative",
            )
        )

    if af is not None:
        ttl = af["total"]
        tot_row = ttl[ttl["component"] == "total"]
        heat_row = ttl[ttl["component"] == "heat"]
        cold_row = ttl[ttl["component"] == "cold"]

        af_total = abs(float(tot_row["af_pct"].iloc[0])) if len(tot_row) else 0.0
        af_heat = abs(float(heat_row["af_pct"].iloc[0])) if len(heat_row) else 0.0
        af_cold = abs(float(cold_row["af_pct"].iloc[0])) if len(cold_row) else 0.0

        ind_rows.append(
            _ind_rows(
                entities, "S", "af_total_inv", _ind_label("af_total_inv", lang),
                max(0.0, 100 - af_total), "positive", raw=af_total,
            )
        )
        ind_rows.append(
            _ind_rows(
                entities, "W", "heat_af_pct", _ind_label("heat_af_pct", lang),
                min(100.0, af_heat), "negative",
            )
        )
        ind_rows.append(
            _ind_rows(
                entities, "W", "cold_af_pct", _ind_label("cold_af_pct", lang),
                min(100.0, af_cold), "negative",
            )
        )

    if burden is not None:
        bt = burden["burden_table"]
        if "component" in bt.columns:
            bt = bt[bt["component"] == "total"]
        cities_b = [str(v) for v in bt["city"].tolist()]
        n_b = len(bt)

        rank_inv = (
            (n_b + 1 - bt["rank"].to_numpy(dtype=float)) / n_b * 100 if n_b > 0 else np.array([])
        )
        an_norm = _norm01(bt["an"].to_numpy(dtype=float)) * 100

        ind_rows.append(
            _ind_rows(
                cities_b, "S", "burden_rank_inv", _ind_label("burden_rank_inv", lang),
                rank_inv, "positive", raw=bt["rank"].to_numpy(dtype=float),
            )
        )
        ind_rows.append(
            _ind_rows(
                cities_b, "T", "burden_an", _ind_label("burden_an", lang),
                an_norm, "negative", raw=bt["an"].to_numpy(dtype=float),
            )
        )

    if dlnm is not None:
        er = dlnm["exposure_response"]
        if er is not None and len(er) > 0:
            rr_95 = float(er.loc[er["pct"].idxmax(), "rr"])
            rr_norm = min(100.0, max(0.0, (rr_95 - 1) * 50))
            ind_rows.append(
                _ind_rows(
                    entities, "T", "heat_rr_p95", _ind_label("heat_rr_p95", lang),
                    rr_norm, "negative", raw=rr_95,
                )
            )

    if sensitivity is not None:
        comp = sensitivity["comparison"]
        if comp is not None and len(comp) > 0:
            hot_rrs = comp["hot_rr"].dropna().to_numpy(dtype=float)

            if hot_rrs.size >= 2:
                rr_ratio = float(np.max(hot_rrs) / max(1e-6, np.min(hot_rrs)))
                rr_ineq = min(100.0, max(0.0, (rr_ratio - 1) * 20))
                ind_rows.append(
                    _ind_rows(
                        entities, "W", "stratum_inequality", _ind_label("stratum_inequality", lang),
                        rr_ineq, "negative", raw=rr_ratio,
                    )
                )

            if hot_rrs.size >= 1:
                max_rr = float(np.max(hot_rrs))
                max_rr_norm = min(100.0, max(0.0, (max_rr - 1) * 50))
                ind_rows.append(
                    _ind_rows(
                        entities, "T", "max_stratum_rr", _ind_label("max_stratum_rr", lang),
                        max_rr_norm, "negative", raw=max_rr,
                    )
                )

    all_inds = pd.concat(ind_rows, ignore_index=True) if ind_rows else pd.DataFrame(
        columns=["entity", "quadrant", "ind_code", "indicator", "raw_value", "norm_score", "direction"]
    )
    n_ind = len(all_inds)

    all_entities = all_inds["entity"].unique().tolist() if n_ind > 0 else []
    n_entities = len(all_entities)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_score"].format(n_entities=n_entities))

    def _quad_score(ent_inds: pd.DataFrame, q: str) -> tuple[float, int]:
        rows = ent_inds[ent_inds["quadrant"] == q]
        if len(rows) == 0:
            return float("nan"), 0
        return float(rows["norm_score"].mean(skipna=True)), len(rows)

    score_rows = []
    for ent in all_entities:
        ent_inds = all_inds[all_inds["entity"] == ent]
        s_score, n_s = _quad_score(ent_inds, "S")
        w_score, n_w = _quad_score(ent_inds, "W")
        o_score, n_o = _quad_score(ent_inds, "O")
        t_score, n_t = _quad_score(ent_inds, "T")
        score_rows.append(
            {
                "entity": ent,
                "S_score": s_score, "n_S": n_s,
                "W_score": w_score, "n_W": n_w,
                "O_score": o_score, "n_O": n_o,
                "T_score": t_score, "n_T": n_t,
            }
        )
    scores_tbl = pd.DataFrame(
        score_rows,
        columns=["entity", "S_score", "n_S", "W_score", "n_W", "O_score", "n_O", "T_score", "n_T"],
    )

    if score_type in ("categorical", "both"):
        cut_edges = np.concatenate(([0.0], breaks_arr, [100.0]))

        def _to_cat(v: pd.Series) -> pd.Series:
            return pd.cut(v, bins=cut_edges, labels=labels, include_lowest=True, right=True).astype(
                object
            )

        scores_tbl["S_cat"] = _to_cat(scores_tbl["S_score"])
        scores_tbl["W_cat"] = _to_cat(scores_tbl["W_score"])
        scores_tbl["O_cat"] = _to_cat(scores_tbl["O_score"])
        scores_tbl["T_cat"] = _to_cat(scores_tbl["T_score"])

    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(n_ent=n_entities, n_ind=n_ind, sources=", ".join(inputs_used))
        )

    return {
        "scores": scores_tbl,
        "indicators": all_inds,
        "meta": {
            "n_entities": n_entities,
            "n_indicators": n_ind,
            "inputs_used": inputs_used,
            "score_type": score_type,
            "breaks": list(breaks),
            "labels": labels,
            "lang": lang,
            "call_time": datetime.now(),
        },
    }
