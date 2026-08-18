"""mod_burden.py — ranked attributable-fraction / excess-count burden table.

Mirrors R: sus_mod_burden.R

Theory: Gasparrini et al. (2017, Lancet Planet Health) — multi-city burden
approach; Gasparrini & Armstrong (2013, Epidemiology) — attributable
fraction.

Not lazy — the R function takes a named list of already-fitted per-city
results (``climasus_af``/``climasus_excess``/``climasus_dlnm`` objects) and
aggregates/ranks them; there is no DuckDB-lazy input to begin with, so this
is pure pandas/NumPy, matching the R source (``dplyr`` on small in-memory
tibbles, never Arrow/DuckDB).

Practical caveat (see IDEIAS.md): in R, ``climasus_af``/``climasus_excess``
objects come from ``sus_mod_af()``/``sus_mod_excess()``, and
``climasus_dlnm`` objects come from ``sus_mod_dlnm()`` — all three are
blocked in climasus4py because they require the R package ``dlnm`` (no
faithful Python port exists, see ``no-port-deps.md``). This module ports
``sus_mod_burden``'s own aggregation/ranking logic faithfully, but Python
callers must supply *pre-computed* AF-shaped or excess-shaped tables
themselves (see the ``fits`` docstring below) — there is no Python producer
for them yet, and the DLNM-to-AF auto-conversion branch cannot run.
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
        "title": "climasus4py — Classificação de Carga de Doença ({n_cities} cidades)",
        "step_validate": "Validando {n_cities} ajuste(s) ({input_type})...",
        "step_rank": "Classificando por '{rank_by}'...",
        "done_af": (
            "Concluido. AN total = {an_total} | FA media = {af_avg}% | "
            "Cidade lider: {top_city}"
        ),
        "done_excess": (
            "Concluido. Excesso total = {exc_total} | Media = {exc_avg}% | "
            "Cidade lider: {top_city}"
        ),
        "fits_not_dict": (
            "'fits' deve ser um dicionário nomeado de objetos climasus_af, "
            "climasus_excess ou climasus_dlnm."
        ),
        "fits_empty": "'fits' não pode ser vazio.",
        "invalid_component": "'component' deve ser 'total', 'heat', 'cold' ou 'all'.",
        "invalid_rank_by": "'rank_by' deve ser 'an', 'af_pct', 'excess' ou 'excess_pct'.",
        "rank_by_not_af": "'rank_by' = '{rank_by}' não é aplicável a entradas AF. Usando 'an'.",
        "rank_by_not_excess": (
            "'rank_by' = '{rank_by}' não é aplicável a entradas de excesso. "
            "Usando 'excess'."
        ),
        "mixed_types": (
            "'fits' contém tipos mistos ({types}). Todos os elementos devem "
            "ser do mesmo tipo."
        ),
        "unsupported_dlnm": (
            "Elemento(s) {bad} parecem ser ajustes DLNM (ou outro objeto não "
            "tabular). O auto-cálculo de FA via sus_mod_af()/sus_mod_dlnm() não "
            "está disponível em climasus4py porque essas funções dependem do "
            "pacote R 'dlnm', sem porte fiel em Python (ver no-port-deps.md). "
            "Forneça diretamente tabelas no formato climasus_af/climasus_excess "
            "(dict com chave 'total' contendo um DataFrame, ou o DataFrame em si)."
        ),
        "invalid_columns": (
            "Elemento(s) {bad} não têm as colunas esperadas de uma tabela "
            "'total' climasus_af (an, af_pct, ...) ou climasus_excess "
            "(excess, excess_pct, ...)."
        ),
        "unsupported_lang": "Idioma não suportado '{lang}'. Usando 'pt'.",
    },
    "en": {
        "title": "climasus4py — Disease Burden Ranking ({n_cities} cities)",
        "step_validate": "Validating {n_cities} fit(s) ({input_type})...",
        "step_rank": "Ranking by '{rank_by}'...",
        "done_af": "Done. Total AN = {an_total} | Mean AF = {af_avg}% | Top city: {top_city}",
        "done_excess": (
            "Done. Total excess = {exc_total} | Mean = {exc_avg}% | "
            "Top city: {top_city}"
        ),
        "fits_not_dict": (
            "'fits' must be a named dict of climasus_af, climasus_excess, "
            "or climasus_dlnm objects."
        ),
        "fits_empty": "'fits' cannot be empty.",
        "invalid_component": "'component' must be 'total', 'heat', 'cold', or 'all'.",
        "invalid_rank_by": "'rank_by' must be 'an', 'af_pct', 'excess', or 'excess_pct'.",
        "rank_by_not_af": "'rank_by' = '{rank_by}' is not applicable for AF inputs. Using 'an'.",
        "rank_by_not_excess": (
            "'rank_by' = '{rank_by}' is not applicable for excess inputs. "
            "Using 'excess'."
        ),
        "mixed_types": (
            "'fits' contains mixed types ({types}). All elements must be "
            "the same type."
        ),
        "unsupported_dlnm": (
            "Element(s) {bad} look like DLNM fits (or another non-tabular "
            "object). Auto-computing AF via sus_mod_af()/sus_mod_dlnm() is not "
            "available in climasus4py because those functions depend on the R "
            "package 'dlnm', which has no faithful Python port (see "
            "no-port-deps.md). Supply pre-computed climasus_af/climasus_excess "
            "-shaped tables instead (a dict with a 'total' key holding a "
            "DataFrame, or the DataFrame itself)."
        ),
        "invalid_columns": (
            "Element(s) {bad} do not have the expected columns of a "
            "climasus_af 'total' table (an, af_pct, ...) or a "
            "climasus_excess one (excess, excess_pct, ...)."
        ),
        "unsupported_lang": "Unsupported language '{lang}'. Using 'pt'.",
    },
    "es": {
        "title": "climasus4py — Clasificación de Carga de Enfermedad ({n_cities} ciudades)",
        "step_validate": "Validando {n_cities} ajuste(s) ({input_type})...",
        "step_rank": "Clasificando por '{rank_by}'...",
        "done_af": (
            "Listo. AN total = {an_total} | FA media = {af_avg}% | "
            "Ciudad principal: {top_city}"
        ),
        "done_excess": (
            "Listo. Exceso total = {exc_total} | Media = {exc_avg}% | "
            "Ciudad principal: {top_city}"
        ),
        "fits_not_dict": (
            "'fits' debe ser un diccionario nombrado de objetos climasus_af, "
            "climasus_excess o climasus_dlnm."
        ),
        "fits_empty": "'fits' no puede estar vacío.",
        "invalid_component": "'component' debe ser 'total', 'heat', 'cold' o 'all'.",
        "invalid_rank_by": "'rank_by' debe ser 'an', 'af_pct', 'excess' o 'excess_pct'.",
        "rank_by_not_af": "'rank_by' = '{rank_by}' no es aplicable para entradas AF. Usando 'an'.",
        "rank_by_not_excess": (
            "'rank_by' = '{rank_by}' no es aplicable para entradas de exceso. "
            "Usando 'excess'."
        ),
        "mixed_types": (
            "'fits' contiene tipos mixtos ({types}). Todos los elementos "
            "deben ser del mismo tipo."
        ),
        "unsupported_dlnm": (
            "Elemento(s) {bad} parecen ser ajustes DLNM (u otro objeto no "
            "tabular). El cálculo automático de FA vía sus_mod_af()/"
            "sus_mod_dlnm() no está disponible en climasus4py porque esas "
            "funciones dependen del paquete R 'dlnm', sin porte fiel en Python "
            "(ver no-port-deps.md). Proporcione directamente tablas en formato "
            "climasus_af/climasus_excess (dict con clave 'total' con un "
            "DataFrame, o el DataFrame en sí)."
        ),
        "invalid_columns": (
            "Elemento(s) {bad} no tienen las columnas esperadas de una tabla "
            "'total' climasus_af (an, af_pct, ...) o climasus_excess "
            "(excess, excess_pct, ...)."
        ),
        "unsupported_lang": "Idioma no soportado '{lang}'. Usando 'pt'.",
    },
}

_AF_COLS = {"an", "af_pct"}
_EXCESS_COLS = {"excess", "excess_pct"}


def sus_mod_burden(
    fits: dict[str, Any],
    component: Literal["total", "heat", "cold", "all"] = "total",
    rank_by: Literal["an", "af_pct", "excess", "excess_pct"] | None = None,
    top_n: int | None = None,
    nsim: int = 0,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Rank disease-burden results across cities or strata.

    Aggregates city-level attributable-fraction or excess mortality/
    morbidity results into a ranked burden table showing each city's
    contribution to the total population burden. Produces a
    concentration curve suitable for Lorenz-style inequality analysis:
    how unevenly burden is distributed across cities.

    Mirrors R's ``sus_mod_burden()``, which accepts ``climasus_af``,
    ``climasus_excess``, or ``climasus_dlnm`` S3 objects (DLNM fits are
    auto-converted to AF via ``sus_mod_af()``). climasus4py has no
    producer for any of those three object types yet — ``sus_mod_af``,
    ``sus_mod_excess``, and ``sus_mod_dlnm`` all depend on the R package
    ``dlnm``, which has no faithful Python port (see the project's
    ``no-port-deps.md``). Consequently:

    - AF-shaped and excess-shaped inputs are supported and computed
      exactly as in R.
    - DLNM-shaped inputs (or any non-tabular object) raise
      ``NotImplementedError`` — the auto-AF conversion branch cannot run.

    Args:
        fits: Named dict where keys become city/stratum labels. Each
            value must be either:

            - A ``pandas.DataFrame`` playing the role of R's
              ``$total`` tibble directly, or
            - A dict with a ``"total"`` key holding such a DataFrame
              (mirroring the ``$total`` element of a ``climasus_af``/
              ``climasus_excess`` list in R; any other keys, e.g.
              ``"daily"``, are ignored since ``sus_mod_burden`` only
              ever reads ``$total``).

            An AF-shaped table needs columns ``component``, ``n_cases``,
            ``an``, ``an_lo``, ``an_hi``, ``af_pct`` (optionally
            ``af_pct_lo``/``af_pct_hi``). An excess-shaped table needs
            ``n_days``, ``observed``, ``expected``, ``excess``,
            ``excess_lo``, ``excess_hi``, ``excess_pct``. All elements
            must be the same shape; mixing AF and excess is not allowed.
        component: Which heat/cold component to display and rank for
            AF inputs: ``"total"`` (default), ``"heat"``, ``"cold"``, or
            ``"all"`` (all three; ranking is always derived from the
            ``"total"`` component). Ignored for excess inputs.
        rank_by: Metric used for ranking. For AF inputs: ``"an"``
            (attributable number, default) or ``"af_pct"``. For excess
            inputs: ``"excess"`` (default) or ``"excess_pct"``. ``None``
            selects the default for the detected input type.
        top_n: Keep only the top-N cities after ranking. ``None``
            (default) retains all cities.
        nsim: Monte Carlo simulations for auto-computing AF from DLNM
            inputs. Kept for signature parity with R; unused in
            climasus4py because the DLNM-to-AF branch is unavailable
            (see above).
        alpha: Significance level for confidence intervals. Kept for
            signature parity with R; unused for the same reason as
            *nsim*. Default ``0.05``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        Dict with the following keys:

        - ``burden_table`` (``pd.DataFrame``): one row per city (three
          rows per city when ``component == "all"``). For AF inputs:
          ``city``, ``component``, ``n_cases``, ``an``, ``an_lo``,
          ``an_hi``, ``af_pct``, ``af_pct_lo``, ``af_pct_hi``, ``rank``,
          ``pct_of_total``. For excess inputs: ``city``, ``n_days``,
          ``observed``, ``expected``, ``excess``, ``excess_lo``,
          ``excess_hi``, ``excess_pct``, ``rank``, ``pct_of_total``.
        - ``concentration`` (``pd.DataFrame``): one row per city with
          ``city``, ``rank``, ``pct_of_total``, ``cumulative_pct``, for
          Lorenz-style concentration analysis. Based on the total
          component only.
        - ``total_burden`` (dict): aggregate statistics — ``an_total``
          and ``af_pct_avg`` for AF inputs, ``excess_total`` and
          ``excess_pct_avg`` for excess inputs; plus ``top_city`` and
          the top city's metric (``top_city_an``/``top_city_excess``).
        - ``meta`` (dict): all parameters used in this call, plus
          ``input_type`` (``"climasus_af"`` or ``"climasus_excess"``,
          matching R's class names) and ``call_time``.

    Raises:
        TypeError: If *fits* is not a dict.
        ValueError: If *fits* is empty, contains mixed AF/excess types,
            contains a table with unrecognized columns, or *component*/
            *rank_by* is invalid.
        NotImplementedError: If any element of *fits* is DLNM-shaped
            (or otherwise non-tabular) — the R auto-AF-from-DLNM branch
            requires ``sus_mod_af()``, which is blocked by the ``dlnm``
            dependency in climasus4py.

    Examples::

        import pandas as pd
        import climasus4py as cs

        af_list = {
            "fortaleza": pd.DataFrame({
                "component": ["total", "heat", "cold"],
                "n_cases": [1000, 1000, 1000],
                "an": [50.0, 30.0, 20.0],
                "an_lo": [40.0, 24.0, 16.0],
                "an_hi": [60.0, 36.0, 24.0],
                "af_pct": [5.0, 3.0, 2.0],
            }),
            "recife": pd.DataFrame({...}),
        }
        burden = cs.sus_mod_burden(af_list, lang="pt")
        burden["burden_table"]
        burden["concentration"]
    """
    if lang not in ("pt", "en", "es"):
        console.print(
            "[yellow]WARNING[/]  " + _MESSAGES["pt"]["unsupported_lang"].format(lang=lang)
        )
        lang = "pt"
    msg = _MESSAGES[lang]

    if not isinstance(fits, dict):
        raise TypeError(msg["fits_not_dict"])
    if len(fits) == 0:
        raise ValueError(msg["fits_empty"])

    city_names = list(fits.keys())
    if any(name == "" for name in city_names):
        fits = {
            (f"city_{i + 1}" if name == "" else name): v
            for i, (name, v) in enumerate(fits.items())
        }
        city_names = list(fits.keys())
    n_cities = len(fits)

    if verbose:
        console.rule("[bold]" + msg["title"].format(n_cities=n_cities) + "[/]")

    # Detect and validate input shape per city.
    kinds: dict[str, str] = {}
    tables: dict[str, pd.DataFrame] = {}
    for nm, fit in fits.items():
        kind, total_df = _classify_fit(fit)
        kinds[nm] = kind
        if total_df is not None:
            tables[nm] = total_df

    dlnm_like = [nm for nm, k in kinds.items() if k == "unsupported"]
    if dlnm_like:
        raise NotImplementedError(msg["unsupported_dlnm"].format(bad=dlnm_like))

    invalid = [nm for nm, k in kinds.items() if k == "invalid_columns"]
    if invalid:
        raise ValueError(msg["invalid_columns"].format(bad=invalid))

    unique_kinds = set(kinds.values())
    if len(unique_kinds) > 1:
        raise ValueError(msg["mixed_types"].format(types=sorted(unique_kinds)))

    detected_kind = unique_kinds.pop()  # "af" or "excess"
    input_type = "climasus_af" if detected_kind == "af" else "climasus_excess"

    # Validate component.
    if component not in ("total", "heat", "cold", "all"):
        raise ValueError(msg["invalid_component"])

    # Resolve rank_by.
    rank_af_ok = ("an", "af_pct")
    rank_exc_ok = ("excess", "excess_pct")
    if rank_by is None:
        rank_by = "an" if detected_kind == "af" else "excess"
    elif rank_by not in (*rank_af_ok, *rank_exc_ok):
        raise ValueError(msg["invalid_rank_by"])
    elif detected_kind == "af" and rank_by in rank_exc_ok:
        console.print("[yellow]WARNING[/]  " + msg["rank_by_not_af"].format(rank_by=rank_by))
        rank_by = "an"
    elif detected_kind == "excess" and rank_by in rank_af_ok:
        console.print(
            "[yellow]WARNING[/]  " + msg["rank_by_not_excess"].format(rank_by=rank_by)
        )
        rank_by = "excess"

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_validate"].format(n_cities=n_cities, input_type=input_type)
        )

    if detected_kind == "af":
        burden_tbl = _burden_from_af(tables, component)
    else:
        burden_tbl = _burden_from_excess(tables)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_rank"].format(rank_by=rank_by))

    burden_tbl, conc_tbl = _burden_rank(burden_tbl, rank_by, component, detected_kind)

    if top_n is not None and isinstance(top_n, (int, float)) and top_n > 0:
        top_n_int = int(top_n)
        keep_n = min(top_n_int, len(conc_tbl))
        keep_cities = conc_tbl["city"].iloc[:keep_n].tolist()
        burden_tbl = burden_tbl[burden_tbl["city"].isin(keep_cities)].reset_index(drop=True)
        conc_tbl = conc_tbl.iloc[:keep_n].reset_index(drop=True)

    if detected_kind == "af":
        if "component" in burden_tbl.columns:
            total_rows = burden_tbl[burden_tbl["component"] == "total"]
        else:
            total_rows = burden_tbl
        an_total = round(total_rows["an"].sum(skipna=True))
        af_avg = round(total_rows["af_pct"].mean(skipna=True), 2)
        top_city = conc_tbl["city"].iloc[0]
        top_an = total_rows.loc[total_rows["city"] == top_city, "an"].iloc[0]

        total_burden = {
            "an_total": an_total,
            "af_pct_avg": af_avg,
            "top_city": top_city,
            "top_city_an": round(top_an),
        }
        if verbose:
            console.print(
                "[green]OK[/]  "
                + msg["done_af"].format(an_total=an_total, af_avg=af_avg, top_city=top_city)
            )
    else:
        exc_total = round(burden_tbl["excess"].sum(skipna=True))
        exc_avg = round(burden_tbl["excess_pct"].mean(skipna=True), 2)
        top_city = conc_tbl["city"].iloc[0]
        top_exc = burden_tbl.loc[burden_tbl["city"] == top_city, "excess"].iloc[0]

        total_burden = {
            "excess_total": exc_total,
            "excess_pct_avg": exc_avg,
            "top_city": top_city,
            "top_city_excess": round(top_exc),
        }
        if verbose:
            console.print(
                "[green]OK[/]  "
                + msg["done_excess"].format(exc_total=exc_total, exc_avg=exc_avg, top_city=top_city)
            )

    meta = {
        "input_type": input_type,
        "component": component,
        "rank_by": rank_by,
        "n_cities": n_cities,
        "top_n": top_n,
        "nsim": nsim,
        "alpha": alpha,
        "call_time": datetime.now(),
    }

    return {
        "burden_table": burden_tbl,
        "concentration": conc_tbl,
        "total_burden": total_burden,
        "meta": meta,
    }


def _classify_fit(fit: Any) -> tuple[str, pd.DataFrame | None]:
    """Classify one ``fits`` element as "af", "excess", "invalid_columns", or "unsupported"."""
    if isinstance(fit, pd.DataFrame):
        total_df = fit
    elif isinstance(fit, dict) and isinstance(fit.get("total"), pd.DataFrame):
        total_df = fit["total"]
    else:
        return "unsupported", None

    cols = set(total_df.columns)
    if cols >= _AF_COLS:
        return "af", total_df
    if cols >= _EXCESS_COLS:
        return "excess", total_df
    return "invalid_columns", None


def _burden_from_af(tables: dict[str, pd.DataFrame], component: str) -> pd.DataFrame:
    """Extract a flat burden table from a dict of AF-shaped 'total' tables."""
    rows_out = []
    for nm, t in tables.items():
        rows = t if component == "all" else t[t["component"] == component]
        rows = rows.reset_index(drop=True)
        rows_out.append(
            pd.DataFrame(
                {
                    "city": nm,
                    "component": rows["component"],
                    "n_cases": rows["n_cases"],
                    "an": rows["an"],
                    "an_lo": rows["an_lo"],
                    "an_hi": rows["an_hi"],
                    "af_pct": rows["af_pct"],
                    "af_pct_lo": rows["af_pct_lo"] if "af_pct_lo" in rows.columns else np.nan,
                    "af_pct_hi": rows["af_pct_hi"] if "af_pct_hi" in rows.columns else np.nan,
                }
            )
        )
    return pd.concat(rows_out, ignore_index=True)


def _burden_from_excess(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Extract a flat burden table from a dict of excess-shaped 'total' tables."""
    rows_out = []
    for nm, tt in tables.items():
        tt = tt.reset_index(drop=True)
        rows_out.append(
            pd.DataFrame(
                {
                    "city": nm,
                    "n_days": tt["n_days"],
                    "observed": tt["observed"],
                    "expected": tt["expected"].round(1),
                    "excess": tt["excess"].round(1),
                    "excess_lo": tt["excess_lo"].round(1),
                    "excess_hi": tt["excess_hi"].round(1),
                    "excess_pct": tt["excess_pct"],
                }
            )
        )
    return pd.concat(rows_out, ignore_index=True)


def _pct_of_total(vals: pd.Series) -> pd.Series:
    """R quirk preserved: ``vals / sum(vals) * 100`` — always sums to 100
    regardless of sign (not ``vals / sum(abs(vals)) * 100``); if the sum is
    ~0, every entry becomes NaN rather than dividing by ~0."""
    s = vals.sum(skipna=True)
    eps = float(np.finfo(float).eps)
    if abs(s) < eps:
        return pd.Series(np.nan, index=vals.index)
    return vals / s * 100


def _burden_rank(
    burden_tbl: pd.DataFrame,
    rank_by: str,
    component: str,
    input_type: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rank burden table and compute the city-level concentration curve.

    For ``component == "all"``, ranking is derived from the "total"
    component rows and spread to all three component rows of the same
    city. The concentration curve is always city-level (one row per
    city) based on the total/primary metric.
    """
    if input_type == "af" and component == "all":
        total_rows = (
            burden_tbl[burden_tbl["component"] == "total"]
            .sort_values(rank_by, ascending=False, kind="stable")
            .reset_index(drop=True)
        )
        total_rows["rank"] = range(1, len(total_rows) + 1)
        total_rows["pct_of_total"] = _pct_of_total(total_rows[rank_by])

        burden_out = burden_tbl.merge(
            total_rows[["city", "rank", "pct_of_total"]], on="city", how="left"
        ).sort_values(["rank", "component"], kind="stable").reset_index(drop=True)

        conc = total_rows[["city", "rank", "pct_of_total"]].copy()
        conc["cumulative_pct"] = conc["pct_of_total"].cumsum()
    else:
        burden_out = (
            burden_tbl.sort_values(rank_by, ascending=False, kind="stable")
            .reset_index(drop=True)
        )
        burden_out["rank"] = range(1, len(burden_out) + 1)
        burden_out["pct_of_total"] = _pct_of_total(burden_out[rank_by])

        conc = burden_out[["city", "rank", "pct_of_total"]].copy()
        conc["cumulative_pct"] = conc["pct_of_total"].cumsum()

    return burden_out, conc
