"""climate_anomaly.py — climate anomalies vs. INMET climatological normals.

Mirrors R: sus_climate_anomaly.R

Not lazy — joins observed station data (from ``sus_climate_inmet``/
``sus_climate_aggregate``) against ``sus_climate_normals`` output and
computes per-variable anomaly columns; both inputs are already small
(station x month/decade), so pandas is the idiomatic fit (matches the
R source, which never routes this through arrow/duckdb either).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

# Observed INMET column -> normals var_code. Expand as normal_meta grows.
_ANOMALY_VAR_MAP: dict[str, str] = {
    "tair_max_c": "t_max",
    "tair_min_c": "t_min",
    "tair_dry_bulb_c": "t_mean_comp",
    "rainfall_mm": "precipitation",
    "rh_mean_porc": "rh_mean",
}

# Aggregation type per INMET column ("mean" or "sum").
_ANOMALY_AGG_TYPE: dict[str, str] = {
    "tair_max_c": "mean",
    "tair_min_c": "mean",
    "tair_dry_bulb_c": "mean",
    "rainfall_mm": "sum",
    "rh_mean_porc": "mean",
}

_PT_MONTHS: dict[int, str] = {
    1: "janeiro", 2: "fevereiro", 3: "marco", 4: "abril",
    5: "maio", 6: "junho", 7: "julho", 8: "agosto",
    9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro",
}
_PT_MONTH_TO_NUM: dict[str, int] = {v: k for k, v in _PT_MONTHS.items()}

_REQUIRED_NORMALS_COLS = ("codigo", "mes", "decada", "var_code", "valor")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Anomalias Climaticas vs. Normais INMET",
        "step_validate": "Validando entradas: {n_obs} obs. | {n_norm} normais...",
        "step_vars": "Variaveis mapeadas ({n_vars}): {vars_str}",
        "step_aggregate": "Agregando observacoes a escala {time_scale}...",
        "step_normals": "Preparando normais (escala {time_scale})...",
        "step_join": "Unindo {n_st} estacao(oes) com normais...",
        "step_compute": "Calculando anomalias (metodo: {method})...",
        "done": (
            "Concluido. {n_rows} periodos | {n_st} estacoes | "
            "{n_vars} variaveis | metodo: {method}"
        ),
        "err_no_station": "Coluna '{col}' nao encontrada em observed. Ajuste station_col.",
        "err_no_date": "Coluna de data '{col}' nao encontrada em observed. Ajuste date_col.",
        "err_missing_norm_cols": "Colunas obrigatorias ausentes em normals: {missing}.",
        "err_no_vars": (
            "Nenhuma variavel mapeavel encontrada. Forneca 'vars' explicitamente "
            "ou verifique o catalogo com sus_climate_normals_meta()."
        ),
        "err_vars_not_in_obs": "Variaveis nao encontradas em observed: {miss}.",
        "err_varcodes_not_in_norm": (
            "var_code nao encontrado em normals: {miss}. Use sus_climate_normals_meta() "
            "para ver os codigos disponiveis."
        ),
        "err_no_join": (
            "Nenhuma correspondencia encontrada apos o join. Verifique se os codigos "
            "de estacao de observed coincidem com 'codigo' em normals."
        ),
        "warn_std_few_years": (
            "Anomalia padronizada: {n_low} estacao/mes com <3 anos de dados; "
            "SD sera impreciso."
        ),
    },
    "en": {
        "title": "Climate Anomalies vs. INMET Normals",
        "step_validate": "Validating inputs: {n_obs} obs. | {n_norm} normals...",
        "step_vars": "Mapped variables ({n_vars}): {vars_str}",
        "step_aggregate": "Aggregating observations to {time_scale} scale...",
        "step_normals": "Preparing normals ({time_scale} scale)...",
        "step_join": "Joining {n_st} station(s) with normals...",
        "step_compute": "Computing anomalies (method: {method})...",
        "done": "Done. {n_rows} periods | {n_st} stations | {n_vars} variables | method: {method}",
        "err_no_station": "Column '{col}' not found in observed. Adjust station_col.",
        "err_no_date": "Date column '{col}' not found in observed. Adjust date_col.",
        "err_missing_norm_cols": "Missing required columns in normals: {missing}.",
        "err_no_vars": (
            "No mappable variables found. Provide 'vars' explicitly or check the "
            "catalogue with sus_climate_normals_meta()."
        ),
        "err_vars_not_in_obs": "Variables not found in observed: {miss}.",
        "err_varcodes_not_in_norm": (
            "var_code not found in normals: {miss}. Use sus_climate_normals_meta() "
            "to see available codes."
        ),
        "err_no_join": (
            "No matches found after join. Verify that station codes in observed "
            "match 'codigo' in normals."
        ),
        "warn_std_few_years": (
            "Standardized anomaly: {n_low} station/month(s) with <3 years of data; "
            "SD will be imprecise."
        ),
    },
    "es": {
        "title": "Anomalias Climaticas vs. Normales INMET",
        "step_validate": "Validando entradas: {n_obs} obs. | {n_norm} normales...",
        "step_vars": "Variables mapeadas ({n_vars}): {vars_str}",
        "step_aggregate": "Agregando observaciones a escala {time_scale}...",
        "step_normals": "Preparando normales (escala {time_scale})...",
        "step_join": "Uniendo {n_st} estacion(es) con normales...",
        "step_compute": "Calculando anomalias (metodo: {method})...",
        "done": (
            "Listo. {n_rows} periodos | {n_st} estaciones | "
            "{n_vars} variables | metodo: {method}"
        ),
        "err_no_station": "Columna '{col}' no encontrada en observed. Ajuste station_col.",
        "err_no_date": "Columna de fecha '{col}' no encontrada en observed. Ajuste date_col.",
        "err_missing_norm_cols": "Columnas requeridas ausentes en normals: {missing}.",
        "err_no_vars": (
            "Ninguna variable mapeable encontrada. Proporcione 'vars' explicitamente "
            "o consulte el catalogo con sus_climate_normals_meta()."
        ),
        "err_vars_not_in_obs": "Variables no encontradas en observed: {miss}.",
        "err_varcodes_not_in_norm": (
            "var_code no encontrado en normals: {miss}. Use sus_climate_normals_meta() "
            "para ver los codigos disponibles."
        ),
        "err_no_join": (
            "Sin coincidencias tras el join. Verifique que los codigos de estacion "
            "de observed coincidan con 'codigo' en normals."
        ),
        "warn_std_few_years": (
            "Anomalia estandarizada: {n_low} estacion/mes con <3 anos de datos; "
            "SD sera impreciso."
        ),
    },
}


def sus_climate_anomaly(
    observed: duckdb.DuckDBPyRelation | pd.DataFrame,
    normals: pd.DataFrame,
    vars: dict[str, str] | None = None,
    method: Literal["absolute", "relative", "standardized", "all"] = "absolute",
    time_scale: Literal["monthly", "decadal"] = "monthly",
    station_col: str = "station_code",
    date_col: str = "date",
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Compute climate anomalies vs. INMET climatological normals.

    Compares observed station data (from ``sus_climate_inmet`` or
    ``sus_climate_aggregate``) against 30-year climatological normals
    (from ``sus_climate_normals``). Three anomaly types are supported:
    absolute (``obs - normal``), relative
    (``(obs - normal) / |normal| * 100%``), and standardized z-score
    (``(obs - normal) / sd_obs``).

    Args:
        observed: Station data with *station_col*, *date_col*, and at
            least one column covered by *vars* (or the built-in
            auto-detect map). A lazy ``DuckDBPyRelation`` is
            materialised (no ``UserWarning`` — the R source itself
            eagerly collects Arrow inputs here too).
        normals: Output of ``sus_climate_normals`` — must contain
            ``codigo``, ``mes``, ``decada``, ``var_code``, ``valor``.
        vars: Mapping of observed column name -> normals ``var_code``
            (e.g. ``{"tair_max_c": "t_max"}``). ``None`` (default)
            auto-detects using the built-in INMET column map.
        method: ``"absolute"`` (default), ``"relative"``,
            ``"standardized"``, or ``"all"``.
        time_scale: ``"monthly"`` (default) averages normals across
            the three decades of each month; ``"decadal"`` preserves
            the 10-day decade structure.
        station_col: Station identifier column name in *observed*.
        date_col: Date/datetime column name in *observed*.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        One row per station x year x month (or x decade). Columns
        ``{station_col}``, ``year``, ``month_num``, ``month_name``,
        optionally ``decade_num``, then per variable: ``{v}_obs``,
        ``{v}_normal``, ``{v}_anomaly``, and (depending on *method*)
        ``{v}_anomaly_pct`` / ``{v}_anomaly_std``. Metadata in
        ``df.attrs["sus_meta"]`` (``stage="climate"``, ``type="anomaly"``).

    Raises:
        ValueError: If required columns are missing, no variable can
            be mapped, or the join produces zero matching rows.

    Examples::

        import climasus4py as cs

        obs = cs.sus_climate_inmet(years=range(2018, 2024), uf="RJ")
        norm = cs.sus_climate_normals(
            period="1991-2020",
            target_var=["t_max", "t_min", "t_mean_comp", "precipitation"],
        )
        anomalies = cs.sus_climate_anomaly(obs, norm)
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    if isinstance(observed, duckdb.DuckDBPyRelation):
        observed = observed.df()

    if station_col not in observed.columns:
        raise ValueError(msg["err_no_station"].format(col=station_col))
    if date_col not in observed.columns:
        raise ValueError(msg["err_no_date"].format(col=date_col))

    missing_norm_cols = [c for c in _REQUIRED_NORMALS_COLS if c not in normals.columns]
    if missing_norm_cols:
        raise ValueError(msg["err_missing_norm_cols"].format(missing=missing_norm_cols))

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_validate"].format(n_obs=len(observed), n_norm=len(normals))
        )

    # -- Resolve variable mapping ------------------------------------------
    norm_codes = set(normals["var_code"].unique())
    if vars is None:
        available = {
            k: v for k, v in _ANOMALY_VAR_MAP.items()
            if k in observed.columns and v in norm_codes
        }
        if not available:
            raise ValueError(msg["err_no_vars"])
        resolved_vars = available
    else:
        miss_obs = [k for k in vars if k not in observed.columns]
        if miss_obs:
            raise ValueError(msg["err_vars_not_in_obs"].format(miss=miss_obs))
        miss_norm = [v for v in vars.values() if v not in norm_codes]
        if miss_norm:
            raise ValueError(msg["err_varcodes_not_in_norm"].format(miss=miss_norm))
        resolved_vars = dict(vars)

    if verbose:
        vars_str = ", ".join(f"{k}->{v}" for k, v in resolved_vars.items())
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_vars"].format(n_vars=len(resolved_vars), vars_str=vars_str)
        )

    # -- Aggregate observed to target time scale ---------------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_aggregate"].format(time_scale=time_scale)
        )
    obs_agg = _aggregate_observed(observed, resolved_vars, station_col, date_col, time_scale)

    # -- Prepare normals as wide table --------------------------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_normals"].format(time_scale=time_scale)
        )
    norm_wide = _prepare_normals(normals, resolved_vars, time_scale)
    norm_wide = norm_wide.rename(columns={"codigo": station_col})

    # -- Join ----------------------------------------------------------------
    n_st = obs_agg[station_col].nunique()
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_join"].format(n_st=n_st))

    join_cols = (
        [station_col, "month_num"]
        if time_scale == "monthly"
        else [station_col, "month_num", "decade_num"]
    )
    joined = obs_agg.merge(norm_wide, on=join_cols, how="left")
    if joined.empty:
        raise ValueError(msg["err_no_join"])

    # -- Compute anomaly columns ---------------------------------------------
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_compute"].format(method=method))
    result = _compute_anomalies(joined, resolved_vars, method, station_col, msg, verbose)

    # -- Finishing touches -----------------------------------------------------
    result["month_name"] = result["month_num"].map(_PT_MONTHS)
    result = result.rename(columns={"year_val": "year"})

    id_cols = [
        c for c in (station_col, "year", "month_num", "month_name", "decade_num")
        if c in result.columns
    ]
    other_cols = [c for c in result.columns if c not in id_cols]
    result = result[id_cols + other_cols]

    normal_period = normals.attrs.get("sus_meta", {}).get("period")
    now = datetime.now()
    result.attrs["sus_meta"] = {
        "stage": "climate",
        "type": "anomaly",
        "method": method,
        "time_scale": time_scale,
        "normal_period": normal_period,
        "n_stations": int(result[station_col].nunique(dropna=True)),
        "n_observations": len(result),
        "n_variables": len(resolved_vars),
        "var_map": resolved_vars,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Climate anomalies — "
            f"method: {method}; scale: {time_scale}; "
            f"normal period: {normal_period or 'unknown'}; "
            f"vars: {', '.join(resolved_vars)}"
        ],
    }

    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(
                n_rows=len(result), n_st=n_st, n_vars=len(resolved_vars), method=method
            )
        )

    return result


def _aggregate_observed(
    df: pd.DataFrame,
    vars: dict[str, str],
    station_col: str,
    date_col: str,
    time_scale: str,
) -> pd.DataFrame:
    """Aggregate observed data to monthly or decadal level."""
    df2 = df.copy()
    date = pd.to_datetime(df2[date_col])
    df2["month_num"] = date.dt.month
    df2["year_val"] = date.dt.year

    group_cols = [station_col, "year_val", "month_num"]
    if time_scale == "decadal":
        day_num = date.dt.day
        df2["decade_num"] = np.where(day_num <= 10, 1, np.where(day_num <= 20, 2, 3))
        group_cols.append("decade_num")

    obs_cols = list(vars.keys())
    sum_vars = [c for c in obs_cols if _ANOMALY_AGG_TYPE.get(c) == "sum"]
    mean_vars = [c for c in obs_cols if c not in sum_vars]

    agg_map = {c: "mean" for c in mean_vars if c in df2.columns}
    agg_map.update({c: "sum" for c in sum_vars if c in df2.columns})

    return df2.groupby(group_cols, as_index=False).agg(agg_map)


def _prepare_normals(
    normals: pd.DataFrame, vars: dict[str, str], time_scale: str
) -> pd.DataFrame:
    """Prepare normals as a wide table ready for joining.

    Monthly: averages across the 3 decades of each station x month.
    Decadal: keeps the three 10-day rows per month. Returns a wide
    table with one ``{inmet_col}_normal`` column per mapped variable.
    """
    norm_codes = set(vars.values())
    rev_map = {code: col for col, code in vars.items()}

    norm_sub = normals[normals["var_code"].isin(norm_codes)].copy()
    norm_sub["month_num"] = norm_sub["mes"].map(_PT_MONTH_TO_NUM)
    norm_sub["col_name"] = norm_sub["var_code"].map(rev_map) + "_normal"

    if time_scale == "monthly":
        agg = (
            norm_sub.groupby(["codigo", "month_num", "col_name"], as_index=False)["valor"]
            .mean()
        )
        return agg.pivot(
            index=["codigo", "month_num"], columns="col_name", values="valor"
        ).reset_index()

    norm_sub["decade_num"] = norm_sub["decada"].astype(int)
    return norm_sub.pivot_table(
        index=["codigo", "month_num", "decade_num"],
        columns="col_name",
        values="valor",
        aggfunc="mean",
    ).reset_index()


def _compute_anomalies(
    df: pd.DataFrame,
    vars: dict[str, str],
    method: str,
    station_col: str,
    msg: dict[str, str],
    verbose: bool,
) -> pd.DataFrame:
    """Compute anomaly columns in-place on the joined table."""
    df = df.copy()

    sd_lookup: pd.DataFrame | None = None
    if method in ("standardized", "all"):
        rows = []
        for v in vars:
            if v not in df.columns:
                continue
            g = (
                df.groupby([station_col, "month_num"], as_index=False)
                .agg(sd_obs=(v, "std"), n_years=("year_val", "nunique"))
            )
            g["var_col"] = v
            rows.append(g)
        sd_lookup = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

        if not sd_lookup.empty:
            n_low = int((sd_lookup["n_years"] < 3).sum())
            if n_low > 0 and verbose:
                console.print(
                    "[yellow]WARN[/]  " + msg["warn_std_few_years"].format(n_low=n_low)
                )

    for v in vars:
        obs_col = v
        normal_col = f"{v}_normal"
        if obs_col not in df.columns or normal_col not in df.columns:
            continue

        obs_vals = df[obs_col].astype(float)
        normal_vals = df[normal_col].astype(float)

        df = df.rename(columns={obs_col: f"{v}_obs"})
        df[f"{v}_anomaly"] = obs_vals - normal_vals

        if method in ("relative", "all"):
            df[f"{v}_anomaly_pct"] = np.where(
                normal_vals.abs() > 1e-9,
                (obs_vals - normal_vals) / normal_vals.abs() * 100,
                np.nan,
            )

        if method in ("standardized", "all") and sd_lookup is not None and not sd_lookup.empty:
            v_sd = sd_lookup.loc[
                sd_lookup["var_col"] == v, [station_col, "month_num", "sd_obs"]
            ]
            df = df.merge(v_sd, on=[station_col, "month_num"], how="left")
            sd_obs = df["sd_obs"]
            df[f"{v}_anomaly_std"] = np.where(
                sd_obs.notna() & (sd_obs > 1e-9),
                df[f"{v}_anomaly"] / sd_obs,
                np.nan,
            )
            df = df.drop(columns=["sd_obs"])

    return df
