"""climate_heatwaves.py — Heatwave detection using multiple standard methodologies.

Mirrors R: sus_climate_compute_heatwaves.R

Not lazy — heatwave detection is a per-station, row-order-dependent
gaps-and-islands (run-length) problem with a 366-day circular rolling
percentile baseline; there is no natural DuckDB SQL expression for it
(unlike ``climate_indicators.py``'s single-threshold run-length window
functions). Accepts a ``DuckDBPyRelation`` or ``pd.DataFrame``; a relation
is materialised with a ``UserWarning`` (same "legacy path" precedent as
``climate_spi.py`` / ``climate_spei.py``).

Implemented methods (mirroring the R docstring exactly):

  WHO   : Tmax > P90(Tmax) for >= N consecutive days (default N=3).
          Threshold smoothed over a 15-day rolling window by
          calendar day-of-year. Reference: WMO & WHO (2015).
  WMO   : Tmax > P90(Tmax) AND Tmin > P90(Tmin) for >= N days (default
          N=5). Reference: Perkins & Alexander (2013).
  INMET : Tmax > mean(Tmax_hist) + 5 degC for >= N days (default N=5).
          Reference: INMET (2009); MCTI/Gov.br (2025).
  EHF   : Excess Heat Factor: EHF = EHIsig * max(1, EHIacc) where
          EHIsig = T3 - P95(Tmean) and EHIacc = T3 - T30.
          Reference: Nairn & Fawcett (2015).
  UTCI  : UTCImax > P90(UTCImax) for >= N days (default N=3).
  WBGT  : WBGTmax > P90(WBGTmax) for >= N days (default N=3).
  HI    : HImax > P90(HImax) for >= N days (default N=3).

References:
  - Perkins, S.E. & Alexander, L.V. (2013). On the measurement of heat
    waves. Journal of Climate, 26(13), 4500-4517.
  - WMO & WHO (2015). Heatwaves and Health: Guidance on Warning-System
    Development. Geneva.
  - Nairn, J.R. & Fawcett, R.J.B. (2015). The Excess Heat Factor.
    Int. J. Environ. Res. Public Health, 12(1), 227-253.
  - Broede, P. et al. (2012). Deriving the operational procedure for
    UTCI. Int. J. Biometeorology, 56(3), 481-494.
  - INMET (2009). Normais Climatologicas do Brasil 1961-1990. Brasilia.
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Literal

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

ALL_METHODS: tuple[str, ...] = ("WHO", "WMO", "INMET", "EHF", "UTCI", "WBGT", "HI")

_DEFAULT_MIN_DURATION: dict[str, int] = {
    "WHO": 3,
    "WMO": 5,
    "INMET": 5,
    "EHF": 3,
    "UTCI": 3,
    "WBGT": 3,
    "HI": 3,
}

_METHOD_COL_MAP: dict[str, str] = {"UTCI": "utci_c", "WBGT": "wbgt_c", "HI": "hi_c"}

_TEMP_ANY_COLS = (
    "tair_dry_bulb_c",
    "tair_max_c",
    "tair_min_c",
    "utci_c",
    "wbgt_c",
    "hi_c",
)

_METHOD_TEMP_COL = {
    "WHO": "tmax",
    "WMO": "tmax",
    "INMET": "tmax",
    "EHF": "tmean",
    "UTCI": "utci_max",
    "WBGT": "wbgt_max",
    "HI": "hi_max",
}
_METHOD_REF_COL = {
    "WHO": "tmax_p",
    "WMO": "tmax_p",
    "INMET": "tmax_hist",
    "EHF": "tmean",
    "UTCI": "utci_p",
    "WBGT": "wbgt_p",
    "HI": "hi_p",
}

_STATION_META_COLS = (
    "region",
    "federal_unit",
    "station_name",
    "zona_climatica",
    "latitude",
    "longitude",
)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Detectando ondas de calor",
        "step_validate": "Validando entrada...",
        "step_daily": "Agregando dados horarios para escala diaria...",
        "step_baseline": "Calculando limiares historicos (baseline)...",
        "step_detect": "Detectando ondas de calor por metodo...",
        "step_events": "Extraindo e classificando eventos...",
        "step_summary": "Gerando resumo anual...",
        "done": "Deteccao concluida: {n_ev} evento(s) em {n_st} estacao(oes).",
        "err_no_cols": (
            "Nenhuma coluna de temperatura encontrada. Esperado: "
            "tair_dry_bulb_c, tair_max_c, tair_min_c, utci_c, wbgt_c ou hi_c."
        ),
        "err_required_cols": "Colunas obrigatorias ausentes: {missing_cols}.",
        "err_baseline_empty": "Nenhum dado disponivel no periodo baseline especificado.",
        "err_date_range": "Intervalo de datas invalido. Verifique a coluna de datas.",
        "err_invalid_method": "Metodo(s) invalido(s): {bad}. Esperado um de: {valid}.",
        "warn_method_missing": (
            "Metodo '{m}' solicitado mas coluna '{col}' nao encontrada. Metodo ignorado."
        ),
        "warn_short_series": (
            "Serie temporal curta ({n_days} dias). Resultados podem ser pouco robustos."
        ),
        "warn_short_baseline": (
            "Periodo baseline curto ({n_years} anos). Recomenda-se ao menos 20 anos."
        ),
        "warn_na_temp": (
            "{n_na} valor(es) NA em colunas de temperatura. Use sus_climate_fill_gap() antes."
        ),
        "info_baseline": "Baseline: {start} a {end} ({n_years} ano(s)).",
        "materialize_warning": (
            "sus_climate_compute_heatwaves: a DuckDBPyRelation de entrada esta sendo "
            "materializada para o calculo com pandas/numpy — este calculo nao e "
            "expressavel em SQL lazy (baseline circular por dia-do-ano e deteccao "
            "de eventos por gaps-and-islands por estacao)."
        ),
    },
    "en": {
        "title": "Detecting heatwaves",
        "step_validate": "Validating input...",
        "step_daily": "Aggregating hourly data to daily scale...",
        "step_baseline": "Computing historical thresholds (baseline)...",
        "step_detect": "Detecting heatwaves by method...",
        "step_events": "Extracting and classifying events...",
        "step_summary": "Generating annual summary...",
        "done": "Detection complete: {n_ev} event(s) across {n_st} station(s).",
        "err_no_cols": (
            "No temperature column found. Expected: tair_dry_bulb_c, "
            "tair_max_c, tair_min_c, utci_c, wbgt_c, or hi_c."
        ),
        "err_required_cols": "Missing required columns: {missing_cols}.",
        "err_baseline_empty": "No data available in the specified baseline period.",
        "err_date_range": "Invalid date range. Check your date column.",
        "err_invalid_method": "Invalid method(s): {bad}. Expected one of: {valid}.",
        "warn_method_missing": (
            "Method '{m}' requested but column '{col}' not found. Method skipped."
        ),
        "warn_short_series": "Short time series ({n_days} days). Results may not be robust.",
        "warn_short_baseline": (
            "Short baseline period ({n_years} years). At least 20 years recommended."
        ),
        "warn_na_temp": (
            "{n_na} NA value(s) in temperature columns. Use sus_climate_fill_gap() first."
        ),
        "info_baseline": "Baseline: {start} to {end} ({n_years} year(s)).",
        "materialize_warning": (
            "sus_climate_compute_heatwaves: the input DuckDBPyRelation is being "
            "materialised for the pandas/numpy computation — this cannot be "
            "expressed as lazy SQL (circular day-of-year baseline and "
            "per-station gaps-and-islands event detection)."
        ),
    },
    "es": {
        "title": "Detectando olas de calor",
        "step_validate": "Validando entrada...",
        "step_daily": "Agregando datos horarios a escala diaria...",
        "step_baseline": "Calculando umbrales historicos (linea base)...",
        "step_detect": "Detectando olas de calor por metodo...",
        "step_events": "Extrayendo y clasificando eventos...",
        "step_summary": "Generando resumen anual...",
        "done": "Deteccion completada: {n_ev} evento(s) en {n_st} estacion(es).",
        "err_no_cols": (
            "No se encontro ninguna columna de temperatura. Esperado: "
            "tair_dry_bulb_c, tair_max_c, tair_min_c, utci_c, wbgt_c o hi_c."
        ),
        "err_required_cols": "Faltan columnas requeridas: {missing_cols}.",
        "err_baseline_empty": "No hay datos disponibles en el periodo baseline especificado.",
        "err_date_range": "Rango de fechas invalido. Verifique su columna de fechas.",
        "err_invalid_method": "Metodo(s) invalido(s): {bad}. Esperado uno de: {valid}.",
        "warn_method_missing": (
            "Metodo '{m}' solicitado pero no se encontro la columna '{col}'. Metodo omitido."
        ),
        "warn_short_series": (
            "Serie temporal corta ({n_days} dias). Los resultados pueden no ser robustos."
        ),
        "warn_short_baseline": (
            "Periodo baseline corto ({n_years} anos). Se recomiendan al menos 20 anos."
        ),
        "warn_na_temp": (
            "{n_na} valor(es) NA en columnas de temperatura. Use sus_climate_fill_gap() primero."
        ),
        "info_baseline": "Baseline: {start} a {end} ({n_years} ano(s)).",
        "materialize_warning": (
            "sus_climate_compute_heatwaves: la DuckDBPyRelation de entrada se esta "
            "materializando para el calculo con pandas/numpy — este calculo no es "
            "expresable en SQL lazy (linea base circular por dia-del-ano y "
            "deteccion de eventos por gaps-and-islands por estacion)."
        ),
    },
}


def sus_climate_compute_heatwaves(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    method: str | list[str] | tuple[str, ...] = ALL_METHODS,
    baseline_start: str | None = None,
    baseline_end: str | None = None,
    percentile: float = 90,
    min_duration: int | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, pd.DataFrame]:
    """Detect heatwaves using multiple standard methodologies.

    Applies up to seven heatwave detection methods (WHO, WMO, INMET, EHF,
    UTCI, WBGT, HI) to hourly/sub-daily station data. Aggregates to daily
    scale, computes smoothed historical percentile thresholds from a
    reference baseline period, identifies heatwave days and discrete
    events per method, and returns three tables: per-event, daily-flag,
    and annual-summary — the Python analog of R's ``climasus_hw`` named
    list (returned here as a plain ``dict``, since Python has no
    ``attr()``-tagged-list equivalent).

    See the module docstring for the exact definition of each method.

    Args:
        df: Sub-daily (hourly) station data with columns ``date`` and
            ``station_code``. Temperature columns ``tair_max_c``,
            ``tair_min_c``, ``tair_dry_bulb_c`` are required for
            WHO/WMO/INMET/EHF; ``utci_c``, ``wbgt_c``, ``hi_c`` are
            required for UTCI/WBGT/HI (e.g. from
            ``sus_climate_compute_indicators``). A lazy
            ``DuckDBPyRelation`` is materialised (with a
            ``UserWarning``) since detection is not expressible as
            lazy SQL.
        method: One or more of ``"WHO"``, ``"WMO"``, ``"INMET"``,
            ``"EHF"``, ``"UTCI"``, ``"WBGT"``, ``"HI"``. Pass ``"all"``
            to run every method. Methods whose required column is
            absent emit a warning and are skipped (their ``hw_<method>``
            column becomes all-``NA``).
        baseline_start: Date string (e.g. ``"1981-01-01"``). Start of
            the reference period for percentile thresholds. ``None``
            uses the full series.
        baseline_end: Date string. End of the reference period, or
            ``None`` for the full series.
        percentile: Percentile (0-100, default ``90``) used for WHO,
            WMO, UTCI, WBGT, and HI threshold calculations.
        min_duration: Minimum consecutive days to qualify as a
            heatwave, or ``None`` to use method defaults: WHO=3, WMO=5,
            INMET=5, EHF=3, UTCI=3, WBGT=3, HI=3.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A ``dict`` with keys:

        - ``"events"``: one row per detected event — ``event_id``,
          ``station_code``, ``method``, ``start_date``, ``end_date``,
          ``duration_days``, ``temp_mean``, ``temp_peak``,
          ``anomaly_mean``, ``anomaly_cumulative``, ``severity_index``,
          ``ehf_peak``, ``ehf_mean``, ``intensity_class``, plus station
          metadata.
        - ``"daily"``: daily aggregated data with boolean flag columns
          ``hw_<method>`` and ``hw_any``.
        - ``"summary"``: annual summary by station and method —
          ``year``, ``n_events``, ``total_days_hw``, ``mean_duration``,
          ``max_duration``, ``mean_intensity``, ``max_intensity``,
          ``mean_anomaly``, ``severity_total``.

        Each of the three DataFrames carries the same metadata in its
        ``.attrs["sus_meta"]`` (``stage="climate"``, ``type="heatwaves"``,
        ``history=[...]``) — same convention as ``sus_climate_normals``/
        ``sus_climate_compute_spi`` — the Python analog of R's
        ``attr(result, "sus_meta")`` on the whole list.

    Raises:
        ValueError: If required columns are missing, no temperature
            column is found, *method* contains an unknown value, the
            baseline period yields zero rows, or the date range is
            invalid.

    Examples::

        import climasus4py as cs

        hw = cs.sus_climate_compute_heatwaves(
            df_indicators,
            method=["WHO", "INMET", "EHF"],
            baseline_start="2000-01-01",
            baseline_end="2020-12-31",
        )
        hw["events"]
        hw["summary"]
        hw["daily"][hw["daily"]["hw_any"]]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    else:
        data = df.copy()

    # Expand "all" / normalise to a list
    if isinstance(method, str):
        method_list = ALL_METHODS if method == "all" else [method]
    else:
        method_list = list(method)
        if len(method_list) == 1 and method_list[0] == "all":
            method_list = list(ALL_METHODS)

    bad = sorted(set(method_list) - set(ALL_METHODS))
    if bad:
        raise ValueError(
            msg["err_invalid_method"].format(bad=bad, valid=list(ALL_METHODS))
        )

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["step_validate"])
    data = _hw_validate(data, method_list, msg)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_daily"])
    daily = _hw_aggregate_daily(data)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_baseline"])
    baseline_start_ts = pd.Timestamp(baseline_start) if baseline_start is not None else None
    baseline_end_ts = pd.Timestamp(baseline_end) if baseline_end is not None else None
    baseline = _hw_compute_baseline(
        daily, baseline_start_ts, baseline_end_ts, percentile, msg, verbose
    )
    daily = daily.merge(baseline, on=["station_code", "yday"], how="left")

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_detect"])
    daily = _hw_apply_all_methods(daily, method_list, min_duration)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_events"])
    events = _hw_extract_events(daily, method_list)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_summary"])
    summary_tbl = _hw_build_summary(events, method_list)

    n_ev = len(events)
    n_st = events["station_code"].nunique() if n_ev else 0
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["done"].format(n_ev=n_ev, n_st=n_st)
        )

    now = datetime.now()
    ref_str = (
        f"{baseline_start_ts.date() if baseline_start_ts is not None else 'full'}/"
        f"{baseline_end_ts.date() if baseline_end_ts is not None else 'full'}"
    )
    sus_meta = {
        "stage": "climate",
        "type": "heatwaves",
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_climate_compute_heatwaves(): methods={','.join(method_list)}; "
            f"{n_ev} event(s); {n_st} station(s); baseline={ref_str}"
        ],
    }
    events.attrs["sus_meta"] = sus_meta
    daily.attrs["sus_meta"] = sus_meta
    summary_tbl.attrs["sus_meta"] = sus_meta

    return {"events": events, "daily": daily, "summary": summary_tbl}


# =============================================================================
# 1. COLUMN VALIDATION
# =============================================================================


def _hw_validate(
    data: pd.DataFrame, method_list: list[str], msg: dict[str, str]
) -> pd.DataFrame:
    """Validate required/available columns; warn on missing pieces."""
    required = ("date", "station_code")
    missing_cols = [c for c in required if c not in data.columns]
    if missing_cols:
        raise ValueError(msg["err_required_cols"].format(missing_cols=missing_cols))

    temp_any = [c for c in _TEMP_ANY_COLS if c in data.columns]
    if not temp_any:
        raise ValueError(msg["err_no_cols"])

    for m in method_list:
        col = _METHOD_COL_MAP.get(m)
        if col is not None and col not in data.columns:
            warnings.warn(
                msg["warn_method_missing"].format(m=m, col=col), UserWarning, stacklevel=3
            )

    n_na = int(data[temp_any].isna().sum().sum())
    if n_na > 0:
        warnings.warn(msg["warn_na_temp"].format(n_na=n_na), UserWarning, stacklevel=3)

    data = data.copy()
    data["date"] = pd.to_datetime(data["date"])

    date_min, date_max = data["date"].dt.normalize().min(), data["date"].dt.normalize().max()
    if pd.isna(date_min) or pd.isna(date_max):
        raise ValueError(msg["err_date_range"])
    n_days = (date_max - date_min).days
    if n_days < 365:
        warnings.warn(msg["warn_short_series"].format(n_days=n_days), UserWarning, stacklevel=3)

    return data


# =============================================================================
# 2. DAILY AGGREGATION
# =============================================================================


def _hw_aggregate_daily(data: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sub-daily rows to one row per station x calendar day."""
    def has(col: str) -> bool:
        return col in data.columns

    data = data.copy()
    data["date_day"] = data["date"].dt.normalize()

    agg: dict[str, pd.Series] = {}

    if has("tair_max_c"):
        agg["tmax"] = data.groupby(["station_code", "date_day"])["tair_max_c"].max()
    elif has("tair_dry_bulb_c"):
        agg["tmax"] = data.groupby(["station_code", "date_day"])["tair_dry_bulb_c"].max()

    if has("tair_min_c"):
        agg["tmin"] = data.groupby(["station_code", "date_day"])["tair_min_c"].min()
    elif has("tair_dry_bulb_c"):
        agg["tmin"] = data.groupby(["station_code", "date_day"])["tair_dry_bulb_c"].min()

    if has("tair_dry_bulb_c"):
        agg["tmean"] = data.groupby(["station_code", "date_day"])["tair_dry_bulb_c"].mean()
    elif has("tair_max_c") and has("tair_min_c"):
        gmax = data.groupby(["station_code", "date_day"])["tair_max_c"].mean()
        gmin = data.groupby(["station_code", "date_day"])["tair_min_c"].mean()
        agg["tmean"] = (gmax + gmin) / 2.0

    for src, (mx, mn) in {
        "utci_c": ("utci_max", "utci_mean"),
        "wbgt_c": ("wbgt_max", "wbgt_mean"),
        "hi_c": ("hi_max", "hi_mean"),
    }.items():
        if has(src):
            g = data.groupby(["station_code", "date_day"])[src]
            agg[mx] = g.max()
            agg[mn] = g.mean()
    if has("pet_c"):
        agg["pet_max"] = data.groupby(["station_code", "date_day"])["pet_c"].max()

    for src, out in {
        "wbgt_c_flag_extreme": "n_hours_extreme_wbgt",
        "utci_c_flag_extreme": "n_hours_extreme_utci",
        "hi_c_flag_extreme": "n_hours_extreme_hi",
    }.items():
        if has(src):
            agg[out] = data.groupby(["station_code", "date_day"])[src].sum()

    for col in _STATION_META_COLS:
        if has(col):
            agg[col] = data.groupby(["station_code", "date_day"])[col].first()

    daily = pd.concat(agg, axis=1).reset_index() if agg else data[["station_code"]].iloc[0:0]

    # Replace inf produced by max/min-of-all-NA groups with NaN (mirrors R's
    # `ifelse(is.infinite(.x), NA, .x)` cleanup across double columns).
    numeric_cols = daily.select_dtypes(include=["float64", "float32"]).columns
    daily[numeric_cols] = daily[numeric_cols].replace([np.inf, -np.inf], np.nan)

    daily["yday"] = daily["date_day"].dt.dayofyear
    daily = daily.sort_values(["station_code", "date_day"]).reset_index(drop=True)
    daily = daily[daily["date_day"].notna()].reset_index(drop=True)
    return daily


# =============================================================================
# 3. BASELINE / THRESHOLDS
# =============================================================================


def _hw_compute_baseline(
    daily: pd.DataFrame,
    baseline_start: pd.Timestamp | None,
    baseline_end: pd.Timestamp | None,
    percentile: float,
    msg: dict[str, str],
    verbose: bool,
) -> pd.DataFrame:
    """Compute a smoothed, circular day-of-year baseline per station.

    For each station and each calendar day-of-year (1..366), pools all
    rows whose ``yday`` falls in a +/-15 day circular window (wrapping
    across the 366/1 boundary) and computes percentile thresholds
    (``tmax_p``, ``tmin_p``, ``utci_p``, ``wbgt_p``, ``hi_p`` — require
    >= 10 non-NA values, else NaN) and historical means (``tmean_hist``,
    ``tmax_hist`` — no minimum-count guard, mirrors the R source as-is).
    """
    ref = daily
    if baseline_start is not None:
        ref = ref[ref["date_day"] >= baseline_start]
    if baseline_end is not None:
        ref = ref[ref["date_day"] <= baseline_end]

    if len(ref) == 0:
        raise ValueError(msg["err_baseline_empty"])

    n_years = ref["date_day"].dt.year.nunique()
    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["info_baseline"].format(
                start=ref["date_day"].min().date(),
                end=ref["date_day"].max().date(),
                n_years=n_years,
            )
        )
    if n_years < 10:
        warnings.warn(msg["warn_short_baseline"].format(n_years=n_years), UserWarning, stacklevel=4)

    q = percentile / 100.0
    rows: list[dict] = []
    for station_code, st_data in ref.groupby("station_code", sort=False):
        yday_arr = st_data["yday"].to_numpy()
        tmax_arr = st_data["tmax"].to_numpy(dtype=float) if "tmax" in st_data else None
        tmin_arr = st_data["tmin"].to_numpy(dtype=float) if "tmin" in st_data else None
        tmean_arr = st_data["tmean"].to_numpy(dtype=float) if "tmean" in st_data else None
        utci_arr = st_data["utci_max"].to_numpy(dtype=float) if "utci_max" in st_data else None
        wbgt_arr = st_data["wbgt_max"].to_numpy(dtype=float) if "wbgt_max" in st_data else None
        hi_arr = st_data["hi_max"].to_numpy(dtype=float) if "hi_max" in st_data else None

        for d in range(1, 367):
            raw = np.arange(d - 15, d + 16) - 1
            window_days = (raw % 366) + 1
            mask = np.isin(yday_arr, window_days)

            row = {"station_code": station_code, "yday": d}
            row["tmax_p"] = _pct_min_n(tmax_arr, mask, q)
            row["tmin_p"] = _pct_min_n(tmin_arr, mask, q)
            row["tmean_hist"] = _nanmean(tmean_arr, mask)
            row["tmax_hist"] = _nanmean(tmax_arr, mask)
            row["utci_p"] = _pct_min_n(utci_arr, mask, q)
            row["wbgt_p"] = _pct_min_n(wbgt_arr, mask, q)
            row["hi_p"] = _pct_min_n(hi_arr, mask, q)
            rows.append(row)

    return pd.DataFrame(rows)


def _pct_min_n(arr: np.ndarray | None, mask: np.ndarray, q: float, min_n: int = 10) -> float:
    """Percentile *q* of ``arr[mask]``, or NaN if fewer than *min_n* non-NaN values."""
    if arr is None:
        return np.nan
    sub = arr[mask]
    n_valid = np.sum(~np.isnan(sub))
    if n_valid < min_n:
        return np.nan
    return float(np.nanquantile(sub, q))


def _nanmean(arr: np.ndarray | None, mask: np.ndarray) -> float:
    if arr is None:
        return np.nan
    sub = arr[mask]
    if sub.size == 0:
        return np.nan
    return float(np.nanmean(sub))


# =============================================================================
# 4. METHOD DETECTION
# =============================================================================


def _hw_apply_all_methods(
    daily: pd.DataFrame, method_list: list[str], min_duration: int | None
) -> pd.DataFrame:
    for m in method_list:
        dur = min_duration if min_duration is not None else _DEFAULT_MIN_DURATION[m]
        if m == "WHO":
            daily = _hw_method_who(daily, dur)
        elif m == "WMO":
            daily = _hw_method_wmo(daily, dur)
        elif m == "INMET":
            daily = _hw_method_inmet(daily, dur)
        elif m == "EHF":
            daily = _hw_method_ehf(daily, dur)
        elif m == "UTCI":
            daily = _hw_method_utci(daily, dur)
        elif m == "WBGT":
            daily = _hw_method_wbgt(daily, dur)
        elif m == "HI":
            daily = _hw_method_hi(daily, dur)

    hw_cols_present = [f"hw_{m.lower()}" for m in method_list if f"hw_{m.lower()}" in daily.columns]
    if hw_cols_present:
        flags = daily[hw_cols_present].fillna(False).infer_objects(copy=False).astype(bool)
        daily["hw_any"] = flags.sum(axis=1) > 0
    return daily


def _hw_method_who(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    above = (daily["tmax"] > daily["tmax_p"]) & daily["tmax"].notna() & daily["tmax_p"].notna()
    daily["hw_who"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _hw_method_wmo(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    above = (
        (daily["tmax"] > daily["tmax_p"])
        & (daily["tmin"] > daily["tmin_p"])
        & daily["tmax"].notna()
        & daily["tmin"].notna()
        & daily["tmax_p"].notna()
        & daily["tmin_p"].notna()
    )
    daily["hw_wmo"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _hw_method_inmet(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    above = (
        (daily["tmax"] > (daily["tmax_hist"] + 5))
        & daily["tmax"].notna()
        & daily["tmax_hist"].notna()
    )
    daily["hw_inmet"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _hw_method_ehf(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    daily = daily.sort_values(["station_code", "date_day"]).reset_index(drop=True)
    hw_ehf = np.zeros(len(daily), dtype=bool)
    ehf_value = np.full(len(daily), np.nan)

    for _, idx in daily.groupby("station_code", sort=False).groups.items():
        pos = daily.index.get_indexer(idx)
        tmean = daily.loc[idx, "tmean"].to_numpy(dtype=float)

        t3 = _slide_mean(tmean, before=2, after=0, complete=True)
        t30 = _slide_mean(tmean, before=32, after=-3, complete=False)
        t95 = np.nanquantile(tmean, 0.95) if np.any(~np.isnan(tmean)) else np.nan

        ehi_sig = t3 - t95
        ehi_acc = t3 - t30
        ehf = ehi_sig * np.maximum(1.0, ehi_acc)
        above_ehf = (ehf > 0) & ~np.isnan(ehf)

        hw_ehf_raw = _slide_any(above_ehf, before=0, after=2)
        hw_ehf_group = _consecutive_flag(hw_ehf_raw, min_dur)

        hw_ehf[pos] = hw_ehf_group
        ehf_value[pos] = np.where(hw_ehf_group, ehf, np.nan)

    daily["hw_ehf"] = hw_ehf
    daily["ehf_value"] = ehf_value
    return daily


def _hw_method_utci(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    if "utci_max" not in daily.columns or daily["utci_max"].isna().all():
        daily["hw_utci"] = pd.NA
        return daily
    above = (
        daily["utci_max"].notna()
        & daily["utci_p"].notna()
        & (daily["utci_max"] > daily["utci_p"])
    )
    daily["hw_utci"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _hw_method_wbgt(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    if "wbgt_max" not in daily.columns or daily["wbgt_max"].isna().all():
        daily["hw_wbgt"] = pd.NA
        return daily
    above = (
        daily["wbgt_max"].notna()
        & daily["wbgt_p"].notna()
        & (daily["wbgt_max"] > daily["wbgt_p"])
    )
    daily["hw_wbgt"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _hw_method_hi(daily: pd.DataFrame, min_dur: int) -> pd.DataFrame:
    if "hi_max" not in daily.columns or daily["hi_max"].isna().all():
        daily["hw_hi"] = pd.NA
        return daily
    above = daily["hi_max"].notna() & daily["hi_p"].notna() & (daily["hi_max"] > daily["hi_p"])
    daily["hw_hi"] = _consecutive_flag_by_group(above, daily["station_code"], min_dur)
    return daily


def _consecutive_flag_by_group(
    above: pd.Series, station_code: pd.Series, min_dur: int
) -> pd.Series:
    """Apply :func:`_consecutive_flag` independently within each station group.

    Rows are assumed already sorted by (station_code, date_day) — the R
    source relies on the same row-order assumption (``rle()`` operates on
    row order, not on actual calendar-date continuity: a gap in the
    dates with a contiguous run of ``TRUE`` flags either side of it is
    still treated as one run — this quirk is preserved, not fixed).
    """
    result = np.zeros(len(above), dtype=bool)
    above_arr = above.to_numpy()
    for _, idx in station_code.groupby(station_code).groups.items():
        pos = station_code.index.get_indexer(idx)
        result[pos] = _consecutive_flag(above_arr[pos], min_dur)
    return pd.Series(result, index=above.index)


def _consecutive_flag(x: np.ndarray, min_dur: int) -> np.ndarray:
    """Gaps-and-islands run detection: flag every element of a run of
    consecutive ``True`` values whose length is >= *min_dur*.

    Direct translation of R's ``rle()``-based ``.hw_consecutive_flag``.
    """
    n = len(x)
    result = np.zeros(n, dtype=bool)
    if n == 0:
        return result
    x = np.asarray(x, dtype=bool)
    change = np.empty(n, dtype=bool)
    change[0] = True
    change[1:] = x[1:] != x[:-1]
    run_ids = np.cumsum(change) - 1
    run_lengths = np.bincount(run_ids)
    qualifies = x & (run_lengths[run_ids] >= min_dur)
    result[qualifies] = True
    return result


def _slide_mean(x: np.ndarray, before: int, after: int, complete: bool) -> np.ndarray:
    """Rolling-window mean over ``x[i-before : i+after+1]`` (R ``slider::slide_dbl``).

    No ``na.rm`` — any NaN present in the window propagates to NaN,
    matching ``mean()`` called without ``na.rm = TRUE`` in the R source.
    ``complete=True`` requires the full window to be in-bounds (no
    partial windows at the series edges); ``complete=False`` allows
    partial (but non-empty) windows.
    """
    n = len(x)
    out = np.full(n, np.nan)
    for i in range(n):
        lo, hi = i - before, i + after
        if complete and (lo < 0 or hi > n - 1):
            continue
        lo_c, hi_c = max(lo, 0), min(hi, n - 1)
        if hi_c < lo_c:
            continue
        window = x[lo_c : hi_c + 1]
        out[i] = np.nan if np.isnan(window).any() else window.mean()
    return out


def _slide_any(x: np.ndarray, before: int, after: int) -> np.ndarray:
    """Rolling-window ``any()`` over ``x[i-before : i+after+1]`` (partial windows allowed)."""
    n = len(x)
    out = np.zeros(n, dtype=bool)
    for i in range(n):
        lo, hi = max(i - before, 0), min(i + after, n - 1)
        if hi < lo:
            continue
        out[i] = bool(x[lo : hi + 1].any())
    return out


# =============================================================================
# 5. EVENT EXTRACTION
# =============================================================================


def _hw_extract_events(daily: pd.DataFrame, method_list: list[str]) -> pd.DataFrame:
    hw_cols_present = [f"hw_{m.lower()}" for m in method_list if f"hw_{m.lower()}" in daily.columns]
    if not hw_cols_present:
        return pd.DataFrame()

    event_frames: list[pd.DataFrame] = []
    for col in hw_cols_present:
        m_name = col[len("hw_"):].upper()
        temp_col = _METHOD_TEMP_COL.get(m_name, "tmax")
        ref_col = _METHOD_REF_COL.get(m_name, "tmax_p")

        method_rows: list[dict] = []
        for station_code, st_data in daily.groupby("station_code", sort=False):
            st_data = st_data.sort_values("date_day")
            flag = st_data[col]
            if flag.isna().all() or not bool(flag.fillna(False).any()):
                continue
            flag_arr = flag.fillna(False).to_numpy(dtype=bool)

            n = len(flag_arr)
            change = np.empty(n, dtype=bool)
            change[0] = True
            change[1:] = flag_arr[1:] != flag_arr[:-1]
            run_ids = np.cumsum(change) - 1
            n_runs = run_ids[-1] + 1

            for run_id in range(n_runs):
                run_mask = run_ids == run_id
                if not flag_arr[run_mask][0]:
                    continue
                sub = st_data.iloc[np.flatnonzero(run_mask)]

                n_sub = len(sub)
                temp_vals = (
                    sub[temp_col].to_numpy(dtype=float)
                    if temp_col in sub else np.full(n_sub, np.nan)
                )
                ref_vals = (
                    sub[ref_col].to_numpy(dtype=float)
                    if ref_col in sub else np.full(n_sub, np.nan)
                )
                anomaly = temp_vals - ref_vals

                ehf_peak_val = np.nan
                ehf_mean_val = np.nan
                if m_name == "EHF" and "ehf_value" in sub.columns:
                    ehf_vals = sub["ehf_value"].to_numpy(dtype=float)
                    if np.any(~np.isnan(ehf_vals)):
                        ehf_peak_val = float(np.nanmax(ehf_vals))
                        ehf_mean_val = float(np.nanmean(ehf_vals))

                start_date = sub["date_day"].min()
                end_date = sub["date_day"].max()
                dur = int((end_date - start_date).days) + 1

                has_temp = np.any(~np.isnan(temp_vals))
                has_anomaly = np.any(~np.isnan(anomaly))
                row = {
                    "station_code": station_code,
                    "method": m_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "duration_days": dur,
                    "temp_mean": float(np.nanmean(temp_vals)) if has_temp else np.nan,
                    "temp_peak": float(np.nanmax(temp_vals)) if has_temp else np.nan,
                    "anomaly_mean": float(np.nanmean(anomaly)) if has_anomaly else np.nan,
                    "anomaly_cumulative": float(np.nansum(anomaly)) if has_anomaly else 0.0,
                    "ehf_peak": ehf_peak_val,
                    "ehf_mean": ehf_mean_val,
                }
                row["severity_index"] = dur * row["anomaly_mean"]
                for meta_col in _STATION_META_COLS:
                    row[meta_col] = sub[meta_col].iloc[0] if meta_col in sub.columns else np.nan
                method_rows.append(row)

        ev = pd.DataFrame(method_rows)
        if ev.empty:
            continue
        ev = ev.sort_values("start_date").reset_index(drop=True)
        ev["event_id"] = [
            f"{sc}_{m_name}_{i + 1}" for i, sc in enumerate(ev["station_code"])
        ]

        if m_name == "EHF":
            relevant_stations = ev["station_code"].unique()
            all_pos = daily.loc[
                daily["station_code"].isin(relevant_stations), "ehf_value"
            ].dropna()
            all_pos = all_pos[all_pos > 0]
            if len(all_pos) > 0:
                ehf85 = float(np.quantile(all_pos, 0.85))
                peak = ev["ehf_peak"]
                # Object-dtype assignment avoids numpy>=2's DTypePromotionError
                # when mixing string choices with a float NaN default (both
                # np.where and np.select raise on that combination).
                classes = np.full(len(ev), np.nan, dtype=object)
                classes[(peak > 0).to_numpy()] = "Low Intensity (LIHW)"
                classes[(peak >= ehf85).to_numpy()] = "Severe (SHW)"
                classes[(peak >= 3 * ehf85).to_numpy()] = "Extreme (EHW)"
                ev["intensity_class"] = classes
            else:
                ev["intensity_class"] = np.nan
        else:
            ev["intensity_class"] = np.nan

        event_frames.append(ev)

    if not event_frames:
        return pd.DataFrame()

    events = pd.concat(event_frames, ignore_index=True)
    events = events.sort_values(["station_code", "method", "start_date"]).reset_index(drop=True)
    return events


# =============================================================================
# 6. ANNUAL SUMMARY
# =============================================================================

_SUMMARY_EMPTY_COLS = [
    "year",
    "station_code",
    "method",
    "n_events",
    "total_days_hw",
    "mean_duration",
    "max_duration",
    "mean_intensity",
    "max_intensity",
    "mean_anomaly",
    "severity_total",
]


def _hw_build_summary(events: pd.DataFrame, method_list: list[str]) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=_SUMMARY_EMPTY_COLS)

    ev = events.copy()
    ev["year"] = ev["start_date"].dt.year

    group_cols = [
        c
        for c in ["year", "station_code", "method", "region", "federal_unit", "zona_climatica"]
        if c in ev.columns
    ]
    summary = (
        ev.groupby(group_cols, dropna=False)
        .agg(
            n_events=("station_code", "size"),
            total_days_hw=("duration_days", "sum"),
            mean_duration=("duration_days", "mean"),
            max_duration=("duration_days", "max"),
            mean_intensity=("temp_mean", "mean"),
            max_intensity=("temp_peak", "max"),
            mean_anomaly=("anomaly_mean", "mean"),
            severity_total=("severity_index", "sum"),
        )
        .reset_index()
    )
    return summary.sort_values(["station_code", "method", "year"]).reset_index(drop=True)


# =============================================================================
# CONVENIENCE HELPERS
# =============================================================================


def hw_get_events(
    hw_result: dict[str, pd.DataFrame], method_filter: list[str] | None = None
) -> pd.DataFrame:
    """Extract the heatwave events table from a ``sus_climate_compute_heatwaves`` result.

    Args:
        hw_result: The dict returned by ``sus_climate_compute_heatwaves``.
        method_filter: If supplied, keep only these methods.

    Returns:
        A ``pd.DataFrame`` of heatwave events.
    """
    ev = hw_result["events"]
    if method_filter is not None:
        ev = ev[ev["method"].isin(method_filter)]
    return ev


def hw_count_by_year(hw_result: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Count heatwave events by year, station, and method.

    Args:
        hw_result: The dict returned by ``sus_climate_compute_heatwaves``.

    Returns:
        A ``pd.DataFrame`` with columns year, station_code, station_name,
        method, n_events, total_days_hw, mean_duration (whichever are
        present in the summary table).
    """
    summary = hw_result["summary"]
    cols = [
        c
        for c in [
            "year",
            "station_code",
            "station_name",
            "method",
            "n_events",
            "total_days_hw",
            "mean_duration",
        ]
        if c in summary.columns
    ]
    return summary[cols]


def hw_active_days(hw_result: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return daily rows where at least one heatwave method is active.

    Args:
        hw_result: The dict returned by ``sus_climate_compute_heatwaves``.

    Returns:
        A filtered ``pd.DataFrame`` from ``hw_result["daily"]``.
    """
    daily = hw_result["daily"]
    active = daily[daily["hw_any"].astype(bool)]
    keep = [c for c in daily.columns if c in ("station_code", "date_day", "tmax", "tmean")]
    keep += [c for c in daily.columns if c.startswith("hw_")]
    return active[keep]
