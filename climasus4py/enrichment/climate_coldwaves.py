"""climate_coldwaves.py — Coldwave detection using multiple standard methodologies.

Mirrors R: sus_climate_compute_coldwaves.R

Not lazy — coldwave detection is per-station run-length (gaps-and-islands)
and rolling-window percentile work with no natural DuckDB SQL expression
(same rationale as ``climate_spi.py`` / ``climate_spei.py``). Accepts a
``DuckDBPyRelation`` or ``pd.DataFrame``; a relation is materialised with a
``UserWarning``. Returns a ``dict`` with three ``pd.DataFrame`` values
(``"events"``, ``"daily"``, ``"summary"``) — the closest Python analogue of
the R function's attributed ``climasus_cw`` list, since a plain ``dict``
has no single ``.attrs`` slot. Each of the three frames carries its own
``df.attrs["sus_meta"]`` (``stage="climate"``, ``type="coldwaves"``).

Implemented methods (thresholds/percentiles computed from the input data
itself, not from any external table — see each ``_method_*`` helper):
  WHO   : Tmin < P10(Tmin) for >= N consecutive days (default N=3).
  WMO   : Tmin < P10(Tmin) AND Tmax < P10(Tmax) for >= N days (default N=5).
  INMET : Tmin < mean(Tmin_hist) - 5 degC for >= N days (default N=5).
  EHF   : Excess Cold Factor (adaptation of Nairn & Fawcett 2015's Excess
          Heat Factor for cold extremes). Exposed under the ``"EHF"``
          method code but internally computed as the ECF formula
          (``ecf = eci_sig * max(1, eci_acc)``) — same naming used by the
          R source.
  UTCI/WBGT/HI : <indicator>_min < P10(<indicator>_min) for >= N days.
          Require ``utci_c``/``wbgt_c``/``hi_c`` from
          ``sus_climate_compute_indicators()``.

Percentile thresholds are smoothed over a 31-day (+/-15) rolling
day-of-year window, computed per station over the reference baseline
period (``baseline_start``/``baseline_end``, or the full series).

References:
  - Perkins, S.E. & Alexander, L.V. (2013). On the measurement of heat
    waves. Journal of Climate, 26(13), 4500-4517. (Adapted for coldwaves)
  - WMO & WHO (2015). Heatwaves and Health: Guidance on Warning-System
    Development. Geneva. (Adapted for coldwaves)
  - Nairn, J.R. & Fawcett, R.J.B. (2015). The Excess Heat Factor. Int. J.
    Environ. Res. Public Health, 12(1), 227-253. (Adapted for cold)
  - INMET (2009). Normais Climatologicas do Brasil 1961-1990. Brasilia.
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, Literal

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

ALL_METHODS: tuple[str, ...] = ("WHO", "WMO", "INMET", "EHF", "UTCI", "WBGT", "HI")

_DEFAULT_MIN_DURATION: dict[str, int] = {
    "WHO": 3, "WMO": 5, "INMET": 5, "EHF": 3, "UTCI": 3, "WBGT": 3, "HI": 3,
}

_METHOD_COLUMN_REQ: dict[str, str] = {"UTCI": "utci_c", "WBGT": "wbgt_c", "HI": "hi_c"}
_TEMP_COLS: tuple[str, ...] = (
    "tair_dry_bulb_c", "tair_max_c", "tair_min_c", "utci_c", "wbgt_c", "hi_c",
)
_TEMP_COL_MAP = {"WHO": "tmin", "WMO": "tmin", "INMET": "tmin", "EHF": "tmean",
                 "UTCI": "utci_min", "WBGT": "wbgt_min", "HI": "hi_min"}
_REF_COL_MAP = {"WHO": "tmin_p", "WMO": "tmin_p", "INMET": "tmin_hist", "EHF": "tmean",
                "UTCI": "utci_p", "WBGT": "wbgt_p", "HI": "hi_p"}

_EVENTS_COLUMNS = [
    "station_code", "method", "start_date", "end_date", "duration_days",
    "temp_mean", "temp_peak", "anomaly_mean", "anomaly_cumulative",
    "severity_index", "ecf_peak", "ecf_mean", "region", "federal_unit",
    "station_name", "zona_climatica", "latitude", "longitude",
    "event_id", "intensity_class",
]
_SUMMARY_COLUMNS_EMPTY = [
    "year", "station_code", "method", "n_events", "total_days_cw",
    "mean_duration", "max_duration", "mean_intensity", "max_intensity",
    "mean_anomaly", "severity_total",
]

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Detectando ondas de frio",
        "step_validate": "Validando entrada...",
        "step_daily": "Agregando dados horários para escala diária...",
        "step_baseline": "Calculando limiares históricos (baseline)...",
        "step_detect": "Detectando ondas de frio por método...",
        "step_events": "Extraindo e classificando eventos...",
        "step_summary": "Gerando resumo anual...",
        "done": "Detecção concluída: {n_ev} evento(s) em {n_st} estação(ões).",
        "err_no_method": "'method' deve conter ao menos um método válido.",
        "err_unknown_method": "Método(s) inválido(s): {bad}. Esperado: {choices}.",
        "err_required_cols": "Colunas obrigatórias ausentes: {cols}.",
        "err_no_cols": (
            "Nenhuma coluna de temperatura encontrada. Esperado: tair_dry_bulb_c, "
            "tair_max_c, tair_min_c, utci_c, wbgt_c ou hi_c."
        ),
        "err_date_range": "Intervalo de datas inválido. Verifique a coluna de datas.",
        "err_baseline_empty": "Nenhum dado disponível no período baseline especificado.",
        "warn_method_missing": (
            "Método '{m}' solicitado mas coluna '{col}' não encontrada. Método ignorado."
        ),
        "warn_short_series": (
            "Série temporal curta ({n_days} dias). Resultados podem ser pouco robustos."
        ),
        "warn_short_baseline": (
            "Período baseline curto ({n_years} anos). Recomenda-se ao menos 20 anos."
        ),
        "warn_na_temp": (
            "{n_na} valor(es) NA em colunas de temperatura. Use sus_climate_fill_inmet() antes."
        ),
        "info_baseline": "Baseline: {start} a {end} ({n_years} ano(s)).",
        "materialize_warning": (
            "sus_climate_compute_coldwaves: a DuckDBPyRelation de entrada está sendo "
            "materializada — a detecção de ondas de frio depende de séries ordenadas "
            "por data e não é expressável em SQL lazy."
        ),
    },
    "en": {
        "title": "Detecting coldwaves",
        "step_validate": "Validating input...",
        "step_daily": "Aggregating hourly data to daily scale...",
        "step_baseline": "Computing historical thresholds (baseline)...",
        "step_detect": "Detecting coldwaves by method...",
        "step_events": "Extracting and classifying events...",
        "step_summary": "Generating annual summary...",
        "done": "Detection complete: {n_ev} event(s) across {n_st} station(s).",
        "err_no_method": "'method' must contain at least one valid method.",
        "err_unknown_method": "Invalid method(s): {bad}. Expected: {choices}.",
        "err_required_cols": "Missing required columns: {cols}.",
        "err_no_cols": (
            "No temperature column found. Expected: tair_dry_bulb_c, tair_max_c, "
            "tair_min_c, utci_c, wbgt_c, or hi_c."
        ),
        "err_date_range": "Invalid date range. Check your date column.",
        "err_baseline_empty": "No data available in the specified baseline period.",
        "warn_method_missing": (
            "Method '{m}' requested but column '{col}' not found. Method skipped."
        ),
        "warn_short_series": "Short time series ({n_days} days). Results may not be robust.",
        "warn_short_baseline": (
            "Short baseline period ({n_years} years). At least 20 years recommended."
        ),
        "warn_na_temp": (
            "{n_na} NA value(s) in temperature columns. Use sus_climate_fill_inmet() first."
        ),
        "info_baseline": "Baseline: {start} to {end} ({n_years} year(s)).",
        "materialize_warning": (
            "sus_climate_compute_coldwaves: the input DuckDBPyRelation is being "
            "materialised — coldwave detection depends on date-ordered series and "
            "cannot be expressed as lazy SQL."
        ),
    },
    "es": {
        "title": "Detectando olas de frío",
        "step_validate": "Validando entrada...",
        "step_daily": "Agregando datos horarios a escala diaria...",
        "step_baseline": "Calculando umbrales históricos (línea base)...",
        "step_detect": "Detectando olas de frío por método...",
        "step_events": "Extrayendo y clasificando eventos...",
        "step_summary": "Generando resumen anual...",
        "done": "Detección completada: {n_ev} evento(s) en {n_st} estación(es).",
        "err_no_method": "'method' debe contener al menos un método válido.",
        "err_unknown_method": "Método(s) inválido(s): {bad}. Esperado: {choices}.",
        "err_required_cols": "Faltan columnas requeridas: {cols}.",
        "err_no_cols": (
            "No se encontró ninguna columna de temperatura. Esperado: tair_dry_bulb_c, "
            "tair_max_c, tair_min_c, utci_c, wbgt_c o hi_c."
        ),
        "err_date_range": "Rango de fechas inválido. Verifique su columna de fechas.",
        "err_baseline_empty": "No hay datos disponibles en el período baseline especificado.",
        "warn_method_missing": (
            "Método '{m}' solicitado pero no se encontró la columna '{col}'. Método omitido."
        ),
        "warn_short_series": (
            "Serie temporal corta ({n_days} días). Los resultados pueden no ser robustos."
        ),
        "warn_short_baseline": (
            "Período baseline corto ({n_years} años). Se recomiendan al menos 20 años."
        ),
        "warn_na_temp": (
            "{n_na} valor(es) NA en columnas de temperatura. Use sus_climate_fill_inmet() primero."
        ),
        "info_baseline": "Baseline: {start} a {end} ({n_years} año(s)).",
        "materialize_warning": (
            "sus_climate_compute_coldwaves: la DuckDBPyRelation de entrada se está "
            "materializando — la detección de olas de frío depende de series "
            "ordenadas por fecha y no es expresable en SQL lazy."
        ),
    },
}


def sus_climate_compute_coldwaves(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    method: list[str] | tuple[str, ...] | str = ALL_METHODS,
    baseline_start: str | None = None,
    baseline_end: str | None = None,
    percentile: float = 10,
    min_duration: int | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, pd.DataFrame]:
    """Detect coldwaves using multiple standard methodologies.

    Applies up to seven coldwave detection methods (WHO, WMO, INMET, EHF,
    UTCI, WBGT, HI) to station climate data. Aggregates hourly data to
    daily scale, computes smoothed historical percentile thresholds from
    a reference baseline, identifies coldwave days and discrete events
    per method, and returns events/daily/summary tables.

    All thresholds are computed from the input data itself (no
    hardcoded/external metadata) over a 31-day (+/-15) rolling
    day-of-year window, per station, within the baseline period.

    Args:
        df: Station climate data with ``date`` and ``station_code``
            columns. Requires at least one of ``tair_dry_bulb_c``,
            ``tair_max_c``, ``tair_min_c`` (for WHO/WMO/INMET/EHF) or
            ``utci_c``/``wbgt_c``/``hi_c`` (for UTCI/WBGT/HI, from
            ``sus_climate_compute_indicators()``). A lazy
            ``DuckDBPyRelation`` is materialised (with a ``UserWarning``).
        method: One or more of ``"WHO"``, ``"WMO"``, ``"INMET"``,
            ``"EHF"``, ``"UTCI"``, ``"WBGT"``, ``"HI"``, or the literal
            string ``"all"``. Methods whose required column is absent
            emit a warning and are skipped (their ``cw_<method>`` column
            is all-``NaN``). Default: every method.
        baseline_start: Start of the reference period for percentile
            thresholds (``"YYYY-MM-DD"``), or ``None`` for the full series.
        baseline_end: End of the reference period, or ``None``.
        percentile: Percentile (0-100) used for WHO, WMO, UTCI, WBGT, and
            HI threshold calculations (lower tail for coldwaves). Default 10.
        min_duration: Minimum consecutive days to qualify as a coldwave,
            overriding every method's default when given. ``None`` uses
            method defaults: WHO=3, WMO=5, INMET=5, EHF=3, UTCI=3, WBGT=3,
            HI=3.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A dict with three ``pd.DataFrame`` values (each carrying
        ``df.attrs["sus_meta"]`` with ``stage="climate"``,
        ``type="coldwaves"``):
          - ``"events"``: one row per detected event (``event_id``,
            ``station_code``, ``method``, ``start_date``, ``end_date``,
            ``duration_days``, ``temp_mean``, ``temp_peak`` (coldest),
            ``anomaly_mean``, ``anomaly_cumulative``, ``severity_index``,
            ``ecf_peak``, ``ecf_mean``, ``intensity_class``, station
            metadata).
          - ``"daily"``: daily aggregated data with boolean flag columns
            ``cw_<method>`` and ``cw_any``.
          - ``"summary"``: annual summary by station and method.

    Raises:
        ValueError: If required columns are missing, no temperature
            column is present, ``method`` is empty/invalid, the date
            range is invalid, or the baseline period has no data.

    Examples::

        import climasus4py as cs

        cw = cs.sus_climate_compute_coldwaves(
            df_indicators,
            method=["WHO", "INMET", "EHF"],
            baseline_start="2000-01-01",
            baseline_end="2020-12-31",
        )
        cw["events"]
        cw["summary"]
        cw["daily"][cw["daily"]["cw_any"]]
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    else:
        data = df.copy()

    methods = _resolve_methods(method, msg)

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["step_validate"])
    data = _validate(data, methods, msg)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_daily"])
    daily = _aggregate_daily(data)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_baseline"])
    baseline_start_ts = pd.Timestamp(baseline_start) if baseline_start is not None else None
    baseline_end_ts = pd.Timestamp(baseline_end) if baseline_end is not None else None
    baseline = _compute_baseline(
        daily, baseline_start_ts, baseline_end_ts, percentile, msg, verbose
    )
    daily = daily.merge(baseline, on=["station_code", "yday"], how="left")

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_detect"])
    daily = _apply_all_methods(daily, methods, min_duration)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_events"])
    events = _extract_events(daily, methods)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_summary"])
    summary = _build_summary(events)

    n_ev = len(events)
    n_st = events["station_code"].nunique() if not events.empty else 0
    if verbose:
        console.print("[green]OK[/]  " + msg["done"].format(n_ev=n_ev, n_st=n_st))

    now = datetime.now()
    history_entry = (
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_climate_compute_coldwaves(): "
        f"methods={','.join(methods)}; {n_ev} event(s); {n_st} station(s); "
        f"baseline={baseline_start_ts.date() if baseline_start_ts is not None else 'full'}/"
        f"{baseline_end_ts.date() if baseline_end_ts is not None else 'full'}"
    )
    sus_meta = {"stage": "climate", "type": "coldwaves", "history": [history_entry]}
    events.attrs["sus_meta"] = sus_meta
    daily.attrs["sus_meta"] = sus_meta
    summary.attrs["sus_meta"] = sus_meta

    return {"events": events, "daily": daily, "summary": summary}


# ---------------------------------------------------------------------------
# Convenience helpers (parity with R's exported cw_get_events / cw_count_by_year
# / cw_active_days)
# ---------------------------------------------------------------------------


def cw_get_events(
    cw_result: dict[str, pd.DataFrame], method_filter: list[str] | None = None
) -> pd.DataFrame:
    """Extract the coldwave events table from a `sus_climate_compute_coldwaves` result.

    Args:
        cw_result: Dict returned by ``sus_climate_compute_coldwaves()``.
        method_filter: If given, keep only these method codes.

    Returns:
        A copy of ``cw_result["events"]``, optionally filtered by method.
    """
    ev = cw_result["events"]
    if method_filter is not None:
        ev = ev[ev["method"].isin(method_filter)]
    return ev


def cw_count_by_year(cw_result: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Count coldwave events by year, station, and method.

    Args:
        cw_result: Dict returned by ``sus_climate_compute_coldwaves()``.

    Returns:
        A subset of ``cw_result["summary"]`` with the columns year,
        station_code, station_name, method, n_events, total_days_cw,
        mean_duration (whichever of these are present).
    """
    wanted = ["year", "station_code", "station_name", "method",
              "n_events", "total_days_cw", "mean_duration"]
    cols = [c for c in wanted if c in cw_result["summary"].columns]
    return cw_result["summary"][cols]


def cw_active_days(cw_result: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Return daily rows where at least one coldwave method is active.

    Args:
        cw_result: Dict returned by ``sus_climate_compute_coldwaves()``.

    Returns:
        A filtered subset of ``cw_result["daily"]``.
    """
    daily = cw_result["daily"]
    cw_cols = [c for c in daily.columns if c.startswith("cw_")]
    base_cols = [c for c in ("station_code", "date_day", "tmin", "tmean") if c in daily.columns]
    cols = base_cols + cw_cols
    return daily.loc[daily["cw_any"] == True, cols]  # noqa: E712 (mirrors R's `cw_any == TRUE`)


# ---------------------------------------------------------------------------
# 0. Method-list resolution
# ---------------------------------------------------------------------------


def _resolve_methods(method: list[str] | tuple[str, ...] | str, msg: dict[str, str]) -> list[str]:
    if isinstance(method, str):
        method = [method]
    method = list(method)
    if len(method) == 1 and method[0] == "all":
        return list(ALL_METHODS)
    if len(method) == 0:
        raise ValueError(msg["err_no_method"])
    bad = [m for m in method if m not in ALL_METHODS]
    if bad:
        raise ValueError(msg["err_unknown_method"].format(bad=bad, choices=list(ALL_METHODS)))
    return method


# ---------------------------------------------------------------------------
# 1. Column validation
# ---------------------------------------------------------------------------


def _validate(data: pd.DataFrame, methods: list[str], msg: dict[str, str]) -> pd.DataFrame:
    required = ["date", "station_code"]
    missing = [c for c in required if c not in data.columns]
    if missing:
        raise ValueError(msg["err_required_cols"].format(cols=missing))

    temp_any = [c for c in _TEMP_COLS if c in data.columns]
    if not temp_any:
        raise ValueError(msg["err_no_cols"])

    for m, col in _METHOD_COLUMN_REQ.items():
        if m in methods and col not in data.columns:
            warnings.warn(
                msg["warn_method_missing"].format(m=m, col=col), UserWarning, stacklevel=3
            )

    n_na = int(data[temp_any].isna().sum().sum())
    if n_na > 0:
        warnings.warn(msg["warn_na_temp"].format(n_na=n_na), UserWarning, stacklevel=3)

    data = data.copy()
    data["date"] = pd.to_datetime(data["date"])
    valid_dates = data["date"].dropna()
    if valid_dates.empty:
        raise ValueError(msg["err_date_range"])
    date_min, date_max = valid_dates.min(), valid_dates.max()
    n_days = (date_max.normalize() - date_min.normalize()).days
    if n_days < 365:
        warnings.warn(msg["warn_short_series"].format(n_days=n_days), UserWarning, stacklevel=3)

    return data


# ---------------------------------------------------------------------------
# 2. Daily aggregation
# ---------------------------------------------------------------------------


def _aggregate_daily(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data["date_day"] = data["date"].dt.normalize()

    def has(col: str) -> bool:
        return col in data.columns

    keys = [data["station_code"], data["date_day"]]

    tmax_src = (
        data["tair_max_c"] if has("tair_max_c")
        else data["tair_dry_bulb_c"] if has("tair_dry_bulb_c")
        else pd.Series(np.nan, index=data.index)
    )
    tmin_src = (
        data["tair_min_c"] if has("tair_min_c")
        else data["tair_dry_bulb_c"] if has("tair_dry_bulb_c")
        else pd.Series(np.nan, index=data.index)
    )

    out: dict[str, pd.Series] = {
        "tmax": tmax_src.groupby(keys, sort=False).max(),
        "tmin": tmin_src.groupby(keys, sort=False).min(),
    }

    if has("tair_dry_bulb_c"):
        out["tmean"] = data["tair_dry_bulb_c"].groupby(keys, sort=False).mean()
    elif has("tair_max_c") and has("tair_min_c"):
        tmax_mean = data["tair_max_c"].groupby(keys, sort=False).mean()
        tmin_mean = data["tair_min_c"].groupby(keys, sort=False).mean()
        out["tmean"] = (tmax_mean + tmin_mean) / 2.0
    else:
        out["tmean"] = pd.Series(np.nan, index=out["tmax"].index)

    for src_col, min_col, mean_col in (
        ("utci_c", "utci_min", "utci_mean"),
        ("wbgt_c", "wbgt_min", "wbgt_mean"),
        ("hi_c", "hi_min", "hi_mean"),
    ):
        if has(src_col):
            out[min_col] = data[src_col].groupby(keys, sort=False).min()
            out[mean_col] = data[src_col].groupby(keys, sort=False).mean()
        else:
            out[min_col] = pd.Series(np.nan, index=out["tmax"].index)
            out[mean_col] = pd.Series(np.nan, index=out["tmax"].index)

    out["pet_min"] = (
        data["pet_c"].groupby(keys, sort=False).min() if has("pet_c")
        else pd.Series(np.nan, index=out["tmax"].index)
    )

    for src_col, out_col in (
        ("wbgt_c_flag_extreme", "n_hours_extreme_wbgt"),
        ("utci_c_flag_extreme", "n_hours_extreme_utci"),
        ("hi_c_flag_extreme", "n_hours_extreme_hi"),
    ):
        out[out_col] = (
            data[src_col].groupby(keys, sort=False).sum() if has(src_col)
            else pd.Series(np.nan, index=out["tmax"].index)
        )

    for col in ("region", "federal_unit", "station_name", "zona_climatica"):
        out[col] = (
            data[col].groupby(keys, sort=False).first() if has(col)
            else pd.Series(np.nan, index=out["tmax"].index)
        )
    for col in ("latitude", "longitude"):
        out[col] = (
            data[col].groupby(keys, sort=False).first() if has(col)
            else pd.Series(np.nan, index=out["tmax"].index)
        )

    daily = pd.DataFrame(out).reset_index()
    daily["yday"] = daily["date_day"].dt.dayofyear
    daily = daily.sort_values(["station_code", "date_day"]).reset_index(drop=True)
    daily = daily[daily["date_day"].notna()].reset_index(drop=True)
    return daily


# ---------------------------------------------------------------------------
# 3. Baseline / thresholds
# ---------------------------------------------------------------------------


def _window_days(d: int) -> np.ndarray:
    """31-day (+/-15) circular day-of-year window, 1-366, matching R exactly."""
    rng = np.arange(d - 15, d + 16) - 1
    return (rng % 366) + 1


_WINDOWS: list[np.ndarray] = [_window_days(d) for d in range(1, 367)]


def _pctl_or_nan(values: np.ndarray, percentile: float, min_n: int = 10) -> float:
    valid = values[~np.isnan(values)]
    if valid.size < min_n:
        return np.nan
    return float(np.percentile(valid, percentile))


def _mean_or_nan(values: np.ndarray) -> float:
    valid = values[~np.isnan(values)]
    if valid.size == 0:
        return np.nan
    return float(np.mean(valid))


def _compute_baseline(
    daily: pd.DataFrame,
    baseline_start: pd.Timestamp | None,
    baseline_end: pd.Timestamp | None,
    percentile: float,
    msg: dict[str, str],
    verbose: bool,
) -> pd.DataFrame:
    ref = daily
    if baseline_start is not None:
        ref = ref[ref["date_day"] >= baseline_start]
    if baseline_end is not None:
        ref = ref[ref["date_day"] <= baseline_end]
    if ref.empty:
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

    rows: list[tuple[Any, ...]] = []
    for station, sub in ref.groupby("station_code", sort=False):
        yday_arr = sub["yday"].to_numpy()
        tmax_arr = sub["tmax"].to_numpy(dtype=float)
        tmin_arr = sub["tmin"].to_numpy(dtype=float)
        tmean_arr = sub["tmean"].to_numpy(dtype=float)
        utci_arr = sub["utci_min"].to_numpy(dtype=float)
        wbgt_arr = sub["wbgt_min"].to_numpy(dtype=float)
        hi_arr = sub["hi_min"].to_numpy(dtype=float)

        for d in range(1, 367):
            mask = np.isin(yday_arr, _WINDOWS[d - 1])
            rows.append((
                station,
                d,
                _pctl_or_nan(tmax_arr[mask], percentile),
                _pctl_or_nan(tmin_arr[mask], percentile),
                _mean_or_nan(tmean_arr[mask]),
                _mean_or_nan(tmin_arr[mask]),
                _pctl_or_nan(utci_arr[mask], percentile),
                _pctl_or_nan(wbgt_arr[mask], percentile),
                _pctl_or_nan(hi_arr[mask], percentile),
            ))

    return pd.DataFrame(
        rows,
        columns=["station_code", "yday", "tmax_p", "tmin_p", "tmean_hist",
                 "tmin_hist", "utci_p", "wbgt_p", "hi_p"],
    )


# ---------------------------------------------------------------------------
# 4. Method detection
# ---------------------------------------------------------------------------


def _consecutive_flag(x: np.ndarray, min_dur: int) -> np.ndarray:
    """Flag every element in a run of >= min_dur consecutive True values.

    Direct translation of the R helper's `rle()`-based gaps-and-islands
    logic: contiguous runs of the same boolean value are found, and only
    True-valued runs at least `min_dur` long are flagged.
    """
    n = len(x)
    if n == 0:
        return np.zeros(0, dtype=bool)
    x = np.asarray(x, dtype=bool)
    change = np.ones(n, dtype=bool)
    change[1:] = x[1:] != x[:-1]
    run_id = np.cumsum(change) - 1
    run_len = np.bincount(run_id)[run_id]
    return x & (run_len >= min_dur)


def _grouped_consecutive_flag(mask: pd.Series, groups: pd.Series, min_dur: int) -> pd.Series:
    return mask.groupby(groups, sort=False, group_keys=False).apply(
        lambda s: pd.Series(_consecutive_flag(s.to_numpy(), min_dur), index=s.index)
    )


def _method_who(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    below = (daily["tmin"] < daily["tmin_p"]) & daily["tmin"].notna() & daily["tmin_p"].notna()
    daily["cw_who"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


def _method_wmo(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    below = (
        (daily["tmin"] < daily["tmin_p"]) & (daily["tmax"] < daily["tmax_p"])
        & daily["tmin"].notna() & daily["tmax"].notna()
        & daily["tmin_p"].notna() & daily["tmax_p"].notna()
    )
    daily["cw_wmo"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


def _method_inmet(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    below = (
        (daily["tmin"] < (daily["tmin_hist"] - 5))
        & daily["tmin"].notna()
        & daily["tmin_hist"].notna()
    )
    daily["cw_inmet"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


def _method_ehf(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    """Excess Cold Factor (ECF) method, exposed as the "EHF" method code.

    R quirk preserved: `.cw_method_ehf` globally re-sorts `daily` by
    `date_day` only (ignoring station grouping) before computing, which
    changes the row order of the returned `daily` table whenever "EHF" is
    among the requested methods. `t5` (P5 of tmean) and the anomaly
    reference column both use the *full* per-station series, ignoring
    `baseline_start`/`baseline_end`/`percentile` entirely (see IDEIAS.md).
    """
    daily = daily.sort_values("date_day", kind="stable").reset_index(drop=True)

    def _one_station(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        tmean = g["tmean"].to_numpy(dtype=float)
        t3 = g["tmean"].rolling(3, min_periods=3).mean().to_numpy()
        t30 = g["tmean"].shift(3).rolling(30, min_periods=1).mean().to_numpy()
        t5 = _pctl_or_nan(tmean, 5, min_n=1)

        eci_sig = t5 - t3
        eci_acc = t30 - t3
        ecf = eci_sig * np.maximum(1.0, eci_acc)
        below_ehf = (ecf > 0) & ~np.isnan(ecf)

        # Forward-looking 3-day window (today + next 2 days), partial at the
        # tail — R: slide_lgl(below_ehf, any, .before=0, .after=2, .complete=FALSE)
        reversed_below = below_ehf[::-1]
        raw = (
            pd.Series(reversed_below)
            .rolling(3, min_periods=1)
            .max()
            .to_numpy()
            .astype(bool)[::-1]
        )
        cw_ehf = _consecutive_flag(raw, dur)
        ecf_value = np.where(cw_ehf, ecf, np.nan)

        g["ecf"] = ecf
        g["cw_ehf"] = cw_ehf
        g["ecf_value"] = ecf_value
        return g

    # ponytail: FutureWarning about grouping-column inclusion in .apply() is
    # cosmetic on current pandas; include_groups=False would need _one_station
    # to re-attach station_code manually. Revisit if pandas removes the
    # deprecated behavior (see IDEIAS.md).
    daily = daily.groupby("station_code", sort=False, group_keys=False).apply(_one_station)
    return daily.reset_index(drop=True)


def _method_utci(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    if "utci_min" not in daily.columns or daily["utci_min"].isna().all():
        daily["cw_utci"] = np.nan
        return daily
    below = (
        daily["utci_min"].notna()
        & daily["utci_p"].notna()
        & (daily["utci_min"] < daily["utci_p"])
    )
    daily["cw_utci"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


def _method_wbgt(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    if "wbgt_min" not in daily.columns or daily["wbgt_min"].isna().all():
        daily["cw_wbgt"] = np.nan
        return daily
    below = (
        daily["wbgt_min"].notna()
        & daily["wbgt_p"].notna()
        & (daily["wbgt_min"] < daily["wbgt_p"])
    )
    daily["cw_wbgt"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


def _method_hi(daily: pd.DataFrame, dur: int) -> pd.DataFrame:
    daily = daily.copy()
    if "hi_min" not in daily.columns or daily["hi_min"].isna().all():
        daily["cw_hi"] = np.nan
        return daily
    below = daily["hi_min"].notna() & daily["hi_p"].notna() & (daily["hi_min"] < daily["hi_p"])
    daily["cw_hi"] = _grouped_consecutive_flag(below, daily["station_code"], dur)
    return daily


_METHOD_FUNCS = {
    "WHO": _method_who, "WMO": _method_wmo, "INMET": _method_inmet,
    "EHF": _method_ehf, "UTCI": _method_utci, "WBGT": _method_wbgt, "HI": _method_hi,
}


def _apply_all_methods(
    daily: pd.DataFrame, methods: list[str], min_duration: int | None
) -> pd.DataFrame:
    for m in methods:
        dur = min_duration if min_duration is not None else _DEFAULT_MIN_DURATION[m]
        daily = _METHOD_FUNCS[m](daily, dur)

    cw_cols = [f"cw_{m.lower()}" for m in methods if f"cw_{m.lower()}" in daily.columns]
    if cw_cols:
        daily = daily.copy()
        daily["cw_any"] = daily[cw_cols].astype(float).fillna(0).sum(axis=1) > 0
    return daily


# ---------------------------------------------------------------------------
# 5. Event extraction
# ---------------------------------------------------------------------------


def _empty_events_df() -> pd.DataFrame:
    return pd.DataFrame({c: pd.Series(dtype="object") for c in _EVENTS_COLUMNS})


def _extract_events(daily: pd.DataFrame, methods: list[str]) -> pd.DataFrame:
    cw_cols = [f"cw_{m.lower()}" for m in methods if f"cw_{m.lower()}" in daily.columns]
    if not cw_cols:
        return _empty_events_df()

    per_method_frames: list[pd.DataFrame] = []

    for col in cw_cols:
        m_name = col[len("cw_"):].upper()
        temp_col = _TEMP_COL_MAP.get(m_name, "tmin")
        ref_col = _REF_COL_MAP.get(m_name, "tmin_p")

        method_rows: list[dict[str, Any]] = []
        for station, sub in daily.groupby("station_code", sort=False):
            sub = sub.sort_values("date_day")
            raw_flag = sub[col].to_numpy()
            flag = np.array([False if pd.isna(v) else bool(v) for v in raw_flag])
            if not flag.any():
                continue

            n = len(flag)
            change = np.ones(n, dtype=bool)
            change[1:] = flag[1:] != flag[:-1]
            run_id = np.cumsum(change) - 1
            run_len = np.bincount(run_id)
            run_starts = np.where(change)[0]

            for rid, start_pos in enumerate(run_starts):
                if not flag[start_pos]:
                    continue
                end_pos = start_pos + run_len[rid] - 1
                seg = sub.iloc[start_pos : end_pos + 1]

                n_seg = len(seg)
                temp_vals = (
                    seg[temp_col].to_numpy(dtype=float)
                    if temp_col in seg.columns else np.full(n_seg, np.nan)
                )
                ref_vals = (
                    seg[ref_col].to_numpy(dtype=float)
                    if ref_col in seg.columns else np.full(n_seg, np.nan)
                )
                anomaly = ref_vals - temp_vals

                ecf_peak_val: float = np.nan
                ecf_mean_val: float = np.nan
                if m_name == "EHF" and "ecf_value" in seg.columns:
                    ev_vals = seg["ecf_value"].to_numpy(dtype=float)
                    if np.any(~np.isnan(ev_vals)):
                        ecf_peak_val = float(np.nanmax(ev_vals))
                        ecf_mean_val = float(np.nanmean(ev_vals))

                dur_days = int((seg["date_day"].max() - seg["date_day"].min()).days) + 1
                mean_anomaly = _mean_or_nan(anomaly)

                method_rows.append({
                    "station_code": station,
                    "method": m_name,
                    "start_date": seg["date_day"].min(),
                    "end_date": seg["date_day"].max(),
                    "duration_days": dur_days,
                    "temp_mean": _mean_or_nan(temp_vals),
                    "temp_peak": (
                        np.nan if np.all(np.isnan(temp_vals)) else float(np.nanmin(temp_vals))
                    ),
                    "anomaly_mean": mean_anomaly,
                    "anomaly_cumulative": float(np.nansum(anomaly)),
                    "severity_index": dur_days * mean_anomaly,
                    "ecf_peak": ecf_peak_val,
                    "ecf_mean": ecf_mean_val,
                })
                row = method_rows[-1]
                for meta_col in (
                    "region", "federal_unit", "station_name",
                    "zona_climatica", "latitude", "longitude",
                ):
                    row[meta_col] = seg[meta_col].iloc[0] if meta_col in seg.columns else np.nan

        if not method_rows:
            continue

        ev_df = pd.DataFrame(method_rows).sort_values("start_date").reset_index(drop=True)
        ev_df["event_id"] = [
            f"{row.station_code}_{m_name}_{i + 1}" for i, row in enumerate(ev_df.itertuples())
        ]

        if m_name == "EHF":
            all_pos: list[float] = []
            for st in ev_df["station_code"].unique():
                v = daily.loc[daily["station_code"] == st, "ecf_value"].dropna()
                all_pos.extend(v[v > 0].tolist())
            if all_pos:
                ecf85 = float(np.percentile(all_pos, 85))

                def classify(peak: float, ecf85: float = ecf85) -> Any:
                    if pd.isna(peak):
                        return np.nan
                    if peak >= 3 * ecf85:
                        return "Extreme (ECW)"
                    if peak >= ecf85:
                        return "Severe (SCW)"
                    if peak > 0:
                        return "Low Intensity (LICW)"
                    return np.nan

                ev_df["intensity_class"] = ev_df["ecf_peak"].map(classify)
            else:
                ev_df["intensity_class"] = np.nan
        else:
            ev_df["intensity_class"] = np.nan

        per_method_frames.append(ev_df)

    if not per_method_frames:
        return _empty_events_df()

    events = pd.concat(per_method_frames, ignore_index=True)
    events = events.sort_values(["station_code", "method", "start_date"]).reset_index(drop=True)
    return events[_EVENTS_COLUMNS]


# ---------------------------------------------------------------------------
# 6. Annual summary
# ---------------------------------------------------------------------------


def _build_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS_EMPTY)

    ev = events.copy()
    ev["year"] = pd.to_datetime(ev["start_date"]).dt.year

    group_cols = ["year", "station_code", "method", "region", "federal_unit", "zona_climatica"]
    # dropna=False: region/federal_unit/zona_climatica are commonly all-NaN
    # (station metadata absent) and pandas' default groupby drops NaN keys,
    # which would silently empty this summary — R's `.by=` grouping does not.
    summary = ev.groupby(group_cols, dropna=False).agg(
        n_events=("method", "size"),
        total_days_cw=("duration_days", "sum"),
        mean_duration=("duration_days", "mean"),
        max_duration=("duration_days", "max"),
        mean_intensity=("temp_mean", "mean"),
        max_intensity=("temp_peak", "min"),
        mean_anomaly=("anomaly_mean", "mean"),
        severity_total=("severity_index", "sum"),
    ).reset_index()

    return summary.sort_values(["station_code", "method", "year"]).reset_index(drop=True)
