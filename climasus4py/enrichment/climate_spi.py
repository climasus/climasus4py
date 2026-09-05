"""climate_spi.py — Standardized Precipitation Index (SPI).

Mirrors R: sus_climate_compute_spi.R (McKee et al., 1993)

Not lazy — the per-municipality rolling window + gamma-distribution fit
is fundamentally row-order-dependent Python/NumPy work with no natural
DuckDB SQL expression (unlike ``climate_indicators.py``'s run-length
window functions). Accepts a ``DuckDBPyRelation`` or ``pd.DataFrame``;
a relation is materialised with a ``UserWarning`` (same "legacy path"
precedent as ``enrichment/census.py``), and a ``pd.DataFrame`` is always
returned with metadata in ``df.attrs["sus_meta"]``.
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

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Calculando SPI (Standardized Precipitation Index)",
        "missing_col": "Coluna obrigatória '{col}' não encontrada.",
        "var_not_found": "Coluna '{var}' não encontrada. Colunas disponíveis: {cols}.",
        "invalid_scales": "'scales' deve ser uma lista de inteiros >= 1.",
        "invalid_ref_period": "'ref_start' deve ser anterior a 'ref_end'.",
        "invalid_min_n": "'min_n' deve ser um inteiro >= 2.",
        "looks_daily": (
            "Os dados parecem ser diários (muitas datas por município). "
            "SPI requer dados mensais."
        ),
        "start_info": "SPI para {n_loc} município(s), {n_dates} mês/meses, {scales_str}",
        "computing_scale": "Calculando SPI-{s} → coluna '{col}'...",
        "done": "Concluído: {n_rows} linhas; {n_na} NA(s) em '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spi: a DuckDBPyRelation de entrada está sendo "
            "materializada para o cálculo com scipy/pandas — este cálculo não é "
            "expressável em SQL lazy (ajuste de distribuição gama por município)."
        ),
    },
    "en": {
        "title": "Computing SPI (Standardized Precipitation Index)",
        "missing_col": "Required column '{col}' not found.",
        "var_not_found": "Column '{var}' not found. Available columns: {cols}.",
        "invalid_scales": "'scales' must be a list of integers >= 1.",
        "invalid_ref_period": "'ref_start' must be earlier than 'ref_end'.",
        "invalid_min_n": "'min_n' must be an integer >= 2.",
        "looks_daily": (
            "Data appears to be daily (too many dates per municipality). "
            "SPI requires monthly data."
        ),
        "start_info": "SPI for {n_loc} municipality/ies, {n_dates} month(s), {scales_str}",
        "computing_scale": "Computing SPI-{s} → column '{col}'...",
        "done": "Complete: {n_rows} rows; {n_na} NA(s) in '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spi: the input DuckDBPyRelation is being "
            "materialised for the scipy/pandas computation — this cannot be "
            "expressed as lazy SQL (per-municipality gamma distribution fit)."
        ),
    },
    "es": {
        "title": "Calculando SPI (Índice Estandarizado de Precipitación)",
        "missing_col": "Columna requerida '{col}' no encontrada.",
        "var_not_found": "Columna '{var}' no encontrada. Columnas disponibles: {cols}.",
        "invalid_scales": "'scales' debe ser una lista de enteros >= 1.",
        "invalid_ref_period": "'ref_start' debe ser anterior a 'ref_end'.",
        "invalid_min_n": "'min_n' debe ser un entero >= 2.",
        "looks_daily": (
            "Los datos parecen ser diarios (demasiadas fechas por municipio). "
            "SPI requiere datos mensuales."
        ),
        "start_info": "SPI para {n_loc} municipio(s), {n_dates} mes/meses, {scales_str}",
        "computing_scale": "Calculando SPI-{s} → columna '{col}'...",
        "done": "Completo: {n_rows} filas; {n_na} NA(s) en '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spi: la DuckDBPyRelation de entrada se está "
            "materializando para el cálculo con scipy/pandas — no es expresable "
            "en SQL lazy (ajuste de distribución gamma por municipio)."
        ),
    },
}


def sus_climate_compute_spi(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    var: str = "rainfall_chirps_mm",
    scales: list[int] | tuple[int, ...] = (1, 3, 6, 12),
    ref_start: str | None = None,
    ref_end: str | None = None,
    min_n: int = 24,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Compute the Standardized Precipitation Index (SPI) at multiple scales.

    SPI (McKee et al., 1993) is a dimensionless precipitation-anomaly
    index: negative values indicate drought, positive values indicate
    wet conditions. Requires monthly precipitation data (one row per
    municipality x month) — e.g. the output of ``sus_grid_chirps`` or
    ``sus_climate_aggregate``.

    Algorithm (per municipality, per scale ``s``): compute the ``s``-month
    trailing rolling sum of precipitation, fit a gamma distribution by
    method of moments (shape = mean²/var, rate = mean/var) over the
    calibration period on positive values only, mix in the empirical
    zero-probability, and transform to a standard-normal Z-score.

    Args:
        df: Monthly precipitation data with ``code_muni`` and ``date``
            columns, plus the column named by *var*. A lazy
            ``DuckDBPyRelation`` is materialised (with a ``UserWarning``)
            since the gamma fit is not expressible in SQL.
        var: Name of the monthly precipitation column (mm).
        scales: SPI timescales in months. Default ``(1, 3, 6, 12)``.
        ref_start: Start of the calibration period (``"YYYY-MM-DD"`` or
            ``None`` for all available data).
        ref_end: End of the calibration period, or ``None``.
        min_n: Minimum number of non-NA, non-zero calibration values
            required per municipality; below this, all SPI columns for
            that municipality are ``NA``. Default ``24`` (2 years).
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        Input data plus one ``spi_{s}mo`` column per requested scale.
        Metadata in ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="spi"``).

    Raises:
        ValueError: If required columns are missing, *scales* is
            invalid, *ref_start* >= *ref_end*, or *min_n* < 2.

    Examples::

        import climasus4py as cs

        spi = cs.sus_climate_compute_spi(
            chirps_monthly, var="rainfall_chirps_mm", scales=[1, 3, 6, 12],
        )
        spi[["code_muni", "date", "spi_3mo"]].head()
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    else:
        data = df.copy()

    for col in ("code_muni", "date"):
        if col not in data.columns:
            raise ValueError(msg["missing_col"].format(col=col))
    if var not in data.columns:
        raise ValueError(
            msg["var_not_found"].format(var=var, cols=", ".join(data.columns))
        )

    scales_list = sorted({int(s) for s in scales})
    if not scales_list or any(s < 1 for s in scales_list):
        raise ValueError(msg["invalid_scales"])

    ref_start_ts = pd.Timestamp(ref_start) if ref_start is not None else None
    ref_end_ts = pd.Timestamp(ref_end) if ref_end is not None else None
    if ref_start_ts is not None and ref_end_ts is not None and ref_start_ts >= ref_end_ts:
        raise ValueError(msg["invalid_ref_period"])

    min_n = int(min_n)
    if min_n < 2:
        raise ValueError(msg["invalid_min_n"])

    data["date"] = pd.to_datetime(data["date"])
    n_loc = data["code_muni"].nunique()
    n_dates = data["date"].nunique()

    if verbose:
        scales_str = ", ".join(f"{s}mo" for s in scales_list)
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(
            "[cyan]INFO[/]  "
            + msg["start_info"].format(n_loc=n_loc, n_dates=n_dates, scales_str=scales_str)
        )

    avg_dates_per_loc = n_dates / max(n_loc, 1)
    if avg_dates_per_loc > 36 and max(scales_list) < avg_dates_per_loc / 12:
        warnings.warn(msg["looks_daily"], UserWarning, stacklevel=2)

    data = data.sort_values(["code_muni", "date"]).reset_index(drop=True)

    for s in scales_list:
        col_name = f"spi_{s}mo"
        if verbose:
            console.print("[cyan]INFO[/]  " + msg["computing_scale"].format(s=s, col=col_name))
        data[col_name] = _spi_compute_scale(
            data, var=var, s=s, ref_start=ref_start_ts, ref_end=ref_end_ts, min_n=min_n
        )

    now = datetime.now()
    scale_cols = [f"spi_{s}mo" for s in scales_list]
    ref_str = (
        f"{ref_start_ts.date() if ref_start_ts is not None else 'all'}/"
        f"{ref_end_ts.date() if ref_end_ts is not None else 'all'}"
    )
    data.attrs["sus_meta"] = {
        "stage": "climate",
        "type": "spi",
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_climate_compute_spi(): scales={'+'.join(f'{s}mo' for s in scales_list)}, "
            f"var={var}, ref={ref_str}"
        ],
    }

    if verbose:
        n_na = int(data[scale_cols[0]].isna().sum())
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(n_rows=len(data), n_na=n_na, col1=scale_cols[0])
        )

    return data


def _spi_compute_scale(
    data: pd.DataFrame,
    var: str,
    s: int,
    ref_start: pd.Timestamp | None,
    ref_end: pd.Timestamp | None,
    min_n: int,
) -> np.ndarray:
    """Compute SPI at one timescale for every municipality in *data*."""
    out = np.full(len(data), np.nan)

    for _, idx in data.groupby("code_muni", sort=False).groups.items():
        pos = data.index.get_indexer(idx)
        x = data.loc[idx, var].to_numpy(dtype=float)
        rain_roll = (
            pd.Series(x).rolling(window=s, min_periods=s).sum().to_numpy()
        )

        if ref_start is not None or ref_end is not None:
            dates = data.loc[idx, "date"]
            in_ref = pd.Series(True, index=dates.index)
            if ref_start is not None:
                in_ref &= dates >= ref_start
            if ref_end is not None:
                in_ref &= dates <= ref_end
            calib = rain_roll[in_ref.to_numpy() & ~np.isnan(rain_roll)]
        else:
            calib = rain_roll[~np.isnan(rain_roll)]

        if len(calib) < min_n:
            continue

        out[pos] = _spi_transform(rain_roll, calib)

    return out


def _spi_transform(x_full: np.ndarray, x_calib: np.ndarray) -> np.ndarray:
    """Fit a gamma distribution (method of moments) and transform to SPI.

    Mixed gamma-zero distribution (McKee et al., 1993): the empirical
    zero-probability ``p0`` handles zero-inflation common in dryland
    Brazil; positive values are fit with a gamma distribution.
    """
    try:
        from scipy import stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required to fit the gamma distribution for SPI. "
            "Install it with: pip install scipy"
        ) from exc

    nz = x_calib[(x_calib > 0) & ~np.isnan(x_calib)]
    p0 = float(np.mean(x_calib == 0))

    spi = np.full(len(x_full), np.nan)
    if len(nz) < 4:
        return spi

    mu = float(np.mean(nz))
    s2 = float(np.var(nz, ddof=1))
    if s2 <= 0 or np.isnan(mu) or np.isnan(s2):
        return spi

    shape = mu**2 / s2
    rate = mu / s2

    non_na = ~np.isnan(x_full)
    if not non_na.any():
        return spi

    xv = x_full[non_na]
    cdf_vals = np.empty(len(xv))
    zero_mask = xv == 0
    pos_mask = xv > 0
    cdf_vals[zero_mask] = p0
    cdf_vals[pos_mask] = p0 + (1 - p0) * stats.gamma.cdf(xv[pos_mask], a=shape, scale=1 / rate)

    eps = np.finfo(float).eps
    cdf_vals = np.clip(cdf_vals, eps, 1 - eps)

    spi[non_na] = stats.norm.ppf(cdf_vals)
    return spi
