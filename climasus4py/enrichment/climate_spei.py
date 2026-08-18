"""climate_spei.py — Standardized Precipitation-Evapotranspiration Index (SPEI).

Mirrors R: sus_climate_compute_spei.R (Vicente-Serrano et al., 2010)

Extends SPI (see ``climate_spi.py``) with atmospheric water demand:
uses the climatic water balance ``D = P - PET`` instead of raw
precipitation. Not lazy, for the same reason as SPI (per-municipality
rolling window + distribution transform is Python/NumPy work).

Faithfulness note: the R docstring for this function describes fitting
a "3-parameter log-logistic distribution via L-moments" — but the R
*implementation* (``.spei_transform``) only ever uses an empirical
Hazen-plotting-position ECDF, never the log-logistic fit the docs
describe. This port replicates the R implementation's actual behaviour
(empirical ECDF), not its aspirational docstring — see IDEIAS.md.
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
        "title": "Calculando SPEI (Standardized Precipitation-Evapotranspiration Index)",
        "missing_col": "Coluna obrigatória '{col}' não encontrada.",
        "var_not_found": "Coluna de {what} '{var}' não encontrada.",
        "what_rain": "precipitação",
        "pet_col_missing": (
            "Coluna de PET '{pet_var}' não encontrada. "
            "Use pet_method='thornthwaite' ou forneça a coluna."
        ),
        "temp_col_missing": (
            "Coluna de temperatura '{temp_var}' necessária para pet_method='thornthwaite'."
        ),
        "invalid_scales": "'scales' deve ser uma lista de inteiros >= 1.",
        "invalid_ref_period": "'ref_start' deve ser anterior a 'ref_end'.",
        "invalid_min_n": "'min_n' deve ser um inteiro >= 2.",
        "start_info": "SPEI para {n_loc} município(s), {n_dates} mês/meses, PET={method}",
        "computing_pet": "Calculando PET Thornthwaite a partir de '{temp_var}'...",
        "computing_scale": "Calculando SPEI-{s} → coluna '{col}'...",
        "done": "Concluído: {n_rows} linhas; {n_na} NA(s) em '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spei: a DuckDBPyRelation de entrada está sendo "
            "materializada para o cálculo com pandas/numpy — este cálculo não é "
            "expressável em SQL lazy."
        ),
    },
    "en": {
        "title": "Computing SPEI (Standardized Precipitation-Evapotranspiration Index)",
        "missing_col": "Required column '{col}' not found.",
        "var_not_found": "Column for {what} '{var}' not found.",
        "what_rain": "precipitation",
        "pet_col_missing": (
            "PET column '{pet_var}' not found. "
            "Use pet_method='thornthwaite' or supply the column."
        ),
        "temp_col_missing": (
            "Temperature column '{temp_var}' required for pet_method='thornthwaite'."
        ),
        "invalid_scales": "'scales' must be a list of integers >= 1.",
        "invalid_ref_period": "'ref_start' must be earlier than 'ref_end'.",
        "invalid_min_n": "'min_n' must be an integer >= 2.",
        "start_info": "SPEI for {n_loc} municipality/ies, {n_dates} month(s), PET={method}",
        "computing_pet": "Computing Thornthwaite PET from '{temp_var}'...",
        "computing_scale": "Computing SPEI-{s} → column '{col}'...",
        "done": "Complete: {n_rows} rows; {n_na} NA(s) in '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spei: the input DuckDBPyRelation is being "
            "materialised for the pandas/numpy computation — this cannot be "
            "expressed as lazy SQL."
        ),
    },
    "es": {
        "title": "Calculando SPEI (Índice Estandarizado de Precipitación-Evapotranspiración)",
        "missing_col": "Columna requerida '{col}' no encontrada.",
        "var_not_found": "Columna de {what} '{var}' no encontrada.",
        "what_rain": "precipitación",
        "pet_col_missing": (
            "Columna PET '{pet_var}' no encontrada. "
            "Use pet_method='thornthwaite' o proporcione la columna."
        ),
        "temp_col_missing": (
            "Columna de temperatura '{temp_var}' requerida para pet_method='thornthwaite'."
        ),
        "invalid_scales": "'scales' debe ser una lista de enteros >= 1.",
        "invalid_ref_period": "'ref_start' debe ser anterior a 'ref_end'.",
        "invalid_min_n": "'min_n' debe ser un entero >= 2.",
        "start_info": "SPEI para {n_loc} municipio(s), {n_dates} mes/meses, PET={method}",
        "computing_pet": "Calculando PET Thornthwaite desde '{temp_var}'...",
        "computing_scale": "Calculando SPEI-{s} → columna '{col}'...",
        "done": "Completo: {n_rows} filas; {n_na} NA(s) en '{col1}'.",
        "materialize_warning": (
            "sus_climate_compute_spei: la DuckDBPyRelation de entrada se está "
            "materializando para el cálculo con pandas/numpy — no es expresable "
            "en SQL lazy."
        ),
    },
}


def sus_climate_compute_spei(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    rain_var: str = "rainfall_chirps_mm",
    pet_var: str = "pet_mm",
    pet_method: Literal["column", "thornthwaite"] = "column",
    temp_var: str = "tair_dry_bulb_c",
    scales: list[int] | tuple[int, ...] = (1, 3, 6, 12),
    ref_start: str | None = None,
    ref_end: str | None = None,
    min_n: int = 24,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Compute the Standardized Precipitation-Evapotranspiration Index (SPEI).

    SPEI (Vicente-Serrano et al., 2010) extends SPI by accounting for
    atmospheric water demand: it standardises the climatic water balance
    ``D = precipitation - PET`` instead of raw precipitation, making it
    more sensitive to warming-amplified drought.

    PET options:
        - ``pet_method="column"`` (default): use a pre-computed PET
          column via *pet_var* (e.g. ERA5-Land PET or FAO Penman-Monteith).
        - ``pet_method="thornthwaite"``: compute PET internally from
          monthly mean temperature (*temp_var*) via Thornthwaite (1948).

    Algorithm (per municipality, per scale ``s``): compute the ``s``-month
    trailing rolling sum of ``D``, then transform to a standard-normal
    Z-score via an empirical Hazen-plotting-position ECDF fit on the
    calibration period — this replicates the R implementation exactly,
    which never applies the log-logistic fit its own docstring describes
    (see the module docstring).

    Args:
        df: Monthly data with ``code_muni`` and ``date`` columns, plus
            *rain_var* and (depending on *pet_method*) *pet_var* or
            *temp_var*. A lazy ``DuckDBPyRelation`` is materialised
            (with a ``UserWarning``).
        rain_var: Name of the monthly precipitation column (mm).
        pet_var: Name of the monthly PET column (mm), used when
            ``pet_method="column"``.
        pet_method: ``"column"`` (default) or ``"thornthwaite"``.
        temp_var: Name of the monthly mean temperature column (°C), used
            when ``pet_method="thornthwaite"``.
        scales: SPEI timescales in months. Default ``(1, 3, 6, 12)``.
        ref_start: Start of the calibration period (``"YYYY-MM-DD"`` or
            ``None`` for all available data).
        ref_end: End of the calibration period, or ``None``.
        min_n: Minimum number of non-NA calibration values required per
            municipality. Default ``24`` (2 years).
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        Input data plus one ``spei_{s}mo`` column per requested scale.
        Metadata in ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="spei"``).

    Raises:
        ValueError: If required columns are missing, *scales* is
            invalid, *ref_start* >= *ref_end*, or *min_n* < 2.

    Examples::

        import climasus4py as cs

        spei = cs.sus_climate_compute_spei(
            era5_chirps, rain_var="rainfall_chirps_mm",
            pet_method="thornthwaite", temp_var="tair_dry_bulb_c",
            scales=[3, 6, 12],
        )
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if pet_method not in ("column", "thornthwaite"):
        raise ValueError("'pet_method' must be 'column' or 'thornthwaite'.")

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    else:
        data = df.copy()

    for col in ("code_muni", "date"):
        if col not in data.columns:
            raise ValueError(msg["missing_col"].format(col=col))
    if rain_var not in data.columns:
        raise ValueError(msg["var_not_found"].format(var=rain_var, what=msg["what_rain"]))
    if pet_method == "column":
        if pet_var not in data.columns:
            raise ValueError(msg["pet_col_missing"].format(pet_var=pet_var))
    elif temp_var not in data.columns:
        raise ValueError(msg["temp_col_missing"].format(temp_var=temp_var))

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
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(
            "[cyan]INFO[/]  "
            + msg["start_info"].format(n_loc=n_loc, n_dates=n_dates, method=pet_method)
        )

    data = data.sort_values(["code_muni", "date"]).reset_index(drop=True)

    if pet_method == "thornthwaite":
        if verbose:
            console.print(
                "[cyan]INFO[/]  " + msg["computing_pet"].format(temp_var=temp_var)
            )
        data["_pet_thornthwaite"] = _thornthwaite_pet(data, temp_var=temp_var)
        pet_var = "_pet_thornthwaite"

    for s in scales_list:
        col_name = f"spei_{s}mo"
        if verbose:
            console.print(
                "[cyan]INFO[/]  " + msg["computing_scale"].format(s=s, col=col_name)
            )
        data[col_name] = _spei_compute_scale(
            data,
            rain_var=rain_var,
            pet_var=pet_var,
            s=s,
            ref_start=ref_start_ts,
            ref_end=ref_end_ts,
            min_n=min_n,
        )

    if "_pet_thornthwaite" in data.columns:
        data = data.drop(columns=["_pet_thornthwaite"])

    now = datetime.now()
    scale_cols = [f"spei_{s}mo" for s in scales_list]
    ref_str = (
        f"{ref_start_ts.date() if ref_start_ts is not None else 'all'}/"
        f"{ref_end_ts.date() if ref_end_ts is not None else 'all'}"
    )
    data.attrs["sus_meta"] = {
        "stage": "climate",
        "type": "spei",
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_climate_compute_spei(): scales={'+'.join(f'{s}mo' for s in scales_list)}, "
            f"pet={pet_method}, ref={ref_str}"
        ],
    }

    if verbose:
        n_na = int(data[scale_cols[0]].isna().sum())
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(n_rows=len(data), n_na=n_na, col1=scale_cols[0])
        )

    return data


def _spei_compute_scale(
    data: pd.DataFrame,
    rain_var: str,
    pet_var: str,
    s: int,
    ref_start: pd.Timestamp | None,
    ref_end: pd.Timestamp | None,
    min_n: int,
) -> np.ndarray:
    """Compute SPEI at one timescale for every municipality in *data*."""
    out = np.full(len(data), np.nan)

    for _, idx in data.groupby("code_muni", sort=False).groups.items():
        pos = data.index.get_indexer(idx)
        water_balance = (
            data.loc[idx, rain_var].to_numpy(dtype=float)
            - data.loc[idx, pet_var].to_numpy(dtype=float)
        )
        d_roll = (
            pd.Series(water_balance).rolling(window=s, min_periods=s).sum().to_numpy()
        )

        if ref_start is not None or ref_end is not None:
            dates = data.loc[idx, "date"]
            in_ref = pd.Series(True, index=dates.index)
            if ref_start is not None:
                in_ref &= dates >= ref_start
            if ref_end is not None:
                in_ref &= dates <= ref_end
            calib = d_roll[in_ref.to_numpy() & ~np.isnan(d_roll)]
        else:
            calib = d_roll[~np.isnan(d_roll)]

        if len(calib) < min_n:
            continue

        out[pos] = _spei_transform(d_roll, calib)

    return out


def _spei_transform(x_full: np.ndarray, x_calib: np.ndarray) -> np.ndarray:
    """Transform to SPEI using an empirical ECDF (Hazen plotting positions).

    Matches the R implementation exactly: a Hazen-corrected empirical
    CDF from the calibration period, then ``qnorm``. Produces mean ~0
    and sd ~1 over the calibration period by construction, and is valid
    for any shape of the water-balance distribution (positive, negative,
    or skewed) — no distributional assumption is made.
    """
    try:
        from scipy import stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required for the normal-quantile transform in SPEI. "
            "Install it with: pip install scipy"
        ) from exc

    valid_calib = x_calib[~np.isnan(x_calib)]
    n = len(valid_calib)
    spei = np.full(len(x_full), np.nan)
    if n < 4:
        return spei

    calib_sorted = np.sort(valid_calib)

    non_na = ~np.isnan(x_full)
    if not non_na.any():
        return spei

    xv = x_full[non_na]
    # findInterval(x, sorted_calib, rightmost.closed = TRUE): count of
    # calibration values <= x (numpy searchsorted, side="right").
    ranks = np.searchsorted(calib_sorted, xv, side="right")
    p_hazen = (ranks - 0.5) / n
    p_hazen = np.clip(p_hazen, 1e-6, 1 - 1e-6)

    spei[non_na] = stats.norm.ppf(p_hazen)
    return spei


def _thornthwaite_pet(data: pd.DataFrame, temp_var: str) -> np.ndarray:
    """Compute Thornthwaite (1948) monthly PET (mm) for every row.

    The annual heat index ``I`` is derived from the mean monthly
    temperature *per calendar month* (a typical 12-month annual cycle),
    as in the original method — not from the full record length. No
    day-length latitude correction is applied (Thornthwaite's original
    simplification, appropriate for tropical Brazil where day length is
    ~12h year-round).
    """
    pet = np.full(len(data), np.nan)

    for _, idx in data.groupby("code_muni", sort=False).groups.items():
        pos = data.index.get_indexer(idx)
        t_mo = data.loc[idx, temp_var].to_numpy(dtype=float)
        dates = data.loc[idx, "date"]
        month_num = dates.dt.month.to_numpy()

        t_month_means = np.array([
            np.mean(np.maximum(0.0, t_mo[(month_num == m) & ~np.isnan(t_mo)]))
            if np.any((month_num == m) & ~np.isnan(t_mo))
            else np.nan
            for m in range(1, 13)
        ])
        i_monthly = (t_month_means / 5) ** 1.514
        heat_index = float(np.nansum(i_monthly))

        if heat_index <= 0:
            pet[pos] = 0.0
            continue

        a = (
            6.75e-7 * heat_index**3
            - 7.71e-5 * heat_index**2
            + 1.792e-2 * heat_index
            + 0.49239
        )

        days_in_month = dates.dt.days_in_month.to_numpy()
        pet_unadj = np.where(
            (t_mo <= 0) | np.isnan(t_mo),
            0.0,
            16 * (10 * np.clip(t_mo, 1e-9, None) / heat_index) ** a,
        )
        pet[pos] = pet_unadj * (days_in_month / 30)

    return pet
