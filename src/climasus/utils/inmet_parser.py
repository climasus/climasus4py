"""inmet_parser.py — INMET CSV file parser.

Mirrors R: .parse_inmet_csv() (internal, climasus4r)

INMET distributes one CSV file per automatic station per year, with a
multi-line metadata header (8 lines) followed by the hourly data block.
Column names and separators vary by year — this module handles all known
variants and normalises them to the canonical ClimaSUS names.

Canonical output columns
------------------------
station_code, station_name, region, UF, latitude, longitude, altitude,
date (UTC, datetime64[ns]), year,
rainfall_mm, patm_mb, patm_max_mb, patm_min_mb,
sr_kj_m2,
tair_dry_bulb_c, tair_max_c, tair_min_c,
dew_tmean_c, dew_tmax_c, dew_tmin_c,
rh_mean_porc, rh_max_porc, rh_min_porc,
ws_2_m_s, ws_gust_m_s, wd_degrees
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Canonical column mapping — raw INMET names → ClimaSUS names
# Keys are lowercased + stripped for fuzzy matching.
# ---------------------------------------------------------------------------

_COL_MAP: dict[str, str] = {
    # precipitation
    "precipitacao total  horario  mm":        "rainfall_mm",
    "precipitacao total, horario (mm)":       "rainfall_mm",
    "precipitacao total (mm)":                "rainfall_mm",
    # atmospheric pressure
    "pressao atmosferica ao nivel da estacao, horaria  mb": "patm_mb",
    "pressao atmosferica ao nivel da estacao (mb)":         "patm_mb",
    "pressao atmosferica max.na hora ant. (aut) (mb)":      "patm_max_mb",
    "pressao atmosferica min. na hora ant. (aut) (mb)":     "patm_min_mb",
    # solar radiation
    "radiacao global  kj m²":                "sr_kj_m2",
    "radiacao global (kj/m²)":               "sr_kj_m2",
    "radiacao global (kj/m2)":               "sr_kj_m2",
    # temperature
    "temperatura do ar - bulbo seco, horaria  °c": "tair_dry_bulb_c",
    "temperatura do ar - bulbo seco (°c)":          "tair_dry_bulb_c",
    "temperatura maxima na hora ant. (aut) (°c)":   "tair_max_c",
    "temperatura minima na hora ant. (aut) (°c)":   "tair_min_c",
    # dew point
    "temperatura do ponto de orvalho  °c":          "dew_tmean_c",
    "temperatura do ponto de orvalho (°c)":         "dew_tmean_c",
    "temperatura max. do ponto de orvalho (aut) (°c)": "dew_tmax_c",
    "temperatura min. do ponto de orvalho (aut) (°c)": "dew_tmin_c",
    # relative humidity
    "umidade relativa do ar, horaria  %":     "rh_mean_porc",
    "umidade relativa do ar (%)":             "rh_mean_porc",
    "umidade rel. max. na hora ant. (aut) (%)": "rh_max_porc",
    "umidade rel. min. na hora ant. (aut) (%)": "rh_min_porc",
    # wind
    "vento, velocidade horaria  m s":         "ws_2_m_s",
    "vento, velocidade horaria (m/s)":        "ws_2_m_s",
    "vento, rajada maxima  m s":              "ws_gust_m_s",
    "vento, rajada maxima (m/s)":             "ws_gust_m_s",
    "vento, direcao horaria  gr":             "wd_degrees",
    "vento, direcao horaria (gr)":            "wd_degrees",
}

# Physical range limits: (min, max)
_PHYSICAL_LIMITS: dict[str, tuple[float, float]] = {
    "rainfall_mm":     (0.0, 500.0),
    "patm_mb":         (700.0, 1100.0),
    "patm_max_mb":     (700.0, 1100.0),
    "patm_min_mb":     (700.0, 1100.0),
    "sr_kj_m2":        (0.0, 40000.0),
    "tair_dry_bulb_c": (-90.0, 60.0),
    "tair_max_c":      (-90.0, 60.0),
    "tair_min_c":      (-90.0, 60.0),
    "dew_tmean_c":     (-90.0, 60.0),
    "dew_tmax_c":      (-90.0, 60.0),
    "dew_tmin_c":      (-90.0, 60.0),
    "rh_mean_porc":    (0.0, 100.0),
    "rh_max_porc":     (0.0, 100.0),
    "rh_min_porc":     (0.0, 100.0),
    "ws_2_m_s":        (0.0, 100.0),
    "ws_gust_m_s":     (0.0, 100.0),
    "wd_degrees":      (0.0, 360.0),
}

# INMET header keys (row 0–7 of each CSV)
_HEADER_KEYS = {
    "regiao":         "region",
    "uf":             "UF",
    "estacao":        "station_name",
    "codigo estacao": "station_code",
    "latitude":       "latitude",
    "longitude":      "longitude",
    "altitude":       "altitude",
}


# ---------------------------------------------------------------------------
# Public parser
# ---------------------------------------------------------------------------

def parse_inmet_csv(path: str | Path) -> pd.DataFrame | None:
    """Parse a single INMET hourly CSV file into a normalised DataFrame.

    Parameters
    ----------
    path:
        Path to a .csv file from INMET's historical data ZIP.

    Returns
    -------
    pd.DataFrame or None
        Normalised DataFrame with canonical columns, or None on failure.
    """
    path = Path(path)
    try:
        return _parse(path)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse(path: Path) -> pd.DataFrame:
    raw = path.read_text(encoding="latin-1", errors="replace")
    lines = raw.splitlines()

    # --- extract metadata from header (first 8 lines) -----------------------
    meta: dict[str, str] = {}
    header_end = 0
    for i, line in enumerate(lines[:10]):
        parts = line.split(";", maxsplit=1)
        if len(parts) == 2:
            key = parts[0].strip().lower().rstrip(":")
            val = parts[1].strip().strip('"')
            canonical = _HEADER_KEYS.get(key)
            if canonical:
                meta[canonical] = val
        # Data block starts after the row containing "DATA"
        if "DATA" in line.upper() and ";" in line:
            header_end = i
            break

    # --- read data block -----------------------------------------------------
    data_lines = lines[header_end:]
    if not data_lines:
        return pd.DataFrame()

    # Use the first data_lines entry as header; handle BOM
    header_line = data_lines[0].lstrip("\ufeff")
    sep = ";" if ";" in header_line else ","

    from io import StringIO
    df = pd.read_csv(
        StringIO("\n".join(data_lines)),
        sep=sep,
        encoding="utf-8",
        na_values=["", "null", "NULL", "-9999", "-9999.0", "///"],
        decimal=",",
        dtype=str,
        low_memory=False,
    )

    if df.empty:
        return pd.DataFrame()

    # --- normalise column names ----------------------------------------------
    df = _rename_columns(df)

    # --- parse date + time ---------------------------------------------------
    df = _parse_datetime(df)

    # --- coerce numerics + replace commas ------------------------------------
    numeric_cols = set(_PHYSICAL_LIMITS.keys())
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )

    # --- QC: physical range --------------------------------------------------
    df = _apply_physical_qc(df)

    # --- QC: dew point consistency (Magnus formula) -------------------------
    df = _qc_dew_point(df)

    # --- QC: nighttime solar radiation → 0 ----------------------------------
    df = _qc_solar_radiation(df)

    # --- attach station metadata from header --------------------------------
    for col, val in meta.items():
        if col not in df.columns:
            df[col] = val

    for coord_col in ("latitude", "longitude", "altitude"):
        if coord_col in df.columns:
            df[coord_col] = pd.to_numeric(
                df[coord_col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )

    return df.reset_index(drop=True)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw INMET column names to canonical ClimaSUS names."""
    mapping: dict[str, str] = {}
    for col in df.columns:
        normalised = _normalise_key(col)
        canonical = _COL_MAP.get(normalised)
        if canonical:
            mapping[col] = canonical
    return df.rename(columns=mapping)


def _normalise_key(s: str) -> str:
    """Lowercase, strip and collapse whitespace for fuzzy key matching."""
    return re.sub(r"\s+", " ", s.strip().lower())


def _parse_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Combine DATA + HORA columns into a UTC datetime column."""
    date_col = next(
        (c for c in df.columns if re.match(r"data", c, re.I)), None
    )
    time_col = next(
        (c for c in df.columns if re.match(r"hora", c, re.I)), None
    )

    if date_col is None:
        df["date"] = pd.NaT
        return df

    date_str = df[date_col].astype(str).str.strip()

    if time_col is not None:
        # HORA can be "0000 UTC", "00:00", "0" etc.
        hour_str = (
            df[time_col]
            .astype(str)
            .str.replace(r"\s*UTC", "", regex=True)
            .str.strip()
            .str.zfill(4)
            .str[:2]  # keep HH only
        )
        combined = date_str + " " + hour_str + ":00"
    else:
        combined = date_str

    df["date"] = pd.to_datetime(combined, errors="coerce", utc=True)
    return df


def _apply_physical_qc(df: pd.DataFrame) -> pd.DataFrame:
    """Set values outside physical limits to NaN."""
    for col, (lo, hi) in _PHYSICAL_LIMITS.items():
        if col in df.columns:
            mask = df[col].notna() & ((df[col] < lo) | (df[col] > hi))
            df.loc[mask, col] = np.nan
    return df


def _qc_dew_point(df: pd.DataFrame) -> pd.DataFrame:
    """Validate dew point via Magnus formula; outliers → NaN."""
    t_col = "tair_dry_bulb_c"
    rh_col = "rh_mean_porc"
    dp_col = "dew_tmean_c"

    if not all(c in df.columns for c in (t_col, rh_col, dp_col)):
        return df

    T = df[t_col]
    RH = df[rh_col]
    # Magnus approximation: Td = (243.04 * γ) / (17.625 - γ)
    # where γ = ln(RH/100) + (17.625 * T) / (243.04 + T)
    with np.errstate(divide="ignore", invalid="ignore"):
        gamma = np.log(RH / 100.0) + (17.625 * T) / (243.04 + T)
        td_calc = (243.04 * gamma) / (17.625 - gamma)

    diff = (df[dp_col] - td_calc).abs()
    df.loc[diff > 3.0, dp_col] = np.nan

    return df


def _qc_solar_radiation(df: pd.DataFrame) -> pd.DataFrame:
    """Set nighttime solar radiation (18h–6h UTC) to 0."""
    sr_col = "sr_kj_m2"
    if sr_col not in df.columns or "date" not in df.columns:
        return df

    hour = df["date"].dt.hour
    nighttime = (hour >= 18) | (hour < 6)
    df.loc[nighttime & df[sr_col].notna(), sr_col] = 0.0

    return df
