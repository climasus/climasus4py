"""inmet_parser.py — INMET CSV file parser.

Mirrors R: .parse_inmet_csv() (internal, climasus4r)

INMET distributes one CSV file per automatic station per year, with a
multi-line metadata header (8 lines) followed by the hourly data block.
Column names and separators vary by year — this module handles all known
variants and normalises them to the canonical ClimaSUS names.

Canonical output columns
------------------------
date (UTC), year,
region, UF, station_name, wmo_code, latitude, longitude, altitude, founded_date,
rainfall_mm, patm_mb, patm_max_mb, patm_min_mb,
sr_kj_m2,
tair_dry_bulb_c, tair_max_c, tair_min_c,
dew_tmean_c, dew_tmax_c, dew_tmin_c,
rh_mean_porc, rh_max_porc, rh_min_porc,
ws_2_m_s, ws_gust_m_s, wd_degrees
"""

from __future__ import annotations

import re
import unicodedata
import warnings
from pathlib import Path

import duckdb

from ..core._sql import quote_ident, sql_string
from ..core.engine import get_connection

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
    "pressao atmosferica ao nivel da estacao, horaria (mb)": "patm_mb",
    "pressao atmosferica ao nivel da estacao (mb)":         "patm_mb",
    "pressao atmosferica max.na hora ant. (aut) (mb)":      "patm_max_mb",
    "pressao atmosferica min. na hora ant. (aut) (mb)":     "patm_min_mb",
    # solar radiation
    "radiacao global  kj m²":                "sr_kj_m2",
    "radiacao global (kj/m²)":               "sr_kj_m2",
    "radiacao global (kj/m2)":               "sr_kj_m2",
    # temperature
    "temperatura do ar - bulbo seco, horaria  °c": "tair_dry_bulb_c",
    "temperatura do ar - bulbo seco, horaria (°c)": "tair_dry_bulb_c",
    "temperatura do ar - bulbo seco (°c)":          "tair_dry_bulb_c",
    "temperatura maxima na hora ant. (aut) (°c)":   "tair_max_c",
    "temperatura minima na hora ant. (aut) (°c)":   "tair_min_c",
    # dew point
    "temperatura do ponto de orvalho  °c":          "dew_tmean_c",
    "temperatura do ponto de orvalho (°c)":         "dew_tmean_c",
    "temperatura max. do ponto de orvalho (aut) (°c)": "dew_tmax_c",
    "temperatura min. do ponto de orvalho (aut) (°c)": "dew_tmin_c",
    "temperatura orvalho max. na hora ant. (aut) (°c)": "dew_tmax_c",
    "temperatura orvalho min. na hora ant. (aut) (°c)": "dew_tmin_c",
    # relative humidity
    "umidade relativa do ar, horaria  %":     "rh_mean_porc",
    "umidade relativa do ar, horaria (%)":    "rh_mean_porc",
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
    "vento, direcao horaria (gr) (° (gr))":   "wd_degrees",
}

_MEASUREMENT_COLUMNS: tuple[str, ...] = (
    "rainfall_mm",
    "patm_mb",
    "patm_max_mb",
    "patm_min_mb",
    "sr_kj_m2",
    "tair_dry_bulb_c",
    "tair_max_c",
    "tair_min_c",
    "dew_tmean_c",
    "dew_tmax_c",
    "dew_tmin_c",
    "rh_mean_porc",
    "rh_max_porc",
    "rh_min_porc",
    "ws_2_m_s",
    "ws_gust_m_s",
    "wd_degrees",
)

_METADATA_COLUMNS: tuple[str, ...] = (
    "region",
    "UF",
    "station_name",
    "wmo_code",
    "latitude",
    "longitude",
    "altitude",
    "founded_date",
)

_OUTPUT_COLUMNS: tuple[str, ...] = (
    "date",
    "year",
    *_METADATA_COLUMNS,
    *_MEASUREMENT_COLUMNS,
)

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

_METADATA_KEYS = {
    "regiao": "region",
    "uf": "UF",
    "estacao": "station_name",
    "codigo estacao": "wmo_code",
    "codigo (wmo)": "wmo_code",
    "latitude": "latitude",
    "longitude": "longitude",
    "altitude": "altitude",
    "data de fundacao": "founded_date",
    "data de fundacao (yyyy-mm-dd)": "founded_date",
}


# ---------------------------------------------------------------------------
# Public parser
# ---------------------------------------------------------------------------

def parse_inmet_csv(path: str | Path) -> duckdb.DuckDBPyRelation | None:
    """Parse a single INMET hourly CSV file into a normalised DuckDB relation.

    Parameters
    ----------
    path:
        Path to a .csv file from INMET's historical data ZIP.

    Returns
    -------
    duckdb.DuckDBPyRelation or None
        Lazy relation with canonical columns, or None on failure.
    """
    path = Path(path)
    try:
        return _parse(path)
    except FileNotFoundError:
        return None
    except ValueError as exc:
        warnings.warn(str(exc), UserWarning, stacklevel=2)
        return None
    except Exception as exc:
        warnings.warn(
            f"Failed to parse INMET CSV {path}: {exc}",
            UserWarning,
            stacklevel=2,
        )
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse(path: Path) -> duckdb.DuckDBPyRelation:
    metadata = _read_inmet_metadata(path)
    header_line, raw_columns = _detect_data_header_line(path)

    conn = get_connection()
    select_clause = _build_select_clause(raw_columns, metadata)
    path_sql = sql_string(str(path).replace("\\", "/"))
    base_sql = (
        f"SELECT {select_clause} "
        f"FROM read_csv({path_sql}, "
        f"skip={header_line}, "
        "delim=';', "
        "header=true, "
        "ignore_errors=true, "
        "all_varchar=true, "
        "null_padding=true, "
        "encoding='latin-1')"
    )
    return conn.sql(_wrap_sql_qc(base_sql))


def _read_inmet_metadata(path: str | Path) -> dict[str, str]:
    """Read station metadata from an INMET CSV header."""
    path = Path(path)
    header_line, _ = _detect_data_header_line(path)
    meta: dict[str, str] = {}

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= header_line:
                break
            parts = line.rstrip("\r\n").split(";", maxsplit=1)
            if len(parts) != 2:
                continue
            key = _normalise_key(parts[0].strip().rstrip(":"))
            canonical = _METADATA_KEYS.get(key)
            if canonical:
                meta[canonical] = parts[1].strip().strip('"')

    return meta


def _detect_data_header_line(path: str | Path) -> tuple[int, list[str]]:
    """Return the line index and raw columns for the INMET data header."""
    path = Path(path)
    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, line in enumerate(f):
            if _is_data_header_line(line):
                raw_columns = [
                    col.lstrip("\ufeff").strip()
                    for col in line.rstrip("\r\n").split(";")
                    if col.strip()
                ]
                return i, raw_columns

    raise ValueError(f"INMET CSV malformed: no HORA header in {path}")


def _build_select_clause(raw_columns: list[str], metadata: dict[str, str]) -> str:
    """Build the canonical DuckDB SELECT list for one INMET CSV."""
    raw_by_canonical = _canonical_raw_columns(raw_columns)
    date_expr = _date_time_sql(raw_columns)

    parts = [
        f"{date_expr} AS {quote_ident('date')}",
        f"CAST(EXTRACT(YEAR FROM {date_expr}) AS INTEGER) AS {quote_ident('year')}",
        f"{_nullable_text_sql(metadata.get('region'))} AS {quote_ident('region')}",
        f"{_nullable_text_sql(metadata.get('UF'))} AS {quote_ident('UF')}",
        f"{_nullable_text_sql(metadata.get('station_name'))} AS {quote_ident('station_name')}",
        f"{_nullable_text_sql(metadata.get('wmo_code'))} AS {quote_ident('wmo_code')}",
        f"{_nullable_double_sql(metadata.get('latitude'))} AS {quote_ident('latitude')}",
        f"{_nullable_double_sql(metadata.get('longitude'))} AS {quote_ident('longitude')}",
        f"{_nullable_double_sql(metadata.get('altitude'))} AS {quote_ident('altitude')}",
        f"{_nullable_date_sql(metadata.get('founded_date'))} AS {quote_ident('founded_date')}",
    ]

    for canonical in _MEASUREMENT_COLUMNS:
        raw = raw_by_canonical.get(canonical)
        if raw is None:
            parts.append(f"CAST(NULL AS DOUBLE) AS {quote_ident(canonical)}")
            continue

        value_expr = _numeric_column_sql(raw)
        lo, hi = _PHYSICAL_LIMITS[canonical]
        parts.append(
            f"CASE WHEN {value_expr} BETWEEN {lo} AND {hi} "
            f"THEN {value_expr} ELSE NULL END AS {quote_ident(canonical)}"
        )

    return ", ".join(parts)


def _wrap_sql_qc(base_sql: str) -> str:
    """Apply cross-column INMET QC rules in SQL."""
    gamma_expr = (
        "LN(rh_mean_porc / 100.0) + "
        "(17.625 * tair_dry_bulb_c) / (243.04 + tair_dry_bulb_c)"
    )
    dew_calc = f"(243.04 * ({gamma_expr})) / (17.625 - ({gamma_expr}))"
    dew_expr = (
        "CASE WHEN dew_tmean_c IS NOT NULL "
        "AND tair_dry_bulb_c IS NOT NULL "
        "AND rh_mean_porc IS NOT NULL "
        "AND rh_mean_porc > 0 "
        f"AND ABS(dew_tmean_c - ({dew_calc})) > 3.0 "
        "THEN NULL ELSE dew_tmean_c END"
    )
    solar_expr = (
        "CASE WHEN sr_kj_m2 IS NOT NULL "
        "AND (EXTRACT(HOUR FROM date) >= 18 OR EXTRACT(HOUR FROM date) < 6) "
        "THEN 0.0 ELSE sr_kj_m2 END"
    )

    select_parts: list[str] = []
    for col in _OUTPUT_COLUMNS:
        if col == "dew_tmean_c":
            select_parts.append(f"{dew_expr} AS {quote_ident(col)}")
        elif col == "sr_kj_m2":
            select_parts.append(f"{solar_expr} AS {quote_ident(col)}")
        else:
            select_parts.append(quote_ident(col))

    return f"WITH base AS ({base_sql}) SELECT {', '.join(select_parts)} FROM base"


def _canonical_raw_columns(raw_columns: list[str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for raw in raw_columns:
        canonical = _COL_MAP.get(_normalise_key(raw))
        if canonical and canonical not in mapped:
            mapped[canonical] = raw
    return mapped


def _date_time_sql(raw_columns: list[str]) -> str:
    date_col = next(
        (
            col
            for col in raw_columns
            if _normalise_key(col) in {"data", "data (yyyy-mm-dd)", "data medicao"}
        ),
        None,
    )
    hour_col = next(
        (col for col in raw_columns if _normalise_key(col).startswith("hora")),
        None,
    )
    if date_col is None or hour_col is None:
        raise ValueError("INMET CSV malformed: data header lacks DATA/HORA columns")

    date_sql = (
        f"TRY_CAST(REPLACE(TRIM({quote_ident(date_col)}), '/', '-') AS DATE)"
    )
    utc_suffix_pattern = sql_string(r"\s*UTC$")
    hour_clean = (
        f"REGEXP_REPLACE(UPPER(TRIM({quote_ident(hour_col)})), "
        f"{utc_suffix_pattern}, '')"
    )
    hour_part = (
        f"CASE WHEN STRPOS({hour_clean}, ':') > 0 "
        f"THEN SPLIT_PART({hour_clean}, ':', 1) "
        f"ELSE SUBSTR({hour_clean}, 1, 2) END"
    )
    time_sql = f"TRY_CAST(LPAD({hour_part}, 2, '0') || ':00:00' AS TIME)"
    return f"({date_sql} + {time_sql})"


def _numeric_column_sql(raw_column: str) -> str:
    quoted = quote_ident(raw_column)
    return f"TRY_CAST(NULLIF(REPLACE(TRIM({quoted}), ',', '.'), '') AS DOUBLE)"


def _nullable_text_sql(value: str | None) -> str:
    if value is None or value.strip() == "":
        return "CAST(NULL AS VARCHAR)"
    return sql_string(value)


def _nullable_double_sql(value: str | None) -> str:
    if value is None or value.strip() == "":
        return "CAST(NULL AS DOUBLE)"
    return f"TRY_CAST(REPLACE({sql_string(value)}, ',', '.') AS DOUBLE)"


def _nullable_date_sql(value: str | None) -> str:
    if value is None or value.strip() == "":
        return "CAST(NULL AS DATE)"
    literal = sql_string(value)
    ddmmyy_pattern = sql_string(r"^\d{2}/\d{2}/\d{2}$")
    return (
        "CASE "
        f"WHEN REGEXP_MATCHES({literal}, {ddmmyy_pattern}) "
        f"THEN CAST(STRPTIME({literal}, '%d/%m/%y') AS DATE) "
        f"ELSE TRY_CAST({literal} AS DATE) "
        "END"
    )


def _is_data_header_line(line: str) -> bool:
    upper = line.lstrip("\ufeff").strip().upper()
    return (";HORA" in upper) or upper.startswith("DATA MEDICAO")


def _normalise_key(s: str) -> str:
    """Lowercase, strip and collapse whitespace for fuzzy key matching."""
    text = unicodedata.normalize("NFKD", s)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text.strip().lower())
