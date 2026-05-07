"""Bioclimatic and thermal-stress indicators — lazy SQL macros for INMET data.

Mirrors R: sus_climate_compute_indicators (Rothfusz 1990, Thom 1959, etc.)
Lazy ponta a ponta: returns DuckDBPyRelation, never materialises internally.

Scientific references:
  HI  : Rothfusz (1990) NWS Tech. Attachment SR/SSD 90-23
  THI : Thom (1959) Weatherwise 12(2):57-59
  AT  : Steadman (1994) / Bureau of Meteorology apparent temperature
  WBGT: Liljegren et al. (2008) simplified (outdoor, no solar → WBGT ≈ 0.67*Twb + 0.33*Tdb)
  DDR : Daily Temperature Range — window function per station per day
  CHD : Consecutive Hot Days — 7-day rolling sum (Tmax > 32°C)
  HWD : Heat Wave Day — 3 consecutive days Tmax > 35°C
  VP  : Vapor pressure — Magnus-Tetens formula
"""

from __future__ import annotations

from typing import Sequence

import duckdb
import pandas as pd

from ..core.engine import get_connection

# ---------------------------------------------------------------------------
# Standard INMET column mapping
# ---------------------------------------------------------------------------

_INMET_COLS = {
    "T": "tair_dry_bulb_c",       # dry-bulb temperature (°C)
    "Tmax": "tair_max_c",         # max temperature (°C)
    "Tmin": "tair_min_c",         # min temperature (°C)
    "RH": "rh_mean_porc",         # relative humidity (%)
    "DEW": "dew_tmean_c",         # dew-point temperature (°C)
    "WS": "ws_2_m_s",             # wind speed at 2 m (m/s)
    "SR": "sr_kj_m2",             # solar radiation (kJ/m²)
}

# Aliases the user can provide for any station column
_STATION_HINTS = ("station_code", "station", "estacao", "cd_estacao")

# ---------------------------------------------------------------------------
# Indicator definitions: code → (output_column, required_cols, sql_expr_template)
#
# Templates are rendered by _render_sql with substituted column names.
# Window functions use {STATION_COL} and {DATE_COL} for lazy partition/order.
# ---------------------------------------------------------------------------

_INDICATOR_DEFS: dict[str, tuple[str, tuple[str, ...], str]] = {
    # ------------------------------------------------------------------
    # Heat Index (HI) — Rothfusz (1990) simplified NWS regression
    # Valid: T ≥ 27°C, RH ≥ 40% — otherwise result may be outside domain
    # ------------------------------------------------------------------
    "heat_index": (
        "hi_c",
        ("T", "RH"),
        (
            "(-8.78469475556 "
            "+ 1.61139411 * {T} "
            "+ 2.33854883889 * {RH} "
            "- 0.14611605 * {T} * {RH} "
            "- 0.012308094 * {T} * {T} "
            "- 0.016424828 * {RH} * {RH} "
            "+ 0.002211732 * {T} * {T} * {RH} "
            "+ 0.00072546 * {T} * {RH} * {RH} "
            "- 0.000003582 * {T} * {T} * {RH} * {RH}) AS hi_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Temperature-Humidity Index (THI) — Thom (1959)
    # ------------------------------------------------------------------
    "thi": (
        "thi_c",
        ("T", "RH"),
        "({T} - (0.55 - 0.0055 * {RH}) * ({T} - 14.5)) AS thi_c",
    ),
    # ------------------------------------------------------------------
    # Apparent Temperature (AT) — Steadman / Australian BOM formula
    # AT = T + 0.33 * e - 0.70 * WS - 4.00
    # where e = vapor pressure (kPa) = (RH/100) * 0.6108 * exp(17.27*T/(T+237.3))
    # ------------------------------------------------------------------
    "apparent_temperature": (
        "at_c",
        ("T", "RH", "WS"),
        (
            "({T} + 0.33 * "
            "  ((({RH} / 100.0) * 0.6108 * EXP(17.27 * {T} / ({T} + 237.3)))) "
            "- 0.70 * {WS} - 4.00) AS at_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Vapor Pressure (VP) — Magnus-Tetens formula
    # e (kPa) = (RH/100) * 0.6108 * exp(17.27*T/(T+237.3))
    # ------------------------------------------------------------------
    "vapor_pressure": (
        "vapor_pressure_kpa",
        ("T", "RH"),
        (
            "(({RH} / 100.0) * 0.6108 * EXP(17.27 * {T} / ({T} + 237.3))) "
            "AS vapor_pressure_kpa"
        ),
    ),
    # ------------------------------------------------------------------
    # Dew-Point Depression (DPD)
    # ------------------------------------------------------------------
    "dew_point_depression": (
        "dpd_c",
        ("T", "DEW"),
        "({T} - {DEW}) AS dpd_c",
    ),
    # ------------------------------------------------------------------
    # Diurnal Temperature Range (DTR) — daily max minus daily min
    # Window function: partition by station × calendar day
    # ------------------------------------------------------------------
    "diurnal_range": (
        "diurnal_range_c",
        ("T",),
        (
            "(MAX({T}) OVER (PARTITION BY {STATION_COL}, {DATE_COL}::DATE) "
            "- MIN({T}) OVER (PARTITION BY {STATION_COL}, {DATE_COL}::DATE)) "
            "AS diurnal_range_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Consecutive Hot Days (CHD) — 7-day rolling count of days Tmax > 32°C
    # Uses station × date window ordered by date
    # ------------------------------------------------------------------
    "consecutive_hot_days": (
        "consecutive_hot_days",
        ("Tmax",),
        (
            "SUM(CASE WHEN {Tmax} > 32.0 THEN 1 ELSE 0 END) "
            "OVER (PARTITION BY {STATION_COL} ORDER BY {DATE_COL} "
            "      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) "
            "AS consecutive_hot_days"
        ),
    ),
    # ------------------------------------------------------------------
    # Heat Wave Day (HWD) — 1 if Tmax > 35°C for ≥ 3 consecutive days
    # ------------------------------------------------------------------
    "heat_wave": (
        "heat_wave",
        ("Tmax",),
        (
            "CASE "
            "  WHEN {Tmax} > 35.0 "
            "  AND LAG({Tmax}, 1) OVER (PARTITION BY {STATION_COL} ORDER BY {DATE_COL}) > 35.0 "
            "  AND LAG({Tmax}, 2) OVER (PARTITION BY {STATION_COL} ORDER BY {DATE_COL}) > 35.0 "
            "  THEN 1 ELSE 0 END AS heat_wave"
        ),
    ),
}

ALL_INDICATORS: tuple[str, ...] = tuple(_INDICATOR_DEFS.keys())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _detect_station_col(rel: duckdb.DuckDBPyRelation) -> str | None:
    cols = rel.limit(0).df().columns.tolist()
    for hint in _STATION_HINTS:
        if hint in cols:
            return hint
    return None


def _detect_date_col(rel: duckdb.DuckDBPyRelation) -> str:
    cols = rel.limit(0).df().columns.tolist()
    for col in cols:
        if "date" in col.lower() or "time" in col.lower():
            return col
    raise ValueError(
        "Could not auto-detect a date/datetime column. "
        "Ensure the relation has a column whose name contains 'date' or 'time'."
    )


def _check_required_cols(
    rel: duckdb.DuckDBPyRelation,
    indicators: list[str],
) -> None:
    """Raise ValueError listing all missing columns for requested indicators."""
    available = set(rel.limit(0).df().columns.tolist())
    missing: list[str] = []
    for ind in indicators:
        _, req_keys, _ = _INDICATOR_DEFS[ind]
        for key in req_keys:
            col = _INMET_COLS.get(key, key)
            if col not in available:
                missing.append(f"'{col}' (needed by '{ind}')")
    if missing:
        raise ValueError(
            "Missing column(s) required by requested indicators:\n  "
            + "\n  ".join(missing)
            + "\nEnsure the input comes from sus_climate_inmet() or sus_climate_fill_inmet()."
        )


def _render_indicator_sql(
    ind: str,
    station_col: str | None,
    date_col: str,
) -> str:
    _, _, template = _INDICATOR_DEFS[ind]
    # Substitute real column names from INMET mapping
    sql = template
    for key, col in _INMET_COLS.items():
        sql = sql.replace("{" + key + "}", col)
    # Station and date placeholders
    sql = sql.replace("{STATION_COL}", station_col or "NULL")
    sql = sql.replace("{DATE_COL}", date_col)
    return sql


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_compute_indicators(
    rel: "duckdb.DuckDBPyRelation | pd.DataFrame",
    *,
    indicators: Sequence[str] | None = None,
    station_col: str | None = None,
    date_col: str | None = None,
    lang: str = "pt",
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Compute bioclimatic and thermal-stress indicators from INMET station data.

    All computation is performed via DuckDB SQL — the result is a lazy
    ``DuckDBPyRelation`` (no materialisation).

    Mirrors ``climasus4r::sus_climate_compute_indicators``.

    Available indicators (``code`` → output column):

    +-----------------------+--------------------+---------------------+
    | Code                  | Output column      | Required INMET cols |
    +=======================+====================+=====================+
    | ``heat_index``        | ``hi_c``           | T, RH               |
    | ``thi``               | ``thi_c``          | T, RH               |
    | ``apparent_temperature``| ``at_c``         | T, RH, WS           |
    | ``vapor_pressure``    | ``vapor_pressure_kpa``| T, RH            |
    | ``dew_point_depression``| ``dpd_c``        | T, DEW              |
    | ``diurnal_range``     | ``diurnal_range_c``| T (window)          |
    | ``consecutive_hot_days``|``consecutive_hot_days``| Tmax (window)|
    | ``heat_wave``         | ``heat_wave``      | Tmax (window)       |
    +-----------------------+--------------------+---------------------+

    Column aliases (standard INMET names):
      T=``tair_dry_bulb_c``, Tmax=``tair_max_c``, Tmin=``tair_min_c``,
      RH=``rh_mean_porc``, DEW=``dew_tmean_c``, WS=``ws_2_m_s``

    Args:
        rel: Input INMET data — lazy ``DuckDBPyRelation`` or ``pd.DataFrame``
            (output of ``sus_climate_inmet`` or ``sus_climate_fill_inmet``).
        indicators: List of indicator codes to compute, or ``None`` for all.
        station_col: Name of the station identifier column (auto-detected if ``None``).
        date_col: Name of the date/datetime column (auto-detected if ``None``).
        lang: Language for messages (``"pt"``, ``"en"``, ``"es"``).
        verbose: Print progress messages when ``True``.

    Returns:
        ``DuckDBPyRelation`` — original columns plus one new column per indicator.

    Raises:
        ValueError: If an unknown indicator code is requested, or if a required
            INMET column is missing from the input.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_climate_inmet(years=2023, uf="AM")
        >>> filled = cs.sus_climate_fill_inmet(rel, target_var="all")
        >>> indicators = cs.sus_climate_compute_indicators(filled)
        >>> hi_only = cs.sus_climate_compute_indicators(filled, indicators=["heat_index"])
    """
    conn = get_connection()

    # Normalise to DuckDBPyRelation
    if isinstance(rel, pd.DataFrame):
        _rel = conn.from_df(rel)
    else:
        _rel = rel

    # Resolve indicator list
    if indicators is None:
        ind_list = list(ALL_INDICATORS)
    else:
        ind_list = list(indicators)
        unknown = set(ind_list) - set(ALL_INDICATORS)
        if unknown:
            raise ValueError(
                f"Unknown indicator code(s): {sorted(unknown)}. "
                f"Available: {sorted(ALL_INDICATORS)}."
            )

    # Auto-detect columns
    _date_col = date_col or _detect_date_col(_rel)
    _station_col = station_col or _detect_station_col(_rel)

    # Validate required columns are present
    _check_required_cols(_rel, ind_list)

    conn.register("_climate_indicators_input", _rel)

    # Build SELECT: all original columns + indicator expressions
    original_cols = _rel.limit(0).df().columns.tolist()
    select_parts = list(original_cols)
    for ind in ind_list:
        select_parts.append(_render_indicator_sql(ind, _station_col, _date_col))

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM _climate_indicators_input"
    )

    if verbose:
        _msg = {
            "pt": f"[sus_climate_compute_indicators] indicadores={ind_list}",
            "en": f"[sus_climate_compute_indicators] indicators={ind_list}",
            "es": f"[sus_climate_compute_indicators] indicadores={ind_list}",
        }
        print(_msg.get(lang, _msg["pt"]))

    return conn.sql(sql)
