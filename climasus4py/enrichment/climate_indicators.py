"""Bioclimatic and thermal-stress indicators — lazy SQL macros for INMET data.

Mirrors R: sus_climate_compute_indicators (Rothfusz 1990, Thom 1959, etc.)
Lazy ponta a ponta: returns DuckDBPyRelation, never materialises internally.

Scientific references:
  HI   : Rothfusz (1990) NWS Tech. Attachment SR/SSD 90-23
         (domain guard: T >= 27°C and RH >= 40%; outside the domain the
         macro returns NULL — the regression is undefined there.)
  THI  : Thom (1959) Weatherwise 12(2):57-59
  AT   : Steadman (1994) / Bureau of Meteorology apparent temperature
  WBGT : simplified outdoor-no-globe form
         (0.67 * Twb + 0.33 * Tdb), Twb via Stull (2011) wet-bulb estimator
         from T and RH (J. Appl. Meteorol. Climatol. 50:2267-2269).
  DPD  : Dew-point depression
  VP   : Vapor pressure — Magnus-Tetens formula
  DTR  : Diurnal temperature range — window per station per calendar day
  CHD  : Consecutive Hot Days — gaps-and-islands run length
         (number of consecutive days ending today with Tmax > 32°C).
         Compatible with ETCCDI CDD-style indicators.
  HWD  : Heat Wave Day — every day belonging to a run of >= 3 consecutive
         days with Tmax > 35°C. Run-length-aware (no false 0 on the first
         two days of an episode).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

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

# Sentinel SQL fragments — CHD and HWD are rendered specially against a
# gaps-and-islands CTE built in sus_climate_compute_indicators().
_SENTINEL_CHD = "__USES_CHD_RUN__"
_SENTINEL_HW = "__USES_HW_RUN__"

# Run thresholds (kept in sync with the CTE built below)
_CHD_THRESHOLD = 32.0
_HW_THRESHOLD = 35.0
_HW_MIN_RUN = 3

# ---------------------------------------------------------------------------
# Indicator definitions: code → (output_column, required_keys, sql_template)
# Templates are rendered by _render_sql with substituted column names.
# ---------------------------------------------------------------------------

_INDICATOR_DEFS: dict[str, tuple[str, tuple[str, ...], str]] = {
    # ------------------------------------------------------------------
    # Heat Index (HI) — Rothfusz (1990) simplified NWS regression.
    # Guard: T >= 27°C and RH >= 40%. Outside that domain the polynomial
    # produces values that are smaller than T, which is biologically
    # meaningless — return NULL so consumers do not silently use bad data.
    # ------------------------------------------------------------------
    "heat_index": (
        "hi_c",
        ("T", "RH"),
        (
            "CASE WHEN {T} >= 27.0 AND {RH} >= 40.0 THEN "
            "(-8.78469475556 "
            "+ 1.61139411 * {T} "
            "+ 2.33854883889 * {RH} "
            "- 0.14611605 * {T} * {RH} "
            "- 0.012308094 * {T} * {T} "
            "- 0.016424828 * {RH} * {RH} "
            "+ 0.002211732 * {T} * {T} * {RH} "
            "+ 0.00072546 * {T} * {RH} * {RH} "
            "- 0.000003582 * {T} * {T} * {RH} * {RH}) "
            "ELSE NULL END AS hi_c"
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
    # where e (kPa) = (RH/100) * 0.6108 * exp(17.27*T/(T+237.3))
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
    # Wet-Bulb Globe Temperature (WBGT) — simplified outdoor form,
    # 0.67 * Twb + 0.33 * Tdb, with Twb estimated from T and RH using
    # Stull (2011), J. Appl. Meteorol. Climatol. 50:2267-2269.
    # No globe sensor input — this is the field-research approximation.
    # ------------------------------------------------------------------
    "wbgt": (
        "wbgt_c",
        ("T", "RH"),
        (
            "(0.67 * "
            "  ({T} * atan(0.151977 * sqrt({RH} + 8.313659)) "
            "   + atan({T} + {RH}) "
            "   - atan({RH} - 1.676331) "
            "   + 0.00391838 * power({RH}, 1.5) * atan(0.023101 * {RH}) "
            "   - 4.686035) "
            " + 0.33 * {T}) AS wbgt_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Vapor Pressure (VP) — Magnus-Tetens formula
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
    # Consecutive Hot Days (CHD) — sentinel; rendered against the
    # gaps-and-islands run CTE built in sus_climate_compute_indicators.
    # ------------------------------------------------------------------
    "consecutive_hot_days": (
        "consecutive_hot_days",
        ("Tmax",),
        _SENTINEL_CHD,
    ),
    # ------------------------------------------------------------------
    # Heat Wave Day (HWD) — sentinel; flags every day that belongs to a
    # run of >= 3 consecutive days with Tmax > 35°C (run-length-aware).
    # ------------------------------------------------------------------
    "heat_wave": (
        "heat_wave",
        ("Tmax",),
        _SENTINEL_HW,
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


def _substitute_inmet_cols(template: str, station_col: str, date_col: str) -> str:
    """Replace {T}, {Tmax}, {STATION_COL}, {DATE_COL} placeholders."""
    out = template
    for key, col in _INMET_COLS.items():
        out = out.replace("{" + key + "}", col)
    out = out.replace("{STATION_COL}", station_col)
    out = out.replace("{DATE_COL}", date_col)
    return out


def _render_indicator_sql(
    ind: str,
    station_col: str,
    date_col: str,
) -> str:
    """Render a regular indicator template; CHD/HWD use special render below."""
    _, _, template = _INDICATOR_DEFS[ind]
    return _substitute_inmet_cols(template, station_col, date_col)


def _render_chd_expr(station_col: str, date_col: str) -> str:
    """Run-length count of consecutive days ending today with Tmax > 32°C.

    Partition includes ``__is_hot32`` so cold days in the surrounding
    run-id bucket do not poison the count of adjacent hot days.
    """
    tmax = _INMET_COLS["Tmax"]
    return (
        f"CASE WHEN {tmax} > {_CHD_THRESHOLD} THEN "
        f"  COUNT(*) OVER ("
        f"    PARTITION BY {station_col}, __is_hot32, __chd_run "
        f"    ORDER BY {date_col} "
        f"    ROWS UNBOUNDED PRECEDING"
        f"  ) "
        f"ELSE 0 END AS consecutive_hot_days"
    )


def _render_hw_expr(station_col: str) -> str:
    """Flag every day belonging to a run of >= 3 days with Tmax > 35°C."""
    tmax = _INMET_COLS["Tmax"]
    return (
        f"CASE WHEN {tmax} > {_HW_THRESHOLD} "
        f"  AND COUNT(*) OVER ("
        f"    PARTITION BY {station_col}, __is_hot35, __hw_run"
        f"  ) >= {_HW_MIN_RUN} "
        f"THEN 1 ELSE 0 END AS heat_wave"
    )


def _build_runs_cte(
    needs_chd: bool,
    needs_hw: bool,
    station_col: str,
    date_col: str,
    input_view: str,
    cte_name: str,
) -> tuple[str, str, list[str]]:
    """Return (cte_sql, source_table, helper_cols) for run-based indicators.

    The helper columns must be EXCLUDEd in the final SELECT.
    """
    if not (needs_chd or needs_hw):
        return "", input_view, []
    tmax = _INMET_COLS["Tmax"]
    parts: list[str] = []
    helpers: list[str] = []
    if needs_chd:
        # Boolean marker — partition key for the count below
        parts.append(
            f"CASE WHEN {tmax} > {_CHD_THRESHOLD} THEN 1 ELSE 0 END AS __is_hot32"
        )
        # Sum-cumulative of "break" rows — same value within a run, but cold
        # rows can share the same run id with adjacent hot rows; combining
        # with __is_hot32 in PARTITION BY isolates the hot-run subset.
        parts.append(
            f"SUM(CASE WHEN ({tmax} IS NULL OR {tmax} <= {_CHD_THRESHOLD}) "
            f"THEN 1 ELSE 0 END) "
            f"OVER (PARTITION BY {station_col} ORDER BY {date_col}) AS __chd_run"
        )
        helpers += ["__is_hot32", "__chd_run"]
    if needs_hw:
        parts.append(
            f"CASE WHEN {tmax} > {_HW_THRESHOLD} THEN 1 ELSE 0 END AS __is_hot35"
        )
        parts.append(
            f"SUM(CASE WHEN ({tmax} IS NULL OR {tmax} <= {_HW_THRESHOLD}) "
            f"THEN 1 ELSE 0 END) "
            f"OVER (PARTITION BY {station_col} ORDER BY {date_col}) AS __hw_run"
        )
        helpers += ["__is_hot35", "__hw_run"]
    cte = (
        f"WITH {cte_name} AS (SELECT *, "
        + ", ".join(parts)
        + f" FROM {input_view})"
    )
    return cte, cte_name, helpers


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_compute_indicators(
    rel: duckdb.DuckDBPyRelation | pd.DataFrame,
    *,
    indicators: Sequence[str] | None = None,
    station_col: str | None = None,
    date_col: str | None = None,
    lang: str = "pt",
    verbose: bool = True,
) -> duckdb.DuckDBPyRelation:
    """Compute bioclimatic and thermal-stress indicators from INMET station data.

    All computation runs in DuckDB SQL — the result is a lazy
    ``DuckDBPyRelation`` (no materialisation).

    Mirrors ``climasus4r::sus_climate_compute_indicators``.

    Available indicators (``code`` → output column):

    +-----------------------+--------------------+---------------------+
    | Code                  | Output column      | Required INMET cols |
    +=======================+====================+=====================+
    | ``heat_index``        | ``hi_c``           | T, RH               |
    | ``thi``               | ``thi_c``          | T, RH               |
    | ``apparent_temperature``| ``at_c``         | T, RH, WS           |
    | ``wbgt``              | ``wbgt_c``         | T, RH               |
    | ``vapor_pressure``    | ``vapor_pressure_kpa``| T, RH            |
    | ``dew_point_depression``| ``dpd_c``        | T, DEW              |
    | ``diurnal_range``     | ``diurnal_range_c``| T (window)          |
    | ``consecutive_hot_days``|``consecutive_hot_days``| Tmax (window)|
    | ``heat_wave``         | ``heat_wave``      | Tmax (window)       |
    +-----------------------+--------------------+---------------------+

    Notes:
      - ``heat_index`` returns ``NULL`` outside its valid domain
        (T >= 27°C and RH >= 40%).
      - ``consecutive_hot_days`` is a true run length (gaps-and-islands),
        not a 7-day rolling count.
      - ``heat_wave`` flags every day belonging to a run of >= 3
        consecutive days with Tmax > 35°C — including the first two days
        of an episode.

    Args:
        rel: Input INMET data — lazy ``DuckDBPyRelation`` or
            ``pd.DataFrame`` (output of ``sus_climate_inmet`` or
            ``sus_climate_fill_inmet``).
        indicators: List of indicator codes to compute, or ``None`` for all.
        station_col: Name of the station identifier column
            (auto-detected if ``None``; falls back to a constant when
            absent — useful for single-station inputs).
        date_col: Name of the date/datetime column (auto-detected if
            ``None``).
        lang: Language for messages (``"pt"``, ``"en"``, ``"es"``).
        verbose: Print progress messages when ``True``.

    Returns:
        ``DuckDBPyRelation`` — original columns plus one new column per
        requested indicator.

    Raises:
        ValueError: If an unknown indicator code is requested, or if a
            required INMET column is missing from the input.
    """
    conn = get_connection()

    # Normalise to DuckDBPyRelation
    _rel = conn.from_df(rel) if isinstance(rel, pd.DataFrame) else rel

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
    _station_col = station_col or _detect_station_col(_rel) or "1"

    # Validate required columns are present
    _check_required_cols(_rel, ind_list)

    # Build the query against a local relation alias via rel.query() —
    # this avoids any global view registration on the singleton connection.
    # uuid suffixes on alias and CTE name protect against state bleeding
    # between sequential calls (DuckDB query plan / view caching).
    suffix = uuid.uuid4().hex[:12]
    input_view = f"_climate_indicators_input_{suffix}"

    needs_chd = "consecutive_hot_days" in ind_list
    needs_hw = "heat_wave" in ind_list

    cte_name = f"_runs_{suffix}"
    cte_sql, source, helpers = _build_runs_cte(
        needs_chd, needs_hw, _station_col, _date_col, input_view, cte_name
    )

    # SELECT: original columns first (excluding helper run-id columns), then indicators
    select_head = f"* EXCLUDE ({', '.join(helpers)})" if helpers else "*"

    indicator_exprs: list[str] = []
    for ind in ind_list:
        if ind == "consecutive_hot_days":
            indicator_exprs.append(_render_chd_expr(_station_col, _date_col))
        elif ind == "heat_wave":
            indicator_exprs.append(_render_hw_expr(_station_col))
        else:
            indicator_exprs.append(_render_indicator_sql(ind, _station_col, _date_col))

    select_clause = ", ".join([select_head, *indicator_exprs])
    sql = f"{cte_sql} SELECT {select_clause} FROM {source}".strip()

    if verbose:
        _msg = {
            "pt": f"[sus_climate_compute_indicators] indicadores={ind_list}",
            "en": f"[sus_climate_compute_indicators] indicators={ind_list}",
            "es": f"[sus_climate_compute_indicators] indicadores={ind_list}",
        }
        print(_msg.get(lang, _msg["pt"]))

    # rel.query(alias, sql) makes `alias` resolve to *this* relation only,
    # leaving the connection's global namespace untouched between calls.
    return _rel.query(input_view, sql)
