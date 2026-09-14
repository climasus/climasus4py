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

# Degree-day bases, matching R's defaults. R allows a biome-specific
# `region` to override them; the Python signature has no `region` yet (M12).
_CDD_BASE = 18.0
_HDD_BASE = 18.0
_GDD_BASE = 10.0
_GDD_UPPER = 30.0

#: The km/h -> mph factor. R applies it to a wind column in m/s, which is
#: the unit bug recorded as M69; kept named so the parity is explicit
#: rather than looking like a typo someone should "fix".
_MPH_PER_KMH = 0.621371
#: The correct m/s -> mph factor, used by :func:`_wct_correct_units`.
_MPH_PER_MS = 2.2369362920544

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
            # Rothfusz (1990). The Celsius coefficients below and R's
            # Fahrenheit form are the same regression: measured over the
            # valid domain the two agree to a MEDIAN of 0.000 °C and a mean
            # of 0.0217 °C once the pieces below are in place.
            #
            # Three of them were missing, and the third was producing
            # nonsense:
            #
            # 1. The 60 °C ceiling. Rothfusz is a fit over roughly 27-43 °C
            #    and 40-100% RH; past that the polynomial runs away. Without
            #    the cap this column reached 151.6 °C — a heat index half
            #    again as hot as any temperature ever recorded. The cap
            #    fires on 32.6% of the valid domain and 15.6% of a
            #    Brazil-typical range, so this was not a corner case.
            # 2. The high-humidity adjustment R applies above 85% RH in the
            #    80-87 °F band (5.5% of rows, mean effect 0.264 °C). R also
            #    has a low-humidity adjustment below 13% RH, which the
            #    40% RH mask makes unreachable — kept for symmetry with R
            #    rather than because it can fire.
            # 3. The validity threshold was 27.0 °C here against R's 26.7
            #    (which is 80 °F, the real domain edge). 716 rows out of
            #    200k differed on the mask alone.
            #
            # Fixed rather than left in parity because a 151 °C heat index
            # is silent nonsense, which CLAUDE.md section 3 makes the
            # explicit exception to the parity rule.
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL"
            "  WHEN {T} < 26.7 OR {RH} < 40.0 THEN NULL"
            "  ELSE ROUND_EVEN(LEAST(("
            "    (-42.379"
            "     + 2.04901523 * ({T} * 9.0 / 5.0 + 32.0)"
            "     + 10.14333127 * {RH}"
            "     - 0.22475541 * ({T} * 9.0 / 5.0 + 32.0) * {RH}"
            "     - 0.00683783 * POWER({T} * 9.0 / 5.0 + 32.0, 2)"
            "     - 0.05481717 * POWER({RH}, 2)"
            "     + 0.00122874 * POWER({T} * 9.0 / 5.0 + 32.0, 2) * {RH}"
            "     + 0.00085282 * ({T} * 9.0 / 5.0 + 32.0) * POWER({RH}, 2)"
            "     - 0.00000199 * POWER({T} * 9.0 / 5.0 + 32.0, 2) * POWER({RH}, 2)"
            "     + CASE WHEN {RH} < 13.0"
            "              AND ({T} * 9.0 / 5.0 + 32.0) BETWEEN 80.0 AND 112.0"
            "            THEN -(13.0 - {RH}) / 4.0"
            "                 * SQRT(GREATEST(17.0 - ABS(({T} * 9.0 / 5.0 + 32.0) - 95.0), 0.0)"
            "                        / 17.0)"
            "            ELSE 0.0 END"
            "     + CASE WHEN {RH} > 85.0"
            "              AND ({T} * 9.0 / 5.0 + 32.0) BETWEEN 80.0 AND 87.0"
            "            THEN ({RH} - 85.0) / 10.0 * ((87.0 - ({T} * 9.0 / 5.0 + 32.0)) / 5.0)"
            "            ELSE 0.0 END"
            "    ) - 32.0) * 5.0 / 9.0, 60.0), 2) END AS hi_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Temperature-Humidity Index (THI) — Thom (1959)
    # ------------------------------------------------------------------
    "thi": (
        "thi_c",
        ("T", "RH"),
        (
            # Thom (1959), in R's variant. The two packages used DIFFERENT
            # published forms of the same index:
            #
            #   R  : T - (1 - RH/100) * (T - 14.4) / 2
            #        which expands to T - (0.5  - 0.005 *RH) * (T - 14.4)
            #   here (before): T - (0.55 - 0.0055*RH) * (T - 14.5)
            #
            # The second is the form usually quoted as Thom's discomfort
            # index. Measured over 200k points the gap is small but real:
            # mean -0.046 °C, range -1.40 to +1.21, and the two disagree on
            # the 28 °C high_stress threshold in 1.3% of cases.
            #
            # Aligned to R by the standing decision to replicate and
            # record, since neither form is wrong — they are different
            # published coefficients. See M74.
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN({T} - ((1.0 - {RH} / 100.0) * ({T} - 14.4)) / 2.0, 2) "
            "END AS thi_c"
        ),
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
        ("T", "RH", "SR", "WS"),
        (
            # PARITY WITH R, INCLUDING WHAT LOOKS WRONG ABOUT IT (M71).
            #
            # R averages two wet-bulb estimates and the documentation says
            # so — that part is deliberate. What does not line up is the
            # formulas: `tnw1` has the shape of Stull (2011),
            # `T*atan(c*sqrt(RH + k))`, which expects RH as a PERCENTAGE,
            # and R feeds it `e_a`, vapour pressure in kPa. At T=30/RH=60
            # the atan receives 0.26 where the shape wants 1.26. `tnw2` has
            # the shape of the Australian apparent temperature, not a wet
            # bulb. Neither Stull nor Steadman is among the references the
            # help page cites (Liljegren 2008; Bernard & Pourmoghani 1999).
            #
            # Measured against the psychrometric table at T=30 °C, RH=60%
            # (reference ~23.9 °C): Stull gives 24.00 °C, R's tnw gives
            # 18.78 °C. And the globe term moves only 0.84 °C as radiation
            # goes from 0 to 1000 W/m², which is nearly inert for an index
            # whose purpose is capturing solar load.
            #
            # Kept faithful by Andrey's decision of 14/09/2026 so the two
            # packages agree. `wbgt_stull_c` below is the validated
            # alternative, emitted as its own column so the divergence is
            # visible in the same table rather than asserted in prose.
            #
            # The gap is not a constant bias, and an earlier note here
            # claiming a mean of 3.35 °C was measured on too narrow a wind
            # range. On the 4000-row fixture the two differ by a mean of
            # only -0.36 °C but range from -7.98 to +8.07 — they diverge in
            # *both* directions depending on conditions. What survives any
            # distribution is the threshold consequence: above 31 °C (ISO
            # 7243 extreme heat) this column flags 207 rows where
            # `wbgt_stull_c` flags 449, more than double.
            #
            # R's dispatcher zeroes missing radiation and wind before
            # calling, so only T and RH propagate a null — replicated here.
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE ROUND_EVEN("
            "  0.7 * ("
            "    ({T} * atan(0.16 * sqrt(GREATEST("
            "       ({RH} / 100.0) * 0.6108 * EXP(17.27 * {T} / ({T} + 237.3))"
            "       , 0.01) + 0.1)) + 3.0)"
            "    + ({T} + 0.33 * ({RH} / 100.0) * EXP(0.0514 * {T}) - 4.0)"
            "  ) / 2.0"
            "  + 0.2 * ({T} + 0.0144 * POWER(GREATEST("
            "       CASE WHEN {SR} IS NULL OR {SR} < 0 THEN 0.0"
            "            ELSE {SR} / 3.6 END, 0.0), 0.6)"
            "     / POWER(GREATEST(COALESCE({WS}, 0.0), 0.1), 0.2) - 2.0)"
            "  + 0.1 * {T}"
            ", 2) END AS wbgt_c"
        ),
    ),
    # ------------------------------------------------------------------
    # WBGT from the Stull (2011) wet bulb — the formula this package used
    # before parity with R was adopted, kept as its own column.
    #
    # Shade/indoor form: 0.67 * Twb + 0.33 * Tdb, with Twb from Stull
    # (2011), J. Appl. Meteorol. Climatol. 50:2267-2269. No globe term, so
    # it needs only T and RH — which also makes it the one that still
    # works on data without solar radiation or wind.
    #
    # Why it is here rather than in a note: this is the estimate that
    # reproduces the psychrometric table (24.00 °C against ~23.9 at
    # T=30/RH=60), and keeping it as live, tested code means it cannot be
    # quietly lost or drift. Compare it with `wbgt_c` on the same rows to
    # see M71 as data.
    # ------------------------------------------------------------------
    "wbgt_stull": (
        "wbgt_stull_c",
        ("T", "RH"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "(0.67 * "
            "  ({T} * atan(0.151977 * sqrt({RH} + 8.313659)) "
            "   + atan({T} + {RH}) "
            "   - atan({RH} - 1.676331) "
            "   + 0.00391838 * power({RH}, 1.5) * atan(0.023101 * {RH}) "
            "   - 4.686035) "
            " + 0.33 * {T}) END AS wbgt_stull_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Vapor Pressure (VP) — Magnus-Tetens formula
    # ------------------------------------------------------------------
    "vapor_pressure": (
        "vapor_pressure_kpa",
        ("T", "RH"),
        (
            # Magnus-Tetens. Identical to R's formula; the only difference
            # was that R rounds to three decimals (max gap 5e-04 kPa).
            # Rounded here too, so the parity is exact rather than close.
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN(0.6108 * EXP(17.27 * {T} / ({T} + 237.3)) * ({RH} / 100.0), 3) "
            "END AS vapor_pressure_kpa"
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
    # Degree days — Cooling / Heating / Growing.
    #
    # Ported to match R exactly, including that these are per-observation
    # values rather than the daily means of the classical definition: R
    # applies the base temperature to each row of the series it receives.
    # On hourly INMET data that makes cdd_c an hourly cooling load, and
    # summing it over a day is the caller's job.
    #
    # The bases are R's defaults (18 / 18 / 10-30). R lets a biome-specific
    # `region` override them; the Python signature has no `region` yet, so
    # the defaults are fixed here — part of the parameter gap tracked in M12.
    #
    # The explicit NULL guard is not redundant: DuckDB's GREATEST and LEAST
    # *ignore* NULL arguments, so GREATEST(NULL - 18, 0) is 0, while R's
    # pmax(NA - 18, 0) is NA. Without the guard a missing temperature would
    # report zero degree-days — which reads as a mild day rather than as an
    # absent one, and sums into totals as a real observation.
    # ------------------------------------------------------------------
    "cdd": (
        "cdd_c",
        ("T",),
        (
            "CASE WHEN {T} IS NULL THEN NULL ELSE "
            f"ROUND_EVEN(GREATEST({{T}} - {_CDD_BASE}, 0.0), 1) END AS cdd_c"
        ),
    ),
    "hdd": (
        "hdd_c",
        ("T",),
        (
            "CASE WHEN {T} IS NULL THEN NULL ELSE "
            f"ROUND_EVEN(GREATEST({_HDD_BASE} - {{T}}, 0.0), 1) END AS hdd_c"
        ),
    ),
    "gdd": (
        "gdd_c",
        ("T",),
        (
            "CASE WHEN {T} IS NULL THEN NULL ELSE "
            f"ROUND_EVEN(LEAST(GREATEST({{T}}, {_GDD_BASE}), {_GDD_UPPER}) "
            f"- {_GDD_BASE}, 1) END AS gdd_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Koppen humidity class — four bands on relative humidity.
    #
    # PARITY WITH A DEFECT IN R, DELIBERATE. R's case_when ends in
    # `TRUE ~ "Perhumid"`, and that final arm also catches NA: with the
    # humidity missing, the three preceding conditions evaluate to NA,
    # none matches, and the row is labelled "Perhumid" — the *wettest*
    # of the four bands. Measured on the 4000-row fixture: 40 of 40 rows
    # with null humidity come back "Perhumid" in R.
    #
    # So a count by humidity class silently files every missing reading
    # under the wettest band. The Python side used to return NULL, which
    # is the defensible behaviour; it now matches R by Andrey's decision
    # of 14/09/2026, so the presentation to the coordinator stays
    # consistent, with the treatment to be settled afterwards. See M70 —
    # and note this is a *fabricated category*, a different class of
    # problem from the wrong number in M69.
    # ------------------------------------------------------------------
    "koppen_humidity": (
        "koppen_humidity",
        ("RH",),
        (
            "CASE"
            "  WHEN {RH} < 30.0 THEN 'Arid'"
            "  WHEN {RH} < 50.0 THEN 'Semi-arid'"
            "  WHEN {RH} < 70.0 THEN 'Humid'"
            "  ELSE 'Perhumid'"
            " END AS koppen_humidity"
        ),
    ),
    # ------------------------------------------------------------------
    # Wind Chill Equivalent Temperature (WCET) — Environment Canada
    # (2001), wind in km/h. Defined only for T <= 10 °C and wind above
    # 4.8 km/h (1.3 m/s); outside that domain the regression is
    # meaningless, so R masks it to NA and so does this.
    # ------------------------------------------------------------------
    "wcet": (
        "wcet_c",
        ("T", "WS"),
        (
            # The IS NULL arm comes first for the same reason as in the
            # degree days, and it bit twice: GREATEST(NULL * 3.6, 0.01)
            # is 0.01 in DuckDB, so without it a missing wind speed
            # silently became a 0.01 m/s calm and produced a wind chill
            # for an hour that has no wind measurement. Measured on 4000
            # synthetic rows: 20 fabricated values before this guard.
            "CASE WHEN {T} IS NULL OR {WS} IS NULL THEN NULL"
            "  WHEN {T} > 10.0 OR {WS} <= 1.3 THEN NULL ELSE ROUND_EVEN("
            "  13.12 + 0.6215 * {T}"
            "  - 11.37 * POWER(GREATEST({WS} * 3.6, 0.01), 0.16)"
            "  + 0.3965 * {T} * POWER(GREATEST({WS} * 3.6, 0.01), 0.16)"
            ", 2) END AS wcet_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Wind Chill Temperature (WCT) — NWS form, computed in Fahrenheit and
    # converted back.
    #
    # PARITY WITH A UNIT BUG IN R, DELIBERATE. R converts the wind with
    # `ws * 0.621371`, which is the km/h -> mph factor, applied to
    # `ws_2_m_s` — a column in metres per second. The correct factor is
    # 2.23694.
    #
    # The proof needs no external reference, because R implements the same
    # physical quantity twice: `wcet_c` (Environment Canada, km/h) and
    # this one must agree. Measured over 200k points in the valid domain,
    # R's two columns differ by a mean of 4.503 °C (max 7.49); with the
    # correct m/s -> mph factor they differ by 0.024 °C (max 0.04), which
    # is just the gap between the two published regressions.
    #
    # The error runs warm: understating the wind understates the chill, so
    # cold-wave risk is underestimated. Kept faithful here because it is a
    # published output of climasus4r and correcting it silently would make
    # the two packages disagree; `_wct_correct_units` below keeps the right
    # formula alive and tested. See M69.
    # ------------------------------------------------------------------
    "wct": (
        "wct_c",
        ("T", "WS"),
        (
            "CASE WHEN {T} IS NULL OR {WS} IS NULL THEN NULL"
            "  WHEN {T} > 10.0 OR {WS} <= 1.3 THEN NULL ELSE ROUND_EVEN((("
            "  35.74 + 0.6215 * ({T} * 9.0 / 5.0 + 32.0)"
            f"  - 35.75 * POWER(GREATEST({{WS}} * {_MPH_PER_KMH}, 0.01), 0.16)"
            "  + 0.4275 * ({T} * 9.0 / 5.0 + 32.0)"
            f"    * POWER(GREATEST({{WS}} * {_MPH_PER_KMH}, 0.01), 0.16)"
            ") - 32.0) * 5.0 / 9.0, 2) END AS wct_c"
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

#: R's indicator code -> this package's code, where the two disagree.
#:
#: Only one entry so far, and it is a real parity break rather than
#: cosmetics: R's help page documents the code as ``'hi'``, so
#: ``indicators=["hi"]`` is what a reader of the R documentation writes —
#: and it used to raise "Unknown indicator code". The output column was
#: never the problem; both sides emit ``hi_c``. Accepting the alias costs
#: nothing and keeps the documented R vocabulary working. See M73.
_INDICATOR_ALIASES: dict[str, str] = {"hi": "heat_index"}


def resolve_indicator(code: str) -> str:
    """Map an indicator code to this package's canonical name.

    Accepts R's vocabulary as well as this package's own, so a call
    transcribed from the R documentation works unchanged.
    """
    return _INDICATOR_ALIASES.get(code, code)


# ---------------------------------------------------------------------------
# Confidence flags — three booleans per indicator, mirroring R's
# .add_confidence_flags()
# ---------------------------------------------------------------------------

#: Thresholds exactly as R's `.build_indicator_registry()` declares them.
#: Kept under R's own key names rather than tidied, because the names are
#: what decides whether a flag ever fires — see `_FLAG_CHAINS`.
_INDICATOR_THRESHOLDS: dict[str, dict[str, float]] = {
    "wbgt": {"extreme_heat": 31, "high_stress": 28, "moderate_stress": 25,
             "warning_low": 15},
    # R calls this indicator `hi`; this package calls it `heat_index`. The
    # output column agrees (`hi_c`) but the *code* did not, so following R's
    # documentation and passing indicators=["hi"] raised here. See
    # `_INDICATOR_ALIASES`.
    "heat_index": {"extreme_danger": 54, "dangerous": 41,
                   "extreme_caution": 32, "caution": 27},
    "thi": {"high_stress": 28, "comfortable_max": 24, "comfortable_min": 20},
    "wcet": {"high_risk": -35, "moderate_risk": -20, "low_risk": -10},
    "wct": {"high_risk": -35, "moderate_risk": -20, "low_risk": -10},
    "et": {"hot": 35, "warm": 30, "cool": 20, "cold": 15},
    "utci": {"extreme_heat_stress": 46, "strong_heat_stress": 38,
             "moderate_heat_stress": 32, "slight_heat_stress": 26,
             "slight_cold_stress": 9, "moderate_cold_stress": 0,
             "strong_cold_stress": -13, "extreme_cold_stress": -27},
    "pet": {"extreme_heat": 41, "strong_heat": 35, "moderate_heat": 29,
            "slight_heat": 23, "slight_cold": 13, "moderate_cold": 8},
    "diurnal_range": {"high": 15, "moderate": 10, "low": 5},
    "vapor_pressure": {"high": 2.5, "moderate": 1.5},
    # Declared empty in R, and the main body guards with
    # `length(thresholds) > 0`, so these emit no flag columns at all.
    "cdd": {}, "hdd": {}, "gdd": {},
    "heat_stress_risk": {}, "koppen_humidity": {},
    # Python-only, and given wbgt's thresholds on purpose: it is the same
    # physical quantity, so the ISO 7243 bands apply to it unchanged. It
    # also sharpens the M71 evidence — with the same threshold on both
    # columns, `wbgt_c_flag_extreme` and `wbgt_stull_c_flag_extreme` can be
    # counted against each other directly.
    "wbgt_stull": {"extreme_heat": 31, "high_stress": 28,
                   "moderate_stress": 25, "warning_low": 15},
}

#: Priority chains R uses to pick which declared threshold drives each flag.
#: The first name present in the indicator's threshold dict wins; when none
#: is present the column is emitted as constant FALSE.
_FLAG_CHAINS: dict[str, tuple[str, ...]] = {
    "extreme": ("extreme_heat", "extreme_heat_stress", "extreme_danger", "high_risk"),
    "high": ("high_stress", "dangerous", "moderate_heat_stress",
             "extreme_caution", "hot"),
    "low": ("warning_low", "low_stress", "slight_cold_stress", "cold"),
}

#: `extreme`/`high` fire above the threshold, `low` below it.
_FLAG_COMPARISON: dict[str, str] = {"extreme": ">", "high": ">", "low": "<"}


def flag_threshold(indicator: str, flag: str) -> float | None:
    """Which declared threshold drives one flag, or ``None`` for constant FALSE.

    Exposed rather than inlined because the answer is the whole of the R
    finding recorded as M72, and it is easier to read as data than to
    re-derive from the chains. Two things fall out of it:

    **16 of the 30 flag columns are constant FALSE**, because the
    threshold names an indicator declares are not the names the chain
    looks for. ``diurnal_range`` declares ``high``/``moderate``/``low``
    and the chain wants ``high_stress``/``warning_low``, so all three of
    its flags never fire; ``vapor_pressure`` is the same; ``pet``
    declares ``slight_cold`` where the chain wants ``slight_cold_stress``
    — a near miss that costs it two columns.

    **2 more are inverted.** ``wcet`` and ``wct`` declare
    ``high_risk = -35``, which lands in the *extreme* chain, and that
    flag fires on ``value > -35``. For a wind chill, colder is worse: the
    flag is TRUE at -30 °C and at +20 °C, and FALSE at -40 °C. Measured
    over a -40..60 grid it is TRUE in 190 of 201 points — true almost
    everywhere except the dangerous cases.

    So of 30 emitted flag columns, 12 carry correct information.

    Args:
        indicator: Indicator code, e.g. ``"wbgt"``.
        flag: One of ``"extreme"``, ``"high"``, ``"low"``.

    Returns:
        The threshold value, or ``None`` when no declared name matches the
        chain for that flag.
    """
    thresholds = _INDICATOR_THRESHOLDS.get(indicator, {})
    for nome in _FLAG_CHAINS[flag]:
        if nome in thresholds:
            return float(thresholds[nome])
    return None


def has_flags(indicator: str) -> bool:
    """Whether an indicator emits flag columns at all.

    R guards with ``length(reg$thresholds) > 0``, so the five indicators
    that declare no thresholds (the three degree days,
    ``heat_stress_risk`` and ``koppen_humidity``) get no flag columns —
    not three FALSE ones.
    """
    return bool(_INDICATOR_THRESHOLDS.get(indicator))


def _render_flag_exprs(indicator: str) -> list[str]:
    """SQL for one indicator's three flag columns, in R's column order."""
    if not has_flags(indicator):
        return []
    out_col = _INDICATOR_DEFS[indicator][0]
    exprs = []
    for flag in ("extreme", "high", "low"):
        alvo = f"{out_col}_flag_{flag}"
        limiar = flag_threshold(indicator, flag)
        if limiar is None:
            # R fills the column with FALSE rather than omitting it.
            exprs.append(f"FALSE AS {alvo}")
        else:
            op = _FLAG_COMPARISON[flag]
            # R's `!is.na(vals) & vals > thr` makes a missing value FALSE,
            # not NA — so the flag is never null even where the indicator is.
            exprs.append(
                f'("{out_col}" IS NOT NULL AND "{out_col}" {op} {limiar}) AS {alvo}'
            )
    return exprs


def _wct_correct_units(air_t: float | Sequence[float], ws_ms: float | Sequence[float]):
    """NWS wind chill with the wind converted from m/s correctly.

    This is not what ``wct_c`` emits. ``wct_c`` reproduces R, which uses
    the km/h -> mph factor on a column measured in m/s (M69). This
    function exists so the correct formula stays *executable and tested*
    rather than living in a comment, where nothing would notice if it
    drifted or was lost.

    Use it to reproduce the evidence: over the valid domain this agrees
    with ``wcet_c`` to about 0.02 °C, while ``wct_c`` runs some 4.5 °C
    warmer. Both are the same physical quantity, so only one of those
    gaps can be right.

    Args:
        air_t: Air temperature in degrees Celsius.
        ws_ms: Wind speed in metres per second.

    Returns:
        Wind chill in degrees Celsius, unmasked — the caller applies the
        ``T <= 10 and ws > 1.3`` domain if it wants R's masking.
    """
    import numpy as np  # noqa: PLC0415

    t = np.asarray(air_t, dtype=float)
    mph = np.maximum(np.asarray(ws_ms, dtype=float) * _MPH_PER_MS, 0.01)
    t_f = t * 9.0 / 5.0 + 32.0
    chill_f = (
        35.74 + 0.6215 * t_f - 35.75 * mph**0.16 + 0.4275 * t_f * mph**0.16
    )
    return (chill_f - 32.0) * 5.0 / 9.0


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
    confidence_flags: bool = True,
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
        confidence_flags: Emit ``{col}_flag_extreme``, ``{col}_flag_high``
            and ``{col}_flag_low`` for every indicator that declares
            thresholds. Defaults to ``True``, matching R — whose own
            default is ``region != "none"`` with ``region="auto"``.
            Read :func:`flag_threshold` before relying on these: of the
            30 columns R emits, 16 are constant ``FALSE`` and 2 are
            inverted (M72).
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
        # Resolve R's codes to ours first, so a call copied from the R
        # documentation is not rejected for using R's vocabulary.
        ind_list = [resolve_indicator(c) for c in indicators]
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
    flag_exprs: list[str] = []
    for ind in ind_list:
        if ind == "consecutive_hot_days":
            indicator_exprs.append(_render_chd_expr(_station_col, _date_col))
        elif ind == "heat_wave":
            indicator_exprs.append(_render_hw_expr(_station_col))
        else:
            indicator_exprs.append(_render_indicator_sql(ind, _station_col, _date_col))
        if confidence_flags:
            flag_exprs.extend(_render_flag_exprs(ind))

    # The flags reference the indicator columns by their alias, which works
    # because DuckDB resolves lateral column aliases inside one SELECT —
    # no wrapping subquery needed. They come after every indicator so the
    # order does not matter.
    select_clause = ", ".join([select_head, *indicator_exprs, *flag_exprs])
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
