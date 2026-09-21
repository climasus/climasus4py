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
import warnings
from collections.abc import Sequence

import duckdb
import numpy as np
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
#: the unit bug recorded as M69. Kept named, and reachable through
#: ``R_WCT_USES_KMH_FACTOR``, so R's number can still be reproduced
#: exactly and does not look like a typo someone should "fix".
_MPH_PER_KMH = 0.621371
#: The correct m/s -> mph factor. What ``wct_c`` uses by default since
#: 21/09/2026, and what :func:`_wct_correct_units` has always used.
_MPH_PER_MS = 2.2369362920544

#: `t_mrt - airT` in R's UTCI, written out. R computes a mean radiant
#: temperature as `airT + 0.07*sr_wm2 - 1.5` and then uses the difference
#: from air temperature three times; inlining it keeps the three uses from
#: drifting apart.
_UTCI_DTMRT = (
    "(0.07 * (CASE WHEN {SR} IS NULL OR {SR} < 0 THEN 0.0"
    "              ELSE {SR} / 3.6 END) - 1.5)"
)

#: The same difference in R's PET, which uses different coefficients
#: (`airT + 0.06*sr_wm2 - 2`).
_PET_DTMRT = (
    "(0.06 * (CASE WHEN {SR} IS NULL OR {SR} < 0 THEN 0.0"
    "              ELSE {SR} / 3.6 END) - 2.0)"
)

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
            "{MASK_HI}"
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
    # Wet-Bulb Globe Temperature (WBGT), as climasus4r computes it.
    # ------------------------------------------------------------------
    "wbgt": (
        "wbgt_c",
        ("T", "RH", "SR", "WS"),
        (
            # PARITY WITH R, INCLUDING WHAT LOOKS WRONG ABOUT IT (M71).
            # The formula itself lives in `_WBGT_EXPR`, shared with
            # `heat_stress_risk`, which classifies this same value.
            #
            # R averages two wet-bulb estimates and its documentation says
            # so -- that part is deliberate. What does not line up is the
            # formulas: `tnw1` has the shape of Stull (2011),
            # `T*atan(c*sqrt(RH + k))`, which expects RH as a PERCENTAGE,
            # and R feeds it vapour pressure in kPa. At T=30/RH=60 the atan
            # receives 0.26 where the shape wants 1.26. `tnw2` has the
            # shape of the Australian apparent temperature, not a wet bulb.
            # Neither Stull nor Steadman is among the references the help
            # page cites (Liljegren 2008; Bernard & Pourmoghani 1999).
            #
            # Measured against the psychrometric table at T=30 C, RH=60%
            # (reference ~23.9 C): Stull gives 24.00 C, R's tnw gives
            # 18.78 C. The globe term moves only 0.84 C as radiation goes
            # from 0 to 1000 W/m2, nearly inert for an index whose purpose
            # is capturing solar load.
            #
            # KEPT FAITHFUL, and reaffirmed by D5 on 21/09/2026 — the
            # decision round that corrected M69 and M72 deliberately did
            # NOT touch this one. The averaging of two terms is written in
            # R's own documentation, so it is a choice and not a slip; what
            # is wrong is the documentation citing Liljegren and
            # Bernard & Pourmoghani for a formula that implements neither.
            # So the fix belongs in R's *documentation* — own the formula
            # as the package's own index and stop citing references it does
            # not use — and not in the formula here. Reimplementing
            # Liljegren would need radiation and wind that the package does
            # not always have.
            #
            # `wbgt_stull` is the validated alternative, emitted as its own
            # column so the divergence shows up as data. The gap is not a
            # constant bias: on the fixture the two differ by a mean of
            # only -0.36 C but range from -7.98 to +8.07. What survives any
            # distribution is the threshold consequence -- above 31 C (ISO
            # 7243 extreme heat) this column flags 207 rows where
            # `wbgt_stull_c` flags 449.
            #
            # R's dispatcher zeroes missing radiation and wind before
            # calling, so only T and RH propagate a null.
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL"
            "  ELSE {WBGT} END AS wbgt_c"
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
            # PARITY WITH R, INCLUDING ITS GROUPING (M76). R builds
            # `day_key <- as.character(as.Date(df[["date"]]))` and
            # aggregates with `tapply(t, day_key, max)` — by DAY ONLY, with
            # no station in the grouping. On multi-station input each row's
            # "diurnal range" therefore becomes the max minus the min
            # across *every* station that day, which folds the spread
            # between stations into a quantity that is supposed to describe
            # one station's day.
            #
            # Measured on the two-station fixture: 3936 of 4000 rows differ
            # from the per-station answer.
            #
            # See `diurnal_range_station` for the per-station version.
            "(MAX({T}) OVER (PARTITION BY {DATE_COL}::DATE) "
            "- MIN({T}) OVER (PARTITION BY {DATE_COL}::DATE)) "
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
            "ROUND_EVEN(GREATEST({T} - {P:cdd_base}, 0.0), 1) END AS cdd_c"
        ),
    ),
    "hdd": (
        "hdd_c",
        ("T",),
        (
            "CASE WHEN {T} IS NULL THEN NULL ELSE "
            "ROUND_EVEN(GREATEST({P:hdd_base} - {T}, 0.0), 1) END AS hdd_c"
        ),
    ),
    "gdd": (
        "gdd_c",
        ("T",),
        (
            "CASE WHEN {T} IS NULL THEN NULL ELSE "
            "ROUND_EVEN(LEAST(GREATEST({T}, {P:gdd_base}), {P:gdd_upper}) "
            "- {P:gdd_base}, 1) END AS gdd_c"
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
            "CASE WHEN {T} IS NULL THEN NULL"
            "{MASK_WIND}"
            "  ELSE ROUND_EVEN("
            "  13.12 + 0.6215 * {T}"
            "  - 11.37 * POWER(GREATEST(COALESCE({WS}, 0.0) * 3.6, 0.01), 0.16)"
            "  + 0.3965 * {T} * POWER(GREATEST(COALESCE({WS}, 0.0) * 3.6, 0.01), 0.16)"
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
    # cold-wave risk is underestimated — the dangerous direction for a
    # cold-wave analysis.
    #
    # CORRECTED ON THE DEFAULT PATH since 21/09/2026 (D5), which flipped
    # the earlier decision to replicate. See `R_WCT_USES_KMH_FACTOR` for
    # the switch and the reasoning. `_wct_correct_units` stays as the
    # reference implementation the tests compare against.
    # ------------------------------------------------------------------
    "wct": (
        "wct_c",
        ("T", "WS"),
        (
            "CASE WHEN {T} IS NULL THEN NULL"
            "{MASK_WIND}"
            "  ELSE ROUND_EVEN((("
            "  35.74 + 0.6215 * ({T} * 9.0 / 5.0 + 32.0)"
            "  - 35.75 * POWER(GREATEST(COALESCE({WS}, 0.0) * {MPH}, 0.01), 0.16)"
            "  + 0.4275 * ({T} * 9.0 / 5.0 + 32.0)"
            "    * POWER(GREATEST(COALESCE({WS}, 0.0) * {MPH}, 0.01), 0.16)"
            ") - 32.0) * 5.0 / 9.0, 2) END AS wct_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Effective Temperature (ET) — Missenard-style form as R writes it.
    #
    # PARITY WITH A LATENT DEFECT (M78). R floors the wind at 0.04 m/s
    # with `pmax(ws, 0.04)` and then takes `(ws_safe - 0.2)^0.5`. Below
    # 0.2 m/s the radicand is negative, so R returns NaN — the floor is
    # defeated by the subtraction that follows it. Verified in R: ws of
    # 0, 0.04, 0.10 and 0.19 all give NA; 0.20 gives a value.
    #
    # Calm air is not a corner case — 76 of the 4000 fixture rows sit
    # below 0.2 m/s, and a still night is exactly when an effective
    # temperature matters. See `et_calm` for the variant that floors the
    # radicand instead.
    #
    # R's dispatcher zeroes a missing wind before calling, which lands in
    # the same NaN branch, so a missing wind also yields no value.
    # ------------------------------------------------------------------
    "et": (
        "et_c",
        ("T", "RH", "WS"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL"
            "  WHEN GREATEST(COALESCE({WS}, 0.0), 0.04) < 0.2 THEN NULL"
            "  ELSE ROUND_EVEN(GREATEST("
            "    {T} - 0.4 * ({T} - 10.0) * (1.0 - {RH} / 100.0)"
            "    - 1.1 * SQRT(GREATEST(COALESCE({WS}, 0.0), 0.04) - 0.2)"
            "  , -273.15), 2) END AS et_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Heat stress risk — six bands on the WBGT.
    #
    # PARITY WITH A DEFECT (M79), the same shape as koppen_humidity: the
    # `case_when` ends in `TRUE ~ "None"`, so a null WBGT is reported as
    # "None" — no heat stress. Verified in R: a missing temperature or
    # humidity both come back "None".
    #
    # Worse than koppen's, because "None" is also the legitimate answer
    # for cold weather: the two become indistinguishable. On the fixture
    # 3049 of 4000 rows read "None" and nothing says which of those had
    # no data. See `heat_stress_risk_strict`.
    # ------------------------------------------------------------------
    "heat_stress_risk": (
        "heat_stress_risk",
        ("T", "RH", "WS", "SR"),
        (
            "CASE"
            "  WHEN {WBGT} > 33.0 THEN 'Extreme'"
            "  WHEN {WBGT} > 30.0 THEN 'Very High'"
            "  WHEN {WBGT} > 28.0 THEN 'High'"
            "  WHEN {WBGT} > 25.0 THEN 'Moderate'"
            "  WHEN {WBGT} > 20.0 THEN 'Low'"
            "  ELSE 'None'"
            " END AS heat_stress_risk"
        ),
    ),
    # ------------------------------------------------------------------
    # Universal Thermal Climate Index, as climasus4r computes it.
    #
    # R's help page calls this a "Fiala-polynomial-inspired multi-term
    # regression" and cites Brode et al. (2012) — the UTCI paper. That
    # hedging is fair: the published UTCI is a 6th-order polynomial in 210
    # terms, and this is six terms, so it is an approximation of the index
    # rather than the index. Worth knowing when reading the column, which
    # is why it is recorded (M80) rather than treated as a defect.
    #
    # Two measurements matter for interpretation. The solar response is
    # 4.97 °C across 0 to 1000 W/m² where the published UTCI runs well
    # over 10 °C above air temperature in full sun; the wind response is
    # -3.01 °C from 0.5 to 20 m/s. Both understate. And the 50 °C ceiling
    # *binds on real data* — the fixture maximum is exactly 50.00.
    # ------------------------------------------------------------------
    "utci": (
        "utci_c",
        ("T", "RH", "WS", "SR"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN(LEAST(GREATEST("
            "  {T}"
            "  + 0.048 * ((({RH} / 100.0)"
            "       * 0.6108 * EXP(17.27 * {T} / ({T} + 237.3))) * 1000.0 - 1000.0) / 100.0"
            "  - 0.29 * LN(GREATEST(COALESCE({WS}, 0.0) * 1.5, 0.5) + 0.1)"
            f"  + 0.016 * {_UTCI_DTMRT}"
            f"  + 0.01 * POWER({_UTCI_DTMRT}, 2) / 10.0"
            f"  - 0.00006 * POWER({_UTCI_DTMRT}, 2)"
            "       * GREATEST(COALESCE({WS}, 0.0) * 1.5, 0.5)"
            "  + {P:utci_correction}"
            ", -60.0), 50.0), 2) END AS utci_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Physiological Equivalent Temperature, as climasus4r computes it.
    #
    # PARITY INCLUDING A SILENT FALLBACK (M81). R's seasonal clothing
    # adjustment reads `df[["date"]]` — a HARDCODED column name, not the
    # `datetime_col` the function accepts and auto-detects. When no column
    # is literally named `date`, `inherits()` fails and the month becomes
    # 6 for every row, so the documented "seasonal clothing adjustment"
    # quietly does not apply. Verified: with the fixture, whose date
    # column is named `datetime`, R's answer equals its month-6 branch
    # exactly.
    #
    # The effect is small — January 31.25 against July 31.13, a 0.12 °C
    # spread — so what is lost is a documented feature rather than much
    # accuracy. Replicated here, `date` column and all.
    #
    # Note also that PET is NOT clamped, unlike UTCI: R returns 63.60 at
    # T=60 and -59.17 at T=-50.
    # ------------------------------------------------------------------
    "pet": (
        "pet_c",
        ("T", "RH", "WS", "SR"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN("
            "  {T} + 0.1 * ({RH} - 50.0) / 10.0"
            "  - 0.3 * GREATEST(COALESCE({WS}, 0.0), 0.1)"
            f"  + 0.05 * ({_PET_DTMRT})"
            "  + 0.5 * (1.0 - CASE"
            "      WHEN {PET_MONTH} IN (12, 1, 2) THEN 0.5 * {P:clothing_factor}"
            "      WHEN {PET_MONTH} IN (3, 4, 5, 9, 10, 11)"
            "        THEN 0.7 * {P:clothing_factor}"
            "      ELSE 0.9 * {P:clothing_factor} END)"
            "  + {P:pet_correction}"
            ", 2) END AS pet_c"
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

# ---------------------------------------------------------------------------
# Regional parameters
# ---------------------------------------------------------------------------
#
# R's `region` argument swaps a table of biome-specific constants into the
# indicator formulas. The values below are `climasus4r:::.region_params_table()`
# verbatim, minus three entries the R package declares and never reads --
# `wbgt_high_warning`, `wbgt_extreme` and `metabolic_factor` (M82). That
# omission matters most for `wbgt_extreme`, which varies from 31 in the Amazon
# to 33 in the Caatinga: the package advertises regionally adapted WBGT
# thresholds and then classifies with the fixed registry ones regardless.

_REGION_PARAMS: dict[str, dict[str, float]] = {
    "amazon": {"hi_min_temp": 24.0, "hi_min_rh": 40.0, "gdd_base": 15.0,
               "gdd_upper": 35.0, "cdd_base": 20.0, "hdd_base": 18.0,
               "utci_correction": 1.5, "pet_correction": 1.0,
               "clothing_factor": 0.5},
    "cerrado": {"hi_min_temp": 25.5, "hi_min_rh": 35.0, "gdd_base": 12.0,
                "gdd_upper": 32.0, "cdd_base": 19.0, "hdd_base": 16.0,
                "utci_correction": 0.5, "pet_correction": 0.3,
                "clothing_factor": 0.6},
    "caatinga": {"hi_min_temp": 25.0, "hi_min_rh": 30.0, "gdd_base": 12.0,
                 "gdd_upper": 34.0, "cdd_base": 20.0, "hdd_base": 14.0,
                 "utci_correction": -0.5, "pet_correction": -0.3,
                 "clothing_factor": 0.4},
    "atlantic_forest": {"hi_min_temp": 25.0, "hi_min_rh": 40.0, "gdd_base": 12.0,
                        "gdd_upper": 32.0, "cdd_base": 19.0, "hdd_base": 15.0,
                        "utci_correction": 1.0, "pet_correction": 0.8,
                        "clothing_factor": 0.5},
    "pampa": {"hi_min_temp": 26.0, "hi_min_rh": 40.0, "gdd_base": 8.0,
              "gdd_upper": 30.0, "cdd_base": 18.0, "hdd_base": 15.0,
              "utci_correction": -1.0, "pet_correction": -0.8,
              "clothing_factor": 0.7},
    "pantanal": {"hi_min_temp": 24.5, "hi_min_rh": 35.0, "gdd_base": 14.0,
                 "gdd_upper": 34.0, "cdd_base": 20.0, "hdd_base": 16.0,
                 "utci_correction": 0.5, "pet_correction": 0.4,
                 "clothing_factor": 0.5},
    "southeast": {"hi_min_temp": 25.5, "hi_min_rh": 40.0, "gdd_base": 10.0,
                  "gdd_upper": 32.0, "cdd_base": 19.0, "hdd_base": 16.0,
                  "utci_correction": 0.0, "pet_correction": 0.0,
                  "clothing_factor": 0.6},
    "south": {"hi_min_temp": 26.0, "hi_min_rh": 40.0, "gdd_base": 8.0,
              "gdd_upper": 30.0, "cdd_base": 18.0, "hdd_base": 14.0,
              "utci_correction": -1.5, "pet_correction": -1.2,
              "clothing_factor": 0.7},
}

#: What the formulas use when `region="none"` — R's own `%||%` fallbacks,
#: scattered through `.dispatch_indicator()` rather than tabulated.
_REGION_NONE: dict[str, float] = {
    "hi_min_temp": 26.7, "hi_min_rh": 40.0, "gdd_base": 10.0,
    "gdd_upper": 30.0, "cdd_base": 18.0, "hdd_base": 18.0,
    "utci_correction": 0.0, "pet_correction": 0.0, "clothing_factor": 0.6,
}

#: Which biome R assigns to each state, from `.detect_region_from_data()`.
#: MT and MS route to a further pantanal-vs-cerrado split in R; here they
#: resolve to pantanal, which is what R's helper returns for them.
_UF_TO_REGION: dict[str, str] = {
    **{uf: "amazon" for uf in ("AC", "AM", "AP", "PA", "RO", "RR", "TO")},
    **{uf: "caatinga" for uf in ("AL", "BA", "CE", "MA", "PB", "PE",
                                 "PI", "RN", "SE")},
    **{uf: "atlantic_forest" for uf in ("ES", "RJ", "SP")},
    "RS": "pampa",
    **{uf: "south" for uf in ("PR", "SC")},
    "MG": "southeast",
    **{uf: "cerrado" for uf in ("DF", "GO")},
    **{uf: "pantanal" for uf in ("MT", "MS")},
}

REGIONS: tuple[str, ...] = tuple(_REGION_PARAMS)


def detect_region(rel: duckdb.DuckDBPyRelation) -> str:
    """Infer the biome the way R's `.detect_region_from_data()` does.

    R tries the most common `UF` first, then the mean `latitude`, and
    falls back to ``"southeast"``. No spatial join is involved, which is
    why this ports cleanly — the `use_cache`/`cache_dir` arguments belong
    to other parts of the R function, not to this.

    Returns:
        A biome name from :data:`REGIONS`.
    """
    cols = rel.limit(0).df().columns.tolist()
    if "UF" in cols:
        linha = rel.aggregate(
            'UF, count(*) AS n', 'UF'
        ).filter("UF IS NOT NULL").order("n DESC").limit(1).fetchone()
        if linha and linha[0] in _UF_TO_REGION:
            return _UF_TO_REGION[linha[0]]
    if "latitude" in cols:
        media = rel.aggregate("avg(latitude)").fetchone()[0]
        if media is not None:
            lat = float(media)
            if lat < -30:
                return "pampa"
            if lat < -25:
                return "south"
            if lat < -15:
                return "southeast"
            if lat < -5:
                return "cerrado"
            return "amazon"
    return "southeast"


def resolve_region_params(
    region: str,
    rel: duckdb.DuckDBPyRelation,
    custom_thresholds: dict[str, float] | None = None,
) -> tuple[str, dict[str, float]]:
    """Resolve `region` to its parameters, mirroring R.

    ``"auto"`` detects from the data; an unknown name warns and falls back
    to ``"southeast"``, as R does; ``"none"`` uses the scattered defaults
    R reaches through `%||%`. `custom_thresholds` then overrides
    individual entries, matching R's `utils::modifyList`.

    Returns:
        ``(resolved_region, params)`` — the name is returned too because
        it decides whether the validity mask applies.
    """
    alvo = (region or "auto").strip().lower()
    if alvo == "none":
        params = dict(_REGION_NONE)
    else:
        if alvo == "auto":
            alvo = detect_region(rel)
        if alvo not in _REGION_PARAMS:
            print(
                f"Unknown region {region!r}; defaulting to 'southeast'. "
                f"Valid: {', '.join(REGIONS)}."
            )
            alvo = "southeast"
        params = dict(_REGION_PARAMS[alvo])
    if custom_thresholds:
        params.update(custom_thresholds)
    return alvo, params


# ---------------------------------------------------------------------------
# Corrected variants
# ---------------------------------------------------------------------------
#
# Wherever climasus4r does something this package believes is wrong, the rule
# is to reproduce R anyway and record the finding (Andrey, 14/09/2026). That
# rule loses work unless the correct version is kept somewhere, so each of
# those cases also gets a *corrected variant*: a real indicator, with its own
# output column and its own tests.
#
# They are deliberately NOT in `ALL_INDICATORS`, so `indicators="all"` yields
# exactly the column set R yields. Ask for one by name to get it.
#
# This replaces what had grown ad hoc — a helper function for the wind chill,
# a column for the WBGT, and nothing at all for two others.

_CORRECTED_DEFS: dict[str, tuple[str, tuple[str, ...], str]] = {
    # ------------------------------------------------------------------
    # THI in the form usually quoted as Thom's discomfort index.
    # `thi_c` follows R's variant, T - (1 - RH/100)(T - 14.4)/2.
    # Neither is wrong — they are different published coefficients — so
    # this one exists to keep the alternative available, not to correct an
    # error. Measured gap: mean -0.046 °C, range -1.40 to +1.21, and 1.3%
    # disagreement on the 28 °C high_stress threshold.
    # ------------------------------------------------------------------
    "thi_classic": (
        "thi_classic_c",
        ("T", "RH"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN({T} - (0.55 - 0.0055 * {RH}) * ({T} - 14.5), 2) "
            "END AS thi_classic_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Koppen humidity that reports missing humidity as missing.
    # `koppen_humidity` follows R, whose `case_when` ends in
    # `TRUE ~ "Perhumid"` and so labels a null reading with the *wettest*
    # of the four bands. A count by class then files every absent reading
    # under "Perhumid" with nothing to show it happened.
    # ------------------------------------------------------------------
    "koppen_humidity_strict": (
        "koppen_humidity_strict",
        ("RH",),
        (
            "CASE"
            "  WHEN {RH} IS NULL THEN NULL"
            "  WHEN {RH} < 30.0 THEN 'Arid'"
            "  WHEN {RH} < 50.0 THEN 'Semi-arid'"
            "  WHEN {RH} < 70.0 THEN 'Humid'"
            "  ELSE 'Perhumid'"
            " END AS koppen_humidity_strict"
        ),
    ),
    # ------------------------------------------------------------------
    # Diurnal range partitioned by station as well as day.
    # `diurnal_range_c` follows R, which groups by day alone and so mixes
    # stations. This is the definition: one station's spread within one
    # calendar day.
    # ------------------------------------------------------------------
    "diurnal_range_station": (
        "diurnal_range_station_c",
        ("T",),
        (
            "(MAX({T}) OVER (PARTITION BY {STATION_COL}, {DATE_COL}::DATE) "
            "- MIN({T}) OVER (PARTITION BY {STATION_COL}, {DATE_COL}::DATE)) "
            "AS diurnal_range_station_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Effective temperature that survives calm air.
    # `et_c` follows R, where `pmax(ws, 0.04)` is defeated by the
    # `- 0.2` that follows: below 0.2 m/s the radicand goes negative and
    # the result is NaN. Here the *radicand* is floored at zero instead,
    # so a still hour yields the still-air effective temperature rather
    # than nothing. At ws >= 0.2 the two agree exactly.
    # ------------------------------------------------------------------
    "et_calm": (
        "et_calm_c",
        ("T", "RH", "WS"),
        (
            "CASE WHEN {T} IS NULL OR {RH} IS NULL THEN NULL ELSE "
            "ROUND_EVEN(GREATEST("
            "  {T} - 0.4 * ({T} - 10.0) * (1.0 - {RH} / 100.0)"
            "  - 1.1 * SQRT(GREATEST(COALESCE({WS}, 0.0) - 0.2, 0.0))"
            ", -273.15), 2) END AS et_calm_c"
        ),
    ),
    # ------------------------------------------------------------------
    # Heat stress risk that reports missing data as missing.
    # `heat_stress_risk` follows R, whose `case_when` ends in
    # `TRUE ~ "None"` and so labels a null WBGT "None" — the same string
    # cold weather legitimately gets. This one returns NULL instead, so
    # "no risk" and "no data" stop being the same answer.
    # ------------------------------------------------------------------
    "heat_stress_risk_strict": (
        "heat_stress_risk_strict",
        ("T", "RH", "WS", "SR"),
        (
            "CASE"
            "  WHEN {T} IS NULL OR {RH} IS NULL THEN NULL"
            "  WHEN {WBGT} > 33.0 THEN 'Extreme'"
            "  WHEN {WBGT} > 30.0 THEN 'Very High'"
            "  WHEN {WBGT} > 28.0 THEN 'High'"
            "  WHEN {WBGT} > 25.0 THEN 'Moderate'"
            "  WHEN {WBGT} > 20.0 THEN 'Low'"
            "  ELSE 'None'"
            " END AS heat_stress_risk_strict"
        ),
    ),
    # ------------------------------------------------------------------
    # NWS wind chill with the wind converted from m/s correctly.
    # `wct_c` follows R, which applies the km/h -> mph factor (0.621371)
    # to a column measured in m/s; the right factor is 2.23694. Promoted
    # from the `_wct_correct_units()` helper to an indicator so every
    # corrected variant lives in one place and is reachable the same way.
    # ------------------------------------------------------------------
    "wct_ms": (
        "wct_ms_c",
        ("T", "WS"),
        (
            "CASE WHEN {T} IS NULL THEN NULL"
            "{MASK_WIND}"
            "  ELSE ROUND_EVEN((("
            "  35.74 + 0.6215 * ({T} * 9.0 / 5.0 + 32.0)"
            f"  - 35.75 * POWER(GREATEST(COALESCE({{WS}}, 0.0) * {_MPH_PER_MS}, 0.01), 0.16)"
            "  + 0.4275 * ({T} * 9.0 / 5.0 + 32.0)"
            f"    * POWER(GREATEST(COALESCE({{WS}}, 0.0) * {_MPH_PER_MS}, 0.01), 0.16)"
            ") - 32.0) * 5.0 / 9.0, 2) END AS wct_ms_c"
        ),
    ),
}

#: `wbgt_stull` was the first corrected variant and shipped inside the
#: default set. Moved out so `indicators="all"` matches R's columns; it is
#: still reachable by name, like the rest.
_CORRECTED_DEFS["wbgt_stull"] = _INDICATOR_DEFS.pop("wbgt_stull")

_INDICATOR_DEFS.update(_CORRECTED_DEFS)

#: Indicators emitted by ``indicators="all"`` — R's set plus the
#: Python-only extras that predate this work. Excludes the corrected
#: variants above.
ALL_INDICATORS: tuple[str, ...] = tuple(
    k for k in _INDICATOR_DEFS if k not in _CORRECTED_DEFS
)

#: Opt-in corrected variants, and what each one is the corrected form of.
CORRECTED_INDICATORS: dict[str, str] = {
    "thi_classic": "thi",
    "koppen_humidity_strict": "koppen_humidity",
    "diurnal_range_station": "diurnal_range",
    "wct_ms": "wct",
    "wbgt_stull": "wbgt",
    "et_calm": "et",
    "heat_stress_risk_strict": "heat_stress_risk",
}

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
    # Each corrected variant inherits the thresholds of the indicator it
    # corrects — they measure the same physical quantity, so the same bands
    # apply. Sharing them is also what makes the comparison a *count*:
    # `wbgt_c_flag_extreme` against `wbgt_stull_c_flag_extreme` on the same
    # threshold is the M71 evidence in one number.
    "wbgt_stull": {"extreme_heat": 31, "high_stress": 28,
                   "moderate_stress": 25, "warning_low": 15},
    "thi_classic": {"high_stress": 28, "comfortable_max": 24,
                    "comfortable_min": 20},
    "diurnal_range_station": {"high": 15, "moderate": 10, "low": 5},
    "wct_ms": {"high_risk": -35, "moderate_risk": -20, "low_risk": -10},
    "koppen_humidity_strict": {},
    "et_calm": {"hot": 35, "warm": 30, "cool": 20, "cold": 15},
    "heat_stress_risk_strict": {},
}

#: Priority chains R uses to pick which declared threshold drives each flag.
#: The first name present in the indicator's threshold dict wins; when none
#: is present the column is emitted as constant FALSE.
_FLAG_CHAINS: dict[str, tuple[str, ...]] = {
    "extreme": ("extreme_heat", "extreme_heat_stress", "extreme_danger", "high_risk"),
    "high": ("high_stress", "dangerous", "moderate_heat_stress",
             "extreme_caution", "hot",
             # Added 21/09/2026 (D5/M72). `diurnal_range` and
             # `vapor_pressure` declare the threshold literally as
             # "high", which the chain did not look for, so all of their
             # flags were constant FALSE. Reviving these needs no
             # judgement about what the number means: the declared name
             # IS the flag's name.
             "high"),
    "low": ("warning_low", "low_stress", "slight_cold_stress", "cold",
            # Same date, same reasoning. "low" is `diurnal_range`'s own
            # name for its low threshold. "slight_cold" is `pet`'s, and
            # the chain already looked for "slight_cold_stress" — the
            # near miss M72 describes, which cost pet two columns.
            "low", "slight_cold"),
}

#: Threshold names that mean "worse BELOW this value", whichever flag
#: they end up feeding.
#:
#: The fix for the second half of M72, applied 21/09/2026 (D5). The
#: comparison used to be chosen by the flag alone — `extreme` and `high`
#: above, `low` below — and for a wind chill that is backwards. ``wcet``
#: and ``wct`` declare ``high_risk = -35``, which lands in the *extreme*
#: chain, so R's flag fires on ``value > -35``: TRUE at -30 °C and at
#: +20 °C, FALSE at -40 °C. Measured over a -40..60 grid it was TRUE in
#: 190 of 201 points, and on the coldest value of the fixture sample
#: (-43.61 °C, a risk to life) it was FALSE. The flag marked the mild
#: side and left out the dangerous one.
#:
#: Corrected rather than replicated because a flag that is FALSE exactly
#: where the hazard is does not carry the information in a poor form — it
#: carries the opposite of it. ``R_COLD_FLAGS_INVERTED`` reproduces R.
_FLAG_BELOW: frozenset[str] = frozenset({
    "high_risk", "moderate_risk", "low_risk",
    "warning_low", "low_stress", "slight_cold_stress", "slight_cold",
    "cold", "moderate_cold", "moderate_cold_stress", "strong_cold_stress",
    "extreme_cold_stress", "comfortable_min", "cool", "low",
})

#: Whether the cold flags keep R's inverted comparison (M72).
#:
#: ``False`` (the default) compares each flag against its threshold in the
#: direction the threshold's own name implies — see :data:`_FLAG_BELOW`.
#: ``True`` reproduces R exactly, and is what the parity fixtures are
#: compared against.
R_COLD_FLAGS_INVERTED: bool = False

#: Fallback by flag, for a threshold whose name says nothing about
#: direction: `extreme`/`high` fire above, `low` below.
_FLAG_COMPARISON: dict[str, str] = {"extreme": ">", "high": ">", "low": "<"}


def flag_comparison(indicator: str, flag: str) -> str:
    """The SQL operator for one flag, ``">"`` or ``"<"``.

    Decided by the name of the threshold that feeds the flag, not by the
    flag, so a cold-risk threshold fires on the cold side wherever it
    lands. With :data:`R_COLD_FLAGS_INVERTED` the flag decides, which is
    what R does.
    """
    if R_COLD_FLAGS_INVERTED:
        return _FLAG_COMPARISON[flag]
    nome = flag_threshold_name(indicator, flag)
    if nome is not None and nome in _FLAG_BELOW:
        return "<"
    return _FLAG_COMPARISON[flag]


def flag_threshold_name(indicator: str, flag: str) -> str | None:
    """Which declared threshold NAME drives one flag, or ``None``.

    The companion of :func:`flag_threshold`, which returns the value. The
    name is what decides the comparison direction, so it has to be
    reachable on its own.
    """
    thresholds = _INDICATOR_THRESHOLDS.get(indicator, {})
    for nome in _FLAG_CHAINS[flag]:
        if nome in thresholds:
            return nome
    return None


def flag_threshold(indicator: str, flag: str) -> float | None:
    """Which declared threshold drives one flag, or ``None`` for constant FALSE.

    Exposed rather than inlined because the answer is the whole of the R
    finding recorded as M72, and it is easier to read as data than to
    re-derive from the chains.

    **In R, 16 of the 30 flag columns are constant FALSE**, because the
    threshold names an indicator declares are not the names the chain
    looks for, and **2 more are inverted** — leaving 12 that carry
    correct information. What this package emits since 21/09/2026 (D5):

    ==========================  ======  =====
    of R's 30 flag columns       in R    here
    ==========================  ======  =====
    constant FALSE                  16     12
    inverted                         2      0
    carrying information            12     18
    ==========================  ======  =====

    **The 2 inverted ones are corrected**, because a flag that is FALSE
    exactly where the hazard is carries the opposite of the information,
    not a poor form of it. See :data:`_FLAG_BELOW`.

    **4 of the 16 dead ones are revived**, and only those where the
    declared name settles the question by itself: ``diurnal_range``
    declares its thresholds literally as ``high`` and ``low``,
    ``vapor_pressure`` as ``high``, and ``pet`` writes ``slight_cold``
    where the chain looked for ``slight_cold_stress`` — the near miss
    M72 describes.

    **The other 12 stay dead on purpose**, and most of them are dead for
    an honest reason: the indicator declares nothing for that end at all.
    ``heat_index`` has no cold threshold, so its ``low`` flag has no
    number to use; ``thi`` declares no extreme. The defect in those cases
    is emitting a constant-FALSE column instead of omitting it, not the
    missing threshold. Where a number *does* exist but under an ambiguous
    name — is ``moderate`` the *high* flag or the *low* one?
    ``caution``? ``comfortable_min``? — reviving it would mean choosing
    what the number means, which is a modelling call and not a port.

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
            op = flag_comparison(indicator, flag)
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


#: What R's `.compute_wbgt()` RETURNS — rounded to two decimals.
#:
#: The rounding belongs in here, not at the call site, and getting that
#: wrong cost three rows: `.compute_heat_stress_risk()` classifies the
#: value `.compute_wbgt()` gives it, which is already rounded, so a WBGT
#: of 19.997 is classified as 20.00 and falls on the "not greater than 20"
#: side. Substituting the raw expression instead put three fixture rows
#: one band too high, each of them sitting exactly on a threshold
#: (20.00, 28.00, 30.00).
#:
#: One fragment also means the risk bands can never drift from the
#: `wbgt_c` column they classify.
_WBGT_EXPR = (
    "ROUND_EVEN(0.7 * ("
    "  ({T} * atan(0.16 * sqrt(GREATEST("
    "     ({RH} / 100.0) * 0.6108 * EXP(17.27 * {T} / ({T} + 237.3))"
    "     , 0.01) + 0.1)) + 3.0)"
    "  + ({T} + 0.33 * ({RH} / 100.0) * EXP(0.0514 * {T}) - 4.0)"
    ") / 2.0"
    " + 0.2 * ({T} + 0.0144 * POWER(GREATEST("
    "     CASE WHEN {SR} IS NULL OR {SR} < 0 THEN 0.0"
    "          ELSE {SR} / 3.6 END, 0.0), 0.6)"
    "   / POWER(GREATEST(COALESCE({WS}, 0.0), 0.1), 0.2) - 2.0)"
    " + 0.1 * {T}, 2)"
)


#: What R falls back to when no column is literally named `date`: month 6.
PET_MONTH_FALLBACK = "6"


def _pet_month_expr(columns: Sequence[str]) -> str:
    """SQL for the month R's PET uses, or the literal fallback.

    R reads `df[["date"]]` by that exact name rather than the
    `datetime_col` it was given, so a series whose date column is called
    anything else silently gets month 6 for every row and the documented
    seasonal clothing adjustment never applies (M81). Replicated here by
    keying off a column literally named `date`.
    """
    return 'MONTH("date")' if "date" in columns else PET_MONTH_FALLBACK


#: The validity masks R applies when `apply_validity_mask` is on AND no
#: region is set. Empty strings when off — see `_mask_clauses`.
_MASK_HI = "  WHEN {T} < {P:hi_min_temp} OR {RH} < {P:hi_min_rh} THEN NULL"
_MASK_WIND = "  WHEN {T} > 10.0 OR COALESCE({WS}, 0.0) <= 1.3 THEN NULL"


#: Whether to reproduce R's coupling of the validity mask to ``region``.
#:
#: R computes ``apply_mask = apply_validity_mask && !use_region`` with
#: ``use_region <- region != "none"``. Its own default is ``region="auto"``,
#: so the second term is ``FALSE`` and the mask is **off** on the default
#: path — ``apply_validity_mask=TRUE`` alone never switches it on. The
#: parameter therefore does the opposite of what its name promises unless
#: the caller also passes ``region="none"``, which most callers will not.
#:
#: What the unmasked path produces is not a rounding difference. Measured
#: on a 4.000-row fixture: ``hi_c`` — a HEAT index — reaches **-16,38 °C**,
#: and ``wcet_c`` — a wind-chill formula, for COLD — reaches **+53,39 °C**
#: (with T=25 and wind 5 m/s it returns 26,34 instead of NA). Those are
#: the regressions extrapolating far outside the domains they were fitted
#: on, published with no warning.
#:
#: ``False`` (the default here) decouples them: ``apply_validity_mask``
#: governs the mask on its own, which is what the parameter's name and R's
#: own documentation describe. This is a DELIBERATE divergence from R's
#: behaviour on the default path, decided on 15/09/2026 — the numbers above
#: have no physical meaning, so replicating them faithfully would ship a
#: known-bad default. Recorded as **M83**.
#:
#: ``True`` reproduces R exactly, and is what the parity fixtures are
#: compared against.
R_COUPLES_MASK_TO_REGION: bool = False

#: Whether ``wct_c`` converts the wind with R's km/h factor (M69).
#:
#: R's ``.compute_wct`` does ``ws_mph <- pmax(ws * 0.621371, 0.01)``.
#: That factor converts **kilometres per hour** to miles per hour, and the
#: input column is ``ws_2_m_s`` — metres per second. The right factor is
#: 2.2369362920544, so R understates the wind by 3.6x.
#:
#: The proof needs no external reference, and that is what makes this one
#: safe to correct: R implements the same physical quantity **twice** —
#: ``wcet_c`` (Environment Canada, wind in km/h, conversion ``ws * 3.6``,
#: correct) and ``wct_c`` (NWS, wind in mph) — and the two published
#: regressions agree with each other to about 0.03 °C. So R's own output
#: shows the error. Measured over 200k points in the valid domain:
#: ``|wct_R - wcet|`` averages 4.503 °C (max 7.49), while
#: ``|wct_corrected - wcet|`` averages 0.024 °C (max 0.04). The 0.024 is
#: the intrinsic gap between the two regressions; the 4.5 is the unit bug.
#:
#: ``False`` (the default since 21/09/2026) uses the correct factor. This
#: **reversed** the earlier decision of 14/09/2026 to replicate: the
#: reason to replicate was to keep the presentation to the coordinator
#: defensible, and once the finding was presented and the decision came
#: back — D5, decided by Andrey on 21/09/2026 — replicating a wrong
#: number stopped being the careful choice. A column that is wrong by
#: 4.6 °C carries no information about wind chill; it is not information
#: in a poor form, which is the line this project draws for replicating.
#: Direction matters too: the error always runs warm, so a cold-wave
#: analysis reports less risk than exists.
#:
#: ``True`` reproduces R exactly, and is what the parity fixtures are
#: compared against.
R_WCT_USES_KMH_FACTOR: bool = False

def _mph_factor() -> float:
    """The m/s -> mph factor in force, per :data:`R_WCT_USES_KMH_FACTOR`."""
    return _MPH_PER_KMH if R_WCT_USES_KMH_FACTOR else _MPH_PER_MS


def _mask_clauses(apply_mask: bool) -> tuple[str, str]:
    """The two mask clauses, or empty strings when the mask is off.

    R computes `apply_mask = apply_validity_mask && !use_region`, so with
    its own defaults (`region="auto"`) the mask is OFF — see M83 for what
    that produces.
    """
    return (_MASK_HI, _MASK_WIND) if apply_mask else ("", "")


def _substitute_inmet_cols(
    template: str,
    station_col: str,
    date_col: str,
    month_expr: str = PET_MONTH_FALLBACK,
    params: dict[str, float] | None = None,
    apply_mask: bool = False,
) -> str:
    """Replace every placeholder: columns, WBGT, month, region params, masks."""
    mask_hi, mask_wind = _mask_clauses(apply_mask)
    out = template.replace("{MASK_HI}", mask_hi).replace("{MASK_WIND}", mask_wind)
    # {WBGT} next: the fragment itself contains {T}/{RH}/{SR}/{WS}, which
    # the column loop below then resolves.
    out = out.replace("{WBGT}", _WBGT_EXPR)
    out = out.replace("{PET_MONTH}", month_expr)
    # Read at render time, not baked in at import: the switch is a module
    # attribute the tests flip with monkeypatch, and a factor frozen into
    # the template string would ignore them.
    out = out.replace("{MPH}", repr(_mph_factor()))
    for chave, valor in (params or _REGION_NONE).items():
        out = out.replace("{P:" + chave + "}", repr(float(valor)))
    for key, col in _INMET_COLS.items():
        out = out.replace("{" + key + "}", col)
    out = out.replace("{STATION_COL}", station_col)
    out = out.replace("{DATE_COL}", date_col)
    return out


def _render_indicator_sql(
    ind: str,
    station_col: str,
    date_col: str,
    month_expr: str = PET_MONTH_FALLBACK,
    params: dict[str, float] | None = None,
    apply_mask: bool = False,
) -> str:
    """Render a regular indicator template; CHD/HWD use special render below."""
    _, _, template = _INDICATOR_DEFS[ind]
    return _substitute_inmet_cols(
        template, station_col, date_col, month_expr, params, apply_mask
    )


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


_SOLAR_CONSTANT = 1361.0  # W/m2, R's .SUS_PHYSICS$solar_constant

# The 13 columns R appends by joining the station table, in R's order.
_STATION_META_COLS = (
    "region", "federal_unit", "station_name", "latitude", "longitude",
    "altitude", "foundation_date", "id_link", "zona_climatica",
    "tipo_umidade", "distr_umidade", "temperatura_id", "descricao",
)


def _join_station_meta(
    rel: duckdb.DuckDBPyRelation,
    verbose: bool,
) -> duckdb.DuckDBPyRelation:
    """Attach the station metadata, as R's unconditional left join does.

    R ends the function with::

        station_meta <- get_spatial_station_cache(...) %>% sf::st_drop_geometry()
        result <- dplyr::left_join(result, station_meta, by = c("station_code"))

    That accounts for 13 of the 60 columns R returns by default - the
    station's identity plus five climate classifications. It is
    unconditional: ``use_cache`` and ``cache_dir`` only decide whether the
    downloaded table is cached, never whether the join happens. The key is
    the literal name ``station_code``, not the ``station_col`` argument.

    Two divergences from R, both forced and both deliberate:

    **Row multiplication refused (M107).** R's source table has 636 rows for
    609 stations, and none of the 26 repeated codes is a true duplicate -
    each pair disagrees about the climate classification, because the
    station point fell on a polygon boundary. R's left join therefore
    doubles every observation row for those stations; measured in R, 100
    rows of input for A134 come back as 200, and any count, mean or sum over
    them is silently doubled. ``climasus-data`` ships the table collapsed to
    one row per station, nulling only the fields whose source contradicts
    itself, so the join here cannot multiply rows.

    **Columns the input already has are kept.** In R ``result`` holds only
    the id columns and the indicators by this point, so all 13 arrive
    cleanly. Here the input's own columns survive (M105), and
    ``sus_climate_inmet`` already emits ``region``, ``station_name``,
    ``latitude``, ``longitude`` and ``altitude``. Those five are left alone
    - the INMET file's own header is at least as authoritative as the
    station table - and only the absent names are added.
    """
    cols = rel.limit(0).df().columns.tolist()
    if "station_code" not in cols:
        return rel

    from ..utils.data import data_path

    caminho = data_path("assets/climate/inmet_station_meta.parquet")
    if not caminho.is_file():
        if verbose:
            warnings.warn(
                "station metadata not found in climasus-data "
                f"({caminho.name}); the 13 station columns R appends will be "
                "absent. Update climasus-data to get them.",
                UserWarning, stacklevel=3,
            )
        return rel

    faltando = [c for c in _STATION_META_COLS if c not in cols]
    if not faltando:
        return rel

    escolhidas = ", ".join(f"m.{c}" for c in faltando)
    view = f"_station_meta_{uuid.uuid4().hex[:12]}"
    ordem = f"__ord_{uuid.uuid4().hex[:8]}"
    # DuckDB's hash join does not preserve row order; dplyr's left_join keeps
    # the left table's. Without this the observations come back scrambled,
    # which for an hourly series is worse than a missing column - measured on
    # a 4-row input that came back as [1200, 0, 500, 0] instead of
    # [0, 500, 0, 1200]. A row number carried across the join pins it.
    return rel.query(
        view,
        f"WITH t AS (SELECT *, row_number() OVER () AS {ordem} FROM {view}) "
        f"SELECT t.* EXCLUDE ({ordem}), {escolhidas} FROM t "
        f"LEFT JOIN read_parquet('{caminho.as_posix()}') AS m "
        f"ON t.station_code = m.station_code "
        f"ORDER BY t.{ordem}",
    )


def _verify_physics(
    rel: duckdb.DuckDBPyRelation,
    date_col: str,
    verbose: bool,
    lang: str,
) -> duckdb.DuckDBPyRelation:
    """Physical consistency check on solar radiation, as R's helper does.

    Mirrors ``.verify_solar_radiation``, which does **two** things - and the
    first one edits the data, which the M28 note originally got wrong:

    1. Negative ``sr_kj_m2`` is clamped to ``0``. Nulls are preserved.
    2. Values above 110% of the extraterrestrial irradiance are **counted and
       warned about**, never altered.

    The clamp is the reason ``verify_physics`` is not merely cosmetic. It is
    also the divergence recorded as latent in M28: without it Python sends a
    negative reading to ``NULL`` via the physical-range filter, while R sends
    it to ``0``.

    The irradiance follows R exactly, including the bias recorded in M28:
    ``ha_rad`` is built from the timestamp's hour as if it were solar time,
    when INMET timestamps are UTC. That misplaces the hour angle by three
    hours for Brazil. Since the value only drives a warning count, the
    consequence is a miscounted warning rather than corrupted data - so it is
    replicated rather than corrected.

    DuckDB's ``GREATEST`` ignores NULL where R's ``pmax`` propagates it, which
    is the trap that produced two defects in this module already. Hence the
    explicit ``CASE`` around ``sin_elev`` instead of ``GREATEST(sin_elev, 0)``:
    with a null latitude, ``GREATEST`` would return 0, making ``G0`` zero and
    every reading count as an exceedance.
    """
    sr = _INMET_COLS.get("SR", "sr_kj_m2")
    cols = rel.limit(0).df().columns.tolist()
    if sr not in cols:
        return rel

    clamp = (
        f"CASE WHEN {sr} IS NOT NULL AND {sr} < 0 THEN 0 ELSE {sr} END AS {sr}"
    )
    view = f"_verify_physics_{uuid.uuid4().hex[:12]}"
    rel = rel.query(view, f"SELECT * REPLACE ({clamp}) FROM {view}")

    if not (verbose and "latitude" in cols and date_col in cols):
        return rel

    doy = f"CAST(strftime({date_col}, '%j') AS INTEGER)"
    hour = f"CAST(strftime({date_col}, '%H') AS INTEGER)"
    lat_rad = "latitude * pi() / 180"
    dec_rad = f"(23.45 * sin(2 * pi() * (284 + {doy}) / 365)) * pi() / 180"
    ha_rad = f"(({hour} - 12) * 15) * pi() / 180"
    sin_elev = (
        f"(sin({lat_rad}) * sin({dec_rad}) "
        f"+ cos({lat_rad}) * cos({dec_rad}) * cos({ha_rad}))"
    )
    # pmax(sin_elev, 0) with R's NA propagation, not DuckDB's NULL-skipping
    sin_pos = (
        f"CASE WHEN {sin_elev} IS NULL THEN NULL "
        f"WHEN {sin_elev} > 0 THEN {sin_elev} ELSE 0 END"
    )
    g0 = (
        f"{_SOLAR_CONSTANT} * (1 + 0.033 * cos(2 * pi() * {doy} / 365)) "
        f"* ({sin_pos}) * 3.6"
    )
    count_view = f"_verify_count_{uuid.uuid4().hex[:12]}"
    n_flag = rel.query(
        count_view,
        f"SELECT count(*) FROM {count_view} "
        f"WHERE {sr} IS NOT NULL AND {sr} > ({g0}) * 1.1",
    ).fetchone()[0]

    if n_flag:
        msgs = {
            "pt": (f"Consistencia fisica: {n_flag} valores de SR > 110% da "
                   f"irradiancia extraterrestre."),
            "es": (f"Consistencia fisica: {n_flag} valores de SR > 110% de la "
                   f"irradiancia extraterrestre."),
            "en": (f"Physical consistency: {n_flag} SR values exceed 110% of "
                   f"extraterrestrial irradiance."),
        }
        warnings.warn(msgs.get(lang, msgs["en"]), UserWarning, stacklevel=3)
    return rel



# ---------------------------------------------------------------------------
# Monte Carlo uncertainty (compute_uncertainty)
# ---------------------------------------------------------------------------

#: Input uncertainty per indicator, exactly as R's indicator registry
#: declares it in ``reg$uncertainty_sd``. Thirteen of the fifteen have it;
#: ``heat_stress_risk`` and ``koppen_humidity`` are classifications and
#: declare none, so they get no interval — which is why
#: ``compute_uncertainty=True`` adds 26 columns and not 30.
#:
#: ``sr_frac`` is a *fractional* uncertainty: R perturbs solar radiation
#: multiplicatively, ``sr * (1 + rnorm(n, 0, sr_frac))``, while the other
#: three are additive.
_UNCERTAINTY_SD: dict[str, dict[str, float]] = {
    "wbgt": {"temp": 0.5, "rh": 3.0, "ws": 0.3, "sr_frac": 0.1},
    "heat_index": {"temp": 0.5, "rh": 3.0},
    "thi": {"temp": 0.5, "rh": 3.0},
    "wcet": {"temp": 0.5, "ws": 0.3},
    "wct": {"temp": 0.5, "ws": 0.3},
    "et": {"temp": 0.5, "rh": 3.0, "ws": 0.3},
    "utci": {"temp": 0.5, "rh": 3.0, "ws": 0.3, "sr_frac": 0.1},
    "pet": {"temp": 0.5, "rh": 3.0, "ws": 0.3, "sr_frac": 0.1},
    "cdd": {"temp": 0.5},
    "hdd": {"temp": 0.5},
    "gdd": {"temp": 0.5},
    "diurnal_range": {"temp": 0.5},
    "vapor_pressure": {"temp": 0.5, "rh": 3.0},
}

#: The order R draws in. It matters: each ``rnorm(n, 0, sd)`` consumes n
#: values from one stream, so a different order gives different (still
#: valid, but non-matching) perturbations.
_UNCERTAINTY_ORDER = ("temp", "rh", "ws", "sr_frac")
_UNCERTAINTY_COLS = {"temp": "T", "rh": "RH", "ws": "WS", "sr_frac": "SR"}

#: R's ``set.seed(2024L)``, called once per indicator inside
#: ``.compute_mc_uncertainty`` — so every indicator restarts the stream.
_MC_SEED = 2024
_MC_N_SIM = 200


def _mc_interval(
    rel: duckdb.DuckDBPyRelation,
    ind: str,
    station_col: str,
    date_col: str,
    month_expr: str,
    params: dict[str, float],
    n_sim: int = _MC_N_SIM,
) -> tuple[np.ndarray, np.ndarray] | None:
    """95% interval for one indicator, by Monte Carlo over input error.

    Reproduces ``.compute_mc_uncertainty`` term for term: ``set.seed(2024)``
    at the start, then ``n_sim`` perturbed evaluations, then the 2.5% and
    97.5% percentiles per row, rounded to two decimals.

    Matching R here is exact, not statistical, because
    :mod:`climasus4py.utils.r_random` reproduces R's stream bit for bit.
    Verified on the reference input: ``wbgt`` gives lower
    ``[20.70, 2.64, 25.96]`` and upper ``[22.28, 3.91, 27.89]`` on both
    sides, with mean widths agreeing to four decimals.

    Two properties inherited from R on purpose:

    - the indicator is dispatched with ``apply_mask=False``, so **an
      interval can exist where the point estimate is NULL**. On the
      reference input ``hi_c`` is NULL on row 2 while its interval reads
      38.10 to 46.76. Replicated, and recorded as M111.
    - a row whose every simulation is NULL yields NULL bounds, as R's
      ``quantile(..., na.rm = TRUE)`` on an all-NA slice does.

    Returns ``None`` for indicators that declare no input uncertainty.
    """
    unc = _UNCERTAINTY_SD.get(ind)
    if not unc:
        return None

    from ..utils.r_random import RRandom

    colunas = rel.limit(0).df().columns.tolist()
    # only the perturbations whose column is actually present, and in R's
    # order, because the order is what fixes the random stream
    ativos = [
        (chave, _INMET_COLS[_UNCERTAINTY_COLS[chave]])
        for chave in _UNCERTAINTY_ORDER
        if chave in unc and _INMET_COLS[_UNCERTAINTY_COLS[chave]] in colunas
    ]
    if not ativos:
        return None

    base = rel.df()
    n = len(base)
    originais = {col: base[col].to_numpy(dtype=float) for _, col in ativos}
    rng = RRandom(_MC_SEED)
    sql_ind = _render_indicator_sql(ind, station_col, date_col, month_expr,
                                    params, apply_mask=False)
    sims = np.empty((n, n_sim), dtype=float)

    for s in range(n_sim):
        perturbado = base.copy()
        for chave, col in ativos:
            ruido = rng.rnorm(n, 0.0, float(unc[chave]))
            if chave == "temp":
                perturbado[col] = originais[col] + ruido
            elif chave == "rh":
                perturbado[col] = np.clip(originais[col] + ruido, 0.0, 100.0)
            elif chave == "ws":
                perturbado[col] = np.maximum(originais[col] + ruido, 0.0)
            else:  # sr_frac: multiplicative
                perturbado[col] = np.maximum(originais[col] * (1.0 + ruido),
                                             0.0)
        alias = f"_mc_{uuid.uuid4().hex[:12]}"
        conexao = get_connection()
        conexao.register(alias, perturbado)
        try:
            sims[:, s] = (conexao.sql(f"SELECT {sql_ind} FROM {alias}")
                          .df().iloc[:, 0].to_numpy(dtype=float))
        finally:
            conexao.unregister(alias)

    # all-NULL rows give NULL bounds, as R's na.rm quantile does; numpy
    # warns on an all-NaN slice, and that warning is not news
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        baixo = np.nanquantile(sims, 0.025, axis=1)
        alto = np.nanquantile(sims, 0.975, axis=1)
    return np.round(baixo, 2), np.round(alto, 2)


def _attach_uncertainty(
    saida: duckdb.DuckDBPyRelation,
    entrada: duckdb.DuckDBPyRelation,
    ind_list: list[str],
    station_col: str,
    date_col: str,
    month_expr: str,
    params: dict[str, float],
    verbose: bool,
) -> duckdb.DuckDBPyRelation:
    """Append ``<col>_ci_low`` / ``<col>_ci_high`` for every eligible indicator.

    The bounds are computed in Python, not in SQL, so they are joined back
    by row position. **This materialises the relation**, which is the one
    place in this function where laziness is given up — a Monte Carlo over
    200 evaluations cannot be expressed as a lazy projection, and pretending
    otherwise would only hide the cost.

    Column order differs from R's, which interleaves each interval right
    after its indicator and before that indicator's flags. Here they are
    appended. Column order carries no meaning, and matching R's would mean
    rebuilding the single SELECT this function is built around.
    """
    colunas: dict[str, np.ndarray] = {}
    for ind in ind_list:
        faixa = _mc_interval(entrada, ind, station_col, date_col, month_expr,
                             params)
        if faixa is None:
            continue
        col = _INDICATOR_DEFS[ind][0]
        colunas[f"{col}_ci_low"], colunas[f"{col}_ci_high"] = faixa

    if not colunas:
        if verbose:
            warnings.warn(
                "compute_uncertainty=True but none of the requested "
                "indicators declares input uncertainty, so no interval was "
                "added. heat_stress_risk and koppen_humidity are "
                "classifications and have none.",
                UserWarning, stacklevel=3,
            )
        return saida

    base = saida.df()
    for nome, valores in colunas.items():
        base[nome] = valores
    return get_connection().from_df(base)


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_climate_compute_indicators(
    rel: duckdb.DuckDBPyRelation | pd.DataFrame,
    *,
    indicators: Sequence[str] | None = None,
    station_col: str | None = None,
    date_col: str | None = None,
    region: str = "auto",
    apply_validity_mask: bool = True,
    custom_thresholds: dict[str, float] | None = None,
    confidence_flags: bool = True,
    verify_physics: bool = True,
    keep_source_vars: bool = True,
    compute_uncertainty: bool = False,
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
        region: Brazilian biome whose constants the formulas use, or
            ``"auto"`` (default) to infer it from a ``UF`` or ``latitude``
            column, or ``"none"`` for the unregionalised defaults. Valid
            biomes: ``"amazon"``, ``"cerrado"``, ``"caatinga"``,
            ``"atlantic_forest"``, ``"pampa"``, ``"pantanal"``,
            ``"southeast"``, ``"south"``.
        keep_source_vars: Whether the input's own columns survive into the
            result. Defaults to ``True``, which is this port's historical
            behaviour and matches R's ``keep_source_vars=TRUE``, **not**
            R's default of ``FALSE`` (M105).

            R builds its result from the id columns alone, so its default
            output carries no input column at all. Here the ``SELECT`` has
            always opened with ``*``. Flipping the default would stop
            returning columns callers already receive, so ``False`` is
            offered as the opt-in that reproduces R's shape: id columns,
            indicators, flags and station metadata, and nothing of the
            input.

            Verified against R on the same input with one indicator:
            ``False`` gives the same 19 columns on both sides, in the same
            order. With ``True`` R returns 23 and this returns 24 - R adds
            back only the variables the requested indicators consume, while
            this keeps every input column, so the Python result is a
            superset.
        verify_physics: Run the solar-radiation consistency check before
            computing anything, as R does. Defaults to ``True``.

            It does two things, and the first one **edits the data**:
            negative ``sr_kj_m2`` is clamped to ``0`` (nulls preserved),
            and readings above 110% of the extraterrestrial irradiance are
            counted and warned about but left alone. Turning it off leaves
            negative readings untouched, which then reach the physical-range
            filter and become ``NULL``. That is the divergence recorded as
            latent in M28; this parameter is where it lives.

            Verified against R on 4.000 rows with 39 negatives, 25 nulls
            and 30 impossible readings: clamping identical on both sides,
            and the warning count identical at 2.347.

            The irradiance replicates R's bias (M28): the hour angle is
            built from the timestamp's hour as if it were solar time, while
            INMET timestamps are UTC - a three-hour misplacement. Since it
            only drives a warning count, it is replicated, not corrected.
        apply_validity_mask: Mask HI, WCET and WCT to ``NULL`` outside
            their meteorological domains. Defaults to ``True`` and, here,
            actually takes effect on its own.

            This is a deliberate divergence from R, which couples the mask
            to *region* — ``apply_validity_mask && !use_region`` — and so
            leaves it **off** on its own default path, where ``hi_c``
            reaches −16,38 °C and ``wcet_c`` reaches +53,39 °C. Set
            :data:`R_COUPLES_MASK_TO_REGION` to ``True`` to reproduce that.
            See M83.
        custom_thresholds: Overrides for individual region constants,
            applied after the biome table (R's ``modifyList``).
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
        # Validate against every KNOWN indicator, not just the default set:
        # the corrected variants are excluded from `indicators="all"` so the
        # default output matches R's columns, but asking for one by name has
        # to work.
        unknown = set(ind_list) - set(_INDICATOR_DEFS)
        if unknown:
            raise ValueError(
                f"Unknown indicator code(s): {sorted(unknown)}. "
                f"Available: {sorted(ALL_INDICATORS)}. "
                f"Corrected variants (opt-in): {sorted(CORRECTED_INDICATORS)}."
            )

    # Auto-detect columns
    _date_col = date_col or _detect_date_col(_rel)
    _station_col = station_col or _detect_station_col(_rel) or "1"

    # Validate required columns are present
    _check_required_cols(_rel, ind_list)

    # R runs this before computing anything, and it EDITS the data: negative
    # solar radiation is clamped to 0. See _verify_physics.
    if verify_physics:
        _rel = _verify_physics(_rel, _date_col, verbose, lang)

    # PET's clothing adjustment keys off a column literally named `date`,
    # because that is the name R hardcodes; anything else falls back to
    # month 6 (M81).
    _month_expr = _pet_month_expr(_rel.limit(0).df().columns.tolist())

    # Region decides the constants. Whether it ALSO decides the validity
    # mask is the M83 divergence — see R_COUPLES_MASK_TO_REGION.
    _region, _params = resolve_region_params(region, _rel, custom_thresholds)
    _apply_mask = (
        apply_validity_mask and _region == "none"
        if R_COUPLES_MASK_TO_REGION
        else apply_validity_mask
    )

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
    # R builds `result` from the id columns alone and only adds the source
    # variables back when keep_source_vars=TRUE, so its default output has no
    # input columns at all. Here the SELECT has always opened with `*`, which
    # means this port's default matches R's keep_source_vars=TRUE. Changing
    # that default would stop returning columns callers already receive
    # (M105), so the default stays and `keep_source_vars=False` is the opt-in
    # that reproduces R's shape: id columns, indicators, flags, station
    # metadata, and nothing of the input.
    if keep_source_vars:
        select_head = f"* EXCLUDE ({', '.join(helpers)})" if helpers else "*"
    else:
        id_cols = [c for c in (_date_col, _station_col)
                   if c != "1" and c in _rel.limit(0).df().columns.tolist()]
        select_head = ", ".join(id_cols) if id_cols else "NULL AS __no_id"

    indicator_exprs: list[str] = []
    flag_exprs: list[str] = []
    for ind in ind_list:
        if ind == "consecutive_hot_days":
            indicator_exprs.append(_render_chd_expr(_station_col, _date_col))
        elif ind == "heat_wave":
            indicator_exprs.append(_render_hw_expr(_station_col))
        else:
            indicator_exprs.append(
                _render_indicator_sql(
                    ind, _station_col, _date_col, _month_expr,
                    _params, _apply_mask,
                )
            )
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
    saida = _rel.query(input_view, sql)

    # R closes with an unconditional left join onto the station table; that
    # is where 13 of its 60 default columns come from. See _join_station_meta.
    saida = _join_station_meta(saida, verbose)

    if compute_uncertainty:
        saida = _attach_uncertainty(saida, _rel, ind_list, _station_col,
                                    _date_col, _month_expr, _params, verbose)
    return saida
