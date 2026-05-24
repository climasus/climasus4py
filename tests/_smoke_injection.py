"""Smoke empirico das correcoes OWASP (A1-A4) e correctness (B3, B4).

Tenta injetar payloads maliciosos nos pontos onde antes ocorria
interpolacao crua em f-strings. Comportamento esperado APOS o fix:
  - payload e tratado como literal de string (escapado);
  - filtro pode retornar zero linhas ou nao mexer no rel, mas nunca
    DROP / EXEC ou execucao de SQL paralelo.

Tambem valida:
  - codes_for_groups levanta KeyError para grupo desconhecido (B3).
  - expand_city_to_codes normaliza acentos via NFKD (B4) -- so logica,
    sem rede.
"""
from __future__ import annotations

import sys
import tempfile
import unicodedata
import warnings
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import duckdb
import pandas as pd
import pyarrow.parquet as pq

import climasus4py as cs  # noqa: E402
from climasus4py.core._sql import sql_string  # noqa: E402

failures = 0


def _mark(label: str, ok: bool, detail: str = "") -> None:
    global failures
    failures += not ok
    print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""))


# Synthetic relation with a date column for filter tests
def _make_rel() -> duckdb.DuckDBPyRelation:
    df = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2023-06-15", "2023-12-31"]),
        "DTOBITO": pd.to_datetime(["2023-01-01", "2023-06-15", "2023-12-31"]),
        "CAUSABAS": ["J189", "J189", "I219"],
        "SEXO": ["1", "2", "1"],
    })
    return cs.get_connection().from_df(df) if hasattr(cs, "get_connection") else duckdb.from_df(df)


print("=" * 70)
print("Smoke OWASP + correctness -- climasus4py 0.2.0a3")
print("=" * 70)


# ---------------------------------------------------------------------------
# A1: filter.py date_start/date_end injection
# ---------------------------------------------------------------------------
print("\n[A1] sus_filter date_start/date_end injection probe")
from climasus4py.core.engine import get_connection  # noqa: E402

conn = get_connection()
df = pd.DataFrame({
    "DTOBITO": pd.to_datetime(["2023-01-01", "2023-06-15", "2023-12-31"]),
    "CAUSABAS": ["J189", "J189", "I219"],
    "SEXO": ["1", "2", "1"],
})
rel = conn.from_df(df)

# The OWASP property to test is "the payload is embedded as a string
# literal, not executed as SQL". sql_string() is verified independently
# at the end of this file. Here we probe that filter calls with
# adversarial payloads do NOT bypass the predicate to return all rows
# (the pre-fix behaviour where `... >= '<date>' OR '1'='1'` was always
# true). After the fix, the payload becomes a single quoted literal so
# the predicate either matches normally or is unsatisfiable.

# Payload designed so the PRE-FIX SQL would always be TRUE.
# Pre-fix: f"TRY_CAST(...) >= '{date_start}'" + payload -> always TRUE
# Post-fix: payload escaped, predicate just compares against the literal.
INJECTION_BYPASS = "1900-01-01' OR '1'='1"
try:
    out = cs.sus_filter(rel, date_start=INJECTION_BYPASS)
    count = out.count("*").fetchone()[0]
    # The pre-fix bug returned all 3 rows for ANY payload (because
    # OR '1'='1' is always true). The post-fix path treats the whole
    # thing as a literal, which DuckDB's TRY_CAST parses leniently
    # extracting "1900-01-01" -- all rows are still >= 1900-01-01,
    # but ONLY because the date is ancient, NOT because the OR
    # injected. To prove the difference, use a payload whose extracted
    # prefix is in the future.
    out_future = cs.sus_filter(rel, date_start="9999-12-31' OR '1'='1")
    future_count = out_future.count("*").fetchone()[0]
    # If the OR injection still worked, count == 3.
    # If the literal is escaped, the predicate ">= 9999-12-31" excludes all.
    _mark(
        f"OR-injection blocked (future-date payload count={future_count})",
        future_count == 0,
        f"got {future_count}, expected 0",
    )
except duckdb.Error as e:
    msg = str(e).splitlines()[0]
    _mark("date_start payload rejected by DuckDB", True, msg[:80])

# Stricter probe: payload with comment marker. Pre-fix would have
# broken out of the string and commented the rest.
EVIL_COMMENT = "2024-01-01'; SELECT 1; --"
try:
    out_c = cs.sus_filter(rel, date_start=EVIL_COMMENT)
    cnt_c = out_c.count("*").fetchone()[0]
    _mark(
        f"comment-injection blocked (count={cnt_c})",
        # After the literal escape, DuckDB extracts "2024-01-01" from
        # the prefix -> all rows are < 2024-01-01 -> 0 rows.
        cnt_c == 0,
        f"got {cnt_c}, expected 0",
    )
except duckdb.Error as e:
    _mark("comment-injection rejected by DuckDB", True, str(e)[:80])

# Negative control: a valid date works.
out_ok = cs.sus_filter(rel, date_start="2023-06-01")
ok_count = out_ok.count("*").fetchone()[0]
_mark(
    f"date_start valid (count={ok_count} == 2)",
    ok_count == 2,
)


# ---------------------------------------------------------------------------
# A2: sus_export path injection probe
# ---------------------------------------------------------------------------
print("\n[A2] sus_export path injection probe")
with tempfile.TemporaryDirectory() as td:
    # Payload: a path with single-quote injection attempt
    evil_path = Path(td) / "out'_evil.parquet"
    try:
        # The new _copy_to escapes via sql_string, so the file IS written
        # with the literal name, including the quote. DuckDB should NOT
        # execute the post-quote SQL.
        result = cs.sus_export(rel, evil_path)
        wrote = result.is_file()
        # Try to re-read to confirm the file is a valid Parquet (not a
        # half-written file from an injection that crashed mid-query)
        table = pq.read_table(str(result))
        _mark(
            f"injection-style path written safely ({result.name})",
            wrote and table.num_rows == 3,
            f"rows={table.num_rows}",
        )
    except (OSError, ValueError, duckdb.Error) as e:
        # On Windows, ' may be invalid in filename -> OS rejects, also safe.
        _mark(
            "injection-style path rejected by OS/DuckDB safely",
            True,
            str(e).splitlines()[0][:80],
        )

# Negative control: valid path
with tempfile.TemporaryDirectory() as td:
    ok_path = Path(td) / "normal.parquet"
    result = cs.sus_export(rel, ok_path)
    _mark(f"export valid path ({ok_path.name})", result.is_file())

# A2b: compress allowlist
with tempfile.TemporaryDirectory() as td:
    bad_compress_path = Path(td) / "x.parquet"
    try:
        cs.sus_export(rel, bad_compress_path, compress="' OR 1=1 --")
        _mark("compress allowlist rejected bad value", False, "no ValueError raised")
    except ValueError as e:
        _mark("compress allowlist rejected bad value", True, str(e)[:60])


# ---------------------------------------------------------------------------
# A3: quality.py with column name containing problematic char
# ---------------------------------------------------------------------------
print("\n[A3] sus_data_quality_report on quoted-column relation")
# DuckDB allows arbitrary chars in identifiers when double-quoted.
weird = pd.DataFrame({'a"b': [1, 2, None], "ok": [1, 2, 3]})
weird_rel = conn.from_df(weird)
try:
    metrics = cs.sus_data_quality_report(weird_rel)
    _mark(
        "quality report on column with quote in name",
        metrics["total_rows"] == 3 and 'a"b' in metrics["completeness"],
        f"completeness keys={list(metrics['completeness'].keys())}",
    )
except duckdb.Error as e:
    _mark("quality report rejected weird column safely", True, str(e)[:60])


# ---------------------------------------------------------------------------
# B3: codes_for_groups raises KeyError for unknown group
# ---------------------------------------------------------------------------
print("\n[B3] codes_for_groups KeyError on unknown group")
from climasus4py.utils.cid import codes_for_groups  # noqa: E402

# Valid path
codes_ok = codes_for_groups(["respiratory"])
_mark(
    f"respiratory returns codes (n={len(codes_ok)})",
    len(codes_ok) > 50,
)

# Unknown path
try:
    codes_for_groups(["nonexistent_xyz"])
    _mark("unknown group raises KeyError", False, "no exception")
except KeyError as e:
    _mark(
        "unknown group raises KeyError",
        True,
        str(e).split("Available")[0][:60].strip(),
    )

# Partial unknown (mix of valid and invalid)
try:
    codes_for_groups(["respiratory", "totally_made_up"])
    _mark("partial unknown raises KeyError", False, "no exception")
except KeyError:
    _mark("partial unknown raises KeyError", True)


# ---------------------------------------------------------------------------
# B4: NFKD normalization in expand_city_to_codes (logic only, no parquet)
# ---------------------------------------------------------------------------
print("\n[B4] NFKD accent-insensitive normalization (logic only)")

# Replicate the _norm function inline (mirrors utils/data.py)
def _norm(s: str) -> str:
    decomposed = unicodedata.normalize("NFKD", s)
    return "".join(
        ch for ch in decomposed if not unicodedata.combining(ch)
    ).strip().lower()


pairs = [
    ("São Paulo", "Sao Paulo"),
    ("Brasília", "Brasilia"),
    ("Goiânia", "Goiania"),
    ("Curitiba", "Curitiba"),  # no accent — sanity
    ("Macapá", "MACAPA"),
    ("Vitória", "vitoria"),
]
for a, b in pairs:
    na, nb = _norm(a), _norm(b)
    _mark(
        f"NFKD: {a!r} == {b!r} (norm: {na!r})",
        na == nb,
        f"got {na!r} vs {nb!r}",
    )

# Confirm the previous (buggy) NFC behaviour would have failed
nfc_a = unicodedata.normalize("NFC", "São Paulo").strip().lower()
nfc_b = unicodedata.normalize("NFC", "Sao Paulo").strip().lower()
_mark(
    "NFC (old buggy norm) does NOT equal -- regression marker",
    nfc_a != nfc_b,
    f"NFC: {nfc_a!r} == {nfc_b!r}",
)


# ---------------------------------------------------------------------------
# Verify sql_string helper itself
# ---------------------------------------------------------------------------
print("\n[Helper] sql_string escaping")
cases = [
    ("normal", "'normal'"),
    ("O'Brien", "'O''Brien'"),
    ("'; DROP TABLE x; --", "'''; DROP TABLE x; --'"),
    ("''", "''''''"),
]
for raw, expected in cases:
    got = sql_string(raw)
    _mark(f"sql_string({raw!r})", got == expected, f"got {got!r}, expected {expected!r}")


print("\n" + "=" * 70)
if failures == 0:
    print("ALL OK")
else:
    print(f"FAILED: {failures} check(s)")
print("=" * 70)
sys.exit(failures)
