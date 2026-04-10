"""Benchmark: climasus4py pipeline — staged vs full pipeline.

Measures each pipeline stage individually and the full sus_pipeline() call.
Compares against R v0.0.1, v0.0.2, and v0.0.1_rc_a benchmarks.

Usage:
    python benchmark/benchmark_pipeline.py
"""

from __future__ import annotations

import csv
import gc
import os
import platform
import sys
import time
import tracemalloc
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("CLIMASUS_DATA_DIR", str(WORKSPACE / "climasus-data"))

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import climasus as cs
from climasus.core.engine import get_connection, read_parquets, schema_columns

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CACHE_DIR = WORKSPACE / "dados" / "cache"

SCENARIOS = {
    "pequeno": {
        "label": "Pequeno (SP 2023)",
        "system": "SIM-DO",
        "uf": "SP",
        "years": [2023],
        "parquets": [CACHE_DIR / "SIM-DO" / "SP_2023_all.parquet"],
    },
    "medio": {
        "label": "Medio (SP 2021-2023)",
        "system": "SIM-DO",
        "uf": "SP",
        "years": [2021, 2022, 2023],
        "parquets": [
            CACHE_DIR / "SIM-DO" / f"SP_{y}_all.parquet"
            for y in [2021, 2022, 2023]
        ],
    },
    "grande": {
        "label": "Grande (SP 2018-2023)",
        "system": "SIM-DO",
        "uf": "SP",
        "years": list(range(2018, 2024)),
        "parquets": [
            CACHE_DIR / "SIM-DO" / f"SP_{y}_all.parquet"
            for y in range(2018, 2024)
        ],
    },
}

# CID filter — same as R benchmark: cardiovascular group I10-I99
CID_CODES_CARDIOVASCULAR = [f"I{i:02d}" for i in range(10, 100)]

N_REPS = 3  # repetitions for each measurement

# R reference values (from comparativo_pipeline_rca_20260406_085100)
R_REFERENCE = {
    "pequeno": {
        "v0.0.1": 8.72,
        "v0.0.2": 8.22,
        "v0.0.1_rc_a": 0.49,
        "v0.0.1_peak_mb": 572.6,
        "v0.0.1_rc_a_peak_mb": 21.4,
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def timed(fn, label: str = "") -> tuple[float, object]:
    """Run fn(), return (elapsed_seconds, result). Does GC before timing."""
    gc.collect()
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    return elapsed, result


def measure_memory(fn) -> tuple[float, float, object]:
    """Run fn() with tracemalloc. Return (elapsed, peak_mb, result)."""
    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return elapsed, peak / (1024 * 1024), result


def median_of(fn, n: int = N_REPS) -> float:
    """Run fn() n times, return median elapsed time."""
    times = []
    for _ in range(n):
        t, _ = timed(fn)
        times.append(t)
    times.sort()
    return times[len(times) // 2]


def count_rows(rel) -> int:
    """Count rows in a DuckDB relation."""
    return rel.count("*").fetchone()[0]


def format_time(sec: float) -> str:
    if sec < 0.001:
        return f"{sec*1_000_000:.0f}µs"
    if sec < 1:
        return f"{sec*1_000:.1f}ms"
    return f"{sec:.3f}s"


def collect_env() -> dict:
    return {
        "os": platform.system(),
        "os_version": platform.version(),
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "pyarrow": pa.__version__,
        "pandas": pd.__version__,
        "climasus": cs.__version__,
        "cpu": platform.processor(),
        "timestamp": datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Benchmark: Staged Pipeline (step-by-step, like R v0.0.1/v0.0.2)
# ---------------------------------------------------------------------------


def benchmark_staged(scenario: dict) -> list[dict]:
    """Run each pipeline stage separately, measuring time and rows."""
    results = []
    parquets = scenario["parquets"]
    system = scenario["system"]

    # Verify files exist
    for p in parquets:
        if not p.is_file():
            print(f"  SKIP: {p.name} not found")
            return results

    # 1. Import (read parquet → DuckDB lazy)
    t, rel = timed(lambda: read_parquets(parquets))
    nrows = count_rows(rel)
    results.append({"stage": "import", "time": t, "rows": nrows, "cols": len(rel.columns)})

    # 2. Clean
    t, rel_clean = timed(lambda: cs.sus_clean(rel))
    nrows_clean = count_rows(rel_clean)
    results.append({"stage": "clean", "time": t, "rows": nrows_clean})

    # 3. Standardize
    t, rel_std = timed(lambda: cs.sus_standardize(rel_clean, system=system))
    results.append({"stage": "standardize", "time": t, "rows": count_rows(rel_std)})

    # 4. Filter (cardiovascular CID codes)
    t, rel_filt = timed(lambda: cs.sus_filter(rel_std, codes=CID_CODES_CARDIOVASCULAR))
    nrows_filt = count_rows(rel_filt)
    results.append({"stage": "filter", "time": t, "rows": nrows_filt})

    # 5. Aggregate (by month)
    t, rel_agg = timed(lambda: cs.sus_aggregate(rel_filt, time="month", geo="state"))
    nrows_agg = count_rows(rel_agg)
    results.append({"stage": "aggregate", "time": t, "rows": nrows_agg})

    # 6. Export (parquet to temp)
    import tempfile
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    t, _ = timed(lambda: cs.sus_export(rel_agg, tmp, fmt="parquet"))
    results.append({"stage": "export", "time": t, "rows": nrows_agg, "size_mb": tmp.stat().st_size / (1024**2)})
    tmp.unlink(missing_ok=True)

    return results


# ---------------------------------------------------------------------------
# Benchmark: Full Pipeline (like R rc_a fast path)
# ---------------------------------------------------------------------------


def benchmark_full_pipeline(scenario: dict) -> tuple[float, float, int]:
    """Run sus_pipeline() end-to-end. Return (elapsed, peak_mb, nrows)."""
    import tempfile
    tmp = Path(tempfile.mktemp(suffix=".parquet"))

    def run():
        return cs.sus_pipeline(
            system=scenario["system"],
            uf=scenario["uf"],
            year=scenario["years"],
            groups="cardiovascular",
            time="month",
            geo="state",
            output=str(tmp),
            cache_dir=str(CACHE_DIR),
            verbose=False,
        )

    elapsed, peak_mb, rel = measure_memory(run)
    nrows = count_rows(rel)
    tmp.unlink(missing_ok=True)
    return elapsed, peak_mb, nrows


# ---------------------------------------------------------------------------
# Benchmark: Raw DuckDB SQL (theoretical floor)
# ---------------------------------------------------------------------------


def benchmark_raw_duckdb(scenario: dict) -> float:
    """Pure DuckDB SQL — fastest possible, no Python overhead."""
    paths = [str(p) for p in scenario["parquets"] if p.is_file()]
    if not paths:
        return float("nan")

    conn = get_connection()
    paths_sql = ", ".join(f"'{p}'" for p in paths)
    codes_sql = ", ".join(f"'{c}'" for c in CID_CODES_CARDIOVASCULAR)

    sql = f"""
        SELECT
            STRFTIME(TRY_CAST("DTOBITO" AS DATE), '%Y-%m') AS time_group,
            COUNT(*) AS count
        FROM read_parquet([{paths_sql}], union_by_name=True)
        WHERE SUBSTR("CAUSABAS", 1, 3) IN ({codes_sql})
        GROUP BY time_group
        ORDER BY time_group
    """

    def run():
        return conn.sql(sql).fetchdf()

    return median_of(run, N_REPS)


# ---------------------------------------------------------------------------
# Benchmark: Materialization cost
# ---------------------------------------------------------------------------


def benchmark_materialize(scenario: dict) -> dict:
    """Measure the cost of materializing to pandas DataFrame."""
    paths = [str(p) for p in scenario["parquets"] if p.is_file()]
    if not paths:
        return {}
    
    conn = get_connection()
    if len(paths) == 1:
        rel = conn.read_parquet(paths[0])
    else:
        rel = conn.read_parquet(paths, union_by_name=True)

    # Lazy → pandas
    t_pandas, peak_pandas, df = measure_memory(lambda: rel.df())
    
    # Lazy → arrow
    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    arrow_table = rel.arrow()
    t_arrow = time.perf_counter() - t0
    _, peak_arrow = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return {
        "to_pandas_sec": t_pandas,
        "to_pandas_peak_mb": peak_pandas,
        "to_arrow_sec": t_arrow,
        "to_arrow_peak_mb": peak_arrow / (1024**2),
        "shape": df.shape,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def print_header(title: str):
    width = 70
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def print_stage_table(stages: list[dict]):
    """Print a formatted table of stage results."""
    print(f"  {'Stage':<20} {'Time':>10} {'Rows':>12} {'Note':>15}")
    print(f"  {'-'*20} {'-'*10} {'-'*12} {'-'*15}")
    total = 0.0
    for s in stages:
        t = s["time"]
        total += t
        rows = f"{s.get('rows', 0):,}" if s.get("rows") else ""
        note = ""
        if "size_mb" in s:
            note = f"{s['size_mb']:.2f} MB"
        if "cols" in s:
            note = f"{s['cols']} cols"
        print(f"  {s['stage']:<20} {format_time(t):>10} {rows:>12} {note:>15}")
    print(f"  {'-'*20} {'-'*10}")
    print(f"  {'TOTAL':<20} {format_time(total):>10}")
    return total


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = ROOT / "benchmark" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    env = collect_env()

    print_header("BENCHMARK: climasus4py v" + cs.__version__)
    print(f"  Python {env['python']} | DuckDB {env['duckdb']} | PyArrow {env['pyarrow']}")
    print(f"  OS: {env['os']} {env['os_version']}")
    print(f"  Date: {env['timestamp']}")

    all_results = []

    for scenario_key in ["pequeno", "medio", "grande"]:
        scenario = SCENARIOS[scenario_key]

        # Check files exist
        missing = [p for p in scenario["parquets"] if not p.is_file()]
        if missing:
            print(f"\n  SKIP {scenario_key}: missing {len(missing)} file(s)")
            continue

        # Count total rows (lazy)
        rel = read_parquets(scenario["parquets"])
        total_rows = count_rows(rel)
        total_size_mb = sum(p.stat().st_size for p in scenario["parquets"]) / (1024**2)

        print_header(f"Cenário: {scenario['label']} ({total_rows:,} rows, {total_size_mb:.1f} MB)")

        # --- 1. Staged Pipeline ---
        print("\n  [1] Pipeline por Etapas (como R v0.0.1/v0.0.2):")
        staged = benchmark_staged(scenario)
        total_staged = print_stage_table(staged)

        # --- 2. Full Pipeline ---
        print(f"\n  [2] Pipeline Completo (sus_pipeline, como R rc_a):")
        times_full = []
        for i in range(N_REPS):
            elapsed, peak_mb, nrows = benchmark_full_pipeline(scenario)
            times_full.append(elapsed)
            if i == 0:
                first_peak = peak_mb
                first_nrows = nrows

        med_full = sorted(times_full)[len(times_full) // 2]
        print(f"  {'sus_pipeline()':<20} {format_time(med_full):>10} {first_nrows:>12,} rows")
        print(f"  {'Peak memory':<20} {first_peak:>10.1f} MB")

        # --- 3. Raw DuckDB ---
        print(f"\n  [3] SQL Puro DuckDB (piso teorico):")
        raw_time = benchmark_raw_duckdb(scenario)
        print(f"  {'raw SQL':<20} {format_time(raw_time):>10}")

        # --- 4. Materialize cost ---
        print(f"\n  [4] Custo de Materializacao ({total_rows:,} rows):")
        mat = benchmark_materialize(scenario)
        if mat:
            print(f"  {'>> pandas':<20} {format_time(mat['to_pandas_sec']):>10}  (peak {mat['to_pandas_peak_mb']:.1f} MB)")
            print(f"  {'>> arrow':<20} {format_time(mat['to_arrow_sec']):>10}  (peak {mat['to_arrow_peak_mb']:.1f} MB)")

        # --- 5. Comparison with R ---
        r_ref = R_REFERENCE.get(scenario_key)
        if r_ref:
            print(f"\n  [5] Comparacao com R (cenario {scenario_key}):")
            print(f"  {'Versao':<25} {'Tempo':>10} {'Speedup vs v0.0.1':>20}")
            print(f"  {'-'*25} {'-'*10} {'-'*20}")
            for label, t in [
                ("R v0.0.1", r_ref["v0.0.1"]),
                ("R v0.0.2", r_ref["v0.0.2"]),
                ("R v0.0.1_rc_a", r_ref["v0.0.1_rc_a"]),
                ("Python staged", total_staged),
                ("Python pipeline", med_full),
                ("Python raw SQL", raw_time),
            ]:
                speedup = r_ref["v0.0.1"] / t if t > 0 else float("inf")
                print(f"  {label:<25} {format_time(t):>10} {speedup:>18.1f}x")

            if "v0.0.1_peak_mb" in r_ref:
                print(f"\n  Memoria:")
                print(f"  {'R v0.0.1':<25} {r_ref['v0.0.1_peak_mb']:>10.1f} MB")
                print(f"  {'R rc_a':<25} {r_ref['v0.0.1_rc_a_peak_mb']:>10.1f} MB")
                print(f"  {'Python pipeline':<25} {first_peak:>10.1f} MB")

        # Collect results
        all_results.append({
            "scenario": scenario_key,
            "total_rows": total_rows,
            "parquet_mb": total_size_mb,
            "staged_total_sec": total_staged,
            "pipeline_median_sec": med_full,
            "pipeline_peak_mb": first_peak,
            "raw_duckdb_sec": raw_time,
            "materialize_pandas_sec": mat.get("to_pandas_sec", 0),
            "materialize_arrow_sec": mat.get("to_arrow_sec", 0),
            "stages": {s["stage"]: s["time"] for s in staged},
        })

    # --- Save CSV ---
    csv_path = results_dir / f"benchmark_python_{timestamp}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "scenario", "total_rows", "parquet_mb",
            "staged_total_sec", "pipeline_median_sec", "pipeline_peak_mb",
            "raw_duckdb_sec",
            "import_sec", "clean_sec", "standardize_sec",
            "filter_sec", "aggregate_sec", "export_sec",
            "materialize_pandas_sec", "materialize_arrow_sec",
        ])
        writer.writeheader()
        for r in all_results:
            row = {k: v for k, v in r.items() if k != "stages"}
            for stage in ["import", "clean", "standardize", "filter", "aggregate", "export"]:
                row[f"{stage}_sec"] = r["stages"].get(stage, 0)
            writer.writerow(row)

    # Save environment
    env_path = results_dir / f"benchmark_python_{timestamp}_env.csv"
    with open(env_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(env.keys()))
        writer.writeheader()
        writer.writerow(env)

    print(f"\n{'=' * 70}")
    print(f"  Resultados salvos em: {csv_path}")
    print(f"  Ambiente salvo em: {env_path}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
