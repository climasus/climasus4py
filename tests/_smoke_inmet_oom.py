"""Smoke empirico do fix de OOM em sus_climate_inmet (nao vai para o CI).

Mede pico de memoria residente do processo durante:
  1. Primeira chamada (cache miss -> download + parse APENAS dos CSVs da UF).
  2. Segunda chamada (cache hit -> DuckDB push-down no Hive partition).
  3. Warning quando uf=None (probe stubado, sem download nacional).

Critério: pico de RSS proximo do baseline + dataset SP (alvo: < 4 GB total).

Uso:
    python tests/_smoke_inmet_oom.py
"""
from __future__ import annotations

import gc
import os
import sys
import tempfile
import threading
import time
import warnings
from pathlib import Path

import psutil

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import climasus4py as cs  # noqa: E402

PROC = psutil.Process(os.getpid())


class RssTracker:
    """Background thread sampling RSS every 250 ms to capture peaks."""

    def __init__(self) -> None:
        self.peak_rss = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "RssTracker":
        self.peak_rss = PROC.memory_info().rss
        self._stop.clear()
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def _sample(self) -> None:
        while not self._stop.is_set():
            try:
                rss = PROC.memory_info().rss
                if rss > self.peak_rss:
                    self.peak_rss = rss
            except Exception:
                pass
            time.sleep(0.25)

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def _mb(b: int) -> str:
    return f"{b / 1024 / 1024:,.0f} MB"


def _mark(label: str, ok: bool) -> str:
    return f"[{'OK' if ok else 'FAIL'}] {label}"


def main() -> int:
    failures = 0
    print("=" * 70)
    print("Smoke OOM -- sus_climate_inmet (climasus4py 0.2.0a3)")
    print(f"Process baseline RSS: {_mb(PROC.memory_info().rss)}")
    print("=" * 70)

    with tempfile.TemporaryDirectory(prefix="climasus_smoke_") as tmp:
        cache_dir = Path(tmp) / "inmet_cache"

        # --- (1) Cache miss: ONLY SP CSVs should be parsed ---
        print("\n[1/3] First call: uf='SP', years=2023 (cache MISS)")
        gc.collect()
        rss_before = PROC.memory_info().rss
        t0 = time.perf_counter()
        with RssTracker() as tracker:
            df1 = cs.sus_climate_inmet(
                years=2023,
                uf="SP",
                cache_dir=cache_dir,
                parallel=False,
                verbose=True,
            )
        dt = time.perf_counter() - t0
        peak1 = tracker.peak_rss
        rss_after = PROC.memory_info().rss
        delta1 = peak1 - rss_before
        print(f"  rows         : {len(df1):,}")
        print(f"  cols         : {len(df1.columns)}")
        ufs = sorted(df1["UF"].dropna().unique().tolist()) if "UF" in df1 else []
        print(f"  ufs returned : {ufs}")
        print(f"  duration     : {dt:.1f} s")
        print(f"  peak RSS     : {_mb(peak1)}")
        print(f"  delta peak   : {_mb(delta1)}")
        print(f"  RSS after    : {_mb(rss_after)}")
        ok1 = peak1 < 4 * 1024**3  # 4 GB target
        ok1_only_sp = ufs == ["SP"]
        print(_mark(f"peak < 4 GB (got {_mb(peak1)})", ok1))
        print(_mark(f"only SP returned (got {ufs})", ok1_only_sp))
        failures += (not ok1) + (not ok1_only_sp)

        del df1
        gc.collect()

        # --- Verify cache layout is Hive per-UF ---
        cache_year_dir = cache_dir / "inmet_parquet" / "year=2023"
        uf_subdirs = sorted(
            p.name for p in cache_year_dir.iterdir() if p.is_dir()
        ) if cache_year_dir.exists() else []
        national_file = (cache_year_dir / "data.parquet").is_file()
        print(f"\n  cache layout : sub-dirs={uf_subdirs}, "
              f"national_file={national_file}")
        ok_layout = uf_subdirs == ["UF=SP"] and not national_file
        print(_mark("Hive partition by UF (no national file)", ok_layout))
        failures += not ok_layout

        # --- (2) Cache hit: DuckDB push-down with UF filter ---
        print("\n[2/3] Second call: uf='SP', years=2023 (cache HIT)")
        rss_before = PROC.memory_info().rss
        t0 = time.perf_counter()
        with RssTracker() as tracker2:
            df2 = cs.sus_climate_inmet(
                years=2023,
                uf="SP",
                cache_dir=cache_dir,
                parallel=False,
                verbose=True,
            )
        dt = time.perf_counter() - t0
        peak2 = tracker2.peak_rss
        delta2 = peak2 - rss_before
        print(f"  rows         : {len(df2):,}")
        print(f"  duration     : {dt:.1f} s")
        print(f"  peak RSS     : {_mb(peak2)}")
        print(f"  delta peak   : {_mb(delta2)}")
        ok2 = peak2 < 4 * 1024**3
        ok2_fast = dt < 30
        print(_mark(f"peak < 4 GB (got {_mb(peak2)})", ok2))
        print(_mark(f"hit faster than miss (got {dt:.1f}s)", ok2_fast))
        failures += (not ok2) + (not ok2_fast)

        del df2
        gc.collect()

        # --- (3) Warning when uf=None (probe stubado) ---
        print("\n[3/3] Warning check: uf=None must emit UserWarning")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                import climasus4py.core.climate_inmet as inmet_mod
                _orig = inmet_mod._download_inmet
                import pandas as pd
                def _stub(years, uf, cache_dir, use_cache, parallel,
                          workers, verbose):
                    raise ValueError("STUB_BLOCKS_DOWNLOAD")
                inmet_mod._download_inmet = _stub
                try:
                    cs.sus_climate_inmet(
                        years=2023,
                        cache_dir=cache_dir,
                        parallel=False,
                        verbose=False,
                    )
                except ValueError as e:
                    if "STUB_BLOCKS_DOWNLOAD" not in str(e):
                        raise
                finally:
                    inmet_mod._download_inmet = _orig
            except Exception as e:
                print(f"  warning probe error: {e!r}")
        warning_msgs = [
            str(x.message) for x in w if issubclass(x.category, UserWarning)
        ]
        oom_warn = [m for m in warning_msgs if "no 'uf' supplied" in m]
        ok_warn = len(oom_warn) > 0
        print(_mark(f"UserWarning emitted (found {len(oom_warn)})", ok_warn))
        if ok_warn:
            print(f"  msg head     : {oom_warn[0][:100]}...")
        else:
            print(f"  warnings seen: {warning_msgs[:3]}")
        failures += not ok_warn

    print("\n" + "=" * 70)
    if failures == 0:
        print("ALL OK")
    else:
        print(f"FAILED: {failures} check(s)")
    print("=" * 70)
    return failures


if __name__ == "__main__":
    raise SystemExit(main())
