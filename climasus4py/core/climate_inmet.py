"""climate_inmet.py — INMET meteorological data import.

Mirrors R: sus_climate_inmet.R / download_inmet()

Downloads, parses, QCs and caches INMET hourly station data from the National
Institute of Meteorology (INMET).

Pipeline
--------
1. Download  — multi-method with automatic retry and back-off
2. Parsing   — INMET CSV with metadata extraction
3. Standardize — canonical column names
4. QC        — physical consistency checks
5. Cache     — two-level (memory + Parquet/Zstd on disk)
6. Parallel  — across years AND within year (CSV files)
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from ..utils.inmet_parser import parse_inmet_csv  # internal CSV parser

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_UFS: frozenset[str] = frozenset({
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
})

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "climate"
_INMET_BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_REFERER = "https://portal.inmet.gov.br/dadoshistoricos"

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "cache_config": "Usando cache em: {dir}",
        "import_start": "Importando dados INMET para {n_years} ano(s)...",
        "import_done": "Importação concluída: {n_rows:,} observações de {n_stations} estações",
        "filter_code": "Filtrando por {n} código(s) de estação...",
        "no_rows_code": "Nenhuma observação encontrada para os códigos de station_code: {codes}",
    },
    "en": {
        "cache_config": "Using cache directory: {dir}",
        "import_start": "Importing INMET data for {n_years} year(s)...",
        "import_done": "Import complete: {n_rows:,} observations from {n_stations} stations",
        "filter_code": "Filtering by {n} station code(s)...",
        "no_rows_code": "No observations found for the provided station_code value(s): {codes}",
    },
    "es": {
        "cache_config": "Usando cache en: {dir}",
        "import_start": "Importando datos INMET para {n_years} año(s)...",
        "import_done": "Importación completada: {n_rows:,} observaciones de {n_stations} estaciones",  # noqa: E501
        "filter_code": "Filtrando por {n} código(s) de estación...",
        "no_rows_code": "No se encontraron observaciones para los códigos de station_code: {codes}",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_climate_inmet(
    years: int | list[int] | range | None = None,
    uf: str | list[str] | None = None,
    station_code: str | list[str] | None = None,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    parallel: bool = True,
    workers: int = 4,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import and process INMET meteorological data.

    Downloads, imports, standardizes, and quality-controls Brazilian
    meteorological data from the National Institute of Meteorology (INMET).

    Parameters
    ----------
    years:
        Year(s) to import. Examples: 2020, [2020, 2021, 2022], range(2020, 2025).
        Must be between 2000 and the current year. If None, imports last 2 years.
    uf:
        Brazilian state code(s) (e.g. "AM", ["RJ", "MG"]). Case insensitive.
        If None (default), imports all 27 states.
    station_code:
        INMET station codes to filter (e.g. ["A101", "A122"]). Optional.
        Matched case-insensitively against the station_code column.
    use_cache:
        If True (default), enables two-level caching:
        session cache (MD5 hash) + Parquet/Zstd on disk.
        Clear with: shutil.rmtree(cache_dir, ignore_errors=True)
    cache_dir:
        Directory path for disk cache. Created automatically.
        Default: ~/.climasus4py_cache/climate
    parallel:
        If True (default), enables two levels of parallelism:
        between years (ThreadPoolExecutor) and within year (CSV files).
    workers:
        Number of parallel workers. Default: 4. Ignored if parallel=False.
    lang:
        Message language: "pt" (default), "en", or "es".
    verbose:
        If True (default), prints cache hits/misses, download progress,
        QC modifications, and final statistics.

    Returns
    -------
    pd.DataFrame
        DataFrame with standardized meteorological columns. Metadata accessible
        via df.attrs["sus_meta"].

    Standardized Columns
    --------------------
    station_code, station_name, region, UF, latitude, longitude, altitude,
    date (UTC), year, rainfall_mm, patm_mb, patm_max_mb, patm_min_mb,
    sr_kj_m2, tair_dry_bulb_c, tair_max_c, tair_min_c, dew_tmean_c,
    dew_tmax_c, dew_tmin_c, rh_mean_porc, rh_max_porc, rh_min_porc,
    ws_2_m_s, ws_gust_m_s, wd_degrees

    Examples
    --------
    >>> df = sus_climate_inmet(years=2023, uf="AM")
    >>> df = sus_climate_inmet(years=[2020, 2021], uf=["SP", "RJ"], parallel=True)
    >>> df = sus_climate_inmet(years=2023, uf="SP", station_code=["A701", "A711"])
    """
    current_year = datetime.now().year

    # --- years ---------------------------------------------------------------
    if years is None:
        years_list = [current_year - 1, current_year]
        if verbose:
            console.print(
                f"[cyan]ℹ[/]  years not supplied — defaulting to "
                f"{years_list[0]}-{years_list[-1]}."
            )
    elif isinstance(years, int):
        years_list = [years]
    else:
        years_list = list(years)

    years_list = [int(y) for y in years_list]
    invalid = [y for y in years_list if y < 2000 or y > current_year]
    if invalid:
        raise ValueError(
            f"Invalid values in 'years': {invalid}. "
            f"Years must be between 2000 and {current_year}."
        )

    # --- lang ----------------------------------------------------------------
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- uf ------------------------------------------------------------------
    uf_list: list[str] | None = None
    if uf is not None:
        uf_list = (
            [uf.upper().strip()]
            if isinstance(uf, str)
            else [u.upper().strip() for u in uf]
        )
        invalid_ufs = sorted(set(uf_list) - _VALID_UFS)
        if invalid_ufs:
            raise ValueError(
                f"Invalid values in 'uf': {invalid_ufs}. "
                f"Valid codes: {sorted(_VALID_UFS)}"
            )

    # --- station_code --------------------------------------------------------
    sc_list: list[str] | None = None
    if station_code is not None:
        sc_list = (
            [station_code.upper().strip()]
            if isinstance(station_code, str)
            else [s.upper().strip() for s in station_code]
        )

    # --- cache_dir -----------------------------------------------------------
    cache_path = Path(cache_dir).expanduser()
    if use_cache:
        cache_path.mkdir(parents=True, exist_ok=True)
        if verbose:
            console.print(f"[cyan]ℹ[/]  {msg['cache_config'].format(dir=cache_path)}")

    # --- import --------------------------------------------------------------
    if verbose:
        console.print(
            f"[cyan]ℹ[/]  {msg['import_start'].format(n_years=len(years_list))}"
        )

    climate_data = _download_inmet(
        years=years_list,
        uf=uf_list,
        cache_dir=cache_path,
        use_cache=use_cache,
        parallel=parallel,
        workers=workers,
        verbose=verbose,
    )

    # --- filter by station_code ----------------------------------------------
    if sc_list is not None:
        if "station_code" not in climate_data.columns:
            raise ValueError(
                "Cannot filter by 'station_code': column not found in the downloaded data."
            )
        if verbose:
            console.print(
                f"[cyan]ℹ[/]  {msg['filter_code'].format(n=len(sc_list))}"
            )
        climate_data = climate_data[
            climate_data["station_code"].str.upper().isin(sc_list)
        ]
        if climate_data.empty:
            raise ValueError(msg["no_rows_code"].format(codes=", ".join(sc_list)))

    # --- metadata ------------------------------------------------------------
    n_stations: int | None = (
        climate_data["station_code"].nunique()
        if "station_code" in climate_data.columns
        else None
    )
    temporal: dict = {}
    if "date" in climate_data.columns and not climate_data["date"].isna().all():
        temporal = {
            "start": climate_data["date"].min(),
            "end": climate_data["date"].max(),
        }

    climate_data.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "inmet",
        "source": "INMET",
        "years": years_list,
        "ufs": uf_list,
        "station_codes": sc_list,
        "cache_used": use_cache,
        "n_stations": n_stations,
        "n_observations": len(climate_data),
        "temporal_coverage": temporal,
        "timestamp": datetime.now().isoformat(),
        "history": [
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INMET data imported"
        ],
        "user": {},
    }

    if verbose:
        console.print(
            f"[green]✔[/]  {msg['import_done'].format(n_rows=len(climate_data), n_stations=n_stations)}"  # noqa: E501
        )

    return climate_data


# ---------------------------------------------------------------------------
# Internal: robust single-file download
# ---------------------------------------------------------------------------

def _download_robust(
    url: str,
    dest: Path,
    max_retries: int = 3,
    verbose: bool = False,
) -> tuple[bool, str | None]:
    """Download url → dest with multi-method retry and exponential back-off.

    Strategy (in order):
      1. requests  — preferred: full header control, streaming write
      2. urllib    — stdlib fallback (no extra deps)
      3. curl bin  — system curl with explicit --user-agent
      4. wget bin  — system wget with explicit --user-agent

    Returns
    -------
    (True, None)       on success.
    (False, reason)    on permanent HTTP 4xx or exhausted retries.
    """
    tmp = Path(str(dest) + ".tmp")
    last_reason = "unknown error"

    def _try_requests() -> bool | str:
        try:
            import requests
        except ImportError:
            return False
        try:
            headers = {
                "User-Agent": _UA,
                "Accept": "application/zip, application/octet-stream, */*",
                "Referer": _REFERER,
            }
            with requests.get(url, headers=headers, stream=True, timeout=3600) as r:
                sc = r.status_code
                if sc == 200:
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            f.write(chunk)
                    if tmp.exists() and tmp.stat().st_size > 0:
                        tmp.rename(dest)
                        return True
                if 400 <= sc < 500:
                    return f"HTTP {sc} from server (permanent error)"
            return False
        except Exception as e:
            if verbose:
                console.print(f"[yellow]⚠[/]  requests: {e}")
            return False

    def _try_urllib() -> bool | str:
        import urllib.request
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": _UA, "Referer": _REFERER}
            )
            with urllib.request.urlopen(req, timeout=3600) as resp:
                if resp.status == 200:
                    with open(tmp, "wb") as f:
                        f.write(resp.read())
                    if tmp.exists() and tmp.stat().st_size > 0:
                        tmp.rename(dest)
                        return True
            return False
        except Exception as e:
            if verbose:
                console.print(f"[yellow]⚠[/]  urllib: {e}")
            return False

    def _try_curl_bin() -> bool | str:
        curl_bin = shutil.which("curl")
        if not curl_bin:
            return False
        try:
            result = subprocess.run(
                [
                    curl_bin, "--silent", "--show-error", "--location",
                    "--max-time", "3600",
                    "--write-out", "%{http_code}",
                    "--user-agent", _UA,
                    "--header", f"Referer: {_REFERER}",
                    "--output", str(tmp), url,
                ],
                capture_output=True, text=True,
            )
            http_code = int(result.stdout.strip() or "0")
            if result.returncode == 0 and http_code == 200 and tmp.exists() and tmp.stat().st_size > 0:  # noqa: E501
                tmp.rename(dest)
                return True
            if 400 <= http_code < 500:
                return f"HTTP {http_code} from server (permanent error)"
            return False
        except Exception as e:
            if verbose:
                console.print(f"[yellow]⚠[/]  curl bin: {e}")
            return False
        finally:
            tmp.unlink(missing_ok=True)

    def _try_wget_bin() -> bool | str:
        wget_bin = shutil.which("wget")
        if not wget_bin:
            return False
        try:
            result = subprocess.run(
                [
                    wget_bin, "--server-response", "--timeout=3600", "--tries=1",
                    f"--user-agent={_UA}",
                    f"--header=Referer: {_REFERER}",
                    "--output-document", str(tmp), url,
                ],
                capture_output=True, text=True,
            )
            if result.returncode == 0 and tmp.exists() and tmp.stat().st_size > 0:
                tmp.rename(dest)
                return True
            # Parse HTTP status from stderr
            for line in (result.stderr or "").splitlines():
                if "HTTP/" in line:
                    for part in line.split():
                        if part.isdigit():
                            code = int(part)
                            if 400 <= code < 500:
                                return f"HTTP {code} from server (permanent error)"
            return False
        except Exception as e:
            if verbose:
                console.print(f"[yellow]⚠[/]  wget bin: {e}")
            return False
        finally:
            tmp.unlink(missing_ok=True)

    try:
        for attempt in range(1, max_retries + 1):
            if verbose and attempt > 1:
                console.print(
                    f"[cyan]ℹ[/]  Download attempt {attempt}/{max_retries}..."
                )

            for try_fn in (_try_requests, _try_urllib, _try_curl_bin, _try_wget_bin):
                result = try_fn()
                if result is True:
                    return True, None
                if isinstance(result, str):
                    # Permanent error — do not retry any method
                    return False, result

            if attempt < max_retries:
                wait = 2 ** attempt
                last_reason = (
                    f"all methods failed on attempt {attempt}/{max_retries}"
                )
                if verbose:
                    console.print(
                        f"[yellow]⚠[/]  All download methods failed "
                        f"(attempt {attempt}/{max_retries}). Retrying in {wait}s..."
                    )
                time.sleep(wait)
    finally:
        tmp.unlink(missing_ok=True)

    return False, last_reason


# ---------------------------------------------------------------------------
# Internal: process one year
# ---------------------------------------------------------------------------

def _process_year(
    year: int,
    uf: list[str] | None,
    cache_dir: Path,
    use_cache: bool,
    parallel: bool,
    workers: int,
    verbose: bool,
) -> pd.DataFrame:
    """Download, unzip, parse and cache INMET data for a single year.

    The Parquet cache always stores the full national dataset so that
    subsequent calls with a different ``uf`` can be served from cache
    without re-downloading the ZIP.
    """
    import re

    dataset_dir = cache_dir / "inmet_parquet"
    zip_file = cache_dir / f"inmet_{year}.zip"
    year_cache_path = dataset_dir / f"year={year}"

    # 1. Parquet disk cache ---------------------------------------------------
    if use_cache and year_cache_path.exists():
        try:
            cached = pq.read_table(str(year_cache_path)).to_pandas()
            if not cached.empty:
                if verbose:
                    console.print(
                        f"[cyan]ℹ[/]  Year {year}: Loading from Parquet cache"
                    )
                if uf and "UF" in cached.columns:
                    cached = cached[cached["UF"].isin(uf)]
                return cached
        except Exception as e:
            if verbose:
                console.print(
                    f"[yellow]⚠[/]  Year {year}: Cache read failed ({e}). Re-downloading."
                )
            shutil.rmtree(year_cache_path, ignore_errors=True)

    # 2. Download ZIP ---------------------------------------------------------
    if zip_file.exists() and zip_file.stat().st_size == 0:
        zip_file.unlink()
        if verbose:
            console.print(
                f"[yellow]⚠[/]  Year {year}: Removed empty ZIP from previous attempt."
            )

    if not zip_file.exists():
        url = _INMET_BASE_URL.format(year=year)
        if verbose:
            console.print(f"[cyan]ℹ[/]  Year {year}: Downloading from {url}")
        ok, reason = _download_robust(url, zip_file, max_retries=3, verbose=verbose)
        if not ok:
            if reason and "HTTP 40" in reason:
                console.print(
                    f"[yellow]⚠[/]  Year {year}: {reason}. "
                    "Check https://portal.inmet.gov.br/dadoshistoricos"
                )
            else:
                console.print(
                    f"[yellow]⚠[/]  Year {year}: Download failed ({reason}). Skipping."
                )
            zip_file.unlink(missing_ok=True)
            return pd.DataFrame()
    else:
        if verbose:
            console.print(f"[cyan]ℹ[/]  Year {year}: Using cached ZIP")

    # 3. Unzip ----------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix=f"inmet_{year}_") as tmpdir:
        tmp_path = Path(tmpdir)

        try:
            with zipfile.ZipFile(zip_file) as zf:
                zf.extractall(tmp_path)
        except Exception as e:
            console.print(
                f"[yellow]⚠[/]  Year {year}: Failed to unzip ({e}). Removing corrupt ZIP."
            )
            zip_file.unlink(missing_ok=True)
            return pd.DataFrame()

        csv_files = sorted(tmp_path.rglob("*.[Cc][Ss][Vv]"))
        if not csv_files:
            console.print(
                f"[yellow]⚠[/]  Year {year}: No CSV files found inside ZIP."
            )
            return pd.DataFrame()

        files_all = csv_files

        # 4. Filename-based UF pre-filter ------------------------------------
        # Keep TWO lists:
        #   files_all     → full national set (used for cache write)
        #   files_for_uf  → UF-filtered (used when cache already exists)
        if uf:
            pattern = "|".join(uf)
            files_for_uf = [
                f for f in files_all
                if re.search(pattern, f.name, re.IGNORECASE)
            ]
            if not files_for_uf:
                if verbose:
                    console.print(
                        f"[yellow]⚠[/]  Year {year}: No CSV matched "
                        f"UF filter ({', '.join(uf)})."
                    )
                return pd.DataFrame()
        else:
            files_for_uf = files_all

        cache_exists = use_cache and year_cache_path.exists()
        # Parse all national files when writing cache; only UF files otherwise.
        files_to_parse = files_all if not cache_exists else files_for_uf

        # 5. Parse CSV files -------------------------------------------------
        if parallel and len(files_to_parse) > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                frames = list(pool.map(parse_inmet_csv, files_to_parse))
        else:
            frames = [parse_inmet_csv(f) for f in files_to_parse]

        frames = [f for f in frames if f is not None and not f.empty]
        if not frames:
            if verbose:
                console.print(
                    f"[yellow]⚠[/]  Year {year}: Parsing produced 0 rows."
                )
            return pd.DataFrame()

        year_data = pd.concat(frames, ignore_index=True)
        year_data["year"] = year

        # 6. Write Parquet cache (full national dataset) ----------------------
        if not cache_exists and use_cache:
            year_cache_path.mkdir(parents=True, exist_ok=True)
            try:
                pq.write_table(
                    pa.Table.from_pandas(year_data),
                    str(year_cache_path / "data.parquet"),
                    compression="zstd",
                    compression_level=6,
                )
                if verbose:
                    console.print(
                        f"[green]✔[/]  Year {year}: Full national dataset cached."
                    )
            except Exception as e:
                if verbose:
                    console.print(
                        f"[yellow]⚠[/]  Year {year}: Failed to write cache ({e})."
                    )

        # 7. Post-cache UF column filter --------------------------------------
        if uf and "UF" in year_data.columns:
            year_data = year_data[year_data["UF"].isin(uf)]

    return year_data.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Internal: multi-year orchestrator
# ---------------------------------------------------------------------------

def _download_inmet(
    years: list[int],
    uf: list[str] | None,
    cache_dir: Path,
    use_cache: bool,
    parallel: bool,
    workers: int,
    verbose: bool,
) -> pd.DataFrame:
    """Download and cache INMET data for one or more years."""
    cache_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        console.rule("[bold]INMET Data Download[/]")
        console.print(f"[cyan]ℹ[/]  Years: {', '.join(str(y) for y in years)}")
        if uf:
            console.print(f"[cyan]ℹ[/]  States: {', '.join(uf)}")
        console.print(
            f"[cyan]ℹ[/]  Cache: {'ENABLED' if use_cache else 'DISABLED'}"
        )
        console.print(f"[cyan]ℹ[/]  Cache dir: {cache_dir}")

    def _process(year: int) -> pd.DataFrame:
        return _process_year(
            year=year,
            uf=uf,
            cache_dir=cache_dir,
            use_cache=use_cache,
            # within-year parallel only when processing a single year
            # (avoids nested ThreadPoolExecutor deadlocks)
            parallel=parallel and len(years) == 1,
            workers=workers,
            verbose=verbose,
        )

    # Between-year parallelism uses threads (I/O bound)
    if parallel and len(years) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, len(years))) as pool:
            results = list(pool.map(_process, years))
    else:
        results = [_process(y) for y in years]

    results = [r for r in results if r is not None and not r.empty]

    if not results:
        raise ValueError(
            f"No data could be downloaded for year(s): {', '.join(str(y) for y in years)}.\n"
            "Check that the year(s) are published at "
            "https://portal.inmet.gov.br/dadoshistoricos\n"
            "Run with verbose=True for per-year failure details."
        )

    combined = pd.concat(results, ignore_index=True)

    if "date" in combined.columns:
        combined = combined.sort_values("date").reset_index(drop=True)

    if verbose:
        console.print(f"[green]✔[/]  Loaded {len(combined):,} total rows.")

    return combined
