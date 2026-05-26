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

import gc
import shutil
import subprocess
import tempfile
import time
import uuid
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd
import pyarrow as pa
from rich.console import Console

from ._sql import quote_ident, register_relation, sql_string
from .engine import get_connection
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
    parallel: bool = False,
    workers: int = 4,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import and process INMET meteorological data.

    Downloads Brazilian meteorological data from INMET, parses hourly CSVs
    through DuckDB SQL, writes Parquet/Zstd cache via DuckDB COPY, and returns
    the final result as a pandas DataFrame for API compatibility.

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
        Matched case-insensitively against the canonical ``wmo_code`` column.
    use_cache:
        If True (default), enables two-level caching:
        session cache (MD5 hash) + Parquet/Zstd on disk.
        Clear with: shutil.rmtree(cache_dir, ignore_errors=True)
    cache_dir:
        Directory path for disk cache. Created automatically.
        Default: ~/.climasus4py_cache/climate
    parallel:
        If True, enables two levels of parallelism: between years
        (ThreadPoolExecutor) and within year (CSV files). **Default
        ``False``** because each year of national INMET data expands to
        ~5-10 GB in pandas; running multiple years in parallel routinely
        triggered OOM crashes on machines with < 32 GB RAM. Set to
        ``True`` explicitly when you have RAM headroom and want speed.
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
    date, year, region, UF, station_name, wmo_code, latitude, longitude,
    altitude, founded_date, rainfall_mm, patm_mb, patm_max_mb, patm_min_mb,
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
    else:
        # Nationwide INMET hourly data expands to ~5-10 GB/year in pandas
        # (~600 stations × 24h × 365d × ~20 cols). Without a UF filter the
        # in-memory footprint is large enough to crash typical machines
        # (reported as OOM in real usage). Pass ``uf=...`` to narrow the
        # download and the in-memory dataset.
        warnings.warn(
            "sus_climate_inmet: no 'uf' supplied — the full national INMET "
            "dataset will be loaded (~5-10 GB/year in pandas, "
            f"{len(years_list)} year(s) requested). For most workflows "
            'prefer ``uf=["SP", "RJ", ...]`` or a single state. Set '
            "``parallel=True`` only on machines with ≥ 32 GB RAM.",
            UserWarning,
            stacklevel=2,
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

    climate_result = _download_inmet(
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
        if verbose:
            console.print(
                f"[cyan]INFO[/]  {msg['filter_code'].format(n=len(sc_list))}"
            )
        if isinstance(climate_result, duckdb.DuckDBPyRelation):
            if "wmo_code" not in climate_result.columns:
                raise ValueError(
                    "Cannot filter by 'station_code': column 'wmo_code' not found "
                    "in the downloaded data."
                )
            codes_sql = ", ".join(sql_string(code) for code in sc_list)
            climate_result = climate_result.filter(
                f"{quote_ident('wmo_code')} IN ({codes_sql})"
            )

    climate_result_is_relation = isinstance(climate_result, duckdb.DuckDBPyRelation)
    if climate_result_is_relation:
        climate_data = _collect_inmet_relation(climate_result)
        del climate_result
        gc.collect()
    else:
        climate_data = climate_result

    if sc_list is not None and not climate_result_is_relation:
        code_col = (
            "wmo_code"
            if "wmo_code" in climate_data.columns
            else "station_code"
            if "station_code" in climate_data.columns
            else None
        )
        if code_col is None:
            raise ValueError(
                "Cannot filter by 'station_code': column not found in the downloaded data."
            )
        climate_data = climate_data[
            climate_data[code_col].str.upper().isin(sc_list)
        ]

    if sc_list is not None and climate_data.empty:
        raise ValueError(msg["no_rows_code"].format(codes=", ".join(sc_list)))

    for col in ("region", "UF", "station_name", "wmo_code"):
        if col in climate_data.columns:
            climate_data[col] = climate_data[col].astype("category")

    # --- metadata ------------------------------------------------------------
    station_id_col = (
        "wmo_code"
        if "wmo_code" in climate_data.columns
        else "station_code"
        if "station_code" in climate_data.columns
        else "station_name"
        if "station_name" in climate_data.columns
        else None
    )
    n_stations: int | None = (
        climate_data[station_id_col].nunique()
        if station_id_col is not None
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
            import requests  # type: ignore[import-untyped]
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
# Internal: read year cache with optional UF push-down filter
# ---------------------------------------------------------------------------

def _year_cache_covers(year_cache_path: Path, uf: list[str] | None) -> bool:
    """Return True if the cached year directory satisfies the UF request.

    Two layouts are accepted:
      * **National (legacy):** ``year=<YYYY>/data.parquet`` — single file
        containing all UFs. Always satisfies any UF request.
      * **Hive per-UF (new):** ``year=<YYYY>/UF=<XX>/data.parquet`` — one
        sub-directory per UF. Satisfies the request only if all requested
        UFs have a corresponding sub-directory.

    When ``uf`` is ``None`` (national request), only the national layout
    counts as a hit; a partial Hive cache forces re-download to avoid
    silent gaps.
    """
    if not year_cache_path.exists():
        return False
    has_national = (year_cache_path / "data.parquet").is_file()
    uf_dirs = {
        p.name.split("=", 1)[1]
        for p in year_cache_path.iterdir()
        if p.is_dir() and p.name.startswith("UF=")
    }
    if uf is None:
        return has_national
    if has_national:
        return True
    return all(u in uf_dirs for u in uf)


def _read_year_cache_filtered(
    year_cache_path: Path,
    *,
    uf: list[str] | None,
) -> duckdb.DuckDBPyRelation:
    """Read a year's Parquet cache relation with optional UF push-down.

    Previously this used ``pq.read_table(...).to_pandas()`` which loaded
    the full national dataset (~5-10 GB) into RAM before the UF filter
    was applied — frequent cause of OOM. DuckDB applies the predicate
    while reading so only the requested UFs reach memory.
    """
    # Glob both ``year=YYYY/data.parquet`` (current writer) and any nested
    # parquet files DuckDB chooses to expose under the directory.
    glob_path = sql_string(
        str(year_cache_path / "**" / "*.parquet").replace("\\", "/")
    )
    conn = get_connection()
    if uf:
        uf_vals = ", ".join(sql_string(u) for u in uf)
        sql = (
            f"SELECT * FROM read_parquet({glob_path}, union_by_name=true) "
            f'WHERE "UF" IN ({uf_vals})'
        )
    else:
        sql = f"SELECT * FROM read_parquet({glob_path}, union_by_name=true)"
    return conn.sql(sql)


def _relation_is_empty(rel: duckdb.DuckDBPyRelation) -> bool:
    """Return True if a relation has no rows without materialising it."""
    return rel.limit(1).fetchone() is None


def _copy_relation_to_parquet(
    rel: duckdb.DuckDBPyRelation,
    dest: Path,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Write a DuckDB relation to Parquet using COPY."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest_sql = sql_string(str(dest).replace("\\", "/"))

    if hasattr(rel, "sql_query"):
        conn.execute(
            f"COPY ({rel.sql_query()}) TO {dest_sql} "
            "(FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)"
        )
        return

    view_name = f"_inmet_copy_{uuid.uuid4().hex}"
    register_relation(conn, rel, view_name)
    try:
        conn.execute(
            f"COPY (SELECT * FROM {quote_ident(view_name)}) TO {dest_sql} "
            "(FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)"
        )
    finally:
        conn.unregister(view_name)


def _materialize_relation_temp(
    rel: duckdb.DuckDBPyRelation,
    conn: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Materialise a relation into a UUID-named DuckDB temp table."""
    table_name = f"_inmet_year_{uuid.uuid4().hex}"
    conn.execute(
        f"CREATE TEMP TABLE {quote_ident(table_name)} AS "
        f"SELECT * FROM ({rel.sql_query()})"
    )
    return conn.sql(f"SELECT * FROM {quote_ident(table_name)}")


def _collect_inmet_relation(rel: duckdb.DuckDBPyRelation) -> pd.DataFrame:
    """Collect an INMET relation to pandas with compact metadata columns."""
    result = rel.arrow()
    table = result.read_all() if hasattr(result, "read_all") else result
    df = table.to_pandas(
        categories=["region", "UF", "station_name", "wmo_code"],
        split_blocks=True,
        self_destruct=True,
    )
    pa.default_memory_pool().release_unused()
    return df


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
) -> duckdb.DuckDBPyRelation | None:
    """Download, unzip, parse and cache INMET data for a single year."""
    import re

    dataset_dir = cache_dir / "inmet_parquet"
    zip_file = cache_dir / f"inmet_{year}.zip"
    year_cache_path = dataset_dir / f"year={year}"
    conn = get_connection()
    conn.execute("SET memory_limit='192MB'")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET threads=1")

    # 1. Parquet disk cache ---------------------------------------------------
    cache_hit = (
        use_cache
        and year_cache_path.exists()
        and _year_cache_covers(year_cache_path, uf)
    )
    if cache_hit:
        try:
            cached = _read_year_cache_filtered(year_cache_path, uf=uf)
            if not _relation_is_empty(cached):
                if verbose:
                    console.print(
                        f"[cyan]INFO[/]  Year {year}: Loading from Parquet cache"
                        + (f" (UF filter: {', '.join(uf)})" if uf else "")
                    )
                return cached
        except Exception as e:
            if verbose:
                console.print(
                    f"[yellow]WARN[/]  Year {year}: Cache read failed ({e}). Re-downloading."
                )
            shutil.rmtree(year_cache_path, ignore_errors=True)

    # 2. Download ZIP ---------------------------------------------------------
    if zip_file.exists() and zip_file.stat().st_size == 0:
        zip_file.unlink()
        if verbose:
            console.print(
                f"[yellow]WARN[/]  Year {year}: Removed empty ZIP from previous attempt."
            )

    if not zip_file.exists():
        url = _INMET_BASE_URL.format(year=year)
        if verbose:
            console.print(f"[cyan]INFO[/]  Year {year}: Downloading from {url}")
        ok, reason = _download_robust(url, zip_file, max_retries=3, verbose=verbose)
        if not ok:
            if reason and "HTTP 40" in reason:
                console.print(
                    f"[yellow]WARN[/]  Year {year}: {reason}. "
                    "Check https://portal.inmet.gov.br/dadoshistoricos"
                )
            else:
                console.print(
                    f"[yellow]WARN[/]  Year {year}: Download failed ({reason}). Skipping."
                )
            zip_file.unlink(missing_ok=True)
            return None
    else:
        if verbose:
            console.print(f"[cyan]INFO[/]  Year {year}: Using cached ZIP")

    # 3. Unzip ----------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix=f"inmet_{year}_") as tmpdir:
        tmp_path = Path(tmpdir)

        try:
            with zipfile.ZipFile(zip_file) as zf:
                zf.extractall(tmp_path)
        except Exception as e:
            console.print(
                f"[yellow]WARN[/]  Year {year}: Failed to unzip ({e}). Removing corrupt ZIP."
            )
            zip_file.unlink(missing_ok=True)
            return None

        csv_files = sorted(tmp_path.rglob("*.[Cc][Ss][Vv]"))
        if not csv_files:
            console.print(
                f"[yellow]WARN[/]  Year {year}: No CSV files found inside ZIP."
            )
            return None

        files_all = csv_files

        # 4. Filename-based UF pre-filter ------------------------------------
        if uf:
            pattern = "|".join(uf)
            files_for_uf = [
                f for f in files_all
                if re.search(pattern, f.name, re.IGNORECASE)
            ]
            if not files_for_uf:
                if verbose:
                    console.print(
                        f"[yellow]WARN[/]  Year {year}: No CSV matched "
                        f"UF filter ({', '.join(uf)})."
                    )
                return None
            files_to_parse = files_for_uf
        else:
            files_to_parse = files_all

        if use_cache and uf:
            try:
                year_cache_path.mkdir(parents=True, exist_ok=True)
                cached_ufs: set[str] = set()
                for idx, csv_path in enumerate(files_to_parse):
                    rel = parse_inmet_csv(csv_path)
                    if rel is None:
                        continue
                    for u in uf:
                        subset = rel.filter(f"{quote_ident('UF')} = {sql_string(u)}")
                        if _relation_is_empty(subset):
                            continue
                        _copy_relation_to_parquet(
                            subset,
                            year_cache_path / f"UF={u}" / f"part-{idx:04d}.parquet",
                            conn,
                        )
                        cached_ufs.add(u)

                if cached_ufs:
                    if verbose:
                        console.print(
                            f"[green]OK[/]  Year {year}: Cached UF partitions "
                            f"({', '.join(sorted(cached_ufs))})."
                        )
                    return _read_year_cache_filtered(year_cache_path, uf=uf)
            except Exception as e:
                if verbose:
                    console.print(
                        f"[yellow]WARN[/]  Year {year}: Failed to stream cache ({e})."
                    )
                shutil.rmtree(year_cache_path, ignore_errors=True)

        # 5. Parse CSV files -------------------------------------------------
        if parallel and len(files_to_parse) > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                rels = list(pool.map(parse_inmet_csv, files_to_parse))
        else:
            rels = [parse_inmet_csv(f) for f in files_to_parse]

        parsed = [rel for rel in rels if rel is not None]
        if not parsed:
            if verbose:
                console.print(
                    f"[yellow]WARN[/]  Year {year}: Parsing produced 0 rows."
                )
            return None

        unioned = parsed[0]
        for rel in parsed[1:]:
            unioned = unioned.union(rel)

        if uf:
            uf_vals = ", ".join(sql_string(u) for u in uf)
            unioned = unioned.filter(f"{quote_ident('UF')} IN ({uf_vals})")

        if _relation_is_empty(unioned):
            if verbose:
                console.print(
                    f"[yellow]WARN[/]  Year {year}: Parsing produced 0 rows."
                )
            return None

        # 6. Write Parquet cache ---------------------------------------------
        cache_written = False
        if use_cache:
            try:
                year_cache_path.mkdir(parents=True, exist_ok=True)
                if uf:
                    cached_ufs: list[str] = []
                    for u in uf:
                        subset = unioned.filter(f"{quote_ident('UF')} = {sql_string(u)}")
                        if _relation_is_empty(subset):
                            continue
                        _copy_relation_to_parquet(
                            subset,
                            year_cache_path / f"UF={u}" / "data.parquet",
                            conn,
                        )
                        cached_ufs.append(u)
                    if verbose and cached_ufs:
                        console.print(
                            f"[green]OK[/]  Year {year}: Cached UF partitions "
                            f"({', '.join(cached_ufs)})."
                        )
                    cache_written = bool(cached_ufs)
                else:
                    _copy_relation_to_parquet(
                        unioned,
                        year_cache_path / "data.parquet",
                        conn,
                    )
                    if verbose:
                        console.print(
                            f"[green]OK[/]  Year {year}: Full national "
                            "dataset cached."
                        )
                    cache_written = True
            except Exception as e:
                if verbose:
                    console.print(
                        f"[yellow]WARN[/]  Year {year}: Failed to write cache ({e})."
                    )

        if use_cache and cache_written:
            return _read_year_cache_filtered(year_cache_path, uf=uf)

        return _materialize_relation_temp(unioned, conn)


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
) -> duckdb.DuckDBPyRelation:
    """Download and cache INMET data for one or more years."""
    cache_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        console.rule("[bold]INMET Data Download[/]")
        console.print(f"[cyan]INFO[/]  Years: {', '.join(str(y) for y in years)}")
        if uf:
            console.print(f"[cyan]INFO[/]  States: {', '.join(uf)}")
        console.print(
            f"[cyan]INFO[/]  Cache: {'ENABLED' if use_cache else 'DISABLED'}"
        )
        console.print(f"[cyan]INFO[/]  Cache dir: {cache_dir}")

    def _process(year: int) -> duckdb.DuckDBPyRelation | None:
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

    if parallel and len(years) > 1:
        with ThreadPoolExecutor(max_workers=min(workers, len(years))) as pool:
            results = list(pool.map(_process, years))
    else:
        results = []
        for y in years:
            results.append(_process(y))
            gc.collect()

    relations = [r for r in results if r is not None and not _relation_is_empty(r)]

    if not relations:
        raise ValueError(
            f"No data could be downloaded for year(s): {', '.join(str(y) for y in years)}.\n"
            "Check that the year(s) are published at "
            "https://portal.inmet.gov.br/dadoshistoricos\n"
            "Run with verbose=True for per-year failure details."
        )

    combined = relations[0]
    for rel in relations[1:]:
        combined = combined.union(rel)

    if "date" in combined.columns:
        combined = combined.order("date")

    if verbose:
        console.print("[green]OK[/]  Loaded INMET DuckDB relation.")

    return combined
