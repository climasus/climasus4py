"""Data import — download from DATASUS and cache as parquet.

Mirrors R: import.R + download-aria2c.R
Three reader backends: pysus (optional), pyreaddbc (optional), dbfread (bundled).
Falls back gracefully depending on what is installed.
"""

from __future__ import annotations

import shutil
import struct
import subprocess
import tempfile
import urllib.request
from pathlib import Path
from typing import Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from climasus.core.engine import read_parquets
from climasus.utils.data import resolve_uf

console = Console(stderr=True)

_DEFAULT_CACHE = Path("dados/cache")


# ---------------------------------------------------------------------------
# Type coercion for DATASUS data  (DBC/DBF → Parquet)
# ---------------------------------------------------------------------------

# Date columns in DATASUS (format DDMMYYYY as string)
_DATE_COLS = {
    "DTOBITO", "DTNASC", "DTCADINF", "DTCADMUN", "DTCONCASO", "DTINVESTIG",
    "DTRECEBIM", "DTRECORIG", "DTCONINV", "DTINTERNACAO", "DTSAIDA",
    "DTCADASTRO", "DTATESTADO", "DTREGCART", "DTCASAM", "DTULTMENST",
    "DTCONSULT", "DTDECLARAC",
}

# Columns that should be numeric (integer)
_NUMERIC_COLS = {
    "CONTADOR", "PESO", "QTDFILVIVO", "QTDFILMORT", "GESTACAO",
    "SEMAGESTAC", "OBITOGRAV", "GRAESSION", "CODMUNNATU", "CODMUNRES",
    "CODMUNOCOR", "CODESTAB", "LOCOCOR", "IDADEMAE", "ESCMAE", "CODOCUPMAE",
    "QTDGESTANT", "QTDPARTNOR", "QTDPARTCES", "IDADEPAI", "ESCPAI",
    "SERIESCPAI", "SERIESCMAE",
}


def _coerce_datasus_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce DATASUS columns to proper types before writing to Parquet.

    - Date columns (DDMMYYYY strings) → datetime64
    - Known numeric columns → numeric (coerced, invalid → NaN)
    - Strips whitespace from string columns
    """
    for col in df.columns:
        if col in _DATE_COLS:
            # DATASUS date format: DDMMYYYY (8 digits)
            df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")
        elif col in _NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            # Strip whitespace from string columns
            df[col] = df[col].astype(str).str.strip().replace({"":None,"nan":None})
    return df

# ---------------------------------------------------------------------------
# FTP URL builders  (mirrors download-aria2c.R)
# ---------------------------------------------------------------------------

_FTP_BASE = "ftp://ftp.datasus.gov.br/dissemin/publicos"


def _urls_sim_do(uf: str, year: int) -> list[str]:
    """SIM-DO: DO{UF}{YYYY}.dbc"""
    fname = f"DO{uf}{year}.dbc"
    urls = []
    if year >= 2022:
        urls.append(f"{_FTP_BASE}/SIM/PRELIM/DORES/{fname}")
    urls.append(f"{_FTP_BASE}/SIM/CID10/DORES/{fname}")
    return urls


def _urls_sim_special(prefix: str, year: int) -> list[str]:
    """SIM sub-systems (DOFET, DOEXT, DOINF, DOMAT) — no UF, 2-digit year."""
    yy = str(year)[2:]
    fname = f"{prefix}{yy}.dbc"
    urls = []
    if year >= 2022:
        urls.append(f"{_FTP_BASE}/SIM/PRELIM/DOFET/{fname}")
    urls.append(f"{_FTP_BASE}/SIM/CID10/DOFET/{fname}")
    return urls


def _urls_sinasc(uf: str, year: int) -> list[str]:
    """SINASC: DN{UF}{YYYY}.dbc"""
    fname = f"DN{uf}{year}.dbc"
    urls = []
    if year >= 2022:
        urls.append(f"{_FTP_BASE}/SINASC/PRELIM/DNRES/{fname}")
    urls.append(f"{_FTP_BASE}/SINASC/NOV/DNRES/{fname}")
    return urls


def _urls_sih(uf: str, year: int, month: int, stype: str = "RD") -> list[str]:
    """SIH: {TYPE}{UF}{YYMM}.dbc"""
    yy = str(year)[2:]
    fname = f"{stype}{uf}{yy}{month:02d}.dbc"
    base = (
        f"{_FTP_BASE}/SIHSUS/200801_/Dados/"
        if year >= 2008
        else f"{_FTP_BASE}/SIHSUS/199201_200712/Dados/"
    )
    return [f"{base}{fname}"]


_SYSTEM_URL_BUILDERS: dict[str, str] = {
    "SIM-DO": "sim_do",
    "SIM-DOFET": "sim_special",
    "SIM-DOEXT": "sim_special",
    "SIM-DOINF": "sim_special",
    "SIM-DOMAT": "sim_special",
    "SINASC": "sinasc",
    "SIH-RD": "sih",
    "SIH-RJ": "sih",
    "SIH-SP": "sih",
    "SIH-ER": "sih",
}

_SIM_SPECIAL_PREFIX = {
    "SIM-DOFET": "DOFET",
    "SIM-DOEXT": "DOEXT",
    "SIM-DOINF": "DOINF",
    "SIM-DOMAT": "DOMAT",
}

_SIH_TYPE = {"SIH-RD": "RD", "SIH-RJ": "RJ", "SIH-SP": "SP", "SIH-ER": "ER"}


def _build_urls(
    system: str, uf: str, year: int, month: int | None = None
) -> list[str]:
    """Build FTP URLs for a system/uf/year/month combination."""
    builder = _SYSTEM_URL_BUILDERS.get(system)
    if builder is None:
        raise ValueError(
            f"System '{system}' not supported for direct FTP download. "
            "Supported: " + ", ".join(_SYSTEM_URL_BUILDERS)
        )
    if builder == "sim_do":
        return _urls_sim_do(uf, year)
    if builder == "sim_special":
        return _urls_sim_special(_SIM_SPECIAL_PREFIX[system], year)
    if builder == "sinasc":
        return _urls_sinasc(uf, year)
    if builder == "sih":
        months = [month] if month else list(range(1, 13))
        urls: list[str] = []
        for m in months:
            urls.extend(_urls_sih(uf, year, m, _SIH_TYPE[system]))
        return urls
    return []


# ---------------------------------------------------------------------------
# .dbc file reader — chain of backends
# ---------------------------------------------------------------------------

def _read_dbc(path: Path) -> pd.DataFrame:
    """Read a .dbc file trying multiple backends.

    Order: pyreaddbc → pysus.utilities → dbfread (after blast decompression).
    """
    # Backend 1: pyreaddbc (fastest, C extension)
    try:
        from pyreaddbc import read_dbc  # type: ignore[import-untyped]
        return read_dbc(str(path))
    except ImportError:
        pass

    # Backend 2: pysus utilities
    try:
        from pysus.utilities.readdbc import read_dbc as pysus_read  # type: ignore[import-untyped]
        return pysus_read(str(path))
    except ImportError:
        pass

    # Backend 3: dbfread (pure Python .dbf reader)
    # .dbc = blast-compressed .dbf — try dbc2dbf CLI if available
    dbc2dbf = shutil.which("dbc2dbf")
    if dbc2dbf:
        try:
            import dbfread  # type: ignore[import-untyped]
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                dbf_path = tmp.name
            subprocess.run(
                [dbc2dbf, str(path), dbf_path],
                check=True, capture_output=True,
            )
            table = dbfread.DBF(dbf_path, encoding="latin1")
            return pd.DataFrame(iter(table))
        except Exception:
            pass

    raise ImportError(
        "Cannot read .dbc files. Install one of:\n"
        "  pip install pysus          # (needs C compiler)\n"
        "  pip install pyreaddbc      # (needs C compiler)\n"
        "  conda install -c conda-forge pysus  # (pre-built)\n"
        "Or use sus_import(path='file.parquet') / sus_import(data=df) instead."
    )


# ---------------------------------------------------------------------------
# FTP download
# ---------------------------------------------------------------------------

def _download_ftp(url: str, dest: Path, timeout: int = 120) -> bool:
    """Download a single file from FTP. Returns True on success."""
    try:
        urllib.request.urlretrieve(url, dest)
        return True
    except Exception:
        return False


def _download_and_cache(
    system: str,
    uf: str,
    year: int,
    month: int | None,
    target: Path,
    verbose: bool,
) -> Path | None:
    """Download a single .dbc from DATASUS FTP, convert to parquet, cache."""
    urls = _build_urls(system, uf, year, month)

    with tempfile.TemporaryDirectory() as tmpdir:
        dbc_path = Path(tmpdir) / "data.dbc"

        # Try each URL (preliminary → general)
        downloaded = False
        for url in urls:
            if _download_ftp(url, dbc_path, timeout=120):
                downloaded = True
                break

        if not downloaded:
            if verbose:
                console.print(f"[red]✗[/]  {uf}_{year}: all FTP URLs failed")
            return None

        try:
            df = _read_dbc(dbc_path)
        except ImportError as e:
            raise e
        except Exception as e:
            if verbose:
                console.print(f"[red]✗[/]  {uf}_{year}: failed to read .dbc: {e}")
            return None

    # Coerce types before writing to Parquet
    df = _coerce_datasus_types(df)

    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), target)

    if verbose:
        console.print(f"[green]✔[/]  {uf}_{year} ({len(df):,} rows)")

    return target


# ---------------------------------------------------------------------------
# PySUS download  (optional high-level backend)
# ---------------------------------------------------------------------------

_PYSUS_SYSTEM_MAP: dict[str, tuple[str, str]] = {
    "SIM-DO": ("pysus.online_data.SIM", "download"),
    "SINASC": ("pysus.online_data.SINASC", "download"),
    "SIH-RD": ("pysus.online_data.SIH", "download"),
    "SINAN-DENGUE": ("pysus.online_data.SINAN", "download"),
}


def _download_pysus(
    system: str, uf: str, year: int, month: int | None = None
) -> pd.DataFrame:
    """Download a single UF/year from DATASUS via PySUS (optional)."""
    if system not in _PYSUS_SYSTEM_MAP:
        raise ValueError(f"System '{system}' not supported via PySUS")

    module_path, func_name = _PYSUS_SYSTEM_MAP[system]
    import importlib

    mod = importlib.import_module(module_path)
    download_fn = getattr(mod, func_name)

    kwargs: dict = {"state": uf, "year": year}
    if month is not None and system.startswith("SIH"):
        kwargs["month"] = month

    return download_fn(**kwargs)


def _pysus_available() -> bool:
    """Check if PySUS is installed."""
    try:
        import pysus  # type: ignore[import-untyped]  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# aria2c parallel download  (optional accelerator)
# ---------------------------------------------------------------------------

def _aria2c_available() -> bool:
    return shutil.which("aria2c") is not None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_import(
    system: str,
    uf: str | list[str],
    year: int | list[int],
    month: int | list[int] | None = None,
    *,
    cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    timeout: int = 600,
    verbose: bool = True,
    path: str | Path | None = None,
    data: pd.DataFrame | None = None,
    backend: Literal["auto", "ftp", "pysus"] = "auto",
) -> "duckdb.DuckDBPyRelation":
    """Import SUS data and return a lazy DuckDB relation.

    Three modes:
      1. ``data=`` : wrap an existing DataFrame
      2. ``path=`` : read from a local file (parquet/csv)
      3. Default   : download from DATASUS, cache as parquet

    For mode 3, the *backend* controls how files are fetched:
      - ``"auto"``  : FTP direct download (no extra deps), falls back to PySUS
      - ``"ftp"``   : FTP download + .dbc reader (pyreaddbc / pysus / dbc2dbf)
      - ``"pysus"`` : Use PySUS library (requires ``pip install pysus``)
    """
    cache_dir = Path(cache_dir)
    ufs = resolve_uf(uf)
    years = [year] if isinstance(year, int) else list(year)
    months = [month] if isinstance(month, int) else (month or [None])

    parquet_paths: list[Path] = []

    if data is not None:
        # Mode 1: inline data
        data = _coerce_datasus_types(data.copy())
        target = cache_dir / system / f"inline_{'_'.join(ufs)}_{years[0]}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(data), target)
        parquet_paths.append(target)

    elif path is not None:
        # Mode 2: local file
        p = Path(path)
        if p.suffix == ".parquet":
            df = pq.read_table(p).to_pandas()
        elif p.suffix == ".csv":
            df = pd.read_csv(p)
        else:
            raise ValueError(f"Unsupported file format: {p.suffix}")
        target = cache_dir / system / f"file_{'_'.join(ufs)}_{years[0]}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df), target)
        parquet_paths.append(target)

    else:
        # Mode 3: download from DATASUS
        needed: list[dict] = []
        for one_uf in ufs:
            for one_year in years:
                for one_month in months:
                    month_str = str(one_month) if one_month else "all"
                    target = (
                        cache_dir / system / f"{one_uf}_{one_year}_{month_str}.parquet"
                    )
                    if cache and target.is_file():
                        parquet_paths.append(target)
                    else:
                        needed.append({
                            "uf": one_uf,
                            "year": one_year,
                            "month": one_month,
                            "target": target,
                        })

        if needed:
            use_pysus = (
                backend == "pysus"
                or (backend == "auto" and _pysus_available() and system in _PYSUS_SYSTEM_MAP)
            )

            engine_label = "PySUS" if use_pysus else "FTP"
            if verbose:
                console.print(
                    f"[cyan]ℹ[/] Downloading {len(needed)} file(s) via {engine_label}..."
                )

            for item in needed:
                result: Path | None = None

                if use_pysus:
                    try:
                        df = _download_pysus(
                            system, item["uf"], item["year"], item["month"]
                        )
                        df = _coerce_datasus_types(df)
                        item["target"].parent.mkdir(parents=True, exist_ok=True)
                        pq.write_table(pa.Table.from_pandas(df), item["target"])
                        result = item["target"]
                        if verbose:
                            console.print(
                                f"[green]✔[/]  {item['uf']}_{item['year']} ({len(df):,} rows)"
                            )
                    except Exception as e:
                        if verbose:
                            console.print(
                                f"[red]✗[/]  {item['uf']}_{item['year']}: {e}"
                            )
                else:
                    result = _download_and_cache(
                        system,
                        item["uf"],
                        item["year"],
                        item["month"],
                        item["target"],
                        verbose,
                    )

                if result:
                    parquet_paths.append(result)

    if not parquet_paths:
        raise RuntimeError("No data imported — check system/uf/year parameters.")

    return read_parquets(parquet_paths)
