"""Data import — download from DATASUS and cache as parquet.

Mirrors R: import.R + download-aria2c.R
Preferred reader: climasus_readdbc_py (pure Python, no C compiler).
Fallback chain: climasus_readdbc_py → climasus_readdbc → pyreaddbc → pysus → dbc2dbf CLI.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

from climasus4py.core.engine import read_parquets
from climasus4py.utils.data import (
    load_datasus_columns_spec,
    load_json,
    load_uf_codes,
    resolve_uf,
)

console = Console(stderr=True)

_DEFAULT_CACHE = Path("dados/cache")
_DATASUS_SOURCES_PATH = "metadata/datasus_sources.json"


# ---------------------------------------------------------------------------
# Type coercion for DATASUS data  (DBC/DBF → Parquet)
# ---------------------------------------------------------------------------

def _datasus_date_cols() -> frozenset[str]:
    return frozenset(load_datasus_columns_spec()["all_date_columns"])


def _datasus_numeric_cols() -> frozenset[str]:
    return frozenset(load_datasus_columns_spec()["all_numeric_columns"])


def _coerce_datasus_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce DATASUS columns to proper types before writing to Parquet.

    - Date columns (DDMMYYYY strings) → datetime64
    - Known numeric columns → numeric (coerced, invalid → NaN)
    - Strips whitespace from string columns
    """
    date_cols = _datasus_date_cols()
    numeric_cols = _datasus_numeric_cols()
    for col in df.columns:
        if col in date_cols:
            # DATASUS date format: DDMMYYYY (8 digits)
            df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")
        elif col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            # Strip whitespace from string columns
            df[col] = df[col].astype(str).str.strip().replace({"":None,"nan":None})
    return df

# ---------------------------------------------------------------------------
# DATASUS source catalog resolution
# ---------------------------------------------------------------------------

def _datasus_sources() -> dict:
    return load_json(_DATASUS_SOURCES_PATH)


def _system_source(system: str) -> dict:
    systems = _datasus_sources()["systems"]
    try:
        return systems[system]
    except KeyError as exc:
        raise ValueError(
            f"System '{system}' not supported for direct FTP download. "
            "Supported: " + ", ".join(sorted(systems))
        ) from exc


def _template_applies(template: dict, year: int) -> bool:
    valid_from = template.get("valid_from_year")
    valid_until = template.get("valid_until_year")
    if valid_from is not None and year < int(valid_from):
        return False
    return not (valid_until is not None and year > int(valid_until))


def _template_context(system_meta: dict, uf: str, year: int, month: int | None) -> dict[str, str]:
    return {
        "uf": uf.upper(),
        "yyyy": str(year),
        "yy": f"{year % 100:02d}",
        "month": f"{month:02d}" if month is not None else "",
        "disease_code": str(system_meta.get("disease_code", "")),
    }


def _build_urls(system: str, uf: str, year: int, month: int | None = None) -> list[str]:
    """Build FTP URLs from the climasus-data DATASUS source catalog."""
    catalog = _datasus_sources()
    system_meta = _system_source(system)
    source = catalog["sources"][system_meta["source"]]
    base_url = source["base_url"].rstrip("/")
    context = _template_context(system_meta, uf, year, month)

    urls = []
    for template in system_meta["url_templates"]:
        if _template_applies(template, year):
            path = template["path_template"].format(**context).lstrip("/")
            urls.append(f"{base_url}/{path}")

    if not urls:
        raise ValueError(f"No DATASUS FTP URL template applies to {system} for {year}.")
    return urls


def _geographic_scope(system: str) -> str:
    return str(_system_source(system).get("geographic_scope", "state"))


def _cache_partition_id(system: str, uf: str) -> str:
    if _geographic_scope(system) == "national":
        return "BR"
    return uf


def _state_filter_expression(system: str, ufs: list[str]) -> str | None:
    system_meta = _system_source(system)
    filter_meta = system_meta.get("partition_filter")
    if not filter_meta:
        return None

    uf_codes = load_uf_codes()
    requested = {uf.upper() for uf in ufs}
    if requested == set(uf_codes):
        return None

    codes = [int(uf_codes[uf]["code"]) for uf in sorted(requested)]
    col = filter_meta["state_column"]
    values = ", ".join(str(code) for code in codes)
    return f'TRY_CAST("{col}" AS INTEGER) IN ({values})'


# ---------------------------------------------------------------------------
# .dbc file reader — chain of backends
# ---------------------------------------------------------------------------

def _read_dbc(path: Path) -> pd.DataFrame:
    """Read a .dbc file trying multiple backends.

    Order: climasus_readdbc_py (pure Python) → climasus_readdbc (legacy)
    → pyreaddbc (C) → pysus → dbc2dbf CLI.
    """
    # Backend 1: climasus_readdbc_py (pure Python, no C compiler needed)
    try:
        import climasus_readdbc_py as readdbc
        return readdbc.read_dbc(path)
    except ImportError:
        pass
    except Exception:
        pass

    # Backend 1b: legacy import path kept for already-published versions.
    try:
        import climasus_readdbc as readdbc
        return readdbc.read_dbc(path)
    except ImportError:
        pass
    except Exception:
        pass

    # Backend 2: pyreaddbc (C extension, fastest)
    try:
        from pyreaddbc import read_dbc  # type: ignore[import-untyped]
        return read_dbc(str(path))
    except ImportError:
        pass

    # Backend 3: pysus utilities
    try:
        from pysus.utilities.readdbc import read_dbc as pysus_read  # type: ignore[import-untyped]
        return pysus_read(str(path))
    except ImportError:
        pass

    # Backend 4: dbc2dbf CLI + dbfread
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
        "Cannot read .dbc files. Install climasus_readdbc_py:\n"
        "  pip install climasus_readdbc_py\n"
        "Or alternatively:\n"
        "  pip install pyreaddbc  # (needs C compiler)\n"
        "  pip install pysus     # (needs C compiler)\n"
        "Or use sus_import(path='file.parquet') / sus_import(data=df) instead."
    )


# ---------------------------------------------------------------------------
# FTP download
# ---------------------------------------------------------------------------

def _download_ftp(url: str, dest: Path, timeout: int = 120) -> bool:
    """Download a single file from FTP. Returns True on success."""
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(url, timeout=timeout) as response:
            dest.write_bytes(response.read())
        return True
    except Exception:
        if dest.exists():
            dest.unlink()
        return False


def _raw_cache_path(url: str, raw_cache_dir: Path) -> Path:
    parsed = urlparse(url)
    parts = [parsed.netloc, *[part for part in parsed.path.split("/") if part]]
    return raw_cache_dir.joinpath(*parts)


def _file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _append_raw_manifest(raw_cache_dir: Path, url: str, path: Path) -> None:
    manifest = raw_cache_dir / "_manifest.jsonl"
    try:
        relative_path = str(path.relative_to(raw_cache_dir))
    except ValueError:
        relative_path = str(path)
    record = {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "path": relative_path,
        "size_bytes": path.stat().st_size,
        "md5": _file_md5(path),
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _resolve_dbc_path(
    urls: list[str],
    tmp_dir: Path,
    timeout: int,
    store_raw: bool,
    raw_cache_dir: Path | None,
) -> Path | None:
    for url in urls:
        if store_raw:
            if raw_cache_dir is None:
                raise ValueError("raw_cache_dir must be provided when store_raw=True")
            dbc_path = _raw_cache_path(url, raw_cache_dir)
            if dbc_path.is_file() and dbc_path.stat().st_size > 0:
                return dbc_path
        else:
            dbc_path = tmp_dir / "data.dbc"

        if _download_ftp(url, dbc_path, timeout=timeout):
            if store_raw and raw_cache_dir is not None:
                _append_raw_manifest(raw_cache_dir, url, dbc_path)
            return dbc_path

    return None


def _download_and_cache(
    system: str,
    uf: str,
    year: int,
    month: int | None,
    target: Path,
    verbose: bool,
    timeout: int,
    store_raw: bool,
    raw_cache_dir: Path | None,
) -> Path | None:
    """Download a single .dbc from DATASUS FTP, convert to parquet, cache."""
    urls = _build_urls(system, uf, year, month)

    with tempfile.TemporaryDirectory() as tmpdir:
        dbc_path = _resolve_dbc_path(
            urls=urls,
            tmp_dir=Path(tmpdir),
            timeout=timeout,
            store_raw=store_raw,
            raw_cache_dir=raw_cache_dir,
        )

        if dbc_path is None:
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
    """Verifica se o binário aria2c está disponível no PATH do sistema."""
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
    store_raw: bool = False,
    raw_cache_dir: str | Path | None = None,
) -> duckdb.DuckDBPyRelation:
    """Import SUS data and return a lazy DuckDB relation.

    Supports three input modes:

    1. **``data=``** — wrap an existing ``pandas.DataFrame``.
    2. **``path=``** — read from a local ``.parquet`` or ``.csv`` file.
    3. **Default** — download from the DATASUS FTP, convert ``.dbc`` to
       Parquet and cache locally; subsequent calls read from cache.

    When downloading (mode 3), the *backend* controls which client is
    used:

    - ``"auto"``  — FTP direct download (no extra deps).
    - ``"ftp"``   — FTP + ``.dbc`` reader chain:
      ``climasus_readdbc_py`` → ``climasus_readdbc`` → ``pyreaddbc`` →
      ``pysus`` → ``dbc2dbf`` CLI.
    - ``"pysus"`` — PySUS high-level API (requires
      ``pip install pysus``; needs C compiler on Windows).

    Args:
        system: SUS system identifier, e.g. ``"SIM-DO"``, ``"SINASC"``,
            or ``"SIH-RD"``.
        uf: State abbreviation(s), e.g. ``"SP"`` or ``["SP", "RJ"]``.
            Use ``"all"`` for all states or a region name such as
            ``"Sudeste"``.
        year: Year(s) to import, e.g. ``2022`` or
            ``[2020, 2021, 2022]``.
        month: Month(s) to import (SIH only). ``None`` downloads all 12
            months.
        cache: If ``True``, skip download when a cached Parquet exists.
        cache_dir: Root directory for the Parquet cache.
        timeout: Download timeout in seconds.
        verbose: Print progress messages via Rich.
        path: Local file path to use instead of downloading.
        data: Existing ``DataFrame`` to wrap instead of downloading.
        backend: Download backend — ``"auto"``, ``"ftp"``, or
            ``"pysus"``.
        store_raw: If ``True``, persist downloaded ``.dbc`` files in
            *raw_cache_dir* before conversion.
        raw_cache_dir: Directory for raw ``.dbc`` files. Defaults to
            ``cache_dir / "_raw"`` when *store_raw* is ``True``.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` over the imported data.

    Raises:
        RuntimeError: If no data could be imported (download failed or
            no cache hit).
        ValueError: If an unsupported file format is supplied via
            *path*.
        ImportError: If ``.dbc`` reading is attempted but no backend is
            available.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_import("SIM-DO", "SP", 2022)
        >>> rel.count()
        334303
        >>> cs.sus_import("SIM-DO", "SP", 2022,
        ...               path="dados/cache/SP_2022.parquet")
    """
    cache_dir = Path(cache_dir)
    raw_cache_path = Path(raw_cache_dir) if raw_cache_dir is not None else cache_dir / "_raw"
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
        partition_ufs = ["BR"] if _geographic_scope(system) == "national" else ufs
        for one_uf in partition_ufs:
            for one_year in years:
                for one_month in months:
                    month_str = f"{one_month:02d}" if one_month else "all"
                    partition_id = _cache_partition_id(system, one_uf)
                    target = cache_dir / system / f"{partition_id}_{one_year}_{month_str}.parquet"
                    if cache and target.is_file():
                        parquet_paths.append(target)
                    else:
                        needed.append(
                            {
                                "uf": one_uf,
                                "year": one_year,
                                "month": one_month,
                                "target": target,
                            }
                        )

        if needed:
            use_pysus = backend == "pysus"

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
                        timeout,
                        store_raw,
                        raw_cache_path if store_raw else None,
                    )

                if result:
                    parquet_paths.append(result)

    if not parquet_paths:
        raise RuntimeError("No data imported — check system/uf/year parameters.")

    rel = read_parquets(parquet_paths)
    filter_expr = _state_filter_expression(system, ufs)
    if filter_expr:
        rel = rel.filter(filter_expr)
    return rel
