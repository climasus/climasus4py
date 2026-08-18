"""grid_chirps.py — CHIRPS precipitation data import for Brazilian municipalities.

Mirrors R: sus_grid_chirps.R

Downloads CHIRPS v2.0 precipitation GeoTIFF rasters from UCSB/CHC, crops
them to Brazil, and spatially aggregates to municipality polygons via
zonal statistics using ``exactextract`` (the literal Python binding for
the same isciences C++ engine that R's ``exactextractr`` wraps — not
``rasterstats``, whose default zonal-stats masking is only an
approximation of the fractional pixel-polygon overlap weighting both
``exactextractr`` and ``exactextract`` compute).

Not lazy — raster I/O and zonal statistics are fundamentally
geometry/row-oriented work with no natural DuckDB SQL expression, and
the R source itself never routes this through Arrow/DuckDB either (it
works directly with ``terra``/``exactextractr`` and returns a
materialised tibble). The Python port mirrors that: results are built
as a municipality x date ``pd.DataFrame`` with metadata attached via
``df.attrs["sus_meta"]``.
"""

from __future__ import annotations

import calendar
import hashlib
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
from rich.console import Console

from ..core.climate_inmet import _download_robust

if TYPE_CHECKING:
    import geopandas as gpd

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "chirps"

_VALID_RESOLUTIONS: tuple[str, ...] = ("monthly", "daily", "annual")
_VALID_AGG: tuple[str, ...] = ("mean", "sum", "median", "min", "max")

# ponytail: CHIRPS annual archive stops at 2024 as of the R source's own
# comment ("CHIRPS annual stops at 2024 currently") — a hardcoded ceiling,
# not derived from the live CHC index. Preserved verbatim rather than
# "fixed" into something dynamic; bump this constant (and the R source's
# twin) when CHC publishes a newer annual file.
_MAX_YEAR_ANNUAL = 2024

_CHIRPS_BASE = "https://data.chc.ucsb.edu/products/CHIRPS-2.0"

# R default: months = 1:12
_ALL_MONTHS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

# terra::ext(-75, -28, -35, 6) is (xmin, xmax, ymin, ymax); stored below as
# (minx, miny, maxx, maxy) to match rioxarray's clip_box() argument order.
_BRAZIL_BBOX: tuple[float, float, float, float] = (-75.0, -35.0, -28.0, 6.0)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados CHIRPS de Precipitação",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' deve ser numérico sem NA.",
        "invalid_years_range": (
            "'years' deve estar entre 1981 e {max_year}. Ano(s) inválido(s): {bad}."
        ),
        "default_years": "Usando anos {years} (padrão: últimos 2 anos completos).",
        "invalid_months": "'months' deve ser inteiro entre 1 e 12.",
        "need_geopandas": "O pacote geopandas é necessário para agregar por municípios.",
        "muni_not_geodataframe": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opções: {valid}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "no_data_params": "Nenhum dado disponível para os parâmetros fornecidos.",
        "download_start": "Baixando {n_files} arquivo(s) CHIRPS do CHC/UCSB...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "parquet_cache_hit": "Todos os arquivos no cache Parquet. Carregando...",
        "parquet_hit": "Cache Parquet: {filename}",
        "parquet_write_warn": "Não foi possível salvar cache Parquet: {filename}",
        "skip_missing": "Arquivo não disponível: {filename}",
        "extract_warn": "Não foi possível processar {filename}: {err}",
        "no_data": "Nenhum dado foi extraído com sucesso.",
        "agg_start": "Agregando para {n_mun} município(s)...",
        "agg_done": "Concluído: {n_rows} observações ({n_mun} municípios).",
        "done_paths": "{n} arquivo(s) GeoTIFF disponível(is) no cache.",
    },
    "en": {
        "title": "CHIRPS Precipitation Data",
        "invalid_resolution": "Invalid 'resolution': {bad}. Use: {valid}.",
        "invalid_years_type": "'years' must be numeric without NA.",
        "invalid_years_range": (
            "'years' must be between 1981 and {max_year}. Invalid year(s): {bad}."
        ),
        "default_years": "Using years {years} (default: last 2 complete years).",
        "invalid_months": "'months' must be integer between 1 and 12.",
        "need_geopandas": "The geopandas package is required to aggregate by municipality.",
        "muni_not_geodataframe": "'municipalities' must be a geopandas.GeoDataFrame.",
        "invalid_agg": "Invalid 'agg_fun': {bad}. Options: {valid}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "no_data_params": "No data available for the provided parameters.",
        "download_start": "Downloading {n_files} CHIRPS file(s) from CHC/UCSB...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "parquet_cache_hit": "All files found in Parquet cache. Loading...",
        "parquet_hit": "Parquet cache: {filename}",
        "parquet_write_warn": "Could not write Parquet cache: {filename}",
        "skip_missing": "File not available: {filename}",
        "extract_warn": "Could not process {filename}: {err}",
        "no_data": "No data was successfully extracted.",
        "agg_start": "Aggregating to {n_mun} municipality/ies...",
        "agg_done": "Complete: {n_rows} observations ({n_mun} municipalities).",
        "done_paths": "{n} GeoTIFF file(s) available in cache.",
    },
    "es": {
        "title": "Datos CHIRPS de Precipitación",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' debe ser numérico sin NA.",
        "invalid_years_range": (
            "'years' debe estar entre 1981 y {max_year}. Año(s) inválido(s): {bad}."
        ),
        "default_years": "Usando años {years} (por defecto: últimos 2 años completos).",
        "invalid_months": "'months' debe ser entero entre 1 y 12.",
        "need_geopandas": "El paquete geopandas es necesario para agregar por municipios.",
        "muni_not_geodataframe": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opciones: {valid}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "no_data_params": "No hay datos disponibles para los parámetros indicados.",
        "download_start": "Descargando {n_files} archivo(s) CHIRPS de CHC/UCSB...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "parquet_cache_hit": "Todos los archivos en caché Parquet. Cargando...",
        "parquet_hit": "Caché Parquet: {filename}",
        "parquet_write_warn": "No se pudo guardar caché Parquet: {filename}",
        "skip_missing": "Archivo no disponible: {filename}",
        "extract_warn": "No se pudo procesar {filename}: {err}",
        "no_data": "No se extrajo ningún dato correctamente.",
        "agg_start": "Agregando a {n_mun} municipio(s)...",
        "agg_done": "Completo: {n_rows} observaciones ({n_mun} municipios).",
        "done_paths": "{n} archivo(s) GeoTIFF disponible(s) en caché.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_chirps(
    resolution: Literal["monthly", "daily", "annual"] = "monthly",
    years: int | list[int] | None = None,
    months: list[int] | tuple[int, ...] = _ALL_MONTHS,
    municipalities: gpd.GeoDataFrame | None = None,
    agg_fun: Literal["mean", "sum", "median", "min", "max"] = "mean",
    crop_brazil: bool = True,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame | dict[str, str]:
    """Import CHIRPS rainfall data for Brazilian municipalities.

    Downloads CHIRPS v2.0 precipitation data, crops it to Brazil,
    spatially aggregates to municipalities, and returns a ``pd.DataFrame``
    compatible with ``sus_mod_dlnm`` / ``sus_climate_anomaly`` (via
    ``sus_grid_join``).

    CHIRPS (Climate Hazards Group InfraRed Precipitation with Station
    data) is a quasi-global, 0.05-degree (~5 km) daily rainfall dataset
    from UCSB CHC covering 1981 to present. It combines satellite
    imagery with station data and is considered the best freely
    available high-resolution rainfall product for Brazil — particularly
    for leptospirosis, diarrheal disease, and dengue analyses where
    precipitation is the key exposure. No authentication is required.

    Args:
        resolution: Temporal resolution of source files. ``"monthly"``
            (default) — one file per month (~5-20 MB each), returns
            mm/month. ``"daily"`` — one file per day (~3 MB each),
            returns mm/day; recommended only for short periods (a full
            year = 365 downloads). ``"annual"`` — one file per year,
            returns mm/year.
        years: Year(s) to download. Coverage: 1981 to present for
            daily/monthly; 1981-2024 for annual. ``None`` (default)
            uses the last two complete years.
        months: Months (1-12) to include for daily and monthly
            resolutions. Ignored for annual. Default: all 12 months.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from ``climasus-data`` municipality
            boundaries). When provided, rasters are aggregated and a
            ``pd.DataFrame`` is returned. If ``None``, a dict mapping
            filename -> cached file path is returned instead (no
            spatial aggregation).
        agg_fun: Spatial aggregation function for ``exactextract``.
            Default ``"mean"`` (area-weighted mean). For rainfall
            totals over a municipality use ``"sum"``.
        crop_brazil: Crop global rasters to Brazil's bounding box before
            aggregation. Reduces memory usage significantly. Default
            ``True``.
        use_cache: Reuse previously downloaded raster files and
            aggregated Parquet caches. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/chirps``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni`` (str), ``date`` (datetime), and
        ``rainfall_chirps_mm`` (float). Metadata in
        ``df.attrs["sus_meta"]`` (``stage="climate"``, ``type="chirps"``).
        If *municipalities* is ``None``: a dict mapping each cached
        GeoTIFF filename to its local path.

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            extracted for the given parameters.
        ImportError: If ``geopandas``, ``rioxarray``, or ``exactextract``
            are required but not installed.

    Units:
        Daily: mm/day. Monthly: mm/month (cumulative). Annual: mm/year
        (cumulative). CHIRPS missing/no-data values (< -999, matching
        the R source's threshold rather than an exact -9999 equality
        check) are converted to NaN.

    Data source:
        Funk, C. et al. (2015). The climate hazards infrared
        precipitation with stations — a new environmental record for
        monitoring extremes. Scientific Data, 2, 150066.
        https://doi.org/10.1038/sdata.2015.66
        Data: https://data.chc.ucsb.edu/products/CHIRPS-2.0/

    Examples::

        import climasus4py as cs

        # Monthly rainfall for a set of municipalities, 2024
        chirps = cs.sus_grid_chirps(
            resolution="monthly", years=2024, municipalities=mt_mun, lang="pt",
        )

        # Annual totals, paths only (no municipalities)
        paths = cs.sus_grid_chirps(resolution="annual", years=[2018, 2019])
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- resolution -------------------------------------------------------
    if resolution not in _VALID_RESOLUTIONS:
        raise ValueError(
            msg["invalid_resolution"].format(
                bad=resolution, valid=", ".join(_VALID_RESOLUTIONS)
            )
        )

    # --- years --------------------------------------------------------------
    current_year = datetime.now().year
    max_year = _MAX_YEAR_ANNUAL if resolution == "annual" else current_year

    if years is None:
        years_list = [current_year - 2, current_year - 1]
        if verbose:
            console.print(
                "[cyan]INFO[/]  "
                + msg["default_years"].format(
                    years=f"{years_list[0]}-{years_list[-1]}"
                )
            )
    else:
        raw_years = [years] if isinstance(years, int) else list(years)
        try:
            years_list = sorted({int(y) for y in raw_years})
        except (TypeError, ValueError) as exc:
            raise ValueError(msg["invalid_years_type"]) from exc
        bad_years = [y for y in years_list if y < 1981 or y > max_year]
        if bad_years:
            raise ValueError(
                msg["invalid_years_range"].format(
                    max_year=max_year, bad=", ".join(str(y) for y in bad_years)
                )
            )

    # --- months -------------------------------------------------------------
    months_list = list(months)
    if not months_list or any(
        not isinstance(m, int) or m < 1 or m > 12 for m in months_list
    ):
        raise ValueError(msg["invalid_months"])
    months_list = sorted(set(months_list))

    # --- municipalities -------------------------------------------------------
    if municipalities is not None:
        try:
            import geopandas as gpd
        except ImportError as exc:
            raise ImportError(
                f"{msg['need_geopandas']} Install it with: pip install geopandas"
            ) from exc
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise ValueError(msg["muni_not_geodataframe"])

        # Mirrors the R source's rlang::check_installed("terra") /
        # check_installed("exactextractr") calls in its validation block
        # (lines 185-188): fail fast, before downloading any raster,
        # rather than only at the first _read_raster()/_zonal_stats() call
        # after a (possibly large) download loop has already run.
        for pkg in ("rioxarray", "exactextract"):
            if find_spec(pkg) is None:
                raise ImportError(
                    f"{pkg} is required to aggregate CHIRPS rasters to "
                    f"municipality polygons. Install it with: pip install {pkg}"
                )

    # --- agg_fun --------------------------------------------------------------
    if agg_fun not in _VALID_AGG:
        raise ValueError(
            msg["invalid_agg"].format(bad=agg_fun, valid=", ".join(_VALID_AGG))
        )

    # --- use_cache / cache_dir -------------------------------------------------
    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_path = Path(cache_dir).expanduser()

    # Parquet cache key: one municipality set can differ between calls, so a
    # hash of the sorted code_muni values disambiguates caches per call
    # (mirrors the R source's digest::digest()-based muni_hash — same
    # motivation, cache collisions across different municipality sets).
    muni_hash = ""
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities, msg)
        codes = sorted(str(c) for c in municipalities[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

    # --- build download manifest ----------------------------------------------
    manifest = _build_manifest(resolution, years_list, months_list, cache_path, muni_hash)
    if manifest.empty:
        raise ValueError(msg["no_data_params"])

    n_files = len(manifest)
    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files))

    # --- Parquet early-return -------------------------------------------------
    unique_pq = manifest["cache_pq"].unique().tolist()
    if municipalities is not None and use_cache and all(Path(p).is_file() for p in unique_pq):
        if verbose:
            console.print("[green]OK[/]  " + msg["parquet_cache_hit"])
        return _build_from_parquet(unique_pq, verbose, msg)

    # --- download rasters -------------------------------------------------------
    unique_files = manifest.drop_duplicates(subset=["filename"])[["filename", "url", "cache_tif"]]
    for row in unique_files.itertuples(index=False):
        _download_chirps_file(row.url, Path(row.cache_tif), use_cache, verbose, msg)

    # --- return file paths if no municipalities ---------------------------------
    if municipalities is None:
        paths = {row.filename: str(row.cache_tif) for row in unique_files.itertuples(index=False)}
        if verbose:
            console.print("[green]OK[/]  " + msg["done_paths"].format(n=len(paths)))
        return paths

    # --- prepare municipalities ---------------------------------------------------
    muni_col = _detect_muni_col(municipalities, msg)
    muni = municipalities.copy()
    muni["code_muni"] = muni[muni_col].astype(str).str.slice(0, 7)
    muni = muni.to_crs(epsg=4326)
    n_mun = len(muni)

    bbox = _BRAZIL_BBOX if crop_brazil else None

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["agg_start"].format(n_mun=n_mun))

    # --- aggregate per Parquet group (1 file per month for daily, else 1:1) -----
    result_frames: list[pd.DataFrame] = []
    for pq_path, group in manifest.groupby("cache_pq", sort=True):
        pq_path = Path(pq_path)

        if use_cache and pq_path.is_file() and pq_path.stat().st_size > 0:
            if verbose:
                console.print(
                    "[green]OK[/]  " + msg["parquet_hit"].format(filename=pq_path.name)
                )
            result_frames.append(pd.read_parquet(pq_path))
            continue

        group_df = _process_manifest_group(group, muni, agg_fun, bbox, verbose, msg)
        if group_df is None or group_df.empty:
            continue

        pq_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            group_df.to_parquet(pq_path, index=False)
        except Exception:
            if verbose:
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["parquet_write_warn"].format(filename=pq_path.name)
                )
        result_frames.append(group_df)

    result_frames = [f for f in result_frames if f is not None and not f.empty]
    if not result_frames:
        raise ValueError(msg["no_data"])

    result = pd.concat(result_frames, ignore_index=True)
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["agg_done"].format(n_rows=n_rows, n_mun=n_mun)
        )

    # --- attach metadata ----------------------------------------------------------
    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "chirps",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "unit": {"daily": "day", "monthly": "month", "annual": "year"}[resolution],
            "source": "ucsb_chirps_v2",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "resolution": resolution,
        "years": years_list,
        "months": months_list if resolution != "annual" else None,
        "agg_fun": agg_fun,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_chirps(): res={resolution}, {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: filename / URL construction (mirrors .chirps_file_info)
# ---------------------------------------------------------------------------

def _chirps_file_info(
    resolution: str, year: int, month: int | None, day: int | None
) -> tuple[str, str]:
    """Build the CHIRPS v2.0 filename and CHC download URL for one unit."""
    if resolution == "annual":
        filename = f"chirps-v2.0.{year:04d}.tif"
        url = f"{_CHIRPS_BASE}/global_annual/tifs/{filename}"
    elif resolution == "monthly":
        filename = f"chirps-v2.0.{year:04d}.{month:02d}.tif.gz"
        url = f"{_CHIRPS_BASE}/global_monthly/tifs/{filename}"
    else:  # daily
        filename = f"chirps-v2.0.{year:04d}.{month:02d}.{day:02d}.tif.gz"
        url = f"{_CHIRPS_BASE}/global_daily/tifs/p05/{year:04d}/{filename}"
    return filename, url


# ---------------------------------------------------------------------------
# Internal: download manifest
# ---------------------------------------------------------------------------

def _build_manifest(
    resolution: str,
    years: list[int],
    months: list[int],
    cache_path: Path,
    muni_hash: str,
) -> pd.DataFrame:
    """Build one row per raster file to download/process.

    Mirrors the R source's manifest construction, including the Parquet
    cache grouping: one Parquet file per month for daily resolution
    (shared across all days in that month), one per year for
    monthly/annual.
    """
    rows: list[dict] = []
    for yr in years:
        if resolution == "annual":
            filename, url = _chirps_file_info("annual", yr, None, None)
            pq_path = cache_path / "parquet" / f"chirps_annual_{yr:04d}{muni_hash}.parquet"
            rows.append({
                "resolution": "annual", "year": yr, "month": None, "day": None,
                "filename": filename, "url": url,
                "cache_tif": cache_path / resolution / filename,
                "cache_pq": pq_path,
                "date": pd.Timestamp(year=yr, month=1, day=1),
            })
        elif resolution == "monthly":
            for mo in months:
                filename, url = _chirps_file_info("monthly", yr, mo, None)
                pq_path = (
                    cache_path / "parquet" / f"chirps_monthly_{yr:04d}{mo:02d}{muni_hash}.parquet"
                )
                rows.append({
                    "resolution": "monthly", "year": yr, "month": mo, "day": None,
                    "filename": filename, "url": url,
                    "cache_tif": cache_path / resolution / filename,
                    "cache_pq": pq_path,
                    "date": pd.Timestamp(year=yr, month=mo, day=1),
                })
        else:  # daily
            for mo in months:
                n_days = calendar.monthrange(yr, mo)[1]
                pq_path = (
                    cache_path / "parquet" / f"chirps_daily_{yr:04d}{mo:02d}{muni_hash}.parquet"
                )
                for dy in range(1, n_days + 1):
                    filename, url = _chirps_file_info("daily", yr, mo, dy)
                    rows.append({
                        "resolution": "daily", "year": yr, "month": mo, "day": dy,
                        "filename": filename, "url": url,
                        "cache_tif": cache_path / resolution / filename,
                        "cache_pq": pq_path,
                        "date": pd.Timestamp(year=yr, month=mo, day=dy),
                    })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Internal: municipality identifier column detection
# ---------------------------------------------------------------------------

def _detect_muni_col(municipalities: gpd.GeoDataFrame, msg: dict[str, str]) -> str:
    """Auto-detect the municipality identifier column in a GeoDataFrame."""
    cols = list(municipalities.columns)
    for candidate in _MUNI_COL_CANDIDATES:
        if candidate in cols:
            return candidate
    for col in cols:
        if col == municipalities.geometry.name:
            continue
        sample = municipalities[col].dropna().astype(str).iloc[:5]
        if len(sample) > 0 and sample.str.match(r"^\d{6,7}$").all():
            return col
    raise ValueError(
        "Could not detect a municipality identifier column. Expected one of: "
        f"{', '.join(_MUNI_COL_CANDIDATES)}."
    )


# ---------------------------------------------------------------------------
# Internal: download one CHIRPS raster file with cache
# ---------------------------------------------------------------------------

def _download_chirps_file(
    url: str, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> None:
    """Download one CHIRPS GeoTIFF file to *cache_path*, reusing the cache.

    Never raises: download failures are logged as warnings and left for
    the per-date extraction step to skip via a missing-file check —
    mirroring the R source, which likewise ignores
    ``.chirps_download_file()``'s return value in the download loop.
    """
    filename = cache_path.name
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        if verbose:
            console.print("[green]OK[/]  " + msg["cache_hit"].format(filename=filename))
        return

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["download_file"].format(filename=filename))

    ok, reason = _download_robust(url, cache_path, max_retries=3, verbose=False)
    if ok:
        if verbose:
            console.print("[green]OK[/]  " + msg["download_done"].format(filename=filename))
    else:
        cache_path.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(filename=filename, err=reason or "unknown error")
        )


# ---------------------------------------------------------------------------
# Internal: raster read + zonal statistics
# ---------------------------------------------------------------------------

def _read_raster(tif_path: Path, bbox: tuple[float, float, float, float] | None):
    """Open a CHIRPS GeoTIFF (optionally gzip-compressed) as an ``xarray.DataArray``.

    Replaces CHIRPS no-data (values < -999, matching the R source's
    loose threshold rather than an exact -9999 equality check) with NaN,
    crops to *bbox* (minx, miny, maxx, maxy) if given, and ensures a
    WGS84 CRS is set (CHIRPS ships as plain WGS84 with no CRS tag on
    some mirrors).
    """
    try:
        import rioxarray
    except ImportError as exc:
        raise ImportError(
            "rioxarray is required to read CHIRPS GeoTIFF rasters. "
            "Install it with: pip install rioxarray"
        ) from exc

    gdal_path = f"/vsigzip/{tif_path}" if str(tif_path).endswith(".gz") else str(tif_path)
    da = rioxarray.open_rasterio(gdal_path, masked=False)

    if da.rio.crs is None:
        da = da.rio.write_crs("EPSG:4326")

    da = da.where(da >= -999)

    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        da = da.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    if "band" in da.dims:
        da = da.squeeze("band", drop=True)
    return da


def _zonal_stats(raster, municipalities: gpd.GeoDataFrame, agg_fun: str):
    """Compute per-polygon zonal statistics with ``exactextract``.

    Uses the ``exactextract`` PyPI package (the Python binding for the
    same isciences C++ engine R's ``exactextractr`` wraps), not
    ``rasterstats`` — see the module docstring.
    """
    try:
        from exactextract import exact_extract
    except ImportError as exc:
        raise ImportError(
            "exactextract is required to aggregate rasters to municipality "
            "polygons. Install it with: pip install exactextract"
        ) from exc

    stats = exact_extract(raster, municipalities, [agg_fun], output="pandas")
    col = agg_fun if agg_fun in stats.columns else stats.columns[-1]
    return stats[col].to_numpy()


# ---------------------------------------------------------------------------
# Internal: process one manifest group (rows sharing one Parquet cache file)
# ---------------------------------------------------------------------------

def _process_manifest_group(
    group: pd.DataFrame,
    municipalities: gpd.GeoDataFrame,
    agg_fun: str,
    bbox: tuple[float, float, float, float] | None,
    verbose: bool,
    msg: dict[str, str],
) -> pd.DataFrame | None:
    """Extract zonal statistics for every date in *group*, one raster at a time."""
    day_frames: list[pd.DataFrame] = []
    for row in group.itertuples(index=False):
        tif_path = Path(row.cache_tif)
        if not tif_path.is_file() or tif_path.stat().st_size == 0:
            console.print(
                "[yellow]WARN[/]  " + msg["skip_missing"].format(filename=row.filename)
            )
            continue

        try:
            raster = _read_raster(tif_path, bbox)
            values = _zonal_stats(raster, municipalities, agg_fun)
            day_frames.append(pd.DataFrame({
                "code_muni": municipalities["code_muni"].to_numpy(),
                "date": row.date,
                "rainfall_chirps_mm": pd.array(values, dtype="float64"),
            }))
        except ImportError:
            raise
        except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
            console.print(
                "[yellow]WARN[/]  "
                + msg["extract_warn"].format(filename=row.filename, err=str(e))
            )

    if not day_frames:
        return None
    return pd.concat(day_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Internal: assemble result from pre-existing Parquet caches (early return)
# ---------------------------------------------------------------------------

def _build_from_parquet(
    pq_paths: list[str], verbose: bool, msg: dict[str, str]
) -> pd.DataFrame:
    """Assemble the output DataFrame purely from cached Parquet files."""
    parts = []
    for p in pq_paths:
        try:
            parts.append(pd.read_parquet(p))
        except Exception:
            continue

    if not parts:
        raise ValueError(msg["no_data"])

    result = pd.concat(parts, ignore_index=True)
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    n_mun = result["code_muni"].nunique()
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["agg_done"].format(n_rows=n_rows, n_mun=n_mun)
        )

    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "chirps",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "source": "ucsb_chirps_v2_cache",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_chirps(): from Parquet cache, {n_rows} obs"
        ],
        "user": {},
    }
    return result
