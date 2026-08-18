"""grid_pollution_ghap.py — GHAP air pollution data for Brazilian municipalities.

Mirrors R: sus_grid_pollution_ghap.R

Downloads GHAP (GlobalHighAirPollutants) NetCDF raster data from Zenodo,
crops it to Brazil, spatially aggregates to municipality polygons via
zonal statistics using ``exactextract`` (the literal Python binding for
the same isciences C++ engine that R's ``exactextractr`` wraps — not
``rasterstats``, whose default zonal-stats masking only approximates the
fractional pixel-polygon overlap weighting both ``exactextractr`` and
``exactextract`` compute), and caches the result as Parquet.

Not lazy — raster I/O and zonal statistics are fundamentally
geometry/row-oriented work with no natural DuckDB SQL expression, and the
R source itself never routes this through Arrow/DuckDB either (it works
directly with ``terra``/``exactextractr`` and returns a materialised
tibble). The Python port mirrors that: results are built as a
municipality x date ``pd.DataFrame`` with metadata attached via
``df.attrs["sus_meta"]``.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import tempfile
import zipfile
from datetime import date, datetime
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd
from rich.console import Console

from ..core.climate_inmet import _download_robust

if TYPE_CHECKING:
    import geopandas as gpd

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "ghap"

_VALID_POLLUTANTS: tuple[str, ...] = ("pm25", "o3", "co")
_ANNUAL_ONLY: tuple[str, ...] = ("o3", "co")
_VALID_RESOLUTIONS: tuple[str, ...] = ("daily", "monthly", "annual")
_VALID_AGG: tuple[str, ...] = ("mean", "sum", "median", "min", "max")

_ALL_MONTHS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

# Available years per pollutant per (effective) resolution.
_AVAIL_YEARS: dict[str, dict[str, tuple[int, ...]]] = {
    "pm25": {
        "daily": tuple(range(2017, 2023)),
        "monthly": tuple(range(2017, 2023)),
        "annual": tuple(range(2017, 2023)),
    },
    "o3": {"annual": tuple(range(2000, 2021))},
    "co": {"annual": tuple(range(2019, 2023))},
}

# Zenodo record IDs, by pollutant and resolution. PM2.5 daily is one
# separate record per year (one ~3 GB ZIP per month inside).
_RECORDS: dict[str, dict[str, object]] = {
    "pm25": {
        "monthly": "10800980",
        "annual": "10800980",
        "daily": {
            2017: "10801181",
            2018: "10795801",
            2019: "10799037",
            2020: "10800555",
            2021: "10799203",
            2022: "10795662",
        },
    },
    "o3": {"annual": "10208188"},
    "co": {"annual": "14207363"},
}

# Resolution code embedded in filenames. O3 is actually ~10 km but is
# still tagged "1K" in the GHAP filenames themselves (preserved verbatim).
_RES_CODE: dict[str, str] = {"pm25": "1K", "o3": "1K", "co": "1K"}
_P_FNAME: dict[str, str] = {"pm25": "PM2.5", "o3": "O3", "co": "CO"}

# terra::ext(-75, -28, -35, 6) is (xmin, xmax, ymin, ymax) — kept in that
# order throughout this module to match the R source's own bbox helpers.
_BRAZIL_BBOX: tuple[float, float, float, float] = (-75.0, -28.0, -35.0, 6.0)
_GLOBAL_BBOX: tuple[float, float, float, float] = (-180.0, 180.0, -90.0, 90.0)

_ZENODO_URL_TEMPLATE = "https://zenodo.org/records/{record_id}/files/{filename}?download=1"

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados GHAP de Poluição Atmosférica",
        "invalid_pollutants_type": "'pollutants' deve ser um vetor de caracteres.",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "no2_unavailable": "NO2 ainda não está disponível no GHAP. Será ignorado.",
        "no_pollutants": "Nenhum poluente válido selecionado.",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "invalid_months": "'months' deve ser um vetor inteiro entre 1 e 12.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opções: {valid}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "need_geopandas": "O pacote geopandas é necessário para agregar por municípios.",
        "muni_not_geodataframe": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "fallback_annual": (
            "{pollutant}: apenas dados anuais disponíveis no GHAP. "
            "Usando resolution='annual'."
        ),
        "years_unavail": (
            "{pollutant}: anos {bad_years} fora do intervalo disponível ({avail}). Ignorando."
        ),
        "no_data_to_download": "Nenhum dado disponível para os parâmetros fornecidos.",
        "download_start": "Baixando {n_files} arquivo(s) GHAP do Zenodo...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "parquet_cache_hit": "Todos os arquivos encontrados no cache Parquet. Carregando...",
        "parquet_hit": "Cache Parquet: {filename}",
        "parquet_write_warn": "Não foi possível salvar cache Parquet: {filename}",
        "skip_missing": "Arquivo não disponível no cache: {filename}",
        "extract_warn": "Não foi possível processar {filename}: {err}",
        "no_data": "Nenhum dado foi extraído com sucesso.",
        "agg_start": "Agregando para {n_mun} município(s)...",
        "agg_done": "Concluído: {n_rows} observações ({n_mun} municípios).",
        "zip_extract": "Extraindo ZIP: {filename} (pode levar alguns minutos)...",
        "zip_no_nc": "Nenhum arquivo .nc encontrado dentro de {filename}.",
        "daily_processing": "Processando {n_days} dia(s) de {month}...",
        "done_paths": "{n} arquivo(s) disponível(is) no cache.",
    },
    "en": {
        "title": "GHAP Atmospheric Pollution Data",
        "invalid_pollutants_type": "'pollutants' must be a character vector.",
        "invalid_pollutants": "Invalid 'pollutants': {bad}. Use: {valid}.",
        "no2_unavailable": "NO2 is not yet publicly available in GHAP and will be skipped.",
        "no_pollutants": "No valid pollutants selected.",
        "invalid_resolution": "Invalid 'resolution': {bad}. Use: {valid}.",
        "invalid_months": "'months' must be an integer vector between 1 and 12.",
        "invalid_agg": "Invalid 'agg_fun': {bad}. Options: {valid}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "need_geopandas": "The geopandas package is required to aggregate by municipality.",
        "muni_not_geodataframe": "'municipalities' must be a geopandas.GeoDataFrame.",
        "fallback_annual": (
            "{pollutant}: only annual data available in GHAP. "
            "Using resolution='annual'."
        ),
        "years_unavail": (
            "{pollutant}: years {bad_years} outside available range ({avail}). Skipping."
        ),
        "no_data_to_download": "No data available for the provided parameters.",
        "download_start": "Downloading {n_files} GHAP file(s) from Zenodo...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "parquet_cache_hit": "All files found in Parquet cache. Loading...",
        "parquet_hit": "Parquet cache: {filename}",
        "parquet_write_warn": "Could not write Parquet cache: {filename}",
        "skip_missing": "File not available in cache: {filename}",
        "extract_warn": "Could not process {filename}: {err}",
        "no_data": "No data was successfully extracted.",
        "agg_start": "Aggregating to {n_mun} municipality/ies...",
        "agg_done": "Complete: {n_rows} observations ({n_mun} municipalities).",
        "zip_extract": "Extracting ZIP: {filename} (may take a few minutes)...",
        "zip_no_nc": "No .nc files found inside {filename}.",
        "daily_processing": "Processing {n_days} day(s) for {month}...",
        "done_paths": "{n} file(s) available in cache.",
    },
    "es": {
        "title": "Datos GHAP de Contaminación Atmosférica",
        "invalid_pollutants_type": "'pollutants' debe ser un vector de caracteres.",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "no2_unavailable": "NO2 aún no está disponible en GHAP y será omitido.",
        "no_pollutants": "Ningún contaminante válido seleccionado.",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "invalid_months": "'months' debe ser un vector entero entre 1 y 12.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opciones: {valid}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "need_geopandas": "El paquete geopandas es necesario para agregar por municipios.",
        "muni_not_geodataframe": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "fallback_annual": (
            "{pollutant}: solo datos anuales disponibles en GHAP. "
            "Usando resolution='annual'."
        ),
        "years_unavail": (
            "{pollutant}: años {bad_years} fuera del rango disponible ({avail}). Omitiendo."
        ),
        "no_data_to_download": "No hay datos disponibles para los parámetros indicados.",
        "download_start": "Descargando {n_files} archivo(s) GHAP de Zenodo...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "parquet_cache_hit": "Todos los archivos encontrados en caché Parquet. Cargando...",
        "parquet_hit": "Caché Parquet: {filename}",
        "parquet_write_warn": "No se pudo guardar caché Parquet: {filename}",
        "skip_missing": "Archivo no disponible en caché: {filename}",
        "extract_warn": "No se pudo procesar {filename}: {err}",
        "no_data": "No se extrajo ningún dato correctamente.",
        "agg_start": "Agregando a {n_mun} municipio(s)...",
        "agg_done": "Completo: {n_rows} observaciones ({n_mun} municipios).",
        "zip_extract": "Extrayendo ZIP: {filename} (puede tardar varios minutos)...",
        "zip_no_nc": "No se encontraron archivos .nc en {filename}.",
        "daily_processing": "Procesando {n_days} día(s) de {month}...",
        "done_paths": "{n} archivo(s) disponible(s) en caché.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_pollution_ghap(
    pollutants: str | list[str] = "pm25",
    resolution: Literal["daily", "monthly", "annual"] = "monthly",
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
    """Import GHAP high-resolution air pollution data for Brazilian municipalities.

    Downloads, crops, spatially aggregates, and caches GHAP
    (GlobalHighAirPollutants) raster data to Brazilian municipalities. No
    API key is required.

    GHAP provides AI-generated, seamless ground-level air pollutant
    fields at 1 km (PM2.5, CO) and 10 km (O3) resolution. Files are
    downloaded as NetCDF from Zenodo, cropped to Brazil, aggregated with
    area-weighted extraction, and the result is stored as a Parquet
    cache for fast subsequent access.

    Available pollutants and temporal coverage:
        - PM2.5 (``"pm25"``) — daily, monthly & annual, 2017-2022, 1 km,
          ug/m3.
        - O3 (``"o3"``) — annual only, 2000-2020, 10 km, ppb.
        - CO (``"co"``) — annual only, 2019-2022, 1 km, mg/m3.
        - NO2 (``"no2"``) — data not yet publicly released.

    Args:
        pollutants: Pollutant(s) to download. Accepted: ``"pm25"``,
            ``"o3"``, ``"co"``, or ``"all"`` (expands to all three).
            Default ``"pm25"``. ``"no2"`` is not yet publicly available
            and is dropped with a warning if present.
        resolution: Temporal aggregation of source files: ``"daily"``
            (PM2.5 only, 2017-2022; each ~3 GB monthly ZIP is downloaded,
            extracted to a temp directory, each daily NetCDF is
            aggregated, and the result is cached as Parquet — subsequent
            calls read from Parquet without re-extracting), ``"monthly"``
            (PM2.5 only), or ``"annual"`` (PM2.5, O3, CO). O3 and CO
            automatically fall back to ``"annual"`` regardless of this
            parameter. Default ``"monthly"``.
        years: Year(s) to download. Availability depends on pollutant
            and resolution (see above). ``None`` (default) returns all
            available years for the selected pollutant(s).
        months: Months (1-12) to include when ``resolution="monthly"``.
            Ignored for annual/daily-ZIP-selection logic (daily still
            uses ``months`` to pick which monthly ZIPs to download).
            Default: all 12 months.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from ``climasus-data`` municipality
            boundaries). When provided, rasters are aggregated and a
            ``pd.DataFrame`` is returned. If ``None``, a dict mapping
            ``"{pollutant}_{year}_{month|'annual'}"`` -> cached file path
            is returned instead (no spatial aggregation).
        agg_fun: Spatial aggregation function for ``exactextract``.
            Default ``"mean"`` (area-weighted mean).
        crop_brazil: Crop global rasters to Brazil's bounding box before
            aggregation. Reduces memory usage significantly. Default
            ``True``.
        use_cache: Reuse previously downloaded raster files and
            aggregated Parquet caches. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/ghap``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni`` (str), ``date`` (datetime), and one column per
        requested pollutant (e.g. ``pm25_mean``, ``o3_mean``). Metadata
        in ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="pollution_ghap"``). If *municipalities* is ``None``: a
        dict mapping each cache key to its local NetCDF/ZIP path.

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            extracted for the given parameters.
        ImportError: If ``geopandas``, ``rioxarray``, or ``exactextract``
            are required but not installed.

    Data source:
        Wei, J. et al. (2023). Estimating 1-km-resolution PM2.5
        concentrations across China using the space-time random forest
        approach. Remote Sensing of Environment, 231, 111221.
        Wei, J. et al. GlobalHighAirPollutants (GHAP) v2. Zenodo.
        CC-BY 4.0.
        PM2.5: https://doi.org/10.5281/zenodo.10800980 |
        O3: https://doi.org/10.5281/zenodo.10208188 |
        CO: https://doi.org/10.5281/zenodo.14207363

    Examples::

        import climasus4py as cs

        ghap = cs.sus_grid_pollution_ghap(
            pollutants="pm25", resolution="monthly", years=2020,
            months=[1, 2, 3], municipalities=mt_mun, lang="pt",
        )
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    pollutants_list = _validate_pollutants(pollutants, msg, verbose)

    if resolution not in _VALID_RESOLUTIONS:
        raise ValueError(
            msg["invalid_resolution"].format(
                bad=resolution, valid=", ".join(_VALID_RESOLUTIONS)
            )
        )

    months_list = list(months)
    if not months_list or any(
        not isinstance(m, int) or m < 1 or m > 12 for m in months_list
    ):
        raise ValueError(msg["invalid_months"])
    months_list = sorted(set(months_list))

    if municipalities is not None:
        try:
            import geopandas as gpd
        except ImportError as exc:
            raise ImportError(
                f"{msg['need_geopandas']} Install it with: pip install geopandas"
            ) from exc
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise ValueError(msg["muni_not_geodataframe"])

        # Fail fast, before downloading anything — mirrors the R
        # source's rlang::check_installed("terra")/("exactextractr").
        for pkg in ("rioxarray", "exactextract"):
            if find_spec(pkg) is None:
                raise ImportError(
                    f"{pkg} is required to aggregate GHAP rasters to "
                    f"municipality polygons. Install it with: pip install {pkg}"
                )

    if agg_fun not in _VALID_AGG:
        raise ValueError(
            msg["invalid_agg"].format(bad=agg_fun, valid=", ".join(_VALID_AGG))
        )

    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_path = Path(cache_dir).expanduser()

    # Parquet cache key: one municipality set can differ between calls, so
    # a hash of the sorted code_muni values disambiguates caches per call.
    # The R source's cache filenames omit this (same bug later found and
    # fixed in sus_grid_pdsi.R's Python port) — fixed here up front rather
    # than replicated, per CLAUDE.md rule 4's silent-incorrect-result
    # exception: two calls with the same pollutants/years but different
    # municipalities would otherwise silently reuse the wrong cache.
    muni_hash = ""
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities, msg)
        codes = sorted(str(c) for c in municipalities[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

    manifest = _build_manifest(
        pollutants_list, resolution, years, months_list, cache_path, muni_hash, verbose, msg
    )
    if manifest.empty:
        raise ValueError(msg["no_data_to_download"])

    n_files = len(manifest)
    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files))

    # --- Parquet early-return: skip download entirely -------------------------
    if municipalities is not None and use_cache and all(
        Path(p).is_file() for p in manifest["cache_pq"]
    ):
        if verbose:
            console.print("[green]OK[/]  " + msg["parquet_cache_hit"])
        return _build_from_parquet(manifest, verbose, msg)

    # --- download raster files (NetCDF or ZIP) ---------------------------------
    for row in manifest.itertuples(index=False):
        _download_file(row.url, Path(row.cache_nc), use_cache, verbose, msg)

    # --- return file paths if no municipalities ---------------------------------
    if municipalities is None:
        paths: dict[str, str] = {}
        for row in manifest.itertuples(index=False):
            key = f"{row.pollutant}_{row.year}_{row.month if row.month else 'annual'}"
            paths[key] = str(row.cache_nc)
        if verbose:
            console.print("[green]OK[/]  " + msg["done_paths"].format(n=n_files))
        return paths

    # --- prepare municipalities ---------------------------------------------------
    muni_col = _detect_muni_col(municipalities, msg)
    muni = municipalities.copy()
    muni["code_muni"] = muni[muni_col].astype(str).str.slice(0, 7)
    muni = muni.to_crs(epsg=4326)
    n_mun = len(muni)

    bbox = _BRAZIL_BBOX if crop_brazil else _GLOBAL_BBOX

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["agg_start"].format(n_mun=n_mun))

    # --- extract raster -> polygons for each manifest row ----------------------
    result_frames: list[pd.DataFrame] = []
    for row in manifest.itertuples(index=False):
        nc_path = Path(row.cache_nc)
        pq_path = Path(row.cache_pq)

        if not nc_path.is_file() or nc_path.stat().st_size == 0:
            console.print(
                "[yellow]WARN[/]  " + msg["skip_missing"].format(filename=row.filename)
            )
            continue

        if use_cache and pq_path.is_file() and pq_path.stat().st_size > 0:
            if verbose:
                console.print(
                    "[green]OK[/]  " + msg["parquet_hit"].format(filename=pq_path.name)
                )
            result_frames.append(pd.read_parquet(pq_path))
            continue

        df_i = _extract_manifest_row(row, muni, agg_fun, bbox, verbose, msg)
        if df_i is None or df_i.empty:
            continue

        pq_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df_i.to_parquet(pq_path, index=False)
        except Exception:
            if verbose:
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["parquet_write_warn"].format(filename=pq_path.name)
                )
        result_frames.append(df_i)

    result_frames = [f for f in result_frames if f is not None and not f.empty]
    if not result_frames:
        raise ValueError(msg["no_data"])

    result = _merge_pollutant_frames(result_frames)
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["agg_done"].format(n_rows=n_rows, n_mun=n_mun)
        )

    # --- attach metadata ----------------------------------------------------------
    now = datetime.now()
    # NOTE: the R source maps resolution -> temporal.unit with a buggy
    # 2-way ternary (`if (resolution == "monthly") "day" else "year"`),
    # which silently mislabels daily/annual data. Per CLAUDE.md rule 4's
    # exception for silent-incorrect-result bugs, this is corrected here
    # (not just replicated) to the correct per-resolution mapping already
    # used by the sibling sus_grid_chirps.
    unit_map = {"daily": "day", "monthly": "month", "annual": "year"}
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "pollution_ghap",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "unit": unit_map[resolution],
            "source": "zenodo_ghap",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "pollutants": pollutants_list,
        "resolution": resolution,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "agg_fun": agg_fun,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_pollution_ghap(): {len(pollutants_list)} pollutant(s), "
            f"res={resolution}, {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: pollutants validation (mirrors the R source's no2/valid-list logic)
# ---------------------------------------------------------------------------

def _validate_pollutants(
    pollutants: str | list[str], msg: dict[str, str], verbose: bool
) -> list[str]:
    if pollutants == "all":
        return list(_VALID_POLLUTANTS)

    raw = [pollutants] if isinstance(pollutants, str) else list(pollutants)
    if not raw or not all(isinstance(p, str) for p in raw):
        raise ValueError(msg["invalid_pollutants_type"])

    # Detection is case-insensitive, but removal only matches the literal
    # "no2"/"NO2" strings — mirrors the R source's tolower()-detect +
    # setdiff(pollutants, c("no2", "NO2"))-remove mismatch verbatim: a
    # mixed-case value like "No2" triggers the warning but is NOT actually
    # removed, and then fails the invalid_pollutants check below instead.
    if any(p.lower() == "no2" for p in raw):
        if verbose:
            console.print("[yellow]WARN[/]  " + msg["no2_unavailable"])
        raw = [p for p in raw if p not in ("no2", "NO2")]

    bad = [p for p in raw if p not in _VALID_POLLUTANTS]
    if bad:
        raise ValueError(
            msg["invalid_pollutants"].format(
                bad=", ".join(bad), valid=", ".join(_VALID_POLLUTANTS)
            )
        )
    if not raw:
        raise ValueError(msg["no_pollutants"])
    return raw


# ---------------------------------------------------------------------------
# Internal: filename / URL construction (mirrors .ghap_file_info)
# ---------------------------------------------------------------------------

def _file_info(
    pollutant: str, resolution: str, year: int, month: int | None
) -> tuple[str, str, str]:
    """Build the GHAP filename, Zenodo record id, and download URL for one file."""
    p_fname = _P_FNAME[pollutant]
    res_code = _RES_CODE[pollutant]

    if resolution == "daily" and month is not None:
        ym = f"{year:04d}{month:02d}"
        filename = f"GHAP_{p_fname}_D{res_code}_{ym}_V1.zip"
        record_id = _RECORDS[pollutant]["daily"].get(year)  # type: ignore[union-attr]
        if not record_id:
            raise ValueError(f"No daily Zenodo record found for {pollutant} year {year}.")
    elif resolution == "monthly" and month is not None:
        ym = f"{year:04d}{month:02d}"
        filename = f"GHAP_{p_fname}_M{res_code}_{ym}_V1.nc"
        record_id = _RECORDS[pollutant]["monthly"]
    else:
        filename = f"GHAP_{p_fname}_Y{res_code}_{year:04d}_V1.nc"
        record_id = _RECORDS[pollutant]["annual"]

    url = _ZENODO_URL_TEMPLATE.format(record_id=record_id, filename=filename)
    return filename, str(record_id), url


# ---------------------------------------------------------------------------
# Internal: download manifest, with per-pollutant resolution/year fallback
# ---------------------------------------------------------------------------

def _build_manifest(
    pollutants: list[str],
    resolution: str,
    years: int | list[int] | None,
    months: list[int],
    cache_dir: Path,
    muni_hash: str,
    verbose: bool,
    msg: dict[str, str],
) -> pd.DataFrame:
    """Build one row per raster file to download/process, per pollutant.

    Auto-falls back non-annual pollutants (o3, co) to resolution="annual"
    with a warning, and clips requested years to the pollutant's actually
    available range (also with a warning), mirroring the R source.

    *muni_hash* (empty string when no municipalities were provided) is
    appended to every Parquet cache filename so that two calls with the
    same pollutants/years/resolution but different municipality sets
    never collide on the same cache file — see the fix note in the
    caller and IDEIAS.md.
    """
    rows: list[dict] = []
    for p in pollutants:
        p_res = resolution
        if p in _ANNUAL_ONLY and p_res in ("monthly", "daily"):
            if verbose:
                console.print(
                    "[yellow]WARN[/]  " + msg["fallback_annual"].format(pollutant=p.upper())
                )
            p_res = "annual"

        avail_years = _AVAIL_YEARS[p][p_res]
        if years is None:
            req_years = list(avail_years)
        else:
            raw_years = [years] if isinstance(years, int) else list(years)
            req_years = sorted({int(y) for y in raw_years})
            bad_years = [y for y in req_years if y not in avail_years]
            if bad_years:
                if verbose:
                    console.print(
                        "[yellow]WARN[/]  "
                        + msg["years_unavail"].format(
                            pollutant=p.upper(),
                            bad_years=", ".join(str(y) for y in bad_years),
                            avail=f"{min(avail_years)}-{max(avail_years)}",
                        )
                    )
                req_years = [y for y in req_years if y in avail_years]
                if not req_years:
                    continue

        for yr in req_years:
            if p_res == "daily":
                for mo in months:
                    filename, record_id, url = _file_info(p, "daily", yr, mo)
                    rows.append({
                        "pollutant": p, "resolution": "daily", "year": yr,
                        "month": f"{mo:02d}", "filename": filename, "record_id": record_id,
                        "url": url,
                        "cache_nc": cache_dir / p / "daily" / filename,
                        "cache_pq": (
                            cache_dir / p / "parquet"
                            / f"{p}_daily_{yr:04d}{mo:02d}{muni_hash}.parquet"
                        ),
                    })
            elif p_res == "monthly":
                for mo in months:
                    filename, record_id, url = _file_info(p, "monthly", yr, mo)
                    rows.append({
                        "pollutant": p, "resolution": "monthly", "year": yr,
                        "month": f"{mo:02d}", "filename": filename, "record_id": record_id,
                        "url": url,
                        "cache_nc": cache_dir / p / filename,
                        "cache_pq": (
                            cache_dir / p / "parquet" / f"{p}_{yr:04d}{mo:02d}{muni_hash}.parquet"
                        ),
                    })
            else:  # annual
                filename, record_id, url = _file_info(p, "annual", yr, None)
                rows.append({
                    "pollutant": p, "resolution": "annual", "year": yr,
                    "month": None, "filename": filename, "record_id": record_id,
                    "url": url,
                    "cache_nc": cache_dir / p / filename,
                    "cache_pq": (
                        cache_dir / p / "parquet" / f"{p}_{yr:04d}_annual{muni_hash}.parquet"
                    ),
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
# Internal: download one GHAP file (NetCDF or ZIP) with cache
# ---------------------------------------------------------------------------

def _download_file(
    url: str, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> None:
    """Download one GHAP file to *cache_path*, reusing the cache.

    Never raises: download failures are logged as warnings and left for
    the per-file extraction step to skip via a missing-file check —
    mirrors the R source's ``.ghap_download_file()``.
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
# Internal: GHAP pixel-grid <-> WGS84 conversion (mirrors .ghap_to_pixel_bbox)
# ---------------------------------------------------------------------------

def _to_pixel_bbox(
    xmin: float, xmax: float, ymin: float, ymax: float
) -> tuple[int, int, int, int]:
    """Convert a WGS84 bounding box to GHAP's pixel coordinate space.

    GHAP NetCDF files store data in a custom pixel grid: x (longitude)
    0-36000 mapping -180 to +180 degrees (100 px/degree); y (latitude)
    0-18000 mapping +90 to -90 degrees, INVERTED (100 px/degree). After
    cropping, the raster must be flipped vertically and re-assigned
    EPSG:4326 to be usable with geopandas/exactextract.
    """
    px_xmin = round((xmin + 180) * 100)
    px_xmax = round((xmax + 180) * 100)
    # y is inverted: geographic ymax (north) -> smallest pixel-y value.
    px_ymin = round((90 - ymax) * 100)
    px_ymax = round((90 - ymin) * 100)
    return px_xmin, px_xmax, px_ymin, px_ymax


def _read_and_fix(
    nc_path: Path, xmin: float, xmax: float, ymin: float, ymax: float
):
    """Read a GHAP NetCDF, crop to bbox in pixel space, flip, and assign WGS84.

    Not verified against a real (multi-GB) GHAP NetCDF download — mirrors
    the R source's ``.ghap_read_and_fix()`` transform (crop in pixel
    space using ``_to_pixel_bbox``, flip vertically because GHAP stores
    latitude north-to-south, then relabel the resulting extent as
    *(xmin, xmax, ymin, ymax)* and assign EPSG:4326) using rioxarray.
    """
    try:
        import rioxarray
    except ImportError as exc:
        raise ImportError(
            "rioxarray is required to read GHAP NetCDF raster files. "
            "Install it with: pip install rioxarray"
        ) from exc

    da = rioxarray.open_rasterio(str(nc_path), masked=False)
    if isinstance(da, list):
        da = da[0]

    # clip_box() requires a CRS to be set even though we crop purely in
    # raw pixel-index space here; the placeholder is overwritten with the
    # real EPSG:4326 assignment below once the true extent is known.
    if da.rio.crs is None:
        da = da.rio.write_crs("EPSG:4326")

    px_xmin, px_xmax, px_ymin, px_ymax = _to_pixel_bbox(xmin, xmax, ymin, ymax)
    da = da.rio.clip_box(minx=px_xmin, miny=px_ymin, maxx=px_xmax, maxy=px_ymax)

    # Flip vertically: GHAP pixel row 0 is the northernmost row.
    da = da.isel(y=slice(None, None, -1))

    ny = da.sizes["y"]
    nx = da.sizes["x"]
    new_x = xmin + (np.arange(nx) + 0.5) * (xmax - xmin) / nx
    new_y = ymax - (np.arange(ny) + 0.5) * (ymax - ymin) / ny
    da = da.assign_coords(x=new_x, y=new_y)
    da = da.rio.write_crs("EPSG:4326")
    return da


# ---------------------------------------------------------------------------
# Internal: zonal statistics (shared by NetCDF and daily-ZIP branches)
# ---------------------------------------------------------------------------

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
# Internal: extract one manifest row (NetCDF or daily-ZIP branch)
# ---------------------------------------------------------------------------

def _extract_manifest_row(
    row, municipalities: gpd.GeoDataFrame, agg_fun: str,
    bbox: tuple[float, float, float, float], verbose: bool, msg: dict[str, str],
) -> pd.DataFrame | None:
    out_col = f"{row.pollutant}_{agg_fun}"
    try:
        if str(row.cache_nc).endswith(".zip"):
            return _extract_daily_zip(
                Path(row.cache_nc), municipalities, bbox, agg_fun, out_col,
                int(row.year), row.month, verbose, msg,
            )
        return _extract_grid_file(
            Path(row.cache_nc), municipalities, bbox, agg_fun, out_col,
            int(row.year), row.month,
        )
    except ImportError:
        raise
    except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
        console.print(
            "[yellow]WARN[/]  "
            + msg["extract_warn"].format(filename=row.filename, err=str(e))
        )
        return None


def _fallback_date(yr: int, month: str | None) -> date:
    mo = int(month) if month else 1
    return date(yr, mo, 1)


def _extract_grid_file(
    nc_path: Path, municipalities: gpd.GeoDataFrame, bbox, agg_fun: str,
    out_col: str, yr: int, month: str | None,
) -> pd.DataFrame:
    """Extract zonal statistics from one monthly/annual GHAP NetCDF file.

    Handles both the common single-band case and a defensive multi-band
    case (one band per day inside a single monthly file) — the latter is
    speculative (not verified against a real multi-band GHAP file) and
    falls back to the file's nominal year/month date whenever no usable
    per-band time coordinate is found, mirroring the R source's own
    fallback in ``sus_grid_pollution_ghap.R``.
    """
    raster = _read_and_fix(nc_path, *bbox)
    n_bands = raster.sizes["band"] if "band" in raster.dims else 1

    if n_bands <= 1:
        r2d = raster.squeeze("band", drop=True) if "band" in raster.dims else raster
        values = _zonal_stats(r2d, municipalities, agg_fun)
        return pd.DataFrame({
            "code_muni": municipalities["code_muni"].to_numpy(),
            "date": _fallback_date(yr, month),
            out_col: pd.array(values, dtype="float64"),
        })

    frames = []
    times = raster.coords["time"].to_numpy() if "time" in raster.coords else None
    for b in range(n_bands):
        band_raster = raster.isel(band=b)
        values = _zonal_stats(band_raster, municipalities, agg_fun)
        if times is not None and b < len(times):
            band_date = pd.Timestamp(times[b]).date()
        else:
            band_date = _fallback_date(yr, month)
        frames.append(pd.DataFrame({
            "code_muni": municipalities["code_muni"].to_numpy(),
            "date": band_date,
            out_col: pd.array(values, dtype="float64"),
        }))
    return pd.concat(frames, ignore_index=True)


def _extract_daily_zip(
    zip_path: Path, municipalities: gpd.GeoDataFrame, bbox, agg_fun: str,
    out_col: str, yr: int, month: str | None, verbose: bool, msg: dict[str, str],
) -> pd.DataFrame | None:
    """Extract and aggregate all daily NetCDF files from a monthly GHAP ZIP.

    Downloads are ~3 GB per ZIP. After aggregation the daily NC files are
    deleted from the temp directory to free disk space; the ZIP itself
    is kept in cache for future calls so the full extraction only runs
    once.
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"ghap_{zip_path.stem}_"))
    try:
        if verbose:
            console.print(
                "[cyan]INFO[/]  " + msg["zip_extract"].format(filename=zip_path.name)
            )
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)

        nc_files = sorted(tmp_dir.rglob("*.nc"))
        if not nc_files:
            console.print(
                "[yellow]WARN[/]  " + msg["zip_no_nc"].format(filename=zip_path.name)
            )
            return None

        if verbose:
            console.print(
                "[cyan]INFO[/]  "
                + msg["daily_processing"].format(
                    n_days=len(nc_files), month=f"{yr}-{month}"
                )
            )

        day_frames: list[pd.DataFrame] = []
        for nc_path in nc_files:
            try:
                raster = _read_and_fix(nc_path, *bbox)
                if "band" in raster.dims:
                    raster = raster.isel(band=0)
                values = _zonal_stats(raster, municipalities, agg_fun)

                # Filename pattern: GHAP_PM2.5_D1K_YYYYMMDD_V1.nc — the
                # date is the single 8-digit token in the stem.
                candidates = [
                    part for part in nc_path.stem.split("_") if re.fullmatch(r"\d{8}", part)
                ]
                if len(candidates) == 1:
                    day_date = datetime.strptime(candidates[0], "%Y%m%d").date()
                else:
                    day_date = _fallback_date(yr, month)

                day_frames.append(pd.DataFrame({
                    "code_muni": municipalities["code_muni"].to_numpy(),
                    "date": day_date,
                    out_col: pd.array(values, dtype="float64"),
                }))
            except ImportError:
                raise
            except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["extract_warn"].format(filename=nc_path.name, err=str(e))
                )

        if not day_frames:
            return None
        return pd.concat(day_frames, ignore_index=True)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Internal: merge per-pollutant frames into one wide muni x date table
# ---------------------------------------------------------------------------

def _merge_pollutant_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine long ``code_muni``/``date``/``<pollutant>_<agg>`` frames.

    Frames sharing the same value column (i.e. same pollutant) are
    row-concatenated; frames for different pollutants are then combined
    via an outer merge on ``(code_muni, date)`` so the final table has
    one row per municipality x date with one column per pollutant — this
    is the wide shape documented by the R source, which it does not
    actually achieve for multi-pollutant requests (its ``do.call(rbind,
    result_list)`` would error on mismatched column names across
    pollutants); see IDEIAS.md.
    """
    groups: dict[str, list[pd.DataFrame]] = {}
    for f in frames:
        value_col = next(c for c in f.columns if c not in ("code_muni", "date"))
        groups.setdefault(value_col, []).append(f)

    merged: pd.DataFrame | None = None
    for col in sorted(groups):
        part = pd.concat(groups[col], ignore_index=True)
        merged = (
            part if merged is None
            else merged.merge(part, on=["code_muni", "date"], how="outer")
        )
    assert merged is not None
    return merged


# ---------------------------------------------------------------------------
# Internal: assemble result from pre-existing Parquet caches (early return)
# ---------------------------------------------------------------------------

def _build_from_parquet(manifest: pd.DataFrame, verbose: bool, msg: dict[str, str]) -> pd.DataFrame:
    """Assemble the output DataFrame purely from cached Parquet files."""
    parts = []
    for p in manifest["cache_pq"]:
        try:
            parts.append(pd.read_parquet(p))
        except Exception:
            continue

    if not parts:
        raise ValueError(msg["no_data"])

    result = _merge_pollutant_frames(parts)
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    n_mun = result["code_muni"].nunique()
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["agg_done"].format(n_rows=n_rows, n_mun=n_mun)
        )

    now = datetime.now()
    # Preserved R quirk: the Parquet-cache early-return path builds a
    # slimmer sus_meta than the full-extraction path (no resolution/
    # pollutants/agg_fun/n_municipalities, distinct temporal.source, no
    # temporal.unit) — mirrors .ghap_build_result_from_parquet() exactly.
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "pollution_ghap",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "source": "zenodo_ghap_cache",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_pollution_ghap(): from Parquet cache, {n_rows} obs"
        ],
        "user": {},
    }
    return result
