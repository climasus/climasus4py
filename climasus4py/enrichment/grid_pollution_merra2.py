"""grid_pollution_merra2.py — NASA MERRA-2 aerosol/pollution data for Brazilian municipalities.

Mirrors R: sus_grid_pollution_merra2.R

MERRA-2 (Modern-Era Retrospective Analysis for Research and Applications,
Version 2; Gelaro et al., 2017) from NASA GMAO provides global atmospheric
reanalysis from 1980 to present at ~0.625x0.5 degree (~55 km) resolution.
It is the longest available reanalysis record for aerosol/pollution
studies. Data are served by NASA GES DISC and require a free Earthdata
Login account (see the ``earthdata_user``/``earthdata_pass``/``netrc_path``
parameters below).

Downloads MERRA-2 NetCDF-4 files, derives one of three pollutant
variables (PM2.5, AOD, or experimental SO2) from their raw aerosol
component fields, spatially aggregates to municipality polygons via
zonal statistics using ``exactextract`` (the literal Python binding for
the same isciences C++ engine that R's ``exactextractr`` wraps — not
``rasterstats``, whose default zonal-stats masking is only an
approximation of the fractional pixel-polygon overlap weighting both
``exactextractr`` and ``exactextract`` compute), and caches both the raw
NetCDF files and the per-(pollutant, year, month) zonal-stats results
(Parquet).

Not lazy — raster I/O and zonal statistics are fundamentally
geometry/row-oriented work with no natural DuckDB SQL expression, and
the R source itself never routes this through Arrow/DuckDB either (it
works directly with ``terra``/``exactextractr`` and returns a
materialised tibble). The Python port mirrors that: results are built
as a municipality x date ``pd.DataFrame`` with metadata attached via
``df.attrs["sus_meta"]``.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from datetime import datetime
from functools import reduce
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd
from rich.console import Console

if TYPE_CHECKING:
    import geopandas as gpd

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Renamed from R's "~/.climasus4r_cache/merra2", mirroring the established
# climasus4py_cache rename already used by every sibling sus_grid_* port.
_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "merra2"

_VALID_RESOLUTIONS: tuple[str, ...] = ("monthly", "daily")
_VALID_AGG: tuple[str, ...] = ("mean", "sum", "median", "min", "max")
_MIN_YEAR = 1980

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

_ALL_MONTHS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

# terra::ext(-75, -28, -35, 6) is (xmin, xmax, ymin, ymax); stored below as
# (minx, miny, maxx, maxy) to match rioxarray's clip_box() argument order.
_BRAZIL_BBOX: tuple[float, float, float, float] = (-75.0, -35.0, -28.0, 6.0)

_GES_DISC_BASE = "https://data.gesdisc.earthdata.nasa.gov/data/MERRA2"

# Variable definitions: pollutant alias -> raw NetCDF variables, output
# column name, and derivation formula. Verified from sus_grid_pollution_merra2.R
# (.merra2_var_map). Units: kg/m3 raw MERRA-2 aerosol mass fields are
# multiplied by 1e9 to convert to ug/m3.
_VAR_MAP: dict[str, dict] = {
    "pm25": {
        "nc_vars": ("DUSMASS25", "SSSMASS25", "BCSMASS", "OCSMASS", "SO4SMASS"),
        "out_col": "pm25_merra2",
        "derive": lambda d: (
            d["DUSMASS25"] + d["SSSMASS25"] + d["BCSMASS"]
            + 1.4 * d["OCSMASS"] + d["SO4SMASS"]
        ) * 1e9,
        "unit": "ug/m3",
    },
    "aod": {
        "nc_vars": ("TOTEXTTAU",),
        "out_col": "aod_merra2",
        "derive": lambda d: d["TOTEXTTAU"],
        "unit": "dimensionless",
    },
    "so2": {
        "nc_vars": ("SO2SMASS",),
        "out_col": "so2_merra2",
        "derive": lambda d: d["SO2SMASS"] * 1e9,
        "unit": "ug/m3",
    },
}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados MERRA-2 de Poluição Atmosférica (NASA GMAO)",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "no_pollutants": "Nenhum poluente válido selecionado.",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "so2_monthly_only": (
            "SO2 só disponível em resolution='monthly'. Removido da lista."
        ),
        "invalid_years_type": "'years' deve ser numérico sem NA.",
        "invalid_years_range": (
            "'years' deve estar entre 1980 e {max_year}. Ano(s) inválido(s): {bad}."
        ),
        "default_years": "Usando anos {years} (padrão: últimos 2 anos completos).",
        "latency_warn": (
            "MERRA-2 tem ~3 semanas de latência. Dados do ano corrente podem "
            "estar incompletos."
        ),
        "invalid_months": "'months' deve ser inteiro entre 1 e 12.",
        "need_geopandas": "O pacote geopandas é necessário para agregar por municípios.",
        "muni_not_geodataframe": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opções: {valid}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "no_auth": (
            "Credenciais Earthdata não encontradas.\n"
            "  Configure via variáveis de ambiente: EARTHDATA_USER e "
            "EARTHDATA_PASSWORD\n"
            "  ou crie um arquivo .netrc e informe seu caminho em 'netrc_path'.\n"
            "  Cadastre-se em: https://urs.earthdata.nasa.gov e ative o acesso "
            "GES DISC em: https://disc.gsfc.nasa.gov/earthdata-login"
        ),
        "no_data_to_download": "Nenhum dado disponível para os parâmetros fornecidos.",
        "download_start": "Baixando {n_files} arquivo(s) MERRA-2 do GES DISC...",
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
        "done_paths": "{n} arquivo(s) NetCDF disponível(is) no cache.",
    },
    "en": {
        "title": "MERRA-2 Atmospheric Pollution Data (NASA GMAO)",
        "invalid_pollutants": "Invalid 'pollutants': {bad}. Use: {valid}.",
        "no_pollutants": "No valid pollutants selected.",
        "invalid_resolution": "Invalid 'resolution': {bad}. Use: {valid}.",
        "so2_monthly_only": (
            "SO2 is only available with resolution='monthly'. Removed from list."
        ),
        "invalid_years_type": "'years' must be numeric without NA.",
        "invalid_years_range": (
            "'years' must be between 1980 and {max_year}. Invalid year(s): {bad}."
        ),
        "default_years": "Using years {years} (default: last 2 complete years).",
        "latency_warn": (
            "MERRA-2 has ~3-week latency. Data for the current year may be "
            "incomplete."
        ),
        "invalid_months": "'months' must be integer between 1 and 12.",
        "need_geopandas": "The geopandas package is required to aggregate by municipality.",
        "muni_not_geodataframe": "'municipalities' must be a geopandas.GeoDataFrame.",
        "invalid_agg": "Invalid 'agg_fun': {bad}. Options: {valid}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "no_auth": (
            "Earthdata credentials not found.\n"
            "  Set environment variables: EARTHDATA_USER and EARTHDATA_PASSWORD\n"
            "  or create a .netrc file and pass its path via 'netrc_path'.\n"
            "  Register at: https://urs.earthdata.nasa.gov and activate GES DISC "
            "access at: https://disc.gsfc.nasa.gov/earthdata-login"
        ),
        "no_data_to_download": "No data available for the provided parameters.",
        "download_start": "Downloading {n_files} MERRA-2 file(s) from GES DISC...",
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
        "done_paths": "{n} NetCDF file(s) available in cache.",
    },
    "es": {
        "title": "Datos MERRA-2 de Contaminación Atmosférica (NASA GMAO)",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "no_pollutants": "Ningún contaminante válido seleccionado.",
        "invalid_resolution": "'resolution' inválido: {bad}. Use: {valid}.",
        "so2_monthly_only": (
            "SO2 solo disponible con resolution='monthly'. Eliminado de la lista."
        ),
        "invalid_years_type": "'years' debe ser numérico sin NA.",
        "invalid_years_range": (
            "'years' debe estar entre 1980 y {max_year}. Año(s) inválido(s): {bad}."
        ),
        "default_years": "Usando años {years} (por defecto: últimos 2 años completos).",
        "latency_warn": (
            "MERRA-2 tiene ~3 semanas de latencia. Los datos del año actual "
            "pueden estar incompletos."
        ),
        "invalid_months": "'months' debe ser entero entre 1 y 12.",
        "need_geopandas": "El paquete geopandas es necesario para agregar por municipios.",
        "muni_not_geodataframe": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opciones: {valid}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "no_auth": (
            "Credenciales Earthdata no encontradas.\n"
            "  Configure las variables de entorno: EARTHDATA_USER y "
            "EARTHDATA_PASSWORD\n"
            "  o cree un archivo .netrc e indique su ruta en 'netrc_path'.\n"
            "  Regístrese en: https://urs.earthdata.nasa.gov y active el acceso "
            "GES DISC en: https://disc.gsfc.nasa.gov/earthdata-login"
        ),
        "no_data_to_download": "No hay datos disponibles para los parámetros indicados.",
        "download_start": "Descargando {n_files} archivo(s) MERRA-2 de GES DISC...",
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
        "done_paths": "{n} archivo(s) NetCDF disponible(s) en caché.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_pollution_merra2(
    pollutants: list[str] | tuple[str, ...] = ("pm25", "aod"),
    resolution: Literal["monthly", "daily"] = "monthly",
    years: int | list[int] | None = None,
    months: list[int] | tuple[int, ...] = _ALL_MONTHS,
    municipalities: gpd.GeoDataFrame | None = None,
    agg_fun: Literal["mean", "sum", "median", "min", "max"] = "mean",
    earthdata_user: str | None = None,
    earthdata_pass: str | None = None,
    netrc_path: str | Path | None = None,
    crop_brazil: bool = True,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame | dict[str, str]:
    """Import NASA MERRA-2 aerosol/pollution data for Brazilian municipalities.

    Downloads, spatially aggregates, and caches NASA MERRA-2 aerosol and
    air quality data. MERRA-2 (Modern-Era Retrospective Analysis for
    Research and Applications, Version 2) from NASA GMAO provides global
    atmospheric reanalysis from 1980 to present at ~0.625x0.5 degree
    (~55 km) resolution — the longest available reanalysis record for
    aerosol/pollution studies, useful for historical trend analysis and
    cross-validation with higher-resolution products such as CAMS or
    GHAP.

    Supported variables:
        - ``"pm25"``: derived from aerosol mass components (ug/m3):
          ``DUSMASS25 + SSSMASS25 + BCSMASS + 1.4*OCSMASS + SO4SMASS``,
          each multiplied by 1e9 to convert kg/m3 to ug/m3.
        - ``"aod"``: total aerosol optical depth at 550 nm
          (``TOTEXTTAU``), dimensionless.
        - ``"so2"`` (experimental): sulfate surface mass concentration
          from M2I3NVAER (ug/m3). Only available with
          ``resolution="monthly"``.

    Authentication:
        A free NASA Earthdata Login account is required
        (https://urs.earthdata.nasa.gov). Provide credentials either via
        the ``EARTHDATA_USER``/``EARTHDATA_PASSWORD`` environment
        variables (or the ``earthdata_user``/``earthdata_pass``
        arguments), or via a ``.netrc`` file passed through
        ``netrc_path`` (takes precedence over the user/pass arguments).
        After registering, activate GES DISC access at
        https://disc.gsfc.nasa.gov/earthdata-login.

    Args:
        pollutants: Variables to download. Allowed: ``"pm25"``,
            ``"aod"``, ``"so2"``. Default ``("pm25", "aod")``.
        resolution: Temporal resolution of source files. ``"monthly"``
            (default) uses the M2TMNXAER monthly-mean collection.
            ``"daily"`` uses M2T1NXAER; ``so2`` is always forced to
            monthly (a warning is emitted and it is dropped from
            *pollutants* if ``resolution="daily"``).
        years: Year(s) to download (1980 to current year). ``None``
            (default) uses the last two complete years.
        months: Months (1-12) to include. Default: all 12 months.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from ``climasus-data`` municipality
            boundaries). When provided, rasters are aggregated and a
            ``pd.DataFrame`` is returned. If ``None``, a dict mapping
            filename -> cached NetCDF file path is returned instead (no
            spatial aggregation).
        agg_fun: Spatial aggregation function for ``exactextract``.
            Default ``"mean"`` (area-weighted mean).
        earthdata_user: Earthdata username. Defaults to the
            ``EARTHDATA_USER`` environment variable (resolved at call
            time, not at import time).
        earthdata_pass: Earthdata password. Defaults to the
            ``EARTHDATA_PASSWORD`` environment variable (resolved at
            call time, not at import time).
        netrc_path: Path to a ``.netrc`` file with Earthdata
            credentials. If provided, takes precedence over
            *earthdata_user*/*earthdata_pass*.
        crop_brazil: Crop global rasters to Brazil's bounding box before
            aggregation. Reduces memory usage significantly. Default
            ``True``.
        use_cache: Reuse previously downloaded NetCDF files and
            aggregated Parquet caches. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/merra2``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni`` (str), ``date`` (datetime, one row per requested
        month), and one column per pollutant (e.g. ``pm25_merra2``,
        ``aod_merra2``). Metadata in ``df.attrs["sus_meta"]``
        (``stage="climate"``, ``type="pollution_merra2"``). If
        *municipalities* is ``None``: a dict mapping each cached NetCDF
        filename to its local path.

    Raises:
        ValueError: If any parameter is invalid, Earthdata credentials
            are missing, or no data could be extracted for the given
            parameters.
        ImportError: If ``geopandas``, ``rioxarray``/``xarray``, or
            ``exactextract`` are required but not installed.

    Data source:
        Gelaro, R. et al. (2017). The Modern-Era Retrospective Analysis
        for Research and Applications, Version 2 (MERRA-2). Journal of
        Climate, 30(14), 5419-5454. https://doi.org/10.1175/JCLI-D-16-0758.1
        NASA/GSFC/EPS GMAO. MERRA-2 tavgM_2d_aer_Nx (M2TMNXAER v5.12.4).
        NASA Goddard Earth Sciences DISC.
        Data: https://data.gesdisc.earthdata.nasa.gov/data/MERRA2/

    Examples::

        import os
        os.environ["EARTHDATA_USER"] = "my_user"
        os.environ["EARTHDATA_PASSWORD"] = "my_pass"

        import climasus4py as cs
        merra2 = cs.sus_grid_pollution_merra2(
            pollutants=["pm25", "aod"], resolution="monthly",
            years=2020, months=[1, 2, 3], municipalities=mt_mun, lang="pt",
        )
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- pollutants -------------------------------------------------------
    valid_pollutants = tuple(_VAR_MAP)
    pollutants_list = list(pollutants) if not isinstance(pollutants, str) else [pollutants]
    if not pollutants_list or any(p not in valid_pollutants for p in pollutants_list):
        bad = ", ".join(sorted(set(pollutants_list) - set(valid_pollutants)))
        raise ValueError(
            msg["invalid_pollutants"].format(bad=bad, valid=", ".join(valid_pollutants))
        )

    # --- resolution -----------------------------------------------------------
    if resolution not in _VALID_RESOLUTIONS:
        raise ValueError(
            msg["invalid_resolution"].format(
                bad=resolution, valid=", ".join(_VALID_RESOLUTIONS)
            )
        )

    # SO2 only available monthly. Unconditional warning (not gated by
    # `verbose`), mirroring the R source's cli_alert_warning() call here.
    if "so2" in pollutants_list and resolution == "daily":
        console.print("[yellow]WARN[/]  " + msg["so2_monthly_only"])
        pollutants_list = [p for p in pollutants_list if p != "so2"]
        if not pollutants_list:
            raise ValueError(msg["no_pollutants"])

    # --- years ------------------------------------------------------------------
    current_year = datetime.now().year
    if years is None:
        years_list = [current_year - 2, current_year - 1]
        if verbose:
            console.print(
                "[cyan]INFO[/]  "
                + msg["default_years"].format(years=f"{years_list[0]}-{years_list[-1]}")
            )
    else:
        raw_years = [years] if isinstance(years, int) else list(years)
        try:
            years_list = sorted({int(y) for y in raw_years})
        except (TypeError, ValueError) as exc:
            raise ValueError(msg["invalid_years_type"]) from exc
        bad_years = [y for y in years_list if y < _MIN_YEAR or y > current_year]
        if bad_years:
            raise ValueError(
                msg["invalid_years_range"].format(
                    max_year=current_year, bad=", ".join(str(y) for y in bad_years)
                )
            )
        # Unconditional warning (not gated by `verbose`), mirroring the R source.
        if current_year in years_list:
            console.print("[yellow]WARN[/]  " + msg["latency_warn"])

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
        # check_installed("exactextractr") calls: fail fast, before
        # downloading any raster, rather than only at the first
        # _read_nc_var()/_zonal_stats() call after a (possibly large)
        # download loop has already run.
        for pkg in ("rioxarray", "xarray", "exactextract"):
            if find_spec(pkg) is None:
                raise ImportError(
                    f"{pkg} is required to aggregate MERRA-2 rasters to "
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

    # --- authentication check ---------------------------------------------------
    # Resolve env-var defaults at call time (not at def time), matching the
    # R source's Sys.getenv() being evaluated on every call.
    resolved_user = earthdata_user if earthdata_user is not None else os.environ.get(
        "EARTHDATA_USER", ""
    )
    resolved_pass = earthdata_pass if earthdata_pass is not None else os.environ.get(
        "EARTHDATA_PASSWORD", ""
    )
    _merra2_check_auth(resolved_user, resolved_pass, netrc_path, msg)

    # Parquet cache key: one municipality set can differ between calls, so a
    # hash of the sorted code_muni values disambiguates caches per call
    # (mirrors the muni_hash fix already applied in sus_grid_chirps/sus_grid_pdsi
    # ports; the R source here has no equivalent disambiguation at all — see
    # IDEIAS.md, same class of silent-cache-collision bug as sus_grid_pdsi.R).
    muni_hash = ""
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities)
        codes = sorted(str(c) for c in municipalities[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

    # --- build download manifest ----------------------------------------------
    manifest = _build_manifest(
        pollutants_list, resolution, years_list, months_list, cache_path, muni_hash
    )
    if manifest.empty:
        raise ValueError(msg["no_data_to_download"])

    unique_nc = manifest.drop_duplicates(subset=["filename"])[["filename", "url", "cache_nc"]]
    n_files = len(unique_nc)
    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files))

    # --- Parquet early-return -------------------------------------------------
    unique_pq = manifest["cache_pq"].unique().tolist()
    if municipalities is not None and use_cache and all(Path(p).is_file() for p in unique_pq):
        if verbose:
            console.print("[green]OK[/]  " + msg["parquet_cache_hit"])
        return _build_from_parquet(manifest, pollutants_list, verbose, msg)

    # --- download NetCDF files with Earthdata auth -----------------------------
    for row in unique_nc.itertuples(index=False):
        _merra2_download_file(
            row.url, Path(row.cache_nc), use_cache,
            resolved_user, resolved_pass, netrc_path, verbose, msg,
        )

    # --- return file paths if no municipalities ---------------------------------
    if municipalities is None:
        paths = {row.filename: str(row.cache_nc) for row in unique_nc.itertuples(index=False)}
        if verbose:
            console.print("[green]OK[/]  " + msg["done_paths"].format(n=len(paths)))
        return paths

    # --- prepare municipalities ---------------------------------------------------
    muni_col = _detect_muni_col(municipalities)
    muni = municipalities.copy()
    muni["code_muni"] = muni[muni_col].astype(str).str.slice(0, 7)
    muni = muni.to_crs(epsg=4326)
    n_mun = len(muni)

    bbox = _BRAZIL_BBOX if crop_brazil else None

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["agg_start"].format(n_mun=n_mun))

    # --- extract and aggregate per manifest row ---------------------------------
    frames_by_pollutant: dict[str, list[pd.DataFrame]] = {p: [] for p in pollutants_list}

    for row in manifest.itertuples(index=False):
        pq_path = Path(row.cache_pq)

        if use_cache and pq_path.is_file() and pq_path.stat().st_size > 0:
            if verbose:
                console.print(
                    "[green]OK[/]  " + msg["parquet_hit"].format(filename=pq_path.name)
                )
            frames_by_pollutant[row.pollutant].append(pd.read_parquet(pq_path))
            continue

        nc_path = Path(row.cache_nc)
        if not nc_path.is_file() or nc_path.stat().st_size == 0:
            console.print(
                "[yellow]WARN[/]  " + msg["skip_missing"].format(filename=row.filename)
            )
            continue

        df_row = _process_manifest_row(row, muni, bbox, agg_fun, msg)
        if df_row is None or df_row.empty:
            continue

        pq_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df_row.to_parquet(pq_path, index=False)
        except Exception:
            if verbose:
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["parquet_write_warn"].format(filename=pq_path.name)
                )
        frames_by_pollutant[row.pollutant].append(df_row)

    result = _merge_pollutant_frames(frames_by_pollutant, pollutants_list, msg)

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
        "type": "pollution_merra2",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "unit": "day" if resolution == "daily" else "month",
            "source": "nasa_merra2",
            "collection": "M2T1NXAER" if resolution == "daily" else "M2TMNXAER",
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
            f"sus_grid_pollution_merra2(): {len(pollutants_list)} pollutant(s), "
            f"res={resolution}, {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: filename / URL construction (mirrors .merra2_file_info)
# ---------------------------------------------------------------------------

def _merra2_version(year: int) -> str:
    """Return the MERRA-2 processing-stream version code for *year*."""
    if year < 1992:
        return "100"
    if year < 2001:
        return "200"
    if year < 2011:
        return "300"
    return "400"


def _merra2_file_info(pollutant: str, resolution: str, year: int, month: int) -> tuple[str, str]:
    """Build the MERRA-2 filename and GES DISC download URL for one manifest row.

    Monthly and daily resolutions build an identical filename/URL for
    non-SO2 pollutants (both use the M2TMNXAER monthly-mean collection) —
    this is a preserved quirk of the R source (``.merra2_file_info()``'s
    ``daily`` branch never actually points at the hourly M2T1NXAER
    collection despite the parameter documentation implying it does; see
    IDEIAS.md). SO2 always uses the M2I3NVAER monthly-mean collection.
    """
    ver = _merra2_version(year)
    if pollutant == "so2":
        fname = f"MERRA2_{ver}.tavgM_2d_aer_Nv.{year:04d}{month:02d}01.nc4"
        base = f"{_GES_DISC_BASE}/M2I3NVAER.5.12.4/{year:04d}/"
    else:
        fname = f"MERRA2_{ver}.tavgM_2d_aer_Nx.{year:04d}{month:02d}01.nc4"
        base = f"{_GES_DISC_BASE}/M2TMNXAER.5.12.4/{year:04d}/"
    return fname, base + fname


# ---------------------------------------------------------------------------
# Internal: download manifest
# ---------------------------------------------------------------------------

def _build_manifest(
    pollutants: list[str],
    resolution: str,
    years: list[int],
    months: list[int],
    cache_path: Path,
    muni_hash: str,
) -> pd.DataFrame:
    """Build one row per (pollutant, year, month) to download/process."""
    rows: list[dict] = []
    for p in pollutants:
        p_res = "monthly" if p == "so2" else resolution  # SO2 forced monthly
        out_col = _VAR_MAP[p]["out_col"]
        for yr in years:
            for mo in months:
                filename, url = _merra2_file_info(p, p_res, yr, mo)
                pq_path = (
                    cache_path / "parquet"
                    / f"{p}_{p_res}_{yr:04d}{mo:02d}{muni_hash}.parquet"
                )
                rows.append({
                    "pollutant": p, "resolution": p_res, "year": yr, "month": mo,
                    "filename": filename, "url": url, "out_col": out_col,
                    "cache_nc": cache_path / "nc" / filename,
                    "cache_pq": pq_path,
                    "date": pd.Timestamp(year=yr, month=mo, day=1),
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Internal: municipality identifier column detection
# ---------------------------------------------------------------------------

def _detect_muni_col(municipalities: gpd.GeoDataFrame) -> str:
    """Auto-detect the municipality identifier column in a GeoDataFrame."""
    cols = list(municipalities.columns)
    for candidate in _MUNI_COL_CANDIDATES:
        if candidate in cols:
            return candidate
    geom_name = getattr(municipalities, "geometry", None)
    geom_col = geom_name.name if geom_name is not None else None
    for col in cols:
        if col == geom_col:
            continue
        sample = municipalities[col].dropna().astype(str).iloc[:5]
        if len(sample) > 0 and sample.str.match(r"^\d{6,7}$").all():
            return col
    raise ValueError(
        "Could not detect a municipality identifier column. Expected one of: "
        f"{', '.join(_MUNI_COL_CANDIDATES)}."
    )


# ---------------------------------------------------------------------------
# Internal: Earthdata authentication check
# ---------------------------------------------------------------------------

def _merra2_check_auth(
    user: str, password: str, netrc_path: str | Path | None, msg: dict[str, str]
) -> None:
    """Validate Earthdata credentials; raise with setup instructions if missing."""
    has_netrc = bool(netrc_path) and Path(netrc_path).is_file()
    has_creds = bool(user) and bool(password)
    if not has_netrc and not has_creds:
        raise ValueError(msg["no_auth"])


# ---------------------------------------------------------------------------
# Internal: download one MERRA-2 NetCDF file with Earthdata authentication
# ---------------------------------------------------------------------------

def _merra2_download_file(
    url: str,
    cache_path: Path,
    use_cache: bool,
    earthdata_user: str,
    earthdata_pass: str,
    netrc_path: str | Path | None,
    verbose: bool,
    msg: dict[str, str],
) -> None:
    """Download one MERRA-2 NetCDF file to *cache_path*, reusing the cache.

    Never raises: download failures are logged as warnings and left for
    the per-row extraction step to skip via a missing-file check,
    mirroring the R source's ``.merra2_download_file()``.
    """
    filename = cache_path.name
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        if verbose:
            console.print("[green]OK[/]  " + msg["cache_hit"].format(filename=filename))
        return

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["download_file"].format(filename=filename))

    ok, reason = _merra2_download_raw(
        url, cache_path, netrc_path, earthdata_user, earthdata_pass
    )
    if ok:
        if verbose:
            console.print("[green]OK[/]  " + msg["download_done"].format(filename=filename))
    else:
        cache_path.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(filename=filename, err=reason or "unknown error")
        )


def _merra2_download_raw(
    url: str,
    dest: Path,
    netrc_path: str | Path | None,
    user: str,
    password: str,
) -> tuple[bool, str | None]:
    """Perform the actual authenticated GET, mirroring the R source's two auth paths.

    - If ``netrc_path`` is given: a system ``curl`` subprocess with
      ``--netrc-file``, mirroring R's ``utils::download.file(method="curl",
      extra="--netrc-file ...")``.
    - Otherwise: HTTP Basic Auth via ``requests``, mirroring R's
      ``httr2::req_auth_basic()`` + ``req_options(followlocation = TRUE)``.

    Note: NASA GES DISC's Earthdata Login redirects across hosts
    (data.gesdisc.earthdata.nasa.gov -> urs.earthdata.nasa.gov and back).
    Both ``requests`` and R's ``httr2`` drop the ``Authorization`` header
    on a cross-host redirect for security, so plain Basic Auth without a
    session/cookie-jar can still fail depending on the exact redirect
    chain — this is a known limitation of both source implementations,
    not something invented here. See IDEIAS.md.
    """
    tmp = Path(str(dest) + ".tmp")
    has_netrc = bool(netrc_path) and Path(netrc_path).is_file()

    if has_netrc:
        curl_bin = shutil.which("curl")
        if not curl_bin:
            return False, "curl binary required for netrc_path authentication but not found"
        try:
            result = subprocess.run(
                [
                    curl_bin, "--silent", "--show-error", "--location",
                    "--netrc-file", str(netrc_path),
                    "--max-time", "3600",
                    "--write-out", "%{http_code}",
                    "--output", str(tmp), url,
                ],
                capture_output=True, text=True,
            )
            http_code = int(result.stdout.strip() or "0")
            if result.returncode == 0 and http_code == 200 and tmp.exists() and tmp.stat().st_size > 0:  # noqa: E501
                tmp.rename(dest)
                return True, None
            return False, f"HTTP {http_code}" if http_code else (result.stderr or "curl error")
        except Exception as e:
            return False, str(e)

    try:
        import requests  # type: ignore[import-untyped]
    except ImportError:
        return False, "the 'requests' package is required for Earthdata Basic Auth downloads"

    try:
        with requests.Session() as session:
            resp = session.get(
                url, auth=(user, password), allow_redirects=True, stream=True, timeout=3600
            )
            if resp.status_code == 200:
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                if tmp.exists() and tmp.stat().st_size > 0:
                    tmp.rename(dest)
                    return True, None
            return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Internal: raster read + zonal statistics
# ---------------------------------------------------------------------------

def _read_nc_var(nc_path: Path, var_name: str, bbox, p_res: str, agg_fun: str):
    """Open one MERRA-2 NetCDF variable as a 2-D ``xarray.DataArray``.

    Reduces the time dimension: for ``resolution="daily"`` this mirrors
    the R source's ``terra::app(..., fun = agg_fun)`` collapse of hourly
    layers into a single daily value; for ``"monthly"`` it takes the
    (single) time layer directly. Because the daily and monthly branches
    of ``.merra2_file_info()`` fetch the identical monthly-mean file
    (see ``_merra2_file_info``), the "hourly collapse" is always a no-op
    over a single time step in practice — preserved rather than fixed
    (see IDEIAS.md).
    """
    import rioxarray  # noqa: F401 (registers the .rio accessor)
    import xarray as xr

    ds = xr.open_dataset(nc_path, decode_coords="all")
    if var_name not in ds.variables:
        raise ValueError(f"Variable '{var_name}' not found in {nc_path.name}.")
    da = ds[var_name]

    x_dim, y_dim = ("lon", "lat") if "lon" in da.dims and "lat" in da.dims else (None, None)

    # rioxarray's spatial-dims/CRS tracking lives on the accessor instance, not
    # on the array's attrs, so it does not survive `.mean()`/`.isel()` below —
    # reapply after every dim-reducing operation rather than once up front.
    def _tag_spatial(arr):
        if x_dim is not None:
            arr = arr.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)
        if arr.rio.crs is None:
            arr = arr.rio.write_crs("EPSG:4326")
        return arr

    da = _tag_spatial(da)

    if "time" in da.dims:
        if p_res == "daily":
            da = da.sum(dim="time") if agg_fun == "sum" else da.mean(dim="time")
        else:
            da = da.isel(time=0)
        da = _tag_spatial(da)

    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        da = da.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
        da = _tag_spatial(da)

    return da


def _zonal_stats(raster, municipalities: gpd.GeoDataFrame, agg_fun: str) -> np.ndarray:
    """Compute per-polygon zonal statistics with ``exactextract``.

    Uses the ``exactextract`` PyPI package (the Python binding for the
    same isciences C++ engine R's ``exactextractr`` wraps), not
    ``rasterstats`` — see the module docstring.
    """
    from exactextract import exact_extract

    stats = exact_extract(raster, municipalities, [agg_fun], output="pandas")
    col = agg_fun if agg_fun in stats.columns else stats.columns[-1]
    return stats[col].to_numpy()


# ---------------------------------------------------------------------------
# Internal: process one manifest row (one pollutant, one year/month)
# ---------------------------------------------------------------------------

def _process_manifest_row(
    row,
    municipalities: gpd.GeoDataFrame,
    bbox,
    agg_fun: str,
    msg: dict[str, str],
) -> pd.DataFrame | None:
    """Extract every raw NC variable for one pollutant, derive, aggregate."""
    try:
        vmap = _VAR_MAP[row.pollutant]
        comp: dict[str, np.ndarray] = {"code_muni": municipalities["code_muni"].to_numpy()}
        for v in vmap["nc_vars"]:
            da = _read_nc_var(Path(row.cache_nc), v, bbox, row.resolution, agg_fun)
            comp[v] = _zonal_stats(da, municipalities, agg_fun)

        comp_df = pd.DataFrame(comp)
        derived = vmap["derive"](comp_df)

        out = pd.DataFrame({
            "code_muni": comp_df["code_muni"],
            "date": row.date,
        })
        out[vmap["out_col"]] = pd.array(np.asarray(derived, dtype="float64"), dtype="float64")
        return out
    except ImportError:
        raise
    except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
        console.print(
            "[yellow]WARN[/]  " + msg["extract_warn"].format(filename=row.filename, err=str(e))
        )
        return None


# ---------------------------------------------------------------------------
# Internal: merge per-pollutant frames into one code_muni x date table
# ---------------------------------------------------------------------------

def _merge_pollutant_frames(
    frames_by_pollutant: dict[str, list[pd.DataFrame]],
    pollutants: list[str],
    msg: dict[str, str],
) -> pd.DataFrame:
    """Concatenate rows within each pollutant, then outer-merge across pollutants.

    The R source instead ``full_join()``s the raw per-(pollutant, year,
    month) frames directly on ``(code_muni, date)`` (line 433 of
    ``sus_grid_pollution_merra2.R``): for two months of the *same*
    pollutant the join keys never match, so dplyr's default
    ``.x``/``.y`` suffixing kicks in and the documented single-column
    return contract (one ``<pollutant>_merra2`` column) is not actually
    honoured by the R implementation once more than one month is
    requested. This is corrected here — concat within a pollutant first,
    then merge across pollutants — to match this port's documented
    return contract (and the R docstring's own stated contract); see
    IDEIAS.md.
    """
    per_pollutant: list[pd.DataFrame] = []
    for p in pollutants:
        frames = [f for f in frames_by_pollutant.get(p, []) if f is not None and not f.empty]
        if not frames:
            continue
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["code_muni", "date"], keep="last")
        per_pollutant.append(merged)

    if not per_pollutant:
        raise ValueError(msg["no_data"])

    result = reduce(
        lambda left, right: pd.merge(left, right, on=["code_muni", "date"], how="outer"),
        per_pollutant,
    )
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# Internal: assemble result from pre-existing Parquet caches (early return)
# ---------------------------------------------------------------------------

def _build_from_parquet(
    manifest: pd.DataFrame, pollutants: list[str], verbose: bool, msg: dict[str, str]
) -> pd.DataFrame:
    """Assemble the output DataFrame purely from cached Parquet files.

    Grouped by pollutant before merging, same as the main code path (see
    ``_merge_pollutant_frames``) — a naive ``pd.concat`` across all
    Parquet files here (mirroring the R source's ``do.call(rbind,
    parts)``) would silently stack frames with different pollutant
    columns into NaN-padded duplicate ``(code_muni, date)`` rows instead
    of raising, which R's ``rbind()`` would refuse to do (mismatched
    columns error). See IDEIAS.md.
    """
    frames_by_pollutant: dict[str, list[pd.DataFrame]] = {p: [] for p in pollutants}
    for row in manifest.itertuples(index=False):
        pq = Path(row.cache_pq)
        if not pq.is_file():
            continue
        try:
            frames_by_pollutant[row.pollutant].append(pd.read_parquet(pq))
        except Exception:
            continue

    result = _merge_pollutant_frames(frames_by_pollutant, pollutants, msg)

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
        "type": "pollution_merra2",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "source": "nasa_merra2_cache",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "pollutants": pollutants,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_pollution_merra2(): from Parquet cache, {n_rows} obs"
        ],
        "user": {},
    }
    return result
