"""grid_era5.py — ERA5-Land daily climate aggregates for Latin America.

Mirrors R: sus_grid_era5.R

Downloads pre-processed ERA5-Land daily aggregates (Saldanha,
rfsaldanha.github.io — ~10 km resolution, 1950-2025) hosted on Zenodo as
one NetCDF file per (indicator x year-month x aggregation), one file per
call to ``_ERA5_ZENODO_IDS[year]``, and optionally aggregates each file to
Brazilian municipality polygons with area-weighted zonal statistics.

Not lazy: the R source itself always materialises to a tibble at the end
(no Arrow/DuckDB relation involved), and the raster extraction step
(``exactextract`` over per-day NetCDF layers) has no natural SQL
expression. The public function always returns a ``pandas.DataFrame``
(when ``municipalities`` is given) with metadata in
``df.attrs["sus_meta"]``, or a plain ``dict`` of downloaded file paths
(when ``municipalities`` is ``None``) — matching the R source's return
contract exactly.
"""

from __future__ import annotations

import calendar
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from functools import reduce
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from rich.console import Console

from ..core.climate_inmet import _download_robust

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "era5"

# Data source cited in the R source's @section Data Source block, quoted
# verbatim (concept DOI for the whole "ERA5-Land Daily Aggregates for Latin
# America" project — individual per-year deposits use the record IDs below).
_ERA5_CONCEPT_DOI = "10.5281/zenodo.10013254"

_VALID_AGG_FUN: tuple[str, ...] = (
    "mean", "sum", "median", "min", "max", "majority", "minority", "count", "variety",
)

# Zenodo record IDs for ERA5-Land Latin America, by year (1950-2025).
# Transcribed verbatim from climasus4r's .era5_zenodo_ids.
_ERA5_ZENODO_IDS: dict[int, int] = {
    1950: 10013255, 1951: 10013696, 1952: 10013781,
    1953: 10014198, 1954: 10014369, 1955: 10014474,
    1956: 10014693, 1957: 10014722, 1958: 10014754,
    1959: 10014771, 1960: 10014790, 1961: 10020497,
    1962: 10020520, 1963: 10020530, 1964: 10020539,
    1965: 10020552, 1966: 10020600, 1967: 10020663,
    1968: 10020679, 1969: 10020690, 1970: 10020859,
    1971: 10021122, 1972: 10021300, 1973: 10021667,
    1974: 10021706, 1975: 10021943, 1976: 10021943,
    1977: 10022017, 1978: 10022061, 1979: 10022145,
    1980: 10022315, 1981: 10022536, 1982: 10022546,
    1983: 10022561, 1984: 10022571, 1985: 10022579,
    1986: 10022589, 1987: 10022593, 1988: 10022607,
    1989: 10022632, 1990: 10022641, 1991: 10032814,
    1992: 10032859, 1993: 10033251, 1994: 10033276,
    1995: 10033306, 1996: 10033353, 1997: 10033755,
    1998: 10033835, 1999: 10033983, 2000: 10033995,
    2001: 10034036, 2002: 10034077, 2003: 10034110,
    2004: 10034145, 2005: 10034179, 2006: 10034204,
    2007: 10034283, 2008: 10034323, 2009: 10034370,
    2010: 10034386, 2011: 10034412, 2012: 10034443,
    2013: 10034494, 2014: 10034523, 2015: 10034541,
    2016: 10034598, 2017: 10034630, 2018: 10036123,
    2019: 10036132, 2020: 10036153, 2021: 10036162,
    2022: 10036168, 2023: 10889682, 2024: 15748090,
    2025: 18256859,
}

# Variable mapping: alias -> Zenodo indicator, aggregation label, output
# column, unit-conversion function. Transcribed verbatim from
# climasus4r's .era5_var_map.
_ERA5_VAR_MAP: dict[str, dict[str, Any]] = {
    "t2m": {
        "ind": "2m_temperature", "agg": "mean",
        "col": "tair_dry_bulb_c", "conv": lambda x: x - 273.15,
    },
    "t2m_max": {
        "ind": "2m_temperature", "agg": "max",
        "col": "tair_max_c", "conv": lambda x: x - 273.15,
    },
    "t2m_min": {
        "ind": "2m_temperature", "agg": "min",
        "col": "tair_min_c", "conv": lambda x: x - 273.15,
    },
    "td2m": {
        "ind": "2m_dewpoint_temperature", "agg": "mean",
        "col": "dew_point_c", "conv": lambda x: x - 273.15,
    },
    "u10": {
        "ind": "10m_u_component_of_wind", "agg": "mean",
        "col": "ws_10m_u_m_s", "conv": lambda x: x,
    },
    "v10": {
        "ind": "10m_v_component_of_wind", "agg": "mean",
        "col": "ws_10m_v_m_s", "conv": lambda x: x,
    },
    "sp": {
        "ind": "surface_pressure", "agg": "mean",
        "col": "patm_hpa", "conv": lambda x: x / 100,
    },
    "tp": {
        "ind": "total_precipitation", "agg": "sum",
        "col": "rainfall_mm", "conv": lambda x: x * 1000,
    },
}

_MUNI_ID_CANDIDATES: tuple[str, ...] = ("code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality")

# Module-level singleton default for `months` (parity with R's `1:12`) —
# avoids a mutable-default-style lint warning on `range(1, 13)` inline.
_ALL_MONTHS: range = range(1, 13)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "missing_years": "'years' é obrigatório.",
        "invalid_years_type": "'years' deve ser um inteiro ou uma lista de inteiros, sem NA.",
        "invalid_years_range": "Ano(s) inválido(s): {bad}. 'years' deve estar entre 1950 e 2025.",
        "invalid_months": "'months' deve ser uma lista de inteiros entre 1 e 12.",
        "invalid_vars_type": "'vars' deve ser uma string ou lista de strings.",
        "invalid_vars": "'vars' contém alias inválido(s): {bad}. Use: {valid}.",
        "need_geopandas": "O pacote 'geopandas' é necessário para agregar por municípios.",
        "municipalities_not_gdf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "need_raster_libs": (
            "Os pacotes 'rioxarray', 'xarray' e 'exactextract' são necessários "
            "para ler os arquivos NetCDF do ERA5-Land. Instale com: "
            "pip install rioxarray xarray exactextract"
        ),
        "invalid_agg": "'agg_fun' inválido: {value}. Opções válidas: {valid}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "invalid_parallel": "'parallel' deve ser True ou False.",
        "invalid_workers": "'workers' deve ser um inteiro positivo.",
        "download_start": "Baixando {n_files} arquivo(s) ERA5-Land do Zenodo...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "muni_col": "Coluna identificadora de municípios: {col}",
        "muni_col_not_found": (
            "Não foi possível detectar uma coluna identificadora de municípios em "
            "'municipalities'. Esperado: {candidates} ou uma coluna com códigos "
            "numéricos de 6 ou 7 dígitos."
        ),
        "agg_start": "Agregando para {n_mun} município(s)...",
        "agg_done": "Agregação concluída: {n_rows} observações ({n_mun} municípios).",
        "extract_warn": "Não foi possível processar {file}",
        "no_data": "Nenhum dado foi extraído com sucesso.",
        "done_paths": "{n} arquivo(s) disponível(is) no cache.",
    },
    "en": {
        "missing_years": "'years' is required.",
        "invalid_years_type": "'years' must be an int or a list of ints, without NA.",
        "invalid_years_range": "Invalid year(s): {bad}. 'years' must be between 1950 and 2025.",
        "invalid_months": "'months' must be a list of integers between 1 and 12.",
        "invalid_vars_type": "'vars' must be a string or a list of strings.",
        "invalid_vars": "'vars' contains invalid alias(es): {bad}. Use: {valid}.",
        "need_geopandas": "Package 'geopandas' is required to aggregate by municipality.",
        "municipalities_not_gdf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "need_raster_libs": (
            "Packages 'rioxarray', 'xarray', and 'exactextract' are required to read "
            "ERA5-Land NetCDF files. Install with: pip install rioxarray xarray exactextract"
        ),
        "invalid_agg": "Invalid 'agg_fun': {value}. Valid options: {valid}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "invalid_parallel": "'parallel' must be True or False.",
        "invalid_workers": "'workers' must be a positive integer.",
        "download_start": "Downloading {n_files} ERA5-Land file(s) from Zenodo...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "muni_col": "Municipality identifier column: {col}",
        "muni_col_not_found": (
            "Could not detect a municipality identifier column in 'municipalities'. "
            "Expected: {candidates} or a column with 6- or 7-digit numeric codes."
        ),
        "agg_start": "Aggregating to {n_mun} municipality/ies...",
        "agg_done": "Aggregation complete: {n_rows} observations ({n_mun} municipalities).",
        "extract_warn": "Could not process {file}",
        "no_data": "No data was successfully extracted.",
        "done_paths": "{n} file(s) available in cache.",
    },
    "es": {
        "missing_years": "'years' es obligatorio.",
        "invalid_years_type": "'years' debe ser un entero o una lista de enteros, sin NA.",
        "invalid_years_range": "Año(s) inválido(s): {bad}. 'years' debe estar entre 1950 y 2025.",
        "invalid_months": "'months' debe ser una lista de enteros entre 1 y 12.",
        "invalid_vars_type": "'vars' debe ser una cadena o lista de cadenas.",
        "invalid_vars": "'vars' contiene alias inválido(s): {bad}. Use: {valid}.",
        "need_geopandas": "El paquete 'geopandas' es necesario para agregar por municipios.",
        "municipalities_not_gdf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "need_raster_libs": (
            "Los paquetes 'rioxarray', 'xarray' y 'exactextract' son necesarios para "
            "leer los archivos NetCDF de ERA5-Land. Instale con: "
            "pip install rioxarray xarray exactextract"
        ),
        "invalid_agg": "'agg_fun' inválido: {value}. Opciones válidas: {valid}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "invalid_parallel": "'parallel' debe ser True o False.",
        "invalid_workers": "'workers' debe ser un entero positivo.",
        "download_start": "Descargando {n_files} archivo(s) ERA5-Land de Zenodo...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "muni_col": "Columna identificadora de municipios: {col}",
        "muni_col_not_found": (
            "No se pudo detectar una columna identificadora de municipios en "
            "'municipalities'. Esperado: {candidates} o una columna con códigos "
            "numéricos de 6 o 7 dígitos."
        ),
        "agg_start": "Agregando a {n_mun} municipio(s)...",
        "agg_done": "Agregación completa: {n_rows} observaciones ({n_mun} municipios).",
        "extract_warn": "No se pudo procesar {file}",
        "no_data": "No se extrajo ningún dato correctamente.",
        "done_paths": "{n} archivo(s) disponible(s) en caché.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_era5(
    years: int | list[int],
    months: list[int] | range = _ALL_MONTHS,
    vars: str | list[str] = ("t2m", "tp"),  # noqa: A002 - parity with R param name
    municipalities: Any | None = None,
    agg_fun: str = "mean",
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    parallel: bool = False,
    workers: int = 2,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame | dict[str, str | None]:
    """Import ERA5-Land daily climate aggregates for Brazilian municipalities.

    Downloads pre-processed ERA5-Land daily climate aggregates for Latin
    America from Zenodo and, optionally, spatially aggregates them to
    Brazilian municipality polygons via area-weighted zonal statistics
    (``exactextract``). No API key is required.

    The data source is the ERA5-Land Daily Aggregates for Latin America
    project (Saldanha, rfsaldanha.github.io) — NetCDF files (one per
    indicator x month x year) at ~10 km resolution for 1950-2025, hosted
    on Zenodo under CC-BY 4.0.

    Args:
        years: Year or list of years to import. Must be between 1950 and
            2025.
        months: Months to import (1-12). Default: all 12 months.
        vars: Variable alias(es) to download, or ``"all"``. Allowed:
            ``"t2m"`` (mean 2m temperature -> ``tair_dry_bulb_c``, degC),
            ``"t2m_max"`` (-> ``tair_max_c``), ``"t2m_min"``
            (-> ``tair_min_c``), ``"td2m"`` (mean dewpoint ->
            ``dew_point_c``), ``"u10"``/``"v10"`` (wind components ->
            ``ws_10m_u_m_s``/``ws_10m_v_m_s``, m/s), ``"sp"`` (surface
            pressure -> ``patm_hpa``, hPa), ``"tp"`` (daily precipitation
            sum -> ``rainfall_mm``, mm). Default: ``("t2m", "tp")``.
        municipalities: A ``geopandas.GeoDataFrame`` with municipality
            polygons (e.g. from ``geobr``/``climasus-data`` boundaries).
            Must contain a column identifying each polygon — auto-detects
            ``code_muni``, ``CD_MUN``, or ``CD_GEOCMU``. If ``None``
            (default), returns a dict of downloaded NetCDF file paths
            instead of aggregating.
        agg_fun: Spatial aggregation function applied over raster pixels
            within each polygon via ``exactextract``. One of ``"mean"``
            (default), ``"sum"``, ``"median"``, ``"min"``, ``"max"``,
            ``"majority"``, ``"minority"``, ``"count"``, ``"variety"``.
        use_cache: If ``True`` (default), downloaded NetCDF files are
            cached under *cache_dir* and reused on subsequent calls.
        cache_dir: Directory for cached NetCDF files. Default:
            ``~/.climasus4py_cache/era5``. Created automatically.
        parallel: If ``True``, downloads run concurrently via a thread
            pool. Default: ``False``.
        workers: Number of parallel download workers when
            ``parallel=True``. Default: ``2``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or
            ``"es"``.
        verbose: If ``True`` (default), prints progress messages.

    Returns:
        If *municipalities* is given: a ``pandas.DataFrame`` with columns
        ``code_muni``, ``date``, and one column per requested variable.
        Metadata in ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="era5_land"``). If *municipalities* is ``None``: a dict
        mapping ``"{year}_{month:02d}_{indicator}_{agg_label}"`` to the
        downloaded file path (or ``None`` if that file's download
        failed).

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            extracted for the requested municipalities.
        ImportError: If ``geopandas`` (when aggregating), or
            ``rioxarray``/``xarray``/``exactextract`` (always — see
            module notes) are not installed.

    Examples::

        import climasus4py as cs

        # Temperature and precipitation for a set of municipalities, Q1 2020
        era5 = cs.sus_grid_era5(
            years=2020, months=[1, 2, 3], vars=["t2m", "tp"],
            municipalities=mt_mun, lang="pt",
        )

        # All variables, no spatial aggregation (returns file paths)
        paths = cs.sus_grid_era5(years=2020, months=[1], vars="all")
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- years ---------------------------------------------------------
    if years is None:
        raise ValueError(msg["missing_years"])
    years_list = [years] if isinstance(years, int) else [int(y) for y in years]
    bad_years = sorted({y for y in years_list if y < 1950 or y > 2025})
    if bad_years:
        raise ValueError(msg["invalid_years_range"].format(bad=", ".join(map(str, bad_years))))
    years_list = sorted(set(years_list))

    # --- months ----------------------------------------------------------
    months_list = [int(m) for m in months]
    if any(m < 1 or m > 12 for m in months_list):
        raise ValueError(msg["invalid_months"])
    months_list = sorted(set(months_list))

    # --- vars --------------------------------------------------------------
    valid_aliases = list(_ERA5_VAR_MAP.keys())
    if vars == "all":
        vars_list = valid_aliases
    else:
        raw_vars = [vars] if isinstance(vars, str) else list(vars)
        if not raw_vars:
            raise ValueError(msg["invalid_vars_type"])
        bad_vars = [v for v in raw_vars if v not in _ERA5_VAR_MAP]
        if bad_vars:
            raise ValueError(
                msg["invalid_vars"].format(
                    bad=", ".join(bad_vars), valid=", ".join(valid_aliases)
                )
            )
        vars_list = raw_vars

    # --- municipalities ------------------------------------------------
    gdf = None
    if municipalities is not None:
        try:
            import geopandas as gpd
        except ImportError as exc:
            raise ImportError(msg["need_geopandas"]) from exc
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise ValueError(msg["municipalities_not_gdf"])
        try:
            from exactextract import exact_extract  # noqa: F401
        except ImportError as exc:
            raise ImportError(msg["need_raster_libs"]) from exc
        gdf = municipalities

    # --- agg_fun -------------------------------------------------------------
    if agg_fun not in _VALID_AGG_FUN:
        raise ValueError(
            msg["invalid_agg"].format(value=agg_fun, valid=", ".join(_VALID_AGG_FUN))
        )

    # --- use_cache / cache_dir --------------------------------------------
    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not isinstance(cache_dir, (str, Path)) or not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_root = Path(cache_dir).expanduser()

    # --- parallel / workers -------------------------------------------------
    if not isinstance(parallel, bool):
        raise ValueError(msg["invalid_parallel"])
    if not isinstance(workers, int) or workers < 1:
        raise ValueError(msg["invalid_workers"])

    # --- require rioxarray/xarray -----------------------------------------
    # R requires `terra` unconditionally at this point, even when
    # `municipalities` is None and only file paths are returned (no raster
    # is actually read in that branch). Preserved as-is — see IDEIAS.md.
    try:
        import rioxarray  # noqa: F401
        import xarray  # noqa: F401
    except ImportError as exc:
        raise ImportError(msg["need_raster_libs"]) from exc

    # =====================================================================
    # BUILD DOWNLOAD MANIFEST
    # =====================================================================
    manifest = _build_manifest(years_list, months_list, vars_list, cache_root)
    file_manifest = _dedupe_file_manifest(manifest)

    n_files = len(file_manifest)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['download_start'].format(n_files=n_files)}")

    # =====================================================================
    # DOWNLOAD WITH CACHE
    # =====================================================================
    if parallel and n_files > 1:
        def _dl(entry: dict[str, Any]) -> str | None:
            return _era5_download_file(entry["url"], entry["cache_path"], use_cache, verbose, msg)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            actual_paths = list(pool.map(_dl, file_manifest))
    else:
        actual_paths = [
            _era5_download_file(e["url"], e["cache_path"], use_cache, verbose, msg)
            for e in file_manifest
        ]
    for entry, path in zip(file_manifest, actual_paths, strict=True):
        entry["actual_path"] = path

    if municipalities is None:
        result_paths = {
            f"{e['year']}_{e['month']:02d}_{e['indicator']}_{e['agg_label']}": e["actual_path"]
            for e in file_manifest
        }
        if verbose:
            console.print(f"[green]OK[/]  {msg['done_paths'].format(n=len(result_paths))}")
        return result_paths

    # =====================================================================
    # DETECT MUNICIPALITY ID COLUMN, NORMALIZE, REPROJECT
    # =====================================================================
    muni_id_col = _era5_detect_muni_col(gdf, msg)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['muni_col'].format(col=muni_id_col)}")

    gdf = gdf.copy()
    gdf["code_muni"] = gdf[muni_id_col].astype(str).str.slice(0, 6)
    gdf = gdf.to_crs(epsg=4326)

    n_mun = len(gdf)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['agg_start'].format(n_mun=n_mun)}")

    # Attach actual downloaded path back onto every (year, month, alias)
    # manifest row via the deduped file lookup, mirroring the R source's
    # `file_manifest$actual_path` join back onto `manifest`.
    path_lookup = {
        (e["year"], e["month"], e["indicator"], e["agg_label"]): e["actual_path"]
        for e in file_manifest
    }
    for entry in manifest:
        entry["actual_path"] = path_lookup[
            (entry["year"], entry["month"], entry["indicator"], entry["agg_label"])
        ]

    # =====================================================================
    # EXTRACT RASTER -> POLYGONS, PER VAR ALIAS
    # =====================================================================
    alias_results: list[pd.DataFrame] = []
    for alias in vars_list:
        vmap = _ERA5_VAR_MAP[alias]
        alias_entries = [e for e in manifest if e["alias"] == alias]
        monthly_dfs: list[pd.DataFrame] = []
        for entry in alias_entries:
            if entry["actual_path"] is None:
                continue
            start_date = date(entry["year"], entry["month"], 1)
            try:
                monthly_dfs.append(
                    _era5_extract_monthly(
                        entry["actual_path"], gdf, agg_fun, vmap["col"], vmap["conv"], start_date
                    )
                )
            except Exception as exc:  # noqa: BLE001 - mirrors R's tryCatch(..., error=warn)
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["extract_warn"].format(file=Path(entry["actual_path"]).name)
                    + f": {exc}"
                )
        if monthly_dfs:
            alias_results.append(pd.concat(monthly_dfs, ignore_index=True))

    if not alias_results:
        raise ValueError(msg["no_data"])

    # =====================================================================
    # MERGE ALL VARIABLE COLUMNS (full outer join on code_muni, date)
    # =====================================================================
    result = reduce(
        lambda left, right: left.merge(right, on=["code_muni", "date"], how="outer"),
        alias_results,
    )
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["agg_done"].format(n_rows=n_rows, n_mun=n_mun)
        )

    # =====================================================================
    # METADATA
    # =====================================================================
    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "era5_land",
        "spatial": False,
        "temporal": {
            "start": result["date"].min() if n_rows else None,
            "end": result["date"].max() if n_rows else None,
            "unit": "day",
            "source": "zenodo_era5land",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "years": years_list,
        "months": months_list,
        "vars": vars_list,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "agg_fun": agg_fun,
        "doi": _ERA5_CONCEPT_DOI,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_grid_era5(): "
            f"{len(vars_list)} var(s), {n_mun} municipalities, {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: manifest construction
# ---------------------------------------------------------------------------

def _era5_nc_filename(indicator: str, year: int, month: int, agg: str) -> str:
    """Build the Zenodo filename for one indicator x year x month x aggregation."""
    last_day = calendar.monthrange(year, month)[1]
    return (
        f"{indicator}_{year:04d}-{month:02d}-01_"
        f"{year:04d}-{month:02d}-{last_day:02d}_day_{agg}.nc"
    )


def _era5_zenodo_url(year: int, filename: str) -> str:
    """Build the Zenodo download URL for one file."""
    record_id = _ERA5_ZENODO_IDS.get(year)
    if record_id is None:
        raise ValueError(f"No Zenodo record ID found for year {year}.")
    return f"https://zenodo.org/records/{record_id}/files/{filename}?download=1"


def _build_manifest(
    years: list[int], months: list[int], vars_list: list[str], cache_root: Path
) -> list[dict[str, Any]]:
    """Build one manifest row per (year, month, var alias) combination."""
    manifest = []
    for year in years:
        for month in months:
            for alias in vars_list:
                vmap = _ERA5_VAR_MAP[alias]
                filename = _era5_nc_filename(vmap["ind"], year, month, vmap["agg"])
                manifest.append({
                    "year": year,
                    "month": month,
                    "alias": alias,
                    "indicator": vmap["ind"],
                    "agg_label": vmap["agg"],
                    "out_col": vmap["col"],
                    "conv": vmap["conv"],
                    "filename": filename,
                    "url": _era5_zenodo_url(year, filename),
                    "cache_path": cache_root / str(year) / filename,
                })
    return manifest


def _dedupe_file_manifest(manifest: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by (year, month, indicator, agg_label).

    Mirrors the R source's ``unique(manifest[, c("year", "month",
    "indicator", "agg_label", "filename", "url", "cache_path")])``. Note
    the R comment above that line claims this avoids double-downloading
    a file shared by ``t2m_max``/``t2m_min`` — but since ``agg_label``
    is embedded in the Zenodo filename (``..._day_max.nc`` vs.
    ``..._day_min.nc``), those two aliases never actually share a file
    and this dedup never collapses them. It only removes exact duplicate
    rows, which arise when the same alias is repeated in ``vars``. The
    R comment appears to misdescribe its own code; the Python behavior
    faithfully mirrors the R *code* (not the incorrect comment) — see
    IDEIAS.md.
    """
    seen: dict[tuple[int, int, str, str], dict[str, Any]] = {}
    for entry in manifest:
        key = (entry["year"], entry["month"], entry["indicator"], entry["agg_label"])
        if key not in seen:
            seen[key] = entry
    return list(seen.values())


# ---------------------------------------------------------------------------
# Internal: download with cache
# ---------------------------------------------------------------------------

def _era5_download_file(
    url: str, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> str | None:
    """Download one NetCDF file, using the on-disk cache if available.

    Returns the local path as a string, or ``None`` if the download
    failed (mirrors the R source's ``NA_character_`` on failure — the
    caller skips ``None`` entries rather than aborting the whole call).
    """
    cache_path = Path(cache_path)
    filename = cache_path.name

    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        if verbose:
            console.print(f"[green]OK[/]  {msg['cache_hit'].format(filename=filename)}")
        return str(cache_path)

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['download_file'].format(filename=filename)}")

    ok, reason = _download_robust(url, cache_path, max_retries=3, verbose=verbose)
    if not ok:
        cache_path.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(filename=filename, err=reason or "unknown error")
        )
        return None

    if verbose:
        console.print(f"[green]OK[/]  {msg['download_done'].format(filename=filename)}")
    return str(cache_path)


# ---------------------------------------------------------------------------
# Internal: municipality ID detection
# ---------------------------------------------------------------------------

def _era5_detect_muni_col(gdf: Any, msg: dict[str, str]) -> str:
    """Detect the municipality identifier column in a GeoDataFrame."""
    found = [c for c in _MUNI_ID_CANDIDATES if c in gdf.columns]
    if found:
        return found[0]

    digit_re = re.compile(r"^\d{6,7}$")
    for col in gdf.columns:
        sample = gdf[col].dropna().astype(str).head(5)
        if len(sample) > 0 and all(digit_re.match(v) for v in sample):
            return col

    raise ValueError(
        msg["muni_col_not_found"].format(candidates=", ".join(_MUNI_ID_CANDIDATES))
    )


# ---------------------------------------------------------------------------
# Internal: raster extraction -> zonal statistics
# ---------------------------------------------------------------------------

def _era5_extract_monthly(
    nc_path: str,
    municipalities: Any,
    agg_fun: str,
    out_col: str,
    conv_fn: Any,
    start_date: date,
) -> pd.DataFrame:
    """Extract one monthly NetCDF's daily layers and aggregate to polygons.

    Returns a long ``pandas.DataFrame`` with columns ``code_muni``,
    ``date``, ``{out_col}``. Uses ``exactextract`` for area-weighted
    zonal statistics (the same isciences C++ engine R's
    ``exactextractr`` wraps), applied one daily layer at a time.
    """
    import xarray as xr
    from exactextract import exact_extract

    ds = xr.open_dataset(nc_path)
    data_vars = list(ds.data_vars)
    if not data_vars:
        raise ValueError(f"No data variables found in {nc_path}.")
    da = ds[data_vars[0]]
    da = da.rio.write_crs("EPSG:4326", inplace=False)

    time_dim = "time" if "time" in da.dims else da.dims[0]
    n_layers = da.sizes[time_dim]

    if "time" in da.coords and len(da["time"]) == n_layers:
        dates = pd.to_datetime(da["time"].values).normalize()
    else:
        # Fallback: build a daily date sequence from start_date, mirroring
        # the R source's fallback when the NetCDF has no readable time axis.
        dates = pd.date_range(start_date, periods=n_layers, freq="D")

    records = []
    for i in range(n_layers):
        layer = da.isel({time_dim: i})
        stats = exact_extract(
            layer,
            municipalities,
            [agg_fun],
            include_cols=["code_muni"],
            output="pandas",
        )
        stats = stats.rename(columns={agg_fun: out_col})
        stats["date"] = dates[i]
        records.append(stats[["code_muni", "date", out_col]])

    out = pd.concat(records, ignore_index=True)
    out[out_col] = conv_fn(out[out_col])
    return out
