"""grid_pdsi.py — Palmer Drought Severity Index (PDSI) import for Brazilian municipalities.

Mirrors R: sus_grid_pdsi.R

Downloads Palmer Drought Severity Index (Palmer, 1965) NetCDF rasters
from one of two public sources (TerraClimate monthly PDSI, or the NOAA
PSL/Dai self-calibrated PDSI), crops them to Brazil, and spatially
aggregates to municipality polygons via zonal statistics using
``exactextract`` (the literal Python binding for the same isciences C++
engine that R's ``exactextractr`` wraps — not ``rasterstats``, whose
default zonal-stats masking only approximates the fractional
pixel-polygon overlap weighting both ``exactextractr`` and
``exactextract`` compute).

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
from datetime import datetime
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

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "pdsi"

_VALID_SOURCES: tuple[str, ...] = ("terraclimate", "noaa_psl")
_VALID_AGG: tuple[str, ...] = ("mean", "sum", "median", "min", "max")

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

_ALL_MONTHS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

# terra::ext(-75, -28, -35, 6) is (xmin, xmax, ymin, ymax); minx, miny, maxx, maxy
_BRAZIL_BBOX: tuple[float, float, float, float] = (-75.0, -35.0, -28.0, 6.0)

# Year coverage and NetCDF variable name per source, verified from the R source.
_YEAR_BOUNDS: dict[str, tuple[int, int]] = {
    "terraclimate": (1950, 2025),
    "noaa_psl": (1850, 2018),
}
_VAR_NAME: dict[str, str] = {
    "terraclimate": "PDSI",
    "noaa_psl": "pdsi",
}

# Verified from sus_grid_pdsi.R: one NetCDF file per year (TerraClimate,
# University of Idaho, no authentication), and a single global file for
# NOAA PSL / Dai (2011) self-calibrated PDSI.
_TERRACLIMATE_BASE = "https://climate.northwestknowledge.net/TERRACLIMATE-DATA"
_NOAA_PSL_URL = "https://downloads.psl.noaa.gov/Datasets/dai_pdsi/pdsi.mon.mean.selfcalibrated.nc"

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados PDSI (Palmer Drought Severity Index)",
        "invalid_source": "'source' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' deve ser numérico sem NA.",
        "invalid_years_range": (
            "'years' deve estar entre {min_year} e {max_year} para esta fonte. "
            "Ano(s) inválido(s): {bad}."
        ),
        "default_years": "Usando anos {years} (padrão: últimos 2 anos completos).",
        "invalid_months": "'months' deve ser inteiro entre 1 e 12.",
        "need_geopandas": "O pacote geopandas é necessário para agregar por municípios.",
        "muni_not_geodataframe": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opções: {valid}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "download_start": "Baixando {n_files} arquivo(s) PDSI ({source}) sem autenticação...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "parquet_cache_hit": "Todos os dados no cache Parquet. Carregando...",
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
        "title": "PDSI (Palmer Drought Severity Index) Data",
        "invalid_source": "Invalid 'source': {bad}. Use: {valid}.",
        "invalid_years_type": "'years' must be numeric without NA.",
        "invalid_years_range": (
            "'years' must be between {min_year} and {max_year} for this source. "
            "Invalid year(s): {bad}."
        ),
        "default_years": "Using years {years} (default: last 2 complete years).",
        "invalid_months": "'months' must be integer between 1 and 12.",
        "need_geopandas": "The geopandas package is required to aggregate by municipality.",
        "muni_not_geodataframe": "'municipalities' must be a geopandas.GeoDataFrame.",
        "invalid_agg": "Invalid 'agg_fun': {bad}. Options: {valid}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "download_start": "Downloading {n_files} PDSI file(s) ({source}), no authentication...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "parquet_cache_hit": "All data found in Parquet cache. Loading...",
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
        "title": "Datos PDSI (Índice de Severidad de Sequía de Palmer)",
        "invalid_source": "'source' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' debe ser numérico sin NA.",
        "invalid_years_range": (
            "'years' debe estar entre {min_year} y {max_year} para esta fuente. "
            "Año(s) inválido(s): {bad}."
        ),
        "default_years": "Usando años {years} (por defecto: últimos 2 años completos).",
        "invalid_months": "'months' debe ser entero entre 1 y 12.",
        "need_geopandas": "El paquete geopandas es necesario para agregar por municipios.",
        "muni_not_geodataframe": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "invalid_agg": "'agg_fun' inválido: {bad}. Opciones: {valid}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "download_start": "Descargando {n_files} archivo(s) PDSI ({source}), sin autenticación...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "parquet_cache_hit": "Todos los datos en caché Parquet. Cargando...",
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

def sus_grid_pdsi(
    years: int | list[int] | None = None,
    months: list[int] | tuple[int, ...] = _ALL_MONTHS,
    source: Literal["terraclimate", "noaa_psl"] = "terraclimate",
    municipalities: gpd.GeoDataFrame | None = None,
    agg_fun: Literal["mean", "sum", "median", "min", "max"] = "mean",
    crop_brazil: bool = True,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame | dict[str, str]:
    """Import Palmer Drought Severity Index (PDSI) for Brazilian municipalities.

    Downloads PDSI data, crops it to Brazil, spatially aggregates to
    municipalities, and returns a ``pd.DataFrame`` compatible with
    ``sus_mod_dlnm`` / ``sus_climate_anomaly`` (via ``sus_grid_join``).

    PDSI (Palmer, 1965) quantifies cumulative moisture departures
    relative to local climate normals using a two-layer soil water
    balance model. Values range from approximately -10 (extreme
    drought) to +10 (extreme wet), with operational categories at -2
    (moderate drought) to -4 (extreme drought). Health applications in
    Brazil: drought (PDSI < -2) is linked to malnutrition, diarrheal
    disease, mental health stress, and vector-borne disease in the
    semi-arid Northeast and Amazônia; wet periods (PDSI > 2) are linked
    to leptospirosis, hepatitis A, and flooding.

    Args:
        years: Year(s) to download. TerraClimate: 1950-2025. NOAA PSL:
            1850-2018. ``None`` (default) uses the last two complete
            years — clamped down to the source's ``max_year`` if that
            falls outside range (e.g. NOAA PSL never defaults past 2018).
        months: Months (1-12) to include. Default: all 12 months.
        source: Data source. ``"terraclimate"`` (default): TerraClimate
            monthly PDSI (Abatzoglou et al., 2018; University of Idaho),
            ~4 km resolution, 1950-2025, WGS84, no authentication, one
            NetCDF file (~165 MB) per year. ``"noaa_psl"``: Dai (2011)
            self-calibrated PDSI from NOAA Physical Sciences Laboratory,
            2.5° resolution, 1850-2018, single global file (~40 MB).
            Coarser but a much longer record.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from ``climasus-data`` municipality
            boundaries). When provided, rasters are aggregated and a
            ``pd.DataFrame`` is returned. If ``None``, a dict mapping
            filename -> cached NetCDF file path is returned instead (no
            spatial aggregation).
        agg_fun: Spatial aggregation function for ``exactextract``.
            Default ``"mean"`` (area-weighted mean).
        crop_brazil: Crop global rasters to Brazil's bounding box before
            aggregation. Reduces memory usage significantly. Default
            ``True``.
        use_cache: Reuse previously downloaded NetCDF files and
            aggregated Parquet caches. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/pdsi``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni`` (str), ``date`` (datetime, one row per month), and
        ``pdsi`` (float, unitless). Metadata in
        ``df.attrs["sus_meta"]`` (``stage="climate"``, ``type="pdsi"``).
        If *municipalities* is ``None``: a dict mapping each cached
        NetCDF filename to its local path.

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            extracted for the given parameters.
        ImportError: If ``geopandas``, ``rioxarray``/``xarray``, or
            ``exactextract`` are required but not installed.

    PDSI classification:
        >= +4.0 extremely wet; +3.0 to +3.99 very wet; +2.0 to +2.99
        moderately wet; -1.99 to +1.99 near normal; -2.0 to -2.99
        moderate drought (D1); -3.0 to -3.99 severe drought (D2);
        <= -4.0 extreme drought (D3-D4).

    Data sources:
        TerraClimate: Abatzoglou, J.T. et al. (2018). TerraClimate, a
        high-resolution global dataset of monthly climate and climatic
        water balance from 1958-2015. Scientific Data, 5, 170191.
        https://doi.org/10.1038/sdata.2017.191
        Data: https://climate.northwestknowledge.net/TERRACLIMATE-DATA/
        NOAA PSL: Dai, A. (2011). Characteristics and trends in various
        forms of the PDSI during 1900-2008. J. Geophys. Res., 116,
        D12115. https://doi.org/10.1029/2010JD015541
        Data: https://downloads.psl.noaa.gov/Datasets/dai_pdsi/

    Examples::

        import climasus4py as cs

        # TerraClimate PDSI for a set of municipalities, 2015-2022
        pdsi_mt = cs.sus_grid_pdsi(
            years=range(2015, 2023), municipalities=mt_mun, lang="pt",
        )

        # Longer history with NOAA PSL (2.5 deg)
        pdsi_hist = cs.sus_grid_pdsi(
            years=range(1960, 2019), source="noaa_psl", municipalities=mt_mun,
        )
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- source -------------------------------------------------------------
    if source not in _VALID_SOURCES:
        raise ValueError(
            msg["invalid_source"].format(bad=source, valid=", ".join(_VALID_SOURCES))
        )
    min_year, max_year = _YEAR_BOUNDS[source]

    # --- years ----------------------------------------------------------------
    current_year = datetime.now().year
    if years is None:
        y0, y1 = current_year - 2, current_year - 1
        if y1 > max_year:
            shift = y1 - max_year
            y0, y1 = y0 - shift, y1 - shift
        years_list = [y0, y1]
        if verbose:
            console.print(
                "[cyan]INFO[/]  " + msg["default_years"].format(years=f"{y0}-{y1}")
            )
    else:
        raw_years = [years] if isinstance(years, int) else list(years)
        try:
            years_list = sorted({int(y) for y in raw_years})
        except (TypeError, ValueError) as exc:
            raise ValueError(msg["invalid_years_type"]) from exc
        bad_years = [y for y in years_list if y < min_year or y > max_year]
        if bad_years:
            raise ValueError(
                msg["invalid_years_range"].format(
                    min_year=min_year, max_year=max_year,
                    bad=", ".join(str(y) for y in bad_years),
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

    # Parquet cache key: disambiguate caches across different municipality
    # sets by hashing the sorted code_muni values. NOTE: the R source's
    # cache_pq filenames (pdsi_terraclimate_%04d.parquet /
    # pdsi_noaa_psl_%d_%d.parquet) have NO municipality-set disambiguation
    # at all — calling sus_grid_pdsi() for two different municipality sets
    # with the same years would silently reuse the first call's cached
    # zonal-stats results. That is a silent-incorrect-result bug (not just
    # a style quirk), so per governance rule 4's OWASP/silent-correctness
    # exception it is fixed here rather than replicated — mirroring the
    # same fix already applied in the sibling sus_grid_chirps port (which
    # DOES have a matching digest()-based muni_hash in its R source; PDSI's
    # R source has no equivalent at all). See IDEIAS.md.
    muni_hash = ""
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities)
        codes = sorted(str(c) for c in municipalities[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

    # --- build download manifest ----------------------------------------------
    manifest = _build_manifest(source, years_list, cache_path, muni_hash)
    n_files = len(manifest)

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(
            "[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files, source=source)
        )

    # --- Parquet early-return -------------------------------------------------
    unique_pq = manifest["cache_pq"].unique().tolist()
    if municipalities is not None and use_cache and all(Path(p).is_file() for p in unique_pq):
        if verbose:
            console.print("[green]OK[/]  " + msg["parquet_cache_hit"])
        return _build_from_parquet(unique_pq, verbose, msg)

    # --- download rasters -------------------------------------------------------
    unique_files = manifest.drop_duplicates(subset=["filename"])[["filename", "url", "cache_nc"]]
    for row in unique_files.itertuples(index=False):
        _download_pdsi_file(row.url, Path(row.cache_nc), use_cache, verbose, msg)

    # --- return file paths if no municipalities ---------------------------------
    if municipalities is None:
        paths = {row.filename: str(row.cache_nc) for row in unique_files.itertuples(index=False)}
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

    # --- extract per manifest row (1 file per year for TerraClimate; a single
    # shared file for NOAA PSL) --------------------------------------------------
    result_frames: list[pd.DataFrame] = []
    for row in manifest.itertuples(index=False):
        pq_path = Path(row.cache_pq)

        if use_cache and pq_path.is_file() and pq_path.stat().st_size > 0:
            if verbose:
                console.print(
                    "[green]OK[/]  " + msg["parquet_hit"].format(filename=pq_path.name)
                )
            result_frames.append(pd.read_parquet(pq_path))
            continue

        nc_path = Path(row.cache_nc)
        if not nc_path.is_file() or nc_path.stat().st_size == 0:
            console.print(
                "[yellow]WARN[/]  " + msg["skip_missing"].format(filename=row.filename)
            )
            continue

        df_row = _process_manifest_row(row, source, months_list, muni, bbox, agg_fun, msg)
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
        result_frames.append(df_row)

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
        "type": "pdsi",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "unit": "month",
            "source": source,
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "years": years_list,
        "months": months_list,
        "source": source,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "agg_fun": agg_fun,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_pdsi(): source={source}, {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: filename / URL construction (mirrors R's manifest construction)
# ---------------------------------------------------------------------------

def _pdsi_file_info(source: str, year: int) -> tuple[str, str]:
    """Build the PDSI NetCDF filename and download URL for one manifest row."""
    if source == "terraclimate":
        filename = f"TerraClimate_PDSI_{year:04d}.nc"
        url = f"{_TERRACLIMATE_BASE}/{filename}"
    else:  # noaa_psl
        filename = "pdsi.mon.mean.selfcalibrated.nc"
        url = _NOAA_PSL_URL
    return filename, url


# ---------------------------------------------------------------------------
# Internal: download manifest
# ---------------------------------------------------------------------------

def _build_manifest(
    source: str,
    years: list[int],
    cache_path: Path,
    muni_hash: str,
) -> pd.DataFrame:
    """Build one row per NetCDF file to download/process.

    TerraClimate: one row per requested year (one file each). NOAA PSL:
    a single row covering all requested years (one shared global file);
    the row's ``years`` field carries the full tuple so the extraction
    step can filter the shared file down to the requested years.
    """
    rows: list[dict] = []
    if source == "terraclimate":
        for yr in years:
            filename, url = _pdsi_file_info(source, yr)
            pq_path = cache_path / "parquet" / f"pdsi_terraclimate_{yr:04d}{muni_hash}.parquet"
            rows.append({
                "years": (yr,),
                "filename": filename,
                "url": url,
                "cache_nc": cache_path / source / filename,
                "cache_pq": pq_path,
            })
    else:  # noaa_psl
        filename, url = _pdsi_file_info(source, years[0])
        pq_path = (
            cache_path / "parquet"
            / f"pdsi_noaa_psl_{min(years):04d}_{max(years):04d}{muni_hash}.parquet"
        )
        rows.append({
            "years": tuple(years),
            "filename": filename,
            "url": url,
            "cache_nc": cache_path / source / filename,
            "cache_pq": pq_path,
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
# Internal: download one PDSI NetCDF file with cache
# ---------------------------------------------------------------------------

def _download_pdsi_file(
    url: str, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> None:
    """Download one PDSI NetCDF file to *cache_path*, reusing the cache.

    Never raises: download failures are logged as warnings and left for
    the per-year extraction step to skip via a missing-file check —
    mirroring the R source, which likewise ignores
    ``.pdsi_download_file()``'s return value in the download loop.
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

def _read_pdsi_raster(nc_path: Path, source: str):
    """Open a PDSI NetCDF file as an ``xarray.DataArray`` with a time dimension.

    Selects the source-specific PDSI variable name (``PDSI`` for
    TerraClimate, ``pdsi`` for NOAA PSL/Dai) and ensures a WGS84 CRS is
    set — both datasets ship in geographic coordinates and TerraClimate
    mirrors sometimes omit an explicit CRS attribute, matching the R
    source's own fallback (``if (is.na(terra::crs(r))) terra::crs(r) <-
    "EPSG:4326"``).
    """
    try:
        import rioxarray  # noqa: F401 (registers the .rio accessor)
        import xarray as xr
    except ImportError as exc:
        raise ImportError(
            "rioxarray and xarray are required to read PDSI NetCDF rasters. "
            "Install them with: pip install rioxarray xarray"
        ) from exc

    var_name = _VAR_NAME[source]
    ds = xr.open_dataset(nc_path, decode_coords="all")
    if var_name not in ds.variables:
        raise ValueError(f"Variable '{var_name}' not found in {nc_path.name}.")
    da = ds[var_name]

    if da.rio.crs is None:
        da = da.rio.write_crs("EPSG:4326")
    return da


def _extract_pdsi_dates(da, year: int) -> pd.DatetimeIndex:
    """Extract per-layer dates from raster time metadata, with a fallback.

    Mirrors the R source: prefer the NetCDF ``time`` coordinate; if
    absent or malformed, fall back to assigning consecutive months
    starting January of *year*.
    """
    if "time" in da.coords:
        try:
            dates = pd.to_datetime(da["time"].values)
            n_layers = da.sizes.get("time", len(dates))
            if len(dates) == n_layers:
                return pd.DatetimeIndex(dates)
        except Exception:
            pass
    n_layers = da.sizes.get("time", 1)
    return pd.date_range(pd.Timestamp(year=year, month=1, day=1), periods=n_layers, freq="MS")


def _zonal_stats(raster, municipalities: gpd.GeoDataFrame, agg_fun: str) -> np.ndarray:
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
# Internal: process one manifest row (one NetCDF file, possibly many months)
# ---------------------------------------------------------------------------

def _process_manifest_row(
    row,
    source: str,
    months: list[int],
    municipalities: gpd.GeoDataFrame,
    bbox: tuple[float, float, float, float] | None,
    agg_fun: str,
    msg: dict[str, str],
) -> pd.DataFrame | None:
    """Extract PDSI for every requested month in *row* and aggregate to municipalities."""
    try:
        da = _read_pdsi_raster(Path(row.cache_nc), source)
        rep_year = row.years[0]
        dates = _extract_pdsi_dates(da, rep_year)

        keep = np.isin(dates.month, months)
        if source == "noaa_psl":
            keep = keep & np.isin(dates.year, row.years)
        if not keep.any():
            return None

        idx = np.flatnonzero(keep)
        dates_keep = dates[keep]
        if "time" in da.dims:
            da = da.isel(time=idx)

        if bbox is not None:
            minx, miny, maxx, maxy = bbox
            da = da.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

        month_frames: list[pd.DataFrame] = []
        for i, dt in enumerate(dates_keep):
            band = da.isel(time=i) if "time" in da.dims else da
            values = _zonal_stats(band, municipalities, agg_fun)
            month_frames.append(pd.DataFrame({
                "code_muni": municipalities["code_muni"].to_numpy(),
                "date": pd.Timestamp(dt),
                "pdsi": pd.array(values, dtype="float64"),
            }))
        if not month_frames:
            return None
        return pd.concat(month_frames, ignore_index=True)
    except ImportError:
        raise
    except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
        console.print(
            "[yellow]WARN[/]  " + msg["extract_warn"].format(filename=row.filename, err=str(e))
        )
        return None


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
        "type": "pdsi",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "source": "cache",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_pdsi(): from Parquet cache, {n_rows} obs"
        ],
        "user": {},
    }
    return result
