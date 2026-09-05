"""grid_smvi.py — Soil Moisture Volatility Index (flash drought) data.

Mirrors R: sus_grid_smvi.R

SMVI (Soil Moisture Volatility Index, Osman et al. 2024) detects *flash
droughts* — rapid, high-impact root-zone soil moisture deficits that
develop within days to weeks. Unlike SPI/SPEI/PDSI (cumulative drought
*severity*), SMVI captures the *speed* of soil moisture decline: an event
begins when a 5-day running average of GLDAS-2 root-zone soil moisture
(0-100 cm) drops below the 20-day running average and exceeds the 20th
percentile deficit threshold.

Downloads a pre-computed global flash-drought event catalogue (not a
raster) from HydroShare: a grid-cell coordinate table (``LONLAT.csv``)
plus one CSV of events per year (1990-2021), packaged as a single
``tar.gz`` archive. Events are point data (one row per detected flash
drought at one 0.25-degree GLDAS grid cell), so — like ``sus_grid_fires``
— this uses a point-in-polygon spatial join (``geopandas.sjoin``), not
raster zonal statistics: no ``exactextract``/``rioxarray`` dependency is
needed for this function.

Not lazy: the R source works entirely with in-memory data.frames (never
Arrow/DuckDB), so this port takes/returns a materialised ``pd.DataFrame``,
matching ``sus_grid_fires``'s precedent.
"""

from __future__ import annotations

import re
import tarfile
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

# Renamed from R's "~/.climasus4r_cache/smvi", matching the established
# climasus4py_cache convention already used by grid_era5/chirps/pdsi/fires.
_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "smvi"

_VALID_AGGREGATE_BY: tuple[str, ...] = ("year", "month")

_MIN_YEAR = 1990
_MAX_YEAR = 2021

_BASE_URL = "http://www.hydroshare.org/resource/642ff72592404a17bb85a8a92b4dbcd6/data/contents"

_BRAZIL_BBOX_LONLAT: tuple[float, float, float, float] = (-75.0, -28.0, -35.0, 6.0)

_EVENTS_CSV_PATTERN = re.compile(r"SMVI_GLDAS_(\d{4})\.csv$")

# Candidate date-column names (order matters: first two present, in this
# order, are treated as onset/recovery — mirrors R's `intersect()`, which
# preserves the order of its first argument).
_DATE_COL_CANDIDATES: tuple[str, ...] = (
    "fstdate", "lstdate", "onset", "recovery", "start_date", "end_date",
)
_SV_COL_CANDIDATES: tuple[str, ...] = ("sv", "severity")

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "SMVI — Índice de Volatilidade de Umidade do Solo (Flash Droughts)",
        "about": (
            "Dados globais 1990-2021 de secas rápidas (GLDAS, 0.25°). "
            "Download único de ~130 MB."
        ),
        "invalid_years_range": "'years' deve estar entre 1990 e 2021. Ano(s) inválido(s): {bad}.",
        "need_geopandas": "O pacote geopandas é necessário para agregar por municípios.",
        "muni_not_gdf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "lonlat_missing": "Não foi possível baixar LONLAT.csv do HydroShare.",
        "reading_lonlat": "Lendo coordenadas de grade (LONLAT.csv)...",
        "lonlat_bad_cols": "LONLAT.csv não tem colunas 'lon'/'lat' esperadas. Colunas: {cols}",
        "cells_found": "{n} célula(s) de grade dentro do Brasil.",
        "extracting": "Extraindo arquivo de eventos SMVI...",
        "no_csv_after_extract": "Nenhum arquivo CSV encontrado após extração do tar.gz.",
        "events_missing": "Arquivo de eventos não disponível no cache.",
        "no_data": "Nenhum arquivo CSV de eventos para os anos solicitados.",
        "loading_events": "Carregando eventos de {n_years} ano(s)...",
        "read_warn": "Não foi possível ler {filename}.",
        "no_brazil_events": "Nenhum evento flash drought encontrado para as células brasileiras.",
        "bad_event_cols": "Colunas de data não encontradas no CSV de eventos. Colunas: {cols}",
        "events_loaded": "{n} evento(s) de flash drought carregado(s).",
        "spatial_join": "Atribuindo eventos a {n_mun} município(s)...",
        "no_events_in_muni": "Nenhum evento encontrado dentro dos polígonos municipais.",
        "agg_done": "Concluído: {n_rows} observações ({n_mun} municípios).",
        "done_raw": "Concluído: {n} eventos brutos retornados.",
    },
    "en": {
        "title": "SMVI — Soil Moisture Volatility Index (Flash Droughts)",
        "about": "Global 1990-2021 flash drought data (GLDAS, 0.25 deg). Single download ~130 MB.",
        "invalid_years_range": "'years' must be between 1990 and 2021. Invalid year(s): {bad}.",
        "need_geopandas": "The geopandas package is required to aggregate by municipality.",
        "muni_not_gdf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "lonlat_missing": "Could not download LONLAT.csv from HydroShare.",
        "reading_lonlat": "Reading grid coordinates (LONLAT.csv)...",
        "lonlat_bad_cols": "LONLAT.csv does not have expected 'lon'/'lat' columns. Found: {cols}",
        "cells_found": "{n} grid cell(s) within Brazil's bounding box.",
        "extracting": "Extracting SMVI events archive...",
        "no_csv_after_extract": "No CSV files found after tar.gz extraction.",
        "events_missing": "Events file not available in cache.",
        "no_data": "No events CSV files for the requested years.",
        "loading_events": "Loading events from {n_years} year(s)...",
        "read_warn": "Could not read {filename}.",
        "no_brazil_events": "No flash drought events found for Brazilian grid cells.",
        "bad_event_cols": "Date columns not found in events CSV. Columns: {cols}",
        "events_loaded": "{n} flash drought event(s) loaded.",
        "spatial_join": "Assigning events to {n_mun} municipality/ies...",
        "no_events_in_muni": "No events found within municipality polygons.",
        "agg_done": "Complete: {n_rows} observations ({n_mun} municipalities).",
        "done_raw": "Complete: {n} raw flash drought events returned.",
    },
    "es": {
        "title": "SMVI — Índice de Volatilidad de Humedad del Suelo (Sequías Rápidas)",
        "about": (
            "Datos globales 1990-2021 de sequías rápidas (GLDAS, 0.25°). "
            "Descarga única ~130 MB."
        ),
        "invalid_years_range": "'years' debe estar entre 1990 y 2021. Año(s) inválido(s): {bad}.",
        "need_geopandas": "El paquete geopandas es necesario para agregar por municipios.",
        "muni_not_gdf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "lonlat_missing": "No se pudo descargar LONLAT.csv de HydroShare.",
        "reading_lonlat": "Leyendo coordenadas de cuadrícula (LONLAT.csv)...",
        "lonlat_bad_cols": "LONLAT.csv no tiene columnas 'lon'/'lat'. Encontradas: {cols}",
        "cells_found": "{n} celda(s) de cuadrícula dentro de Brasil.",
        "extracting": "Extrayendo archivo de eventos SMVI...",
        "no_csv_after_extract": "No se encontraron archivos CSV tras la extracción.",
        "events_missing": "Archivo de eventos no disponible en caché.",
        "no_data": "No hay archivos CSV de eventos para los años solicitados.",
        "loading_events": "Cargando eventos de {n_years} año(s)...",
        "read_warn": "No se pudo leer {filename}.",
        "no_brazil_events": "No se encontraron eventos de sequía rápida para celdas brasileñas.",
        "bad_event_cols": "Columnas de fecha no encontradas. Columnas: {cols}",
        "events_loaded": "{n} evento(s) de sequía rápida cargado(s).",
        "spatial_join": "Asignando eventos a {n_mun} municipio(s)...",
        "no_events_in_muni": "No se encontraron eventos dentro de los polígonos municipales.",
        "agg_done": "Completo: {n_rows} observaciones ({n_mun} municipios).",
        "done_raw": "Completo: {n} eventos brutos retornados.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_smvi(
    years: int | list[int] | None = None,
    municipalities: gpd.GeoDataFrame | None = None,
    aggregate_by: Literal["year", "month"] = "year",
    brazil_only: bool = True,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import Soil Moisture Volatility Index (SMVI / flash drought) data.

    Downloads the global flash drought inventory based on the Soil
    Moisture Volatility Index (SMVI; Osman et al., 2024), spatially
    assigns events to Brazilian municipalities, and returns annual or
    monthly flash drought statistics as a ``pd.DataFrame`` compatible
    with ``sus_grid_join`` / ``sus_mod_dlnm``.

    SMVI detects *flash droughts* — rapid, high-impact soil moisture
    deficits that develop within days to weeks. Unlike SPI/SPEI/PDSI
    (which measure cumulative drought severity), SMVI captures the
    *speed* of soil moisture decline by comparing a 5-day running average
    of root-zone soil moisture (0-100 cm) against a 20-day running
    average. A flash drought event begins when the 5-day average drops
    below the 20-day average AND reaches the 20th percentile deficit
    threshold.

    Health connections in Brazil: malnutrition/food insecurity (crops
    destroyed within weeks, 1-3 month lag to child malnutrition in the
    semi-arid Northeast and Amazonia); leptospirosis (rapid soil
    rewetting after an event concentrates rodents near water, 1-4 week
    lag); leishmaniasis (drought-driven migration to water sources
    increases sandfly contact, 2-year lag documented in Bahia);
    waterborne disease (water shortage increases dependence on unclean
    sources).

    Args:
        years: Year or list of years to include. Must be within
            1990-2021. ``None`` (default) returns all available years.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons. When provided, flash drought events are spatially
            assigned to municipalities and statistics are aggregated. If
            ``None``, returns the raw event data for Brazil filtered to
            the requested years.
        aggregate_by: Temporal resolution of the output when
            *municipalities* is provided: ``"year"`` (default) or
            ``"month"``.
        brazil_only: Filter the global dataset to Brazil's bounding box
            (-75 to -28 lon, -35 to 6 lat) before processing. Default
            ``True``.
        use_cache: Reuse cached files. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/smvi``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni``, ``date`` (Jan 1 for annual; first day of month for
        monthly), ``n_fd_events`` (int), ``fd_total_days`` (int),
        ``fd_mean_severity`` (float), ``fd_max_severity`` (float).
        Metadata: ``stage="climate"``, ``type="smvi"``.
        If *municipalities* is ``None``: a ``pd.DataFrame`` with raw
        event columns ``cell_id``, ``Lon``, ``Lat``, ``fstdate``,
        ``lstdate``, ``duration_days``, ``SV``, ``source``.
        In both cases, metadata is available via ``df.attrs["sus_meta"]``.

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            downloaded/extracted for the given parameters.
        ImportError: If ``geopandas`` is required (municipalities given)
            but not installed.

    Algorithm (Osman et al., 2024):
        1. Compute 5-day and 20-day running averages of root-zone soil
           moisture (RZSM, 0-100 cm) from NASA GLDAS-2 data.
        2. A flash drought *onset* occurs when the 5-day average drops
           below the 20-day average.
        3. A flash drought *event* is confirmed when the RZSM deficit
           relative to the 20th percentile threshold is exceeded.
        4. Severity (SV) quantifies the cumulative soil moisture deficit
           during the event.

    Data source:
        Osman, M. et al. (2024). A global flash drought inventory based
        on soil moisture volatility. Scientific Data, 11, 916.
        https://doi.org/10.1038/s41597-024-03809-9
        Data: http://www.hydroshare.org/resource/642ff72592404a17bb85a8a92b4dbcd6/
        (CC BY-SA 4.0, no authentication required).

    Examples::

        import climasus4py as cs

        smvi_mt = cs.sus_grid_smvi(
            years=list(range(2000, 2022)), municipalities=mt_mun,
            aggregate_by="year", lang="pt",
        )
        combined = cs.sus_grid_join(sih_mt, smvi_mt)
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    if aggregate_by not in _VALID_AGGREGATE_BY:
        raise ValueError(
            f"'aggregate_by' must be one of {_VALID_AGGREGATE_BY}, got {aggregate_by!r}."
        )

    # --- years --------------------------------------------------------------
    req_years: list[int] | None
    if years is None:
        req_years = None
    else:
        raw_years = [years] if isinstance(years, int) else list(years)
        req_years = sorted({int(y) for y in raw_years})
        bad_years = [y for y in req_years if y < _MIN_YEAR or y > _MAX_YEAR]
        if bad_years:
            raise ValueError(msg["invalid_years_range"].format(bad=", ".join(map(str, bad_years))))

    # --- municipalities -------------------------------------------------------
    if municipalities is not None:
        if find_spec("geopandas") is None:
            raise ImportError(
                f"{msg['need_geopandas']} Install with: pip install climasus4py[spatial]"
            )
        import geopandas as gpd

        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise ValueError(msg["muni_not_gdf"])

    # --- use_cache / cache_dir -------------------------------------------------
    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_path = Path(cache_dir).expanduser()

    lonlat_csv = cache_path / "LONLAT.csv"
    events_gz = cache_path / "SMVI_GLDAS_Events.tar.gz"
    events_dir = cache_path / "events"

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(f"[cyan]INFO[/]  {msg['about']}")

    # --- download LONLAT.csv ---------------------------------------------------
    _smvi_download_once(
        f"{_BASE_URL}/LONLAT.csv", lonlat_csv, use_cache, verbose, msg, "LONLAT.csv"
    )
    if not lonlat_csv.is_file():
        raise ValueError(msg["lonlat_missing"])

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['reading_lonlat']}")
    lonlat = pd.read_csv(lonlat_csv)
    lonlat.columns = [str(c).lower() for c in lonlat.columns]
    if "lon" not in lonlat.columns or "lat" not in lonlat.columns:
        raise ValueError(msg["lonlat_bad_cols"].format(cols=", ".join(lonlat.columns)))
    lonlat = lonlat.copy()
    lonlat["cell_id"] = range(1, len(lonlat) + 1)

    if brazil_only:
        xmin, xmax, ymin, ymax = _BRAZIL_BBOX_LONLAT
        mask = (
            (lonlat["lon"] >= xmin) & (lonlat["lon"] <= xmax)
            & (lonlat["lat"] >= ymin) & (lonlat["lat"] <= ymax)
        )
        lonlat_br = lonlat[mask].copy()
    else:
        lonlat_br = lonlat

    n_cells = len(lonlat_br)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['cells_found'].format(n=n_cells)}")

    # --- download and extract events archive ------------------------------------
    _smvi_download_once(
        f"{_BASE_URL}/FD_Events/SMVI_GLDAS_Events.tar.gz",
        events_gz, use_cache, verbose, msg, "SMVI_GLDAS_Events.tar.gz (~109 MB)",
    )
    if not events_gz.is_file():
        raise ValueError(msg["events_missing"])

    if not events_dir.is_dir() or not any(events_dir.glob("*.csv")):
        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['extracting']}")
        events_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(events_gz, mode="r:gz") as tf:
            tf.extractall(events_dir, filter="data")  # noqa: S202 - trusted cache archive

    csv_files = [
        p for p in events_dir.glob("*.csv") if _EVENTS_CSV_PATTERN.search(p.name)
    ]
    if not csv_files:
        raise ValueError(msg["no_csv_after_extract"])

    avail_years = {int(_EVENTS_CSV_PATTERN.search(p.name).group(1)): p for p in csv_files}  # type: ignore[union-attr]
    target_years = req_years if req_years is not None else sorted(avail_years)
    use_files = [avail_years[y] for y in target_years if y in avail_years]
    if not use_files:
        raise ValueError(msg["no_data"])

    # --- read and filter events to Brazil cells ----------------------------------
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['loading_events'].format(n_years=len(use_files))}")

    br_cell_ids = set(lonlat_br["cell_id"])
    events_frames: list[pd.DataFrame] = []
    for f in use_files:
        try:
            ev = pd.read_csv(f)
            ev.columns = [str(c).lower() for c in ev.columns]
            if "cell_id" not in ev.columns:
                ev = ev.rename(columns={ev.columns[0]: "cell_id"})
            ev = ev[ev["cell_id"].isin(br_cell_ids)]
            events_frames.append(ev)
        except Exception:
            if verbose:
                console.print(f"[yellow]WARN[/]  {msg['read_warn'].format(filename=f.name)}")

    if not events_frames:
        raise ValueError(msg["no_brazil_events"])

    events_df = pd.concat(events_frames, ignore_index=True)

    # --- standardise date columns ------------------------------------------------
    date_cols = [c for c in _DATE_COL_CANDIDATES if c in events_df.columns]
    if len(date_cols) < 2:
        raise ValueError(msg["bad_event_cols"].format(cols=", ".join(events_df.columns)))
    fst_col, lst_col = date_cols[0], date_cols[1]
    events_df["fstdate"] = pd.to_datetime(events_df[fst_col])
    events_df["lstdate"] = pd.to_datetime(events_df[lst_col])
    events_df["duration_days"] = (events_df["lstdate"] - events_df["fstdate"]).dt.days + 1

    sv_col = next((c for c in _SV_COL_CANDIDATES if c in events_df.columns), None)
    events_df["SV"] = (
        pd.to_numeric(events_df[sv_col], errors="coerce") if sv_col is not None
        else float("nan")
    )

    events_df = events_df.merge(
        lonlat_br[["cell_id", "lon", "lat"]], on="cell_id", how="left"
    )
    events_df = events_df[
        ["cell_id", "lon", "lat", "fstdate", "lstdate", "duration_days", "SV"]
    ].reset_index(drop=True)

    n_events = len(events_df)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['events_loaded'].format(n=n_events)}")

    now = datetime.now()

    # --- return raw events if no municipalities ----------------------------------
    if municipalities is None:
        out_df = events_df.rename(columns={"lon": "Lon", "lat": "Lat"})
        out_df["source"] = "SMVI_GLDAS"
        out_df.attrs["sus_meta"] = {
            "system": None,
            "stage": "climate",
            "type": "smvi",
            "spatial": False,
            "temporal": {
                "start": events_df["fstdate"].min() if n_events else pd.NaT,
                "end": events_df["lstdate"].max() if n_events else pd.NaT,
                "unit": "event",
                "source": "HydroShare_SMVI",
            },
            "created": now.isoformat(),
            "modified": now.isoformat(),
            "years": target_years,
            "n_observations": n_events,
            "history": [
                f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
                f"sus_grid_smvi(): {n_events} events, no aggregation"
            ],
            "user": {},
        }
        if verbose:
            console.print(f"[green]OK[/]  {msg['done_raw'].format(n=n_events)}")
        return out_df

    # --- assign flash drought events to municipalities ---------------------------
    muni_id_col = _smvi_detect_muni_col(municipalities, msg)
    muni = municipalities.copy()
    muni["code_muni"] = muni[muni_id_col].astype(str).str[:7]
    muni = muni.to_crs(epsg=4326)
    n_mun = len(muni)

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['spatial_join'].format(n_mun=n_mun)}")

    import geopandas as gpd

    cell_sf = gpd.GeoDataFrame(
        lonlat_br[["cell_id", "lon", "lat"]],
        geometry=gpd.points_from_xy(lonlat_br["lon"], lonlat_br["lat"]),
        crs="EPSG:4326",
    )
    muni_slim = muni[["code_muni", muni.geometry.name]]
    cell_joined = gpd.sjoin(cell_sf, muni_slim, predicate="within", how="inner")
    cell_muni = pd.DataFrame(cell_joined[["cell_id", "code_muni"]])

    events_muni = events_df.merge(cell_muni, on="cell_id", how="inner")
    if events_muni.empty:
        console.print(f"[yellow]WARN[/]  {msg['no_events_in_muni']}")

    events_muni = events_muni.copy()
    events_muni["year_val"] = events_muni["fstdate"].dt.year
    events_muni["month_val"] = events_muni["fstdate"].dt.month

    group_cols = ["code_muni", "year_val"] if aggregate_by == "year" else [
        "code_muni", "year_val", "month_val"
    ]
    if events_muni.empty:
        result = pd.DataFrame(columns=[*group_cols, "n_fd_events", "fd_total_days",
                                        "fd_mean_severity", "fd_max_severity"])
    else:
        grouped = events_muni.groupby(group_cols, as_index=False)
        result = grouped.agg(
            n_fd_events=("cell_id", "size"),
            fd_total_days=("duration_days", "sum"),
            fd_mean_severity=("SV", "mean"),
            fd_max_severity=("SV", "max"),
        )

    if aggregate_by == "year":
        result["date"] = pd.to_datetime(
            {"year": result["year_val"], "month": 1, "day": 1}
        ) if not result.empty else pd.Series(dtype="datetime64[ns]")
        result = result.drop(columns=["year_val"])
    else:
        result["date"] = pd.to_datetime(
            {"year": result["year_val"], "month": result["month_val"], "day": 1}
        ) if not result.empty else pd.Series(dtype="datetime64[ns]")
        result = result.drop(columns=["year_val", "month_val"])

    result["n_fd_events"] = result["n_fd_events"].astype("int64")
    result["fd_total_days"] = result["fd_total_days"].astype("int64")
    result = result[
        ["code_muni", "date", "n_fd_events", "fd_total_days", "fd_mean_severity", "fd_max_severity"]
    ]
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        console.print(f"[green]OK[/]  {msg['agg_done'].format(n_rows=n_rows, n_mun=n_mun)}")

    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "smvi",
        "spatial": False,
        "temporal": {
            "start": result["date"].min() if n_rows else pd.NaT,
            "end": result["date"].max() if n_rows else pd.NaT,
            "unit": aggregate_by,
            "source": "HydroShare_SMVI_Osman2024",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "years": target_years,
        "aggregate_by": aggregate_by,
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_grid_smvi(): "
            f"{len(events_muni)} events -> {n_rows} obs ({aggregate_by}), {n_mun} municipalities"
        ],
        "user": {},
    }
    return result


# ---------------------------------------------------------------------------
# Internal: download one SMVI file with cache
# ---------------------------------------------------------------------------

def _smvi_download_once(
    url: str,
    cache_path: Path,
    use_cache: bool,
    verbose: bool,
    msg: dict[str, str],
    label: str,
) -> None:
    """Download *url* to *cache_path*, reusing the cache when present.

    Never raises: download failures are logged as warnings; the caller
    checks ``cache_path.is_file()`` afterwards and raises a clear
    ``ValueError`` if the download never succeeded — mirrors R's
    ``.smvi_download_once()``, which likewise swallows the error inside
    a ``tryCatch`` and lets the subsequent ``file.exists()`` check abort.
    """
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        if verbose:
            console.print(f"[green]OK[/]  {msg['cache_hit'].format(filename=label)}")
        return

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['download_file'].format(filename=label)}")

    ok, reason = _download_robust(url, cache_path, max_retries=3, verbose=False)
    if ok:
        if verbose:
            console.print(f"[green]OK[/]  {msg['download_done'].format(filename=label)}")
    else:
        cache_path.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(filename=label, err=reason or "unknown error")
        )


# ---------------------------------------------------------------------------
# Internal: municipality identifier column detection
# ---------------------------------------------------------------------------

def _smvi_detect_muni_col(municipalities: gpd.GeoDataFrame, msg: dict[str, str]) -> str:
    """Auto-detect the municipality identifier column in a GeoDataFrame."""
    for candidate in _MUNI_COL_CANDIDATES:
        if candidate in municipalities.columns:
            return candidate
    for col in municipalities.columns:
        if col == municipalities.geometry.name:
            continue
        sample = municipalities[col].dropna().astype(str).head(5)
        if len(sample) > 0 and sample.str.match(r"^\d{6,7}$").all():
            return col
    raise ValueError(
        "Could not detect a municipality identifier column. Expected one of: "
        f"{', '.join(_MUNI_COL_CANDIDATES)}."
    )
