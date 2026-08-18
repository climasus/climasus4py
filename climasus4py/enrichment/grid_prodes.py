"""grid_prodes.py — INPE PRODES annual deforestation data for Brazilian municipalities.

Mirrors R: sus_grid_prodes.R

Downloads INPE PRODES (Projeto de Monitoramento do Desmatamento na
Amazônia Legal por Satélite) annual deforestation polygons from the
TerraBrasilis WFS API, spatially intersects them with municipality
boundaries, and aggregates municipality x year deforestation area.
PRODES is a key exposure layer for frontier malaria (SINAN), dengue,
leishmaniasis, and biomass-burning respiratory disease research in
Brazil.

Not lazy — WFS/GeoJSON download and spatial intersection are
fundamentally geometry/row-oriented work with no natural DuckDB SQL
expression, and the R source itself never routes this through
Arrow/DuckDB either (it works directly with ``sf`` and returns a
materialised tibble). The Python port mirrors that: results are built
as a municipality x year ``pd.DataFrame`` (or a raw polygon
``pd.DataFrame`` when *municipalities* is ``None``) with metadata
attached via ``df.attrs["sus_meta"]``.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from tempfile import NamedTemporaryFile
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

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "prodes"

_ALL_BIOMES: tuple[str, ...] = (
    "Amazon", "Cerrado", "MataAtlantica", "Caatinga", "Pampa", "Pantanal",
)

_MUNI_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

_VALID_UF: tuple[str, ...] = (
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
)

# Verified from sus_grid_prodes.R (.prodes_wfs_config): base URL and
# layer name per biome, all on the same TerraBrasilis WFS server, no
# authentication required.
_WFS_BASE = "https://terrabrasilis.dpi.inpe.br/geoserver/wfs"
_WFS_CONFIG: dict[str, dict] = {
    "Amazon": {"typename": "prodes-amazon-nb:yearly_deforestation_biome", "min_year": 2007},
    "Cerrado": {"typename": "prodes-cerrado-nb:yearly_deforestation", "min_year": 2000},
    "MataAtlantica": {
        "typename": "prodes-mata-atlantica-nb:yearly_deforestation", "min_year": 2000,
    },
    "Caatinga": {"typename": "prodes-caatinga-nb:yearly_deforestation", "min_year": 2000},
    "Pampa": {"typename": "prodes-pampa-nb:yearly_deforestation", "min_year": 2000},
    "Pantanal": {"typename": "prodes-pantanal-nb:yearly_deforestation", "min_year": 2000},
}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados PRODES de Desmatamento (TerraBrasilis / INPE)",
        "missing_years": "'years' é obrigatório.",
        "invalid_years_type": "'years' deve ser numérico sem NA.",
        "invalid_years_range": (
            "'years' deve estar entre 1988 e o ano atual. Ano(s) inválido(s): {bad}."
        ),
        "invalid_biomes": "'biomes' inválido: {bad}. Use: {valid}.",
        "biome_years_warn": (
            "PRODES {biome} disponível a partir de {min_year}. Ignorando anos {bad}."
        ),
        "invalid_uf": "'uf' inválido: {bad}. Use: {valid}.",
        "need_geopandas": "O pacote geopandas é necessário para ler os polígonos do PRODES.",
        "muni_not_geodataframe": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "no_data_params": "Nenhum dado disponível para os parâmetros fornecidos.",
        "download_start": "Baixando {n_files} camada(s) PRODES do TerraBrasilis...",
        "parquet_cache_hit": "Todos os dados no cache Parquet. Carregando...",
        "parquet_hit": "Cache Parquet: {filename}",
        "parquet_write_warn": "Não foi possível salvar cache Parquet: {filename}",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename} ({n} polígonos)",
        "download_error": "Falha ao baixar {filename}: {err}",
        "no_data_year": "Nenhum polígono encontrado para {biome} {year}.",
        "spatial_start": "Calculando interseções com {n_mun} município(s)...",
        "intersect_warn": "Erro ao processar interseção para {biome} {year}: {err}",
        "no_data": "Nenhum dado foi processado com sucesso.",
        "done": "Concluído: {n_rows} observações ({n_mun} municípios).",
    },
    "en": {
        "title": "PRODES Deforestation Data (TerraBrasilis / INPE)",
        "missing_years": "'years' is required.",
        "invalid_years_type": "'years' must be numeric without NA.",
        "invalid_years_range": (
            "'years' must be between 1988 and the current year. Invalid year(s): {bad}."
        ),
        "invalid_biomes": "Invalid 'biomes': {bad}. Use: {valid}.",
        "biome_years_warn": "PRODES {biome} available from {min_year}. Skipping years {bad}.",
        "invalid_uf": "Invalid 'uf': {bad}. Use: {valid}.",
        "need_geopandas": "The geopandas package is required to read PRODES polygons.",
        "muni_not_geodataframe": "'municipalities' must be a geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "no_data_params": "No data available for the provided parameters.",
        "download_start": "Downloading {n_files} PRODES layer(s) from TerraBrasilis...",
        "parquet_cache_hit": "All data found in Parquet cache. Loading...",
        "parquet_hit": "Parquet cache: {filename}",
        "parquet_write_warn": "Could not write Parquet cache: {filename}",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename} ({n} polygons)",
        "download_error": "Failed to download {filename}: {err}",
        "no_data_year": "No polygons found for {biome} {year}.",
        "spatial_start": "Computing intersections with {n_mun} municipality/ies...",
        "intersect_warn": "Error processing intersection for {biome} {year}: {err}",
        "no_data": "No data was successfully processed.",
        "done": "Complete: {n_rows} observations ({n_mun} municipalities).",
    },
    "es": {
        "title": "Datos PRODES de Deforestación (TerraBrasilis / INPE)",
        "missing_years": "'years' es obligatorio.",
        "invalid_years_type": "'years' debe ser numérico sin NA.",
        "invalid_years_range": (
            "'years' debe estar entre 1988 y el año actual. Año(s) inválido(s): {bad}."
        ),
        "invalid_biomes": "'biomes' inválido: {bad}. Use: {valid}.",
        "biome_years_warn": "PRODES {biome} disponible desde {min_year}. Omitiendo años {bad}.",
        "invalid_uf": "'uf' inválido: {bad}. Use: {valid}.",
        "need_geopandas": "El paquete geopandas es necesario para leer los polígonos del PRODES.",
        "muni_not_geodataframe": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "no_data_params": "No hay datos disponibles para los parámetros indicados.",
        "download_start": "Descargando {n_files} capa(s) PRODES de TerraBrasilis...",
        "parquet_cache_hit": "Todos los datos en caché Parquet. Cargando...",
        "parquet_hit": "Caché Parquet: {filename}",
        "parquet_write_warn": "No se pudo guardar caché Parquet: {filename}",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename} ({n} polígonos)",
        "download_error": "Error al descargar {filename}: {err}",
        "no_data_year": "No se encontraron polígonos para {biome} {year}.",
        "spatial_start": "Calculando intersecciones con {n_mun} municipio(s)...",
        "intersect_warn": "Error al procesar intersección para {biome} {year}: {err}",
        "no_data": "No se procesó ningún dato correctamente.",
        "done": "Completo: {n_rows} observaciones ({n_mun} municipios).",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_prodes(
    years: int | list[int],
    biomes: list[str] | tuple[str, ...] = _ALL_BIOMES,
    uf: str | list[str] | None = None,
    municipalities: gpd.GeoDataFrame | None = None,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import INPE PRODES annual deforestation data for Brazilian municipalities.

    Downloads INPE PRODES annual deforestation polygons from the
    TerraBrasilis WFS API, spatially intersects them with municipality
    boundaries, and returns municipality-level annual deforestation
    areas compatible with ``sus_mod_dlnm`` / ``sus_climate_anomaly``
    (via ``sus_grid_join``).

    PRODES (Projeto de Monitoramento do Desmatamento na Amazônia Legal
    por Satélite) is INPE's primary deforestation monitoring program.
    Deforestation exposure is a key driver of frontier malaria (SINAN),
    dengue, leishmaniasis, and respiratory disease from biomass burning
    in Brazil.

    Args:
        years: Year(s) to download. Availability by biome: Amazon
            2007-present; all other biomes 2000-present. Must be
            between 1988 and the current year.
        biomes: Biome(s) to include. Any combination of ``"Amazon"``,
            ``"Cerrado"``, ``"MataAtlantica"``, ``"Caatinga"``,
            ``"Pampa"``, ``"Pantanal"``. Default: all six biomes.
        uf: Optional state filter (e.g. ``["MT", "PA"]``). Only used to
            tag the aggregated Parquet cache; the actual state cut is
            performed by the spatial intersection with
            *municipalities* below (the WFS ``state`` field is
            inconsistent between biomes and cannot be trusted for a
            server-side filter — see the "PRODES `state` field"
            note below). ``None`` (default) = no state tag.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from ``climasus-data`` municipality
            boundaries). When provided, deforestation polygons are
            spatially intersected with municipality boundaries and
            annual areas are aggregated. If ``None``, raw deforestation
            polygons are returned instead (no aggregation).
        use_cache: Reuse previously downloaded GeoJSON files and
            aggregated Parquet caches. Default ``True``.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/prodes``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        If *municipalities* is provided: a ``pd.DataFrame`` with columns
        ``code_muni`` (str), ``date`` (datetime, Jan 1 of the PRODES
        year), ``year`` (int), ``deforested_area_km2`` (float, total
        area of deforestation polygons intersecting the municipality),
        ``n_patches`` (int, count of distinct deforestation patches),
        and ``biome`` (str). Metadata in ``df.attrs["sus_meta"]``
        (``stage="climate"``, ``type="prodes"``).
        If *municipalities* is ``None``: a ``pd.DataFrame`` of raw
        deforestation polygon attributes with columns ``year``,
        ``state``, ``area_km``, and ``biome``.

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            processed for the given parameters.
        ImportError: If ``geopandas`` is not installed.

    PRODES year convention:
        A PRODES year N covers August 1 of year N-1 through July 31 of
        year N. The ``date`` column is set to ``YYYY-01-01`` by
        convention (the publication year). Most deforestation peaks
        occur during the dry season (July-October).

    Data source:
        INPE PRODES via TerraBrasilis WFS. No authentication required.
        All biomes accessed via
        https://terrabrasilis.dpi.inpe.br/geoserver/wfs

    Examples::

        import climasus4py as cs

        # Annual deforestation for Mato Grosso municipalities, Amazon biome
        prodes = cs.sus_grid_prodes(
            years=range(2015, 2023), biomes=["Amazon"], uf="MT",
            municipalities=mt_mun, lang="pt",
        )

        # Raw polygon data (no aggregation)
        prodes_raw = cs.sus_grid_prodes(years=2022, biomes=["Amazon"], uf="PA")
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- years ----------------------------------------------------------------
    if years is None:
        raise ValueError(msg["missing_years"])
    current_year = datetime.now().year
    raw_years = [years] if isinstance(years, int) else list(years)
    try:
        years_list = sorted({int(y) for y in raw_years})
    except (TypeError, ValueError) as exc:
        raise ValueError(msg["invalid_years_type"]) from exc
    bad_years = [y for y in years_list if y < 1988 or y > current_year]
    if bad_years:
        raise ValueError(
            msg["invalid_years_range"].format(bad=", ".join(str(y) for y in bad_years))
        )

    # --- biomes -----------------------------------------------------------------
    biomes_list = list(dict.fromkeys(biomes))  # unique, order-preserving
    bad_biomes = [b for b in biomes_list if b not in _WFS_CONFIG]
    if bad_biomes:
        raise ValueError(
            msg["invalid_biomes"].format(
                bad=", ".join(bad_biomes), valid=", ".join(_WFS_CONFIG)
            )
        )

    # Warn if requested years are before a biome's start year.
    for b in biomes_list:
        b_min = _WFS_CONFIG[b]["min_year"]
        early = [y for y in years_list if y < b_min]
        if early:
            console.print(
                "[yellow]WARN[/]  "
                + msg["biome_years_warn"].format(
                    biome=b, min_year=b_min, bad=", ".join(str(y) for y in early)
                )
            )

    # Remove years that have NO valid biome (all biomes skip those years).
    years_with_data = [
        y for y in years_list
        if any(y >= _WFS_CONFIG[b]["min_year"] for b in biomes_list)
    ]
    if not years_with_data:
        raise ValueError(msg["no_data_params"])
    years_list = years_with_data

    # --- uf -----------------------------------------------------------------------
    uf_list: list[str] | None = None
    if uf is not None:
        raw_uf = [uf] if isinstance(uf, str) else list(uf)
        uf_list = sorted({u.strip().upper() for u in raw_uf})
        bad_uf = [u for u in uf_list if u not in _VALID_UF]
        if bad_uf:
            raise ValueError(
                msg["invalid_uf"].format(bad=", ".join(bad_uf), valid=", ".join(_VALID_UF))
            )

    # --- geopandas is required unconditionally (reading WFS GeoJSON) --------------
    # Mirrors the R source's unconditional rlang::check_installed("sf") —
    # sf::st_read() is used to parse every downloaded GeoJSON regardless
    # of whether `municipalities` is provided.
    if find_spec("geopandas") is None:
        raise ImportError(f"{msg['need_geopandas']} Install it with: pip install geopandas")
    import geopandas as gpd

    if municipalities is not None and not isinstance(municipalities, gpd.GeoDataFrame):
        raise ValueError(msg["muni_not_geodataframe"])

    # --- use_cache / cache_dir -------------------------------------------------------
    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_path = Path(cache_dir).expanduser()

    # Aggregated Parquet cache key: the R source tags this only with
    # `uf` (a "sorted(uf)-joined" tag, or "all"); two calls with the
    # same `uf` but *different* `municipalities` sets silently reuse
    # the wrong aggregated cache — the same cross-cache correctness bug
    # already found (and fixed) in sus_grid_pdsi's Parquet cache key.
    # Fixed here by also hashing the sorted code_muni values, mirroring
    # grid_chirps.py's muni_hash pattern.
    uf_tag = "-".join(uf_list) if uf_list else "all"
    muni_hash = ""
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities, msg)
        codes = sorted(str(c) for c in municipalities[muni_col])
        muni_hash = "_" + hashlib.md5("|".join(codes).encode("utf-8")).hexdigest()[:10]

    # --- build manifest (biome x year) ---------------------------------------------
    manifest = _build_manifest(biomes_list, years_list, cache_path, uf_tag, muni_hash)
    if manifest.empty:
        raise ValueError(msg["no_data_params"])

    n_files = len(manifest)
    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files))

    # --- Parquet early-return ----------------------------------------------------
    if municipalities is not None and use_cache and all(
        Path(p).is_file() for p in manifest["cache_pq"]
    ):
        if verbose:
            console.print("[green]OK[/]  " + msg["parquet_cache_hit"])
        return _build_from_parquet(manifest["cache_pq"].tolist(), verbose, msg)

    # --- prepare municipalities once -----------------------------------------------
    muni_slim = None
    n_mun = 0
    if municipalities is not None:
        muni_col = _detect_muni_col(municipalities, msg)
        muni = municipalities.copy()
        muni["code_muni"] = muni[muni_col].astype(str).str.slice(0, 7)
        muni = muni.set_geometry(muni.geometry.make_valid())
        muni = muni.to_crs(epsg=4326)
        muni_slim = muni[["code_muni", muni.geometry.name]]
        n_mun = len(muni)
        if verbose:
            console.print("[cyan]INFO[/]  " + msg["spatial_start"].format(n_mun=n_mun))

    # --- download + process each biome/year -----------------------------------------
    result_frames: list[pd.DataFrame] = []
    for row in manifest.itertuples(index=False):
        # Per-row Parquet cache hit.
        if (
            municipalities is not None
            and use_cache
            and Path(row.cache_pq).is_file()
            and Path(row.cache_pq).stat().st_size > 0
        ):
            if verbose:
                console.print(
                    "[green]OK[/]  " + msg["parquet_hit"].format(filename=Path(row.cache_pq).name)
                )
            result_frames.append(pd.read_parquet(row.cache_pq))
            continue

        defor = _fetch_wfs(row.biome, row.year, Path(row.cache_json), use_cache, verbose, msg)
        if defor is None or len(defor) == 0:
            if verbose:
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["no_data_year"].format(biome=row.biome, year=row.year)
                )
            continue

        if "area_km" not in defor.columns:
            defor["area_km"] = _geodesic_area_km2(defor.geometry)
        defor["biome"] = row.biome
        defor["year"] = int(row.year)

        if municipalities is None:
            keep_cols = [
                c for c in ("year", "state", "area_km", "biome", "uid") if c in defor.columns
            ]
            result_frames.append(pd.DataFrame(defor[keep_cols]))
            continue

        df_i = _intersect_and_aggregate(
            defor, muni_slim, row.biome, row.year, verbose, msg
        )
        if df_i is not None and len(df_i) > 0:
            pq_path = Path(row.cache_pq)
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

    result = pd.concat(result_frames, ignore_index=True)
    # Mirrors R's order(biome, year, if (!is.null(municipalities)) code_muni else state):
    # R's order() silently drops a NULL sort key, so fall back gracefully when
    # `state` isn't present in the raw-mode result (some WFS layers omit it).
    sort_cols = ["biome", "year"] + (["code_muni"] if municipalities is not None else ["state"])
    sort_cols = [c for c in sort_cols if c in result.columns]
    result = result.sort_values(sort_cols).reset_index(drop=True)

    n_rows = len(result)
    n_mun_out = result["code_muni"].nunique() if municipalities is not None else None
    if verbose:
        console.print(
            "[green]OK[/]  " + msg["done"].format(n_rows=n_rows, n_mun=n_mun_out)
        )

    # --- attach metadata --------------------------------------------------------------
    now = datetime.now()
    if municipalities is not None:
        temporal_start, temporal_end = result["date"].min(), result["date"].max()
    else:
        temporal_start = pd.Timestamp(year=int(result["year"].min()), month=1, day=1)
        temporal_end = pd.Timestamp(year=int(result["year"].max()), month=1, day=1)

    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "prodes",
        "spatial": False,
        "temporal": {
            "start": temporal_start,
            "end": temporal_end,
            "unit": "year",
            "source": "terrabrasilis_inpe_prodes",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "biomes": biomes_list,
        "years": years_list,
        "uf": uf_list,
        "n_municipalities": n_mun if municipalities is not None else None,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_prodes(): biomes={'+'.join(biomes_list)}, "
            f"years={min(years_list)}-{max(years_list)}, {n_rows} obs"
        ],
        "user": {},
    }
    return result


# ---------------------------------------------------------------------------
# Internal: download manifest (biome x year)
# ---------------------------------------------------------------------------

def _build_manifest(
    biomes: list[str],
    years: list[int],
    cache_path: Path,
    uf_tag: str,
    muni_hash: str,
) -> pd.DataFrame:
    """Build one row per biome/year combination to download/process.

    ``cache_json`` (raw WFS GeoJSON) is national and shared across
    different ``uf``/``municipalities`` calls, since it holds the whole
    biome/year layer — mirrors the R source's comment that the WFS
    ``state`` field is unreliable for a server-side cut, so no
    per-uf/per-muni tag belongs on that cache. ``cache_pq`` (the
    aggregated result) does carry both the `uf` tag and the muni_hash
    fix (see the correctness note in ``sus_grid_prodes``).
    """
    rows: list[dict] = []
    for b in biomes:
        b_min = _WFS_CONFIG[b]["min_year"]
        for yr in years:
            if yr < b_min:
                continue
            gjson_fn = f"prodes_{b.lower()}_{yr:04d}.geojson"
            pq_fn = f"prodes_{b.lower()}_{yr:04d}_{uf_tag}{muni_hash}.parquet"
            rows.append({
                "biome": b,
                "year": yr,
                "cache_json": cache_path / "geojson" / gjson_fn,
                "cache_pq": cache_path / "parquet" / pq_fn,
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
# Internal: geodesic area (mirrors sf::st_area on lon/lat geometries)
# ---------------------------------------------------------------------------

def _geodesic_area_km2(geometries) -> pd.Series:
    """Compute geodesic (WGS84 ellipsoid) area in km^2 for each geometry.

    ``sf::st_area()`` on geographic (lon/lat) geometries computes a
    geodesic area (via S2) rather than a naive planar one. ``pyproj``
    (already a hard dependency of ``geopandas``) exposes the same
    WGS84-ellipsoid area calculation via ``Geod.geometry_area_perimeter``,
    used here instead of reprojecting to an arbitrary equal-area CRS.
    """
    from pyproj import Geod

    geod = Geod(ellps="WGS84")
    areas = []
    for geom in geometries:
        if geom is None or geom.is_empty:
            areas.append(0.0)
            continue
        area_m2, _ = geod.geometry_area_perimeter(geom)
        areas.append(abs(area_m2) / 1e6)
    return pd.Series(areas, index=geometries.index if hasattr(geometries, "index") else None)


# ---------------------------------------------------------------------------
# Internal: WFS download with GeoJSON cache
# ---------------------------------------------------------------------------

def _fetch_wfs(
    biome: str, year: int, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> gpd.GeoDataFrame | None:
    """Download PRODES deforestation polygons for one biome/year via WFS.

    No CQL server-side state filter is applied — mirrors the R source's
    documented decision (the WFS ``state`` field is inconsistent between
    biomes, sometimes an abbreviation, sometimes a full name — a
    server-side filter would silently drop half the states). The
    correct state cut is the spatial intersection with
    ``municipalities`` performed by the caller.
    """
    import geopandas as gpd

    filename = cache_path.name
    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 1000:
        if verbose:
            console.print("[green]OK[/]  " + msg["cache_hit"].format(filename=filename))
        return gpd.read_file(cache_path)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cfg = _WFS_CONFIG[biome]
    from urllib.parse import urlencode

    query = urlencode({
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": cfg["typename"],
        "CQL_FILTER": f"year={int(year)}",
        "outputFormat": "application/json",
    })
    url = f"{_WFS_BASE}?{query}"

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["download_file"].format(filename=f"PRODES {biome} {year}")
        )

    with NamedTemporaryFile(suffix=".geojson", delete=False) as f:
        tmp = Path(f.name)
    tmp.unlink(missing_ok=True)  # _download_robust expects a fresh (non-existent) dest
    ok, reason = _download_robust(url, tmp, max_retries=3, verbose=False)
    if not ok:
        tmp.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(
                filename=f"PRODES {biome} {year}", err=reason or "unknown error"
            )
        )
        return None

    try:
        defor = gpd.read_file(tmp)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        console.print(
            "[yellow]WARN[/]  "
            + msg["download_error"].format(filename=f"PRODES {biome} {year}", err=str(e))
        )
        return None

    if len(defor) > 0:
        tmp.replace(cache_path)
        if verbose:
            console.print(
                "[green]OK[/]  "
                + msg["download_done"].format(filename=f"PRODES {biome} {year}", n=len(defor))
            )
    tmp.unlink(missing_ok=True)
    return defor


# ---------------------------------------------------------------------------
# Internal: spatial intersection + per-municipality aggregation
# ---------------------------------------------------------------------------

def _intersect_and_aggregate(
    defor: gpd.GeoDataFrame,
    muni_slim: gpd.GeoDataFrame,
    biome: str,
    year: int,
    verbose: bool,
    msg: dict[str, str],
) -> pd.DataFrame | None:
    """Intersect one biome/year's deforestation polygons with municipalities.

    Returns ``None`` (not an error) when the intersection is empty —
    mirrors the R source's explicit comment: yielding the block value
    ``NULL`` inside ``tryCatch({...})`` only skips this one item, while
    an early ``return(NULL)`` there would abort ``sus_grid_prodes()``'s
    entire outer loop on the first biome/UF combination with no data
    (e.g. Amazon x DF) instead of continuing to the next biome/year.
    """
    import geopandas as gpd

    try:
        defor_clean = defor.set_geometry(defor.geometry.make_valid())
        defor_clean = defor_clean.to_crs(epsg=4326)

        intersection = gpd.overlay(defor_clean, muni_slim, how="intersection")
        if len(intersection) == 0:
            return None

        intersection["area_km2_intersected"] = _geodesic_area_km2(intersection.geometry)
        has_uid = "uid" in intersection.columns

        if has_uid:
            agg = intersection.groupby(["code_muni", "year"], as_index=False).agg(
                deforested_area_km2=("area_km2_intersected", "sum"),
                n_patches=("uid", "nunique"),
            )
        else:
            agg = intersection.groupby(["code_muni", "year"], as_index=False).agg(
                deforested_area_km2=("area_km2_intersected", "sum"),
                n_patches=("area_km2_intersected", "size"),
            )
        agg["biome"] = biome
        agg["date"] = pd.Timestamp(year=int(year), month=1, day=1)
        agg["n_patches"] = agg["n_patches"].astype("int64")
        return agg[["code_muni", "date", "year", "deforested_area_km2", "n_patches", "biome"]]
    except Exception as e:  # noqa: BLE001 - mirrors R's tryCatch(..., error=)
        console.print(
            "[yellow]WARN[/]  " + msg["intersect_warn"].format(biome=biome, year=year, err=str(e))
        )
        return None


# ---------------------------------------------------------------------------
# Internal: assemble result from pre-existing Parquet caches (early return)
# ---------------------------------------------------------------------------

def _build_from_parquet(pq_paths: list[str], verbose: bool, msg: dict[str, str]) -> pd.DataFrame:
    """Assemble the output DataFrame purely from cached aggregated Parquet files."""
    parts = []
    for p in pq_paths:
        try:
            parts.append(pd.read_parquet(p))
        except Exception:
            continue

    if not parts:
        raise ValueError(msg["no_data"])

    result = pd.concat(parts, ignore_index=True)
    result = result.sort_values(["biome", "year", "code_muni"]).reset_index(drop=True)

    n_rows = len(result)
    n_mun = result["code_muni"].nunique()
    if verbose:
        console.print("[green]OK[/]  " + msg["done"].format(n_rows=n_rows, n_mun=n_mun))

    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "prodes",
        "spatial": False,
        "temporal": {
            "start": result["date"].min(),
            "end": result["date"].max(),
            "source": "terrabrasilis_cache",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "n_municipalities": n_mun,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_grid_prodes(): from Parquet cache, {n_rows} obs"
        ],
        "user": {},
    }
    return result
