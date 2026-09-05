"""grid_fires.py — fire hotspot (active fire) data for Brazilian municipalities.

Mirrors R: sus_grid_fires.R

Unlike the other ``sus_grid_*`` functions, this is **not** a pre-processed
raster/Parquet dataset pulled from a fixed Zenodo/GitHub archive — the R
source queries two live point-based APIs directly, one month at a time,
with each monthly response cached to disk as CSV:

- ``"inpe"`` (default) — INPE Queimadas focos API
  (``https://queimadas.dgi.inpe.br/api/focos/``), no authentication,
  1998-present, Brazil only. Documented dataset DOI: 10.2312/inpe.2022.009.
- ``"firms_modis"`` / ``"firms_viirs"`` — NASA FIRMS area API
  (``https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/{product}/...``),
  requires a free MAP KEY, global, queried in <=10-day chunks.

When *municipalities* is provided, fire points are assigned to polygons via
a point-in-polygon spatial join (``geopandas.sjoin(..., predicate="within")``,
the same operation as R's ``sf::st_join(..., join = st_within)``) and
aggregated to municipality x day counts + mean Fire Radiative Power (FRP).
This is point-in-polygon aggregation, not raster zonal statistics — no
``exactextract``/``rioxarray`` dependency is needed for this function.

Not lazy: the R source never routes fire data through Arrow/DuckDB either
(it works entirely with in-memory data.frames/sf objects), so the Python
port returns a materialised ``pd.DataFrame`` at the API edge, matching
``sus_climate_uniplu``'s precedent for "small aggregate result" imports.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd
from rich.console import Console

from ..core.climate_inmet import _VALID_UFS

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Renamed from R's "~/.climasus4r_cache/fires", mirroring the established
# climasus4py_cache rename already used by sus_climate_inmet/sus_climate_uniplu.
_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "fires"

_VALID_SOURCES: tuple[str, ...] = ("inpe", "firms_modis", "firms_viirs")
_MIN_YEAR: dict[str, int] = {"inpe": 1998, "firms_modis": 2000, "firms_viirs": 2012}
_VALID_BIOMES: tuple[str, ...] = (
    "Amazonia", "Cerrado", "Mata Atlantica", "Caatinga", "Pampa", "Pantanal",
)

_INPE_BASE_URL = "https://queimadas.dgi.inpe.br/api/focos/"
_FIRMS_BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

# Approximate bounding boxes for Brazilian states (WGS84: xmin, ymin, xmax, ymax).
# Ported verbatim from R's .fires_uf_bbox() — only used as a FIRMS API bbox
# query parameter, not a metadata lookup, so it stays local to this module.
_UF_BBOX: dict[str, tuple[float, float, float, float]] = {
    "AC": (-74, -11, -66, -7), "AL": (-38, -10, -35, -8),
    "AP": (-52, 1, -49, 4), "AM": (-74, -9, -57, 2),
    "BA": (-46, -18, -37, -8), "CE": (-41, -8, -37, -3),
    "DF": (-48, -16, -47, -15), "ES": (-42, -21, -39, -18),
    "GO": (-53, -19, -45, -13), "MA": (-48, -6, -41, -1),
    "MT": (-61, -18, -50, -7), "MS": (-58, -24, -51, -17),
    "MG": (-51, -23, -39, -14), "PA": (-59, -9, -46, 2),
    "PB": (-39, -8, -34, -6), "PR": (-54, -27, -48, -22),
    "PE": (-41, -9, -34, -7), "PI": (-46, -9, -40, -2),
    "RJ": (-45, -23, -40, -21), "RN": (-38, -7, -34, -4),
    "RS": (-54, -34, -49, -27), "RO": (-66, -14, -59, -7),
    "RR": (-61, 1, -58, 5), "SC": (-54, -30, -48, -25),
    "SP": (-53, -25, -44, -19), "SE": (-38, -11, -36, -9),
    "TO": (-50, -13, -45, -5),
}

_ALL_MONTHS: tuple[int, ...] = tuple(range(1, 13))

_MUNI_ID_CANDIDATES: tuple[str, ...] = (
    "code_muni", "CD_MUN", "CD_GEOCMU", "code_municipality",
)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados de Focos de Incendio",
        "no_firms_key": (
            "FIRMS MAP KEY não encontrada. Defina a variável de ambiente "
            "FIRMS_MAP_KEY ou use 'firms_key'."
        ),
        "need_geopandas": "O pacote geopandas é necessário para agregação espacial.",
        "muni_not_gdf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "download_start": "Baixando dados de {n_months} mes(es)...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename} ({n} registros)",
        "download_error": "Falha ao baixar {filename}: {err}",
        "no_fires_period": "Nenhum foco encontrado para {filename}",
        "read_warn": "Não foi possível ler {filename}.",
        "no_data": "Nenhum dado de foco encontrado para os parâmetros fornecidos.",
        "points_loaded": "{n_points} foco(s) carregado(s).",
        "spatial_join": "Atribuindo focos a {n_mun} município(s)...",
        "no_fires_in_muni": "Nenhum foco encontrado dentro dos polígonos fornecidos.",
        "agg_done": "Concluído: {n_rows} observações ({n_mun} municípios).",
        "done_points": "Concluído: {n_rows} focos retornados.",
    },
    "en": {
        "title": "Fire Hotspot Data",
        "no_firms_key": (
            "FIRMS MAP KEY not found. Set the FIRMS_MAP_KEY environment "
            "variable or use 'firms_key'."
        ),
        "need_geopandas": "The geopandas package is required for spatial aggregation.",
        "muni_not_gdf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "download_start": "Downloading data for {n_months} month(s)...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename} ({n} records)",
        "download_error": "Failed to download {filename}: {err}",
        "no_fires_period": "No fire hotspots found for {filename}",
        "read_warn": "Could not read {filename}.",
        "no_data": "No fire data found for the provided parameters.",
        "points_loaded": "{n_points} hotspot(s) loaded.",
        "spatial_join": "Assigning hotspots to {n_mun} municipality/ies...",
        "no_fires_in_muni": "No hotspots found within the provided polygons.",
        "agg_done": "Complete: {n_rows} observations ({n_mun} municipalities).",
        "done_points": "Complete: {n_rows} fire hotspots returned.",
    },
    "es": {
        "title": "Datos de Focos de Incendio",
        "no_firms_key": (
            "FIRMS MAP KEY no encontrado. Configure la variable de entorno "
            "FIRMS_MAP_KEY o use 'firms_key'."
        ),
        "need_geopandas": "El paquete geopandas es necesario para la agregación espacial.",
        "muni_not_gdf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "download_start": "Descargando datos de {n_months} mes(es)...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename} ({n} registros)",
        "download_error": "Error al descargar {filename}: {err}",
        "no_fires_period": "Ningún foco encontrado para {filename}",
        "read_warn": "No se pudo leer {filename}.",
        "no_data": "Ningún dato de foco encontrado para los parámetros indicados.",
        "points_loaded": "{n_points} foco(s) cargado(s).",
        "spatial_join": "Asignando focos a {n_mun} municipio(s)...",
        "no_fires_in_muni": "Ningún foco encontrado dentro de los polígonos proporcionados.",
        "agg_done": "Completo: {n_rows} observaciones ({n_mun} municipios).",
        "done_points": "Completo: {n_rows} focos de incendio retornados.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_fires(
    years: int | list[int],
    months: list[int] | range | None = None,
    uf: str | list[str] | None = None,
    bbox: tuple[float, float, float, float] | list[float] | None = None,
    source: Literal["inpe", "firms_modis", "firms_viirs"] = "inpe",
    municipalities: object | None = None,
    agg_fun: str = "count",
    biome: str | list[str] | None = None,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    firms_key: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import fire hotspot (active fire) data for Brazilian municipalities.

    Downloads fire hotspot data from INPE Queimadas or NASA FIRMS,
    optionally aggregates counts and Fire Radiative Power (FRP) to
    Brazilian municipalities via point-in-polygon spatial join, and
    returns a ``pd.DataFrame`` with ``.attrs["sus_meta"]``.

    Sources:
        - ``"inpe"`` (default): INPE Queimadas portal. No authentication.
          Historical data from 1998 to present. Brazil only.
        - ``"firms_modis"``: NASA FIRMS MODIS Collection 6.1. Requires a
          free MAP KEY. Global, 1 km resolution, 2000 to present.
        - ``"firms_viirs"``: NASA FIRMS VIIRS SNPP. Same key as
          ``"firms_modis"``. 375 m resolution, 2012 to present.

    Args:
        years: Year or list of years. ``"inpe"``: 1998-present.
            ``"firms_modis"``: 2000-present. ``"firms_viirs"``:
            2012-present.
        months: Month(s), 1-12. ``None`` (default) means all 12 months.
        uf: Brazilian state code(s), e.g. ``"MT"`` or ``["MT", "PA"]``.
            Case-insensitive. ``None`` = all Brazil. Filters INPE
            queries and, when *bbox* is not given, derives a FIRMS bbox.
        bbox: ``(xmin, ymin, xmax, ymax)`` in WGS84 lon/lat, used by
            FIRMS for spatial subsetting. If ``None`` and *uf* is given,
            derived from Brazilian state bounding boxes. If both are
            ``None``, Brazil's approximate bbox is used.
        source: Data source: ``"inpe"`` (default), ``"firms_modis"``, or
            ``"firms_viirs"``.
        municipalities: A ``geopandas.GeoDataFrame`` of municipality
            polygons (e.g. from the municipality boundaries used
            elsewhere in climasus4py). When provided, fire points are
            spatially joined to polygons and aggregated to daily
            municipality-level counts. If ``None``, raw fire points are
            returned instead.
        agg_fun: Aggregation strategy when *municipalities* is provided.
            Only ``"count"`` is currently supported (count of hotspots
            per municipality per day, plus mean FRP).
        biome: INPE biome filter — one or more of ``"Amazonia"``,
            ``"Cerrado"``, ``"Mata Atlantica"``, ``"Caatinga"``,
            ``"Pampa"``, ``"Pantanal"``. ``None`` = no filter.
        use_cache: If ``True`` (default), reuse previously downloaded
            CSV files. Set ``False`` to force re-download.
        cache_dir: Root cache directory. Default
            ``~/.climasus4py_cache/fires``.
        firms_key: NASA FIRMS MAP KEY. Defaults to the ``FIRMS_MAP_KEY``
            environment variable. Required when
            ``source in ("firms_modis", "firms_viirs")``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress. Default ``True``.

    Returns:
        If *municipalities* is provided: a DataFrame with columns
        ``code_muni``, ``date``, ``n_fires`` (hotspot count),
        ``frp_mean`` (mean Fire Radiative Power in MW, ``NaN`` when not
        available). Metadata: ``stage="climate"``, ``type="fires"``.
        If *municipalities* is ``None``: a DataFrame with raw fire point
        columns ``date``, ``lat``, ``lon``, ``frp``, ``biome``,
        ``estado``, ``source``. Metadata via ``df.attrs["sus_meta"]``.

    Raises:
        ValueError: If any parameter is invalid, the FIRMS key is
            missing, no fire data is found, or ``municipalities`` is not
            a ``geopandas.GeoDataFrame``.

    Examples::

        import climasus4py as cs

        # INPE fire count for Mato Grosso municipalities, fire season 2020
        fires = cs.sus_grid_fires(
            years=2020, months=range(7, 11), uf="MT",
            municipalities=mt_mun, lang="pt",
        )

        # Raw fire points for Amazonia, no aggregation
        fires_pts = cs.sus_grid_fires(years=2022, months=[8], biome="Amazonia")
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    if source not in _VALID_SOURCES:
        raise ValueError(f"'source' must be one of {_VALID_SOURCES}, got {source!r}.")

    # --- years ------------------------------------------------------------
    if years is None:
        raise ValueError("'years' is required.")
    years_list = [years] if isinstance(years, int) else list(years)
    if not years_list or any(y is None for y in years_list):
        raise ValueError("'years' must be numeric without missing values.")
    years_list = sorted({int(y) for y in years_list})

    current_year = datetime.now().year
    min_year = _MIN_YEAR[source]
    bad_years = [y for y in years_list if y < min_year or y > current_year]
    if bad_years:
        # Preserved R quirk: the error message only names the lower bound
        # even though the check also rejects years above the current year.
        raise ValueError(
            f"'years' must be >= {min_year}. Invalid year(s): {bad_years}."
        )

    # --- months -------------------------------------------------------------
    if months is None:
        months = _ALL_MONTHS
    months_list = sorted({int(m) for m in months})
    if any(m < 1 or m > 12 for m in months_list):
        raise ValueError("'months' must be integers between 1 and 12.")

    # --- uf -------------------------------------------------------------------
    uf_list: list[str] | None = None
    if uf is not None:
        raw_uf = [uf] if isinstance(uf, str) else list(uf)
        uf_list = [u.upper().strip() for u in raw_uf]
        bad_uf = sorted(set(uf_list) - _VALID_UFS)
        if bad_uf:
            raise ValueError(f"Invalid 'uf': {bad_uf}. Valid codes: {sorted(_VALID_UFS)}.")

    # --- biome ------------------------------------------------------------------
    biome_list: list[str] | None = None
    if biome is not None:
        biome_list = [biome] if isinstance(biome, str) else list(biome)
        bad_biome = sorted(set(biome_list) - set(_VALID_BIOMES))
        if bad_biome:
            raise ValueError(f"Invalid 'biome': {bad_biome}. Valid values: {_VALID_BIOMES}.")

    # --- firms key ----------------------------------------------------------
    if source.startswith("firms"):
        if not firms_key:
            firms_key = os.environ.get("FIRMS_MAP_KEY", "")
        if not firms_key:
            raise ValueError(msg["no_firms_key"])

    # --- municipalities -------------------------------------------------------
    if municipalities is not None:
        try:
            import geopandas as gpd
        except ImportError as exc:
            raise ImportError(
                f"{msg['need_geopandas']} Install with: pip install climasus4py[spatial]"
            ) from exc
        if not isinstance(municipalities, gpd.GeoDataFrame):
            raise ValueError(msg["muni_not_gdf"])

    if not isinstance(use_cache, bool):
        raise ValueError("'use_cache' must be True or False.")

    if not str(cache_dir).strip():
        raise ValueError("'cache_dir' must be a non-empty string.")
    cache_path = Path(cache_dir).expanduser()

    # --- bbox: provided, derived from uf, or Brazil default ---------------------
    if source.startswith("firms") and bbox is None:
        bbox = _fires_uf_bbox(uf_list) if uf_list else (-75, -35, -28, 6)

    # --- build manifest (year x month) -----------------------------------------
    manifest: list[dict] = []
    for yr in years_list:
        for mo in months_list:
            last_day = _days_in_month(yr, mo)
            manifest.append({
                "year": yr,
                "month": mo,
                "date_start": f"{yr:04d}-{mo:02d}-01",
                "date_end": f"{yr:04d}-{mo:02d}-{last_day:02d}",
                "cache_path": cache_path / source / f"{yr:04d}_{mo:02d}.csv",
            })

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(f"[cyan]INFO[/]  {msg['download_start'].format(n_months=len(manifest))}")

    # --- download with cache -----------------------------------------------
    for row in manifest:
        _fires_download_month(
            source=source,
            date_start=row["date_start"],
            date_end=row["date_end"],
            cache_path=row["cache_path"],
            uf=uf_list,
            biome=biome_list,
            bbox=bbox,
            firms_key=firms_key,
            use_cache=use_cache,
            verbose=verbose,
            msg=msg,
        )

    # --- read and combine all CSV files -----------------------------------
    frames: list[pd.DataFrame] = []
    for row in manifest:
        fp: Path = row["cache_path"]
        if not fp.is_file() or fp.stat().st_size == 0:
            continue
        try:
            frames.append(pd.read_csv(fp, encoding="utf-8"))
        except Exception:
            if verbose:
                console.print(f"[yellow]WARN[/]  {msg['read_warn'].format(filename=fp.name)}")

    if not frames:
        raise ValueError(msg["no_data"])

    raw_df = pd.concat(frames, ignore_index=True)
    raw_df = _fires_normalize_cols(raw_df, source)

    n_points = len(raw_df)
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['points_loaded'].format(n_points=n_points)}")

    # --- raw points (no municipalities) --------------------------------------
    if municipalities is None:
        raw_df["source"] = source
        keep_cols = [
            c for c in ("date", "lat", "lon", "frp", "biome", "estado", "source")
            if c in raw_df.columns
        ]
        result = raw_df[keep_cols].sort_values("date").reset_index(drop=True)

        if verbose:
            console.print(f"[green]OK[/]  {msg['done_points'].format(n_rows=len(result))}")

        result.attrs["sus_meta"] = _fires_build_meta(
            result, years_list, months_list, source, uf_list, biome_list,
            n_points, len(result),
        )
        return result

    # --- point-in-polygon spatial join to municipalities --------------------
    # Preserved R quirk: 'agg_fun' is accepted but never actually read in the
    # R source (only "count" behavior is implemented regardless of its
    # value) — replicated as-is rather than hardening it into a ValueError.
    _ = agg_fun

    if verbose:
        console.print(
            f"[cyan]INFO[/]  {msg['spatial_join'].format(n_mun=len(municipalities))}"
        )

    raw_df = raw_df.dropna(subset=["lat", "lon"])

    import geopandas as gpd

    fire_gdf = gpd.GeoDataFrame(
        raw_df,
        geometry=gpd.points_from_xy(raw_df["lon"], raw_df["lat"]),
        crs="EPSG:4326",
    )

    muni_id_col = _fires_detect_muni_col(municipalities)
    muni = municipalities.copy()
    muni["code_muni"] = muni[muni_id_col].astype(str).str[:7]
    muni = muni.to_crs("EPSG:4326")
    muni_slim = muni[["code_muni", muni.geometry.name]]

    fire_joined = gpd.sjoin(fire_gdf, muni_slim, predicate="within", how="inner")

    if fire_joined.empty:
        if verbose:
            console.print(f"[yellow]WARN[/]  {msg['no_fires_in_muni']}")
        result = pd.DataFrame({
            "code_muni": pd.Series(dtype="object"),
            "date": pd.Series(dtype="datetime64[ns]"),
            "n_fires": pd.Series(dtype="int64"),
            "frp_mean": pd.Series(dtype="float64"),
        })
        result.attrs["sus_meta"] = _fires_build_meta(
            result, years_list, months_list, source, uf_list, biome_list, n_points, 0,
        )
        return result

    fire_plain = pd.DataFrame(fire_joined.drop(columns=[fire_joined.geometry.name]))

    grouped = fire_plain.groupby(["code_muni", "date"], as_index=False)
    result = grouped.agg(
        n_fires=("code_muni", "size"),
        frp_mean=("frp", lambda s: np.nan if s.isna().all() else s.mean()),
    )
    result["n_fires"] = result["n_fires"].astype("int64")
    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        n_mun = result["code_muni"].nunique()
        console.print(f"[green]OK[/]  {msg['agg_done'].format(n_rows=n_rows, n_mun=n_mun)}")

    result.attrs["sus_meta"] = _fires_build_meta(
        result, years_list, months_list, source, uf_list, biome_list, n_points, n_rows,
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _days_in_month(year: int, month: int) -> int:
    """Return the number of days in *month* of *year* (stdlib only)."""
    next_month_first = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return (next_month_first - date(year, month, 1)).days


def _fires_uf_bbox(uf_list: list[str]) -> tuple[float, float, float, float]:
    """Merge the bounding boxes of the requested UFs (ported from R's .fires_uf_bbox)."""
    boxes = [_UF_BBOX[u] for u in uf_list]
    xmin = min(b[0] for b in boxes)
    ymin = min(b[1] for b in boxes)
    xmax = max(b[2] for b in boxes)
    ymax = max(b[3] for b in boxes)
    return (xmin, ymin, xmax, ymax)


def _fires_firms_product(source: str) -> str:
    """FIRMS product name per source (ported from R's .fires_firms_product)."""
    return {"firms_modis": "MODIS_C6_1", "firms_viirs": "VIIRS_SNPP_SP"}.get(source, "MODIS_C6_1")


def _fires_normalize_cols(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Normalize raw INPE/FIRMS columns to the canonical fire schema.

    Ported from R's .fires_normalize_cols(): INPE exposes
    lat/lon/datahora/bioma/estado/frp directly; FIRMS exposes
    latitude/longitude/acq_date/frp.
    """
    df = df.copy()
    if source == "inpe":
        if "datahora" in df.columns:
            df["date"] = pd.to_datetime(df["datahora"].astype(str).str[:10], errors="coerce")
        elif "data" in df.columns:
            df["date"] = pd.to_datetime(df["data"], errors="coerce")
        if "bioma" in df.columns:
            df["biome"] = df["bioma"]
    else:
        if "latitude" in df.columns:
            df["lat"] = df["latitude"]
        if "longitude" in df.columns:
            df["lon"] = df["longitude"]
        if "acq_date" in df.columns:
            df["date"] = pd.to_datetime(df["acq_date"], errors="coerce")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce") if "lat" in df.columns else np.nan
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce") if "lon" in df.columns else np.nan
    df["date"] = pd.to_datetime(df["date"], errors="coerce") if "date" in df.columns else pd.NaT
    df["frp"] = pd.to_numeric(df["frp"], errors="coerce") if "frp" in df.columns else np.nan
    return df


def _fires_detect_muni_col(municipalities) -> str:
    """Auto-detect the municipality identifier column in a GeoDataFrame.

    Ported from R's .fires_detect_muni_col(): tries known candidate
    names first, then scans for a column whose first values look like
    6-7 digit IBGE codes.
    """
    found = [c for c in _MUNI_ID_CANDIDATES if c in municipalities.columns]
    if found:
        return found[0]
    for col in municipalities.columns:
        vals = municipalities[col].dropna().astype(str).head(5)
        if len(vals) > 0 and vals.str.match(r"^\d{6,7}$").all():
            return col
    raise ValueError(
        "Could not detect a municipality identifier column. "
        f"Expected one of: {', '.join(_MUNI_ID_CANDIDATES)}."
    )


def _fires_build_meta(
    result: pd.DataFrame,
    years: list[int],
    months: list[int],
    source: str,
    uf: list[str] | None,
    biome: list[str] | None,
    n_raw_points: int,
    n_obs: int,
) -> dict:
    """Build the sus_meta dict for sus_grid_fires output (ported from R's .fires_build_meta)."""
    now = datetime.now()
    date_col = result["date"] if "date" in result.columns else pd.Series(dtype="datetime64[ns]")
    return {
        "system": None,
        "stage": "climate",
        "type": "fires",
        # Preserved R quirk: spatial is always False in the source metadata,
        # even for the municipality-aggregated (spatially joined) output.
        "spatial": False,
        "temporal": {
            "start": date_col.min() if len(result) > 0 else None,
            "end": date_col.max() if len(result) > 0 else None,
            "unit": "day",
            "source": source,
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "years": years,
        "months": months,
        "source": source,
        "uf": uf,
        "biome": biome,
        "n_raw_points": n_raw_points,
        "n_observations": n_obs,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_grid_fires(): "
            f"source={source}, {n_raw_points} raw points, {n_obs} obs"
        ],
        "user": {},
    }


def _http_get_text(
    url: str,
    params: dict | None = None,
    timeout: int = 60,
    max_retries: int = 3,
) -> str:
    """GET *url* (optionally with query *params*), retrying up to *max_retries* times.

    Uses ``requests`` when available (matches the rest of the codebase's
    optional-dependency pattern), else falls back to stdlib ``urllib``.
    Mirrors R's ``httr2::req_retry(max_tries = 3)``.
    """
    last_err: str = "unknown error"
    for _ in range(max_retries):
        try:
            try:
                import requests  # type: ignore[import-untyped]
            except ImportError:
                requests = None  # type: ignore[assignment]

            if requests is not None:
                resp = requests.get(url, params=params, timeout=timeout)
                if resp.status_code == 200:
                    return resp.text
                last_err = f"HTTP {resp.status_code}"
                continue

            import urllib.parse
            import urllib.request

            full_url = url if not params else f"{url}?{urllib.parse.urlencode(params)}"
            req = urllib.request.Request(full_url)
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                if resp.status == 200:
                    return resp.read().decode("utf-8")
                last_err = f"HTTP {resp.status}"
        except Exception as e:  # noqa: BLE001 - retried, final failure raised by caller
            last_err = str(e)
    raise ValueError(f"Failed to fetch {url}: {last_err}")


def _fires_fetch_inpe(
    date_start: str,
    date_end: str,
    uf: list[str] | None,
    biome: list[str] | None,
) -> pd.DataFrame:
    """Fetch fire data from the INPE Queimadas focos API for one month."""
    params = {
        "dataInicio": f"{date_start}T00:00:00",
        "dataFim": f"{date_end}T23:59:59",
        "pais": "Brasil",
    }
    if uf:
        params["estado"] = ",".join(uf)
    if biome:
        params["bioma"] = ",".join(biome)

    body = _http_get_text(_INPE_BASE_URL, params=params)
    body = body.strip()
    if not body or body in ("[]", "null"):
        return pd.DataFrame()

    parsed = json.loads(body)
    if isinstance(parsed, list) and parsed:
        return pd.DataFrame(parsed)
    return pd.DataFrame()


def _fires_fetch_firms(
    date_start: str,
    date_end: str,
    bbox: tuple[float, float, float, float] | list[float],
    firms_key: str,
    product: str,
) -> pd.DataFrame:
    """Fetch fire data from the NASA FIRMS area API for one month, in 10-day chunks."""
    import io
    from datetime import timedelta

    start_d = date.fromisoformat(date_start)
    end_d = date.fromisoformat(date_end)

    chunks: list[tuple[date, date]] = []
    cur = start_d
    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=9), end_d)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)

    bbox_str = ",".join(str(v) for v in bbox)
    parts: list[pd.DataFrame] = []
    for chunk_start, chunk_end in chunks:
        n_days = (chunk_end - chunk_start).days + 1
        url = (
            f"{_FIRMS_BASE_URL}/{firms_key}/{product}/{bbox_str}/{n_days}/"
            f"{chunk_start.isoformat()}"
        )
        body = _http_get_text(url)
        if not body.strip() or not body.strip().startswith("latitude"):
            continue
        try:
            parts.append(pd.read_csv(io.StringIO(body)))
        except Exception:
            continue

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _fires_download_month(
    source: str,
    date_start: str,
    date_end: str,
    cache_path: Path,
    uf: list[str] | None,
    biome: list[str] | None,
    bbox: tuple[float, float, float, float] | list[float] | None,
    firms_key: str | None,
    use_cache: bool,
    verbose: bool,
    msg: dict[str, str],
) -> Path:
    """Download one month of fire data (INPE or FIRMS) and cache it as CSV.

    Ported from R's .fires_download_month(): when the period has no
    fires, an empty (0-byte) sentinel file is written. Preserved R quirk:
    both R's cache guard (``file.size(...) > 0``) and this port's
    (``stat().st_size > 0``) treat a 0-byte file as "not cached", so an
    empty month is actually re-downloaded on every subsequent call
    despite the sentinel file existing — R's own inline comment claims
    the opposite ("so the cache check doesn't re-download"), which is
    incorrect; that incorrect comment is not replicated, but the
    re-download behavior itself is.
    """
    filename = cache_path.name

    if use_cache and cache_path.is_file() and cache_path.stat().st_size > 0:
        if verbose:
            console.print(f"[green]OK[/]  {msg['cache_hit'].format(filename=filename)}")
        return cache_path

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['download_file'].format(filename=filename)}")

    try:
        if source == "inpe":
            df = _fires_fetch_inpe(date_start, date_end, uf, biome)
        else:
            df = _fires_fetch_firms(
                date_start, date_end, bbox, firms_key, _fires_firms_product(source)
            )
    except Exception as e:  # noqa: BLE001 - matches R's tryCatch -> cli_warn + NULL
        if verbose:
            console.print(
                "[yellow]WARN[/]  "
                + msg["download_error"].format(filename=filename, err=str(e))
            )
        df = None

    if df is not None and not df.empty:
        df.to_csv(cache_path, index=False, encoding="utf-8")
        if verbose:
            console.print(
                f"[green]OK[/]  {msg['download_done'].format(filename=filename, n=len(df))}"
            )
    else:
        cache_path.write_text("", encoding="utf-8")
        if verbose:
            console.print(
                f"[yellow]WARN[/]  {msg['no_fires_period'].format(filename=filename)}"
            )

    return cache_path
