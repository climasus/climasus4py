"""grid_pollution_cams.py — CAMS atmospheric pollution data for Brazilian municipalities.

Mirrors R: sus_grid_pollution_cams.R

Downloads pre-processed CAMS (Copernicus Atmosphere Monitoring Service)
daily pollution Parquet files — already aggregated to municipality level
by the upstream author — from Zenodo, one file per (pollutant, metric)
combination, and merges them on ``(code_muni, date)``. No API key and no
raster/zonal-statistics step is involved: unlike ``sus_grid_chirps`` /
``sus_grid_era5`` / ``sus_grid_pdsi``, the Zenodo files here are already
per-municipality tables, so this module needs no ``geopandas`` /
``rioxarray`` / ``exactextract``.

Not lazy: the R source always materialises to a tibble at the end (no
Arrow/DuckDB relation involved past the per-file lazy year filter). The
Python port mirrors that: the public function always returns a
``pandas.DataFrame`` with metadata attached via ``df.attrs["sus_meta"]``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from rich.console import Console

from ..core.climate_inmet import _download_robust

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "cams"

_VALID_METRICS: tuple[str, ...] = ("mean", "max", "min")

# Zenodo record IDs for CAMS pollution data, by pollutant. Transcribed
# verbatim from climasus4r's .cams_zenodo_ids.
_CAMS_ZENODO_IDS: dict[str, str] = {
    "pm25": "16374139",
    "pm10": "16419737",
    "co": "18641834",
    "o3": "18641945",
    "no2": "18642048",
    "so2": "18642198",
}

# Measurement units for CAMS pollutants. Transcribed verbatim from
# climasus4r's .cams_units. Not currently attached to the output (the R
# source declares this constant but never surfaces it in the return
# value's metadata either); kept for documentation/parity.
_CAMS_UNITS: dict[str, str] = {
    "pm25": "ug/m3",
    "pm10": "ug/m3",
    "co": "ppm",
    "o3": "ug/m3",
    "no2": "ug/m3",
    "so2": "ug/m3",
}

_ID_COL_CANDIDATES: tuple[str, ...] = (
    "code_muni", "id_municipio", "geocodigo", "cod_municipio",
)
_DATE_COL_CANDIDATES: tuple[str, ...] = ("date", "data", "dt")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Dados CAMS de Poluição Atmosférica",
        "invalid_pollutants_type": "'pollutants' deve ser um vetor de caracteres.",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "invalid_metric": "'metric' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' deve ser um vetor numérico sem NA.",
        "invalid_years_range": "'years' deve estar entre 2003 e 2024. Ano(s) inválido(s): {bad}.",
        "invalid_use_cache": "'use_cache' deve ser True ou False.",
        "invalid_cache_dir": "'cache_dir' deve ser uma string não vazia.",
        "download_start": "Baixando {n_files} arquivo(s) CAMS do Zenodo...",
        "cache_hit": "Cache encontrado: {filename}",
        "download_file": "Baixando: {filename}",
        "download_done": "Concluído: {filename}",
        "download_error": "Falha ao baixar {filename}: {err}",
        "skip_missing": "Arquivo não encontrado no cache: {filename}",
        "bad_schema": "Schema inesperado em {filename} (colunas: {cols}).",
        "no_value_col": "Nenhuma coluna de valor encontrada em {filename}.",
        "read_warn": "Não foi possível ler {filename}: {err}",
        "no_data": "Nenhum dado foi lido com sucesso.",
        "done": "Concluído: {n_rows} observações carregadas.",
    },
    "en": {
        "title": "CAMS Atmospheric Pollution Data",
        "invalid_pollutants_type": "'pollutants' must be a character vector.",
        "invalid_pollutants": "Invalid 'pollutants': {bad}. Use: {valid}.",
        "invalid_metric": "Invalid 'metric': {bad}. Use: {valid}.",
        "invalid_years_type": "'years' must be a numeric vector without NA.",
        "invalid_years_range": "'years' must be between 2003 and 2024. Invalid year(s): {bad}.",
        "invalid_use_cache": "'use_cache' must be True or False.",
        "invalid_cache_dir": "'cache_dir' must be a non-empty string.",
        "download_start": "Downloading {n_files} CAMS file(s) from Zenodo...",
        "cache_hit": "Cache found: {filename}",
        "download_file": "Downloading: {filename}",
        "download_done": "Done: {filename}",
        "download_error": "Failed to download {filename}: {err}",
        "skip_missing": "File not found in cache: {filename}",
        "bad_schema": "Unexpected schema in {filename} (columns: {cols}).",
        "no_value_col": "No value column found in {filename}.",
        "read_warn": "Could not read {filename}: {err}",
        "no_data": "No data was successfully loaded.",
        "done": "Complete: {n_rows} observations loaded.",
    },
    "es": {
        "title": "Datos CAMS de Contaminación Atmosférica",
        "invalid_pollutants_type": "'pollutants' debe ser un vector de caracteres.",
        "invalid_pollutants": "'pollutants' inválido(s): {bad}. Use: {valid}.",
        "invalid_metric": "'metric' inválido: {bad}. Use: {valid}.",
        "invalid_years_type": "'years' debe ser un vector numérico sin NA.",
        "invalid_years_range": "'years' debe estar entre 2003 y 2024. Año(s) inválido(s): {bad}.",
        "invalid_use_cache": "'use_cache' debe ser True o False.",
        "invalid_cache_dir": "'cache_dir' debe ser una cadena no vacía.",
        "download_start": "Descargando {n_files} archivo(s) CAMS de Zenodo...",
        "cache_hit": "Caché encontrado: {filename}",
        "download_file": "Descargando: {filename}",
        "download_done": "Completado: {filename}",
        "download_error": "Error al descargar {filename}: {err}",
        "skip_missing": "Archivo no encontrado en caché: {filename}",
        "bad_schema": "Esquema inesperado en {filename} (columnas: {cols}).",
        "no_value_col": "Ninguna columna de valor encontrada en {filename}.",
        "read_warn": "No se pudo leer {filename}: {err}",
        "no_data": "No se cargó ningún dato correctamente.",
        "done": "Completo: {n_rows} observaciones cargadas.",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_pollution_cams(
    pollutants: str | list[str] = ("pm25", "pm10"),
    metric: str | list[str] = "mean",
    years: int | list[int] | None = None,
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import CAMS pollution data for Brazilian municipalities.

    Downloads pre-processed CAMS (Copernicus Atmosphere Monitoring
    Service) daily pollution data for Brazilian municipalities from
    Zenodo. No API key is required. Data cover six pollutants — PM2.5,
    PM10, CO, O3, NO2, and SO2 — aggregated to daily means/maxima/minima
    per municipality from 2003 to 2024. Files are hosted on Zenodo
    (CC-BY 4.0) as Parquet and are downloaded once and cached locally.

    Unlike ``sus_grid_chirps``/``sus_grid_era5``/``sus_grid_pdsi``, the
    Zenodo files here are already pre-aggregated to municipality level
    by the upstream author, so no raster reading or zonal statistics
    (``geopandas``/``rioxarray``/``exactextract``) is involved.

    Args:
        pollutants: Pollutant(s) to include, or ``"all"``. Allowed
            values: ``"pm25"``, ``"pm10"``, ``"co"``, ``"o3"``,
            ``"no2"``, ``"so2"``. Default: ``("pm25", "pm10")``.
        metric: Daily aggregation statistic. One of ``"mean"``
            (default), ``"max"``, ``"min"``, or ``"all"`` (all three).
        years: Year(s) to include (2003-2024). ``None`` (default)
            returns all available years.
        use_cache: Reuse previously downloaded Parquet files. Default
            ``True``.
        cache_dir: Directory for cached Parquet files. Default:
            ``~/.climasus4py_cache/cams``.
        lang: Message language: ``"pt"`` (default), ``"en"``, or
            ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A ``pandas.DataFrame`` with columns ``code_muni`` (str), ``date``
        (datetime), and one ``{pollutant}_{metric}`` column per requested
        pollutant x metric combination (e.g. ``pm25_mean``, ``no2_max``).
        Metadata in ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="pollution_cams"``).

    Raises:
        ValueError: If any parameter is invalid, or no data could be
            loaded for the given parameters.

    Units:
        PM2.5, PM10, O3, NO2, SO2: ug/m3. CO: ppm.

    Data source:
        Saldanha, R. et al. CAMS pollution daily averages for Brazilian
        municipalities (2003-2024). Zenodo. CC-BY 4.0.
        PM2.5: https://doi.org/10.5281/zenodo.16374139 |
        PM10: https://doi.org/10.5281/zenodo.16419737 |
        CO: https://doi.org/10.5281/zenodo.18641834 |
        O3: https://doi.org/10.5281/zenodo.18641945 |
        NO2: https://doi.org/10.5281/zenodo.18642048 |
        SO2: https://doi.org/10.5281/zenodo.18642198

    Examples::

        import climasus4py as cs

        # PM2.5 and NO2 daily means for 2019-2022
        cams = cs.sus_grid_pollution_cams(
            pollutants=["pm25", "no2"], metric="mean", years=list(range(2019, 2023)),
        )

        # All pollutants, all metrics, all years
        cams_full = cs.sus_grid_pollution_cams(pollutants="all", metric="all")
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- pollutants ---------------------------------------------------------
    valid_pollutants = list(_CAMS_ZENODO_IDS.keys())
    if pollutants == "all":
        pollutants_list = valid_pollutants
    else:
        raw_pollutants = [pollutants] if isinstance(pollutants, str) else list(pollutants)
        if not raw_pollutants:
            raise ValueError(msg["invalid_pollutants_type"])
        bad_pollutants = [p for p in raw_pollutants if p not in valid_pollutants]
        if bad_pollutants:
            raise ValueError(
                msg["invalid_pollutants"].format(
                    bad=", ".join(bad_pollutants), valid=", ".join(valid_pollutants)
                )
            )
        pollutants_list = raw_pollutants

    # --- metric ---------------------------------------------------------------
    if metric == "all":
        metric_list = list(_VALID_METRICS)
    else:
        raw_metric = [metric] if isinstance(metric, str) else list(metric)
        bad_metric = [m for m in raw_metric if m not in _VALID_METRICS]
        if not raw_metric or bad_metric:
            raise ValueError(
                msg["invalid_metric"].format(
                    bad=", ".join(bad_metric) if bad_metric else "",
                    valid=", ".join(_VALID_METRICS),
                )
            )
        metric_list = raw_metric

    # --- years -------------------------------------------------------------------
    years_list: list[int] | None = None
    if years is not None:
        raw_years = [years] if isinstance(years, int) else list(years)
        try:
            years_list = [int(y) for y in raw_years]
        except (TypeError, ValueError) as exc:
            raise ValueError(msg["invalid_years_type"]) from exc
        bad_years = [y for y in years_list if y < 2003 or y > 2024]
        if bad_years:
            raise ValueError(
                msg["invalid_years_range"].format(bad=", ".join(str(y) for y in bad_years))
            )

    # --- use_cache / cache_dir -----------------------------------------------------
    if not isinstance(use_cache, bool):
        raise ValueError(msg["invalid_use_cache"])
    if not isinstance(cache_dir, (str, Path)) or not str(cache_dir).strip():
        raise ValueError(msg["invalid_cache_dir"])
    cache_path = Path(cache_dir).expanduser()

    # =========================================================================
    # BUILD DOWNLOAD MANIFEST — one row per (pollutant, metric)
    # =========================================================================
    manifest = _build_manifest(pollutants_list, metric_list, cache_path)

    n_files = len(manifest)
    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print("[cyan]INFO[/]  " + msg["download_start"].format(n_files=n_files))

    # =========================================================================
    # DOWNLOAD WITH CACHE
    # =========================================================================
    for entry in manifest:
        _cams_download_file(entry["url"], entry["cache_path"], use_cache, verbose, msg)

    # =========================================================================
    # READ, FILTER YEARS, NORMALIZE COLUMNS
    # =========================================================================
    result_frames: list[pd.DataFrame] = []
    for entry in manifest:
        df_i = _read_and_normalize(entry, years_list, msg)
        if df_i is not None:
            result_frames.append(df_i)

    if not result_frames:
        raise ValueError(msg["no_data"])

    # =========================================================================
    # MERGE ALL POLLUTANT x METRIC COLUMNS (full outer join on code_muni, date)
    # =========================================================================
    result = result_frames[0]
    for other in result_frames[1:]:
        result = result.merge(other, on=["code_muni", "date"], how="outer")

    result = result.sort_values(["code_muni", "date"]).reset_index(drop=True)

    n_rows = len(result)
    if verbose:
        console.print("[green]OK[/]  " + msg["done"].format(n_rows=n_rows))

    # =========================================================================
    # METADATA
    # =========================================================================
    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "pollution_cams",
        "spatial": False,
        "temporal": {
            "start": result["date"].min() if n_rows else None,
            "end": result["date"].max() if n_rows else None,
            "unit": "day",
            "source": "zenodo_cams",
        },
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "pollutants": pollutants_list,
        "metric": metric_list,
        "years": years_list if years_list is not None else list(range(2003, 2025)),
        "n_municipalities": result["code_muni"].nunique() if n_rows else 0,
        "n_observations": n_rows,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_grid_pollution_cams(): "
            f"{len(pollutants_list)} pollutant(s) x {len(metric_list)} metric(s), {n_rows} obs"
        ],
        "user": {},
    }

    return result


# ---------------------------------------------------------------------------
# Internal: manifest construction
# ---------------------------------------------------------------------------

def _build_manifest(
    pollutants: list[str], metrics: list[str], cache_root: Path
) -> list[dict[str, str | Path]]:
    """Build one manifest row per (pollutant, metric) combination.

    The filename pattern is transcribed verbatim from the R source's
    ``paste0(pollutant, "_", metric, "_mean.parquet")`` — the trailing
    ``"_mean"`` is invariant across *metric* values (e.g.
    ``pm25_max_mean.parquet``). This is plausibly correct upstream
    naming (spatial mean over the municipality polygon, with *metric*
    naming the temporal daily statistic) rather than a bug, but it is
    not documented in the R source and unverified against the actual
    Zenodo deposits — see IDEIAS.md.
    """
    manifest: list[dict[str, str | Path]] = []
    for pollutant in pollutants:
        record_id = _CAMS_ZENODO_IDS[pollutant]
        for m in metrics:
            filename = f"{pollutant}_{m}_mean.parquet"
            manifest.append({
                "pollutant": pollutant,
                "metric": m,
                "out_col": f"{pollutant}_{m}",
                "filename": filename,
                "url": f"https://zenodo.org/records/{record_id}/files/{filename}?download=1",
                "cache_path": cache_root / filename,
            })
    return manifest


# ---------------------------------------------------------------------------
# Internal: download one CAMS Parquet file with cache
# ---------------------------------------------------------------------------

def _cams_download_file(
    url: str, cache_path: Path, use_cache: bool, verbose: bool, msg: dict[str, str]
) -> None:
    """Download one CAMS Parquet file to *cache_path*, reusing the cache.

    Never raises: download failures are logged as warnings and left for
    the read step to skip via a missing-file check — mirrors the R
    source's ``.cams_download_file()``, whose return value is ignored
    in the download loop.
    """
    cache_path = Path(cache_path)
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
# Internal: read one cached Parquet file, filter years, normalize columns
# ---------------------------------------------------------------------------

def _read_and_normalize(
    entry: dict[str, str | Path], years: list[int] | None, msg: dict[str, str]
) -> pd.DataFrame | None:
    """Read one cached CAMS Parquet file and normalize to ``code_muni``/``date``/``{out_col}``.

    Returns ``None`` (with a warning printed) if the file is missing,
    unreadable, or has a schema that cannot be mapped to id/date/value
    columns. In the R source, ``.cams_read()`` (bad-schema and
    no-value-column ``cli_abort`` calls included) runs entirely inside a
    ``tryCatch(..., error = function(e) { cli_warn(...); NULL })`` — an
    R condition raised by ``cli_abort`` is still an ``error`` condition
    and is caught by that handler just like any other exception. So a
    per-file schema mismatch degrades to a skipped-with-warning file,
    never an abort of the whole call; only "no data survived from any
    file" is fatal (see the ``no_data`` check in the caller). Mirrored
    here by catching ``ValueError`` in the same ``except`` as any other
    read failure, rather than re-raising it.
    """
    cache_path = Path(entry["cache_path"])
    filename = str(entry["filename"])
    out_col = str(entry["out_col"])

    if not cache_path.is_file() or cache_path.stat().st_size == 0:
        console.print("[yellow]WARN[/]  " + msg["skip_missing"].format(filename=filename))
        return None

    try:
        raw = pd.read_parquet(cache_path)

        id_cols = [c for c in _ID_COL_CANDIDATES if c in raw.columns]
        date_cols = [c for c in _DATE_COL_CANDIDATES if c in raw.columns]
        if not id_cols or not date_cols:
            raise ValueError(
                msg["bad_schema"].format(filename=filename, cols=", ".join(raw.columns))
            )

        if years is not None:
            raw = raw[pd.to_datetime(raw[date_cols[0]]).dt.year.isin(years)]

        value_cols = [c for c in raw.columns if c not in id_cols and c not in date_cols]

        out = raw.rename(columns={id_cols[0]: "code_muni", date_cols[0]: "date"})

        if len(value_cols) == 1:
            out = out.rename(columns={value_cols[0]: out_col})
            out = out[["code_muni", "date", out_col]]
        elif len(value_cols) > 1:
            # Multiple value columns: keep only the one matching the metric
            # name (e.g. "pm25_mean_mean" or "pm25_mean"); fall back to
            # the first — mirrors the R source's grep(out_col, ..., fixed=TRUE).
            match_cols = [c for c in value_cols if out_col in c]
            match_col = match_cols[0] if match_cols else value_cols[0]
            out = out[["code_muni", "date", match_col]].rename(columns={match_col: out_col})
        else:
            raise ValueError(msg["no_value_col"].format(filename=filename))

        out["code_muni"] = out["code_muni"].astype(str)
        out["date"] = pd.to_datetime(out["date"])
        return out
    except Exception as exc:  # noqa: BLE001 - mirrors R's tryCatch(..., error=warn),
        # which also catches cli_abort()-raised conditions — see docstring above.
        console.print(
            "[yellow]WARN[/]  " + msg["read_warn"].format(filename=filename, err=str(exc))
        )
        return None
