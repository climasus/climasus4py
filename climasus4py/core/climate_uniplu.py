"""climate_uniplu.py — UNIPLU-BR unified Brazilian rainfall dataset import.

Mirrors R: sus_climate_uniplu.R

Downloads, caches, and imports data from the Unified Brazilian Rainfall
Dataset (UNIPLU-BR) — 21,000+ rain gauges from five monitoring networks
covering 140 years (1885-2025), distributed as a single ~1.6 GB ZIP on
Zenodo containing two Parquet files (station metadata + observations).

Like ``sus_climate_inmet``, this is a large-volume import: the two cached
Parquet files are read and filtered through DuckDB (join, year/UF/network
filter push-down, temporal aggregation) so only the requested subset is
ever materialised into memory. The public function still returns a
``pd.DataFrame`` — the R source itself never routes this data through
a lazy Arrow/DuckDB pipeline (it calls ``dplyr::collect()`` immediately
after ``arrow::read_parquet()``), so there is no lazy-relation contract
to mirror at the API edge.
"""

from __future__ import annotations

import shutil
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd
from rich.console import Console

from ._sql import quote_ident, register_relation, sql_string
from .climate_inmet import _VALID_UFS, _download_robust, _relation_is_empty
from .engine import get_connection

console = Console(stderr=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Same on-disk cache root as sus_climate_inmet (climate_inmet.py's
# _DEFAULT_CACHE) — mirrors R's shared default cache_dir
# "~/.climasus4r_cache/climate".
_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "climate"

_VALID_NETWORKS: tuple[str, ...] = ("Hidroweb", "INMET", "ICEA", "CEMADEN", "Telemetria")
_VALID_AGGREGATE: tuple[str, ...] = ("none", "day", "month", "year")

_ZENODO_ZIP_URL = (
    "https://zenodo.org/records/18883358/files/"
    "Brazilian%20Unified%20Rainfall%20Dataset%20(1885%20-%202025).zip"
    "?download=1"
)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "cache_config": "Cache em: {dir}",
        "cache_hit": "Dados UNIPLU-BR encontrados no cache. Lendo arquivos Parquet...",
        "cache_miss": "Cache não encontrado. Baixando UNIPLU-BR do Zenodo (~1,6 GB)...",
        "download_start": "Download iniciado. Isso pode levar vários minutos.",
        "download_done": "Download concluído. Extraindo arquivos Parquet...",
        "extract_done": "Arquivos extraídos e salvos em cache: {dir}",
        "filter_year": "Filtrando por ano(s): {years}...",
        "filter_uf": "Filtrando por UF: {uf}...",
        "filter_net": "Filtrando por rede: {network}...",
        "agg_start": "Agregando para resolução: {agg}...",
        "import_done": "Concluído: {n_rows} observações de {n_stations} estações",
    },
    "en": {
        "cache_config": "Cache directory: {dir}",
        "cache_hit": "UNIPLU-BR found in cache. Reading Parquet files...",
        "cache_miss": "Cache not found. Downloading UNIPLU-BR from Zenodo (~1.6 GB)...",
        "download_start": "Download started. This may take several minutes.",
        "download_done": "Download complete. Extracting Parquet files...",
        "extract_done": "Files extracted and cached at: {dir}",
        "filter_year": "Filtering by year(s): {years}...",
        "filter_uf": "Filtering by UF: {uf}...",
        "filter_net": "Filtering by network: {network}...",
        "agg_start": "Aggregating to resolution: {agg}...",
        "import_done": "Done: {n_rows} observations from {n_stations} stations",
    },
    "es": {
        "cache_config": "Directorio de caché: {dir}",
        "cache_hit": "UNIPLU-BR encontrado en caché. Leyendo archivos Parquet...",
        "cache_miss": "Caché no encontrado. Descargando UNIPLU-BR de Zenodo (~1,6 GB)...",
        "download_start": "Descarga iniciada. Esto puede tardar varios minutos.",
        "download_done": "Descarga completada. Extrayendo archivos Parquet...",
        "extract_done": "Archivos extraídos y guardados en caché: {dir}",
        "filter_year": "Filtrando por año(s): {years}...",
        "filter_uf": "Filtrando por UF: {uf}...",
        "filter_net": "Filtrando por red: {network}...",
        "agg_start": "Agregando a la resolución: {agg}...",
        "import_done": "Completado: {n_rows} observaciones de {n_stations} estaciones",
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_climate_uniplu(
    years: int | list[int] | range | None = None,
    uf: str | list[str] | None = None,
    network: str | list[str] | None = None,
    aggregate_to: Literal["none", "day", "month", "year"] = "day",
    use_cache: bool = True,
    cache_dir: str | Path = _DEFAULT_CACHE,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Import UNIPLU-BR: Unified Brazilian Rainfall Dataset.

    Downloads, caches, and imports data from the Unified Brazilian
    Rainfall Dataset (UNIPLU-BR) — the most comprehensive national
    rainfall database for Brazil, covering 21,000+ gauges from five
    monitoring networks (Hidroweb, INMET, ICEA, CEMADEN, Telemetria)
    over 140 years (1885-2025).

    Pipeline: (1) download the ~1.6 GB Zenodo ZIP on first call only,
    (2) extract and cache the two Parquet files locally, (3) join and
    standardize column names to climasus4py conventions, (4) filter by
    year / UF / network, (5) optionally aggregate sub-daily observations
    to daily/monthly/yearly totals.

    Args:
        years: Year(s) to import, e.g. ``2020``, ``range(2020, 2025)``,
            ``[2015, 2020, 2024]``. Must be between 1885 and the current
            year. If ``None`` (default), imports the last 2 years.
        uf: Brazilian state code(s), e.g. ``"RN"``, ``["SP", "RJ"]``.
            Case insensitive. If ``None`` (default), all 27 states are
            returned.
        network: Monitoring network(s) to include — one or more of
            ``"Hidroweb"``, ``"INMET"``, ``"ICEA"``, ``"CEMADEN"``,
            ``"Telemetria"``. Case insensitive. If ``None`` (default),
            all networks are returned.
        aggregate_to: Temporal resolution for aggregating sub-daily
            observations into totals. One of ``"day"`` (default, daily
            totals), ``"month"``, ``"year"``, or ``"none"`` (raw
            observations at original resolution). When ``"none"``,
            ``time_step_min`` and ``utc_offset`` are also returned.
        use_cache: If ``True`` (default), skips download when both
            cached Parquet files already exist under
            ``cache_dir/uniplu/``. The ~1.6 GB ZIP is only downloaded
            once. Set ``False`` to force a re-download (e.g. after a
            new dataset version is published on Zenodo). Note: a fresh
            download is always written to the on-disk cache regardless
            of this flag — ``use_cache=False`` only skips *reading* an
            existing cache, mirroring the R source's behavior.
        cache_dir: Directory path for the disk cache. Default:
            ``~/.climasus4py_cache/climate``. Created automatically.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: If ``True`` (default), prints progress messages
            including cache status, download progress, filtering
            counts, and aggregation summary.

    Returns:
        DataFrame with columns ``station_code``, ``station_name``,
        ``uf``, ``latitude``, ``longitude``, ``altitude``, ``network``,
        ``date``, ``rainfall_mm``, plus ``time_step_min`` and
        ``utc_offset`` when ``aggregate_to="none"``. Metadata accessible
        via ``df.attrs["sus_meta"]`` (``stage="climate"``,
        ``type="uniplu"``).

    Raises:
        ValueError: If any parameter is invalid, the download/extraction
            fails, or no observations remain after filtering.

    Examples::

        import climasus4py as cs

        # Daily rainfall for Rio Grande do Norte, last 2 years (default)
        rain_rn = cs.sus_climate_uniplu(uf="RN")

        # Multiple states, specific years, daily totals
        rain_ne = cs.sus_climate_uniplu(
            years=range(2015, 2025),
            uf=["RN", "CE", "PB", "PE"],
            aggregate_to="day",
        )

        # Only ANA/Hidroweb network, monthly totals
        rain_hidroweb = cs.sus_climate_uniplu(
            years=range(2000, 2021), network="Hidroweb", aggregate_to="month"
        )
    """
    current_year = datetime.now().year

    # --- years ----------------------------------------------------------
    if years is None:
        years_list = [current_year - 1, current_year]
        if verbose:
            # ponytail: this default-years notice is hardcoded in English
            # in the R source too (not routed through the msg dict) —
            # preserved as-is rather than "fixed" to be localized.
            console.print(
                "[cyan]INFO[/]  'years' not supplied — defaulting to "
                f"{years_list[0]}-{years_list[-1]}."
            )
    elif isinstance(years, int):
        years_list = [years]
    else:
        years_list = [int(y) for y in years]

    invalid_years = [y for y in years_list if y < 1885 or y > current_year]
    if invalid_years:
        raise ValueError(
            f"Invalid values in 'years': {invalid_years}. "
            f"Years must be between 1885 and {current_year}."
        )

    # --- lang -------------------------------------------------------------
    if lang not in ("pt", "en", "es"):
        raise ValueError("'lang' must be one of 'pt', 'en', 'es'.")
    msg = _MESSAGES[lang]

    # --- uf -----------------------------------------------------------------
    uf_list: list[str] | None = None
    if uf is not None:
        uf_list = [uf.upper().strip()] if isinstance(uf, str) else [u.upper().strip() for u in uf]
        invalid_ufs = sorted(set(uf_list) - _VALID_UFS)
        if invalid_ufs:
            raise ValueError(
                f"Invalid values in 'uf': {invalid_ufs}. Valid codes: {sorted(_VALID_UFS)}"
            )

    # --- network --------------------------------------------------------
    network_list: list[str] | None = None
    if network is not None:
        raw_networks = [network] if isinstance(network, str) else list(network)
        network_list = []
        invalid_networks = []
        for n in raw_networks:
            match = next(
                (v for v in _VALID_NETWORKS if v.lower() == n.strip().lower()), None
            )
            if match is None:
                invalid_networks.append(n)
            else:
                network_list.append(match)
        if invalid_networks:
            raise ValueError(
                f"Invalid 'network' value(s): {invalid_networks}. "
                f"Valid options: {list(_VALID_NETWORKS)}"
            )

    # --- aggregate_to -----------------------------------------------------
    if aggregate_to not in _VALID_AGGREGATE:
        raise ValueError(f"'aggregate_to' must be one of: {', '.join(_VALID_AGGREGATE)}.")

    # --- cache setup ------------------------------------------------------
    cache_path = Path(cache_dir).expanduser()
    uniplu_dir = cache_path / "uniplu"
    uniplu_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['cache_config'].format(dir=uniplu_dir)}")

    info_parquet, data_parquet = _ensure_uniplu_cache(uniplu_dir, use_cache, msg, verbose)

    # --- read + standardize + filter: year --------------------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["filter_year"].format(years=", ".join(str(y) for y in years_list))
        )

    conn = get_connection()
    rel = _build_uniplu_relation(conn, info_parquet, data_parquet, years_list)

    if _relation_is_empty(rel):
        raise ValueError(
            f"No observations found for years: {', '.join(str(y) for y in years_list)}. "
            "The UNIPLU-BR dataset covers 1885-2025."
        )

    # --- filter: UF --------------------------------------------------------
    if uf_list is not None:
        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['filter_uf'].format(uf=', '.join(uf_list))}")
        uf_sql = ", ".join(sql_string(u) for u in uf_list)
        rel = rel.filter(f"{quote_ident('uf')} IN ({uf_sql})")
        if _relation_is_empty(rel):
            raise ValueError(f"No observations found for UF: {', '.join(uf_list)}.")

    # --- filter: network ----------------------------------------------------
    if network_list is not None:
        if verbose:
            console.print(
                f"[cyan]INFO[/]  {msg['filter_net'].format(network=', '.join(network_list))}"
            )
        net_sql = ", ".join(sql_string(n) for n in network_list)
        rel = rel.filter(f"{quote_ident('network')} IN ({net_sql})")
        if _relation_is_empty(rel):
            raise ValueError(f"No observations found for network: {', '.join(network_list)}.")

    # --- temporal aggregation ----------------------------------------------
    if aggregate_to != "none":
        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['agg_start'].format(agg=aggregate_to)}")
        rel = _aggregate_uniplu_relation(conn, rel, aggregate_to)

    rel = rel.order(f"{quote_ident('station_code')}, {quote_ident('date')}")
    climate_data = rel.df()

    # --- metadata ------------------------------------------------------------
    n_stations_final = int(climate_data["station_code"].nunique())
    now = datetime.now()
    date_min = climate_data["date"].min() if not climate_data.empty else None
    date_max = climate_data["date"].max() if not climate_data.empty else None
    years_present = sorted(pd.to_datetime(climate_data["date"]).dt.year.unique().tolist())

    climate_data.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "uniplu",
        "spatial": False,
        "temporal": {"start": date_min, "end": date_max, "unit": aggregate_to},
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "source": "UNIPLU-BR",
        "doi": "10.5281/zenodo.18883358",
        "years": years_present,
        "ufs": uf_list,
        "networks": network_list,
        "n_stations": n_stations_final,
        "n_observations": len(climate_data),
        "aggregate_to": aggregate_to,
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"UNIPLU-BR rainfall data imported (aggregate_to='{aggregate_to}')"
        ],
        "user": {},
    }

    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["import_done"].format(n_rows=len(climate_data), n_stations=n_stations_final)
        )

    return climate_data


# ---------------------------------------------------------------------------
# Internal: cache download + extraction
# ---------------------------------------------------------------------------

def _ensure_uniplu_cache(
    uniplu_dir: Path,
    use_cache: bool,
    msg: dict[str, str],
    verbose: bool,
) -> tuple[Path, Path]:
    """Download (if needed) and return paths to the two cached Parquet files.

    Mirrors the R source's cache logic exactly: a fresh download is
    always written to *uniplu_dir* regardless of *use_cache* — the flag
    only controls whether an existing cache is trusted for *reading*.
    """
    info_parquet = uniplu_dir / "table_info.parquet"
    data_parquet = uniplu_dir / "table_data.parquet"

    cache_valid = (
        use_cache
        and info_parquet.is_file() and info_parquet.stat().st_size > 0
        and data_parquet.is_file() and data_parquet.stat().st_size > 0
    )
    if cache_valid:
        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['cache_hit']}")
        return info_parquet, data_parquet

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['cache_miss']}")
        console.print(f"[cyan]INFO[/]  {msg['download_start']}")

    with tempfile.TemporaryDirectory(prefix="uniplu_dl_") as tmp_dl:
        zip_dest = Path(tmp_dl) / "uniplu.zip"
        ok, reason = _download_robust(_ZENODO_ZIP_URL, zip_dest, max_retries=3, verbose=verbose)
        if not ok:
            raise ValueError(
                "Failed to download UNIPLU-BR from Zenodo. "
                f"{reason or 'All download methods failed.'} "
                "Check your internet connection and try again. "
                f"URL: {_ZENODO_ZIP_URL}"
            )

        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['download_done']}")

        with tempfile.TemporaryDirectory(prefix="uniplu_extract_") as tmp_extract:
            extract_dir = Path(tmp_extract)
            try:
                with zipfile.ZipFile(zip_dest) as zf:
                    zf.extractall(extract_dir)
            except zipfile.BadZipFile as e:
                raise ValueError(
                    f"Failed to extract ZIP archive. {e} "
                    "The downloaded file may be corrupt. Try again with use_cache=False."
                ) from e

            found_info = sorted(extract_dir.rglob("table_info.parquet"))
            found_data = sorted(extract_dir.rglob("table_data.parquet"))
            if not found_info or not found_data:
                all_files = sorted(
                    str(p.relative_to(extract_dir)) for p in extract_dir.rglob("*") if p.is_file()
                )
                raise ValueError(
                    "Expected Parquet files not found in the ZIP archive. "
                    f"Files found: {', '.join(all_files)}. "
                    "The Zenodo archive may have changed structure. Please report this at "
                    "https://github.com/ByMaxAnjos/climasus4r/issues"
                )

            uniplu_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(found_info[0], info_parquet)
            shutil.copy2(found_data[0], data_parquet)

    if verbose:
        console.print(f"[green]OK[/]  {msg['extract_done'].format(dir=uniplu_dir)}")

    return info_parquet, data_parquet


# ---------------------------------------------------------------------------
# Internal: join + standardize + year-filter push-down
# ---------------------------------------------------------------------------

def _build_uniplu_relation(
    conn: duckdb.DuckDBPyConnection,
    info_parquet: Path,
    data_parquet: Path,
    years: list[int],
) -> duckdb.DuckDBPyRelation:
    """Join the two UNIPLU-BR Parquet tables, rename columns, filter by year.

    Column renames mirror the R source's ``dplyr::rename()`` mapping:
    ``gauge_code -> station_code``, ``city -> station_name``,
    ``state -> uf``, ``lat/long -> latitude/longitude``,
    ``elevation -> altitude``, ``time_step -> time_step_min``,
    ``utc -> utc_offset``, ``datetime -> date``, ``rain_mm -> rainfall_mm``.
    """
    info_sql = sql_string(str(info_parquet).replace("\\", "/"))
    data_sql = sql_string(str(data_parquet).replace("\\", "/"))
    years_sql = ", ".join(str(y) for y in years)

    sql = f"""
        SELECT
            CAST(d.gauge_code AS VARCHAR) AS station_code,
            i.city AS station_name,
            UPPER(TRIM(i.state)) AS uf,
            i.lat AS latitude,
            i.long AS longitude,
            i.elevation AS altitude,
            i.network AS network,
            i.time_step AS time_step_min,
            i.utc AS utc_offset,
            CAST(d.datetime AS TIMESTAMP) AS date,
            d.rain_mm AS rainfall_mm
        FROM read_parquet({data_sql}) AS d
        LEFT JOIN read_parquet({info_sql}) AS i
            ON d.gauge_code = i.gauge_code
        WHERE EXTRACT(YEAR FROM CAST(d.datetime AS TIMESTAMP)) IN ({years_sql})
    """
    return conn.sql(sql)


# ---------------------------------------------------------------------------
# Internal: temporal aggregation
# ---------------------------------------------------------------------------

def _aggregate_uniplu_relation(
    conn: duckdb.DuckDBPyConnection,
    rel: duckdb.DuckDBPyRelation,
    aggregate_to: Literal["day", "month", "year"],
) -> duckdb.DuckDBPyRelation:
    """Aggregate rainfall to day/month/year totals.

    Drops ``time_step_min`` and ``utc_offset`` — they are not part of the
    grouping key, mirroring the R source's ``group_cols`` (which
    likewise excludes them once aggregated).
    """
    group_cols = [
        "station_code", "station_name", "uf",
        "latitude", "longitude", "altitude", "network",
    ]
    group_sql = ", ".join(quote_ident(c) for c in group_cols)

    if aggregate_to == "day":
        date_expr = f"CAST({quote_ident('date')} AS DATE)"
    elif aggregate_to == "month":
        date_expr = f"date_trunc('month', {quote_ident('date')})"
    else:  # "year"
        date_expr = f"date_trunc('year', {quote_ident('date')})"

    view_name = f"_uniplu_agg_{uuid.uuid4().hex}"
    register_relation(conn, rel, view_name)
    try:
        sql = (
            f"SELECT {group_sql}, "
            f"CAST({date_expr} AS TIMESTAMP) AS date, "
            f"SUM({quote_ident('rainfall_mm')}) AS rainfall_mm "
            f"FROM {quote_ident(view_name)} "
            f"GROUP BY {group_sql}, {date_expr}"
        )
        table_name = f"_uniplu_aggtbl_{uuid.uuid4().hex}"
        conn.execute(f"CREATE TEMP TABLE {quote_ident(table_name)} AS {sql}")
    finally:
        conn.unregister(view_name)
    return conn.sql(f"SELECT * FROM {quote_ident(table_name)}")
