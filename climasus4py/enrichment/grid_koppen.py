"""grid_koppen.py — Koppen-Geiger climate zone for Brazilian municipalities.

Mirrors R: sus_grid_koppen.R

Unlike the other ``sus_grid_*`` functions in this batch, **there is no
downloadable raster/Parquet grid here** — the R source never fetches a
Koppen climate-zone raster from Zenodo/GitHub. It only cites the
Alvares et al. (2013) paper (``doi:10.1127/0941-2948/2013/0507``) as the
theoretical source for its classification rules; that DOI resolves to a
journal article, not a downloadable shapefile/raster, and no file URL for
it appears anywhere in the R source. Two assignment modes are available:

- ``"approx"`` (default): a rule-based lookup keyed on each municipality's
  UF code and centroid latitude/longitude, hand-derived from Alvares et
  al. (2013) Table 1 and the published map pattern (~85% accuracy at the
  municipal level per the R docstring). No raster, no zonal statistics,
  no new dependency.
- ``"exact"``: a point-in-polygon spatial join between municipality
  centroids and a user-supplied polygon layer with a ``koppen`` column
  (typically the Alvares et al. 2013 shapefile, which the *user* must
  obtain and load themselves — this package never downloads it). This is
  the only path that needs ``geopandas`` (Python analogue of R's ``sf``),
  imported lazily.

Municipality centroid lon/lat and UF code (the R ``municipio_meta.rds``
table) are read from ``climasus_data``'s ``geo/municipios.json`` instead
of a bundled RDS file, per the "no hardcoded metadata" rule.

Time-invariant classification: like the R source, this function adds one
``zona_koppen`` column to whatever municipality-level data it is given —
there is no ``date``/``years`` dimension, and ``sus_grid_join`` should be
called with ``by=["code_muni"]`` to broadcast this onto time-series health
data. Not lazy: the R source works entirely on in-memory data.frames/sf
objects (never Arrow/DuckDB), so this port takes/returns a materialised
``pd.DataFrame``, matching ``sus_grid_join``'s precedent.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Literal

import duckdb
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

if TYPE_CHECKING:
    import geopandas as gpd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Canonical Koppen zone order for Brazil (Alvares et al. 2013).
_KOPPEN_LEVELS: tuple[str, ...] = ("Af", "Am", "As", "Aw", "BSh", "Cf", "Cw")

_VALID_MODES: tuple[str, ...] = ("approx", "exact")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "climasus4py — Koppen",
        "step_validate": "Validando entradas...",
        "step_lookup": "Carregando metadados municipais ({n_mun} municipios)...",
        "step_join": "Atribuindo zonas de Koppen (modo: {mode})...",
        "step_spatial": "Realizando juncao espacial com shapefile de Koppen...",
        "done": "Concluido. Distribuicao: {dist_str}",
        "err_no_muni": "Coluna 'code_muni' nao encontrada. Disponiveis: {avail}.",
        "err_no_koppen_col": (
            "O objeto 'koppen_sf' deve conter uma coluna chamada 'koppen' "
            "com os codigos de zona."
        ),
        "err_no_geopandas": (
            "Pacote 'geopandas' necessario para mode='exact'. "
            "Instale com pip install climasus4py[spatial]."
        ),
        "warn_lang": "Idioma '{lang}' nao suportado. Usando 'pt'.",
        "warn_unmatched": (
            "{n_na} municipio(s) sem correspondencia Koppen (NA). "
            "Verifique os codigos IBGE."
        ),
    },
    "en": {
        "title": "climasus4py — Koppen",
        "step_validate": "Validating inputs...",
        "step_lookup": "Loading municipality metadata ({n_mun} municipalities)...",
        "step_join": "Assigning Koppen zones (mode: {mode})...",
        "step_spatial": "Performing spatial join with Koppen shapefile...",
        "done": "Done. Distribution: {dist_str}",
        "err_no_muni": "Column 'code_muni' not found. Available: {avail}.",
        "err_no_koppen_col": (
            "The 'koppen_sf' object must contain a column named 'koppen' "
            "with zone codes."
        ),
        "err_no_geopandas": (
            "Package 'geopandas' required for mode='exact'. "
            "Install with pip install climasus4py[spatial]."
        ),
        "warn_lang": "Unsupported language '{lang}'. Using 'pt'.",
        "warn_unmatched": (
            "{n_na} municipality(ies) without Koppen match (NA). "
            "Check IBGE codes."
        ),
    },
    "es": {
        "title": "climasus4py — Koppen",
        "step_validate": "Validando entradas...",
        "step_lookup": "Cargando metadatos municipales ({n_mun} municipios)...",
        "step_join": "Asignando zonas de Koppen (modo: {mode})...",
        "step_spatial": "Realizando union espacial con shapefile de Koppen...",
        "done": "Listo. Distribucion: {dist_str}",
        "err_no_muni": "Columna 'code_muni' no encontrada. Disponibles: {avail}.",
        "err_no_koppen_col": (
            "El objeto 'koppen_sf' debe contener una columna llamada 'koppen' "
            "con los codigos de zona."
        ),
        "err_no_geopandas": (
            "Paquete 'geopandas' necesario para mode='exact'. "
            "Instale con pip install climasus4py[spatial]."
        ),
        "warn_lang": "Idioma '{lang}' no soportado. Usando 'pt'.",
        "warn_unmatched": (
            "{n_na} municipio(s) sin correspondencia Koppen (NA). "
            "Verifique los codigos IBGE."
        ),
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_grid_koppen(
    data: pd.DataFrame | duckdb.DuckDBPyRelation | gpd.GeoDataFrame,
    mode: Literal["approx", "exact"] = "approx",
    koppen_sf: gpd.GeoDataFrame | None = None,
    as_factor: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Assign Koppen-Geiger climate zones to Brazilian municipalities.

    Adds a ``zona_koppen`` column with municipality-level Koppen-Geiger
    climate classification following Alvares et al. (2013), who produced
    the first high-resolution (1 km) Koppen map for Brazil.

    Two assignment modes are available:

    - ``"approx"`` (default): rule-based assignment from municipality
      centroid coordinates (loaded from ``climasus_data``). Fast and
      dependency-free, with ~85% accuracy at the municipal level.
    - ``"exact"``: point-in-polygon spatial join between municipality
      centroids and a user-supplied ``geopandas.GeoDataFrame`` polygon
      layer (*koppen_sf*). Produces exact results when the original
      Alvares et al. (2013) shapefile is supplied. Requires
      ``geopandas``.

    Koppen zones for Brazil (Alvares et al. 2013): ``Af`` tropical humid
    (Amazon/NE coast), ``Am`` tropical monsoon (North/Center-West),
    ``As`` tropical summer dry (NE coast), ``Aw`` tropical winter dry /
    savanna (Central Brazil), ``BSh`` hot semi-arid (NE sertao), ``Cf``
    subtropical humid (South/coastal SE), ``Cw`` subtropical winter dry
    (SE plateau/Center-West uplands).

    Args:
        data: A ``pd.DataFrame`` (with a ``code_muni`` column, 6- or
            7-digit IBGE municipality code), a ``duckdb.DuckDBPyRelation``,
            or a ``geopandas.GeoDataFrame`` of Brazilian municipalities.
        mode: Assignment mode: ``"approx"`` (default, no extra
            dependencies) or ``"exact"`` (requires ``geopandas`` and
            *koppen_sf*).
        koppen_sf: A ``geopandas.GeoDataFrame`` polygon layer with a
            column named ``koppen`` containing the zone codes (``"Af"``,
            ``"Am"``, etc.). Required when ``mode="exact"``. The Alvares
            et al. (2013) shapefile referenced by the paper
            (doi:10.1127/0941-2948/2013/0507) is not bundled or
            downloaded by this function — the user must supply it.
            Ignored for ``mode="approx"``.
        as_factor: If ``True`` (default), return ``zona_koppen`` as an
            ordered ``pandas.Categorical`` with canonical level order
            (``Af < Am < As < Aw < BSh < Cf < Cw``). If ``False``, a
            plain string column.
        lang: Language for messages: ``"pt"`` (default), ``"en"``,
            ``"es"``.
        verbose: If ``True`` (default), print progress messages.

    Returns:
        The input data (materialised as a ``pd.DataFrame``) with an
        additional ``zona_koppen`` column. Metadata accessible via
        ``df.attrs["sus_meta"]`` (``stage="climate"``, ``type="koppen"``).

    Raises:
        ValueError: If *data* lacks a ``code_muni`` column, *mode* is
            not one of ``"approx"``/``"exact"``, or ``mode="exact"`` is
            requested without a valid *koppen_sf*.
        ImportError: If ``mode="exact"`` is requested and ``geopandas``
            is not installed.

    References:
        Alvares, C.A., Stape, J.L., Sentelhas, P.C., Goncalves, J.L.M., &
        Sparovek, G. (2013). Koppen's climate classification map for
        Brazil. Meteorologische Zeitschrift, 22(6), 711-728.
        doi:10.1127/0941-2948/2013/0507

    Examples::

        import climasus4py as cs

        df_koppen = cs.sus_grid_koppen(df_aggregated, mode="approx")
        df_koppen["zona_koppen"].value_counts()

        # Exact mode with a user-supplied Alvares et al. 2013 shapefile
        import geopandas as gpd
        kop_sf = gpd.read_file("koppen_brazil_alvares2013.shp")
        kop_sf = kop_sf.rename(columns={"zone": "koppen"})
        df_exact = cs.sus_grid_koppen(df_aggregated, mode="exact", koppen_sf=kop_sf)
    """
    if lang not in ("pt", "en", "es"):
        console.print(f"[yellow]WARN[/]  Unsupported language {lang!r}. Using 'pt'.")
        lang = "pt"
    msg = _MESSAGES[lang]

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    if mode not in _VALID_MODES:
        raise ValueError(f"'mode' must be one of {_VALID_MODES!r}, got {mode!r}.")

    # --- 1. Materialize the input, capturing any existing sus_meta --------
    input_meta: dict = {}
    if isinstance(data, duckdb.DuckDBPyRelation):
        df = data.df()
    elif type(data).__module__.startswith("geopandas"):
        df = pd.DataFrame(data.drop(columns=[data.geometry.name]))
        input_meta = getattr(data, "attrs", {}).get("sus_meta", {})
    else:
        df = data.copy()
        input_meta = data.attrs.get("sus_meta", {})

    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['step_validate']}")

    # --- 2. Validate code_muni ----------------------------------------------
    if "code_muni" not in df.columns:
        raise ValueError(
            msg["err_no_muni"].format(avail=", ".join(str(c) for c in df.columns))
        )

    df["code_muni_6"] = df["code_muni"].astype(str).str[:6]

    # --- 3. Load municipality centroid metadata (climasus_data) -----------
    meta = _load_municipio_meta()
    if verbose:
        console.print(f"[cyan]INFO[/]  {msg['step_lookup'].format(n_mun=len(meta))}")
        console.print(f"[cyan]INFO[/]  {msg['step_join'].format(mode=mode)}")

    # --- 4. Assign Koppen ----------------------------------------------------
    if mode == "approx":
        lookup = _koppen_approx_lookup(meta)
        df = df.merge(lookup[["code_muni_6", "zona_koppen"]], on="code_muni_6", how="left")
    else:  # "exact"
        try:
            import geopandas as gpd  # noqa: F401
        except ImportError as exc:
            raise ImportError(msg["err_no_geopandas"]) from exc
        if koppen_sf is None or "koppen" not in koppen_sf.columns:
            raise ValueError(msg["err_no_koppen_col"])

        if verbose:
            console.print(f"[cyan]INFO[/]  {msg['step_spatial']}")
        lookup = _koppen_spatial_join(meta, koppen_sf)
        df = df.merge(lookup[["code_muni_6", "zona_koppen"]], on="code_muni_6", how="left")

    # --- 5. Clean up helper column -------------------------------------------
    df = df.drop(columns=["code_muni_6"])

    # --- 6. Warn on unmatched --------------------------------------------------
    n_na = int(df["zona_koppen"].isna().sum())
    if n_na > 0:
        console.print(f"[yellow]WARN[/]  {msg['warn_unmatched'].format(n_na=n_na)}")

    # --- 7. Optionally coerce to ordered categorical --------------------------
    if as_factor:
        df["zona_koppen"] = pd.Categorical(
            df["zona_koppen"], categories=list(_KOPPEN_LEVELS), ordered=True
        )

    # --- 8. Report ---------------------------------------------------------------
    if verbose:
        dist = df["zona_koppen"].value_counts(dropna=True)
        dist_str = ", ".join(f"{k}={v}" for k, v in dist.items())
        console.print(f"[green]OK[/]  {msg['done'].format(dist_str=dist_str)}")

    # --- 9. Metadata --------------------------------------------------------------
    now = datetime.now()
    base_meta = dict(input_meta)
    base_meta["stage"] = "climate"
    base_meta["type"] = "koppen"
    base_meta["source"] = "Alvares et al. (2013)"
    base_meta["doi"] = "10.1127/0941-2948/2013/0507"
    base_meta["modified"] = now.isoformat()
    base_meta.setdefault("created", now.isoformat())
    history = list(base_meta.get("history", []))
    history.append(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"sus_grid_koppen: zona_koppen added (mode={mode})"
    )
    base_meta["history"] = history
    df.attrs["sus_meta"] = base_meta

    return df


# ---------------------------------------------------------------------------
# Internal: municipality centroid metadata (climasus_data)
# ---------------------------------------------------------------------------

def _load_municipio_meta() -> pd.DataFrame:
    """Load municipality centroid lon/lat/UF code from ``climasus_data``.

    Python analogue of the R source's bundled ``municipio_meta.rds``.
    Reads ``geo/municipios.json`` directly with ``utf-8-sig`` because
    that file is UTF-8-BOM-encoded and ``climasus_data.load_json()`` /
    ``climasus4py.utils.data.load_json()`` both open files with plain
    ``utf-8``, which raises ``JSONDecodeError`` on the BOM (flagged in
    IDEIAS.md rather than patched here, since both loaders are shared
    infrastructure outside this function's scope).
    """
    import climasus_data

    path = climasus_data.get_path("geo/municipios.json")
    records = json.loads(path.read_text(encoding="utf-8-sig"))
    meta = pd.DataFrame.from_records(records)
    meta = meta.rename(columns={
        "latitude": "lat",
        "longitude": "lon",
        "codigo_uf": "uf_code",
    })
    meta["code_muni_6"] = meta["geocodigo"].astype(str).str[:6]
    return meta[["code_muni_6", "uf_code", "lon", "lat"]]


# ---------------------------------------------------------------------------
# Internal: approximate rule-based classification
# ---------------------------------------------------------------------------

def _classify_one(uf_code: int, lon: float, lat: float) -> str:
    """Classify one municipality by UF code + centroid lon/lat.

    Rules are simplified from Alvares et al. (2013) Table 1 and the map
    pattern (~85% accuracy at municipal level vs. the original 1-km
    raster). Faithful, line-for-line port of the R source's
    ``classify_one()`` closure inside ``.koppen_approx_lookup`` — branch
    order and the (partially redundant/unreachable) conditions are
    preserved exactly rather than "cleaned up", per the no-fix-R-bugs
    rule.
    """
    # Tropical semi-arid (BSh) -- NE sertao
    # States: CE, RN, PB, PE, AL, PI (interior), BA (interior)
    bsh_states = {23, 24, 25, 26, 27, 22}
    if uf_code in bsh_states and lat > -12 and lon > -45:
        return "BSh"

    # Subtropical humid (Cf) -- South
    south_states = {41, 42, 43}
    if uf_code in south_states and lat < -23:
        return "Cf"
    # SE coastal + RJ
    if uf_code == 33 and lon > -44:
        return "Cf"

    # Subtropical winter dry (Cw) -- SE plateau and Center-West uplands
    cw_states = {31, 35, 32, 52, 53}
    if uf_code in cw_states and lat < -15:
        return "Cw"
    if uf_code in south_states and lat >= -23:
        return "Cw"

    # Tropical savanna winter dry (Aw) -- Center-West, parts of N/NE/SE
    aw_states = {51, 50, 52, 53, 11, 12, 14, 17, 21}
    if uf_code in aw_states:
        return "Aw"
    if uf_code in {31, 35} and lat >= -15:
        return "Aw"
    if uf_code == 29 and lat > -14:
        return "Aw"

    # Tropical summer dry (As) -- NE coast
    if uf_code in {21, 22, 26, 27, 28, 29} and lon > -40:
        return "As"

    # Tropical monsoon (Am) -- North / Amazon basin
    am_states = {13, 16, 15}
    if uf_code in am_states:
        return "Am"

    # Tropical humid (Af) -- equatorial Amazon / NE coast
    af_states = {11, 12, 14, 16}
    if uf_code in af_states and lat > -5:
        return "Af"

    # Default: Aw for remaining tropical municipalities
    if lat > -23:
        return "Aw"

    # Default south: Cf
    return "Cf"


def _koppen_approx_lookup(meta: pd.DataFrame) -> pd.DataFrame:
    """Apply :func:`_classify_one` to every municipality in *meta*."""
    out = meta.copy()
    out["zona_koppen"] = out.apply(
        lambda row: _classify_one(int(row["uf_code"]), float(row["lon"]), float(row["lat"])),
        axis=1,
    )
    return out


# ---------------------------------------------------------------------------
# Internal: exact spatial join against a user-supplied Koppen polygon layer
# ---------------------------------------------------------------------------

def _koppen_spatial_join(meta: pd.DataFrame, koppen_sf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Point-in-polygon join of municipality centroids against *koppen_sf*.

    Python analogue of the R source's ``.koppen_spatial_join()``:
    builds a WGS84 point layer from centroid lon/lat, reprojects
    *koppen_sf* to EPSG:4326 if needed, and performs a left spatial join
    (mirroring ``sf::st_join(..., left = TRUE)``, whose default predicate
    is intersects).
    """
    import geopandas as gpd

    # ponytail: reclassifies all ~5,570 municipalities on every call (same
    # as R, which builds `pts` from the full `meta` table) even when `data`
    # only has a handful of rows. Fine at this size; revisit if `meta` ever
    # grows enough for this to be a measurable cost.
    pts = gpd.GeoDataFrame(
        meta[["code_muni_6"]],
        geometry=gpd.points_from_xy(meta["lon"], meta["lat"]),
        crs="EPSG:4326",
    )

    # Note: unlike R's `st_crs(koppen_sf)$epsg %in% c(4326, NA)` (which
    # treats an unrecognised/WKT-only CRS as "already 4326" and skips the
    # transform — arguably a latent R bug for non-EPSG CRSes), this always
    # reprojects when the CRS isn't EPSG:4326, including when to_epsg()
    # returns None for a WKT-only CRS. Deliberate divergence, not a fix of
    # R's exact behavior — see IDEIAS.md.
    if koppen_sf.crs is not None and koppen_sf.crs.to_epsg() != 4326:
        koppen_sf = koppen_sf.to_crs(epsg=4326)
    elif koppen_sf.crs is None:
        koppen_sf = koppen_sf.set_crs(epsg=4326)

    joined = gpd.sjoin(pts, koppen_sf[["koppen", koppen_sf.geometry.name]], how="left")
    lookup = joined[["code_muni_6", "koppen"]].rename(columns={"koppen": "zona_koppen"})
    lookup["zona_koppen"] = lookup["zona_koppen"].astype("object")
    return lookup
