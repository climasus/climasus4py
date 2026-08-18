"""Spatial contiguity weights construction from municipality polygons.

Mirrors R: sus_mod_spatial_weights.R

Not lazy — this operates on an in-memory ``geopandas.GeoDataFrame`` of
polygon geometries (the Python analog of R's ``sf`` object), not a
``DuckDBPyRelation``. There is no natural DuckDB-lazy representation of
polygon contiguity detection, so this module (like R's) sits outside the
``import -> clean -> standardize -> filter -> variables -> aggregate``
pipeline: it is a standalone spatial-modelling helper consumed by the
other ``sus_mod_spatial_*`` functions (``sus_mod_spatial_moran``,
``sus_mod_spatial_reg``, ``sus_mod_plot_spatial_moran``).

Theory:
  Cliff & Ord (1981) - Spatial Processes: Models and Applications
  Anselin (1988) - Spatial Econometrics: Methods and Models
  Bivand & Wong (2018) - Comparing implementations of global indicators
    of spatial association, TEST 27(3):716-748
  Tiefelsdorf, M., Griffith, D. A., & Boots, B. (1999). A variance-
    stabilizing coding scheme for spatial link matrices. Environment and
    Planning A, 31(1), 165-180.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from rich.console import Console

console = Console(stderr=True)

VALID_STYLES: tuple[str, ...] = ("W", "B", "C", "U", "S", "minmax")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_validate": "Validando geometria de {n_muni} municipios...",
        "step_makevalid": "Reparando geometrias invalidas...",
        "step_nb": "Construindo matriz de vizinhanca ({type_nb}, snap = {snap_val})...",
        "step_listw": "Convertendo para lista de pesos espaciais (estilo {style})...",
        "step_matrix": "Construindo matriz densa W ({n} x {n})...",
        "warn_islands": (
            "{n_islands} municipio(s) sem vizinho (ilhas espaciais): {ids}. "
            "Use zero_policy=True ou verifique a geometria."
        ),
        "done": (
            "Pesos espaciais prontos. Regioes: {n}  |  Ilhas: {n_isl}  |  "
            "Vizinhos med/min/max: {mn}/{mi}/{mx}"
        ),
        "err_not_gdf": (
            "'municipalities' deve ser um geopandas.GeoDataFrame. Obtenha poligonos "
            "com geobr (R) ou uma fonte equivalente (ex.: IBGE malha municipal)."
        ),
        "err_no_rows": "'municipalities' esta vazio (0 linhas).",
        "err_style": "Estilo de normalizacao '{style}' invalido. Use um de: {valid}.",
        "err_islands_strict": (
            "{n_islands} ilha(s) encontrada(s) e zero_policy=False. "
            "Defina zero_policy=True ou remova as ilhas."
        ),
    },
    "en": {
        "step_validate": "Validating geometry of {n_muni} municipalities...",
        "step_makevalid": "Repairing invalid geometries...",
        "step_nb": "Building neighbourhood matrix ({type_nb}, snap = {snap_val})...",
        "step_listw": "Converting to spatial weights list (style {style})...",
        "step_matrix": "Building dense W matrix ({n} x {n})...",
        "warn_islands": (
            "{n_islands} municipality/municipalities with no neighbour (spatial "
            "islands): {ids}. Use zero_policy=True or check geometry."
        ),
        "done": (
            "Spatial weights ready. Regions: {n}  |  Islands: {n_isl}  |  "
            "Neighbours avg/min/max: {mn}/{mi}/{mx}"
        ),
        "err_not_gdf": (
            "'municipalities' must be a geopandas.GeoDataFrame. Obtain polygons "
            "from geobr (R) or an equivalent source (e.g. IBGE municipal mesh)."
        ),
        "err_no_rows": "'municipalities' is empty (0 rows).",
        "err_style": "Normalisation style '{style}' is invalid. Use one of: {valid}.",
        "err_islands_strict": (
            "{n_islands} island(s) found and zero_policy=False. "
            "Set zero_policy=True or remove islands."
        ),
    },
    "es": {
        "step_validate": "Validando geometria de {n_muni} municipios...",
        "step_makevalid": "Reparando geometrias invalidas...",
        "step_nb": "Construyendo matriz de vecindad ({type_nb}, snap = {snap_val})...",
        "step_listw": "Convirtiendo a lista de pesos espaciales (estilo {style})...",
        "step_matrix": "Construyendo matriz densa W ({n} x {n})...",
        "warn_islands": (
            "{n_islands} municipio(s) sin vecino (islas espaciales): {ids}. "
            "Use zero_policy=True o verifique la geometria."
        ),
        "done": (
            "Pesos espaciales listos. Regiones: {n}  |  Islas: {n_isl}  |  "
            "Vecinos prom/min/max: {mn}/{mi}/{mx}"
        ),
        "err_not_gdf": (
            "'municipalities' debe ser un geopandas.GeoDataFrame. Obtenga poligonos "
            "de geobr (R) o una fuente equivalente (p.ej. malla municipal del IBGE)."
        ),
        "err_no_rows": "'municipalities' esta vacio (0 filas).",
        "err_style": "Estilo de normalizacion '{style}' invalido. Use uno de: {valid}.",
        "err_islands_strict": (
            "{n_islands} isla(s) encontrada(s) y zero_policy=False. "
            "Defina zero_policy=True o elimine las islas."
        ),
    },
}

_DEFAULT_SNAP = 1e-3


def sus_mod_spatial_weights(
    municipalities: Any,
    style: str = "W",
    queen: bool = True,
    snap: float | None = None,
    zero_policy: bool = True,
    return_matrix: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Build spatial contiguity weights from municipality polygons.

    Constructs a ``libpysal`` spatial weights object from a
    ``geopandas.GeoDataFrame`` of municipality (or any polygon)
    boundaries. The result is the required input for all other
    ``sus_mod_spatial_*`` functions in the climasus4py pipeline
    (``sus_mod_spatial_moran``, ``sus_mod_spatial_reg``,
    ``sus_mod_plot_spatial_moran``).

    Contiguity neighbours are detected with ``libpysal.weights.Queen``
    (default, shared edge **or** vertex) or ``libpysal.weights.Rook``
    (shared edge only) — the Python equivalents of R's
    ``spdep::poly2nb(..., queen=...)``. Municipalities that share no
    boundary ("spatial islands") are automatically detected and
    reported. The resulting binary neighbour structure is normalised
    into weights according to *style*.

    Snap distance:
        Municipal boundaries digitised at different scales often have
        small gaps or overlaps at shared borders. R's
        ``spdep::poly2nb()`` accepts a ``snap`` distance tolerance so
        that boundaries within that distance are treated as touching.
        ``libpysal.weights.Queen``/``Rook`` build contiguity from exact
        polygon topology and have **no equivalent tolerance
        parameter** — *snap* is accepted here for signature parity
        with R, and its resolved value (``1e-3`` when ``None``, mirroring
        R's default) is only used in the progress message. This is a
        known behavioural gap versus R for geometries with tiny digitising
        gaps; see ``IDEIAS.md``.

    Weight styles:
        * ``"W"`` (default) — row-standardised: each neighbour weight
          = ``1 / n_neighbours``. Uses ``libpysal``'s native ``"R"``
          transform.
        * ``"B"`` — binary: ``1`` if neighbour, ``0`` otherwise. Uses
          ``libpysal``'s native ``"B"`` transform.
        * ``"C"`` — globally standardised: every neighbour edge gets
          weight ``n / L`` (``n`` = number of regions, ``L`` = total
          number of neighbour links), so the full matrix sums to
          ``n``. No ``libpysal`` native transform; computed by hand.
        * ``"U"`` — ``"C"`` divided by ``n`` (weight ``1 / L`` per
          edge), so the full matrix sums to ``1``. No ``libpysal``
          native transform; computed by hand.
        * ``"S"`` — variance-stabilising coding (Tiefelsdorf et al.
          1999). Uses ``libpysal``'s native ``"V"`` transform, which
          implements the same published formula R's ``spdep`` cites.
        * ``"minmax"`` — every neighbour edge gets weight
          ``1 / max(cardinalities)``, i.e. binary weights divided by
          the largest neighbour count in the dataset. No ``libpysal``
          native transform; computed by hand. R's ``spdep`` documents
          this as dividing by ``max(max(rowSums(B)), max(colSums(B)))``
          (Kelejian & Prucha's matrix-norm normalisation); for the
          symmetric Queen/Rook contiguity matrices built here, row
          sums and column sums coincide (``rowSum_i == colSum_i ==
          cardinality_i``), so this reduces to ``1 / max(cardinalities)``
          exactly as implemented.

    Args:
        municipalities: A ``geopandas.GeoDataFrame`` with **polygon**
            or **multipolygon** geometry (the Python analog of R's
            ``sf`` object). A column named ``code_muni`` is used as
            row labels when present; otherwise 1-based integer indices
            are used (matching R's ``as.character(seq_len(n_muni))``).
        style: Weight normalisation style. One of ``"W"`` (default),
            ``"B"``, ``"C"``, ``"U"``, ``"S"``, ``"minmax"``. See
            **Weight styles** above.
        queen: If ``True`` (default), Queen contiguity is used (shared
            edge or vertex). If ``False``, Rook contiguity (shared
            edge only).
        snap: Distance tolerance conceptually passed to the R
            neighbour-detection step. ``None`` (default) resolves to
            ``1e-3``. See **Snap distance** above for the current
            limitation in the Python port.
        zero_policy: If ``True`` (default), municipalities with no
            neighbours ("islands") are allowed and reported via a
            warning. If ``False``, a ``ValueError`` is raised when
            islands are found.
        return_matrix: If ``True``, a dense *n x n* spatial weights
            matrix is included in the output (key ``"W"``). Can be
            memory-intensive for large datasets. Default ``False``.
        lang: Language for progress/warning messages: ``"pt"``
            (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A ``dict`` (the Python analog of R's ``climasus_weights``
        S3-classed list) with keys:

        - ``"listw"``: ``libpysal.weights.W`` object ready for spatial
          modelling — the Python analog of R's ``spdep::listw``.
        - ``"nb"``: ``dict[str, list[str]]`` mapping each region id to
          its list of neighbour ids (binary neighbour structure before
          weight normalisation) — the Python analog of R's ``spdep::nb``.
        - ``"n_regions"``: ``int``. Total number of regions.
        - ``"n_islands"``: ``int``. Number of regions with zero
          neighbours.
        - ``"island_ids"``: ``list[str]``. Region ids with zero
          neighbours (empty if none).
        - ``"W"``: ``numpy.ndarray`` dense *n x n* weight matrix, or
          ``None`` when *return_matrix* is ``False``.
        - ``"style"``: ``str``. Weight normalisation style used.
        - ``"meta"``: ``dict`` with ``stage="mod"``, ``type="spatial_weights"``,
          and a ``history`` list entry — the Python analog of R's
          ``attr(result, "sus_meta")`` (this object is a plain ``dict``,
          not a DataFrame, so there is no ``.attrs`` to attach to; the
          metadata is nested under this key instead).

        Row/column order contract: ``result["W"]`` and
        ``result["listw"].id_order`` both follow the row order of the
        input *municipalities* GeoDataFrame (region ids are the
        ``code_muni`` strings when present, else ``"1"..str(n)``).
        Callers that align a data vector to the weights matrix (e.g.
        ``sus_mod_spatial_moran``, ``sus_mod_spatial_reg``) must use
        this order, not an arbitrary re-sort.

        R's ``$call`` slot (the matched call) has no meaningful Python
        analog and is not reproduced.

    Raises:
        TypeError: If *municipalities* is not a ``geopandas.GeoDataFrame``.
        ValueError: If *municipalities* is empty, *style* is invalid,
            or islands are found while *zero_policy* is ``False``.
        ImportError: If ``geopandas`` or ``libpysal`` is not installed.

    Examples::

        import geopandas as gpd
        import climasus4py as cs

        muni = gpd.read_file("municipios_ne.gpkg")

        # Row-standardised Queen contiguity (default)
        w = cs.sus_mod_spatial_weights(muni)

        # Binary Rook weights, returning the dense W matrix
        w_rook = cs.sus_mod_spatial_weights(
            municipalities=muni,
            style="B",
            queen=False,
            return_matrix=True,
        )
        w_rook["W"].shape
    """
    try:
        import geopandas as gpd
    except ImportError as exc:
        raise ImportError(
            "geopandas is required for sus_mod_spatial_weights(). "
            "Install it with: pip install geopandas"
        ) from exc
    try:
        import numpy as np
        from libpysal.weights import Queen, Rook
        from libpysal.weights import W as PysalW
    except ImportError as exc:
        raise ImportError(
            "libpysal is required for sus_mod_spatial_weights(). "
            "Install it with: pip install libpysal"
        ) from exc

    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if style not in VALID_STYLES:
        raise ValueError(
            msg["err_style"].format(style=style, valid=", ".join(VALID_STYLES))
        )

    if not isinstance(municipalities, gpd.GeoDataFrame):
        raise TypeError(msg["err_not_gdf"])
    if len(municipalities) == 0:
        raise ValueError(msg["err_no_rows"])

    n_muni = len(municipalities)
    snap_val = snap if snap is not None else _DEFAULT_SNAP

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_validate"].format(n_muni=n_muni))

    # -- repair invalid geometries (mirrors sf::st_make_valid()) -------------
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_makevalid"])
    municipalities = municipalities.set_geometry(municipalities.geometry.make_valid())

    # -- row labels: code_muni if present, else 1-based integer indices ------
    if "code_muni" in municipalities.columns:
        row_names = municipalities["code_muni"].astype(str).tolist()
    else:
        row_names = [str(i) for i in range(1, n_muni + 1)]

    # -- build binary neighbour structure -------------------------------------
    type_nb = "Queen" if queen else "Rook"
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_nb"].format(type_nb=type_nb, snap_val=snap_val)
        )

    contiguity_cls = Queen if queen else Rook
    w_bin = contiguity_cls.from_dataframe(
        municipalities, ids=row_names, silence_warnings=True
    )

    # -- detect islands --------------------------------------------------------
    cardinalities = w_bin.cardinalities  # dict id -> neighbour count
    island_ids = [rid for rid in row_names if cardinalities.get(rid, 0) == 0]
    n_islands = len(island_ids)

    if n_islands > 0 and not zero_policy:
        raise ValueError(msg["err_islands_strict"].format(n_islands=n_islands))

    if n_islands > 0 and verbose:
        console.print(
            "[yellow]WARNING[/]  "
            + msg["warn_islands"].format(n_islands=n_islands, ids=", ".join(island_ids))
        )

    # -- build the styled weights object --------------------------------------
    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_listw"].format(style=style))

    listw = _apply_style(w_bin, style, PysalW)

    # -- optional dense matrix -------------------------------------------------
    W: Any = None
    if return_matrix:
        if verbose:
            console.print("[cyan]INFO[/]  " + msg["step_matrix"].format(n=n_muni))
        W, _ = listw.full()

    # -- summary stats -----------------------------------------------------------
    nonzero_card = [c for c in cardinalities.values() if c > 0]
    mn = round(float(np.mean(nonzero_card)), 2) if nonzero_card else 0.0
    mi = min(nonzero_card) if nonzero_card else 0
    mx = max(nonzero_card) if nonzero_card else 0

    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(n=n_muni, n_isl=n_islands, mn=mn, mi=mi, mx=mx)
        )

    now = datetime.now()
    nb = {rid: list(w_bin.neighbors.get(rid, [])) for rid in row_names}

    meta = {
        "stage": "mod",
        "type": "spatial_weights",
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"sus_mod_spatial_weights(): style={style}; queen={queen}; "
            f"n_regions={n_muni}; n_islands={n_islands}"
        ],
    }

    return {
        "listw": listw,
        "nb": nb,
        "n_regions": n_muni,
        "n_islands": n_islands,
        "island_ids": island_ids,
        "W": W,
        "style": style,
        "meta": meta,
    }


def _apply_style(w_bin: Any, style: str, pysal_w_cls: Any) -> Any:
    """Return a ``libpysal.weights.W`` object normalised to *style*.

    ``"W"``, ``"B"``, and ``"S"`` map directly onto libpysal's native
    ``transform`` codes (``"R"``, ``"B"``, ``"V"`` respectively).
    ``"C"``, ``"U"``, and ``"minmax"`` have no libpysal-native
    transform and are computed by hand from the binary neighbour
    structure, following the formulas documented in
    ``spdep::nb2listw()`` (Bivand et al.):

    - ``"C"`` (globally standardised): every neighbour edge gets
      weight ``n / L``, where ``L`` is the total number of neighbour
      links (sum of all cardinalities) — the whole matrix sums to
      ``n``, matching R's "sums over all links to n".
    - ``"U"`` (``"C"`` divided by ``n``): every neighbour edge gets
      weight ``1 / L`` — the whole matrix sums to ``1``, matching R's
      "sums over all links to unity".
    - ``"minmax"``: every neighbour edge gets weight
      ``1 / max(cardinalities)``. R's ``spdep`` documents this as
      ``1 / max(max(rowSums(B)), max(colSums(B)))``; for the symmetric
      binary contiguity matrices built here row sums and column sums
      always coincide with cardinalities, so the two formulas agree.
    """
    if style == "W":
        w = pysal_w_cls(w_bin.neighbors, id_order=w_bin.id_order, silence_warnings=True)
        w.transform = "R"
        return w
    if style == "B":
        w = pysal_w_cls(w_bin.neighbors, id_order=w_bin.id_order, silence_warnings=True)
        w.transform = "B"
        return w
    if style == "S":
        w = pysal_w_cls(w_bin.neighbors, id_order=w_bin.id_order, silence_warnings=True)
        w.transform = "V"
        return w

    cardinalities = w_bin.cardinalities
    n = len(cardinalities)
    total_links = sum(cardinalities.values())

    if style == "C":
        edge_weight = (n / total_links) if total_links > 0 else 0.0
    elif style == "U":
        edge_weight = (1.0 / total_links) if total_links > 0 else 0.0
    elif style == "minmax":
        max_card = max(cardinalities.values()) if cardinalities else 0
        edge_weight = (1.0 / max_card) if max_card > 0 else 0.0
    else:  # pragma: no cover - guarded by VALID_STYLES check upstream
        raise ValueError(f"Unknown style: {style}")

    weights = {
        rid: [edge_weight] * len(neighbours) for rid, neighbours in w_bin.neighbors.items()
    }
    return pysal_w_cls(
        w_bin.neighbors, weights, id_order=w_bin.id_order, silence_warnings=True
    )
