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

#: Folga usada só para escolher os pares CANDIDATOS a vizinho, antes do
#: teste de ``snap``. É a contrapartida da imprecisão que o s2 injeta no
#: ``st_intersects()`` do lado do R -- a ajuda do ``spdep::poly2nb`` diz
#: que, em geometria esférica, o ``snap`` vale só para confirmar o
#: vizinho, e os candidatos saem do ``st_intersects()``.
#:
#: O valor é o default geográfico que o próprio ``poly2nb`` documenta
#: (~10 mm). Não é número ajustado a dedo: varrido em SP, MG e BA contra
#: o R, qualquer folga de 1e-8 a 1e-6 dá o mesmo resultado, e é o melhor
#: resultado -- SP e MG exatos, BA com 6 de 2.374. Abaixo disso a BA
#: perde 24; a partir de 1e-5 MG deixa de ser exato; em 1e-4 os três
#: pioram juntos. 1e-7 fica no meio do platô.
_CANDIDATE_EPS = 1e-7


def _poly2nb_snap(
    geoms: Any,
    row_names: list[str],
    snap: float,
    queen: bool,
) -> dict[str, list[str]]:
    """Contiguidade com tolerância, como ``spdep::poly2nb(snap=)``.

    O ``libpysal.weights.Queen``/``Rook`` só liga dois polígonos quando
    eles COMPARTILHAM vértice -- coordenada idêntica, bit a bit.
    Fronteira digitalizada em escalas diferentes não tem coordenada
    idêntica, tem coordenada PRÓXIMA, e cada vão de micrograu sumia com
    um vizinho sem avisar. O grafo do Python era um SUBCONJUNTO ESTRITO
    do do R: 3.520 arestas contra 3.600 em SP, 3.658 contra 4.778 em MG
    (23% a menos) e 1.674 contra 2.374 na BA (29%). E 333 das 3.520 em
    comum em SP ainda saíam com PESO errado, porque o estilo ``"W"``
    divide por cardinalidade (M172).

    A regra aqui é a da ajuda do ``poly2nb``, ao pé da letra: *"boundary
    points less than `snap` distance apart are considered to indicate
    contiguity"*, com ``criterion`` de 1 ponto (queen) ou 2 (rook).

    Args:
        geoms: Sequência de geometrias shapely, já reparadas.
        row_names: Identificador de cada geometria, na mesma ordem.
        snap: Distância em unidades do CRS. Pontos de fronteira a menos
            que isso indicam contiguidade.
        queen: ``True`` basta um ponto próximo; ``False`` exige dois.

    Returns:
        ``dict`` de identificador para a lista ordenada de vizinhos.
    """
    import numpy as np
    import shapely
    from scipy.spatial import cKDTree

    criterio = 1 if queen else 2
    n = len(row_names)
    arr = np.asarray(geoms, dtype=object)

    # Os vértices de cada polígono -- todos os anéis, todas as partes.
    pontos = [shapely.get_coordinates(g) for g in arr]
    arvores = [cKDTree(p) if len(p) else None for p in pontos]

    # Candidatos: quem quase se toca. Ver _CANDIDATE_EPS.
    pares = shapely.STRtree(arr).query(
        arr, predicate="dwithin", distance=_CANDIDATE_EPS
    ).T

    vizinhos: dict[int, set[int]] = {i: set() for i in range(n)}
    for i, j in pares:
        if i >= j or arvores[i] is None or arvores[j] is None:
            continue
        # Consulta a árvore do polígono com MENOS vértices, com os pontos
        # do outro: mesma conta, menos trabalho.
        a, b = (i, j) if len(pontos[i]) <= len(pontos[j]) else (j, i)
        proximos = arvores[a].query_ball_point(pontos[b], r=snap)
        if sum(len(v) for v in proximos) >= criterio:
            vizinhos[i].add(j)
            vizinhos[j].add(i)

    return {
        row_names[i]: sorted(row_names[j] for j in vizinhos[i]) for i in range(n)
    }


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
        that boundaries within that distance are treated as touching,
        and its help states the rule plainly: *"boundary points less
        than `snap` distance apart are considered to indicate
        contiguity"*, needing one such point for queen and two for rook.

        ``libpysal.weights.Queen``/``Rook`` take a stricter view: they
        link two polygons only when a vertex is **shared**, i.e. the
        coordinate is identical bit for bit. Boundaries digitised at
        different scales do not have identical coordinates, they have
        *near* ones. So *snap* used to be accepted here **for signature
        parity only**, its value reaching nothing but the progress
        message — which printed ``snap = 0.001`` while the code snapped
        nothing.

        That gap was measured on 30/10/2026 and closed (M172). Fed the
        identical WKT, with the bounding boxes matching on every
        municipality in all four corners — so the divergence was purely
        algorithmic — the two packages disagreed, and by far more than
        the first state suggested::

            queen       R      this, before   this, after
            SP      3,600    3,520 (-80)        3,600   exact
            MG      4,778    3,658 (-1,120)     4,778   exact
            BA      2,374    1,674 (-700)       2,376   +4/-2

        Minas Gerais was losing 23% of its links and Bahia 29%. São
        Paulo's 2% was the *benign* case, and was the only one measured
        before, which is how the gap passed for rounding.

        The damage did not stop at the missing links. Style ``"W"``
        divides by cardinality, so one lost neighbour reweights every
        other neighbour of that municipality: in SP, **333 of the 3,520
        links the two sides had in common carried a different weight**,
        9.5% of the matrix. The total of the weights matched exactly
        (644 either side), because the row-standardisation itself was
        right — checking the sum would have revealed nothing.

        :func:`_poly2nb_snap` closes it. Exact against R on SP and MG,
        queen and rook; Bahia still differs on 6 of 2,374 directed
        links (0.25%), all sub-metre knife-edge cases — two pairs at
        4e-10 and 2e-9 degrees that this links and R does not, and one
        pair 0.36 m apart whose closest approach is mid-segment rather
        than vertex to vertex. ``snap=0`` asks for the exact-topology
        path instead.

        **What snap does not do**: it does not close a 111 m gap, here
        or in R, although ``1e-3`` degrees is the default on both
        sides. Measured in R with two squares and a growing gap, under
        ``snap = 1e-3``: gaps of 0 and 1e-9 become contiguity, 1e-7
        (1 cm) and above do not. For spherical geometry the candidate
        pairs come from ``st_intersects()``, with s2's own tolerance,
        and *snap* only confirms them. ``tests/test_spatial_weights_snap.py``
        pins that contract to a ten-case matrix taken from climasus4r.

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
        snap: Distance tolerance for neighbour detection, in the units
            of the CRS. Boundary points less than this far apart count
            as contiguity. ``None`` (default) resolves to ``1e-3``,
            mirroring R; ``0`` asks for exact topology instead. See
            **Snap distance** above.
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

    if snap_val and snap_val > 0:
        # O caminho com tolerância, que é o do R. Ver _poly2nb_snap e M172.
        w_bin = PysalW(
            dict(_poly2nb_snap(
                municipalities.geometry.values, row_names, snap_val, queen
            )),
            id_order=list(row_names),
            silence_warnings=True,
        )
    else:
        # snap=0 pede topologia exata, que é o que o libpysal faz nativamente.
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
