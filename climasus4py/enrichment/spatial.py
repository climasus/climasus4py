"""Spatial enrichment — join health data with Brazilian municipality geometries (lazy).

Mirrors R: spatial.R

Lazy contract: takes a ``DuckDBPyRelation``, JOINs against a spatial
parquet from ``climasus-data`` (or a custom path), and returns a
``DuckDBPyRelation``. Health data is never materialised internally —
the relation stays lazy until the user calls ``.df()`` or
``cs.materialize(...)``.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from uuid import uuid4

import duckdb

from ..core._guards import _unwrap_sus_relation
from ..core._sql import quote_ident, sql_string
from ..core._stage import add_history, set_stage
from ..core.engine import get_connection
from ..utils.data import detect_geo_column, load_json, spatial_asset_path

_REQUIRED_SPATIAL_COLS = {"geometry_wkt", "name"}


def _mapa_de_bases() -> dict[str, list[str]]:
    """``municipality_by_basis`` from the shared metadata, without the key
    that holds the fallback order.

    Read from ``climasus-data`` rather than copied here: a second copy of
    a priority list is exactly what produced M119 and M138.
    """
    cfg = load_json("metadata/datasus_columns.json")
    mapa = cfg.get("municipality_by_basis", {})
    return {k: v for k, v in mapa.items() if k != "_fallback_order"}


def _bases_validas() -> set[str]:
    return set(_mapa_de_bases())


def _apelidos_de_base() -> dict[str, str]:
    """Every accepted spelling of a basis → its canonical English key.

    The keys of ``municipality_by_basis`` are English, but the package is
    trilingual and ``sus_filter`` already takes values in the three
    languages (``sex="Masculino"``), so ``basis=`` had to as well.
    Portuguese is also what ``climasus4r`` calls these internally — its
    ``geo_groups`` are ``residencia``, ``ocorrencia``, ``notificacao``,
    ``generico`` — so whoever comes from the R side types those first.

    Read from ``climasus-data`` rather than hardcoded, for the same
    reason the column lists are (M119, M138).
    """
    from ..utils.data import _norm_city

    cfg = load_json("metadata/datasus_columns.json")
    mapa: dict[str, str] = {}
    for canonica, grafias in cfg.get("basis_aliases", {}).items():
        if canonica.startswith("_"):
            continue
        for g in grafias:
            mapa[_norm_city(g)] = canonica
    # The canonical keys always resolve, even if an alias list is missing.
    for canonica in _mapa_de_bases():
        mapa.setdefault(_norm_city(canonica), canonica)
    return mapa


def _base_da_coluna(coluna: str) -> str | None:
    """Which basis owns *coluna*, or ``None`` if no basis claims it."""
    for base in _mapa_de_bases():
        if coluna in _colunas_da_base(base):
            return base
    return None


def resolve_join_col(
    join_col: str, colunas: list[str]
) -> tuple[str | None, str | None]:
    """Resolve *join_col* to ``(coluna, base)``.

    Accepts three things, in this order:

    1. **A basis** — ``"residence"``, ``"residencia"``, ``"ocorrência"``,
       ``"generico"``… Accent- and case-insensitive. The column is then
       resolved from the data, so the caller need not know which of the
       four spellings of that column the frame happens to carry.
    2. **A column name a basis claims** — ``"CODMUNRES"``,
       ``"residence_municipality_code"``. This is the form
       ``climasus4r::sus_spatial_join(join_col=)`` takes, so a value
       copied from R code works here unchanged.
    3. **Any other column present in the data** — same permissiveness as
       R, which only checks ``join_col %in% names(df)``.

    Returns ``(None, base)`` when a basis was named but the data has no
    column of it: the caller decides whether that warns or raises.
    """
    from ..utils.data import _norm_city

    apelidos = _apelidos_de_base()
    base = apelidos.get(_norm_city(join_col))
    if base is not None:
        return detect_geo_column(colunas, level="municipality",
                                 basis=base), base

    if join_col in colunas:
        return join_col, _base_da_coluna(join_col)

    raise ValueError(
        f"join_col={join_col!r} is neither a basis nor a column of this "
        f"data. Pass a basis — {sorted(apelidos)} — or the name of a "
        f"column that is present. Available geographic columns: "
        f"{[c for c in colunas if _base_da_coluna(c)]}."
    )


def _colunas_da_base(basis: str) -> set[str]:
    from ..utils.data import expand_column_synonyms

    return set(expand_column_synonyms(_mapa_de_bases().get(basis, [])))


def _sql_path(p: Path) -> str:
    """Return a safely-quoted SQL string for a filesystem path."""
    return sql_string(str(p).replace("\\", "/"))


def _case_from_map(expr: str, mapping: dict[str, str]) -> str:
    """Build a SQL ``CASE`` translating *expr* through *mapping*.

    Inline rather than a registered lookup table so the relation stays
    lazy: 27 states and 5 regions are small enough that the SQL text
    costs nothing, and registering a table would tie the result to one
    connection.
    """
    if not mapping:
        return "NULL"
    ramos = " ".join(
        f"WHEN {sql_string(k)} THEN {sql_string(v)}" for k, v in mapping.items()
    )
    return f"CASE {expr} {ramos} ELSE NULL END"


def _state_names(lang: str) -> dict[str, str]:
    """UF abbreviation to state name, in *lang*.

    Returns an empty mapping when the metadata is unreadable, and the
    caller then emits ``NULL`` for the label. Joining geometry is this
    function's job; a human-readable state name is a convenience, and a
    missing convenience must not take the join down with it.
    """
    try:
        estados = load_json("metadata/uf_codes.json").get("states", {})
    except (FileNotFoundError, ValueError, KeyError):
        return {}
    saida: dict[str, str] = {}
    for sigla, dados in estados.items():
        nome = dados.get("name", {})
        if isinstance(nome, dict):
            rotulo = nome.get(lang) or nome.get("pt") or nome.get("en")
        else:
            rotulo = nome
        if rotulo:
            saida[str(sigla)] = str(rotulo)
    return saida


def _region_names(lang: str) -> dict[str, str]:
    """Region slug (``"sudeste"``) to its label, in *lang*.

    Empty when the metadata is unreadable — see :func:`_state_names`.
    """
    try:
        categorias = load_json("metadata/regions.json").get("categories", {})
    except (FileNotFoundError, ValueError, KeyError):
        return {}
    regioes = categorias.get("ibge_macro", {}).get("regions", {})
    saida: dict[str, str] = {}
    for slug, dados in regioes.items():
        rotulo = dados.get("label", {})
        if isinstance(rotulo, dict):
            rotulo = rotulo.get(lang) or rotulo.get("pt") or rotulo.get("en")
        if rotulo:
            saida[str(slug)] = str(rotulo)
    return saida


def sus_spatial_join(
    rel: duckdb.DuckDBPyRelation,
    *,
    spatial_path: str | Path | None = None,
    geo_level: str = "municipality",
    simplified: bool = True,
    join_col: str | None = None,
    lang: str = "pt",
) -> duckdb.DuckDBPyRelation:
    """Join health data with Brazilian municipality or state spatial data (lazy).

    Reads a spatial parquet from ``climasus-data`` (or *spatial_path*)
    via DuckDB SQL. The relation remains lazy until the user materialises
    with ``.df()`` or ``cs.materialize(...)``.

    Mirrors ``climasus4r::sus_spatial_join`` (legacy reference).

    Args:
        rel: Lazy DuckDB relation with health data containing a
            recognised municipality column.
        spatial_path: Custom path to a spatial parquet that must contain
            ``code_muni``, ``name``, and ``geometry_wkt`` columns. Uses
            the bundled municipalities parquet when ``None``, at the
            resolution *simplified* selects.
        geo_level: Geographic level — ``"municipality"`` (default) or
            ``"state"``.
        simplified: ``True`` (default) joins the simplified geometry,
            which is what ``climasus4r`` gets from
            ``geobr::read_municipality(simplified = TRUE)``. The polygon
            is repeated on every health row, so the resolution decides
            the size of anything you export: measured on 5,196 rows, a
            Parquet of 2.1 MB simplified against 31.0 MB full (M9).
            Pass ``False`` for full-resolution boundaries. Ignored when
            *spatial_path* is given, since that names a file directly.
        join_col: Which municipality the geometry comes from. Same name
            as ``climasus4r::sus_spatial_join(join_col=)``, and it takes
            the R form plus two more:

            - a **basis** — ``"residence"``, ``"occurrence"``,
              ``"notification"``, ``"unspecified"``, or the Portuguese
              and Spanish spellings (``"residencia"``, ``"ocorrência"``,
              ``"notificacion"``, ``"generico"``), accent- and
              case-insensitive. Preferred: the same column has four
              spellings depending on the stage, and naming the basis
              spares the caller from knowing which one the frame carries.
            - a **column name a basis claims** — ``"CODMUNRES"``,
              ``"residence_municipality_code"``. This is what R takes, so
              a value copied from R code works unchanged.
            - **any other column present in the data**, which is all R
              checks.

            ``None`` (default) keeps the shared detection order, which is
            residence first.

            **The choice is not cosmetic.** Measured on SIM-DO/SP/2023,
            residence and occurrence disagree on **20.4%** of records
            (1,059 of 5,196), and the disagreement crosses state lines:
            a resident of Planalto/SP who died in Boa Vista/RR gets the
            polygon of Planalto under ``"residence"`` and of Boa Vista
            under ``"occurrence"``.

            Residence is the default because this is a health **and
            climate** package: what matters is the climate the person
            was exposed to, not the municipality they happened to die
            in. ``climasus4r`` chooses the opposite — its
            ``priority_order`` puts occurrence in the first three
            positions (M137).
        lang: Language of the ``name_state`` and ``name_region`` labels —
            ``"pt"`` (default, as in R), ``"en"`` or ``"es"``.

    Returns:
        ``DuckDBPyRelation`` with the health columns plus ``spatial_name``,
        ``geometry_wkt``, ``code_muni_7``, ``code_state``,
        ``abbrev_state``, ``name_state``, ``code_region`` and
        ``name_region``. Registers ``stage="spatial"`` in sus_meta with a
        timestamped history entry — it used to register the generic
        ``"enrichment"``, which the climate and census joins wrote too,
        so the three were indistinguishable afterwards (M34).

        The six geographic fields beyond name and geometry closed M8:
        analysis by state or region used to need a manual join, because
        the join returned only the municipality name and its polygon.
        ``code_state`` and ``code_region`` are taken from the first two
        and the first digit of the IBGE municipality code — exact by
        construction, verified one-to-one across all 27 states — rather
        than from a lookup that could drift.

        A field is **skipped with a warning** when the health data
        already has a column of that name, instead of being added twice.
        R suffixes the collision (its ``year.x``/``year.y`` are exactly
        that), which silently leaves two columns where the caller
        expected one.

    Raises:
        TypeError: If *rel* is not a DuckDB relation.
        ValueError: If health data has no recognised geo column, or if
            the spatial parquet is missing required columns.

    Example:
        >>> import climasus4py as cs
        >>> out = cs.sus_spatial_join(rel)
        >>> out.df()["geometry_wkt"].iloc[0]
        'POINT (-46.63 -23.55)'
    """
    _ = geo_level  # reserved for future state-level joins; preserved for parity
    _original_rel = rel
    rel = _unwrap_sus_relation(rel, "sus_spatial_join")

    if spatial_path is None:
        resolved_path: Path = spatial_asset_path(
            "municipalities", simplified=simplified
        )
    else:
        resolved_path = Path(spatial_path)

    conn = get_connection()

    # Validate spatial parquet schema before building the join
    spatial_rel = conn.read_parquet(str(resolved_path).replace("\\", "/"))
    spatial_cols = set(spatial_rel.columns)
    missing = _REQUIRED_SPATIAL_COLS - spatial_cols
    if missing:
        raise ValueError(
            f"Spatial parquet missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(spatial_cols)}"
        )

    colunas = list(rel.columns)

    # `basis` was accepted by detect_geo_column all along and no public
    # function passed it, so the caller had no way to ask for occurrence
    # even when occurrence was what they wanted (M137).
    if join_col is not None:
        geo_col, base_pedida = resolve_join_col(join_col, colunas)

        # Naming a basis the data cannot satisfy is a silent wrong answer,
        # so it warns — but it falls back to the *shared detection order*,
        # not to the literal "municipality_code": that name is a last
        # resort for data with no recognised column at all, and using it
        # here produced a binder error against a column that does not
        # exist.
        if geo_col is None:
            geo_col = detect_geo_column(colunas, level="municipality")
            warnings.warn(
                f"sus_spatial_join: join_col={join_col!r} resolves to the "
                f"{base_pedida!r} basis, but this data has no column of "
                f"it; the geometry is being joined on {geo_col!r} instead. "
                f"Residence and occurrence disagree on about 20% of SIM "
                f"records, so this is not a cosmetic fallback (M137).",
                UserWarning,
                stacklevel=2,
            )
    else:
        geo_col = detect_geo_column(colunas, level="municipality")

    geo_col = geo_col or "municipality_code"

    p_sql = _sql_path(resolved_path)
    h_geo = quote_ident(geo_col)

    # --- geographic metadata (M8) ----------------------------------------
    # The join used to return the municipality name and its polygon and
    # nothing else, so any analysis by state or region needed a manual
    # join against another table. R returns the state fields because
    # geobr's layer carries them; the region fields it gets only through
    # `sus_census_join`, so these two go slightly beyond parity — and
    # they cost nothing, since the IBGE code already encodes them.
    codigo7 = 'CAST(s.code_muni AS VARCHAR)'
    candidatas: list[tuple[str, str]] = [
        ("spatial_name", "s.name"),
        ("geometry_wkt", "s.geometry_wkt"),
        ("code_muni_7", codigo7),
        # First two digits are the IBGE state code, the first is the
        # region code — one-to-one across all 27 states, checked.
        ("code_state", f"LEFT({codigo7}, 2)"),
        ("abbrev_state", "s.state" if "state" in spatial_cols else "NULL"),
        ("name_state", _case_from_map("s.state", _state_names(lang))
         if "state" in spatial_cols else "NULL"),
        ("code_region", f"LEFT({codigo7}, 1)"),
        ("name_region", _case_from_map("s.region", _region_names(lang))
         if "region" in spatial_cols else "NULL"),
    ]

    ja_existem = set(rel.columns)
    projecoes: list[str] = []
    puladas: list[str] = []
    for nome, expr in candidatas:
        if nome in ja_existem:
            puladas.append(nome)
            continue
        projecoes.append(f'{expr} AS "{nome}"')

    if puladas:
        warnings.warn(
            f"sus_spatial_join: health data already has "
            f"{', '.join(puladas)}; kept the existing column(s) instead of "
            f"adding a second one. R would suffix the collision (its "
            f"year.x/year.y are exactly that), leaving two columns where "
            f"you asked for one.",
            UserWarning,
            stacklevel=2,
        )

    # Every candidate colliding is unlikely but not impossible — joining
    # an already-joined relation does it — and an empty list would leave
    # a dangling comma in the SQL.
    seleciona = "h.*" + (f", {', '.join(projecoes)}" if projecoes else "")
    # The view name has to be unique per call. With a fixed
    # `_spatial_health`, joining the result of a join made DuckDB try to
    # bind the view inside its own definition and fail with "infinite
    # recursion detected: attempting to recursively bind view" — a
    # message that says nothing about this function to whoever just
    # re-ran a pipeline step. See M122.
    vista = f"_spatial_health_{uuid4().hex[:12]}"
    sql = (
        f"SELECT {seleciona} "
        f"FROM {vista} h "
        f"LEFT JOIN read_parquet({p_sql}) s "
        f"  ON LEFT(CAST(h.{h_geo} AS VARCHAR), 6) "
        f"   = LEFT(CAST(s.code_muni AS VARCHAR), 6)"
    )

    result = rel.query(vista, sql)
    result = result.set_alias("enrichment")
    # "spatial", not the generic "enrichment" the three joins all
    # wrote: a spatial join, a climate join and a census join were
    # indistinguishable in the metadata, and climasus4r tells them
    # apart. See M34.
    result = set_stage(result, "spatial", _inherit_from=_original_rel)
    result = add_history(
        result,
        f"Spatial join: {len(projecoes)} column(s) added "
        f"({', '.join(n for n, _ in candidatas if n not in ja_existem)}) "
        f"via {resolved_path.name}; geo_col={geo_col}; geo_level={geo_level}; "
        f"simplified={simplified}; lang={lang}"
        + (f"; skipped (already present): {', '.join(puladas)}"
           if puladas else "")
    )
    return result
