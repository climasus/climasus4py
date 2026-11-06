"""Column standardization, value translation and date conversion.

Mirrors R: standardize.R
Pipeline stage: "standardize" (registered in sus_meta after processing).
"""

from __future__ import annotations

import warnings

import duckdb

from ..utils.data import detect_system, load_json
from ._guards import _assert_lazy
from ._stage import add_history, get_meta, set_stage
from .engine import get_connection, schema_columns

# ---------------------------------------------------------------------------
# Fixed-width DATASUS codes and their official widths
# ---------------------------------------------------------------------------
# Keyed by the RAW DATASUS name, and applied before the rename on purpose.
# Keying by the translated names would need one entry per language *and* per
# system: CODESTAB is health_facility_code / codigo_estabelecimento /
# codigo_establecimiento_salud, and CODOCUPMAE is mother_occupation_code in
# SIM-DO but mother_occupation_cbo in SINASC — with no PT translation at all
# in one of them. One entry per raw column covers every language and system.
#
# The declaration now lives in the climasus-data metadata, as
# ``all_identifier_columns`` of ``metadata/datasus_columns.json``
# (schema_version 2, M64). This used to be a hardcoded stopgap here,
# labelled as one, because splitting the shared list needed the
# coordinator; that decision was taken on 21/09/2026.
#
# Kept as a function rather than a constant so an install that updates
# climasus-data does not need a climasus4py release to see a new code.
def _code_column_widths() -> dict[str, int]:
    """Fixed-width identifier columns and their widths, from the metadata.

    Entries with a null width are skipped: the metadata declares them as
    identifiers whose width could not be measured, and padding to a
    guessed width would invent digits.
    """
    from ..utils.data import load_datasus_columns_spec

    declarados = load_datasus_columns_spec().get(
        "all_identifier_columns") or {}
    return {col: int(w) for col, w in declarados.items() if w}


def _yyyymmdd_shape_sql(bruto: str) -> str:
    """SQL condition: *bruto* (a VARCHAR expression) is shaped ``YYYYMMDD``.

    Eight characters, the first four a year in 1900-2100 and the next two
    a month 01-12. A genuine ``DDMMYYYY`` date can never satisfy it: its
    characters 5-6 are the start of its year — 19, 20 or 21 for any real
    date — and none of those is a month. So the test splits the two
    shapes without looking at the data, and the day is left to strptime,
    which rejects 30 February and the like. Shared with the fast path of
    ``sus_pipeline`` (``core/pipeline.py``), which parses dates on its own.
    """
    return (
        f"(LENGTH({bruto}) = 8 "
        f"AND TRY_CAST(SUBSTR({bruto}, 1, 4) AS INTEGER) BETWEEN 1900 AND 2100 "
        f"AND TRY_CAST(SUBSTR({bruto}, 5, 2) AS INTEGER) BETWEEN 1 AND 12)"
    )


#: Systems whose eight-digit dates are written year first (YYYYMMDD).
#: climasus4r picks the format by system, not by value, and so must we
#: for the values that fit both readings — see ``_date_parse_expr``.
#: SIA joined on 05/11/2026: its DTNASC (SIA-PS, SIA-SAD) and the APAC
#: dates are YYYYMMDD too, and a typo in SIA-AD ``request_date`` came out
#: 0101-03-02 here against 0203-01-01 in the R. The importer reads this
#: same tuple to keep those columns as text in the cache (M188).
_SISTEMAS_ANO_PRIMEIRO = ("SIH", "SINAN", "CNES", "SIA")


def _ano_primeiro(system: str | None) -> bool:
    return bool(system) and str(system).upper().split("-")[0] in _SISTEMAS_ANO_PRIMEIRO


def _date_parse_expr(col: str, ano_primeiro: bool = False) -> str:
    """SQL that turns *col* into a DATE, whatever shape DATASUS wrote it in.

    Four shapes, tried in order, and the order is the whole design:

    1. ``YYYYMM`` — six digits naming a MONTH, resolved to its first day.
    2. ``YYYYMMDD`` — eight digits, year first: SIH, SINAN and CNES.
    3. ``DDMMYYYY`` — the usual DATASUS string, ``05012023``: SIM, SINASC.
    4. Anything DuckDB casts on its own, which covers an ISO string and a
       column that is already a DATE.

    Shape 2 was missing until 05/11/2026 (M181), and its absence was
    silent. ``20230113`` (DT_INTER, SIH) read as ``DDMMYYYY`` gives month
    23 and became NULL; ``20080710`` gave day 20, month 08, year 0710 and
    became 0710-08-20 — an absurd date, not a null. Measured: admission,
    discharge and birth dates of SIH-RD 0% equal to climasus4r, the SINAN
    notification date NULL in all 73,778 rows, and the calendar
    variables of both systems 100% NULL — so neither could be joined to
    climate. When a value has the ``YYYYMMDD`` shape (see
    ``_yyyymmdd_shape_sql``) it is parsed ONLY that way: an invalid day
    such as ``20080732`` comes back NULL instead of falling through to
    ``DDMMYYYY`` and becoming 0732-08-20.

    ``ano_primeiro=True`` (SIH, SINAN, CNES — ``_ano_primeiro(system)``)
    reads EVERY eight-digit value as ``YYYYMMDD``, whatever the year, as
    climasus4r does for those systems. It only matters for typos, which
    may fit both readings: CNES ``permit_issue_date`` ``02070117`` is
    0207-01-17 in the R and was 0117-07-02 here — 13 of 85,574 rows,
    measured. Absurd years are kept, in both packages; M185 proposes
    nulling them with a warning in both.

    Shape 1 exists for SIA, whose ``PA_CMP`` (competence) and ``PA_MVM``
    (processing) are month-granularity: ``202301`` means January 2023, not
    a day. Neither of the other two parses it — ``TRY_CAST('202301' AS
    DATE)`` is NULL.

    WHY IT GOES FIRST, which is not obvious and was measured. ``%d%m%Y``
    does not require four digits for the year, so on a six-digit string
    it reads the first two as the day and the LAST TWO as the year:
    ``201012``, meaning October 2010, comes back as 20/10/0012. That bites
    exactly the competence months from 2000 to 2012, where digits three
    and four happen to form a valid month — about a third of the range SIA
    has published. Putting the month shape first costs nothing, because
    its guards make it decline anything that is not a plausible
    ``YYYYMM``: the year must read 1900-2100, and ``%Y%m`` itself rejects
    a month outside 01-12.

    What the guards deliberately do NOT catch is a genuine ``DDMMYY``
    column — ``010223`` falls through to shape 2 and comes back as year
    23. That is unchanged from before this helper existed, no column in
    the candidate lists is written that way, and the shape-based fallback
    below only converts a column when 100% of its values parse.

    WHAT THE FIRST OF THE MONTH COSTS. It is a convention, not a fact: the
    source does not record a day, and every SIA row in a month lands on
    the same date. Day-of-week, is_weekend and the epidemiological week
    derived from it describe the convention and not the care. Anything
    that aggregates by month, quarter or year is unaffected. The
    alternative was to leave the column as text, which is what produced
    the silent damage this replaces: ``detect_date_column`` found nothing,
    the calendar block was skipped, and the frame came back without the
    15 variables the caller asked for. climasus4r does not get this far —
    it reaches ``as.Date("202301")`` and raises "character string is not
    in a standard unambiguous format" (M163).

    """
    bruto = f'CAST("{col}" AS VARCHAR)'
    oito_digitos = (f"regexp_full_match({bruto}, '[0-9]{{8}}')" if ano_primeiro
                    else _yyyymmdd_shape_sql(bruto))
    return (
        f"CASE WHEN {oito_digitos} "
        f"THEN TRY_STRPTIME({bruto}, '%Y%m%d')::DATE "
        f"ELSE COALESCE("
        f"CASE WHEN LENGTH({bruto}) = 6 "
        f"AND TRY_CAST(SUBSTR({bruto}, 1, 4) AS INTEGER) BETWEEN 1900 AND 2100 "
        f"THEN TRY_STRPTIME({bruto}, '%Y%m')::DATE END, "
        f"TRY_STRPTIME({bruto}, '%d%m%Y')::DATE, "
        f"{_yyyymmdd_any_year_sql(bruto)}, "
        f'TRY_CAST("{col}" AS DATE)'
        f") END"
    )


def _yyyymmdd_any_year_sql(bruto: str) -> str:
    """Last resort: ``YYYYMMDD`` with ANY year, as climasus4r reads it.

    For when the system is unknown (and for the fast path of
    ``sus_pipeline``); with a known SIH/SINAN/CNES system
    ``_date_parse_expr(ano_primeiro=True)`` already reads every
    eight-digit value this way. Reached only by an eight-digit value that
    neither the guarded ``YYYYMMDD`` shape nor ``DDMMYYYY`` accepted, so
    nothing that parsed before changes. What reaches it are typos: in
    CNES-ST SP jan/2023, 21 of 72,672 ``permit_issue_date`` values read
    ``00140221``, ``30000303``, ``10180221``... Before, those 21 made the
    whole column fail the 100% rule of the shape fallback and it stayed
    TEXT; climasus4r converts the column and keeps them as 0014-02-21,
    3000-03-03, 1018-02-21. Following the R was a decision of
    05/11/2026. Absurd years are therefore kept, in both packages — see
    M185 for the proposal to null them with a warning in both.
    """
    return (
        f"CASE WHEN regexp_full_match({bruto}, '[0-9]{{8}}') "
        f"THEN TRY_STRPTIME({bruto}, '%Y%m%d')::DATE END"
    )


# ---------------------------------------------------------------------------
# Dictionary loaders
# ---------------------------------------------------------------------------

def _load_column_dict(lang: str, system: str | None = None) -> dict[str, str]:
    """Load column translation dictionary for the requested language.

    Loads COMMON first (lowest priority), then the system-specific section
    (highest priority) to avoid conflicts between systems that share column
    names with different meanings (e.g. PESO → birth_weight in SIM vs
    birth_weight_grams in SINASC).

    Args:
        lang: Language code — ``"pt"``, ``"en"`` or ``"es"``.
        system: SUS system identifier (e.g. ``"SIM-DO"``). Used to select
            the correct section from the JSON. If ``None``, all sections
            are merged (may cause conflicts for shared column names).

    Returns:
        Flat dict mapping original DATASUS column name to translated name.
        Empty dict if language file not found.
    """
    json_path = f"dictionaries/pt-{lang}/columns.json"
    try:
        data = load_json(json_path)
    except FileNotFoundError:
        return {}

    mapping: dict[str, str] = {}

    # COMMON always loaded first (lowest priority)
    common = data.get("COMMON", {})
    if isinstance(common, dict):
        mapping.update(common)

    if system is not None:
        # extract family: "SIM-DO" → "SIM", "SIH-RD" → "SIH", "SIA-PA" → "SIA"
        family = system.split("-")[0]
        specific = data.get(system, data.get(family, {}))
        if isinstance(specific, dict):
            mapping.update(specific)  # overrides COMMON
    else:
        # no system — merge all sections (may have conflicts)
        for section, cols in data.items():
            if not section.startswith("_") and section != "COMMON":
                if isinstance(cols, dict):
                    mapping.update(cols)

    return mapping


def _load_category_dict(
    lang: str, system: str | None = None
) -> dict[str, dict[str, str]]:
    """Load category value translation dictionary for the requested language.

    Mirrors R, which keeps **one dictionary per system** —
    ``get_translation_dict_pt()$values``, ``_pt_sih``, ``_pt_sinan`` and the
    rest — and applies the one for the system at hand.

    The keys are column names **after** the rename, e.g. ``"sex"`` →
    ``{"1": "Male", "2": "Female"}``. A column the rename does not touch
    keeps its source name and is keyed by it (``TERCEIRO``, which R
    translates in no section).

    Why the system matters, with the case that forced it (M104): the
    translated column ``education_level`` is fed by ``INSTRU`` from **SIH**
    (``1`` Analfabeto, ``2`` 1 Grau, ``3`` 2 Grau, ``4`` 3 Grau) *and* by
    ``CS_ESCOL_N`` from **SINAN** (``1`` "1 a 4 serie incompleta EF", ``2``
    "4 serie completa EF", ...). The same codes mean different things, and
    a flat file can only hold one book — so SINAN data came out labelled
    with the SIH scale.

    **Merged per column, replaced per book.** A system section replaces the
    whole code book of every column it declares, and COMMON still covers
    the columns it does not mention.

    Per book and not per code, because R is the authority and R merges
    nothing: ``.standardize_tibble_internal`` picks exactly **one**
    dictionary — ``if (system == "SINAN") translations <-
    get_translation_dict_pt_sinan()`` — and iterates it. A per-code merge
    was the first thing written here and it was wrong in the way this
    whole finding is about: with SIH data, ``education_level`` code ``5``
    would have kept the label SINAN gives it, because the SIH book stops
    at ``4``. Labelling a code with another system's book is inventing a
    meaning, which is the M70/M79 defect by another route. Under
    replacement that code comes out raw, which is honest.

    COMMON is kept as the fallback for unmentioned columns, and that part
    *is* a superset of R: R would leave those untranslated. The gain is
    real — it is what the M7 work restored — and it cannot relabel
    anything, because a column the system's book does not mention has no
    competing meaning to lose.

    Args:
        lang: Language code — ``"pt"``, ``"en"`` or ``"es"``.
        system: SUS system identifier (e.g. ``"SIM-DO"``, ``"SINAN-DENG"``).
            Selects the section, falling back to the family (``"SIA-PA"`` →
            ``"SIA"``). ``None`` returns COMMON alone — the generic book,
            which is byte for byte what every caller got before this file
            gained sections.

    Returns:
        Dict mapping column_name → {source_value → label}. Empty dict if
        the language file is not found.
    """
    json_path = f"dictionaries/pt-{lang}/categories.json"
    try:
        data = load_json(json_path)
    except FileNotFoundError:
        return {}

    secoes = {k: v for k, v in data.items()
              if not k.startswith("_") and isinstance(v, dict)}

    # Pre-5.0 files are flat: column name straight at the top level, with
    # no COMMON. Detected by the absence of COMMON rather than by the
    # version string, so a hand-edited file without the bump still loads.
    if "COMMON" not in secoes:
        return secoes

    mapping: dict[str, dict[str, str]] = {
        col: dict(livro) for col, livro in secoes["COMMON"].items()
        if isinstance(livro, dict)
    }

    def sobrepor(secao: dict) -> None:
        for col, livro in secao.items():
            if isinstance(livro, dict):
                mapping[col] = dict(livro)

    if system is not None:
        family = system.split("-")[0]
        especifica = secoes.get(system, secoes.get(family))
        if isinstance(especifica, dict):
            sobrepor(especifica)

    # With no system, COMMON alone — deliberately NOT a merge of every
    # section, which is where :func:`_load_column_dict` goes. Merging
    # column *names* mostly works, because the raw names differ between
    # systems. Merging code *books* picks a winner per code, and the
    # winner is whichever section the loop reached last: measured, code
    # "3" of ``sex`` came out "Unknown" from a late section where the flat
    # file said "Female". That would relabel data silently, and for a
    # reason as arbitrary as key order. COMMON is the generic book, so it
    # is also exactly what every caller got before this file gained
    # sections.
    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_standardize(
    rel: duckdb.DuckDBPyRelation,
    *,
    lang: str = "en",
    system: str | None = None,
) -> duckdb.DuckDBPyRelation:
    """Standardize column names, translate categorical values and convert dates.

    Mirrors ``climasus4r::sus_data_standardize()``. Three operations in order:

    1. **Column rename** — maps DATASUS uppercase names to readable names in
       the requested language using ``dictionaries/pt-{lang}/columns.json``.
       Uses system-specific section to avoid conflicts between systems sharing
       column names (e.g. PESO means different things in SIM vs SINASC).
    2. **Value translation** — maps coded values (e.g. SEXO ``"1"`` → ``"Male"``)
       using ``dictionaries/pt-{lang}/categories.json`` (applied after rename).
    3. **Date conversion** — converts DATASUS DDMMYYYY strings to DATE type
       for known date columns.

    Registers ``stage="standardize"`` and a timestamped history entry in
    ``sus_meta``, propagating ``system`` and ``type`` from the previous stage.

    Args:
        rel: Lazy DuckDB relation (must be at least at stage ``"import"``).
        lang: Target language — ``"en"`` (default), ``"pt"`` or ``"es"``.
            ``"pt"`` renames to Portuguese snake_case (e.g. ``data_obito``).
            ``"en"`` renames to English (e.g. ``death_date``).
        system: Override system detection (e.g. ``"SIM-DO"``). If ``None``,
            auto-detected from column signatures.

    Returns:
        Lazy DuckDB relation with standardized column names, translated values
        and converted dates, registered at stage ``"standardize"`` in sus_meta.

    Example:
        >>> import climasus4py as cs
        >>> rel    = cs.sus_data_import("SIM-DO", "SE", 2023)
        >>> clean  = cs.sus_data_clean_encoding(rel)
        >>> stand  = cs.sus_data_standardize(clean, lang="en")
        >>> cs.sus_meta(stand, "stage")
        'standardize'
    """
    _assert_lazy(rel)

    # capture original rel before any transformation
    _original_rel = rel

    columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # Which DATASUS system is this? Three sources, in order of trust.
    # ------------------------------------------------------------------
    # 1. What the caller said.
    # 2. What sus_data_import RECORDED. It knows the system exactly — it
    #    was an argument — and writes it to the stage metadata
    #    (importer.py, set_stage(..., system=system)), which
    #    sus_data_clean_encoding carries forward with _inherit_from.
    # 3. Guessing from the column names, which is all that is left for a
    #    relation that never went through sus_data_import — a parquet
    #    opened by hand, say.
    #
    # Until 27/10/2026 only (3) existed, and when it failed the code took
    # the DuckDB ALIAS as the system name. For a relation read from files
    # that alias is `parquet_5957eec4d0d15e67`. _load_column_dict then
    # looked for a section by that name, found none, fell back to COMMON's
    # 15 entries — and said nothing. Measured on 2023 extracts: 0 of 118
    # SINAN columns translated, 0 of 208 CNES, 0 of 60 SIA, where
    # climasus4r translated 116, 89 and 60. The pipeline then ran on raw
    # column names and produced plausible wrong numbers: a SINAN frame
    # dated by the string in DT_NOTIFIC, a SINASC frame aged from
    # IDADEMAE (M162).
    #
    # The alias is gone as a source. A name that was never a system is
    # worse than no name: with None, _load_column_dict merges every
    # section and still translates most of the file, and the warning
    # below tells the caller what to do about the rest.
    if system is None:
        system = get_meta(rel).get("system") if get_meta(rel) else None
    if system is None:
        system = detect_system(columns)
    if system is None:
        warnings.warn(
            "sus_data_standardize: could not tell which DATASUS system "
            "this is — neither recorded by sus_data_import nor "
            "recognisable from the column names. Falling back to the "
            "merged dictionary of every system, which translates most "
            "columns but can pick the wrong code book for a categorical "
            "one. Pass system= (e.g. system='SIA-PA') to remove the "
            "guesswork.",
            UserWarning,
            stacklevel=2,
        )

    # ------------------------------------------------------------------
    # 0. Fixed-width codes back to zero-padded strings
    # ------------------------------------------------------------------
    # Runs before the rename so the raw DATASUS names are still in place —
    # see the note on _CODE_COLUMN_WIDTHS.
    #
    # The importer no longer numericises these (M64, schema_version 2 of
    # datasus_columns.json), so a freshly imported file already arrives as
    # padded text. This step stays for the caches written BEFORE that fix,
    # where the damage is on disk: on SIM-DO SP 2023, 13.294 of 262.909
    # records with CODESTAB (5,1%) are short, 0000057 stored as 57 — a
    # value that matches no establishment in any external registry.
    #
    # NOT plain LPAD: DuckDB's LPAD **truncates** when the value is longer
    # than the width, so LPAD('12345678', 7, '0') returns '1234567'. On a
    # code that is one digit too long — bad source data, or a width wrong
    # in the metadata — that turns a quality problem into lost data, which
    # is the defect this whole step exists to undo. Pad only what is
    # short, and leave anything else exactly as it came.
    for code_col, width in _code_column_widths().items():
        if code_col not in columns:
            continue
        # TRY_CAST via BIGINT normalises the float rendering a numericised
        # cache carries ("57.0" → "57"); a value that is already text and
        # not a number falls back to itself rather than to NULL.
        limpo = (f'COALESCE(CAST(TRY_CAST("{code_col}" AS BIGINT) AS VARCHAR), '
                 f'CAST("{code_col}" AS VARCHAR))')
        expr = (
            f'CASE WHEN "{code_col}" IS NULL THEN NULL '
            f'WHEN LENGTH({limpo}) >= {width} THEN {limpo} '
            f'ELSE LPAD({limpo}, {width}, \'0\') END AS "{code_col}"'
        )
        rel = rel.project(
            ", ".join(expr if c == code_col else f'"{c}"' for c in columns)
        )
        columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # 1. Column rename — system-specific to avoid cross-system conflicts
    # ------------------------------------------------------------------
    col_map = _load_column_dict(lang, system=system)
    renames = {col: col_map[col] for col in columns if col in col_map}

    if renames:
        projections = []
        for col in columns:
            if col in renames:
                projections.append(f'"{col}" AS "{renames[col]}"')
            else:
                projections.append(f'"{col}"')
        conn = get_connection()
        rel = conn.sql(f"SELECT {', '.join(projections)} FROM rel")

    # ------------------------------------------------------------------
    # 2. Date conversion (DDMMYYYY → DATE)
    # ------------------------------------------------------------------
    new_columns = schema_columns(rel)
    date_candidates = [
        # EN
        "death_date", "birth_date", "admission_date", "discharge_date",
        "notification_date", "first_symptom_date", "investigation_date",
        "case_conclusion_date", "registration_date", "receipt_date",
        "reception_date", "investigation_conclusion_date",
        "investigation_registration_date", "certificate_date",
        "digitization_date",
        # PT snake_case
        "data_obito", "data_nascimento", "data_internacao", "data_saida",
        "data_notificacao", "data_primeiro_sintoma", "data_investigacao",
        "data_conclusao_caso", "data_cadastro", "data_recebimento",
        # ES
        "fecha_muerte", "fecha_nacimiento", "fecha_ingreso", "fecha_alta",
        "fecha_notificacion", "fecha_primeros_sintomas",
        # SIA — month-granularity, see _date_parse_expr
        "competence_date", "data_competencia", "fecha_competencia",
        "processing_date", "data_processamento", "fecha_procesamiento",
        # DATASUS original (if not renamed)
        "DTOBITO", "DTNASC", "DT_INTER", "DT_SAIDA",
        "DT_NOTIFIC", "DT_SIN_PRI", "DTINVESTIG", "DTCONCASO",
        "DTCADASTRO", "DTRECEBIM", "PA_CMP", "PA_MVM",
    ]

    for dc in date_candidates:
        if dc in new_columns:
            rel = rel.project(
                ", ".join(
                    f'{_date_parse_expr(dc, _ano_primeiro(system))} AS "{dc}"'
                    if c == dc else f'"{c}"'
                    for c in new_columns
                )
            )
            new_columns = schema_columns(rel)

    # --- 2b. Date columns the explicit list does not name -------------
    # The list above is hand-maintained, so a column it never heard of stays
    # raw text: ``original_receipt_date`` (DTRECORIGA) came out as the string
    # "05012023" for all 334.303 records of SIM-DO SP 2023 while the R
    # returned a date. Ver M4.
    #
    # Reading ``all_date_columns`` from the climasus-data metadata does NOT
    # fix it: that list publishes ``DTRECORIG``, without the trailing A, so it
    # does not match the real DATASUS column. Correcting the metadata is the
    # right long-term move and needs the coordinator.
    #
    # So the fallback is by shape, and it CONVERTS ONLY WHAT ACTUALLY PARSES.
    # The guard is not paranoia: ``batch_number`` (NUMEROLOTE) also holds
    # eight digits — "20230001", a year plus a sequence — and a name/shape
    # heuristic without the guard would have turned a batch number into a
    # date. Measured on the same dataset: 0 of 334.303 batch numbers parse as
    # DDMMYYYY, against 334.303 of 334.303 receipt dates.
    _DATE_NAME_HINTS = ("date", "data_", "fecha_", "dt_")
    for col in list(new_columns):
        if col in date_candidates:
            continue
        nome = col.lower()
        parece_data = (
            any(h in nome for h in _DATE_NAME_HINTS) or nome.startswith("dt")
        )
        if not parece_data:
            continue
        expr = _date_parse_expr(col, _ano_primeiro(system))
        try:
            total, convertidos = rel.aggregate(
                f'COUNT("{col}") AS n, COUNT({expr}) AS ok'
            ).fetchone()
        except duckdb.Error:
            continue  # tipo em que a expressao nao se aplica: deixar como esta
        if not total or convertidos != total:
            # Uma unica linha que nao parseia e sinal de que a coluna nao e
            # data. Exigir 100% em vez de uma maioria: converter "quase tudo"
            # transformaria as sobras em NULL silencioso, que e o defeito do
            # M59 por outro caminho.
            continue
        rel = rel.project(
            ", ".join(
                f'{expr} AS "{col}"' if c == col else f'"{c}"' for c in new_columns
            )
        )
        new_columns = schema_columns(rel)

    # ------------------------------------------------------------------
    # 3. Value translation (categorical labels)
    # Applied AFTER rename — keys in categories.json are translated names.
    # The system goes in for the same reason it goes into the rename: two
    # systems feed the same translated column with different code books,
    # and without it SINAN data came out on the SIH education scale (M104).
    # ------------------------------------------------------------------
    cat_map = _load_category_dict(lang, system=system)

    if cat_map:
        new_columns = schema_columns(rel)
        cat_cols = [c for c in new_columns if c in cat_map]

        if cat_cols:
            cat_projections = []
            for col in new_columns:
                if col in cat_map:
                    cases = " ".join([
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = '{k}' THEN '{v}' "
                        f"WHEN CAST(TRY_CAST(\"{col}\" AS INTEGER) AS VARCHAR) = '{k}' THEN '{v}'"
                        for k, v in cat_map[col].items()
                    ])
                    cat_projections.append(
                        f"CASE "
                        f"WHEN \"{col}\" IS NULL THEN NULL "
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = 'None' THEN NULL "
                        f"WHEN CAST(\"{col}\" AS VARCHAR) = 'nan' THEN NULL "
                        f"{cases} "
                        f"ELSE CAST(\"{col}\" AS VARCHAR) "
                        f"END AS \"{col}\""
                    )
                else:
                    cat_projections.append(f'"{col}"')

            conn = get_connection()
            rel = conn.sql(
                f"SELECT {', '.join(cat_projections)} FROM rel"
            )

    # ------------------------------------------------------------------
    # 4. Register stage and history — mirrors climasus4r
    # set_alias before set_stage to avoid losing meta on new DuckDB object
    # ------------------------------------------------------------------
    n_renamed  = len(renames)
    n_cat_cols = len([c for c in schema_columns(rel) if c in cat_map]) if cat_map else 0
    history_msg = (
        f"Standardized column names and types to {lang.upper()}; "
        f"{n_renamed} columns renamed; "
        f"{n_cat_cols} categorical columns translated"
    )

    rel = rel.set_alias(system or "unknown")
    # Record the system, resolved above from the argument or from
    # detect_system(). Only sus_data_import used to write it, so a chain that
    # started from a raw parquet lost it even when the caller had said which
    # system it was — and then sus_data_aggregate had nothing to read and
    # picked the geographic column by default order (M21). set_stage keeps the
    # inherited value when this is None, so nothing is overwritten with a
    # blank.
    rel = set_stage(rel, "standardize", system=system, _inherit_from=_original_rel)
    rel = add_history(rel, history_msg)

    return rel
