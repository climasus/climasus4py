"""Data loading and column detection utilities.

Mirrors R: utils-data.R — JSON loading, column/system detection, UF resolution.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any

import climasus_data

# ---------------------------------------------------------------------------
# Data directory resolution
# ---------------------------------------------------------------------------

_DATA_DIR: Path | None = None


def _find_data_dir() -> Path:
    """Locate the climasus-data directory.

    Priority:
    1. CLIMASUS_DATA_DIR environment variable (explicit override)
    2. climasus_data package (installed dependency — preferred)
    """
    global _DATA_DIR
    if _DATA_DIR is not None:
        return _DATA_DIR

    # Honour environment variable first (explicit override)
    env = os.environ.get("CLIMASUS_DATA_DIR")
    if env:
        p = Path(env)
        if (p / "manifest.json").is_file():
            _DATA_DIR = p
            return p

    # Use installed climasus_data package
    _DATA_DIR = climasus_data.data_root()
    return _DATA_DIR


def data_path(relative: str) -> Path:
    """Return the absolute path to a file inside the climasus-data directory.

    Args:
        relative: Path relative to the climasus-data root, e.g.
            ``"metadata/datasus_systems.json"``.

    Returns:
        Absolute ``pathlib.Path`` to the requested file.

    Example:
        >>> data_path("metadata/datasus_systems.json").exists()
        True
    """
    return _find_data_dir() / relative


# ---------------------------------------------------------------------------
# JSON loading (cached)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=32)
def load_json(relative: str) -> dict[str, Any]:
    """Load and cache a JSON file from the climasus-data directory.

    Results are cached with ``lru_cache`` — the file is read only once
    per process regardless of how many times this function is called
    with the same *relative* path.

    Args:
        relative: Path relative to the climasus-data root, e.g.
            ``"disease_groups/core.json"``.

    Returns:
        Parsed JSON object (``dict`` or ``list``).

    Raises:
        FileNotFoundError: If the file does not exist in climasus-data.

    Example:
        >>> data = load_json("metadata/datasus_systems.json")
        >>> list(data.keys())
        ['systems']
    """
    path = data_path(relative)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado em climasus-data: {relative}\n"
                                "Certifique-se de que o diretório clonado está presente e atualizado.")  # noqa: E501
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Compatibility loaders for optional metadata files
# ---------------------------------------------------------------------------

_FALLBACK_DATASUS_COLUMNS: dict[str, Any] = {
    "all_date_columns": [
        "DTOBITO", "DTNASC", "DTCADINF", "DTCADMUN", "DTCONCASO",
        "DTINVESTIG", "DTRECEBIM", "DTRECORIG", "DTCONINV", "DTINTERNACAO",
        "DTSAIDA", "DTCADASTRO", "DTATESTADO", "DTREGCART", "DTCASAM",
        "DTULTMENST", "DTCONSULT", "DTDECLARAC",
    ],
    # Split in three (M64). This used to be the same 23-entry list the
    # metadata published, and it carried the same defect: CODESTAB came
    # out as a number and 5.1% of the rows lost their leading zeros. The
    # fallback has to agree with schema_version 2 of
    # metadata/datasus_columns.json, or an install without climasus-data
    # would quietly go back to corrupting identifiers. GRAESSION is gone
    # from here too — it matches no column in any DATASUS file available.
    "all_numeric_columns": [
        "CONTADOR", "PESO", "QTDFILVIVO", "QTDFILMORT", "SEMAGESTAC",
        "IDADEMAE", "QTDGESTANT", "QTDPARTNOR", "QTDPARTCES", "IDADEPAI",
    ],
    "all_identifier_columns": {
        "CODESTAB": 7,
        "CODMUNRES": 6,
        "CODMUNOCOR": 6,
        "CODMUNNATU": 6,
        # Seis porque o codigo de ocupacao CBO-2002 tem seis digitos: nao
        # aparece em nenhum SIM-DO disponivel aqui, entao a largura vem do
        # sistema de codigos e nao de medicao -- como o paliativo do M5 ja
        # declarava.
        "CODOCUPMAE": 6,
    },
    "all_categorical_columns": [
        "GESTACAO", "OBITOGRAV", "LOCOCOR", "ESCMAE", "ESCPAI",
        "SERIESCMAE", "SERIESCPAI",
        "CODIFICADO", "ESC", "ESC2010", "ESCFALAGR1", "ESCMAE2010",
        "ESCMAEAGR1",
    ],
    "system_signatures": {
        "SIM-DO":      {"any_of": ["CAUSABAS", "DTOBITO"]},
        "SIH-RD":      {"any_of": ["DIAG_PRINC"]},
        "SINAN-DENGUE":{"any_of": ["NU_NOTIFIC"]},
        "SINASC":      {"any_of": ["NUMERODN"]},
    },
    "role_priority": {
        "date":  ["death_date", "date", "DTOBITO", "DTNASC", "admission_date",
                  "DT_NOTIFIC", "DT_INTER"],
        "cause": ["underlying_cause", "cause", "CAUSABAS", "DIAG_PRINC"],
        "age":   ["age", "age_years", "age_code", "IDADE", "IDADEMAE"],
        "sex":   ["sex", "SEXO", "CS_SEXO"],
        # Municipality and state were MISSING from this fallback while
        # detect_geo_column kept its own hardcoded dict, so nothing
        # noticed. Now that the detector reads the metadata (D6/M96), an
        # install without climasus-data would raise KeyError without
        # these. Same union and same order as the published file.
        "municipality": [
            "CODMUNRES", "residence_municipality_code",
            "codigo_municipio_residencia", "MUNI_RES",
            "CODMUNOCOR", "occurrence_municipality_code",
            "codigo_municipio_ocurrencia",
            "ID_MUNICIP", "notification_municipality_code",
            "municipality_code", "code_muni",
        ],
        "state": ["state", "SG_UF", "UF", "SG_UF_NOT"],
    },
    "municipality_by_basis": {
        "_fallback_order": ["residence", "occurrence", "notification",
                            "unspecified"],
        "residence": ["CODMUNRES", "residence_municipality_code",
                      "codigo_municipio_residencia", "MUNI_RES"],
        "occurrence": ["CODMUNOCOR", "occurrence_municipality_code",
                       "codigo_municipio_ocurrencia"],
        "notification": ["ID_MUNICIP", "notification_municipality_code"],
        "unspecified": ["municipality_code", "code_muni"],
    },
}


@lru_cache(maxsize=1)
def load_datasus_columns_spec() -> dict[str, Any]:
    """Load DATASUS column specs, with fallback for older climasus-data.

    A release before ``schema_version`` 2 publishes the single 23-entry
    ``all_numeric_columns`` and none of the three split lists. Returning
    it as it comes would put the M64 corruption straight back — the
    importer would coerce ``CODESTAB`` to a number again and 5.1% of the
    rows would lose their leading zeros — so the three lists are taken
    from the fallback instead, and the user is told why. Everything else
    in the file (dates, signatures, role priority) is honoured as
    published.
    """
    try:
        data = load_json("metadata/datasus_columns.json")
    except FileNotFoundError:
        return _FALLBACK_DATASUS_COLUMNS.copy()

    if not isinstance(data, dict):
        return _FALLBACK_DATASUS_COLUMNS.copy()

    versao = int(data.get("schema_version") or 1)
    if versao >= 3:
        return data

    import warnings

    corrigido = dict(data)
    faltando: list[str] = []

    if versao < 2:
        faltando.append(
            "the single all_numeric_columns list of 23 entries, which mixes "
            "quantities with fixed-width identifier codes and loses the "
            "leading zero of CODESTAB when coerced (M64)")
        for chave in ("all_numeric_columns", "all_identifier_columns",
                      "all_categorical_columns"):
            corrigido[chave] = _FALLBACK_DATASUS_COLUMNS[chave]

    if versao < 3:
        faltando.append(
            "no municipality_by_basis, and a role_priority.municipality of "
            "3 names against the 11 the detectors need — residence and "
            "occurrence could not be told apart (M96)")
        corrigido["municipality_by_basis"] = (
            _FALLBACK_DATASUS_COLUMNS["municipality_by_basis"])
        papeis = dict(corrigido.get("role_priority") or {})
        for chave in ("municipality", "state", "date"):
            papeis[chave] = _FALLBACK_DATASUS_COLUMNS["role_priority"][chave]
        corrigido["role_priority"] = papeis

    warnings.warn(
        "climasus-data publishes metadata/datasus_columns.json at "
        f"schema_version {data.get('schema_version')!r}, which has "
        + "; and ".join(faltando)
        + ". Using the corrected lists bundled with climasus4py instead; "
        "run update_climasus_data() to get the published ones.",
        UserWarning,
        stacklevel=2,
    )
    return corrigido


# ---------------------------------------------------------------------------
# Update function: baixa/atualiza climasus-data localmente
# ---------------------------------------------------------------------------

def update_climasus_data(
    repo_url: str = "https://github.com/climasus/climasus-data.git",
    target_dir: str | None = None,
    branch: str = "main",
) -> None:
    """Baixa ou atualiza o repositório climasus-data localmente."""
    if target_dir is None:
        env = os.environ.get("CLIMASUS_DATA_DIR")
        if env:
            target_dir = env
        else:
            try:
                target_dir = str(climasus_data.data_root())
            except FileNotFoundError:
                target_dir = str(Path(__file__).resolve().parent.parent.parent / "climasus-data")

    target = Path(target_dir)
    if target.exists() and (target / ".git").is_dir():
        print(f"Atualizando climasus-data em {target}...")
        subprocess.run(["git", "-C", str(target), "pull", "origin", branch], check=True)
    elif target.exists():
        if not (target / "manifest.json").is_file():
            raise RuntimeError(
                f"Diretório {target} não parece ser climasus-data "
                f"(manifest.json não encontrado). Remoção abortada por segurança."
            )
        print(f"Removendo diretório existente e clonando climasus-data em {target}...")
        shutil.rmtree(target)
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)  # noqa: E501
    else:
        print(f"Clonando climasus-data em {target}...")
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)  # noqa: E501
    print("climasus-data atualizado com sucesso.")


def load_systems() -> dict:
    """Load SUS system definitions from climasus-data."""
    return load_json("metadata/datasus_systems.json")["systems"]


def load_uf_codes() -> dict:
    """Load Brazilian state (UF) codes from climasus-data."""
    return load_json("metadata/uf_codes.json")["states"]


def load_regions() -> dict:
    """Load Brazilian region definitions from climasus-data."""
    return load_json("metadata/regions.json")["categories"]


# ---------------------------------------------------------------------------
# UF resolution  (mirrors .resolve_uf)
# ---------------------------------------------------------------------------

def resolve_uf(uf: str | list[str]) -> list[str]:
    """Resolve a UF specification to a list of 2-letter state codes.

    Accepts:
    - A single UF string: ``"SP"``
    - A list of UFs: ``["SP", "RJ"]``
    - ``"all"`` — all 27 states
    - A region name in PT, EN or ES, e.g. ``"nordeste"``, ``"northeast"``,
      ``"amazonia_legal"``, ``"semi_arid"``

    Args:
        uf: UF abbreviation(s), ``"all"``, or a region name.

    Returns:
        List of upper-case 2-letter UF abbreviations.

    Examples:
        >>> resolve_uf("SP")
        ['SP']
        >>> resolve_uf("nordeste")
        ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
        >>> resolve_uf("northeast")   # EN alias
        ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
        >>> resolve_uf("noreste")     # ES alias
        ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE']
        >>> len(resolve_uf("all"))
        27
    """
    if isinstance(uf, str):
        uf_list = [uf]
    else:
        uf_list = list(uf)

    if len(uf_list) == 1:
        token = uf_list[0]

        # "all" → todos os 27 estados
        if token.lower() == "all":
            return list(load_uf_codes().keys())

        # verifica se é nome de região ou alias (PT/EN/ES)
        regions = load_regions()
        token_lower = token.lower()
        for category in regions.values():
            for region_name, region_data in category.get("regions", {}).items():
                # checa nome canônico
                if token_lower == region_name.lower():
                    return region_data["states"]
                # checa aliases (PT, EN, ES)
                for alias in region_data.get("aliases", []):
                    if token_lower == alias.lower():
                        return region_data["states"]

    return [u.upper() for u in uf_list]


# ---------------------------------------------------------------------------
# System / column detection  (mirrors .detect_*)
# ---------------------------------------------------------------------------

def _load_datasus_columns_json() -> dict:
    return load_datasus_columns_spec()


def detect_system(columns: list[str]) -> str | None:
    """Detect the SUS system from a list of column names."""
    col_set = set(columns)
    signatures = _load_datasus_columns_json()["system_signatures"]
    for system, spec in signatures.items():
        if col_set & set(spec["any_of"]):
            return system
    return None


def _detect_column(columns: list[str], candidates: list[str]) -> str | None:
    """Return first matching column from ordered candidates."""
    col_set = set(columns)
    for c in candidates:
        if c in col_set:
            return c
    return None


def detect_date_column(columns: list[str]) -> str | None:
    """Return the first recognised date column from a list of column names."""
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["date"])


def detect_cause_column(columns: list[str]) -> str | None:
    """Return the first recognised ICD-10 cause column from a list of column names."""
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["cause"])


def detect_age_column(columns: list[str]) -> str | None:
    """Return the first recognised age column from a list of column names."""
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["age"])


def detect_sex_column(columns: list[str]) -> str | None:
    """Return the first recognised sex column from a list of column names."""
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["sex"])


def period_bound(value: str, *, end: bool = False):
    """Parse a date string, expanding a partial one to its period boundary.

    ``pd.Timestamp`` reads a partial date as the *first instant* of the
    period: ``"2023"`` and ``"2023-06"`` both become a single day. So a
    caller writing ``baseline_start="2023", baseline_end="2023"``, meaning
    the year 2023, silently got a **one-day** window — enough data to pass
    an "is it empty?" check and nowhere near enough to compute a
    percentile. On INMET SP 2023 that produced 135 heatwave events against
    245 for the full year, a 45% difference with nothing to show it.

    Here a partial date resolves to the start or the end of the period it
    names, whichever the caller is asking for, so ``("2023", "2023")``
    means the whole year. A complete date is a one-day period and
    resolves to itself, unchanged.

    Args:
        value: Date string — ``"2023"``, ``"2023-06"``, ``"2023-06-15"``.
        end: When ``True``, return the last day of the period rather than
            the first.

    Returns:
        ``pandas.Timestamp`` at day resolution.

    Example:
        >>> period_bound("2023")
        Timestamp('2023-01-01 00:00:00')
        >>> period_bound("2023", end=True)
        Timestamp('2023-12-31 00:00:00')
        >>> period_bound("2023-06-15", end=True)
        Timestamp('2023-06-15 00:00:00')
    """
    import pandas as pd

    try:
        period = pd.Period(value)
    except (ValueError, TypeError):
        # Formats Period rejects but Timestamp accepts (e.g. one carrying a
        # time). Nothing to expand in those — they name an instant.
        return pd.Timestamp(value).normalize()
    return (period.end_time if end else period.start_time).normalize()


def decode_age_sql(age_col: str) -> str:
    """Return a DuckDB SQL expression that decodes SIM-DO coded age to years.

    The DATASUS encoding is three digits: the first is the unit (0 minutes,
    1 hours, 2 days, 3 months — all under a year; 4 years; 5 years past
    100) and the rest is the amount.

    A three-digit code whose unit digit is not one of those is not an age.
    ``999`` is the sentinel for *unknown*, and it used to fall through to
    ``TRY_CAST`` and come back as **999 years** — then the derived columns
    happily filed those records under ``age_group='60+'``,
    ``ibge_age_group='80+'`` and ``climate_risk_group='High Risk (65+)'``,
    inflating the elderly bands with people of unknown age. On SIM-DO SP
    2023 that was 335 records. The R returns NA.

    Note this is a *structural* rule rather than a lookup in
    :data:`_IGNORED_VALUES`, which does list ``"999"``. That list also
    holds ``"0"``, ``"9"`` and ``"99"``, which are sentinels for
    categorical fields such as sex and race — applying it wholesale here
    would discard a legitimately coded age. Rejecting an out-of-range unit
    digit covers ``999`` and any other undecodable code without guessing
    at short numeric values.
    """
    v = f'TRIM(CAST("{age_col}" AS VARCHAR))'
    return (
        f"CASE"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) NOT IN"
        f"       ('0', '1', '2', '3', '4', '5')"
        f"    THEN NULL"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) = '5'"
        f"    THEN 100 + TRY_CAST(SUBSTR({v}, 2) AS INTEGER)"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) = '4'"
        f"    THEN TRY_CAST(SUBSTR({v}, 2) AS INTEGER)"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) IN ('0', '1', '2', '3')"
        f"    THEN 0"
        f"  ELSE TRY_CAST({v} AS INTEGER)"
        f" END"
    )


#: Levels the shared metadata does not describe, so they stay here.
_GEO_LOCAL_LEVELS: dict[str, list[str]] = {
    "region": ["region"],
    "country": ["country"],
}


def municipality_candidates(basis: str | None = None) -> list[str]:
    """Municipality column names, in order, for one geographic basis.

    Reads ``municipality_by_basis`` from the shared metadata (D6/M96).
    ``basis`` of ``None`` returns the declared fallback union, which is
    what ``role_priority.municipality`` holds.

    Args:
        basis: ``"residence"``, ``"occurrence"``, ``"notification"``,
            ``"unspecified"``, or ``None`` for the full fallback order.

    Returns:
        Ordered candidate names.

    Raises:
        ValueError: If *basis* is not a declared one.
    """
    spec = _load_datasus_columns_json()
    if basis is None:
        return list(spec["role_priority"]["municipality"])
    porbase = spec.get("municipality_by_basis") or {}
    if basis not in porbase or basis.startswith("_"):
        validos = [k for k in porbase if not k.startswith("_")]
        raise ValueError(
            f"Unknown geographic basis {basis!r}. Declared: "
            f"{sorted(validos)}."
        )
    return list(porbase[basis])


def detect_geo_column(
    columns: list[str],
    level: str = "municipality",
    basis: str | None = None,
) -> str | None:
    """Return the first recognised geographic column for the requested level.

    Municipality names come from the **shared metadata**, which is the
    single source since 21/09/2026 (D6). There used to be five lists
    disagreeing on both membership and order: this function's own dict,
    ``role_priority.municipality``, ``climate_aggregate``'s
    ``_MUNI_CANDIDATES``, and one in each plotting module. Order is what
    matters, because the first match wins.

    That divergence is how M96 happened: ``climate_aggregate`` validated
    against its own list, which *did* include ``code_muni``, so a
    relation keyed on it passed validation and then died deep in the join
    with ``Binder Error: ... does not have a column named "None"`` — the
    detector had returned ``None`` and it went straight into the SQL.
    With one list, validation and detection cannot disagree again.

    Args:
        columns: Column names to search.
        level: ``"municipality"``, ``"state"``, ``"region"`` or
            ``"country"``.
        basis: For ``level="municipality"``, which geographic cut to
            require: ``"residence"``, ``"occurrence"``,
            ``"notification"`` or ``"unspecified"``. ``None`` (default)
            walks the declared fallback order, residence first.

            Worth asking for explicitly when it matters: residence and
            occurrence are **different epidemiological cuts** and the
            choice changes the result (M21). With ``None`` the order
            decides in silence, which is the behaviour that was there
            before and is kept so nothing breaks — but a caller that
            cares should say which one it means.

    Returns:
        The first matching column name, or ``None``.

    Raises:
        ValueError: If *basis* is given with a level other than
            municipality, or is not a declared basis.
    """
    if level == "municipality":
        return _detect_column(columns, municipality_candidates(basis))
    if basis is not None:
        raise ValueError(
            f"basis={basis!r} only applies to level='municipality', not "
            f"{level!r}."
        )
    if level == "state":
        return _detect_column(
            columns, _load_datasus_columns_json()["role_priority"]["state"])
    return _detect_column(columns, _GEO_LOCAL_LEVELS.get(level, []))


def system_family(system: str) -> str:
    """Extract the family prefix from a SUS system identifier."""
    return system.split("-")[0]


# ---------------------------------------------------------------------------
# Sub-plano D helpers (parity with climasus4r legacy)
# ---------------------------------------------------------------------------

_IGNORED_VALUES: tuple[str, ...] = (
    "9", "99", "999", "0", "",
    "Ignorado", "ignorado",
    "Unknown", "unknown",
    "Desconocido", "desconocido",
    "Desconhecido", "desconhecido",
    "NaN", "nan", "null", "NULL",
)

_IGNORABLE_DEMO_COLUMNS: tuple[str, ...] = (
    "sex", "SEXO", "CS_SEXO",
    "race", "RACACOR",
    "education", "education_2010", "ESC", "ESC2010",
    "age", "age_code", "IDADE",
)


def detect_education_column(columns: list[str]) -> str | None:
    """Return the first recognised education column from *columns*."""
    return _detect_column(columns, ["education", "education_2010", "ESC", "ESC2010"])


def expand_city_to_codes(city: str | list[str]) -> list[str]:
    """Resolve city name(s) to IBGE 6-digit municipality codes."""
    import unicodedata
    import warnings

    import pandas as pd

    parquet_path = data_path("spatial/municipalities.parquet")
    if not parquet_path.is_file():
        raise FileNotFoundError(
            "spatial/municipalities.parquet not found in climasus-data. "
            "Run cs.update_climasus_data() to refresh."
        )

    df = pd.read_parquet(parquet_path)

    name_col = next(
        (c for c in df.columns if c.lower() in ("municipality_name", "name", "nome", "municipio")),
        None,
    )
    _code_aliases = ("municipality_code", "code", "codigo", "cod_mun", "codmun")
    code_col = next(
        (c for c in df.columns if c.lower() in _code_aliases),
        None,
    )
    if name_col is None or code_col is None:
        raise ValueError(
            f"municipalities.parquet must have name and code columns. "
            f"Found: {list(df.columns)}"
        )

    def _norm(s: str) -> str:
        decomposed = unicodedata.normalize("NFKD", s)
        return "".join(
            ch for ch in decomposed if not unicodedata.combining(ch)
        ).strip().lower()

    city_list = [city] if isinstance(city, str) else list(city)
    all_codes: list[str] = []

    for name in city_list:
        _n = _norm(name)
        mask = df[name_col].apply(lambda x, __n=_n: _norm(str(x)) == __n)
        matches = df.loc[mask, code_col].astype(str).tolist()
        if not matches:
            raise ValueError(
                f"City {name!r} not found in municipalities.parquet. "
                "Check spelling or use municipality_code directly."
            )
        if len(matches) > 1:
            warnings.warn(
                f"City {name!r} matches {len(matches)} municipalities "
                f"(e.g. {matches[:3]}). All codes will be used for filtering.",
                UserWarning,
                stacklevel=3,
            )
        all_codes.extend(matches)

    return list(dict.fromkeys(all_codes))