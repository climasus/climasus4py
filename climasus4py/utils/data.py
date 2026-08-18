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
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Compatibility loaders for optional metadata files
# ---------------------------------------------------------------------------

_FALLBACK_DATASUS_COLUMNS: dict[str, Any] = {
    "all_date_columns": [
        "DTOBITO",
        "DTNASC",
        "DTCADINF",
        "DTCADMUN",
        "DTCONCASO",
        "DTINVESTIG",
        "DTRECEBIM",
        "DTRECORIG",
        "DTCONINV",
        "DTINTERNACAO",
        "DTSAIDA",
        "DTCADASTRO",
        "DTATESTADO",
        "DTREGCART",
        "DTCASAM",
        "DTULTMENST",
        "DTCONSULT",
        "DTDECLARAC",
    ],
    "all_numeric_columns": [
        "CONTADOR",
        "PESO",
        "QTDFILVIVO",
        "QTDFILMORT",
        "GESTACAO",
        "SEMAGESTAC",
        "OBITOGRAV",
        "GRAESSION",
        "CODMUNNATU",
        "CODMUNRES",
        "CODMUNOCOR",
        "CODESTAB",
        "LOCOCOR",
        "IDADEMAE",
        "ESCMAE",
        "CODOCUPMAE",
        "QTDGESTANT",
        "QTDPARTNOR",
        "QTDPARTCES",
        "IDADEPAI",
        "ESCPAI",
        "SERIESCPAI",
        "SERIESCMAE",
    ],
    "system_signatures": {
        "SIM-DO": {"any_of": ["CAUSABAS", "DTOBITO"]},
        "SIH-RD": {"any_of": ["DIAG_PRINC"]},
        "SINAN-DENGUE": {"any_of": ["NU_NOTIFIC"]},
        "SINASC": {"any_of": ["NUMERODN"]},
    },
    "role_priority": {
        "date": ["death_date", "date", "DTOBITO", "DTNASC", "admission_date"],
        "cause": ["underlying_cause", "cause", "CAUSABAS", "DIAG_PRINC"],
        "age": ["age", "age_years", "age_code", "IDADE", "IDADEMAE"],
        "sex": ["sex", "SEXO", "CS_SEXO"],
    },
}


@lru_cache(maxsize=1)
def load_datasus_columns_spec() -> dict[str, Any]:
    """Load DATASUS column specs, with fallback for older climasus-data releases."""
    try:
        data = load_json("metadata/datasus_columns.json")
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        pass
    return _FALLBACK_DATASUS_COLUMNS.copy()


# ---------------------------------------------------------------------------
# Update function: baixa/atualiza climasus-data localmente
# ---------------------------------------------------------------------------

def update_climasus_data(repo_url: str = "https://github.com/climasus/climasus-data.git", target_dir: str | None = None, branch: str = "main") -> None:  # noqa: E501
    """Baixa ou atualiza o repositório climasus-data localmente.

    Normalmente não é necessário quando ``climasus-data`` está instalado
    como pacote Python. Útil durante desenvolvimento ou para forçar
    atualização dos arquivos de referência offline.

    Args:
        repo_url: URL do repositório Git a clonar.
        target_dir: Diretório de destino. Se ``None``, usa a variável de
            ambiente ``CLIMASUS_DATA_DIR`` ou o diretório do pacote
            instalado.
        branch: Branch Git a clonar/atualizar. Padrão: ``"main"``.

    Raises:
        RuntimeError: Se *target_dir* existe mas não contém
            ``manifest.json`` (proteção contra remoção acidental).
        subprocess.CalledProcessError: Se o comando ``git`` falhar.

    Example:
        >>> import climasus4py as cs
        >>> cs.update_climasus_data()
        Clonando climasus-data em ...
        climasus-data atualizado com sucesso.
    """
    if target_dir is None:
        env = os.environ.get("CLIMASUS_DATA_DIR")
        if env:
            target_dir = env
        else:
            # Tenta usar o diretório do pacote instalado
            try:
                target_dir = str(climasus_data.data_root())
            except FileNotFoundError:
                target_dir = str(Path(__file__).resolve().parent.parent.parent / "climasus-data")

    target = Path(target_dir)
    if target.exists() and (target / ".git").is_dir():
        # Já existe: git pull
        print(f"Atualizando climasus-data em {target}...")
        subprocess.run(["git", "-C", str(target), "pull", "origin", branch], check=True)
    elif target.exists():
        # Existe mas não é git: remove e clona
        # Safety check: only remove if it looks like a climasus-data directory
        if not (target / "manifest.json").is_file():
            raise RuntimeError(
                f"Diretório {target} não parece ser climasus-data "
                f"(manifest.json não encontrado). Remoção abortada por segurança."
            )
        print(f"Removendo diretório existente e clonando climasus-data em {target}...")
        shutil.rmtree(target)
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)  # noqa: E501
    else:
        # Não existe: clona
        print(f"Clonando climasus-data em {target}...")
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)  # noqa: E501
    print("climasus-data atualizado com sucesso.")


def load_systems() -> dict:
    """Load SUS system definitions from climasus-data.

    Returns:
        Dict mapping system identifiers to their metadata.

    Example:
        >>> systems = load_systems()
        >>> "SIM-DO" in systems
        True
    """
    return load_json("metadata/datasus_systems.json")["systems"]


def load_uf_codes() -> dict:
    """Load Brazilian state (UF) codes from climasus-data.

    Returns:
        Dict mapping 2-letter UF abbreviations to state metadata.

    Example:
        >>> ufs = load_uf_codes()
        >>> "name" in ufs["SP"]
        True
    """
    return load_json("metadata/uf_codes.json")["states"]


def load_regions() -> dict:
    """Load Brazilian region definitions from climasus-data.

    Returns:
        Dict mapping region categories to their region metadata and
        member state lists.

    Example:
        >>> regions = load_regions()
        >>> list(regions.keys())
    """
    return load_json("metadata/regions.json")["categories"]


# ---------------------------------------------------------------------------
# UF resolution  (mirrors .resolve_uf)
# ---------------------------------------------------------------------------

def resolve_uf(uf: str | list[str]) -> list[str]:
    """Resolve a UF specification to a list of 2-letter state codes.

    Args:
        uf: A single UF string (e.g. ``"SP"``), a list of UFs
            (e.g. ``["SP", "RJ"]``), the special token ``"all"`` to
            expand to all 27 states, or a region name
            (e.g. ``"Sudeste"``) to expand to its member states.

    Returns:
        List of upper-case 2-letter UF abbreviations.

    Example:
        >>> resolve_uf("SP")
        ['SP']
        >>> resolve_uf("Sudeste")
        ['ES', 'MG', 'RJ', 'SP']
        >>> len(resolve_uf("all"))
        27
    """
    if isinstance(uf, str):
        uf_list = [uf]
    else:
        uf_list = list(uf)

    if len(uf_list) == 1:
        token = uf_list[0]
        if token.lower() == "all":
            return list(load_uf_codes().keys())

        # Check if it's a region name
        regions = load_regions()
        for category in regions.values():
            for region_name, region_data in category.get("regions", {}).items():
                if token == region_name:
                    return region_data["states"]

    return [u.upper() for u in uf_list]


# ---------------------------------------------------------------------------
# System / column detection  (mirrors .detect_*)
# ---------------------------------------------------------------------------

def _load_datasus_columns_json() -> dict:
    return load_datasus_columns_spec()


def detect_system(columns: list[str]) -> str | None:
    """Detect the SUS system from a list of column names.

    Uses characteristic columns as signatures: e.g. ``CAUSABAS`` and
    ``DTOBITO`` identify SIM-DO; ``NUMERODN`` identifies SINASC.

    Args:
        columns: Column names present in the dataset.

    Returns:
        System identifier string (e.g. ``"SIM-DO"``), or ``None`` when
        no known signature is found.

    Example:
        >>> detect_system(["CAUSABAS", "DTOBITO", "IDADE"])
        'SIM-DO'
        >>> detect_system(["UNKNOWN_COL"]) is None
        True
    """
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
    """Return the first recognised date column from a list of column names.

    Searches *columns* for known DATASUS and standardised date column
    names in priority order: ``death_date``, ``date``, ``DTOBITO``,
    ``DTNASC``, ``admission_date``.

    Args:
        columns: Column names present in the dataset.

    Returns:
        Matching column name, or ``None`` if no date candidate is found.

    Example:
        >>> detect_date_column(["DTOBITO", "CAUSABAS", "IDADE"])
        'DTOBITO'
        >>> detect_date_column(["UNKNOWN"]) is None
        True
    """
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["date"])


def detect_cause_column(columns: list[str]) -> str | None:
    """Return the first recognised ICD-10 cause column from a list of column names.

    Searches *columns* for known DATASUS and standardised cause column
    names in priority order: ``underlying_cause``, ``cause``,
    ``CAUSABAS``, ``DIAG_PRINC``.

    Args:
        columns: Column names present in the dataset.

    Returns:
        Matching column name, or ``None`` if no cause candidate is found.

    Example:
        >>> detect_cause_column(["CAUSABAS", "DTOBITO"])
        'CAUSABAS'
        >>> detect_cause_column(["OTHER"]) is None
        True
    """
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["cause"])


def detect_age_column(columns: list[str]) -> str | None:
    """Return the first recognised age column from a list of column names.

    Searches *columns* for known DATASUS and standardised age column
    names in priority order: ``age``, ``age_years``, ``age_code``,
    ``IDADE``, ``IDADEMAE``.

    Args:
        columns: Column names present in the dataset.

    Returns:
        Matching column name, or ``None`` if no age candidate is found.

    Example:
        >>> detect_age_column(["IDADE", "SEXO"])
        'IDADE'
        >>> detect_age_column(["UNKNOWN"]) is None
        True
    """
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["age"])


def detect_sex_column(columns: list[str]) -> str | None:
    """Return the first recognised sex column from a list of column names.

    Searches *columns* for known DATASUS and standardised sex column
    names in priority order: ``sex``, ``SEXO``, ``CS_SEXO``.

    Args:
        columns: Column names present in the dataset.

    Returns:
        Matching column name, or ``None`` if no sex candidate is found.

    Example:
        >>> detect_sex_column(["SEXO", "IDADE"])
        'SEXO'
        >>> detect_sex_column(["UNKNOWN"]) is None
        True
    """
    return _detect_column(columns, _load_datasus_columns_json()["role_priority"]["sex"])


def decode_age_sql(age_col: str) -> str:
    """Return a DuckDB SQL expression that decodes SIM-DO coded age to years.

    DATASUS SIM-DO encodes age as a 3-digit string where the first
    digit is a unit code:

    - ``5xx`` — 100 + xx years (centenarians, e.g. 501 = 101 years)
    - ``4xx`` — xx years       (e.g. 435 = 35 years)
    - ``3xx`` — months         (decoded to 0 years)
    - ``2xx`` — days           (decoded to 0 years)
    - ``1xx`` — hours          (decoded to 0 years)
    - ``0xx`` — minutes        (decoded to 0 years)

    Args:
        age_col: Column name containing the raw DATASUS age code.

    Returns:
        SQL ``CASE`` expression string that evaluates to an integer
        representing age in years.

    Example:
        >>> expr = decode_age_sql("IDADE")
        >>> conn.sql(f"SELECT ({expr}) AS age_years FROM rel").df()
    """
    v = f'TRIM(CAST("{age_col}" AS VARCHAR))'
    return (
        f"CASE"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) = '5'"
        f"    THEN 100 + TRY_CAST(SUBSTR({v}, 2) AS INTEGER)"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) = '4'"
        f"    THEN TRY_CAST(SUBSTR({v}, 2) AS INTEGER)"
        f"  WHEN LENGTH({v}) = 3 AND SUBSTR({v}, 1, 1) IN ('0', '1', '2', '3')"
        f"    THEN 0"
        f"  ELSE TRY_CAST({v} AS INTEGER)"
        f" END"
    )


def detect_geo_column(columns: list[str], level: str = "municipality") -> str | None:
    """Return the first recognised geographic column for the requested level.

    Args:
        columns: Column names present in the dataset.
        level: Geographic level to detect — ``"municipality"``
            (default), ``"state"``, ``"region"``, or ``"country"``.

    Returns:
        Matching column name, or ``None`` if no candidate is found.

    Example:
        >>> detect_geo_column(["CODMUNRES", "DTOBITO"])
        'CODMUNRES'
        >>> detect_geo_column(["UF", "DTOBITO"], level="state")
        'UF'
    """
    candidates = {
        "municipality": ["municipality_code", "CODMUNRES", "ID_MUNICIP"],
        "state": ["state", "SG_UF", "UF", "SG_UF_NOT"],
        "region": ["region"],
        "country": ["country"],
    }
    return _detect_column(columns, candidates.get(level, []))


def system_family(system: str) -> str:
    """Extract the family prefix from a SUS system identifier.

    Args:
        system: Full system name, e.g. ``"SIM-DO"`` or ``"SIH-RD"``.

    Returns:
        Family prefix string, e.g. ``"SIM"`` or ``"SIH"``.

    Example:
        >>> system_family("SIM-DO")
        'SIM'
        >>> system_family("SIH-RD")
        'SIH'
    """
    return system.split("-")[0]


# ---------------------------------------------------------------------------
# Sub-plano D helpers (parity with climasus4r legacy)
# ---------------------------------------------------------------------------

#: Values considered "ignored/unknown" across DATASUS demographic columns.
_IGNORED_VALUES: tuple[str, ...] = (
    "9", "99", "999", "0", "",
    "Ignorado", "ignorado",
    "Unknown", "unknown",
    "Desconocido", "desconocido",
    "Desconhecido", "desconhecido",
    "NaN", "nan", "null", "NULL",
)

#: Demographic columns that carry coded "ignored" markers in DATASUS.
_IGNORABLE_DEMO_COLUMNS: tuple[str, ...] = (
    "sex", "SEXO", "CS_SEXO",
    "race", "RACACOR",
    "education", "education_2010", "ESC", "ESC2010",
    "age", "age_code", "IDADE",
)


def detect_education_column(columns: list[str]) -> str | None:
    """Return the first recognised education column from *columns*.

    Priority: ``education``, ``education_2010``, ``ESC``, ``ESC2010``.

    Mirrors ``climasus4r::sus_data_filter_demographics`` education= logic.

    Args:
        columns: Column names present in the dataset.

    Returns:
        Matching column name, or ``None`` if not found.

    Example:
        >>> detect_education_column(["ESC", "CAUSABAS"])
        'ESC'
        >>> detect_education_column(["UNKNOWN"]) is None
        True
    """
    return _detect_column(columns, ["education", "education_2010", "ESC", "ESC2010"])


def expand_city_to_codes(city: str | list[str]) -> list[str]:
    """Resolve city name(s) to IBGE 6-digit municipality codes.

    Reads ``spatial/municipalities.parquet`` from the climasus-data
    package and matches on the ``municipality_name`` column (case-
    insensitive, accent-insensitive via NFKD decomposition + combining-mark
    strip). When a name matches multiple municipalities (e.g. "São José"),
    all codes are returned and a :class:`UserWarning` is emitted.

    Mirrors ``climasus4r::sus_data_filter_demographics`` city= handling.

    Args:
        city: One or more city names to resolve.

    Returns:
        List of IBGE 6-digit municipality code strings.

    Raises:
        ValueError: If a city name has no match in the municipalities
            parquet.
        FileNotFoundError: If ``spatial/municipalities.parquet`` is not
            present in the climasus-data directory.

    Example:
        >>> expand_city_to_codes("São Paulo")
        ['355030']
        >>> expand_city_to_codes(["São Paulo", "Rio de Janeiro"])
        ['355030', '330455']
    """
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

    # Find the name column (case-insensitive search)
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
        # NFKD decomposes "ç" → "ç", "ã" → "ã", etc.;
        # the comprehension drops the combining marks so "São Paulo"
        # and "Sao Paulo" both normalise to "sao paulo".
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

    return list(dict.fromkeys(all_codes))  # deduplicate, preserve order
