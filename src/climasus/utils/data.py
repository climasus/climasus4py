"""Data loading and column detection utilities.

Mirrors R: utils-data.R — JSON loading, column/system detection, UF resolution.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

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
            ``"metadata/sus_systems.json"``.

    Returns:
        Absolute ``pathlib.Path`` to the requested file.

    Example:
        >>> data_path("metadata/sus_systems.json").exists()
        True
    """
    return _find_data_dir() / relative


# ---------------------------------------------------------------------------
# JSON loading (cached)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=32)
def load_json(relative: str) -> Any:
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
        >>> data = load_json("metadata/sus_systems.json")
        >>> list(data.keys())
        ['systems']
    """
    path = data_path(relative)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado em climasus-data: {relative}\n"
                                "Certifique-se de que o diretório clonado está presente e atualizado.")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Update function: baixa/atualiza climasus-data localmente
# ---------------------------------------------------------------------------

def update_climasus_data(repo_url: str = "https://github.com/climasus/climasus-data.git", target_dir: str | None = None, branch: str = "main") -> None:
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
        >>> import climasus as cs
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
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)
    else:
        # Não existe: clona
        print(f"Clonando climasus-data em {target}...")
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)
    print("climasus-data atualizado com sucesso.")


def load_systems() -> dict:
    """Load SUS system definitions from climasus-data.

    Returns:
        Dict mapping system identifiers to their metadata.

    Example:
        >>> systems = load_systems()
        >>> list(systems.keys())[:3]
        ['SIM-DO', 'SINASC', 'SIH-RD']
    """
    return load_json("metadata/sus_systems.json")["systems"]


def load_uf_codes() -> dict:
    """Load Brazilian state (UF) codes from climasus-data.

    Returns:
        Dict mapping 2-letter UF abbreviations to state metadata.

    Example:
        >>> ufs = load_uf_codes()
        >>> ufs["SP"]["name"]
        'São Paulo'
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

_SYSTEM_SIGNATURES: dict[str, list[str]] = {
    "SIM-DO": ["CAUSABAS", "DTOBITO"],
    "SIH-RD": ["DIAG_PRINC"],
    "SINAN-DENGUE": ["NU_NOTIFIC"],
    "SINASC": ["NUMERODN"],
}


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
    for system, signatures in _SYSTEM_SIGNATURES.items():
        if col_set & set(signatures):
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
    return _detect_column(columns, ["death_date", "date", "DTOBITO", "DTNASC", "admission_date"])


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
    return _detect_column(columns, ["underlying_cause", "cause", "CAUSABAS", "DIAG_PRINC"])


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
    return _detect_column(columns, ["age", "age_years", "age_code", "IDADE", "IDADEMAE"])


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
    return _detect_column(columns, ["sex", "SEXO", "CS_SEXO"])


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
