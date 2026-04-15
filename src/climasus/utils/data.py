"""Data loading and column detection utilities.

Mirrors R: utils-data.R — JSON loading, column/system detection, UF resolution.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Data directory resolution
# ---------------------------------------------------------------------------

_DATA_DIR: Path | None = None


def _find_data_dir() -> Path:
    """Locate the climasus-data directory (bundled or external)."""
    global _DATA_DIR
    if _DATA_DIR is not None:
        return _DATA_DIR

    # Honour environment variable first
    import os
    env = os.environ.get("CLIMASUS_DATA_DIR")
    if env:
        p = Path(env)
        if (p / "manifest.json").is_file():
            _DATA_DIR = p
            return p

    # Walk up from this file to find climasus-data/
    anchor = Path(__file__).resolve()
    for parent in anchor.parents:
        candidate = parent / "climasus-data"
        if (candidate / "manifest.json").is_file():
            _DATA_DIR = candidate
            return candidate

    # Also try cwd and home
    for p in [Path.cwd() / "climasus-data", Path.home() / ".climasus" / "climasus-data"]:
        if (p / "manifest.json").is_file():
            _DATA_DIR = p
            return p

    msg = (
        "climasus-data not found. Clone the repo alongside climasus4py or set "
        "CLIMASUS_DATA_DIR environment variable."
    )
    raise FileNotFoundError(msg)


def data_path(relative: str) -> Path:
    """Return absolute path to a file inside climasus-data."""
    return _find_data_dir() / relative


# ---------------------------------------------------------------------------
# JSON loading (cached)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=32)
def load_json(relative: str) -> Any:
    """Load and cache a JSON file from climasus-data (apenas local)."""
    path = data_path(relative)
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado em climasus-data: {relative}\n"
                                "Certifique-se de que o diretório clonado está presente e atualizado.")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Update function: baixa/atualiza climasus-data localmente
# ---------------------------------------------------------------------------
import subprocess
import shutil
import sys
def update_climasus_data(repo_url: str = "https://github.com/climasus/climasus-data.git", target_dir: str | None = None, branch: str = "main") -> None:
    """Baixa ou atualiza o repositório climasus-data localmente.
    Por padrão, busca ao lado do projeto (ou usa CLIMASUS_DATA_DIR).

    Observação: o URL padrão agora aponta para https://github.com/climasus/climasus-data.git
    """
    import os
    if target_dir is None:
        # Prioriza env var, senão ao lado do projeto
        env = os.environ.get("CLIMASUS_DATA_DIR")
        if env:
            target_dir = env
        else:
            # Caminho padrão: ao lado do projeto
            anchor = Path(__file__).resolve()
            for parent in anchor.parents:
                candidate = parent / "climasus-data"
                if candidate.exists():
                    target_dir = str(candidate)
                    break
            else:
                # Se não existe, define ao lado do projeto
                target_dir = str(anchor.parent.parent.parent / "climasus-data")

    target = Path(target_dir)
    if target.exists() and (target / ".git").is_dir():
        # Já existe: git pull
        print(f"Atualizando climasus-data em {target}...")
        subprocess.run(["git", "-C", str(target), "pull", "origin", branch], check=True)
    elif target.exists():
        # Existe mas não é git: remove e clona
        print(f"Removendo diretório existente e clonando climasus-data em {target}...")
        shutil.rmtree(target)
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)
    else:
        # Não existe: clona
        print(f"Clonando climasus-data em {target}...")
        subprocess.run(["git", "clone", "--depth", "1", "-b", branch, repo_url, str(target)], check=True)
    print("climasus-data atualizado com sucesso.")


def load_systems() -> dict:
    """Load SUS system definitions."""
    return load_json("metadata/sus_systems.json")["systems"]


def load_uf_codes() -> dict:
    """Load Brazilian state codes."""
    return load_json("metadata/uf_codes.json")["states"]


def load_regions() -> dict:
    """Load Brazilian region definitions."""
    return load_json("metadata/regions.json")["categories"]


# ---------------------------------------------------------------------------
# UF resolution  (mirrors .resolve_uf)
# ---------------------------------------------------------------------------

def resolve_uf(uf: str | list[str]) -> list[str]:
    """Resolve UF specification to list of state codes.

    Accepts: single UF ("SP"), list of UFs, "all", or region name ("Sudeste").
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
    """Detect SUS system from column names."""
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
    return _detect_column(columns, ["death_date", "date", "DTOBITO", "DTNASC", "admission_date"])


def detect_cause_column(columns: list[str]) -> str | None:
    return _detect_column(columns, ["underlying_cause", "cause", "CAUSABAS", "DIAG_PRINC"])


def detect_age_column(columns: list[str]) -> str | None:
    return _detect_column(columns, ["age", "age_years", "age_code", "IDADE", "IDADEMAE"])


def detect_sex_column(columns: list[str]) -> str | None:
    return _detect_column(columns, ["sex", "SEXO", "CS_SEXO"])


def detect_geo_column(columns: list[str], level: str = "municipality") -> str | None:
    candidates = {
        "municipality": ["municipality_code", "CODMUNRES", "ID_MUNICIP"],
        "state": ["state", "SG_UF", "UF", "SG_UF_NOT"],
        "region": ["region"],
        "country": ["country"],
    }
    return _detect_column(columns, candidates.get(level, []))


def system_family(system: str) -> str:
    """Extract system family: 'SIM-DO' → 'SIM'."""
    return system.split("-")[0]
