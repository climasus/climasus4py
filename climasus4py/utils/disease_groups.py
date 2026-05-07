"""Disease group utilities for climasus4py.

Provides thin wrappers around the ``climasus-data/disease_groups/`` JSON
files so users can discover groups programmatically without knowing the
file structure.

Paridade com ``climasus4r`` legacy:
- ``list_disease_groups()``  ↔  ``sus_list_disease_groups()``
- ``get_disease_group_details()``  ↔  ``sus_disease_group_details()``
"""

from __future__ import annotations

from .data import load_json

_CORE_PATH = "disease_groups/core.json"
_CLIMATE_PATH = "disease_groups/climate_sensitive.json"

_VALID_LANGS: frozenset[str] = frozenset({"pt", "en", "es"})


def _load_groups(climate_sensitive_only: bool) -> dict:
    """Return merged groups from core + climate_sensitive JSONs.

    When *climate_sensitive_only* is True, loads only climate_sensitive.json.
    Otherwise merges core + climate_sensitive (core wins on conflict).
    Meta keys starting with ``_`` are stripped.
    """
    if climate_sensitive_only:
        raw = load_json(_CLIMATE_PATH)
    else:
        raw = {**load_json(_CORE_PATH), **load_json(_CLIMATE_PATH)}

    return {k: v for k, v in raw.items() if not k.startswith("_")}


def list_disease_groups(
    *,
    climate_sensitive_only: bool = False,
    lang: str = "pt",
) -> list[dict]:
    """List all available disease groups.

    Args:
        climate_sensitive_only: When True, returns only groups marked as
            ``climate_sensitive``. When False (default), returns all groups.
        lang: Language for the ``label`` and ``description`` fields.
            One of ``"pt"`` (default), ``"en"``, ``"es"``.

    Returns:
        List of dicts with keys ``group``, ``label``, ``description``,
        ``codes``, ``climate_sensitive``.

    Raises:
        ValueError: If *lang* is not one of the supported values.
        FileNotFoundError: If ``climasus-data`` is not available.

    Examples::

        import climasus4py as cs

        cs.list_disease_groups()
        # [{'group': 'dengue', 'label': 'Dengue', 'description': '...', ...}, ...]

        cs.list_disease_groups(climate_sensitive_only=True, lang="en")
        # [...only climate-sensitive groups in English...]
    """
    if lang not in _VALID_LANGS:
        raise ValueError(
            f"list_disease_groups: unsupported lang {lang!r}. "
            f"Choose one of {sorted(_VALID_LANGS)}."
        )

    groups = _load_groups(climate_sensitive_only)
    result = []
    for group_key, data in groups.items():
        is_cs = bool(data.get("climate_sensitive", False))
        if climate_sensitive_only and not is_cs:
            continue
        result.append(
            {
                "group": group_key,
                "label": data.get("label", {}).get(lang, group_key),
                "description": data.get("description", {}).get(lang, ""),
                "codes": data.get("codes", []),
                "climate_sensitive": is_cs,
            }
        )
    return result


def get_disease_group_details(
    group_name: str,
    *,
    lang: str = "pt",
) -> dict:
    """Return full details for a specific disease group.

    Searches both ``core.json`` and ``climate_sensitive.json``.

    Args:
        group_name: Group key (e.g. ``"dengue"``, ``"respiratory"``).
        lang: Language for the ``label``, ``description`` fields.
            One of ``"pt"`` (default), ``"en"``, ``"es"``.

    Returns:
        Dict with keys ``group``, ``label``, ``description``, ``codes``,
        ``climate_sensitive``, ``climate_factors`` (list or empty list).

    Raises:
        KeyError: If *group_name* is not found in either JSON.
        ValueError: If *lang* is not supported.
        FileNotFoundError: If ``climasus-data`` is not available.

    Examples::

        import climasus4py as cs

        cs.get_disease_group_details("dengue", lang="en")
        # {
        #     'group': 'dengue',
        #     'label': 'Dengue',
        #     'description': 'Classical dengue and dengue hemorrhagic fever',
        #     'codes': ['A90', 'A91'],
        #     'climate_sensitive': True,
        #     'climate_factors': ['temperature', 'precipitation', 'humidity'],
        # }
    """
    if lang not in _VALID_LANGS:
        raise ValueError(
            f"get_disease_group_details: unsupported lang {lang!r}. "
            f"Choose one of {sorted(_VALID_LANGS)}."
        )

    groups = _load_groups(climate_sensitive_only=False)
    if group_name not in groups:
        available = sorted(groups.keys())
        raise KeyError(
            f"Disease group {group_name!r} not found. "
            f"Available groups: {available}"
        )

    data = groups[group_name]
    return {
        "group": group_name,
        "label": data.get("label", {}).get(lang, group_name),
        "description": data.get("description", {}).get(lang, ""),
        "codes": data.get("codes", []),
        "climate_sensitive": bool(data.get("climate_sensitive", False)),
        "climate_factors": data.get("climate_factors", []),
    }
