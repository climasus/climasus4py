"""Metadata exploration — browse disease groups, systems, regions.

Mirrors R: explore.R
"""

from __future__ import annotations

from climasus.utils.data import load_json, load_systems, load_uf_codes, load_regions


def sus_explore(
    what: str = "systems",
) -> dict:
    """Browse climasus-data metadata interactively.

    Returns the requested metadata dictionary from climasus-data,
    useful for discovering available systems, disease groups, regions,
    and state codes before building a pipeline.

    Args:
        what: Topic to browse. Accepted values:

            - ``"systems"`` — available SUS systems and their
              properties.
            - ``"groups"`` — disease group definitions (core and
              climate-sensitive).
            - ``"regions"`` — Brazilian macro-regions and member
              states.
            - ``"uf"`` — 2-letter state codes and names.

    Returns:
        Dictionary with the requested metadata. Structure depends on
        the *what* argument.

    Raises:
        ValueError: If *what* is not one of the accepted values.

    Example:
        >>> systems = sus_explore("systems")
        >>> list(systems.keys())[:3]
        >>> sus_explore("groups")["climate_sensitive"]
    """
    if what == "systems":
        return load_systems()

    elif what == "groups":
        core = load_json("disease_groups/core.json")
        sensitive = load_json("disease_groups/climate_sensitive.json")
        return {"core": core, "climate_sensitive": sensitive}

    elif what == "regions":
        return load_regions()

    elif what == "uf":
        return load_uf_codes()

    else:
        raise ValueError(f"Unknown topic: {what}. Use systems, groups, regions, or uf.")
