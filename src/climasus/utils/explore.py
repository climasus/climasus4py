"""Metadata exploration — browse disease groups, systems, regions.

Mirrors R: explore.R
"""

from __future__ import annotations

from climasus.utils.data import load_json, load_systems, load_uf_codes, load_regions


def sus_explore(
    what: str = "systems",
) -> dict:
    """Browse climasus metadata.

    Parameters
    ----------
    what : "systems", "groups", "regions", "uf"
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
