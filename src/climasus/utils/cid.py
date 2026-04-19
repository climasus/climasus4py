"""CID-10 (ICD-10) code expansion and disease group utilities.

Mirrors R: utils-cid.R — expand_cid_ranges, codes_for_groups.
"""

from __future__ import annotations

import string

from climasus.utils.data import load_json


def expand_cid_range(start: str, end: str) -> list[str]:
    """Expand an ICD-10 range into a list of all codes it contains.

    Handles the standard letter+two-digit-number format (A00–Z99).
    Both endpoints are inclusive.

    Args:
        start: Starting ICD-10 code, e.g. ``"A00"``.
        end: Ending ICD-10 code (inclusive), e.g. ``"A09"``.

    Returns:
        List of ICD-10 code strings spanning from *start* to *end*.

    Example:
        >>> expand_cid_range("J00", "J06")
        ['J00', 'J01', 'J02', 'J03', 'J04', 'J05', 'J06']
    """
    s_letter, s_num = start[0].upper(), int(start[1:])
    e_letter, e_num = end[0].upper(), int(end[1:])

    codes: list[str] = []
    for letter in string.ascii_uppercase[
        string.ascii_uppercase.index(s_letter) : string.ascii_uppercase.index(e_letter) + 1
    ]:
        lo = s_num if letter == s_letter else 0
        hi = e_num if letter == e_letter else 99
        for num in range(lo, hi + 1):
            codes.append(f"{letter}{num:02d}")
    return codes


def expand_cid_ranges(codes: list[str]) -> list[str]:
    """Expand a mixed list of ICD-10 codes, ranges, and letter prefixes.

    Each item in *codes* is parsed and expanded:

    - **Single code** (e.g. ``"A90"``) — returned as-is.
    - **Range** (e.g. ``"A00-A09"``) — expanded via
      :func:`expand_cid_range`.
    - **Letter prefix** (e.g. ``"J"``) — expanded to all codes
      ``J00``–``J99``.

    Args:
        codes: Mixed list of ICD-10 codes, ranges, and/or letter
            prefixes.

    Returns:
        Flat list of individual ICD-10 code strings.

    Example:
        >>> expand_cid_ranges(["A90", "J00-J06", "K"])
        ['A90', 'J00', 'J01', ..., 'K00', ..., 'K99']
    """
    expanded: list[str] = []
    for item in codes:
        item = item.strip().upper()
        if "-" in item:
            parts = item.split("-", 1)
            expanded.extend(expand_cid_range(parts[0], parts[1]))
        elif len(item) == 1 and item.isalpha():
            expanded.extend(expand_cid_range(f"{item}00", f"{item}99"))
        else:
            expanded.append(item)
    return expanded


def codes_for_groups(group_names: list[str]) -> list[str]:
    """Load ICD-10 codes for named disease groups from climasus-data.

    Searches both ``disease_groups/core.json`` and
    ``disease_groups/climate_sensitive.json``. Matches groups by
    identifier key or by any language variant of the ``label`` field.
    Supports the flat schema ``{group_id: {codes: [...]}}`` used by
    the current climasus-data release.

    Args:
        group_names: List of group identifiers or labels to look up,
            e.g. ``["respiratory", "dengue"]``.

    Returns:
        Sorted, deduplicated list of ICD-10 code strings for all
        matched groups.

    Raises:
        FileNotFoundError: If the required JSON files are not present
            in the climasus-data directory.

    Example:
        >>> codes_for_groups(["respiratory"])
        ['J00', 'J01', ..., 'J99']
        >>> len(codes_for_groups(["cardiovascular", "dengue"]))
    """
    all_codes: list[str] = []

    for json_file in ("disease_groups/core.json", "disease_groups/climate_sensitive.json"):
        data = load_json(json_file)

        # Flat format: {group_id: {codes: [...]}} (current climasus-data)
        for group_id, group_data in data.items():
            if group_id.startswith("_"):
                continue  # skip _meta
            if not isinstance(group_data, dict):
                continue
            if group_id in group_names:
                raw = group_data.get("codes", group_data.get("icd10_codes", []))
                all_codes.extend(expand_cid_ranges(raw))
                continue
            # Also match by label
            label = group_data.get("label", {})
            if isinstance(label, dict):
                if any(v in group_names for v in label.values()):
                    raw = group_data.get("codes", group_data.get("icd10_codes", []))
                    all_codes.extend(expand_cid_ranges(raw))

    return sorted(set(all_codes))
