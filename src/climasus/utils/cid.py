"""CID-10 (ICD-10) code expansion and disease group utilities.

Mirrors R: utils-cid.R — expand_cid_ranges, codes_for_groups.
"""

from __future__ import annotations

import string

from climasus.utils.data import load_json


def expand_cid_range(start: str, end: str) -> list[str]:
    """Expand an ICD-10 range like ('A00', 'A09') into all codes between.

    Handles letter+number format: A00..A99, B00..B99, etc.
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
    """Expand a mixed list of ICD-10 codes and ranges.

    Items can be:
    - Single code: "A90"
    - Range: "A00-A09"
    - Prefix: "J" (matches J00-J99)
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

    Searches both core.json and climate_sensitive.json.
    Supports flat format ``{group_id: {codes: [...]}}`` and nested formats.
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
