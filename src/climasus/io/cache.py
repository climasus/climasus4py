"""Cache management — inspect and clear cached parquet files.

Mirrors R: cache.R
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

_DEFAULT_CACHE = Path("dados/cache")


def sus_cache_info(cache_dir: str | Path = _DEFAULT_CACHE) -> pd.DataFrame:
    """List all cached parquet files with metadata."""
    cache_dir = Path(cache_dir)
    records = []

    if cache_dir.is_dir():
        for pq_file in cache_dir.rglob("*.parquet"):
            stat = pq_file.stat()
            records.append({
                "file": pq_file.name,
                "path": str(pq_file),
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            })

    return pd.DataFrame(records)


def sus_cache_clear(
    cache_dir: str | Path = _DEFAULT_CACHE,
    *,
    system: str | None = None,
    uf: str | None = None,
    before: str | None = None,
) -> int:
    """Delete cached parquet files matching the given filters. Returns count deleted."""
    cache_dir = Path(cache_dir)
    count = 0

    if not cache_dir.is_dir():
        return 0

    for pq_file in cache_dir.rglob("*.parquet"):
        # Filter by system (subdirectory name)
        if system and pq_file.parent.name != system:
            continue
        # Filter by UF (filename starts with UF_)
        if uf and not pq_file.stem.startswith(uf.upper() + "_"):
            continue
        # Filter by modification date
        if before:
            mod_date = datetime.fromtimestamp(pq_file.stat().st_mtime)
            cutoff = datetime.fromisoformat(before)
            if mod_date >= cutoff:
                continue

        pq_file.unlink()
        count += 1

    return count
