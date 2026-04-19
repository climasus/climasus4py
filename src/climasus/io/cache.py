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
    """List all cached Parquet files with metadata.

    Walks *cache_dir* recursively and collects file-level statistics
    for every ``.parquet`` file found.

    Args:
        cache_dir: Root directory of the Parquet cache. Defaults to
            ``"dados/cache"``.

    Returns:
        ``pandas.DataFrame`` with columns: ``file`` (basename),
        ``path`` (full path string), ``size_mb`` (file size in MB),
        and ``modified`` (ISO-format last-modified timestamp).

    Example:
        >>> info = sus_cache_info()
        >>> info.sort_values("size_mb", ascending=False).head()
    """
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
    """Delete cached Parquet files matching the given filters.

    All filters are applied as AND conditions. Files that satisfy every
    specified criterion are permanently deleted.

    Args:
        cache_dir: Root directory of the Parquet cache.
        system: If provided, only delete files whose parent directory
            name equals this system identifier (e.g. ``"SIM-DO"``).
        uf: If provided, only delete files whose stem starts with
            ``"{UF}_"`` (case-insensitive match).
        before: ISO date string (``"YYYY-MM-DD"``). Only files
            last-modified before this date are deleted.

    Returns:
        Number of files deleted.

    Example:
        >>> sus_cache_clear(system="SIM-DO", uf="SP")
        3
        >>> sus_cache_clear(before="2024-01-01")
    """
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
