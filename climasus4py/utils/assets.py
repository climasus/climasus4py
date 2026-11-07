"""On-demand download of the climasus-data parquet assets.

The climasus-data package on PyPI -- and the wheel built from its repository
-- ships only the JSON dictionaries and metadata. The parquet files under
``assets/`` (census, municipality/state/region polygons, INMET tables; about
200 MB) live in the repository through Git LFS and were never installed with
the package: a ``pip install climasus4py`` user had none of them, and
``sus_spatial_join``, ``sus_census``, the climate join and the maps stopped
with "file not found". Getting them meant cloning the repository with
``git-lfs`` and pointing ``CLIMASUS_DATA_DIR`` at the clone.

climasus4r has no such step because it downloads at use time (geobr,
censobr). This does the same with the files climasus-data already prepared
from those sources: the first time a function asks for an asset that is not
on disk, it is downloaded from GitHub at a **pinned commit**, checked against
the MD5 that commit's ``manifest.json`` records, and kept in a user cache.
Later calls read the cache; nothing is downloaded twice.

Why a pinned commit and not a branch: the same climasus4py install must give
the same result next year. A branch moves; a commit does not. Bump
:data:`ASSETS_REF` when climasus-data publishes new data, or set
``CLIMASUS_DATA_REF`` to try another commit or branch.

A local copy always wins: with an editable climasus-data or
``CLIMASUS_DATA_DIR`` pointing at a full clone, nothing is downloaded. A local
file that is only a Git LFS *pointer* (a clone made without ``git-lfs``) is
treated as missing.

Environment variables:
    CLIMASUS_DATA_REF: commit, tag or branch to download from.
    CLIMASUS_ASSETS_CACHE: cache directory (default: the user cache dir,
        ``%LOCALAPPDATA%\\climasus4py`` on Windows, ``~/.cache/climasus4py``
        elsewhere).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

#: climasus-data commit the assets are downloaded from. d76f2fc (19/10/2026,
#: branch andrey) is the first with the real IBGE 2010 census; every R x
#: Python comparison since then ran on exactly these files.
ASSETS_REF = "d76f2fc3777180509663d601f81ff2f1551e9467"

_REPO = "climasus/climasus-data"
# LFS content is served by media.githubusercontent.com; raw.githubusercontent
# returns the 130-byte pointer for an LFS file, and the real file otherwise.
_MEDIA_URL = "https://media.githubusercontent.com/media/{repo}/{ref}/{path}"
_RAW_URL = "https://raw.githubusercontent.com/{repo}/{ref}/{path}"
_LFS_POINTER_PREFIX = b"version https://git-lfs"
_TIMEOUT = 120

_manifests: dict[str, dict[str, dict[str, Any]]] = {}


def assets_ref() -> str:
    """The climasus-data commit (or ref) assets are downloaded from."""
    return os.environ.get("CLIMASUS_DATA_REF") or ASSETS_REF


def cache_root() -> Path:
    """Directory holding the downloaded assets of the current ref."""
    base = os.environ.get("CLIMASUS_ASSETS_CACHE")
    if base:
        root = Path(base)
    elif sys.platform == "win32" and os.environ.get("LOCALAPPDATA"):
        root = Path(os.environ["LOCALAPPDATA"]) / "climasus4py"
    else:
        root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "climasus4py"
    return root / "climasus-data" / assets_ref()[:12]


def is_lfs_pointer(path: Path) -> bool:
    """True for a Git LFS pointer file -- a clone made without git-lfs."""
    try:
        if path.stat().st_size > 1024:
            return False
        with open(path, "rb") as f:
            return f.read(len(_LFS_POINTER_PREFIX)) == _LFS_POINTER_PREFIX
    except OSError:
        return False


def _usable(path: Path) -> bool:
    return path.is_file() and not is_lfs_pointer(path)


def _has_local_assets(data_dir: Path) -> bool:
    """True when *data_dir* has its own assets: any real parquet under it."""
    pasta = data_dir / "assets"
    return pasta.is_dir() and any(_usable(p) for p in pasta.rglob("*.parquet"))


def _http_get(url: str) -> bytes:
    """GET *url* and return the body. Isolated so tests can replace it."""
    req = urllib.request.Request(url, headers={"User-Agent": "climasus4py"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:  # noqa: S310 - fixed https host
        return resp.read()


def _manifest() -> dict[str, dict[str, Any]]:
    """``path -> entry`` from manifest.json at the current ref, cached on disk."""
    ref = assets_ref()
    if ref in _manifests:
        return _manifests[ref]
    local = cache_root() / "manifest.json"
    if local.is_file():
        texto = local.read_text(encoding="utf-8")
    else:
        url = _RAW_URL.format(repo=_REPO, ref=ref, path="manifest.json")
        texto = _http_get(url).decode("utf-8")
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(texto, encoding="utf-8")
    entradas = {e["path"]: e for e in json.loads(texto).get("files", []) if "path" in e}
    _manifests[ref] = entradas
    return entradas


def _download(entry: dict[str, Any], dest: Path) -> None:
    rel = entry["path"]
    mb = entry.get("size_bytes", 0) / 1e6
    print(f"climasus4py: downloading {rel} ({mb:.1f} MB) from climasus-data@{assets_ref()[:7]}; "
          f"kept in {cache_root()} for next time.", file=sys.stderr)
    corpo = _http_get(_MEDIA_URL.format(repo=_REPO, ref=assets_ref(), path=rel))
    md5 = hashlib.md5(corpo).hexdigest()  # noqa: S324 - integrity check, not security
    if entry.get("md5") and md5 != entry["md5"]:
        raise OSError(
            f"climasus-data asset {rel} failed its integrity check: MD5 {md5}, the "
            f"manifest at {assets_ref()[:7]} records {entry['md5']}. Nothing was "
            f"saved; try again, or set CLIMASUS_DATA_DIR to a full clone."
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(corpo)
    os.replace(tmp, dest)


def _cached(entry: dict[str, Any]) -> Path:
    dest = cache_root() / entry["path"]
    tamanho = entry.get("size_bytes")
    if not (dest.is_file() and (tamanho is None or dest.stat().st_size == tamanho)):
        _download(entry, dest)
    return dest


def resolve_asset(data_dir: Path, relative: str) -> Path:
    """Path to the asset *relative* (``assets/...``), downloading it if needed.

    *data_dir* is the climasus-data directory in use. When its ``assets/``
    holds any real parquet, that copy is the source of truth and the local
    path is returned as is, present or not -- a full clone, an older one
    without some file, or a test fixture keeps its own behaviour (fallbacks,
    "not found" messages) and is never mixed with files from another commit.
    Only an ``assets/`` with no real parquet -- the pip-installed package, or
    a clone made without git-lfs -- is served from the download cache.

    A path the manifest does not list comes back unchanged, so the caller's
    own "not found" handling applies. A directory comes back as the cache
    directory with every manifest file under it present.
    """
    rel = relative.strip("/").replace("\\", "/")
    local = data_dir / rel
    if _has_local_assets(data_dir):
        return local
    try:
        entradas = _manifest()
        if rel in entradas:
            return _cached(entradas[rel])
        dentro = [e for p, e in entradas.items() if p.startswith(rel + "/")]
        if not dentro:
            return local
        for e in dentro:
            _cached(e)
        return cache_root() / rel
    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
        raise OSError(
            f"climasus-data asset '{rel}' is not installed and could not be "
            f"downloaded ({exc}). It is fetched once from GitHub and cached; "
            f"connect to the internet and try again, or point CLIMASUS_DATA_DIR "
            f"at a full clone of climasus-data (made with git-lfs)."
        ) from exc
