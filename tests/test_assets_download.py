"""climasus-data parquet assets downloaded on demand (07/11/2026).

The climasus-data wheel ships no parquet, so a pip-only install had no census,
polygons or INMET tables. Missing assets are now fetched once from GitHub at a
pinned commit, MD5-checked against that commit's manifest and cached. The
network is faked here; ``test_real_download`` hits GitHub and runs only with
``CLIMASUS_NETWORK_TESTS=1``.
"""

from __future__ import annotations

import hashlib
import json
import os
import urllib.error
from pathlib import Path

import pytest

from climasus4py.utils import assets
from climasus4py.utils import data as data_mod

PARQUET = b"PAR1" + b"x" * 200 + b"PAR1"
OUTRO = b"PAR1" + b"y" * 50 + b"PAR1"


def _entry(path: str, body: bytes) -> dict:
    return {"path": path, "size_bytes": len(body), "md5": hashlib.md5(body).hexdigest()}


@pytest.fixture
def rede(tmp_path, monkeypatch):
    """A fake climasus-data dir with no assets, an empty cache, and fake HTTP."""
    local = tmp_path / "climasus_data"
    local.mkdir()
    (local / "manifest.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CLIMASUS_ASSETS_CACHE", str(tmp_path / "cache"))
    monkeypatch.delenv("CLIMASUS_DATA_REF", raising=False)
    monkeypatch.setattr(data_mod, "_DATA_DIR", local)
    monkeypatch.setattr(assets, "_manifests", {})

    corpos = {
        "assets/census/census_2010.parquet": PARQUET,
        "assets/census/census_2022.parquet": OUTRO,
        "assets/spatial/municipalities.parquet": PARQUET * 3,
        "assets/spatial/municipalities_simplified.parquet": OUTRO,
    }
    manifest = {"files": [_entry(p, b) for p, b in corpos.items()]}
    pedidos: list[str] = []

    def http_get(url: str) -> bytes:
        pedidos.append(url)
        if url.endswith("manifest.json"):
            return json.dumps(manifest).encode()
        rel = url.split(assets.assets_ref() + "/", 1)[1]
        return corpos[rel]

    monkeypatch.setattr(assets, "_http_get", http_get)
    return {"local": local, "corpos": corpos, "pedidos": pedidos, "tmp": tmp_path}


def _baixados(rede) -> list[str]:
    return [u for u in rede["pedidos"] if not u.endswith("manifest.json")]


def test_missing_asset_is_downloaded_once_and_cached(rede):
    p = data_mod.data_path("assets/census/census_2010.parquet")
    assert p.read_bytes() == PARQUET
    assert p.is_relative_to(assets.cache_root())
    assert data_mod.data_path("assets/census/census_2010.parquet") == p
    assert len(_baixados(rede)) == 1
    assert "media.githubusercontent.com" in _baixados(rede)[0]
    assert assets.ASSETS_REF in _baixados(rede)[0]


def test_local_copy_wins_and_nothing_is_downloaded(rede):
    alvo = rede["local"] / "assets/census/census_2010.parquet"
    alvo.parent.mkdir(parents=True)
    alvo.write_bytes(OUTRO)
    assert data_mod.data_path("assets/census/census_2010.parquet") == alvo
    assert rede["pedidos"] == []


def test_local_assets_are_the_source_even_when_a_file_is_missing(rede, monkeypatch):
    """A clone (or fixture) with its own parquets is never mixed with downloads:
    an older clone without the simplified layer keeps falling back to its own
    full layer, as before."""
    monkeypatch.setattr(data_mod, "_AVISOU_SEM_SIMPLIFICADO", set())  # warns once per session
    cheio = rede["local"] / "assets/spatial/municipalities.parquet"
    cheio.parent.mkdir(parents=True)
    cheio.write_bytes(PARQUET)
    with pytest.warns(UserWarning, match="simplified.parquet not found"):
        assert data_mod.spatial_asset_path("municipalities") == cheio
    assert rede["pedidos"] == []


def test_lfs_pointer_counts_as_missing(rede):
    alvo = rede["local"] / "assets/census/census_2010.parquet"
    alvo.parent.mkdir(parents=True)
    alvo.write_bytes(b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1248511\n")
    p = data_mod.data_path("assets/census/census_2010.parquet")
    assert p != alvo and p.read_bytes() == PARQUET


def test_md5_mismatch_raises_and_keeps_nothing(rede, monkeypatch):
    original = assets._http_get
    monkeypatch.setattr(assets, "_http_get",
                        lambda url: original(url) if url.endswith("manifest.json")
                        else b"corrompido")
    with pytest.raises(OSError, match="integrity check"):
        data_mod.data_path("assets/census/census_2010.parquet")
    assert not list((assets.cache_root() / "assets").rglob("*.parquet"))


def test_directory_request_brings_every_file_under_it(rede):
    d = data_mod.data_path("assets/census")
    nomes = sorted(p.name for p in d.glob("census_*.parquet"))
    assert nomes == ["census_2010.parquet", "census_2022.parquet"]


def test_path_outside_the_manifest_is_returned_unchanged(rede):
    p = data_mod.data_path("assets/climate/inmet_observations_2031.parquet")
    assert p == rede["local"] / "assets/climate/inmet_observations_2031.parquet"
    assert not p.exists() and _baixados(rede) == []


def test_non_asset_paths_never_touch_the_network(rede):
    data_mod.data_path("metadata/datasus_systems.json")
    assert rede["pedidos"] == []


def test_offline_gives_a_clear_error(rede, monkeypatch):
    def sem_rede(url):
        raise urllib.error.URLError("no route")

    monkeypatch.setattr(assets, "_http_get", sem_rede)
    with pytest.raises(OSError, match="CLIMASUS_DATA_DIR"):
        data_mod.data_path("assets/census/census_2010.parquet")


def test_simplified_layer_does_not_pull_the_full_one(rede):
    """municipalities.parquet is 150 MB; asking for the simplified layer must not fetch it."""
    p = data_mod.spatial_asset_path("municipalities")
    assert p.name == "municipalities_simplified.parquet"
    assert not any(u.endswith("/municipalities.parquet") for u in _baixados(rede))


def test_ref_and_cache_can_be_overridden(rede, monkeypatch):
    monkeypatch.setenv("CLIMASUS_DATA_REF", "andrey")
    assert assets.assets_ref() == "andrey"
    assert assets.cache_root().name == "andrey"


@pytest.mark.skipif(os.environ.get("CLIMASUS_NETWORK_TESTS") != "1",
                    reason="hits GitHub; set CLIMASUS_NETWORK_TESTS=1")
def test_real_download(tmp_path, monkeypatch):
    monkeypatch.setenv("CLIMASUS_ASSETS_CACHE", str(tmp_path))
    monkeypatch.delenv("CLIMASUS_DATA_REF", raising=False)
    monkeypatch.setattr(assets, "_manifests", {})
    vazio = tmp_path / "vazio"
    rel = "assets/census/census_2022.parquet"
    p = assets.resolve_asset(vazio, rel)
    assert hashlib.md5(p.read_bytes()).hexdigest() == "bd34cd8c5efd9cc4c0c902647cb02a2e"
    assert p.read_bytes()[:4] == b"PAR1"
    assert isinstance(p, Path) and p.is_relative_to(tmp_path)
