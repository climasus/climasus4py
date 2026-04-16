"""Tests for cache management — sus_cache_info and sus_cache_clear."""

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from climasus.io.cache import sus_cache_info, sus_cache_clear


@pytest.fixture
def cache_dir(tmp_path):
    """Create a fake cache directory with sample parquet files."""
    sim = tmp_path / "SIM-DO"
    sim.mkdir()
    sih = tmp_path / "SIH-RD"
    sih.mkdir()

    table = pa.table({"x": [1, 2, 3]})
    pq.write_table(table, sim / "SP_2023_all.parquet")
    pq.write_table(table, sim / "RJ_2023_all.parquet")
    pq.write_table(table, sih / "SP_2022_all.parquet")

    return tmp_path


class TestCacheInfo:
    def test_lists_all_parquets(self, cache_dir):
        df = sus_cache_info(cache_dir)
        assert len(df) == 3
        assert "file" in df.columns
        assert "size_mb" in df.columns

    def test_empty_cache(self, tmp_path):
        df = sus_cache_info(tmp_path)
        assert len(df) == 0

    def test_nonexistent_dir(self, tmp_path):
        df = sus_cache_info(tmp_path / "noexist")
        assert len(df) == 0


class TestCacheClear:
    def test_clear_all(self, cache_dir):
        count = sus_cache_clear(cache_dir)
        assert count == 3
        assert sus_cache_info(cache_dir).empty

    def test_clear_by_system(self, cache_dir):
        count = sus_cache_clear(cache_dir, system="SIM-DO")
        assert count == 2
        remaining = sus_cache_info(cache_dir)
        assert len(remaining) == 1
        assert remaining.iloc[0]["file"] == "SP_2022_all.parquet"

    def test_clear_by_uf(self, cache_dir):
        count = sus_cache_clear(cache_dir, uf="SP")
        assert count == 2  # SP in SIM-DO and SIH-RD
        remaining = sus_cache_info(cache_dir)
        assert len(remaining) == 1

    def test_clear_empty_dir(self, tmp_path):
        count = sus_cache_clear(tmp_path)
        assert count == 0

    def test_clear_nonexistent_dir(self, tmp_path):
        count = sus_cache_clear(tmp_path / "noexist")
        assert count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
