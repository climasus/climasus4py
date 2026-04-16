"""Tests for sus_export — parquet, csv, xlsx export."""

import pandas as pd
import pyarrow.parquet as pq
import pytest

from climasus.core.engine import get_connection
from climasus.io.export import sus_export


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


@pytest.fixture
def sample_rel():
    return _make_rel({
        "name": ["Alice", "Bob", "Carol"],
        "value": [10, 20, 30],
    })


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Carol"],
        "value": [10, 20, 30],
    })


class TestExportParquet:
    def test_relation_to_parquet(self, tmp_path, sample_rel):
        path = tmp_path / "out.parquet"
        result = sus_export(sample_rel, path)
        assert result == path
        assert path.is_file()
        table = pq.read_table(path)
        assert table.num_rows == 3

    def test_dataframe_to_parquet(self, tmp_path, sample_df):
        path = tmp_path / "out.parquet"
        sus_export(sample_df, path)
        assert path.is_file()
        table = pq.read_table(path)
        assert table.num_rows == 3


class TestExportCSV:
    def test_relation_to_csv(self, tmp_path, sample_rel):
        path = tmp_path / "out.csv"
        sus_export(sample_rel, path)
        assert path.is_file()
        df = pd.read_csv(path)
        assert len(df) == 3

    def test_dataframe_to_csv(self, tmp_path, sample_df):
        path = tmp_path / "out.csv"
        sus_export(sample_df, path)
        assert path.is_file()
        df = pd.read_csv(path)
        assert len(df) == 3


class TestExportEdgeCases:
    def test_overwrite_false_raises(self, tmp_path, sample_rel):
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        with pytest.raises(FileExistsError):
            sus_export(sample_rel, path, overwrite=False)

    def test_unsupported_format_raises(self, tmp_path, sample_rel):
        path = tmp_path / "out.xyz"
        with pytest.raises(ValueError, match="Unsupported format"):
            sus_export(sample_rel, path)

    def test_creates_parent_dirs(self, tmp_path, sample_rel):
        path = tmp_path / "sub" / "dir" / "out.csv"
        sus_export(sample_rel, path)
        assert path.is_file()

    def test_explicit_format_overrides_extension(self, tmp_path, sample_rel):
        path = tmp_path / "out.txt"
        sus_export(sample_rel, path, fmt="csv")
        assert path.is_file()
        df = pd.read_csv(path)
        assert len(df) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
