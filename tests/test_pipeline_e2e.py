"""End-to-end pipeline test using cached parquet from R version."""

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

# Add src to path for editable install
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import climasus4py as cs

PARQUET = Path(os.getenv("CLIMASUS_TEST_PARQUET", ""))

pytestmark = pytest.mark.skipif(
    not PARQUET.is_file(),
    reason=(
        "Integration parquet not found. "
        "Set CLIMASUS_TEST_PARQUET=/path/to/file.parquet to run e2e tests."
    ),
)


def test_import_path():
    """sus_data_import(path=) returns a DuckDB relation."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    assert rel is not None
    nrows = rel.count("*").fetchone()[0]
    print(f"  import: {nrows:,} rows, {len(rel.columns)} cols")
    assert nrows > 0


def test_import_data():
    """sus_data_import(data=) accepts a DataFrame."""
    df = pq.read_table(PARQUET).to_pandas().head(100)
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, data=df)
    nrows = rel.count("*").fetchone()[0]
    assert nrows == 100


def test_clean():
    """sus_data_clean_encoding removes duplicates."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    cleaned = cs.sus_data_clean_encoding(rel)
    assert cleaned is not None
    n = cleaned.count("*").fetchone()[0]
    print(f"  clean: {n:,} rows")
    assert n > 0


def test_standardize():
    """sus_data_standardize renames columns using dictionaries."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    std = cs.sus_data_standardize(rel, system="SIM-DO")
    print(f"  standardize: cols = {std.columns[:5]}...")
    assert std is not None


def test_filter_cid():
    """sus_filter by CID codes."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    std = cs.sus_data_standardize(rel, system="SIM-DO")
    # Filter dengue codes
    filtered = cs.sus_filter(std, codes=["A90", "A91"])
    nrows = filtered.count("*").fetchone()[0]
    print(f"  filter(A90-A91): {nrows:,} rows")


def test_quality():
    """sus_data_quality_report returns a report dict."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    report = cs.sus_data_quality_report(rel)
    assert isinstance(report, dict)
    assert "total_rows" in report
    assert report["total_rows"] > 0
    print(f"  quality: {report['total_rows']:,} rows, {report['total_cols']} cols")


def test_export_parquet(tmp_path):
    """sus_export writes parquet."""
    rel = cs.sus_data_import("SIM-DO", "SP", 2023, path=str(PARQUET))
    out = tmp_path / "test_export.parquet"
    cs.sus_export(rel, str(out), fmt="parquet")
    assert out.exists()
    assert out.stat().st_size > 0
    print(f"  export: {out.stat().st_size:,} bytes")


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "-s"])
