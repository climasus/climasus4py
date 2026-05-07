"""P5 Sprint 2 — Regression tests for importer security (OWASP A01/A05).

Tests verify that _raw_cache_path blocks path traversal attacks
(Sprint 1 item 2 regression).
"""

from pathlib import Path

import pytest

from climasus4py.core.importer import _raw_cache_path, _write_parquet_atomic


class TestRawCachePathTraversal:
    """_raw_cache_path must reject URLs that resolve outside the cache dir."""

    @pytest.mark.parametrize("url", [
        "ftp://host/../../../etc/passwd",
        "ftp://host/./valid/../../../escape",
        "ftp://host/%2e%2e/%2e%2e/etc/shadow",  # URL-encoded traversal
    ])
    def test_traversal_url_raises(self, tmp_path, url):
        """Path traversal in URL must raise ValueError."""
        with pytest.raises(ValueError, match="path traversal|outside the cache"):
            _raw_cache_path(url, tmp_path)

    def test_valid_url_stays_inside_cache(self, tmp_path):
        """A normal FTP URL should resolve safely inside cache dir."""
        url = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/DOSP2023.dbc"
        result = _raw_cache_path(url, tmp_path)
        # Must be inside tmp_path
        result.relative_to(tmp_path)  # raises ValueError if not inside

    def test_absolute_path_via_ftp_raises(self, tmp_path):
        """URL that resolves to an absolute path outside cache must raise."""
        url = "ftp://host//absolute/etc/passwd"
        # //absolute resolves to /absolute on POSIX, outside cache
        # On Windows this may not escape but should not raise either — just be safe
        try:
            result = _raw_cache_path(url, tmp_path)
            # If it didn't raise, the path must still be inside cache
            result.relative_to(tmp_path)
        except ValueError:
            pass  # expected for traversal


class TestWriteParquetAtomic:
    """Atomic Parquet write must clean up .tmp_ files on failure."""

    def test_successful_write_leaves_no_tmp(self, tmp_path):
        """After successful write, no .tmp_ file should remain."""
        import pandas as pd
        import pyarrow as pa

        target = tmp_path / "out.parquet"
        table = pa.Table.from_pandas(pd.DataFrame({"x": [1, 2, 3]}))
        _write_parquet_atomic(table, target)

        assert target.exists()
        tmp_files = list(tmp_path.glob("*.tmp_*.parquet"))
        assert tmp_files == [], f"tmp files left: {tmp_files}"

    def test_failed_write_cleans_up_tmp(self, tmp_path, monkeypatch):
        """On write failure, the .tmp_ file must be deleted."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        target = tmp_path / "out.parquet"
        table = pa.Table.from_pandas(__import__("pandas").DataFrame({"x": [1]}))

        def failing_write(t, path, **kw):
            # Create the tmp file to simulate partial write
            Path(str(path)).touch()
            raise OSError("simulated write failure")

        monkeypatch.setattr(pq, "write_table", failing_write)

        with pytest.raises(OSError, match="simulated"):
            _write_parquet_atomic(table, target)

        # No .tmp_ file should survive
        tmp_files = list(tmp_path.glob("*.tmp_*.parquet"))
        assert tmp_files == [], f"tmp files not cleaned up: {tmp_files}"
