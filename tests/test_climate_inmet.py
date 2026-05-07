"""P6 Sprint 2 — Coverage tests for sus_climate_inmet (climate_inmet.py target ≥ 50%).

Strategy: monkeypatch _download_inmet to return a synthetic DataFrame so that
the public API body (parameter validation, station_code filter, metadata) is
fully exercised without actual network I/O.
Additional tests cover _download_robust with mocked requests.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from climasus4py.core.climate_inmet import (
    _VALID_UFS,
    sus_climate_inmet,
)


# ---------------------------------------------------------------------------
# Shared fixture — minimal INMET-like DataFrame
# ---------------------------------------------------------------------------

def _make_inmet_df(n: int = 10) -> pd.DataFrame:
    """Synthetic INMET observation data sufficient for all smoke tests."""
    dates = pd.date_range("2023-01-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame({
        "station_code": (["A701"] * (n // 2)) + (["A702"] * (n - n // 2)),
        "station_name": ["SAO PAULO"] * n,
        "region": ["SUDESTE"] * n,
        "UF": ["SP"] * n,
        "latitude": [-23.55] * n,
        "longitude": [-46.63] * n,
        "altitude": [760.0] * n,
        "date": dates,
        "year": [2023] * n,
        "tair_dry_bulb_c": [22.5] * n,
        "rainfall_mm": [0.0] * n,
        "rh_mean_porc": [75.0] * n,
    })


# ---------------------------------------------------------------------------
# Helper that patches _download_inmet inside the module
# ---------------------------------------------------------------------------

_PATCH_TARGET = "climasus4py.core.climate_inmet._download_inmet"


# ---------------------------------------------------------------------------
# Parameter validation — covered without any network call
# ---------------------------------------------------------------------------

class TestParameterValidation:
    def test_invalid_year_raises(self):
        with pytest.raises(ValueError, match="Invalid values in 'years'"):
            sus_climate_inmet(years=1999)

    def test_future_year_raises(self, monkeypatch):
        future = datetime.datetime.now().year + 1
        with pytest.raises(ValueError, match="Invalid values in 'years'"):
            sus_climate_inmet(years=future)

    def test_invalid_lang_raises(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.core.climate_inmet._download_inmet",
            lambda **kw: _make_inmet_df(),
        )
        with pytest.raises(ValueError, match="'lang' must be one of"):
            sus_climate_inmet(years=2023, lang="de")  # type: ignore[arg-type]

    def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="Invalid values in 'uf'"):
            sus_climate_inmet(years=2023, uf="XX")

    def test_multi_uf_with_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid values in 'uf'"):
            sus_climate_inmet(years=2023, uf=["SP", "XX"])


class TestSuccessfulImport:
    """Tests that exercise the full sus_climate_inmet body via mocked download."""

    @pytest.fixture
    def df_out(self, tmp_path, monkeypatch):
        """Patch _download_inmet and call sus_climate_inmet."""
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df())
        return sus_climate_inmet(
            years=2023, uf="SP", cache_dir=tmp_path, verbose=False
        )

    def test_returns_dataframe(self, df_out):
        assert isinstance(df_out, pd.DataFrame)

    def test_has_rows(self, df_out):
        assert len(df_out) == 10

    def test_has_expected_columns(self, df_out):
        assert "tair_dry_bulb_c" in df_out.columns
        assert "station_code" in df_out.columns

    def test_sus_meta_attached(self, df_out):
        assert "sus_meta" in df_out.attrs

    def test_sus_meta_has_years(self, df_out):
        assert df_out.attrs["sus_meta"]["years"] == [2023]

    def test_sus_meta_has_n_stations(self, df_out):
        # 2 synthetic stations
        assert df_out.attrs["sus_meta"]["n_stations"] == 2

    def test_sus_meta_has_temporal_coverage(self, df_out):
        meta = df_out.attrs["sus_meta"]
        assert "temporal_coverage" in meta
        assert meta["temporal_coverage"]["start"] is not None

    def test_multi_year_range(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df(20))
        df = sus_climate_inmet(
            years=[2022, 2023], cache_dir=tmp_path, verbose=False
        )
        assert isinstance(df, pd.DataFrame)

    def test_years_as_range(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df(5))
        df = sus_climate_inmet(
            years=range(2021, 2023), cache_dir=tmp_path, verbose=False
        )
        assert len(df) == 5


class TestStationCodeFilter:
    """station_code parameter narrows the returned data."""

    def test_valid_station_code_filters(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df(10))
        df = sus_climate_inmet(
            years=2023, station_code="A701", cache_dir=tmp_path, verbose=False
        )
        assert (df["station_code"] == "A701").all()
        assert len(df) == 5  # half the rows

    def test_station_code_no_match_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df(10))
        with pytest.raises(ValueError, match="No observations found|Nenhuma"):
            sus_climate_inmet(
                years=2023, station_code="Z999", cache_dir=tmp_path, verbose=False
            )

    def test_station_code_list(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df(10))
        df = sus_climate_inmet(
            years=2023, station_code=["A701", "A702"],
            cache_dir=tmp_path, verbose=False,
        )
        assert len(df) == 10  # both codes kept


class TestLanguageMessages:
    """Verify the 3 language paths do not raise."""

    @pytest.mark.parametrize("lang", ["pt", "en", "es"])
    def test_lang_does_not_raise(self, lang, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df())
        df = sus_climate_inmet(
            years=2023, lang=lang, cache_dir=tmp_path, verbose=True
        )
        assert isinstance(df, pd.DataFrame)


class TestDefaultYears:
    """When years=None, the function defaults to last 2 years."""

    def test_years_none_defaults(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_inmet_df())
        df = sus_climate_inmet(years=None, cache_dir=tmp_path, verbose=False)
        meta = df.attrs["sus_meta"]
        current = datetime.datetime.now().year
        assert current - 1 in meta["years"]
        assert current in meta["years"]


class TestDownloadRobust:
    """Unit tests for _download_robust — mock requests so no I/O occurs."""

    def test_requests_success(self, tmp_path):
        from climasus4py.core.climate_inmet import _download_robust

        dest = tmp_path / "out.zip"
        fake_chunk = b"PKZIP_DATA"

        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.status_code = 200
        mock_response.iter_content.return_value = iter([fake_chunk])

        mock_requests = MagicMock()
        mock_requests.get.return_value = mock_response

        import sys
        sys.modules["requests"] = mock_requests

        try:
            ok, reason = _download_robust("https://example.com/x.zip", dest, max_retries=1)
            assert ok is True
            assert reason is None
        finally:
            del sys.modules["requests"]

    def test_requests_http_404_permanent(self, tmp_path):
        from climasus4py.core.climate_inmet import _download_robust

        dest = tmp_path / "out.zip"
        mock_response = MagicMock()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.status_code = 404

        mock_requests = MagicMock()
        mock_requests.get.return_value = mock_response

        import sys
        sys.modules["requests"] = mock_requests

        try:
            ok, reason = _download_robust("https://example.com/x.zip", dest, max_retries=1)
            assert ok is False
            assert reason is not None
            assert "404" in reason
        finally:
            del sys.modules["requests"]

    def test_no_requests_falls_through(self, tmp_path, monkeypatch):
        """When requests, curl, wget all fail, returns (False, reason)."""
        from climasus4py.core.climate_inmet import _download_robust
        from climasus4py.core import climate_inmet as _ci_mod

        dest = tmp_path / "out.zip"

        monkeypatch.setattr(
            _ci_mod, "subprocess",
            MagicMock(run=MagicMock(return_value=MagicMock(returncode=1, stdout="0", stderr="")))
        )
        import sys
        original = sys.modules.get("requests")
        sys.modules["requests"] = None  # type: ignore

        try:
            ok, reason = _download_robust("https://example.com/x.zip", dest, max_retries=1, verbose=False)
            assert ok is False
        finally:
            if original is None:
                sys.modules.pop("requests", None)
            else:
                sys.modules["requests"] = original
