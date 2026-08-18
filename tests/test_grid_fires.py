"""Tests for sus_grid_fires.

Strategy: monkeypatch _fires_download_month to write synthetic CSV cache
files directly (mirroring INPE/FIRMS raw schemas), so the public API body
(validation, read+normalize, point-in-polygon spatial join, aggregation,
metadata assembly) is exercised without any network I/O.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.grid_fires import (
    _fires_firms_product,
    _fires_normalize_cols,
    _fires_uf_bbox,
    sus_grid_fires,
)

_PATCH_TARGET = "climasus4py.enrichment.grid_fires._fires_download_month"


def _fake_download_factory(inpe_rows: dict[str, pd.DataFrame]):
    """Return a fake _fires_download_month that writes pre-baked CSVs.

    *inpe_rows* maps "{year}_{month:02d}" -> DataFrame of raw INPE rows.
    Months with no entry get an empty sentinel file (mirroring R's
    "no fires this period" cache behavior).
    """

    def _fake(
        source, date_start, date_end, cache_path, uf, biome, bbox,
        firms_key, use_cache, verbose, msg,
    ):
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        key = date_start[:7].replace("-", "_")
        df = inpe_rows.get(key)
        if df is not None and not df.empty:
            df.to_csv(cache_path, index=False, encoding="utf-8")
        else:
            cache_path.write_text("", encoding="utf-8")
        return cache_path

    return _fake


@pytest.fixture()
def inpe_fixture() -> dict[str, pd.DataFrame]:
    return {
        "2020_07": pd.DataFrame({
            "lat": [-10.0, -10.1, -11.0],
            "lon": [-55.0, -55.1, -56.0],
            "datahora": [
                "2020-07-01T12:00:00", "2020-07-01T13:00:00", "2020-07-02T10:00:00",
            ],
            "frp": [12.5, 8.0, None],
            "bioma": ["Amazonia", "Amazonia", "Cerrado"],
            "estado": ["MT", "MT", "MT"],
        }),
    }


@pytest.fixture()
def patched_download(monkeypatch, inpe_fixture):
    monkeypatch.setattr(_PATCH_TARGET, _fake_download_factory(inpe_fixture))
    return inpe_fixture


@pytest.fixture()
def mt_municipalities():
    """Two municipality squares covering the fixture's fire points."""
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import box

    return gpd.GeoDataFrame(
        {"code_muni": ["5100102", "5100103"]},
        geometry=[
            box(-55.5, -10.5, -54.5, -9.5),  # covers (-55.0,-10.0), (-55.1,-10.1)
            box(-56.5, -11.5, -55.5, -10.5),  # covers (-56.0,-11.0)
        ],
        crs="EPSG:4326",
    )


class TestValidation:
    def test_missing_years_raises(self, patched_download, tmp_path):
        with pytest.raises(TypeError):
            sus_grid_fires(cache_dir=tmp_path, verbose=False)  # type: ignore[call-arg]

    def test_invalid_lang_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="'lang' must be one of"):
            sus_grid_fires(years=2020, lang="fr", cache_dir=tmp_path, verbose=False)

    def test_invalid_source_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="'source' must be one of"):
            sus_grid_fires(years=2020, source="modis", cache_dir=tmp_path, verbose=False)

    def test_year_below_min_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="must be >= 1998"):
            sus_grid_fires(years=1990, cache_dir=tmp_path, verbose=False)

    def test_invalid_months_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="'months' must be integers"):
            sus_grid_fires(years=2020, months=[0, 13], cache_dir=tmp_path, verbose=False)

    def test_invalid_uf_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="Invalid 'uf'"):
            sus_grid_fires(years=2020, uf="XX", cache_dir=tmp_path, verbose=False)

    def test_invalid_biome_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="Invalid 'biome'"):
            sus_grid_fires(years=2020, biome="Savanna", cache_dir=tmp_path, verbose=False)

    def test_firms_without_key_raises(self, monkeypatch, patched_download, tmp_path):
        monkeypatch.delenv("FIRMS_MAP_KEY", raising=False)
        with pytest.raises(ValueError, match="FIRMS MAP KEY"):
            sus_grid_fires(
                years=2020, source="firms_modis", cache_dir=tmp_path, verbose=False
            )

    def test_muni_not_geodataframe_raises(self, patched_download, tmp_path):
        pytest.importorskip("geopandas")
        with pytest.raises(ValueError, match="GeoDataFrame"):
            sus_grid_fires(
                years=2020, months=[7], municipalities=pd.DataFrame({"code_muni": ["1"]}),
                cache_dir=tmp_path, verbose=False,
            )


class TestRawPoints:
    def test_returns_raw_points_with_metadata(self, patched_download, tmp_path):
        df = sus_grid_fires(years=2020, months=[7], cache_dir=tmp_path, verbose=False)

        assert list(df.columns) == ["date", "lat", "lon", "frp", "biome", "estado", "source"]
        assert len(df) == 3
        assert (df["source"] == "inpe").all()

        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "fires"
        assert meta["spatial"] is False
        assert meta["n_raw_points"] == 3
        assert meta["n_observations"] == 3

    def test_no_data_raises(self, patched_download, tmp_path):
        with pytest.raises(ValueError, match="Nenhum dado de foco"):
            sus_grid_fires(years=2020, months=[1], cache_dir=tmp_path, verbose=False)


class TestMunicipalityAggregation:
    def test_aggregates_to_muni_day_counts(
        self, patched_download, mt_municipalities, tmp_path
    ):
        df = sus_grid_fires(
            years=2020, months=[7], municipalities=mt_municipalities,
            cache_dir=tmp_path, verbose=False,
        )

        assert list(df.columns) == ["code_muni", "date", "n_fires", "frp_mean"]
        assert set(df["code_muni"]) == {"5100102", "5100103"}

        row_5100102 = df[df["code_muni"] == "5100102"]
        assert row_5100102["n_fires"].sum() == 2
        # frp_mean over [12.5, 8.0] on the same day
        assert row_5100102["frp_mean"].iloc[0] == pytest.approx(10.25)

        row_5100103 = df[df["code_muni"] == "5100103"]
        assert row_5100103["n_fires"].iloc[0] == 1
        assert row_5100103["frp_mean"].isna().iloc[0]  # frp was None for this point

        meta = df.attrs["sus_meta"]
        assert meta["n_observations"] == len(df)
        assert meta["spatial"] is False  # preserved R quirk

    def test_no_fires_in_polygons_returns_empty(self, patched_download, tmp_path):
        gpd = pytest.importorskip("geopandas")
        from shapely.geometry import box

        far_away = gpd.GeoDataFrame(
            {"code_muni": ["9999999"]},
            geometry=[box(10, 10, 11, 11)],
            crs="EPSG:4326",
        )
        df = sus_grid_fires(
            years=2020, months=[7], municipalities=far_away,
            cache_dir=tmp_path, verbose=False,
        )
        assert len(df) == 0
        assert list(df.columns) == ["code_muni", "date", "n_fires", "frp_mean"]
        assert df.attrs["sus_meta"]["n_observations"] == 0

    def test_agg_fun_value_is_ignored_like_r(
        self, patched_download, mt_municipalities, tmp_path
    ):
        # Preserved R quirk: 'agg_fun' is accepted but never read in the R
        # source — any value (even a nonsense one) still produces the same
        # count + frp_mean aggregation, no error raised.
        df = sus_grid_fires(
            years=2020, months=[7], municipalities=mt_municipalities,
            agg_fun="frp_sum", cache_dir=tmp_path, verbose=False,
        )
        assert list(df.columns) == ["code_muni", "date", "n_fires", "frp_mean"]
        assert df["n_fires"].sum() == 3


class TestGridJoinCompatibility:
    def test_output_joins_with_sus_grid_join(
        self, patched_download, mt_municipalities, tmp_path
    ):
        """Verify the real downstream contract: sus_grid_join merges on
        (code_muni, date) with a plain pandas .merge(), so dtypes must
        match exactly (not just column names)."""
        from climasus4py.enrichment.grid_join import sus_grid_join

        fires = sus_grid_fires(
            years=2020, months=[7], municipalities=mt_municipalities,
            cache_dir=tmp_path, verbose=False,
        )
        health = pd.DataFrame({
            "code_muni": ["5100102", "5100103"],
            "date": pd.to_datetime(["2020-07-01", "2020-07-02"]),
            "n_cases": [3, 1],
        })
        joined = sus_grid_join(health, fires, verbose=False)
        assert joined.loc[joined["code_muni"] == "5100102", "n_fires"].iloc[0] == 2
        assert joined.loc[joined["code_muni"] == "5100103", "n_fires"].iloc[0] == 1
        assert joined["n_fires"].notna().all()


class TestInternalHelpers:
    def test_uf_bbox_merges_states(self):
        bbox = _fires_uf_bbox(["MT", "PA"])
        assert bbox[0] <= -61 and bbox[2] >= -46

    def test_firms_product_names(self):
        assert _fires_firms_product("firms_modis") == "MODIS_C6_1"
        assert _fires_firms_product("firms_viirs") == "VIIRS_SNPP_SP"

    def test_normalize_cols_inpe(self):
        df = pd.DataFrame({
            "lat": ["-10.0"], "lon": ["-55.0"],
            "datahora": ["2020-07-01T12:00:00"], "frp": ["12.5"], "bioma": ["Amazonia"],
        })
        out = _fires_normalize_cols(df, "inpe")
        assert out["date"].iloc[0] == pd.Timestamp("2020-07-01")
        assert out["biome"].iloc[0] == "Amazonia"

    def test_normalize_cols_firms(self):
        df = pd.DataFrame({
            "latitude": [-10.0], "longitude": [-55.0],
            "acq_date": ["2020-07-01"], "frp": [9.9],
        })
        out = _fires_normalize_cols(df, "firms_modis")
        assert out["lat"].iloc[0] == -10.0
        assert out["lon"].iloc[0] == -55.0
        assert out["date"].iloc[0] == pd.Timestamp("2020-07-01")

    def test_firms_chunking_splits_month_into_10day_periods(self, monkeypatch):
        """A 31-day month must be split into 10/10/10/1-day chunks,
        matching R's .fires_fetch_firms() while-loop."""
        from climasus4py.enrichment import grid_fires

        seen_urls: list[str] = []

        def _fake_get(url, params=None, timeout=60, max_retries=3):
            seen_urls.append(url)
            return "latitude,longitude,acq_date,frp\n"

        monkeypatch.setattr(grid_fires, "_http_get_text", _fake_get)
        grid_fires._fires_fetch_firms(
            "2020-07-01", "2020-07-31", (-60, -20, -50, -10), "KEY", "MODIS_C6_1"
        )

        assert len(seen_urls) == 4
        starts = [url.rsplit("/", 1)[-1] for url in seen_urls]
        days = [url.rsplit("/", 2)[-2] for url in seen_urls]
        assert starts == ["2020-07-01", "2020-07-11", "2020-07-21", "2020-07-31"]
        assert days == ["10", "10", "10", "1"]
