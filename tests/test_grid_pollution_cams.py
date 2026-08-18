"""Tests for sus_grid_pollution_cams.

Unlike sus_grid_chirps/era5/pdsi, this function needs no geopandas/
rioxarray/exactextract — the Zenodo files are already pre-aggregated to
municipality level. All tests here run fully offline in the base
environment by monkeypatching the download step (``_cams_download_file``)
to write a synthetic Parquet file instead of hitting the network.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.grid_pollution_cams import (
    _CAMS_ZENODO_IDS,
    _VALID_METRICS,
    _build_manifest,
    sus_grid_pollution_cams,
)

_DL_TARGET = "climasus4py.enrichment.grid_pollution_cams._cams_download_file"


def _make_fake_download(frames: dict[str, pd.DataFrame]):
    """Return a fake `_cams_download_file` that writes a real Parquet file.

    *frames* maps filename -> the DataFrame to write for that filename,
    keyed by the manifest entry's cache_path.name.
    """

    def _fake(url, cache_path, use_cache, verbose, msg):
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df = frames.get(cache_path.name)
        if df is not None:
            df.to_parquet(cache_path, index=False)

    return _fake


def _synthetic_frame(out_value: float = 1.5) -> pd.DataFrame:
    return pd.DataFrame({
        "code_muni": ["3106200", "3550308"],
        "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
        "value": [out_value, out_value * 2],
    })


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="lang"):
            sus_grid_pollution_cams(lang="fr", verbose=False)

    def test_invalid_pollutants_raises(self):
        with pytest.raises(ValueError, match="pollutants"):
            sus_grid_pollution_cams(pollutants=["not_a_pollutant"], verbose=False)

    def test_empty_pollutants_raises(self):
        with pytest.raises(ValueError, match="pollutants"):
            sus_grid_pollution_cams(pollutants=[], verbose=False)

    def test_invalid_metric_raises(self):
        with pytest.raises(ValueError, match="metric"):
            sus_grid_pollution_cams(metric="bogus", verbose=False)

    def test_invalid_years_range_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_pollution_cams(years=1999, verbose=False)

    def test_invalid_years_range_above_max_raises(self):
        with pytest.raises(ValueError, match="years"):
            sus_grid_pollution_cams(years=2030, verbose=False)

    def test_invalid_use_cache_raises(self):
        with pytest.raises(ValueError, match="use_cache"):
            sus_grid_pollution_cams(use_cache="yes", verbose=False)

    def test_invalid_cache_dir_raises(self):
        with pytest.raises(ValueError, match="cache_dir"):
            sus_grid_pollution_cams(cache_dir="", verbose=False)


class TestConstants:
    def test_zenodo_ids_cover_six_pollutants(self):
        assert set(_CAMS_ZENODO_IDS) == {"pm25", "pm10", "co", "o3", "no2", "so2"}

    def test_valid_metrics(self):
        assert _VALID_METRICS == ("mean", "max", "min")

    def test_manifest_filename_always_ends_in_mean(self, tmp_path):
        """Filename pattern transcribed verbatim from the R source: the
        trailing "_mean" is invariant across metric values (see
        IDEIAS.md for the unverified-naming-convention caveat)."""
        manifest = _build_manifest(["pm25"], ["max"], tmp_path)
        assert manifest[0]["filename"] == "pm25_max_mean.parquet"
        assert manifest[0]["out_col"] == "pm25_max"
        assert manifest[0]["url"] == (
            "https://zenodo.org/records/16374139/files/pm25_max_mean.parquet?download=1"
        )

    def test_all_expands_to_every_combination(self, tmp_path):
        manifest = _build_manifest(list(_CAMS_ZENODO_IDS), list(_VALID_METRICS), tmp_path)
        assert len(manifest) == 18


class TestHappyPath:
    def test_single_pollutant_metric(self, monkeypatch, tmp_path):
        frames = {"pm25_mean_mean.parquet": _synthetic_frame(10.0)}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25"], metric="mean", cache_dir=tmp_path, verbose=False,
        )

        assert list(df.columns) == ["code_muni", "date", "pm25_mean"]
        assert len(df) == 2
        assert df["pm25_mean"].tolist() == [10.0, 20.0]

    def test_multiple_pollutants_full_outer_join(self, monkeypatch, tmp_path):
        frames = {
            "pm25_mean_mean.parquet": _synthetic_frame(10.0),
            "no2_mean_mean.parquet": _synthetic_frame(5.0),
        }
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25", "no2"], metric="mean", cache_dir=tmp_path, verbose=False,
        )

        assert set(df.columns) == {"code_muni", "date", "pm25_mean", "no2_mean"}
        assert len(df) == 2

    def test_years_filter_drops_out_of_range_rows(self, monkeypatch, tmp_path):
        multi_year = pd.DataFrame({
            "code_muni": ["3106200", "3106200"],
            "date": pd.to_datetime(["2020-01-01", "2021-01-01"]),
            "value": [1.0, 2.0],
        })
        frames = {"pm25_mean_mean.parquet": multi_year}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25"], metric="mean", years=[2021],
            cache_dir=tmp_path, verbose=False,
        )
        assert len(df) == 1
        assert df["date"].iloc[0] == pd.Timestamp("2021-01-01")

    def test_missing_cache_file_is_skipped_not_fatal(self, monkeypatch, tmp_path):
        """One pollutant's download fails (empty file never written); the
        other still produces a result — mirrors R's per-file tryCatch."""
        frames = {"pm25_mean_mean.parquet": _synthetic_frame(1.0)}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25", "no2"], metric="mean", cache_dir=tmp_path, verbose=False,
        )
        assert "pm25_mean" in df.columns
        assert "no2_mean" not in df.columns

    def test_bad_schema_file_skipped_not_fatal(self, monkeypatch, tmp_path):
        """A file with no recognizable id/date column degrades to a
        skipped-with-warning file (mirrors R's cli_abort-inside-tryCatch
        behavior), not a fatal error, as long as another file succeeds."""
        bad_schema_df = pd.DataFrame({"foo": [1], "bar": [2]})
        frames = {
            "pm25_mean_mean.parquet": bad_schema_df,
            "no2_mean_mean.parquet": _synthetic_frame(1.0),
        }
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25", "no2"], metric="mean", cache_dir=tmp_path, verbose=False,
        )
        assert "pm25_mean" not in df.columns
        assert "no2_mean" in df.columns

    def test_no_data_at_all_raises(self, monkeypatch, tmp_path):
        monkeypatch.setattr(_DL_TARGET, _make_fake_download({}))
        with pytest.raises(ValueError, match="No data|Nenhum dado|No se"):
            sus_grid_pollution_cams(pollutants=["pm25"], cache_dir=tmp_path, verbose=False)

    def test_metadata_assembly(self, monkeypatch, tmp_path):
        frames = {"pm25_mean_mean.parquet": _synthetic_frame(1.0)}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25"], metric="mean", cache_dir=tmp_path, verbose=False,
        )
        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "pollution_cams"
        assert meta["pollutants"] == ["pm25"]
        assert meta["metric"] == ["mean"]
        assert meta["n_municipalities"] == 2
        assert meta["n_observations"] == 2
        assert meta["temporal"]["source"] == "zenodo_cams"

    def test_default_years_metadata_is_full_range(self, monkeypatch, tmp_path):
        frames = {"pm25_mean_mean.parquet": _synthetic_frame(1.0)}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25"], metric="mean", cache_dir=tmp_path, verbose=False,
        )
        assert df.attrs["sus_meta"]["years"] == list(range(2003, 2025))

    def test_alternate_value_column_name_matched_by_metric(self, monkeypatch, tmp_path):
        """Simulates a Parquet file with an ambiguous value column name
        (e.g. two candidate value columns) — the one containing the
        out_col substring should be picked over the first column."""
        df_src = pd.DataFrame({
            "code_muni": ["3106200"],
            "date": pd.to_datetime(["2020-01-01"]),
            "pm25_other": [999.0],
            "pm25_mean": [7.0],
        })
        frames = {"pm25_mean_mean.parquet": df_src}
        monkeypatch.setattr(_DL_TARGET, _make_fake_download(frames))

        df = sus_grid_pollution_cams(
            pollutants=["pm25"], metric="mean", cache_dir=tmp_path, verbose=False,
        )
        assert df["pm25_mean"].iloc[0] == 7.0
