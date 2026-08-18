"""Tests for sus_climate_normals / sus_climate_normals_meta.

Strategy: monkeypatch _download_normal_var to return a synthetic tidy
DataFrame so the public API body (validation, filtering, metadata
assembly) is exercised without any network I/O. The real catalogue
(climasus-data's metadata/inmet_normals.json) is used as-is since it's
a bundled dependency, not a network call.
"""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.core.climate_normals import (
    _get_normal_meta,
    _make_unique,
    _slugify_month,
    sus_climate_normals,
    sus_climate_normals_meta,
)

_PATCH_TARGET = "climasus4py.core.climate_normals._download_normal_var"


def _make_var_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame({
        "codigo": ["A701"] * n,
        "nome_estacao": ["SAO PAULO"] * n,
        "uf": ["SP"] * n,
        "mes": ["janeiro", "janeiro", "fevereiro", "fevereiro"][:n],
        "decada": ["1", "2", "1", "2"][:n],
        "valor": [25.0, 26.0, 27.0, 28.0][:n],
        "var_code": ["t_max"] * n,
    })


class TestValidation:
    def test_invalid_period_raises(self):
        with pytest.raises(ValueError, match="Invalid 'period'"):
            sus_climate_normals(period="1900-2000")

    def test_invalid_lang_warns_and_defaults_to_pt(self, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_var_df())
        with pytest.warns(UserWarning, match="não suportado"):
            df = sus_climate_normals(target_var="t_max", lang="fr", verbose=False)
        assert not df.empty

    def test_unknown_target_var_raises(self):
        with pytest.raises(ValueError, match="None of the requested"):
            sus_climate_normals(target_var="not_a_real_code", verbose=False)


class TestHappyPath:
    def test_returns_tidy_dataframe_with_metadata(self, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: _make_var_df())
        df = sus_climate_normals(period="1991-2020", target_var="t_max", verbose=False)

        assert set(df.columns) >= {
            "codigo", "nome_estacao", "uf", "mes", "decada", "valor",
            "var_code", "variable_pt", "variable_en", "variable_es", "period",
        }
        assert (df["period"] == "1991-2020").all()

        meta = df.attrs["sus_meta"]
        assert meta["stage"] == "climate"
        assert meta["type"] == "normals"
        assert meta["period"] == "1991-2020"
        assert meta["n_observations"] == len(df)

    def test_no_data_raises(self, monkeypatch):
        monkeypatch.setattr(_PATCH_TARGET, lambda **kw: None)
        with pytest.raises(ValueError, match="No data downloaded|Nenhum dado"):
            sus_climate_normals(target_var="t_max", verbose=False)


class TestMeta:
    def test_get_normal_meta_matches_climasus_data_schema(self):
        meta = _get_normal_meta()
        expected = {
            "var_code", "period", "variable_pt", "variable_en", "variable_es", "code_link",
        }
        assert expected <= set(meta.columns)
        assert set(meta["period"].unique()) <= {"1961-1990", "1981-2010", "1991-2020"}

    def test_sus_climate_normals_meta_filters_by_period(self):
        out = sus_climate_normals_meta(period="1991-2020")
        expected_cols = ["var_code", "variable_label", "period", "var_slug", "code_link"]
        assert list(out.columns) == expected_cols
        assert (out["period"] == "1991-2020").all()

    def test_sus_climate_normals_meta_label_language(self):
        pt = sus_climate_normals_meta(period="1991-2020", lang="pt")
        en = sus_climate_normals_meta(period="1991-2020", lang="en")
        assert not pt["variable_label"].equals(en["variable_label"])


class TestSlugifyAndMakeUnique:
    def test_slugify_month_transliterates_and_lowercases(self):
        assert _slugify_month("JANEIRO") == "janeiro"
        assert _slugify_month("Precipitação Média") == "precipitacao_media"

    def test_make_unique_suffixes_duplicates(self):
        assert _make_unique(["a", "a", "b", "a"], sep="_d") == ["a", "a_d1", "b", "a_d2"]
