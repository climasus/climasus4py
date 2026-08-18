"""Tests for sus_data_cid_select (interactive disease groups explorer)."""

import pandas as pd
import pytest

from climasus4py.utils.cid_select import sus_data_cid_select

_EXPECTED_COLUMNS = {
    "name",
    "icd_codes",
    "climate_factors",
    "description",
    "category",
    "climate_sensitive",
}


class TestConsoleOutput:
    def test_returns_dataframe_with_expected_columns(self):
        df = sus_data_cid_select(output="console", verbose=False)
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == _EXPECTED_COLUMNS
        assert len(df) > 0

    def test_dengue_group_present_with_codes(self):
        df = sus_data_cid_select(output="console", verbose=False)
        dengue = df[df["name"] == "dengue"]
        assert len(dengue) == 1
        assert "A90" in dengue.iloc[0]["icd_codes"]

    def test_filter_climate_keeps_only_climate_sensitive(self):
        df = sus_data_cid_select(output="console", filter_climate=True, verbose=False)
        assert len(df) > 0
        assert df["climate_sensitive"].all()

    def test_verbose_prints_summary(self, capsys):
        sus_data_cid_select(output="console", verbose=True)
        captured = capsys.readouterr()
        assert "Disease Groups Explorer" in captured.out

    def test_result_usable_with_sus_filter_groups(self):
        # The returned "name" column is exactly what sus_filter(groups=...) expects.
        df = sus_data_cid_select(output="console", verbose=False)
        assert "dengue" in df["name"].tolist()


class TestValidation:
    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="invalid lang"):
            sus_data_cid_select(lang="fr")

    def test_invalid_output_raises(self):
        with pytest.raises(ValueError, match="invalid output"):
            sus_data_cid_select(output="shiny")


class TestBrowserOutput:
    def test_opens_browser_and_returns_dataframe(self, monkeypatch):
        opened_urls = []
        monkeypatch.setattr(
            "climasus4py.utils.cid_select.webbrowser.open",
            lambda url: opened_urls.append(url),
        )

        df = sus_data_cid_select(output="browser", verbose=False)

        assert isinstance(df, pd.DataFrame)
        assert len(opened_urls) == 1
        assert opened_urls[0].startswith("file://")
        assert opened_urls[0].endswith(".html")

    def test_html_contains_category_and_cid_codes(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.utils.cid_select.webbrowser.open", lambda url: None
        )

        captured_html = {}
        from climasus4py.utils import cid_select as mod

        original = mod._generate_disease_groups_html

        def spy(groups_df, lang):
            html = original(groups_df, lang)
            captured_html["html"] = html
            return html

        monkeypatch.setattr(mod, "_generate_disease_groups_html", spy)

        sus_data_cid_select(output="browser", lang="en", verbose=False)

        html = captured_html["html"]
        assert "Infectious" in html
        assert "A90" in html  # dengue ICD-10 code
        assert "dengue" in html
        assert "<html" in html and "</html>" in html

    def test_html_in_portuguese(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.utils.cid_select.webbrowser.open", lambda url: None
        )
        from climasus4py.utils.cid_select import (
            _generate_disease_groups_html,
            _get_disease_groups_data,
        )

        df = _get_disease_groups_data("pt", filter_climate=False)
        html = _generate_disease_groups_html(df, "pt")
        assert "Explorador de Grupos de Doencas" in html
