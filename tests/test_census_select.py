"""Tests for sus_census_select (interactive Census variables explorer)."""

import pandas as pd
import pytest

from climasus4py.utils.census_select import sus_census_select

_EXPECTED_COLUMNS = {
    "code",
    "name",
    "dataset",
    "type",
    "category",
    "has_categories",
    "n_categories",
    "year",
}

# Small synthetic dictionary standing in for the (not-yet-shipped)
# climasus-data census dictionaries. Keys mirror the real shape:
# {"columns": {code: name}, "values": {code: [labels]}}.
_FAKE_DICTS = {
    "population": {
        "columns": {"V0601": "sexo", "V0606": "cor_raca", "M0601": "marcador_sexo"},
        "values": {"V0601": ["masculino", "feminino"]},
    },
    "households": {
        "columns": {"V0201": "domicilio_agua"},
        "values": {},
    },
}


def _fake_loader(dataset, lang):
    return _FAKE_DICTS.get(dataset)


@pytest.fixture(autouse=True)
def _patch_dictionaries(monkeypatch):
    monkeypatch.setattr(
        "climasus4py.utils.census_select._load_census_dictionary", _fake_loader
    )


class TestCodesOutput:
    def test_returns_list_of_unique_codes(self):
        codes = sus_census_select(dataset="population", output="codes", verbose=False)
        assert isinstance(codes, list)
        assert set(codes) == {"V0601", "V0606", "M0601"}

    def test_all_datasets_dedups_codes(self):
        codes = sus_census_select(dataset="all", output="codes", verbose=False)
        # Only population + households have fake dicts among the 5 loaded.
        assert set(codes) == {"V0601", "V0606", "M0601", "V0201"}


class TestConsoleOutput:
    def test_returns_dataframe_with_expected_columns(self):
        df = sus_census_select(dataset="population", output="console", verbose=False)
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == _EXPECTED_COLUMNS
        assert len(df) == 3

    def test_imputation_marker_detected(self):
        df = sus_census_select(dataset="population", output="console", verbose=False)
        row = df[df["code"] == "M0601"].iloc[0]
        assert row["type"] == "imputation_marker"
        row_regular = df[df["code"] == "V0601"].iloc[0]
        assert row_regular["type"] == "regular_variable"

    def test_categorical_flag_and_count(self):
        df = sus_census_select(dataset="population", output="console", verbose=False)
        row = df[df["code"] == "V0601"].iloc[0]
        assert bool(row["has_categories"]) is True
        assert row["n_categories"] == 2
        row2 = df[df["code"] == "V0606"].iloc[0]
        assert bool(row2["has_categories"]) is False
        assert row2["n_categories"] == 0

    def test_category_assignment(self):
        df = sus_census_select(dataset="population", output="console", verbose=False)
        row = df[df["code"] == "V0601"].iloc[0]
        assert row["category"] == "demographics"

    def test_year_column_reflects_param_even_when_dict_is_2010_only(self):
        # Preserved R quirk: dictionaries don't vary by year.
        df = sus_census_select(
            dataset="population", year=2000, output="console", verbose=False
        )
        assert (df["year"] == 2000).all()

    def test_verbose_prints_summary(self, capsys):
        sus_census_select(dataset="population", output="console", verbose=True)
        captured = capsys.readouterr()
        assert "Census Variables Explorer" in captured.out

    def test_sorted_by_dataset_then_code(self):
        df = sus_census_select(dataset="all", output="console", verbose=False)
        assert list(df["dataset"]) == sorted(df["dataset"])


class TestValidation:
    def test_invalid_dataset_raises(self):
        with pytest.raises(ValueError, match="invalid dataset"):
            sus_census_select(dataset="nope")

    def test_invalid_year_raises(self):
        with pytest.raises(ValueError, match="invalid year"):
            sus_census_select(year=1991)

    def test_invalid_lang_raises(self):
        with pytest.raises(ValueError, match="invalid lang"):
            sus_census_select(lang="fr")

    def test_invalid_output_raises(self):
        with pytest.raises(ValueError, match="invalid output"):
            sus_census_select(output="shiny")

    def test_tracts_dataset_raises_no_dictionary(self):
        # Preserved R quirk: "tracts" is a documented valid dataset value but
        # has no dictionary source (R errors on NULL[["pt"]] the same way).
        with pytest.raises(ValueError, match="no dictionary source"):
            sus_census_select(dataset="tracts", output="codes", verbose=False)

    def test_missing_dictionary_raises_when_nothing_loads(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.utils.census_select._load_census_dictionary",
            lambda dataset, lang: None,
        )
        with pytest.raises(ValueError, match="No variables found"):
            sus_census_select(dataset="population", output="codes", verbose=False)


class TestBrowserOutput:
    def test_opens_browser_and_returns_dataframe(self, monkeypatch):
        opened_urls = []
        monkeypatch.setattr(
            "climasus4py.utils.census_select.webbrowser.open",
            lambda url: opened_urls.append(url),
        )

        df = sus_census_select(dataset="population", output="browser", verbose=False)

        assert isinstance(df, pd.DataFrame)
        assert len(opened_urls) == 1
        assert opened_urls[0].startswith("file://")
        assert opened_urls[0].endswith(".html")

    def test_html_contains_codes_and_categories(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.utils.census_select.webbrowser.open", lambda url: None
        )

        captured_html = {}
        from climasus4py.utils import census_select as mod

        original = mod._generate_forest_theme_html

        def spy(vars_df, year, lang):
            html = original(vars_df, year, lang)
            captured_html["html"] = html
            return html

        monkeypatch.setattr(mod, "_generate_forest_theme_html", spy)

        sus_census_select(dataset="population", output="browser", lang="en", verbose=False)

        html = captured_html["html"]
        assert "<html" in html and "</html>" in html
        assert "V0601" in html
        assert "V0606" in html
        assert "demographics" in html
        assert "POPULATION" in html  # dataset header, uppercased

    def test_html_uses_python_list_syntax_not_r_syntax(self, monkeypatch):
        # Deliberate divergence from R (see IDEIAS.md): the copy-to-clipboard
        # help text uses JSON/Python list syntax, not R's c("...") syntax.
        monkeypatch.setattr(
            "climasus4py.utils.census_select.webbrowser.open", lambda url: None
        )
        from climasus4py.utils.census_select import _build_catalog, _generate_forest_theme_html

        df = _build_catalog("population", 2010, "en", verbose=False)
        html = _generate_forest_theme_html(df, 2010, "en")
        assert "sus_census()" in html
        assert 'c("' not in html

    def test_html_in_portuguese(self, monkeypatch):
        monkeypatch.setattr(
            "climasus4py.utils.census_select.webbrowser.open", lambda url: None
        )
        from climasus4py.utils.census_select import _build_catalog, _generate_forest_theme_html

        df = _build_catalog("population", 2010, "pt", verbose=False)
        html = _generate_forest_theme_html(df, 2010, "pt")
        assert "Explorador de Variaveis do Censo Brasileiro" in html
