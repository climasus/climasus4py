"""Tests for list_disease_groups and get_disease_group_details."""

import pytest

from climasus4py.utils.disease_groups import (
    get_disease_group_details,
    list_disease_groups,
)


# ---------------------------------------------------------------------------
# list_disease_groups
# ---------------------------------------------------------------------------

class TestListDiseaseGroups:
    def test_returns_list(self):
        result = list_disease_groups()
        assert isinstance(result, list)
        assert len(result) > 0

    def test_each_item_has_required_keys(self):
        result = list_disease_groups()
        for item in result:
            assert "group" in item
            assert "label" in item
            assert "codes" in item
            assert "climate_sensitive" in item

    def test_climate_sensitive_only_filter(self):
        all_groups = list_disease_groups()
        cs_only = list_disease_groups(climate_sensitive_only=True)
        assert len(cs_only) <= len(all_groups)
        assert all(g["climate_sensitive"] for g in cs_only)

    def test_dengue_is_present(self):
        groups = list_disease_groups()
        keys = [g["group"] for g in groups]
        assert "dengue" in keys

    def test_lang_en_returns_english_labels(self):
        groups = list_disease_groups(lang="en")
        dengue = next(g for g in groups if g["group"] == "dengue")
        assert dengue["label"] == "Dengue"

    def test_lang_es_returns_spanish_labels(self):
        groups = list_disease_groups(lang="es")
        dengue = next(g for g in groups if g["group"] == "dengue")
        assert dengue["label"] == "Dengue"  # same in all langs

    def test_invalid_lang_raises_valueerror(self):
        with pytest.raises(ValueError, match="unsupported lang"):
            list_disease_groups(lang="fr")

    def test_codes_is_list(self):
        groups = list_disease_groups()
        for g in groups:
            assert isinstance(g["codes"], list)


# ---------------------------------------------------------------------------
# get_disease_group_details
# ---------------------------------------------------------------------------

class TestGetDiseaseGroupDetails:
    def test_returns_dict(self):
        result = get_disease_group_details("dengue")
        assert isinstance(result, dict)

    def test_dengue_codes(self):
        result = get_disease_group_details("dengue")
        assert "A90" in result["codes"]

    def test_dengue_climate_sensitive(self):
        result = get_disease_group_details("dengue")
        assert result["climate_sensitive"] is True

    def test_dengue_climate_factors(self):
        result = get_disease_group_details("dengue")
        assert "temperature" in result["climate_factors"]

    def test_lang_en(self):
        result = get_disease_group_details("dengue", lang="en")
        assert result["label"] == "Dengue"
        assert "dengue" in result["description"].lower()

    def test_all_keys_present(self):
        result = get_disease_group_details("dengue")
        assert set(result.keys()) == {
            "group", "label", "description", "codes",
            "climate_sensitive", "climate_factors"
        }

    def test_unknown_group_raises_keyerror(self):
        with pytest.raises(KeyError, match="not found"):
            get_disease_group_details("xyz_nonexistent_group")

    def test_invalid_lang_raises_valueerror(self):
        with pytest.raises(ValueError, match="unsupported lang"):
            get_disease_group_details("dengue", lang="de")

    def test_returns_group_field_matching_input(self):
        result = get_disease_group_details("dengue")
        assert result["group"] == "dengue"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
