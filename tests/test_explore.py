"""Tests for sus_explore — metadata browsing."""

import pytest

from climasus.utils.explore import sus_explore


class TestExplore:
    def test_systems(self):
        result = sus_explore("systems")
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_groups(self):
        result = sus_explore("groups")
        assert "core" in result or "climate_sensitive" in result

    def test_regions(self):
        result = sus_explore("regions")
        assert isinstance(result, dict)

    def test_uf(self):
        result = sus_explore("uf")
        assert isinstance(result, dict)
        # Should contain Brazilian states
        assert "SP" in result or "RJ" in result

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown topic"):
            sus_explore("nonexistent")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
