"""Tests for sus_meta and the expanded _stage.py metadata tracking."""

import duckdb
import pytest

from climasus4py.core._stage import get_meta, get_stage, set_stage
from climasus4py.core.meta import sus_meta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_rel() -> duckdb.DuckDBPyRelation:
    """Return a minimal DuckDB relation with no metadata."""
    con = duckdb.connect()
    return con.sql("SELECT 1 AS x")


def _staged_rel(stage: str, system: str = "SIM-DO", rel_type: str = "health"):
    rel = _fresh_rel()
    set_stage(rel, stage, system=system, rel_type=rel_type)
    return rel


# ---------------------------------------------------------------------------
# _stage.py — set_stage / get_stage / get_meta
# ---------------------------------------------------------------------------

class TestSetGetStage:
    def test_set_and_get_stage(self):
        rel = _fresh_rel()
        set_stage(rel, "import")
        assert get_stage(rel) == "import"

    def test_get_stage_unknown_returns_none(self):
        rel = _fresh_rel()
        assert get_stage(rel) is None

    def test_set_stage_returns_same_rel(self):
        rel = _fresh_rel()
        returned = set_stage(rel, "clean")
        assert returned is rel

    def test_meta_has_all_keys(self):
        rel = _staged_rel("standardize", system="SINAN", rel_type="notificacao")
        meta = get_meta(rel)
        assert set(meta.keys()) == {"stage", "system", "type", "history"}

    def test_meta_system_and_type(self):
        rel = _staged_rel("filter", system="SIM-DO", rel_type="health")
        meta = get_meta(rel)
        assert meta["system"] == "SIM-DO"
        assert meta["type"] == "health"

    def test_meta_history_accumulates(self):
        rel = _fresh_rel()
        set_stage(rel, "import")
        set_stage(rel, "clean")
        set_stage(rel, "filter")
        meta = get_meta(rel)
        assert meta["history"] == ["import", "clean", "filter"]

    def test_meta_history_no_duplicate_consecutive(self):
        rel = _fresh_rel()
        set_stage(rel, "import")
        set_stage(rel, "import")  # same stage twice
        assert get_meta(rel)["history"] == ["import"]

    def test_meta_system_preserved_across_stages(self):
        rel = _fresh_rel()
        set_stage(rel, "import", system="SIM-DO")
        set_stage(rel, "clean")  # no system arg
        assert get_meta(rel)["system"] == "SIM-DO"


# ---------------------------------------------------------------------------
# sus_meta — introspection API
# ---------------------------------------------------------------------------

class TestSusMeta:
    def test_returns_none_for_untracked_rel(self):
        rel = _fresh_rel()
        assert sus_meta(rel) is None

    def test_returns_full_dict(self):
        rel = _staged_rel("aggregate")
        result = sus_meta(rel)
        assert isinstance(result, dict)
        assert result["stage"] == "aggregate"

    def test_field_stage(self):
        rel = _staged_rel("filter")
        assert sus_meta(rel, field="stage") == "filter"

    def test_field_system(self):
        rel = _staged_rel("import", system="SIM-DO")
        assert sus_meta(rel, field="system") == "SIM-DO"

    def test_field_history(self):
        rel = _fresh_rel()
        set_stage(rel, "import", system="SIM-DO")
        set_stage(rel, "clean")
        assert sus_meta(rel, field="history") == ["import", "clean"]

    def test_invalid_field_raises_valueerror(self):
        rel = _staged_rel("import")
        with pytest.raises(ValueError, match="unknown field"):
            sus_meta(rel, field="nope")

    def test_non_relation_raises_typeerror(self):
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            sus_meta("not a relation")

    def test_add_history_does_not_mutate_stored(self):
        rel = _staged_rel("import")
        extended = sus_meta(rel, add_history="clean")
        stored = sus_meta(rel, field="history")
        assert extended["history"] == ["import", "clean"]
        assert stored == ["import"]  # unchanged

    def test_add_history_with_field(self):
        rel = _staged_rel("import")
        result = sus_meta(rel, field="history", add_history="clean")
        assert result == ["import", "clean"]

    def test_none_when_no_meta_and_field_requested(self):
        rel = _fresh_rel()
        assert sus_meta(rel, field="stage") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
