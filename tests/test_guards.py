"""Tests for lazy pipeline type guards."""

import gc

import pandas as pd
import pytest

from climasus4py.core.clean import sus_data_clean_encoding
from climasus4py.core.engine import get_connection
from climasus4py.core.standardize import sus_data_standardize
from climasus4py.core._stage import get_stage, set_stage


class GeoDataFrame(pd.DataFrame):
    __module__ = "geopandas.geodataframe"


def test_core_rejects_pandas_dataframe():
    df = pd.DataFrame({"IDADE": ["420"], "CONTADOR": [1]})

    with pytest.raises(TypeError, match="DuckDBPyRelation"):
        sus_data_clean_encoding(df)


def test_core_rejects_geodataframe_with_spatial_message():
    gdf = GeoDataFrame({"IDADE": ["420"]})

    with pytest.raises(TypeError, match="materialize|sus_export|DuckDBPyRelation"):
        sus_data_standardize(gdf)


# ---------------------------------------------------------------------------
# P5 Sprint 2 — GC reuse regression (WeakKeyDictionary, Sprint 1 item 9)
# ---------------------------------------------------------------------------

class TestStageGCReuse:
    """Verify _stage uses WeakKeyDictionary so GC'd relations don't bleed
    their stage into a new relation that happens to reuse the same id()."""

    def test_stage_does_not_leak_via_id_reuse(self):
        """A new relation must have no stage even if it shares id() with a GC'd one."""
        conn = get_connection()
        rel1 = conn.from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel1, "filter")
        assert get_stage(rel1) == "filter"

        # Delete and force GC so CPython may reuse the memory address
        del rel1
        gc.collect()

        rel2 = conn.from_df(pd.DataFrame({"x": [2]}))
        # Even if id(rel2) == id(rel1) was true, WeakKeyDictionary removes
        # the entry when rel1 is collected → rel2 must have no stage
        assert get_stage(rel2) is None, (
            "Stage leaked from GC'd relation to new relation with same id()."
        )

    def test_stage_survives_while_relation_alive(self):
        """A relation that stays alive must retain its stage across other GC cycles."""
        conn = get_connection()
        rel = conn.from_df(pd.DataFrame({"x": [1, 2, 3]}))
        set_stage(rel, "clean")

        # Create and destroy many other relations to trigger GC pressure
        for _ in range(50):
            tmp = conn.from_df(pd.DataFrame({"y": [0]}))
            del tmp
        gc.collect()

        assert get_stage(rel) == "clean"

