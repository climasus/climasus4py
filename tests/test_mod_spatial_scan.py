"""Tests for sus_mod_spatial_scan (stub — SpatialEpi has no Python port)."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_spatial_scan import sus_mod_spatial_scan


def test_sus_mod_spatial_scan_raises_not_implemented():
    df = pd.DataFrame({"code_muni": ["1100015"], "cases": [3], "population": [1000]})
    with pytest.raises(NotImplementedError, match="SpatialEpi"):
        sus_mod_spatial_scan(df=df, cases="cases", population="population", municipalities=None)
