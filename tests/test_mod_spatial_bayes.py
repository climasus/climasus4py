"""Tests for sus_mod_spatial_bayes (stub — CARBayes has no Python port)."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_spatial_bayes import sus_mod_spatial_bayes


def test_sus_mod_spatial_bayes_raises_not_implemented():
    df = pd.DataFrame(
        {
            "code_muni": ["1100015", "1100023", "1100031"],
            "deaths": [3, 1, 4],
            "population": [1000, 500, 700],
        }
    )
    with pytest.raises(NotImplementedError, match="CARBayes"):
        sus_mod_spatial_bayes(df=df, outcome="deaths", W={}, offset="population")
