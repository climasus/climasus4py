"""Tests for sus_mod_spacetime_bayes (stub — INLA has no Python port)."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_spacetime_bayes import sus_mod_spacetime_bayes


def test_sus_mod_spacetime_bayes_raises_not_implemented():
    df = pd.DataFrame(
        {
            "code_muni": ["1100015", "1100015", "1100023", "1100023"],
            "date": ["2020-01-01", "2020-02-01", "2020-01-01", "2020-02-01"],
            "deaths": [3, 5, 1, 2],
            "population": [1000, 1000, 500, 500],
        }
    )
    with pytest.raises(NotImplementedError, match="INLA"):
        sus_mod_spacetime_bayes(df=df, outcome="deaths", W={}, offset="population")
