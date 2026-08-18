"""Tests for sus_mod_metaregression (stub — dlnm/mvmeta have no Python port)."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_metaregression import sus_mod_metaregression


def test_sus_mod_metaregression_raises_not_implemented():
    covariates = pd.DataFrame({"mean_temp": [28.5, 22.1]}, index=["fortaleza", "curitiba"])
    with pytest.raises(NotImplementedError, match="dlnm"):
        sus_mod_metaregression(fits={}, covariates=covariates)
