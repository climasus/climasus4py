"""Tests for sus_mod_spacetime_predict (stub — depends on the INLA-only
sus_mod_spacetime_bayes fit object)."""

from __future__ import annotations

import pytest

from climasus4py.enrichment.mod_spacetime_predict import sus_mod_spacetime_predict


def test_sus_mod_spacetime_predict_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="INLA"):
        sus_mod_spacetime_predict(fit={}, horizon=6)
