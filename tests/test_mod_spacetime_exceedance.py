"""Tests for sus_mod_spacetime_exceedance (stub — depends on the INLA-only
sus_mod_spacetime_bayes fit object)."""

from __future__ import annotations

import pytest

from climasus4py.enrichment.mod_spacetime_exceedance import (
    sus_mod_spacetime_exceedance,
)


def test_sus_mod_spacetime_exceedance_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="INLA"):
        sus_mod_spacetime_exceedance(fit={}, thresholds=(1.0, 1.5))
