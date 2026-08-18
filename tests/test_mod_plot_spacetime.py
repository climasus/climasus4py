"""Tests for sus_mod_plot_spacetime (stub — depends on the INLA-only
sus_mod_spacetime_bayes / sus_mod_spacetime_exceedance objects)."""

from __future__ import annotations

import pytest

from climasus4py.viz.mod_plot_spacetime import sus_mod_plot_spacetime


def test_sus_mod_plot_spacetime_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="INLA"):
        sus_mod_plot_spacetime(x={}, type="rr_map")
