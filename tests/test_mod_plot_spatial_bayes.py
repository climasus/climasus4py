"""Tests for sus_mod_plot_spatial_bayes (stub — depends on the CARBayes/INLA-only
sus_mod_spatial_bayes fit object)."""

from __future__ import annotations

import pytest

from climasus4py.viz.mod_plot_spatial_bayes import sus_mod_plot_spatial_bayes


def test_sus_mod_plot_spatial_bayes_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="CARBayes"):
        sus_mod_plot_spatial_bayes(x={}, type="rr")
