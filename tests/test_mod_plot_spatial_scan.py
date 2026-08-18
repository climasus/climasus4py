"""Tests for sus_mod_plot_spatial_scan (stub — consumes a climasus_spatial_scan object, itself unportable)."""

from __future__ import annotations

import pytest

from climasus4py.viz.mod_plot_spatial_scan import sus_mod_plot_spatial_scan


def test_sus_mod_plot_spatial_scan_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="sus_mod_spatial_scan"):
        sus_mod_plot_spatial_scan(x=None, municipalities=None)
