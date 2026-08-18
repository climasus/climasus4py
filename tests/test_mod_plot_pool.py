"""Tests for sus_mod_plot_pool (stub — consumes a climasus_pool object, itself unportable)."""

from __future__ import annotations

import pytest

from climasus4py.viz.mod_plot_pool import sus_mod_plot_pool


def test_sus_mod_plot_pool_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="sus_mod_pool"):
        sus_mod_plot_pool(x=None)
