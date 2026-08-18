"""Tests for sus_mod_pool (stub — dlnm/mvmeta have no Python port)."""

from __future__ import annotations

import pytest

from climasus4py.enrichment.mod_pool import sus_mod_pool


def test_sus_mod_pool_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="dlnm"):
        sus_mod_pool(fits={})
