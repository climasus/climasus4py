"""Shared test setup.

Closing matplotlib figures after every test (M108)
--------------------------------------------------
There was no ``conftest.py`` here and no ``plt.close()`` anywhere in the
suite, so every figure a plotting test created stayed open for the rest of
the session. Three times in three days — 2026-09-16, 09-17 and 09-18, in
three *different* files — a ``TestRenderizacao::test_desenha_sem_erro``
failed in a full run and passed both in isolation and on the next full run.
Running only the plotting tests never reproduced it.

Accumulated figures are a known cause of exactly that pattern, so the
figures are closed here. Honesty about what this is: a **fix for the
hypothesis, not a diagnosed cause.** The traceback was never captured —
twice the run instrumented to capture it passed. If the failure returns
after this, the hypothesis was wrong and the note should say so.

The backend is also pinned to Agg. A test run has no display, and letting
matplotlib pick an interactive backend is another way the same class of
failure appears.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _fecha_figuras():
    """Close every matplotlib figure the test left open."""
    yield
    try:
        import matplotlib.pyplot as plt
    except ImportError:  # pragma: no cover - matplotlib is optional
        return
    plt.close("all")


def pytest_configure(config):  # noqa: ARG001
    """Pin the non-interactive backend before any test imports pyplot."""
    try:
        import matplotlib
    except ImportError:  # pragma: no cover
        return
    matplotlib.use("Agg", force=True)
