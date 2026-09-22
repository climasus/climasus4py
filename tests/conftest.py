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
def _limpa_caches_de_dados():
    """Clear the climasus-data caches between tests (M124).

    Several tests point the data directory at a `tmp_path` holding a
    stripped-down catalog — `test_lazy_enrichments` sets
    `CLIMASUS_DATA_DIR` and resets `_DATA_DIR`, others patch `data_path`
    directly. Restoring the env var and the module global, which
    `monkeypatch` does, is **not enough**: `load_json` and its siblings
    are `lru_cache`d on the *relative path* alone, so the fake file the
    test loaded stays cached for the rest of the process and every later
    test gets it.

    Measured: `test_colunas_geograficas.py::test_o_metadado_declara_os_recortes`
    passes alone and fails with `KeyError: 'schema_version'` when
    `test_lazy_enrichments.py` runs first — against the committed code,
    so this is not something a recent change introduced. The suite's
    result depended on its order, which is also the shape of the M108
    complaint this file was created for.

    Clearing afterwards costs one file read per test that needs one.
    """
    yield
    from climasus4py.utils import data as _data

    for nome in ("load_json", "load_datasus_columns_spec", "_municipio_meta",
                 "_column_synonym_index"):
        alvo = getattr(_data, nome, None)
        limpar = getattr(alvo, "cache_clear", None)
        if limpar is not None:
            limpar()

    try:
        import climasus_data as _cd
    except ImportError:  # pragma: no cover - the package is a hard dependency
        return
    for nome in dir(_cd):
        limpar = getattr(getattr(_cd, nome, None), "cache_clear", None)
        if limpar is not None:
            limpar()


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
