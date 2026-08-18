"""Tests for sus_welcome — parity helper with climasus4r::sus_welcome."""

from __future__ import annotations

import os

import climasus4py as cs
from climasus4py.utils import welcome


def test_console_only_prints_and_returns_none(capsys):
    result = cs.sus_welcome(lang="en", output="console")
    assert result is None
    out = capsys.readouterr().out
    assert "climasus4py" in out
    assert "sus_data_import()" in out


def test_html_only_writes_file_and_returns_path(monkeypatch):
    monkeypatch.setattr(welcome.sys.stdout, "isatty", lambda: False)
    path = cs.sus_welcome(lang="pt", output="html")
    assert path is not None
    assert os.path.exists(path)
    content = open(path, encoding="utf-8").read()
    assert "climasus4py" in content
    assert "sus_data_import()" in content
    os.remove(path)


def test_browser_opened_when_interactive_and_html_requested(monkeypatch):
    monkeypatch.setattr(welcome.sys.stdout, "isatty", lambda: True)
    opened: list[str] = []
    monkeypatch.setattr(welcome.webbrowser, "open", lambda u: opened.append(u) or True)
    path = cs.sus_welcome(lang="es", output="html", open=True)
    assert opened == [f"file://{path}"]
    os.remove(path)


def test_invalid_lang_raises():
    try:
        cs.sus_welcome(lang="fr")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_invalid_output_raises():
    try:
        cs.sus_welcome(output="pdf")
        assert False, "expected ValueError"
    except ValueError:
        pass
