"""Tests for sus_chat — parity helper with climasus4r::sus_chat."""

from __future__ import annotations

import climasus4py as cs
from climasus4py.utils import chat


def test_returns_default_url_when_no_args(monkeypatch, capsys):
    """Without args and without env var, returns the legacy default URL."""
    monkeypatch.delenv("CLIMASUS_CHAT_URL", raising=False)
    monkeypatch.setattr(chat.sys.stdout, "isatty", lambda: False)
    url = cs.sus_chat()
    assert url == chat.DEFAULT_CHAT_URL
    assert chat.DEFAULT_CHAT_URL in capsys.readouterr().out


def test_explicit_url_takes_precedence_over_env(monkeypatch, capsys):
    monkeypatch.setenv("CLIMASUS_CHAT_URL", "https://env-url.example/")
    monkeypatch.setattr(chat.sys.stdout, "isatty", lambda: False)
    url = cs.sus_chat("https://explicit.example/")
    assert url == "https://explicit.example/"


def test_env_url_used_when_no_explicit_arg(monkeypatch, capsys):
    monkeypatch.setenv("CLIMASUS_CHAT_URL", "https://env-url.example/")
    monkeypatch.setattr(chat.sys.stdout, "isatty", lambda: False)
    url = cs.sus_chat()
    assert url == "https://env-url.example/"


def test_browser_opened_when_interactive(monkeypatch):
    monkeypatch.setattr(chat.sys.stdout, "isatty", lambda: True)
    opened: list[str] = []
    monkeypatch.setattr(chat.webbrowser, "open", lambda u: opened.append(u) or True)
    url = cs.sus_chat("https://claude.ai/")
    assert opened == ["https://claude.ai/"]
    assert url == "https://claude.ai/"


def test_browser_failure_falls_back_to_print(monkeypatch, capsys):
    monkeypatch.setattr(chat.sys.stdout, "isatty", lambda: True)

    def boom(_url: str) -> bool:
        raise RuntimeError("no display")

    monkeypatch.setattr(chat.webbrowser, "open", boom)
    url = cs.sus_chat("https://x.example/")
    assert url == "https://x.example/"
    assert "https://x.example/" in capsys.readouterr().out
