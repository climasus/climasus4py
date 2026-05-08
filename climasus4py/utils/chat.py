"""Chat helper — sus_chat.

Opens the ClimaSUS AI chat interface in the default browser (interactive
sessions) or prints the URL (non-interactive / CI environments).

Mirrors R: climasus4r::sus_chat().
"""

from __future__ import annotations

import os
import sys
import webbrowser

DEFAULT_CHAT_URL: str = "https://claude.ai/new"


def sus_chat(url: str | None = None) -> str:
    """Open (or print) the ClimaSUS chat URL.

    Behaviour:
    1. Resolve URL: explicit *url* → env var ``CLIMASUS_CHAT_URL`` →
       ``DEFAULT_CHAT_URL``.
    2. Print the resolved URL to stdout.
    3. If ``sys.stdout.isatty()`` is ``True``, attempt to open the URL
       in the default browser via ``webbrowser.open()``. Any exception
       from ``webbrowser`` is silently suppressed (the URL was already
       printed as fallback).
    4. Return the resolved URL string.

    Args:
        url: Override URL. If ``None``, the env var
            ``CLIMASUS_CHAT_URL`` is checked first, then
            ``DEFAULT_CHAT_URL`` is used as the final fallback.

    Returns:
        The resolved URL that was used.

    Example:
        >>> import climasus4py as cs
        >>> cs.sus_chat()
        https://claude.ai/new
        'https://claude.ai/new'
    """
    if url is None:
        url = os.environ.get("CLIMASUS_CHAT_URL", DEFAULT_CHAT_URL)

    print(url)

    if sys.stdout.isatty():
        import contextlib
        with contextlib.suppress(Exception):
            webbrowser.open(url)

    return url
