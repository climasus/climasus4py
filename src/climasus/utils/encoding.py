"""UTF-8 encoding fixes for DATASUS data.

Mirrors R: utils-encoding.R — fixes mojibake from latin1→utf8 double-encoding.
"""

from __future__ import annotations


def fix_encoding(text: str) -> str:
    """Fix mojibake caused by UTF-8 text decoded as latin1/cp1252.

    Standard Python approach: re-encode as cp1252 then decode as utf-8.
    This reverses the double-encoding that happens in many DATASUS files.
    """
    try:
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text
