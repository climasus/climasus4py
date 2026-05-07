"""UTF-8 encoding fixes for DATASUS data.

Mirrors R: utils-encoding.R — fixes mojibake from latin1→utf8 double-encoding.
"""

from __future__ import annotations


def fix_encoding(text: str) -> str:
    """Fix mojibake caused by UTF-8 text mistakenly decoded as Latin-1/CP-1252.

    Many DATASUS ``.dbc`` files contain UTF-8 strings that were read
    with a Latin-1/CP-1252 codec, producing garbled characters
    (mojibake). The fix re-encodes the mangled string as CP-1252 then
    decodes it as UTF-8, reversing the double-encoding.

    Args:
        text: String that may contain mojibake characters.

    Returns:
        Corrected UTF-8 string, or the original *text* unchanged if
        the re-encoding fails (e.g. the text was already valid UTF-8).

    Example:
        >>> fix_encoding("S\u00c3\u00a3o Paulo")  # mojibake for "São Paulo"
        'São Paulo'
        >>> fix_encoding("already fine")
        'already fine'
    """
    try:
        return text.encode("cp1252").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text
