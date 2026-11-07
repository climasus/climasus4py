"""Keep ``DataFrame.attrs["sus_meta"]`` JSON-serialisable.

pandas writes ``DataFrame.attrs`` into the Parquet schema as JSON when the
frame is saved with ``to_parquet()``. A ``pd.Timestamp`` (or a numpy scalar)
anywhere in the metadata dict makes that fail -- "Object of type Timestamp is
not JSON serializable" -- so a user could compute ``sus_grid_chirps(...)``
and then not save it. The metadata already stores ``created``/``modified``
as ISO strings; this applies the same convention to every value.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import PurePath
from typing import Any

import numpy as np
import pandas as pd


def json_safe_meta(obj: Any) -> Any:
    """Return *obj* with every value converted to a JSON-native type.

    Dates and timestamps become ISO-8601 strings, numpy scalars become
    Python scalars, tuples/sets/arrays become lists, paths become strings
    and missing markers (``pd.NA``/``NaT``) become ``None``. Plain JSON
    types pass through unchanged.
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): json_safe_meta(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [json_safe_meta(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return [json_safe_meta(v) for v in obj.tolist()]
    if obj is pd.NaT or obj is pd.NA:
        return None
    if isinstance(obj, (pd.Timestamp, _dt.datetime, _dt.date, _dt.time)):
        return obj.isoformat()
    if isinstance(obj, np.datetime64):
        return None if np.isnat(obj) else pd.Timestamp(obj).isoformat()
    if isinstance(obj, (pd.Timedelta, _dt.timedelta, np.timedelta64)):
        return str(pd.Timedelta(obj))
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, PurePath):
        return str(obj)
    return str(obj)
