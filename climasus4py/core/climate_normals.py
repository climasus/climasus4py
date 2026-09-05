"""climate_normals.py — INMET 30-year climatological normals import.

Mirrors R: sus_climate_normals.R

Downloads INMET climatological normals (1961-1990, 1981-2010, 1991-2020)
from the INMET portal, one Excel file per variable, and returns a tidy
long-format ``pd.DataFrame``. Small-volume import (a handful of Excel
sheets per call) — unlike ``sus_climate_inmet``, there is no lazy DuckDB
stage here because the R source itself never routes this data through
arrow/duckdb; pandas is the idiomatic fit.

The variable catalogue (``normal_meta`` in R) is sourced from the
``climasus-data`` package (``metadata/inmet_normals.json``) instead of
R's bundled-parquet/cache/GitHub-download fallback chain — climasus-data
is always available as an installed dependency, so no separate caching
is needed for the catalogue itself. Only the per-variable Excel downloads
are cached under ``cache_dir``.
"""

from __future__ import annotations

import re
import unicodedata
import warnings
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from rich.console import Console

from ..utils.data import load_json
from .climate_inmet import _download_robust

console = Console(stderr=True)

_DEFAULT_CACHE: Path = Path.home() / ".climasus4py_cache" / "normals"
_VALID_PERIODS: tuple[str, ...] = ("1961-1990", "1981-2010", "1991-2020")
_INMET_NORMALS_URL = "https://portal.inmet.gov.br/uploads/normais/{code_link}.xlsx"

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "Normais Climatológicas INMET",
        "found_vars": "Variáveis encontradas para o período {period}: {n}",
        "downloading": "Baixando: {label}",
        "no_data": "Nenhum dado foi baixado. Verifique a conexão e os códigos de variável.",
        "done": "Download concluído: {n_rows} linhas | {n_vars} variáveis | {n_stations} estações.",
        "unsupported_lang": "Idioma não suportado {lang!r}. Usando 'pt'.",
    },
    "en": {
        "title": "INMET Climate Normals",
        "found_vars": "Variables found for period {period}: {n}",
        "downloading": "Downloading: {label}",
        "no_data": "No data downloaded. Check connection and variable codes.",
        "done": "Download complete: {n_rows} rows | {n_vars} variables | {n_stations} stations.",
        "unsupported_lang": "Unsupported language {lang!r}. Using 'pt'.",
    },
    "es": {
        "title": "Normales Climatológicas INMET",
        "found_vars": "Variables encontradas para el período {period}: {n}",
        "downloading": "Descargando: {label}",
        "no_data": "No se descargaron datos. Verifique la conexión y los códigos de variable.",
        "done": "Descarga completada: {n_rows} filas | {n_vars} variables | "
        "{n_stations} estaciones.",
        "unsupported_lang": "Idioma no soportado {lang!r}. Usando 'pt'.",
    },
}

_LABEL_COL: dict[str, str] = {"pt": "variable_pt", "en": "variable_en", "es": "variable_es"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_climate_normals(
    period: Literal["1961-1990", "1981-2010", "1991-2020"] = "1991-2020",
    target_var: str | list[str] | None = None,
    cache_dir: str | Path = _DEFAULT_CACHE,
    use_cache: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Download and process INMET 30-year climatological normals.

    Retrieves climatological normals for one of three reference periods
    from the INMET portal, one Excel file per variable, and returns a
    tidy long-format table. Climate normals serve as the climatological
    baseline against which observed exposures are compared in
    epidemiological studies (heat-health, DLNM reference exposure,
    vulnerability-index hazard components).

    Args:
        period: Reference climatological period. One of ``"1961-1990"``,
            ``"1981-2010"``, or ``"1991-2020"`` (default).
        target_var: ``var_code`` value(s) to download (e.g.
            ``["t_max", "precipitation"]``). ``None`` (default) downloads
            all variables available for *period*. Call
            ``sus_climate_normals_meta()`` to inspect the catalogue.
        cache_dir: Local directory for disk-cached per-variable Excel
            parses. Default: ``~/.climasus4py_cache/normals``.
        use_cache: If ``True`` (default), loads from and saves to
            *cache_dir*. Set ``False`` to force a fresh download.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        Tidy long-format DataFrame with columns ``codigo``,
        ``nome_estacao``, ``uf``, ``mes``, ``decada``, ``valor``,
        ``var_code``, ``variable_pt``, ``variable_en``, ``variable_es``,
        ``period``. Metadata accessible via ``df.attrs["sus_meta"]``
        (``stage="climate"``, ``type="normals"``).

    Raises:
        ValueError: If *period* is invalid, no variables match
            *target_var*, or no data could be downloaded.

    Examples::

        import climasus4py as cs

        normals = cs.sus_climate_normals(period="1991-2020")
        temp_normals = cs.sus_climate_normals(
            period="1991-2020",
            target_var=["t_max", "t_min", "t_mean_comp"],
            lang="pt",
        )
    """
    if lang not in ("pt", "en", "es"):
        warnings.warn(
            _MESSAGES["pt"]["unsupported_lang"].format(lang=lang), UserWarning, stacklevel=2
        )
        lang = "pt"
    msg = _MESSAGES[lang]

    if period not in _VALID_PERIODS:
        raise ValueError(
            f"Invalid 'period': {period!r}. Must be one of {_VALID_PERIODS}."
        )

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    normal_meta = _get_normal_meta()
    period_vars = normal_meta[normal_meta["period"] == period]
    if period_vars.empty:
        raise ValueError(f"No variables found for period {period!r}.")

    if target_var is not None:
        target_list = [target_var] if isinstance(target_var, str) else list(target_var)
        period_vars = period_vars[period_vars["var_code"].isin(target_list)]
        if period_vars.empty:
            raise ValueError(
                f"None of the requested 'target_var' codes found for period {period!r}. "
                "Run sus_climate_normals_meta() to see available codes."
            )

    if verbose:
        console.print(
            f"[cyan]INFO[/]  {msg['found_vars'].format(period=period, n=len(period_vars))}"
        )

    label_col = _LABEL_COL[lang]
    cache_path = Path(cache_dir).expanduser()

    all_data: list[pd.DataFrame] = []
    for row in period_vars.itertuples(index=False):
        if verbose:
            label = getattr(row, label_col)
            console.print(f"[cyan]INFO[/]  {msg['downloading'].format(label=label)}")

        var_data = _download_normal_var(
            code_link=row.code_link,
            var_code=row.var_code,
            period=period,
            cache_dir=cache_path,
            use_cache=use_cache,
            verbose=verbose,
        )
        if var_data is None or var_data.empty:
            continue

        var_data = var_data.copy()
        var_data["variable_pt"] = row.variable_pt
        var_data["variable_en"] = row.variable_en
        var_data["variable_es"] = row.variable_es
        var_data["period"] = period
        all_data.append(var_data)

    if not all_data:
        raise ValueError(msg["no_data"])

    result = pd.concat(all_data, ignore_index=True)

    n_stations = result["codigo"].nunique(dropna=True)
    n_variables = result["var_code"].nunique()
    now = datetime.now()
    result.attrs["sus_meta"] = {
        "system": None,
        "stage": "climate",
        "type": "normals",
        "period": period,
        "n_stations": n_stations,
        "n_observations": len(result),
        "n_variables": n_variables,
        "created": now.isoformat(),
        "modified": now.isoformat(),
        "history": [
            f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"INMET climate normals imported — period: {period}"
        ],
        "user": {},
    }

    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(n_rows=len(result), n_vars=n_variables, n_stations=n_stations)
        )

    return result


def sus_climate_normals_meta(
    period: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    cache_dir: str | Path = _DEFAULT_CACHE,
    use_cache: bool = True,
    verbose: bool = False,
) -> pd.DataFrame:
    """Browse the INMET climate normals variable catalogue.

    Returns the variable catalogue as a DataFrame so available
    variables and their ``var_code`` values can be inspected before
    calling ``sus_climate_normals()``.

    Args:
        period: Filter to a specific period, or ``None`` (default) to
            return the full catalogue.
        lang: Language for the returned label column: ``"pt"``
            (default), ``"en"``, ``"es"``.
        cache_dir: Unused by the catalogue itself (kept for signature
            parity with the R function); the catalogue always comes
            from the installed ``climasus-data`` package. Applies only
            if a future refresh mechanism needs it.
        use_cache: Unused by the catalogue itself — see *cache_dir*.
        verbose: Print messages. Default ``False``.

    Returns:
        DataFrame with columns ``var_code``, ``variable_label``,
        ``period``, ``var_slug``, ``code_link``.

    Examples::

        import climasus4py as cs

        cs.sus_climate_normals_meta()
        cs.sus_climate_normals_meta(period="1991-2020", lang="en")
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"

    meta = _get_normal_meta()
    label_col = _LABEL_COL[lang]

    out = meta.copy()
    out["variable_label"] = out[label_col]
    out = out[["var_code", "variable_label", "period", "var_slug", "code_link"]]

    if period is not None:
        out = out[out["period"] == period]

    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Internal: variable catalogue (climasus-data, not hardcoded)
# ---------------------------------------------------------------------------

def _get_normal_meta() -> pd.DataFrame:
    """Return the INMET normals variable catalogue from climasus-data.

    Source of truth: ``climasus-data``'s ``metadata/inmet_normals.json``
    (columns: ``id``, ``variable_name``, ``code_link``, ``period``,
    ``variable_pt``, ``variable_en``, ``variable_es``, ``var_slug``,
    ``var_code``).
    """
    records = load_json("metadata/inmet_normals.json")
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Internal: download and cache one variable's Excel file
# ---------------------------------------------------------------------------

def _download_normal_var(
    code_link: str,
    var_code: str,
    period: str,
    cache_dir: Path,
    use_cache: bool,
    verbose: bool,
) -> pd.DataFrame | None:
    """Download (or load from cache) and parse one normals variable."""
    period_dir = cache_dir / period
    cache_file = period_dir / f"{var_code}.parquet"

    if use_cache and cache_file.is_file():
        try:
            return pd.read_parquet(cache_file)
        except Exception:  # pragma: no cover - corrupt cache, re-download
            cache_file.unlink(missing_ok=True)

    period_dir.mkdir(parents=True, exist_ok=True)
    url = _INMET_NORMALS_URL.format(code_link=code_link)
    tmp_file = period_dir / f"{var_code}.xlsx.tmp"

    ok, reason = _download_robust(url, tmp_file, max_retries=3, verbose=verbose)
    if not ok:
        if verbose:
            console.print(f"[yellow]WARN[/]  Failed to download {var_code}: {reason}")
        tmp_file.unlink(missing_ok=True)
        return None

    try:
        data = _read_inmet_normals_excel(tmp_file, var_code)
    finally:
        tmp_file.unlink(missing_ok=True)

    if data is None or data.empty:
        return None

    if use_cache:
        try:
            data.to_parquet(cache_file, index=False)
        except Exception as e:  # pragma: no cover - non-fatal cache failure
            if verbose:
                console.print(f"[yellow]WARN[/]  Failed to cache {var_code}: {e}")

    return data


# ---------------------------------------------------------------------------
# Internal: parse INMET climate normals Excel file
# ---------------------------------------------------------------------------

def _slugify_month(text: str) -> str:
    """Latin-ASCII transliterate, replace non-alnum with '_', strip edges."""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_text.lower())
    return slug.strip("_")


def _make_unique(names: list[str], sep: str = "_d") -> list[str]:
    """Mirror R's ``make.unique(names, sep=sep)``: first occurrence kept as-is."""
    counts: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        if name not in counts:
            counts[name] = 0
            out.append(name)
        else:
            counts[name] += 1
            out.append(f"{name}{sep}{counts[name]}")
    return out


def _read_inmet_normals_excel(file_path: Path, var_code: str) -> pd.DataFrame | None:
    """Parse one INMET normals Excel file into tidy long format.

    INMET Excel structure (after skipping the first 2 rows):
        row 0  = month names (JANEIRO, FEVEREIRO, ...) with merged cells —
                 only the first column of each merge carries the name;
                 continuation columns are NaN.
        row 1+ = station data rows (no separate decade sub-header row).

    Decade (1/2/3) is inferred from position within each month's merged
    cell group: the named cell = decade 1, first NaN = decade 2, second
    NaN = decade 3.
    """
    try:
        import openpyxl  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "openpyxl is required to parse INMET climate normals Excel files. "
            'Install it with: pip install "climasus4py[excel]"'
        ) from exc

    try:
        raw = pd.read_excel(file_path, skiprows=2, header=None, engine="openpyxl")
    except Exception as e:
        console.print(f"[yellow]WARN[/]  Error reading Excel for {var_code}: {e}")
        return None

    header_months = raw.iloc[0]
    data = raw.iloc[1:].reset_index(drop=True)

    col_names = ["codigo", "nome_estacao", "uf"]
    current_month = ""
    month_pos = 0
    for i in range(3, len(header_months)):
        m = header_months.iloc[i]
        if pd.notna(m) and str(m).strip() not in ("", "NA"):
            current_month = _slugify_month(str(m))
            month_pos = 1
        else:
            month_pos += 1
        col_names.append(f"{current_month}_{month_pos}")

    col_names = _make_unique(col_names, sep="_d")

    if len(col_names) != data.shape[1]:
        console.print(
            f"[yellow]WARN[/]  Column count mismatch for {var_code}: "
            f"expected {data.shape[1]} got {len(col_names)}. Skipping variable."
        )
        return None

    data.columns = col_names
    data["codigo"] = data["codigo"].astype(str)
    data["nome_estacao"] = data["nome_estacao"].astype(str)
    data["uf"] = data["uf"].astype(str)
    value_cols = col_names[3:]
    data[value_cols] = data[value_cols].astype(str)

    data_long = data.melt(
        id_vars=["codigo", "nome_estacao", "uf"],
        var_name="mes_decada",
        value_name="valor",
    )

    split = data_long["mes_decada"].str.extract(r"^(.*)_([0-9])$")
    data_long["mes"] = split[0]
    data_long["decada"] = split[1]
    data_long["valor"] = pd.to_numeric(
        data_long["valor"].replace("-", pd.NA), errors="coerce"
    )
    data_long["var_code"] = var_code

    return data_long[["codigo", "nome_estacao", "uf", "mes", "decada", "valor", "var_code"]]
