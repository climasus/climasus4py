"""grid_join.py — join gridded environmental data to the health pipeline.

Mirrors R: sus_grid_join.R

Bridge between the gridded environmental pipeline (``sus_grid_*``) and the
health pipeline (``sus_data_import`` -> ... -> ``sus_spatial_join``) —
the same role ``sus_climate`` plays for station-based INMET data. Not
lazy: both inputs are already small (municipality x date), and the R
source itself always materialises to tibbles before joining.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import duckdb
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

_GEOM_COLS = ("geometry", "geom")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "sus_grid_join: Unindo Dados Gridded ao Pipeline de Saude",
        "invalid_by": "'by' deve ser uma lista de strings nao vazia.",
        "missing_by_health": "Coluna(s) de join ausente(s) em health_data: {cols}.",
        "missing_by_grid": "Coluna(s) de join ausente(s) em grid_data: {cols}.",
        "no_grid_cols": "grid_data nao tem colunas alem das chaves de join.",
        "column_collision": (
            "Atencao: coluna(s) '{cols}' existem em ambos os objetos e serao sobrescritas."
        ),
        "temporal_mismatch": (
            "Aviso temporal: health_data tem {n_health} datas unicas, grid_data tem "
            "{n_grid} ({pct}% de match). Considere agregar o clima antes do join."
        ),
        "join_start": "Unindo {n_health} linhas com {n_grid_col} coluna(s) gridded: {grid_cols}",
        "join_done": "Concluido: {n_rows} linhas; {n_na} NA(s) em '{out_col}'.",
    },
    "en": {
        "title": "sus_grid_join: Joining Gridded Data to the Health Pipeline",
        "invalid_by": "'by' must be a non-empty list of strings.",
        "missing_by_health": "Join column(s) missing from health_data: {cols}.",
        "missing_by_grid": "Join column(s) missing from grid_data: {cols}.",
        "no_grid_cols": "grid_data has no columns beyond the join keys.",
        "column_collision": (
            "Warning: column(s) '{cols}' exist in both objects and will be overwritten."
        ),
        "temporal_mismatch": (
            "Temporal warning: health_data has {n_health} unique dates, grid_data has "
            "{n_grid} ({pct}% match). Consider aggregating the climate data before joining."
        ),
        "join_start": "Joining {n_health} rows with {n_grid_col} gridded column(s): {grid_cols}",
        "join_done": "Complete: {n_rows} rows; {n_na} NA(s) in '{out_col}'.",
    },
    "es": {
        "title": "sus_grid_join: Uniendo Datos Gridded al Pipeline de Salud",
        "invalid_by": "'by' debe ser una lista de strings no vacia.",
        "missing_by_health": "Columna(s) de join ausente(s) en health_data: {cols}.",
        "missing_by_grid": "Columna(s) de join ausente(s) en grid_data: {cols}.",
        "no_grid_cols": "grid_data no tiene columnas mas alla de las claves de join.",
        "column_collision": (
            "Advertencia: columna(s) '{cols}' existen en ambos objetos y seran sobrescritas."
        ),
        "temporal_mismatch": (
            "Advertencia temporal: health_data tiene {n_health} fechas unicas, grid_data "
            "tiene {n_grid} ({pct}% de coincidencia). Considere agregar el clima antes del join."
        ),
        "join_start": "Uniendo {n_health} filas con {n_grid_col} columna(s) gridded: {grid_cols}",
        "join_done": "Completo: {n_rows} filas; {n_na} NA(s) en '{out_col}'.",
    },
}


def sus_grid_join(
    health_data: duckdb.DuckDBPyRelation | pd.DataFrame,
    grid_data: duckdb.DuckDBPyRelation | pd.DataFrame,
    by: list[str] | tuple[str, ...] = ("code_muni", "date"),
    type_out: str | None = None,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> pd.DataFrame:
    """Join gridded environmental data to health data.

    Merges the municipality x date output of any ``sus_grid_*`` function
    (ERA5, CHIRPS, GHAP, CAMS, fires, PRODES, ...) with health data
    produced by ``sus_spatial_join`` or ``sus_data_aggregate``. The
    result carries all health columns plus the environmental variable
    columns from *grid_data*, ready for ``sus_mod_dlnm``,
    ``sus_climate_anomaly``, or ``sus_mod_vulnerability_index``.

    Args:
        health_data: Health data with *by* columns. A lazy
            ``DuckDBPyRelation`` is materialised via ``.df()``.
        grid_data: Output of any ``sus_grid_*`` function, with *by*
            columns and at least one environmental variable column.
            A lazy ``DuckDBPyRelation`` is materialised via ``.df()``.
        by: Join key columns. Default ``("code_muni", "date")``. Use
            ``("code_muni",)`` when joining annual grid data (e.g.
            PRODES) to monthly/daily health data — the annual value
            broadcasts to every matching row.
        type_out: Overrides the output ``sus_meta["type"]``. ``None``
            (default) inherits the type from *grid_data*.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        All columns from *health_data* plus the environmental variable
        columns from *grid_data*. Rows with no matching grid row get
        ``NaN`` for the grid columns. Metadata in
        ``df.attrs["sus_meta"]`` (``stage="climate"``, ``type`` inherited
        from *grid_data* or *type_out*).

    Raises:
        ValueError: If *by* is empty, a join column is missing from
            either input, or *grid_data* has no columns beyond the
            join keys.

    Examples::

        import climasus4py as cs

        era5 = cs.sus_grid_era5(years=2020, municipalities=mt_mun, vars=["t2m", "tp"])
        combined = cs.sus_grid_join(sih_agg, era5)

        # Annual deforestation joined to monthly health data.
        prodes = cs.sus_grid_prodes(years=2020, municipalities=mt_mun)
        combined2 = cs.sus_grid_join(sih_agg, prodes, by=["code_muni"])
    """
    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if isinstance(health_data, duckdb.DuckDBPyRelation):
        health_df = health_data.df()
    else:
        health_df = health_data.copy()

    if isinstance(grid_data, duckdb.DuckDBPyRelation):
        grid_df = grid_data.df()
        grid_meta_in: dict = {}
    else:
        grid_df = grid_data.copy()
        grid_meta_in = grid_data.attrs.get("sus_meta", {})

    by_list = list(by)
    if not by_list:
        raise ValueError(msg["invalid_by"])

    missing_h = [c for c in by_list if c not in health_df.columns]
    if missing_h:
        raise ValueError(msg["missing_by_health"].format(cols=missing_h))
    missing_g = [c for c in by_list if c not in grid_df.columns]
    if missing_g:
        raise ValueError(msg["missing_by_grid"].format(cols=missing_g))

    grid_cols = [c for c in grid_df.columns if c not in by_list and c not in _GEOM_COLS]
    if not grid_cols:
        raise ValueError(msg["no_grid_cols"])

    collisions = [
        c for c in grid_cols if c in health_df.columns and c not in by_list
    ]
    if collisions:
        console.print(
            "[yellow]WARN[/]  " + msg["column_collision"].format(cols=", ".join(collisions))
        )

    if "date" in by_list:
        health_dates = pd.to_datetime(health_df["date"]).dt.normalize().unique()
        grid_dates = pd.to_datetime(grid_df["date"]).dt.normalize().unique()
        if len(grid_dates) > 0 and len(health_dates) > 0:
            pct_match = float(pd.Series(grid_dates).isin(health_dates).mean())
            if pct_match < 0.5 and len(grid_dates) > len(health_dates):
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["temporal_mismatch"].format(
                        n_health=len(health_dates),
                        n_grid=len(grid_dates),
                        pct=round(pct_match * 100),
                    )
                )

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")
        console.print(
            "[cyan]INFO[/]  "
            + msg["join_start"].format(
                n_health=len(health_df),
                n_grid_col=len(grid_cols),
                grid_cols=", ".join(grid_cols),
            )
        )

    drop_cols = [c for c in _GEOM_COLS if c in health_df.columns] + collisions
    health_clean = health_df.drop(columns=drop_cols) if drop_cols else health_df
    grid_clean = grid_df[by_list + grid_cols]

    result = health_clean.merge(grid_clean, on=by_list, how="left")

    base_meta = dict(health_df.attrs.get("sus_meta", {}))
    grid_type = grid_meta_in.get("type")
    out_type = type_out or grid_type or "grid"
    now = datetime.now()
    base_meta["stage"] = "climate"
    base_meta["type"] = out_type
    base_meta["modified"] = now.isoformat()
    history = list(base_meta.get("history", []))
    history.append(
        f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] sus_grid_join(): "
        f"{len(grid_cols)} col(s) added ({', '.join(grid_cols)}), join by: {', '.join(by_list)}"
    )
    base_meta["history"] = history
    result.attrs["sus_meta"] = base_meta

    if verbose:
        n_na = int(result[grid_cols[0]].isna().sum())
        console.print(
            "[green]OK[/]  "
            + msg["join_done"].format(n_rows=len(result), n_na=n_na, out_col=grid_cols[0])
        )

    return result
