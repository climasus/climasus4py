"""Time-series visualisation of aggregated DATASUS health outcomes — plotnine.

Mirrors R: sus_data_plot_aggregate_ts (sus_data_plot_aggregate_ts.R)

``sus_data_plot_aggregate_ts`` renders one or more of four publication-style
panels from a table with a date column and a health-outcome column:
``"epidemic"`` (area + optional smooth curve over time), ``"seasonal"``
(monthly boxplot distribution), ``"heatmap"`` (year x month calendar tile),
and ``"trend"`` (annual bar chart with year-over-year %% change). When
multiple ``plot_type`` values are requested, panels are stacked vertically
using plotnine's native composition operator (``/``) — the direct
equivalent of R's optional ``patchwork::wrap_plots(ncol = 1)``, without an
extra dependency.

This is a plotting utility, not a lazy pipeline stage: a
``duckdb.DuckDBPyRelation`` input is materialised via ``.df()`` immediately
and the function never touches the lazy pipeline afterwards.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_data_plot_aggregate_ts(df_agg, lang="en")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import re
import warnings
from functools import reduce
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ..utils.data import data_path, expand_city_to_codes

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

# ---------------------------------------------------------------------------
# Constants mirroring the R source's literal hardcoded lists
# ---------------------------------------------------------------------------

_VALID_PLOT_TYPES = ("epidemic", "seasonal", "heatmap", "trend")

# NB: this is the R function's *actual* default (see IDEIAS.md) — R's
# `plot_type = c("epidemic", "seasonal", "heatmap", "trend")` combined with
# `match.arg(plot_type, several.ok = TRUE)` returns the FULL vector when the
# caller omits the argument, not just "epidemic" as the Roxygen @param text
# claims. Preserved literally rather than "fixed" to match the docstring.
_DEFAULT_PLOT_TYPE = ("epidemic", "seasonal", "heatmap", "trend")

_VALID_SMOOTH_METHODS = ("loess", "gam", "lm", "none")

_DATE_CANDIDATES = (
    "date", "data", "DT_NOTIFIC", "DTOBITO", "DT_INTER",
    "DTNASC", "DT_COMPET", "dt_obito", "dt_inter",
    "dt_notific", "dt_nasc", "dt_compet",
)

_OUTCOME_CANDIDATES = (
    "n_obitos", "n_internacoes", "n_nascimentos", "n_casos",
    "n_procedimentos", "n_estabelecimentos",
    "n_deaths", "n_hospitalizations", "n_births", "n_procedures",
    "n_establishments",
    "n_muertes", "n_hospitalizaciones", "n_nacimientos",
    "n_procedimientos", "n_establecimientos",
    "count", "n", "total",
)

_MUNI_CODE_COLUMNS = (
    "codigo_municipio_residencia", "residence_municipality_code",
    "codigo_municipio_ocorrencia", "codigo_municipio_ocurrencia",
    "occurrence_municipality_code", "codigo_municipio_notificacao",
    "codigo_municipio_notificacion", "notification_municipality_code",
    "codigo_municipio_nascimento", "codigo_municipio_nacimiento",
    "birth_municipality_code", "codigo_municipio_paciente",
    "patient_municipality_code", "uf_municipio_estabelecimento",
    "facility_uf_municipality", "uf_municipio_establecimiento",
    "codigo_municipio", "municipality_code", "code_muni",
    "CODMUNRES", "MUNI_RES",
)

_MUNI_COL_REGEX = re.compile(r"municipio|municipality|muni|ibge", re.IGNORECASE)

_VALID_LANGS = ("pt", "en", "es")

_PAL_FALLBACK = ("#185FA5", "#D85A30", "#1D9E75")

_HEATMAP_PAL_ANCHORS = ("#F7FBFF", "#C6DBEF", "#6BAED6", "#2171B5", "#08306B")

# ---------------------------------------------------------------------------
# i18n (mirrors R's .ts_msgs)
# ---------------------------------------------------------------------------

_I18N = {
    "pt": {
        "detecting_col": "Detectando coluna de desfecho...",
        "col_detected": "Coluna detectada: {col}",
        "no_col": "Nenhuma coluna de desfecho encontrada. Use value_col.",
        "wrong_stage": (
            "Dados devem estar no stage 'aggregate' (ou posterior). "
            "Stage atual: {stage}."
        ),
        "no_date": "Coluna de data não encontrada. Esperada: 'date' ou 'data'.",
        "warn_lang": "Idioma '{lang}' não suportado. Usando 'pt'.",
        "done": "Gráfico gerado: {n} linhas, tipo '{type}'.",
        "subtitle_fmt": "n = {n} registros | Desfecho: {col} | {mind} a {maxd}",
        "caption": "Fonte: DATASUS / Ministério da Saúde",
        "title_epidemic": "Curva epidêmica de eventos de saúde notificados",
        "title_seasonal": "Distribuição sazonal de eventos de saúde notificados",
        "title_heatmap": "Mapa de calor mensal de eventos de saúde notificados",
        "title_trend": "Tendência anual de eventos de saúde notificados",
        "y_label": "Eventos notificados",
        "x_year": "Ano",
        "x_month": "Mês",
        "log_note": " (escala log1p; rótulos em contagem original)",
        "month_abbr": ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                        "Jul", "Ago", "Set", "Out", "Nov", "Dez"],
        "fill_label_hm": "Média de eventos notificados",
        "loading_meta": "Carregando metadados municipais...",
        "city_filter": "Filtrando municipio(s): {city}",
        "no_muni_col": "Nenhuma coluna de codigo municipal encontrada para aplicar city.",
    },
    "en": {
        "detecting_col": "Detecting outcome column...",
        "col_detected": "Column detected: {col}",
        "no_col": "No outcome column found. Use value_col.",
        "wrong_stage": "Data must be at stage 'aggregate' (or later). Current stage: {stage}.",
        "no_date": "Date column not found. Expected: 'date' or 'data'.",
        "warn_lang": "Language '{lang}' not supported. Using 'pt'.",
        "done": "Plot generated: {n} rows, type '{type}'.",
        "subtitle_fmt": "n = {n} records | Outcome: {col} | {mind} to {maxd}",
        "caption": "Source: DATASUS / Brazilian Ministry of Health",
        "title_epidemic": "Epidemic curve of reported health events",
        "title_seasonal": "Seasonal distribution of reported health events",
        "title_heatmap": "Monthly heatmap of reported health events",
        "title_trend": "Annual trend in reported health events",
        "y_label": "Reported events",
        "x_year": "Year",
        "x_month": "Month",
        "log_note": " (log1p scale; labels shown as original counts)",
        "month_abbr": ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        "fill_label_hm": "Mean reported events",
        "loading_meta": "Loading municipality metadata...",
        "city_filter": "Filtering municipality/cities: {city}",
        "no_muni_col": "No municipality-code column found to apply city.",
    },
    "es": {
        "detecting_col": "Detectando columna de desenlace...",
        "col_detected": "Columna detectada: {col}",
        "no_col": "No se encontró columna de desenlace. Use value_col.",
        "wrong_stage": (
            "Los datos deben estar en stage 'aggregate' (o posterior). "
            "Stage actual: {stage}."
        ),
        "no_date": "Columna de fecha no encontrada. Esperada: 'date' o 'data'.",
        "warn_lang": "Idioma '{lang}' no admitido. Usando 'pt'.",
        "done": "Gráfico generado: {n} filas, tipo '{type}'.",
        "subtitle_fmt": "n = {n} registros | Desenlace: {col} | {mind} a {maxd}",
        "caption": "Fuente: DATASUS / Ministerio de Salud de Brasil",
        "title_epidemic": "Curva epidémica de eventos de salud notificados",
        "title_seasonal": "Distribución estacional de eventos de salud notificados",
        "title_heatmap": "Mapa de calor mensual de eventos de salud notificados",
        "title_trend": "Tendencia anual de eventos de salud notificados",
        "y_label": "Eventos notificados",
        "x_year": "Año",
        "x_month": "Mes",
        "log_note": " (escala log1p; etiquetas en conteo original)",
        "month_abbr": ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                        "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"],
        "fill_label_hm": "Media de eventos notificados",
        "loading_meta": "Cargando metadatos municipales...",
        "city_filter": "Filtrando municipio(s): {city}",
        "no_muni_col": "No se encontró columna de código municipal para aplicar city.",
    },
}


def _tsm(lbl: dict, key: str, **kwargs: Any) -> str:
    txt = lbl.get(key, key)
    return txt.format(**kwargs) if kwargs else txt


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_data_plot_aggregate_ts requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


# ---------------------------------------------------------------------------
# Palette helper (mirrors R's .ts_palette)
# ---------------------------------------------------------------------------


def _color_ramp(colors: tuple[str, ...] | list[str], n: int) -> list[str]:
    """Linearly interpolate n hex colors along the given control-point ramp.

    Approximates R's grDevices::colorRampPalette without a new dependency
    (matplotlib is already pulled in transitively by plotnine).
    """
    from matplotlib.colors import LinearSegmentedColormap, to_hex

    if n <= 0:
        return []
    if n == 1:
        return [colors[0]]
    cmap = LinearSegmentedColormap.from_list("_ts_ramp", list(colors))
    return [to_hex(cmap(i / (n - 1))) for i in range(n)]


def _ts_palette(palette: str, n: int = 8) -> list[str]:
    """Return *n* hex colors for *palette*.

    NOTE: R's ``.ts_palette`` uses ``ggsci`` (pal_lancet/pal_nejm/pal_jco/
    pal_uchicago) when installed, falling back to a fixed 3-color
    ``colorRampPalette`` otherwise. ``ggsci`` has no Python port, so this
    function always takes R's fallback branch — the ``palette`` argument
    is accepted for signature parity but currently has no visible effect
    (every named palette, including "viridis", resolves to the same ramp,
    which mirrors a real quirk in the R source: "viridis" is not one of
    the `switch()` cases either, so it silently falls into R's own generic
    fallback, not true viridis colors). See IDEIAS.md.
    """
    _ = palette  # accepted for parity; see docstring note above
    cols = list(_PAL_FALLBACK)
    if len(cols) < n:
        cols = _color_ramp(cols, n)
    return cols[:n]


# ---------------------------------------------------------------------------
# Theme helper (mirrors R's .ts_theme)
# ---------------------------------------------------------------------------


def _ts_theme(base_size: float = 11) -> Any:
    import plotnine as p9

    return p9.theme_minimal(base_size=base_size) + p9.theme(
        panel_grid_minor=p9.element_blank(),
        panel_grid_major_x=p9.element_line(color="#EBEBEB", size=0.25),
        panel_grid_major_y=p9.element_line(color="#E0E0E0", size=0.30),
        panel_border=p9.element_rect(color="#D9D9D9", fill=None, size=0.3),
        strip_background=p9.element_rect(fill="#F2F2F2", color=None),
        strip_text=p9.element_text(color="#333333", weight="bold", size=base_size * 0.85),
        axis_text_x=p9.element_text(angle=45, ha="right"),
        legend_position="bottom",
        plot_title=p9.element_text(weight="bold", size=base_size * 1.1),
        plot_subtitle=p9.element_text(color="#595959", size=base_size * 0.90),
        plot_caption=p9.element_text(color="#808080", size=base_size * 0.72, ha="left"),
    )


# ---------------------------------------------------------------------------
# Municipality column detection / city filter / labels
# ---------------------------------------------------------------------------


def _is_muni_col(col: str | None) -> bool:
    if col is None:
        return False
    return col in _MUNI_CODE_COLUMNS or bool(_MUNI_COL_REGEX.search(col))


def _detect_muni_col(columns: list[str], preferred: list[str | None]) -> str | None:
    for cand in preferred:
        if cand is not None and cand in columns and _is_muni_col(cand):
            return cand
    for cand in _MUNI_CODE_COLUMNS:
        if cand in columns:
            return cand
    for col in columns:
        if _is_muni_col(col):
            return col
    return None


def _load_municipality_labels() -> dict[str, str] | None:
    """Load a 6-digit IBGE code -> label lookup from climasus-data.

    Mirrors R's ``.ts_load_muni_meta`` + ``.ts_add_municipality_labels``.
    Returns ``None`` (rather than raising) when the parquet asset is not
    available, since municipality *labelling* is a cosmetic enhancement —
    unlike ``city=`` filtering (which raises via ``expand_city_to_codes``
    because filtering silently on the wrong rows would be worse than
    erroring).
    """
    parquet_path = data_path("spatial/municipalities.parquet")
    if not parquet_path.is_file():
        return None

    df = pd.read_parquet(parquet_path)
    name_col = next(
        (c for c in df.columns if c.lower() in ("municipality_name", "name", "nome", "municipio")),
        None,
    )
    _code_aliases = ("municipality_code", "code", "codigo", "cod_mun", "codmun")
    code_col = next(
        (c for c in df.columns if c.lower() in _code_aliases),
        None,
    )
    if name_col is None or code_col is None:
        return None

    uf_col = next(
        (c for c in df.columns if c.lower() in ("abbrev_state", "uf", "state", "sg_uf")),
        None,
    )

    labels = df[name_col].astype(str)
    if uf_col is not None:
        labels = labels + " (" + df[uf_col].astype(str) + ")"

    keys = df[code_col].astype(str).str.slice(0, 6)
    return dict(zip(keys, labels, strict=False))


def _add_municipality_labels(
    df: pd.DataFrame, muni_cols: list[str], lookup: dict[str, str]
) -> tuple[pd.DataFrame, dict[str, str]]:
    replacements: dict[str, str] = {}
    for col in muni_cols:
        if col not in df.columns:
            continue
        label_col = f"{col}_nome"
        if label_col in df.columns:
            label_col = f"{label_col}_1"
        keys = df[col].astype(str).str.slice(0, 6)
        values = keys.map(lookup)
        values = values.fillna(df[col].astype(str))
        df[label_col] = values
        replacements[col] = label_col
    return df, replacements


# ---------------------------------------------------------------------------
# Plot engine: epidemic
# ---------------------------------------------------------------------------


def _resolve_smooth_method(smooth_method: str) -> str:
    if smooth_method == "gam":
        warnings.warn(
            "sus_data_plot_aggregate_ts: plotnine has no GAM smoother "
            "backend (unlike R's mgcv::gam). Falling back to 'loess', "
            "which approximates but does not reproduce the R output.",
            UserWarning,
            stacklevel=3,
        )
        return "loess"
    return smooth_method


def _ts_epidemic(
    df: pd.DataFrame,
    value_col: str,
    group_col: str | None,
    facet_col: str | None,
    facet_ncol: int,
    smooth_method: str,
    smooth_span: float,
    log_transform: bool,
    free_scales: bool,
    pal: list[str],
    title: str | None,
    subtitle: str | None,
    caption: str | None,
    date_labels: str,
    year_breaks: str,
    base_size: float,
    lbl: dict,
) -> Any:
    import plotnine as p9

    df_e = df.copy()
    df_e["_val"] = pd.to_numeric(df_e[value_col], errors="coerce")

    if log_transform:
        df_e["_y"] = np.log1p(df_e["_val"])
        y_lab = _tsm(lbl, "y_label") + _tsm(lbl, "log_note")

        def _fmt(vals: Any) -> list[str]:
            return [f"{v:,.0f}" for v in np.expm1(vals)]

        y_scale = p9.scale_y_continuous(name=y_lab, labels=_fmt)
    else:
        df_e["_y"] = df_e["_val"]
        y_lab = _tsm(lbl, "y_label")
        y_scale = p9.scale_y_continuous(
            name=y_lab,
            labels=lambda vals: [f"{v:,.0f}" for v in vals],
            expand=(0.01, 0, 0.12, 0),
        )

    primary_col = pal[0]
    secondary_col = pal[min(5, len(pal) - 1)]

    has_group = group_col is not None and group_col in df_e.columns

    if has_group:
        p = (
            p9.ggplot(df_e, p9.aes(x="date", y="_y", color=group_col, fill=group_col))
            + p9.geom_area(alpha=0.18, na_rm=True)
            + p9.geom_line(size=0.7, na_rm=True)
        )
    else:
        p = (
            p9.ggplot(df_e, p9.aes(x="date", y="_y"))
            + p9.geom_area(alpha=0.18, fill=primary_col, na_rm=True)
            + p9.geom_line(size=0.7, color=primary_col, na_rm=True)
        )

    if smooth_method != "none":
        method = _resolve_smooth_method(smooth_method)
        smooth_kwargs: dict[str, Any] = {
            "se": True,
            "color": secondary_col,
            "fill": secondary_col,
            "alpha": 0.15,
            "size": 0.9,
            "na_rm": True,
            "method": method,
        }
        if method == "loess":
            smooth_kwargs["span"] = smooth_span
        p = p + p9.geom_smooth(**smooth_kwargs)

    p = (
        p
        + p9.scale_x_datetime(date_breaks=year_breaks, date_labels=date_labels)
        + y_scale
        + p9.labs(
            title=title or _tsm(lbl, "title_epidemic"),
            subtitle=subtitle,
            caption=caption,
            x=None,
            color=group_col,
            fill=group_col,
        )
        + _ts_theme(base_size)
    )

    if has_group:
        n_grps = df_e[group_col].nunique()
        cols = _ts_palette("lancet", n_grps)
        p = p + p9.scale_color_manual(values=cols) + p9.scale_fill_manual(values=cols)

    if facet_col is not None and facet_col in df_e.columns:
        scales = "free_y" if free_scales else "fixed"
        p = p + p9.facet_wrap(facet_col, ncol=facet_ncol, scales=scales)

    return p


# ---------------------------------------------------------------------------
# Plot engine: seasonal
# ---------------------------------------------------------------------------


def _ts_seasonal(
    df: pd.DataFrame,
    value_col: str,
    group_col: str | None,
    facet_col: str | None,
    facet_ncol: int,
    log_transform: bool,
    free_scales: bool,
    pal: list[str],
    title: str | None,
    subtitle: str | None,
    caption: str | None,
    base_size: float,
    lbl: dict,
) -> Any:
    import plotnine as p9

    month_abbr = lbl["month_abbr"]

    df_s = df.copy()
    df_s["_month_num"] = df_s["date"].dt.month
    df_s["_month_lbl"] = pd.Categorical(
        [month_abbr[m - 1] for m in df_s["_month_num"]], categories=month_abbr, ordered=True
    )
    val = pd.to_numeric(df_s[value_col], errors="coerce")
    df_s["_val_s"] = np.log1p(val) if log_transform else val

    y_lab = _tsm(lbl, "y_label") + (_tsm(lbl, "log_note") if log_transform else "")

    has_group = group_col is not None and group_col in df_s.columns
    fill_var = group_col if has_group else "_month_lbl"

    p = (
        p9.ggplot(df_s, p9.aes(x="_month_lbl", y="_val_s", fill=fill_var))
        + p9.geom_boxplot(
            alpha=0.60,
            outlier_size=1.0,
            outlier_alpha=0.50,
            size=0.35,
            width=0.65,
            color="#4D4D4D",
        )
        + p9.stat_summary(
            fun_y=np.median,
            geom="line",
            mapping=p9.aes(group=1),
            color=pal[min(5, len(pal) - 1)],
            size=0.90,
            alpha=0.85,
        )
        + p9.stat_summary(
            fun_y=np.median,
            geom="point",
            color=pal[min(5, len(pal) - 1)],
            size=1.8,
            shape="o",
            fill="white",
            stroke=1.2,
        )
        + p9.scale_y_continuous(
            name=y_lab,
            labels=(
                (lambda vals: [f"{v:,.0f}" for v in np.expm1(vals)])
                if log_transform
                else (lambda vals: [f"{v:,.0f}" for v in vals])
            ),
        )
        + p9.scale_x_discrete(name=_tsm(lbl, "x_month"))
        + p9.labs(
            title=title or _tsm(lbl, "title_seasonal"),
            subtitle=subtitle,
            caption=caption,
            x=_tsm(lbl, "x_month"),
            fill=group_col,
        )
        + _ts_theme(base_size)
        + p9.theme(axis_text_x=p9.element_text(angle=0, ha="center"))
    )

    if has_group:
        n_grps = df_s[group_col].nunique()
        p = p + p9.scale_fill_manual(values=_ts_palette("lancet", n_grps))
    else:
        p = p + p9.scale_fill_manual(values=_color_ramp(pal, 12), guide=None)

    if facet_col is not None and facet_col in df_s.columns:
        scales = "free_y" if free_scales else "fixed"
        p = p + p9.facet_wrap(facet_col, ncol=facet_ncol, scales=scales)

    return p


# ---------------------------------------------------------------------------
# Plot engine: heatmap
# ---------------------------------------------------------------------------


def _ts_heatmap(
    df: pd.DataFrame,
    value_col: str,
    facet_col: str | None,
    facet_ncol: int,
    log_transform: bool,
    free_scales: bool,
    title: str | None,
    subtitle: str | None,
    caption: str | None,
    base_size: float,
    lbl: dict,
) -> Any:
    import plotnine as p9

    month_abbr = lbl["month_abbr"]
    hm_pal = _color_ramp(_HEATMAP_PAL_ANCHORS, 9)

    group_keys = ["_year_val", "_month_num"]
    df_h = df.copy()
    df_h["_year_val"] = df_h["date"].dt.year
    df_h["_month_num"] = df_h["date"].dt.month
    df_h["_num_val"] = pd.to_numeric(df_h[value_col], errors="coerce")

    has_facet = facet_col is not None and facet_col in df_h.columns
    if has_facet:
        group_keys = [*group_keys, facet_col]

    agg = (
        df_h.groupby(group_keys, as_index=False)["_num_val"]
        .mean()
        .rename(columns={"_num_val": "_fill_val"})
    )
    if log_transform:
        agg["_fill_val"] = np.log1p(agg["_fill_val"])

    fill_lab = _tsm(lbl, "fill_label_hm") + ("\n(log1p)" if log_transform else "")

    agg["_year_fct"] = pd.Categorical(agg["_year_val"].astype(str))

    p = (
        p9.ggplot(agg, p9.aes(x="_month_num", y="_year_fct", fill="_fill_val"))
        + p9.geom_tile(color="white", size=0.4, na_rm=True)
        + p9.scale_fill_gradientn(
            colors=hm_pal,
            na_value="#F2F2F2",
            name=fill_lab,
            labels=(
                (lambda vals: [f"{v:,.0f}" for v in np.expm1(vals)])
                if log_transform
                else (lambda vals: [f"{v:,.0f}" for v in vals])
            ),
        )
        + p9.scale_x_continuous(
            breaks=list(range(1, 13)), labels=month_abbr, expand=(0, 0), name=_tsm(lbl, "x_month")
        )
        + p9.scale_y_discrete(expand=(0, 0))
        + p9.labs(
            title=title or _tsm(lbl, "title_heatmap"),
            subtitle=subtitle,
            caption=caption,
            y=_tsm(lbl, "x_year"),
        )
        + _ts_theme(base_size)
        + p9.theme(
            legend_position="bottom",
            legend_direction="horizontal",
            axis_text_x=p9.element_text(angle=0, ha="center", size=base_size * 0.78),
            axis_text_y=p9.element_text(size=base_size * 0.78),
            panel_grid=p9.element_blank(),
        )
    )

    if has_facet:
        scales = "free_y" if free_scales else "fixed"
        p = p + p9.facet_wrap(facet_col, ncol=facet_ncol, scales=scales)

    return p


# ---------------------------------------------------------------------------
# Plot engine: trend
# ---------------------------------------------------------------------------


def _ts_trend(
    df: pd.DataFrame,
    value_col: str,
    facet_col: str | None,
    facet_ncol: int,
    free_scales: bool,
    pal: list[str],
    title: str | None,
    subtitle: str | None,
    caption: str | None,
    base_size: float,
    lbl: dict,
) -> Any:
    import plotnine as p9

    primary_col = pal[0]

    df_t = df.copy()
    df_t["_year_val"] = df_t["date"].dt.year
    df_t["_num_val"] = pd.to_numeric(df_t[value_col], errors="coerce")

    has_facet = facet_col is not None and facet_col in df_t.columns
    group_keys = [facet_col, "_year_val"] if has_facet else ["_year_val"]
    sort_keys = group_keys

    agg = (
        df_t.groupby(group_keys, as_index=False)["_num_val"]
        .sum()
        .rename(columns={"_num_val": "_annual_total"})
        .sort_values(sort_keys)
    )

    lag_group = agg.groupby(facet_col)["_annual_total"] if has_facet else agg["_annual_total"]
    prev = lag_group.shift(1)
    agg["_pct_change"] = (agg["_annual_total"] / prev - 1.0) * 100.0

    def _fmt_pct(v: float) -> str:
        if pd.isna(v):
            return ""
        return f"+{v:.1f}%" if v > 0 else f"{v:.1f}%"

    agg["_label_pct"] = agg["_pct_change"].map(_fmt_pct)
    agg["_year_fct"] = pd.Categorical(agg["_year_val"].astype(str))
    agg["_is_increase"] = agg["_pct_change"] > 0

    p = (
        p9.ggplot(agg, p9.aes(x="_year_fct", y="_annual_total"))
        + p9.geom_col(width=0.72, alpha=0.88, fill=primary_col, show_legend=False)
        + p9.geom_text(
            p9.aes(y="_annual_total", label="_label_pct", color="_is_increase"),
            va="bottom",
            size=base_size * 0.9,
            fontweight="bold",
            na_rm=True,
        )
        + p9.scale_color_manual(values={False: "#2196F3", True: "#C0392B"}, guide=None)
        + p9.scale_y_continuous(
            name=_tsm(lbl, "y_label"),
            labels=lambda vals: [f"{v:,.0f}" for v in vals],
            expand=(0, 0, 0.18, 0),
        )
        + p9.labs(
            title=title or _tsm(lbl, "title_trend"),
            subtitle=subtitle,
            caption=caption,
            x=_tsm(lbl, "x_year"),
        )
        + _ts_theme(base_size)
        + p9.theme(
            panel_grid_major_x=p9.element_blank(),
            axis_text_x=p9.element_text(angle=45, ha="right"),
        )
    )

    if has_facet:
        scales = "free_y" if free_scales else "fixed"
        p = p + p9.facet_wrap(facet_col, ncol=facet_ncol, scales=scales)

    return p


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_data_plot_aggregate_ts(
    df: pd.DataFrame | duckdb.DuckDBPyRelation,
    value_col: str | None = None,
    group_col: str | None = None,
    facet_col: str | None = None,
    facet_ncol: int = 3,
    plot_type: str | list[str] = _DEFAULT_PLOT_TYPE,
    smooth_method: str = "loess",
    smooth_span: float = 0.25,
    log_transform: bool = False,
    free_scales: bool = True,
    palette: str = "lancet",
    title: str | None = None,
    subtitle: str | None = None,
    caption: str | None = None,
    theme_style: str = "publication",
    date_labels: str = "%b/%y",
    year_breaks: str = "3 months",
    base_size: float = 11,
    interactive: bool = False,
    city: str | list[str] | None = None,
    use_cache: bool = True,
    cache_dir: str = "~/.climasus4r_cache/spatial",
    lang: str = "pt",
    verbose: bool = True,
) -> Any:
    """Plot time-series visualisations of aggregated DATASUS health outcomes.

    Produces publication-style time-series plots (epidemic curve, seasonal
    boxplot, calendar heatmap, and/or annual trend) from a table at stage
    ``"aggregate"`` or later — the intended output of
    ``sus_data_aggregate()``. Mirrors ``climasus4r::sus_data_plot_aggregate_ts()``.

    **Known input-contract gap** (see IDEIAS.md): the R function's real
    contract expects a ``date``-like column plus a health-outcome column
    such as ``n_obitos``/``n_internacoes`` — the shape historically
    produced by ``climasus4r::sus_data_aggregate()``. The Python
    ``sus_data_aggregate()`` currently in this package returns a
    ``time_group`` string column (not a parseable date in every mode,
    e.g. ``"2020-Q1"``) and generic ``count``/``sum_*``/``mean_*`` outcome
    columns instead. This function is implemented against the R contract
    (a literal ``date`` column and an outcome column), so it does **not**
    currently chain directly onto ``climasus4py.sus_data_aggregate()``
    output without the caller renaming/reshaping columns first — the same
    category of contract mismatch already flagged for
    ``sus_climate_aggregate``/``sus_climate_plot_aggregate``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        df: Table (or a ``duckdb.DuckDBPyRelation``, materialised via
            ``.df()`` immediately) with a date column and a health-outcome
            column. Accepted date column names: ``date``, ``data``,
            ``DT_NOTIFIC``, ``DTOBITO``, ``DT_INTER``, ``DTNASC``,
            ``DT_COMPET`` (plus lowercase variants).
        value_col: Name of the outcome column to plot. If ``None``
            (default), auto-detects the first match from ``n_obitos``,
            ``n_internacoes``, ``n_nascimentos``, ``n_casos``,
            ``n_procedimentos``, ``n_estabelecimentos``, and their
            English/Spanish equivalents, or ``count``/``n``/``total``.
        group_col: Optional column used for colour-grouping in
            ``"epidemic"`` and ``"seasonal"`` plots.
        facet_col: Optional column for facet panels.
        facet_ncol: Number of facet columns. Default ``3``.
        plot_type: One or more of ``"epidemic"``, ``"seasonal"``,
            ``"heatmap"``, ``"trend"``. When multiple types are supplied,
            panels are stacked with plotnine's native ``/`` composition
            (one per row). **Default is all four types** — this mirrors
            the R function's actual default behaviour under
            ``match.arg(..., several.ok = TRUE)`` with a full-vector
            default, not the single ``"epidemic"`` value implied by the R
            Roxygen ``@param`` text (see IDEIAS.md).
        smooth_method: Smoothing method for the ``"epidemic"`` plot:
            ``"loess"`` (default), ``"gam"``, ``"lm"``, or ``"none"``.
            plotnine has no GAM backend; ``"gam"`` falls back to
            ``"loess"`` with a warning.
        smooth_span: Span for LOESS smoothing. Default ``0.25``.
        log_transform: Apply ``log1p`` on the y-axis for ``"epidemic"``
            and ``"seasonal"`` plots, and to the fill scale in
            ``"heatmap"``. Default ``False``.
        free_scales: Allow free y-axis scales in faceted plots. Default
            ``True``.
        palette: Colour palette name — ``"lancet"`` (default), ``"nejm"``,
            ``"jco"``, ``"uchicago"``, or ``"viridis"``. ``ggsci`` (which
            the R function uses) has no Python port, so every value
            currently resolves to the same fallback ramp — accepted for
            signature parity, see IDEIAS.md.
        title: Plot title. Auto-generated if ``None``.
        subtitle: Plot subtitle. ``None`` auto-generates one from row
            count, outcome column name, and date range.
        caption: Figure caption. ``None`` uses the DATASUS source string
            for *lang*.
        theme_style: Reserved for future theme variants. Currently
            unused (matches the R source, which never reads this
            parameter inside the function body either).
        date_labels: ``strftime`` format string for x-axis date labels.
            Default ``"%b/%y"``.
        year_breaks: ``date_breaks`` string for the x-axis, e.g.
            ``"3 months"``. Default ``"3 months"``.
        base_size: Base font size for the plotnine theme. Default ``11``.
        interactive: Return a Plotly widget instead of a static plotnine
            object. **Not currently supported** — ``plotly`` is not
            bundled with climasus4py; raises ``ImportError``. See
            IDEIAS.md.
        city: Municipality name(s) or IBGE code(s). When supplied, rows
            are filtered to those municipalities via
            ``climasus-data/spatial/municipalities.parquet`` before
            rendering (same resolution logic as ``sus_filter(city=)``).
        use_cache: Accepted for signature parity with R. Currently a
            no-op — municipality metadata is read directly from
            ``climasus-data`` (already a local package asset) rather than
            R's bundled-parquet -> local-cache -> GitHub-download chain.
            See IDEIAS.md.
        cache_dir: Accepted for signature parity with R. Currently unused
            (see ``use_cache``).
        lang: Language for labels and messages: ``"pt"`` (default),
            ``"en"``, or ``"es"``. An unsupported value falls back to
            ``"pt"`` with a warning (matches R — it does not raise).
        verbose: Print progress messages. Default ``True``.

    Returns:
        A ``plotnine.ggplot`` object (call ``.draw()`` or ``.save(path)``).
        When multiple ``plot_type`` values are requested, the result is a
        plotnine composed plot (stacked via ``/``).

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if
            ``interactive=True``.
        TypeError: If *df* is neither a ``pandas.DataFrame`` nor a
            ``duckdb.DuckDBPyRelation``.
        ValueError: If *df* has 0 rows, no recognised date column, no
            resolvable outcome column, or an unknown *plot_type*.

    Example:
        >>> import climasus4py as cs
        >>> p = cs.sus_data_plot_aggregate_ts(df_agg, lang="en")
        >>> p.draw()
        >>> p2 = cs.sus_data_plot_aggregate_ts(
        ...     df_agg, plot_type="seasonal", log_transform=True, lang="en"
        ... )
    """
    _ = (theme_style, use_cache, cache_dir)  # accepted for R signature parity only

    if interactive:
        raise ImportError(
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike climasus4r's plotly "
            "path). Install plotly manually if needed; see IDEIAS.md for the "
            "open decision on adding it as a first-class extra."
        )

    _require_plotnine()

    if lang not in _VALID_LANGS:
        warnings.warn(_I18N["pt"]["warn_lang"].format(lang=lang), UserWarning, stacklevel=2)
        lang = "pt"
    lbl = _I18N[lang]

    if not isinstance(df, pd.DataFrame):
        if not hasattr(df, "df"):
            raise TypeError(
                f"sus_data_plot_aggregate_ts: df must be a pandas.DataFrame or "
                f"duckdb.DuckDBPyRelation, got {type(df).__name__!r}."
            )
        _valid_later_stages = {"aggregate", "spatial", "climate", "census"}
        try:
            from ..core.meta import sus_meta

            current_stage = sus_meta(df, field="stage")
        except (TypeError, ValueError):
            current_stage = None
        if current_stage is not None and current_stage not in _valid_later_stages:
            warnings.warn(_tsm(lbl, "wrong_stage", stage=current_stage), UserWarning, stacklevel=2)
        df = df.df()
    else:
        df = df.copy()

    if len(df) == 0:
        raise ValueError("df has 0 rows. Nothing to plot.")

    types = [plot_type] if isinstance(plot_type, str) else list(plot_type)
    bad_types = [t for t in types if t not in _VALID_PLOT_TYPES]
    if bad_types:
        raise ValueError(
            f"plot_type must be one or more of: {list(_VALID_PLOT_TYPES)!r}. "
            f"Unknown: {bad_types!r}"
        )

    if smooth_method not in _VALID_SMOOTH_METHODS:
        warnings.warn(
            f"smooth_method '{smooth_method}' unknown. Using 'loess'.",
            UserWarning,
            stacklevel=2,
        )
        smooth_method = "loess"

    # -- Date column detection / normalisation --------------------------------
    date_col_found = next((c for c in _DATE_CANDIDATES if c in df.columns), None)
    if date_col_found is None:
        raise ValueError(_tsm(lbl, "no_date"))
    df["date"] = pd.to_datetime(df[date_col_found], errors="coerce")

    # -- Outcome column auto-detection ----------------------------------------
    if value_col is not None:
        if value_col not in df.columns:
            raise ValueError(f"value_col '{value_col}' not found in df.")
    else:
        if verbose:
            print(_tsm(lbl, "detecting_col"))
        value_col = next((c for c in _OUTCOME_CANDIDATES if c in df.columns), None)
        if value_col is None:
            raise ValueError(_tsm(lbl, "no_col"))
        if verbose:
            print(_tsm(lbl, "col_detected", col=value_col))

    # -- group_col / facet_col validation --------------------------------------
    if group_col is not None and group_col not in df.columns:
        warnings.warn(
            f"group_col '{group_col}' not found in df. Ignoring.", UserWarning, stacklevel=2
        )
        group_col = None
    if facet_col is not None and facet_col not in df.columns:
        warnings.warn(
            f"facet_col '{facet_col}' not found in df. Ignoring.", UserWarning, stacklevel=2
        )
        facet_col = None

    # -- Municipality labels + optional city filter ----------------------------
    muni_cols = [c for c in {group_col, facet_col} if c is not None and _is_muni_col(c)]

    city_muni_col = None
    if city is not None:
        city_list = [city] if isinstance(city, str) else list(city)
        city_muni_col = _detect_muni_col(list(df.columns), [facet_col, group_col])
        if city_muni_col is None:
            raise ValueError(_tsm(lbl, "no_muni_col"))
        if city_muni_col not in muni_cols:
            muni_cols.append(city_muni_col)

    if muni_cols:
        if verbose:
            print(_tsm(lbl, "loading_meta"))

        if city is not None:
            city_codes = expand_city_to_codes(city_list)
            codes6 = {c[:6] for c in city_codes}
            if verbose:
                print(_tsm(lbl, "city_filter", city=", ".join(city_list)))
            keys = df[city_muni_col].astype(str).str.slice(0, 6)
            df = df.loc[keys.isin(codes6)].copy()

        lookup = _load_municipality_labels()
        if lookup:
            df, replacements = _add_municipality_labels(df, muni_cols, lookup)
            if group_col in replacements:
                group_col = replacements[group_col]
            if facet_col in replacements:
                facet_col = replacements[facet_col]

    # -- Palette ----------------------------------------------------------------
    pal = _ts_palette(palette, n=8)

    # -- Auto subtitle / caption --------------------------------------------------
    n_obs = len(df)
    try:
        min_dt = df["date"].min().strftime("%Y-%m")
        max_dt = df["date"].max().strftime("%Y-%m")
    except (ValueError, AttributeError):
        min_dt = max_dt = "?"
    auto_subtitle = _tsm(
        lbl, "subtitle_fmt", n=f"{n_obs:,}", col=value_col, mind=min_dt, maxd=max_dt
    )
    plot_subtitle = subtitle if subtitle is not None else auto_subtitle
    plot_caption = (
        caption if caption is not None else f"{_tsm(lbl, 'caption')} | climasus4py"
    )

    # -- Dispatch -----------------------------------------------------------------
    plots = []
    for tp in types:
        if tp == "epidemic":
            plots.append(
                _ts_epidemic(
                    df, value_col, group_col, facet_col, facet_ncol,
                    smooth_method, smooth_span, log_transform, free_scales, pal,
                    title, plot_subtitle, plot_caption, date_labels, year_breaks,
                    base_size, lbl,
                )
            )
        elif tp == "seasonal":
            plots.append(
                _ts_seasonal(
                    df, value_col, group_col, facet_col, facet_ncol,
                    log_transform, free_scales, pal, title, plot_subtitle,
                    plot_caption, base_size, lbl,
                )
            )
        elif tp == "heatmap":
            plots.append(
                _ts_heatmap(
                    df, value_col, facet_col, facet_ncol, log_transform,
                    free_scales, title, plot_subtitle, plot_caption, base_size, lbl,
                )
            )
        else:  # "trend"
            plots.append(
                _ts_trend(
                    df, value_col, facet_col, facet_ncol, free_scales, pal,
                    title, plot_subtitle, plot_caption, base_size, lbl,
                )
            )

    out = plots[0] if len(plots) == 1 else reduce(lambda a, b: a / b, plots)

    if verbose:
        print(_tsm(lbl, "done", n=n_obs, type="+".join(types)))

    return out
