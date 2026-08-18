"""Publication-quality demographic visualisation — ggplot-style via plotnine.

Mirrors R: sus_data_plot_demographics (sus_data_plot_demographics.R)

``sus_data_plot_demographics`` produces tables, charts, and a composite
dashboard summarising the demographic and climate-risk composition of a
standardised SUS/DATASUS dataset (SIM, SIH, SINAN, CNES, SIA, SINASC).
Visual style follows a Lancet/Nature-Medicine layout; colour palettes mirror
the ggsci palettes the R source uses (via its own hardcoded fallback hex
values — climasus4py does not add a ggsci-equivalent Python dependency, see
IDEIAS.md).

This function operates on an already-materialised table — it is a plotting
utility, not a pipeline stage, so a ``duckdb.DuckDBPyRelation`` input is
materialised via ``.df()`` at the top and the function otherwise never
touches the lazy pipeline.

Requires the optional [plot] extra:
    pip install climasus4py[plot]

Usage:
    >>> import climasus4py as cs
    >>> p = cs.sus_data_plot_demographics(df, type="pyramid", lang="pt")
    >>> p.draw()        # display inline (Jupyter)
    >>> p.save("out.png")
"""

from __future__ import annotations

import re
import warnings
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ..core.meta import sus_meta

if TYPE_CHECKING:  # pragma: no cover
    import duckdb

# ---------------------------------------------------------------------------
# Palettes (mirrors R's .vd_palette fallback hex vectors -- ggsci is not a
# Python dependency here, so these fallback values ARE the implementation,
# not just a fallback path; see IDEIAS.md)
# ---------------------------------------------------------------------------

_VD_PALETTES: dict[str, list[str]] = {
    "lancet": [
        "#00468B", "#ED0000", "#42B540", "#0099B4", "#925E9F",
        "#FDAF91", "#AD002A", "#ADB6B6", "#1B1919",
    ],
    "nature": [
        "#E64B35", "#4DBBD5", "#00A087", "#3C5488", "#F39B7F",
        "#8491B4", "#91D1C2", "#DC0000", "#7E6148", "#B09C85",
    ],
    "nejm": [
        "#BC3C29", "#0072B5", "#E18727", "#20854E",
        "#7876B1", "#6F99AD", "#FFDC91", "#EE4C97",
    ],
    "jco": [
        "#0073C2", "#EFC000", "#868686", "#CD534C", "#7AA6DC",
        "#003C67", "#8F7700", "#3B3B3B", "#A73030", "#4A6990",
    ],
    "aaas": [
        "#3B4992", "#EE0000", "#008B45", "#631879", "#008280",
        "#BB0021", "#5F559B", "#A20056", "#808180", "#1B1919",
    ],
    "sus": [
        "#1B6CA8", "#E84855", "#3BB273", "#F4A261",
        "#7B2D8B", "#2EC4B6", "#FF9F1C", "#CBCBCB",
    ],
    "colorblind": [
        "#0072B2", "#D55E00", "#009E73", "#CC79A7",
        "#56B4E9", "#E69F00", "#F0E442", "#999999",
    ],
    "science": [
        "#3B4992", "#EE0000", "#008B45", "#631879",
        "#008280", "#BB0021", "#5F559B", "#A20056",
    ],
    "viridis": [
        "#440154", "#3B528B", "#21908C", "#5DC863",
        "#FDE725", "#31688E", "#35B779", "#8FD744",
    ],
}

_VALID_TYPES = (
    "table", "bar", "pyramid", "heatmap",
    "temporal", "climate", "race_equity", "dashboard",
)
_VALID_TIME_UNITS = ("month", "epi_week", "year", "quarter", "semester")
_VALID_FILL_METRICS = ("pct_row", "pct_col", "pct_total", "count")

# IBGE 2022 Census national race/colour proportions (percent) -- default
# reference for type="race_equity" when benchmark= is not supplied. Copied
# verbatim from the R source's inline constant (R hardcodes this the same
# way; climasus-data has no census race-proportion table yet). See IDEIAS.md.
_IBGE_2022_RACE: dict[str, float] = {
    "Branca": 43.5, "Parda": 45.3, "Preta": 10.2, "Amarela": 0.5, "Indigena": 0.5,
    "White": 43.5, "Brown": 45.3, "Black": 10.2, "Yellow": 0.5, "Indigenous": 0.5,
    "Blanca": 43.5, "Negra": 10.2,
}

# ---------------------------------------------------------------------------
# Demographic column detection (mirrors R's .vd_detect_cols / .find_col)
# ---------------------------------------------------------------------------

_DEMO_COL_CANDIDATES: dict[str, tuple[str, ...]] = {
    "sex": ("sex", "sexo", "SEXO", "CS_SEXO"),
    "race": ("race", "raca", "raza", "RACACOR", "RACA_COR", "CS_RACA"),
    "age": ("age_years", "idade", "edad", "NU_IDADE_N"),
    "age_group": (
        "ibge_age_group", "age_group", "faixa_etaria",
        "grupo_etario", "age_group_5yr",
    ),
    "ibge_age_group": ("ibge_age_group",),
    "education": (
        "education_level", "education", "escolaridade",
        "escolaridad", "ESC", "ESC2010", "CS_ESCOL_N",
    ),
    "climate_risk": ("climate_risk_group", "grupo_risco_climatico"),
    "region": ("manager_uf", "uf_gestor", "UF_ZI", "SG_UF_NOT", "notification_uf"),
    "municipality": (
        "residence_municipality_code", "municipality_code",
        "CODMUNRES", "municipio_residencia",
    ),
}

# time_unit -> candidate column names. NOTE: "epi_week" is added to the
# epi_week candidates as a deliberate extension beyond the R source's list
# (which only checks "epidemiological_week"/"semana_epidemiologica"/
# "SEM_NOT") because climasus4py's sus_data_create_variables() actually
# names its derived column "epi_week". Without this addition, type=
# "temporal" with time_unit="epi_week" would never find a real
# climasus4py-generated column. See IDEIAS.md.
_TIME_COL_CANDIDATES: dict[str, tuple[str, ...]] = {
    "month": ("month", "mes"),
    "epi_week": ("epi_week", "epidemiological_week", "semana_epidemiologica", "SEM_NOT"),
    "year": ("year", "ano", "ANO_NOT"),
    "quarter": ("quarter", "trimestre"),
    "semester": ("semester", "semestre"),
}

_PYRAMID_MALE_VALS = frozenset({"Male", "Masculino", "M", "1", "male", "masculino"})
_PYRAMID_FEMALE_VALS = frozenset(
    {"Female", "Feminino", "F", "2", "female", "feminino"}
)


def _find_col(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


def _detect_demo_cols(columns: list[str]) -> dict[str, str | None]:
    return {k: _find_col(columns, v) for k, v in _DEMO_COL_CANDIDATES.items()}


# ---------------------------------------------------------------------------
# Internationalisation strings (mirrors R's .vd_labels / .vl())
# ---------------------------------------------------------------------------

_I18N: dict[str, dict[str, str]] = {
    "pt": {
        "system_detected": "Sistema detectado: {}",
        "saved_to": "Saída salva: {}",
        "sex": "Sexo",
        "race": "Raça/Cor",
        "age": "Idade (anos)",
        "age_group": "Faixa Etária",
        "ibge_age_group": "Faixa Etária",
        "education": "Escolaridade",
        "climate_risk": "Risco Climático",
        "region": "Estado/Região",
        "municipality": "Município",
        "dimension": "Dimensão",
        "demographic_summary": "Resumo Demográfico",
        "category": "Categoria",
        "count": "Contagem",
        "percent": "Percentual",
        "male": "Masculino",
        "female": "Feminino",
        "sex_ratio": "Razão de sexo (M:F)",
        "bar_title_sex": "Distribuição por Sexo",
        "bar_title_race": "Distribuição por Raça/Cor",
        "bar_title_age": "Distribuição por Idade",
        "bar_title_age_group": "Distribuição por Faixa Etária",
        "bar_title_ibge_age_group": "Distribuição por Faixa Etária",
        "bar_title_education": "Distribuição por Escolaridade",
        "bar_title_climate_risk": "Distribuição por Grupo de Risco Climático",
        "bar_title_region": "Distribuição por Estado/Região",
        "bar_title_municipality": "Distribuição por Município",
        "pyramid_title": "Distribuição etária por sexo",
        "legend_count": "Registros",
        "temporal_title": "Distribuição temporal dos registros notificados",
        "month": "Mês",
        "epi_week": "Semana Epidemiológica",
        "year": "Ano",
        "quarter": "Trimestre",
        "semester": "Semestre",
        "climate_bar_title": "Registros por grupo de risco climático",
        "climate_heat_title": "Distribuição sazonal (Mês × Estação)",
        "season": "Estação",
        "equity_title": "Equidade Racial vs. Censo Nacional (IBGE 2022)",
        "equity_subtitle": "Diferença em pontos percentuais: observado vs. Censo IBGE 2022",
        "equity_axis": "Diferença da proporção nacional (pp)",
        "overrep": "Sobre-representado",
        "underrep": "Sub-representado",
        "dashboard_title": "Perfil Demográfico",
        "source_datasus": "Fonte: DATASUS / Ministério da Saúde",
        "heatmap_title": "Perfil demográfico cruzado",
        "fill_metric_label": "Métrica",
        "within_row": "dentro da linha",
        "within_col": "dentro da coluna",
        "of_total": "do total",
        "done": "Gráfico gerado com sucesso.",
    },
    "en": {
        "system_detected": "System detected: {}",
        "saved_to": "Output saved: {}",
        "sex": "Sex",
        "race": "Race/Colour",
        "age": "Age (years)",
        "age_group": "Age Group",
        "ibge_age_group": "Age Group",
        "education": "Education",
        "climate_risk": "Climate Risk",
        "region": "State/Region",
        "municipality": "Municipality",
        "dimension": "Dimension",
        "demographic_summary": "Demographic Summary",
        "category": "Category",
        "count": "Count",
        "percent": "Percent",
        "male": "Male",
        "female": "Female",
        "sex_ratio": "Sex ratio (M:F)",
        "bar_title_sex": "Distribution by Sex",
        "bar_title_race": "Distribution by Race/Colour",
        "bar_title_age": "Distribution by Age",
        "bar_title_age_group": "Distribution by Age Group",
        "bar_title_ibge_age_group": "Distribution by Age Group",
        "bar_title_education": "Distribution by Education Level",
        "bar_title_climate_risk": "Distribution by Climate Risk Group",
        "bar_title_region": "Distribution by State/Region",
        "bar_title_municipality": "Distribution by Municipality",
        "pyramid_title": "Age-sex distribution",
        "legend_count": "Records",
        "temporal_title": "Temporal distribution of reported records",
        "month": "Month",
        "epi_week": "Epidemiological Week",
        "year": "Year",
        "quarter": "Quarter",
        "semester": "Semester",
        "climate_bar_title": "Records by climate risk group",
        "climate_heat_title": "Seasonal distribution (Month × Season)",
        "season": "Season",
        "equity_title": "Race/Colour Equity vs. National Census Benchmark",
        "equity_subtitle": (
            "Percentage-point difference: observed vs. IBGE 2022 Census proportions"
        ),
        "equity_axis": "Difference from national proportion (pp)",
        "overrep": "Over-represented",
        "underrep": "Under-represented",
        "dashboard_title": "Demographic Profile",
        "source_datasus": "Source: DATASUS / Brazilian Ministry of Health",
        "heatmap_title": "Cross-tabulated demographic profile",
        "fill_metric_label": "Metric",
        "within_row": "within row",
        "within_col": "within column",
        "of_total": "of total",
        "done": "Plot generated successfully.",
    },
    "es": {
        "system_detected": "Sistema detectado: {}",
        "saved_to": "Salida guardada: {}",
        "sex": "Sexo",
        "race": "Raza/Color",
        "age": "Edad (años)",
        "age_group": "Grupo de Edad",
        "ibge_age_group": "Grupo de Edad",
        "education": "Escolaridad",
        "climate_risk": "Riesgo Climático",
        "region": "Estado/Región",
        "municipality": "Municipio",
        "dimension": "Dimensión",
        "demographic_summary": "Resumen Demográfico",
        "category": "Categoría",
        "count": "Conteo",
        "percent": "Porcentaje",
        "male": "Masculino",
        "female": "Femenino",
        "sex_ratio": "Razón de sexo (M:F)",
        "bar_title_sex": "Distribución por Sexo",
        "bar_title_race": "Distribución por Raza/Color",
        "bar_title_age": "Distribución por Edad",
        "bar_title_age_group": "Distribución por Grupo de Edad",
        "bar_title_ibge_age_group": "Distribución por Grupo de Edad",
        "bar_title_education": "Distribución por Nivel Educativo",
        "bar_title_climate_risk": "Distribución por Grupo de Riesgo Climático",
        "bar_title_region": "Distribución por Estado/Región",
        "bar_title_municipality": "Distribución por Municipio",
        "pyramid_title": "Distribución por edad y sexo",
        "legend_count": "Registros",
        "temporal_title": "Distribución temporal de los registros notificados",
        "month": "Mes",
        "epi_week": "Semana Epidemiológica",
        "year": "Año",
        "quarter": "Trimestre",
        "semester": "Semestre",
        "climate_bar_title": "Registros por grupo de riesgo climático",
        "climate_heat_title": "Distribución estacional (Mes × Estación)",
        "season": "Estación",
        "equity_title": "Equidad Racial vs. Censo Nacional (IBGE 2022)",
        "equity_subtitle": "Diferencia en puntos porcentuales: observado vs. Censo IBGE 2022",
        "equity_axis": "Diferencia de la proporción nacional (pp)",
        "overrep": "Sobrerrepresentado",
        "underrep": "Subrepresentado",
        "dashboard_title": "Perfil Demográfico",
        "source_datasus": "Fuente: DATASUS / Ministerio de Salud de Brasil",
        "heatmap_title": "Perfil demográfico cruzado",
        "fill_metric_label": "Métrica",
        "within_row": "dentro de fila",
        "within_col": "dentro de columna",
        "of_total": "del total",
        "done": "Gráfico generado correctamente.",
    },
}

_MONTH_LABELS = {
    "pt": ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"],
    "es": ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"],
    "en": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
}


def _vl(lbl: dict[str, str], key: str, *args: Any) -> str:
    """Look up a label, falling back to the raw key (mirrors R's .vl())."""
    txt = lbl.get(key, key)
    return txt.format(*args) if args else txt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_plotnine() -> None:
    """Raise a clear ImportError if plotnine is not installed."""
    try:
        import plotnine  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "sus_data_plot_demographics requires plotnine. "
            "Install with: pip install climasus4py[plot]"
        ) from exc


def _vd_palette(name: str) -> list[str]:
    """Return the fallback hex palette for *name* (mirrors R's .vd_palette)."""
    if name not in _VD_PALETTES:
        warnings.warn(
            f"Palette {name!r} not recognised. Using 'lancet'.",
            UserWarning,
            stacklevel=3,
        )
        return _VD_PALETTES["lancet"]
    return _VD_PALETTES[name]


def _lancet_theme(base_size: float) -> Any:
    import plotnine as p9

    return p9.theme_classic(base_size=base_size) + p9.theme(
        plot_title=p9.element_text(weight="bold", size=base_size + 1, ha="left"),
        plot_subtitle=p9.element_text(color="#666666", size=base_size - 1, ha="left"),
        plot_caption=p9.element_text(color="#808080", size=base_size - 2, ha="left"),
        axis_title=p9.element_text(size=base_size - 0.5, color="#333333"),
        axis_text=p9.element_text(size=base_size - 1, color="#333333"),
        axis_line=p9.element_line(color="#4D4D4D", size=0.4),
        axis_ticks=p9.element_line(color="#999999", size=0.3),
        panel_grid_major_x=p9.element_blank(),
        panel_grid_major_y=p9.element_line(color="#E6E6E6", size=0.30),
        panel_grid_minor=p9.element_blank(),
        legend_key=p9.element_blank(),
        legend_text=p9.element_text(size=base_size - 1),
        strip_background=p9.element_rect(fill="#F2F2F2", color=None),
        strip_text=p9.element_text(weight="bold", size=base_size - 1),
        figure_size=(7, 5),
    )


def _numeric_leading_order(values: list[str]) -> list[str]:
    """Order unique string values by their leading integer, NA-values last.

    Mirrors R's ``order(suppressWarnings(as.numeric(stringr::str_extract(x,
    "^\\d+"))))`` -- values without a leading digit sequence sort after all
    numeric-leading ones (R's default ``na.last = TRUE``).
    """
    uniq = list(dict.fromkeys(values))
    leads: list[int | None] = []
    for v in uniq:
        m = re.match(r"^(\d+)", str(v))
        leads.append(int(m.group(1)) if m else None)
    order = sorted(
        range(len(uniq)),
        key=lambda i: (leads[i] is None, leads[i] if leads[i] is not None else 0),
    )
    return [uniq[i] for i in order]


def _pct_labels(n: pd.Series) -> pd.Series:
    total = n.sum()
    return (100 * n / total).round(1) if total else n * 0.0


# ---------------------------------------------------------------------------
# Internal: table
# ---------------------------------------------------------------------------


def _vd_table(df: pd.DataFrame, var: str | None, lang: str, lbl: dict[str, str]) -> pd.DataFrame:
    demo_cols = _detect_demo_cols(list(df.columns))

    if var is None:
        rows = []
        for dim, col in demo_cols.items():
            if col is None or col not in df.columns:
                continue
            counts = df[col].value_counts(dropna=True).rename("n").reset_index()
            counts.columns = ["category", "n"]
            counts["category"] = counts["category"].astype(str)
            counts["dimension"] = _vl(lbl, dim)
            counts["pct"] = _pct_labels(counts["n"])
            rows.append(counts[["dimension", "category", "n", "pct"]])
        if not rows:
            return pd.DataFrame(columns=["dimension", "category", "n", "pct"])
        tbl = pd.concat(rows, ignore_index=True)
        return tbl.sort_values("n", ascending=False).reset_index(drop=True)

    col = demo_cols.get(var)
    if col is None or col not in df.columns:
        raise ValueError(f"Column for {var!r} not found in dataset.")
    counts = df[col].value_counts(dropna=True).rename("n").reset_index()
    counts.columns = ["category", "n"]
    counts["category"] = counts["category"].astype(str)
    counts["pct"] = _pct_labels(counts["n"])
    return counts[["category", "n", "pct"]].sort_values("n", ascending=False).reset_index(
        drop=True
    )


# ---------------------------------------------------------------------------
# Internal: bar chart
# ---------------------------------------------------------------------------


def _vd_bar(
    df: pd.DataFrame,
    var: str | None,
    lang: str,
    palette: str,
    caption: str,
    base_size: float,
    lbl: dict[str, str],
) -> Any:
    import plotnine as p9

    if var is None:
        raise ValueError(
            "var is required for type='bar'. "
            "Options: sex, race, age_group, education, climate_risk, region."
        )

    demo_cols = _detect_demo_cols(list(df.columns))
    col = demo_cols.get(var)
    pal = _vd_palette(palette)

    if col is None or col not in df.columns:
        raise ValueError(
            f"Column for {var!r} not found. Run sus_data_create_variables() first."
        )

    bar_data = df.loc[df[col].notna(), [col]].copy()
    bar_data.columns = ["category"]
    bar_data["category"] = bar_data["category"].astype(str)
    counts = bar_data.value_counts("category").rename("n").reset_index()
    counts["pct"] = _pct_labels(counts["n"])
    counts = counts.sort_values("n").reset_index(drop=True)
    counts["category"] = pd.Categorical(
        counts["category"], categories=counts["category"], ordered=True
    )
    counts["pct_label"] = counts["pct"].astype(str) + "%"

    color_map = {
        cat: pal[i % len(pal)] for i, cat in enumerate(counts["category"])
    }

    p = (
        p9.ggplot(counts, p9.aes(x="category", y="n", fill="category"))
        + p9.geom_col(width=0.72, show_legend=False)
        + p9.geom_text(
            p9.aes(label="pct_label"),
            ha="left",
            size=base_size * 0.9,
            color="#4D4D4D",
            nudge_y=counts["n"].max() * 0.015 if len(counts) else 0,
        )
        + p9.coord_flip()
        + p9.scale_fill_manual(values=color_map, guide=None)
        + p9.scale_y_continuous(expand=(0, 0, 0.18, 0))
        + p9.labs(
            title=_vl(lbl, f"bar_title_{var}"),
            x=None,
            y=_vl(lbl, "count"),
            caption=caption,
        )
        + _lancet_theme(base_size)
        + p9.theme(
            axis_line_y=p9.element_blank(),
            panel_grid_major_y=p9.element_blank(),
            panel_grid_minor_y=p9.element_blank(),
        )
    )
    return p


# ---------------------------------------------------------------------------
# Internal: population pyramid
# ---------------------------------------------------------------------------


def _vd_pyramid(
    df: pd.DataFrame, lang: str, palette: str, caption: str, base_size: float, lbl: dict[str, str]
) -> Any:
    import plotnine as p9

    _ = palette  # pyramid uses a fixed 2-colour male/female map, like R
    demo_cols = _detect_demo_cols(list(df.columns))
    age_col = demo_cols["age_group"]
    sex_col = demo_cols["sex"]

    if age_col is None or sex_col is None:
        raise ValueError(
            "Population pyramid requires age group and sex columns. "
            "Run sus_data_create_variables() to generate 'age_group'."
        )

    lab_male = _vl(lbl, "male")
    lab_female = _vl(lbl, "female")

    sub = df.loc[df[age_col].notna() & df[sex_col].notna(), [age_col, sex_col]].copy()
    sub.columns = ["age_group", "sex_raw"]
    counts = sub.value_counts(["age_group", "sex_raw"]).rename("n").reset_index()
    counts["n"] = counts["n"].astype(float)

    def _sex_label(v: Any) -> str | None:
        s = str(v)
        if s in _PYRAMID_MALE_VALS:
            return lab_male
        if s in _PYRAMID_FEMALE_VALS:
            return lab_female
        return None

    counts["sex_label"] = counts["sex_raw"].map(_sex_label)
    counts = counts.loc[counts["sex_label"].notna()].copy()
    if counts.empty:
        raise ValueError(
            "Population pyramid: no rows matched the recognised sex "
            "values after decoding."
        )

    ordered_ages = _numeric_leading_order(counts["age_group"].astype(str).tolist())
    counts["age_group"] = pd.Categorical(
        counts["age_group"].astype(str), categories=ordered_ages, ordered=True
    )

    total_male = counts.loc[counts["sex_label"] == lab_male, "n"].sum()
    total_female = counts.loc[counts["sex_label"] == lab_female, "n"].sum()

    counts["n_plot"] = np.where(counts["sex_label"] == lab_male, -counts["n"], counts["n"])
    counts["pct"] = np.where(
        counts["sex_label"] == lab_male,
        (100 * counts["n"] / total_male).round(1) if total_male else np.nan,
        (100 * counts["n"] / total_female).round(1) if total_female else np.nan,
    )

    sex_ratio = round(total_male / total_female, 2) if total_female > 0 else None
    sub_txt = f"{_vl(lbl, 'sex_ratio')}: {sex_ratio}" if sex_ratio is not None else ""

    col_map = {lab_male: "#2166AC", lab_female: "#D6604D"}

    p = (
        p9.ggplot(counts, p9.aes(x="age_group", y="n_plot", fill="sex_label"))
        + p9.geom_col(width=0.82)
        + p9.geom_hline(yintercept=0, color="white", size=0.4)
        + p9.coord_flip()
        + p9.scale_y_continuous(
            labels=lambda vals: [f"{abs(float(v)):,.0f}" for v in vals]
        )
        + p9.scale_fill_manual(values=col_map, name=None)
        + p9.labs(
            title=_vl(lbl, "pyramid_title"),
            subtitle=sub_txt,
            x=_vl(lbl, "age_group"),
            y=_vl(lbl, "count"),
            caption=caption,
        )
        + _lancet_theme(base_size)
        + p9.theme(legend_position="top")
    )
    return p


# ---------------------------------------------------------------------------
# Internal: cross-demographic heatmap
# ---------------------------------------------------------------------------


def _vd_heatmap(
    df: pd.DataFrame,
    row_var: str | None,
    col_var: str | None,
    fill_metric: str,
    lang: str,
    caption: str,
    base_size: float,
    lbl: dict[str, str],
) -> Any:
    import plotnine as p9

    demo_cols = _detect_demo_cols(list(df.columns))
    avail = [k for k, v in demo_cols.items() if v is not None]

    if row_var is None:
        if "ibge_age_group" in avail:
            row_var = "ibge_age_group"
        elif "age_group" in avail:
            row_var = "age_group"
        elif avail:
            row_var = avail[0]

    if col_var is None:
        if row_var != "race" and "race" in avail:
            col_var = "race"
        elif row_var != "sex" and "sex" in avail:
            col_var = "sex"
        else:
            remaining = [v for v in avail if v != row_var]
            col_var = remaining[0] if remaining else None

    if col_var is None or row_var is None or row_var == col_var:
        raise ValueError(
            f"heatmap_row and heatmap_col must be different variables. Available: {avail}"
        )

    row_col = demo_cols.get(row_var) or _find_col(list(df.columns), (row_var,))
    col_col = demo_cols.get(col_var) or _find_col(list(df.columns), (col_var,))

    if row_col is None or row_col not in df.columns:
        raise ValueError(f"Row variable {row_var!r} not found. Available: {avail}")
    if col_col is None or col_col not in df.columns:
        raise ValueError(f"Column variable {col_var!r} not found. Available: {avail}")

    if fill_metric not in _VALID_FILL_METRICS:
        warnings.warn(
            f"Unknown fill_metric {fill_metric!r}; using 'pct_row'.",
            UserWarning,
            stacklevel=3,
        )
        fill_metric = "pct_row"

    sub = df.loc[df[row_col].notna() & df[col_col].notna(), [row_col, col_col]].copy()
    sub.columns = ["row_val", "col_val"]
    sub["row_val"] = sub["row_val"].astype(str)
    sub["col_val"] = sub["col_val"].astype(str)
    heat = sub.value_counts(["row_val", "col_val"]).rename("n").reset_index()
    heat["n"] = heat["n"].astype(float)

    if heat.empty:
        raise ValueError(f"No data after filtering NA in {row_col!r} and {col_col!r}.")

    if fill_metric == "pct_row":
        heat["fill_val"] = (100 * heat["n"] / heat.groupby("row_val")["n"].transform("sum")).round(
            1
        )
    elif fill_metric == "pct_col":
        heat["fill_val"] = (100 * heat["n"] / heat.groupby("col_val")["n"].transform("sum")).round(
            1
        )
    elif fill_metric == "pct_total":
        heat["fill_val"] = (100 * heat["n"] / heat["n"].sum()).round(1)
    else:  # "count"
        heat["fill_val"] = heat["n"]

    row_levels = _numeric_leading_order(heat["row_val"].tolist())
    all_numeric_leading = all(re.match(r"^\d+", v) for v in heat["row_val"].unique())
    if not all_numeric_leading:
        row_levels = sorted(heat["row_val"].unique())
    col_levels = sorted(heat["col_val"].unique())

    heat["row_val"] = pd.Categorical(heat["row_val"], categories=row_levels, ordered=True)
    heat["col_val"] = pd.Categorical(heat["col_val"], categories=col_levels, ordered=True)

    fill_label = {
        "pct_row": f"% {_vl(lbl, 'within_row')}",
        "pct_col": f"% {_vl(lbl, 'within_col')}",
        "pct_total": f"% {_vl(lbl, 'of_total')}",
        "count": _vl(lbl, "legend_count"),
    }[fill_metric]
    row_label = _vl(lbl, row_var) if row_var in lbl else row_col
    col_label = _vl(lbl, col_var) if col_var in lbl else col_col

    def _cell_fmt(x: float) -> str:
        return f"{x:,.0f}" if fill_metric == "count" else f"{round(x, 1)}%"

    heat["cell_label"] = heat["fill_val"].map(_cell_fmt)
    heat_threshold = heat["fill_val"].median()
    heat["text_color"] = np.where(heat["fill_val"] > heat_threshold, "white", "#333333")

    hm_seq_pal = [
        "#F7FBFF", "#DEEBF7", "#C6DBEF", "#9ECAE1",
        "#6BAED6", "#3182BD", "#08519C",
    ]

    row_levels_rev = list(reversed(row_levels))

    p = (
        p9.ggplot(heat, p9.aes(x="col_val", y="row_val", fill="fill_val"))
        + p9.geom_tile(color="white", size=0.3)
        + p9.geom_text(
            p9.aes(label="cell_label", color="text_color"),
            size=base_size * 0.9,
            show_legend=False,
        )
        + p9.scale_color_manual(
            values={"white": "white", "#333333": "#333333"}, guide=None
        )
        + p9.scale_fill_gradientn(colors=hm_seq_pal, name=fill_label, na_value="#F2F2F2")
        + p9.scale_x_discrete(limits=col_levels)
        + p9.scale_y_discrete(limits=row_levels_rev)
        + p9.labs(
            title=f"{_vl(lbl, 'heatmap_title')}: {row_label} x {col_label}",
            subtitle=f"{_vl(lbl, 'fill_metric_label')}: {fill_label}",
            x=col_label,
            y=row_label,
            caption=caption,
        )
        + p9.theme_classic(base_size=base_size)
        + p9.theme(
            axis_text_x=p9.element_text(angle=30, ha="right", size=base_size * 0.85),
            axis_text_y=p9.element_text(size=base_size * 0.85),
            axis_ticks=p9.element_blank(),
            axis_line=p9.element_blank(),
            panel_grid=p9.element_blank(),
            legend_position="right",
            plot_title=p9.element_text(weight="bold", size=base_size + 1),
            plot_subtitle=p9.element_text(color="#808080", size=base_size - 1),
        )
    )
    return p


# ---------------------------------------------------------------------------
# Internal: temporal epidemic curve
# ---------------------------------------------------------------------------


def _vd_temporal(
    df: pd.DataFrame,
    time_unit: str,
    fill_var: str | None,
    show_ci: bool,
    lang: str,
    palette: str,
    caption: str,
    base_size: float,
    lbl: dict[str, str],
) -> Any:
    import plotnine as p9

    columns = list(df.columns)
    time_col = _find_col(columns, _TIME_COL_CANDIDATES[time_unit])
    if time_col is None:
        raise ValueError(
            f"No column found for {time_unit!r}. "
            "Run sus_data_create_variables() to generate temporal variables."
        )

    pal = _vd_palette(palette)
    fill_col = None
    if fill_var is not None:
        candidates = (fill_var, f"{fill_var}_group", fill_var.replace("_", "."))
        fill_col = _find_col(columns, candidates)
        if fill_col is None:
            warnings.warn(
                f"Column {fill_var!r} not found; ignoring stratification.",
                UserWarning,
                stacklevel=3,
            )

    if fill_col is not None:
        sub = df.loc[df[time_col].notna() & df[fill_col].notna(), [time_col, fill_col]].copy()
        sub.columns = ["time_x", "group"]
        agg = sub.value_counts(["time_x", "group"]).rename("n").reset_index()
    else:
        sub = df.loc[df[time_col].notna(), [time_col]].copy()
        sub.columns = ["time_x"]
        agg = sub.value_counts("time_x").rename("n").reset_index()
        agg["group"] = _vl(lbl, "count")
    agg["n"] = agg["n"].astype(float)
    agg["time_x"] = agg["time_x"].astype(str)

    if show_ci:
        from scipy.stats import poisson

        agg["ci_lo"] = poisson.ppf(0.025, agg["n"])
        agg["ci_hi"] = poisson.ppf(0.975, agg["n"])

    x_label = _vl(lbl, time_unit)
    groups = agg["group"].unique().tolist()
    fill_vals = {g: pal[i % len(pal)] for i, g in enumerate(groups)}

    agg = agg.sort_values("time_x")

    p = (
        p9.ggplot(agg, p9.aes(x="time_x", y="n", color="group", group="group"))
        + p9.geom_line(size=0.8)
        + p9.geom_point(size=1.8, shape="o")
    )

    if show_ci:
        p = (
            p
            + p9.geom_ribbon(
                p9.aes(ymin="ci_lo", ymax="ci_hi", fill="group"),
                alpha=0.12,
                color=None,
                show_legend=False,
            )
            + p9.scale_fill_manual(values=fill_vals)
        )

    p = (
        p
        + p9.scale_color_manual(values=fill_vals, name=None)
        + p9.scale_y_continuous(expand=(0, 0, 0.08, 0))
        + p9.labs(
            title=_vl(lbl, "temporal_title"),
            x=x_label,
            y=_vl(lbl, "count"),
            caption=caption,
        )
        + _lancet_theme(base_size)
        + p9.theme(legend_position="top", axis_text_x=p9.element_text(angle=45, ha="right"))
    )
    return p


# ---------------------------------------------------------------------------
# Internal: climate-risk distribution
# ---------------------------------------------------------------------------


def _vd_climate(
    df: pd.DataFrame, lang: str, palette: str, caption: str, base_size: float, lbl: dict[str, str]
) -> Any:
    import plotnine as p9

    columns = list(df.columns)
    clim_col = _find_col(columns, ("climate_risk_group", "grupo_risco_climatico"))
    season_col = _find_col(columns, ("astronomical_season", "estacao_astronomica", "season"))
    month_col = _find_col(columns, ("month", "mes"))
    pal = _vd_palette(palette)

    if clim_col is None:
        raise ValueError(
            "Column 'climate_risk_group' not found. "
            "Run sus_data_create_variables() to generate climate risk variables."
        )

    bar_d = df.loc[df[clim_col].notna(), [clim_col]].copy()
    bar_d.columns = ["group"]
    bar_d["group"] = bar_d["group"].astype(str)
    counts = bar_d.value_counts("group").rename("n").reset_index()
    counts["pct"] = _pct_labels(counts["n"])
    counts = counts.sort_values("n").reset_index(drop=True)
    counts["group"] = pd.Categorical(counts["group"], categories=counts["group"], ordered=True)
    counts["pct_label"] = counts["pct"].astype(str) + "%"

    n_rows = len(counts)
    from matplotlib.colors import LinearSegmentedColormap, to_hex

    if n_rows <= 1:
        clim_pal = [pal[0]] * max(n_rows, 1)
    else:
        cmap = LinearSegmentedColormap.from_list("_vd_clim", ["#f7f7f7", pal[0]])
        clim_pal = [to_hex(cmap(i / (n_rows - 1))) for i in range(n_rows)]
    color_map = dict(zip(counts["group"], clim_pal, strict=False))

    p_bar = (
        p9.ggplot(counts, p9.aes(x="group", y="n", fill="group"))
        + p9.geom_col(width=0.7, show_legend=False)
        + p9.geom_text(
            p9.aes(label="pct_label"), ha="left", size=base_size * 0.9, color="#4D4D4D"
        )
        + p9.coord_flip()
        + p9.scale_fill_manual(values=color_map, guide=None)
        + p9.scale_y_continuous(expand=(0, 0, 0.2, 0))
        + p9.labs(title=_vl(lbl, "climate_bar_title"), x=None, y=_vl(lbl, "count"))
        + _lancet_theme(base_size)
    )

    if season_col is not None and month_col is not None:
        month_labs = _MONTH_LABELS.get(lang, _MONTH_LABELS["en"])

        heat_sub = df.loc[
            df[month_col].notna() & df[season_col].notna(), [month_col, season_col]
        ].copy()
        heat_sub.columns = ["month", "season"]
        heat_d = heat_sub.value_counts(["month", "season"]).rename("n").reset_index()
        heat_d["month"] = pd.Categorical(
            heat_d["month"].astype(str),
            categories=[str(m) for m in range(1, 13)],
            ordered=True,
        )
        heat_d["n_label"] = heat_d["n"].map(lambda v: f"{v:,.0f}")

        p_heat = (
            p9.ggplot(heat_d, p9.aes(x="month", y="season", fill="n"))
            + p9.geom_tile(color="white", size=0.4)
            + p9.geom_text(p9.aes(label="n_label"), size=base_size * 0.7, color="#333333")
            + p9.scale_x_discrete(labels=month_labs)
            + p9.scale_fill_gradientn(colors=["#f7f7f7", pal[1 % len(pal)]], name=_vl(lbl, "count"))
            + p9.labs(
                title=_vl(lbl, "climate_heat_title"),
                x=_vl(lbl, "month"),
                y=_vl(lbl, "season"),
            )
            + _lancet_theme(base_size)
            + p9.theme(axis_text_x=p9.element_text(size=base_size - 1), legend_position="right")
        )
        # NOTE: plotnine's composition (`/`, `|`) has no patchwork::
        # plot_annotation() equivalent, so a caption cannot be attached to
        # the combined figure directly -- it is attached to the bottom
        # (season heatmap) panel instead. See IDEIAS.md.
        p_heat = p_heat + p9.labs(caption=caption)
        return p_bar / p_heat

    return p_bar + p9.labs(caption=caption)


# ---------------------------------------------------------------------------
# Internal: race/colour equity diverging plot
# ---------------------------------------------------------------------------


def _vd_race_equity(
    df: pd.DataFrame,
    benchmark: dict[str, float] | None,
    lang: str,
    caption: str,
    base_size: float,
    lbl: dict[str, str],
) -> Any:
    import plotnine as p9

    columns = list(df.columns)
    race_col = _find_col(columns, ("race", "raca", "raza", "RACACOR", "RACA_COR"))
    if race_col is None:
        raise ValueError("Race/colour column not found. Expected: race, raca, RACACOR.")

    sub = df.loc[df[race_col].notna(), [race_col]].copy()
    sub.columns = ["race"]
    sub["race"] = sub["race"].astype(str)
    obs_tab = sub.value_counts("race").rename("n").reset_index()
    obs_tab["obs_pct"] = 100 * obs_tab["n"] / obs_tab["n"].sum()

    ref_table = benchmark if benchmark is not None else _IBGE_2022_RACE
    obs_tab["ref_pct"] = obs_tab["race"].map(ref_table)
    obs_tab = obs_tab.loc[obs_tab["ref_pct"].notna()].copy()

    if obs_tab.empty:
        raise ValueError(
            "No race/colour categories matched the reference benchmark. "
            "Pass benchmark= with keys matching the observed category labels."
        )

    obs_tab["diff"] = (obs_tab["obs_pct"] - obs_tab["ref_pct"]).round(2)
    obs_tab["dir"] = np.where(
        obs_tab["diff"] >= 0, _vl(lbl, "overrep"), _vl(lbl, "underrep")
    )
    obs_tab = obs_tab.sort_values("diff").reset_index(drop=True)
    obs_tab["race"] = pd.Categorical(obs_tab["race"], categories=obs_tab["race"], ordered=True)
    obs_tab["diff_label"] = obs_tab["diff"].map(lambda x: f"{'+' if x > 0 else ''}{x} pp")

    col_over, col_under = "#B22222", "#1B6CA8"
    fill_map = {_vl(lbl, "overrep"): col_over, _vl(lbl, "underrep"): col_under}

    p = (
        p9.ggplot(obs_tab, p9.aes(x="race", y="diff", fill="dir"))
        + p9.geom_col(width=0.68)
        + p9.geom_hline(yintercept=0, size=0.6, color="#4D4D4D")
        + p9.coord_flip()
        + p9.scale_fill_manual(values=fill_map, name=None)
        + p9.labs(
            title=_vl(lbl, "equity_title"),
            subtitle=_vl(lbl, "equity_subtitle"),
            x=_vl(lbl, "race"),
            y=_vl(lbl, "equity_axis"),
            caption=caption,
        )
        + _lancet_theme(base_size)
        + p9.theme(legend_position="top")
    )
    return p


# ---------------------------------------------------------------------------
# Internal: composite dashboard
# ---------------------------------------------------------------------------


def _vd_dashboard(
    df: pd.DataFrame,
    lang: str,
    palette: str,
    caption: str,
    base_size: float,
    show_ci: bool,
    lbl: dict[str, str],
) -> Any:
    def _try_panel(fn: Any) -> Any:
        try:
            return fn()
        except (ValueError, KeyError) as exc:
            warnings.warn(f"Panel skipped: {exc}", UserWarning, stacklevel=3)
            return None

    p_pyr = _try_panel(lambda: _vd_pyramid(df, lang, palette, caption, base_size, lbl))
    p_time = _try_panel(
        lambda: _vd_temporal(df, "month", None, show_ci, lang, palette, caption, base_size, lbl)
    )
    p_sex = _try_panel(lambda: _vd_bar(df, "sex", lang, palette, caption, base_size, lbl))
    p_race = _try_panel(lambda: _vd_bar(df, "race", lang, palette, caption, base_size, lbl))
    p_age = _try_panel(lambda: _vd_bar(df, "age_group", lang, palette, caption, base_size, lbl))
    p_heat = _try_panel(
        lambda: _vd_heatmap(df, None, None, "pct_row", lang, caption, base_size, lbl)
    )

    import plotnine as p9

    n_rows = len(df)
    dashboard_subtitle = f"N = {n_rows:,}"
    # NOTE: plotnine's composition (`/`, `|`) has no patchwork::
    # plot_annotation() equivalent for attaching a title/subtitle/caption to
    # the *combined* figure, so they are attached to the first panel
    # instead. See IDEIAS.md.
    title_labs = p9.labs(
        title=_vl(lbl, "dashboard_title"), subtitle=dashboard_subtitle, caption=caption
    )
    if p_pyr is not None:
        p_pyr = p_pyr + title_labs
    elif p_sex is not None:
        p_sex = p_sex + title_labs

    left_plots = [p for p in (p_pyr, p_time) if p is not None]
    right_plots = [p for p in (p_sex, p_race, p_age, p_heat) if p is not None]
    all_plots = left_plots + right_plots

    if not all_plots:
        raise ValueError("No valid panels could be generated for the dashboard.")

    if len(left_plots) >= 2 and len(right_plots) >= 2:
        left_col = left_plots[0] / left_plots[1]
        right_col = (right_plots[0] | right_plots[1]) / (right_plots[2] | right_plots[3])
        combined = left_col | right_col
    else:
        combined = all_plots[0]
        for extra in all_plots[1:]:
            combined = combined | extra

    return combined


# ---------------------------------------------------------------------------
# Save helper (mirrors R's .vd_save, minus gt/DT/htmlwidgets -- not bundled)
# ---------------------------------------------------------------------------


def _vd_save(
    out: Any, save_path: str, width: float, height: float, dpi: int, lbl: dict[str, str]
) -> None:
    if hasattr(out, "save"):
        out.save(save_path, width=width, height=height, dpi=dpi, verbose=False)
        print(_vl(lbl, "saved_to", save_path))
    elif isinstance(out, pd.DataFrame):
        ext = save_path.rsplit(".", 1)[-1].lower() if "." in save_path else ""
        if ext == "csv":
            out.to_csv(save_path, index=False)
            print(_vl(lbl, "saved_to", save_path))
        elif ext == "html":
            out.to_html(save_path, index=False)
            print(_vl(lbl, "saved_to", save_path))
        else:
            warnings.warn(
                f"Cannot auto-save a table to extension '.{ext}'. "
                "Use .csv or .html, or call the table's own save method.",
                UserWarning,
                stacklevel=2,
            )
    else:
        warnings.warn(
            f"Cannot auto-save object of type {type(out).__name__!r}.",
            UserWarning,
            stacklevel=2,
        )


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------


def sus_data_plot_demographics(
    df: pd.DataFrame | duckdb.DuckDBPyRelation,
    type: str = "table",
    var: str | None = None,
    time_unit: str = "month",
    fill_var: str | None = None,
    palette: str = "lancet",
    heatmap_row: str | None = None,
    heatmap_col: str | None = None,
    fill_metric: str = "pct_row",
    show_ci: bool = False,
    benchmark: dict[str, float] | None = None,
    interactive: bool = False,
    base_size: float = 11,
    lang: str = "pt",
    subtitle: str | None = None,
    caption: str | None = None,
    theme_style: str = "publication",
    caption_suffix: str | None = None,
    save_path: str | None = None,
    width: float | None = None,
    height: float | None = None,
    dpi: int = 300,
    verbose: bool = True,
    **kwargs: Any,
) -> Any:
    """Visualise demographic profiles from a standardised DATASUS dataset.

    Produces frequency tables, ``plotnine`` charts, or a composite dashboard
    summarising the demographic and climate-risk composition of a
    standardised SUS/DATASUS table (SIM, SIH, SINAN, CNES, SIA, SINASC).
    Mirrors ``climasus4r::sus_data_plot_demographics()``.

    Requires the optional ``[plot]`` extra::

        pip install climasus4py[plot]

    Args:
        df: Table with demographic columns (``sex``, ``race``, ``age_group``,
            ``education``, etc.). Accepts a ``pandas.DataFrame`` or a
            ``duckdb.DuckDBPyRelation`` (the latter is materialised via
            ``.df()`` immediately). Run ``sus_data_create_variables`` first
            to generate ``age_group`` and other derived columns.
        type: Visualisation type. One of ``"table"`` (frequency table,
            returned as a ``pandas.DataFrame``), ``"bar"`` (horizontal bar
            chart for one variable), ``"pyramid"`` (age-sex population
            pyramid), ``"heatmap"`` (cross-demographic tile matrix),
            ``"temporal"`` (epidemic curve), ``"climate"`` (climate-risk
            group distribution), ``"race_equity"`` (race/colour equity
            diverging plot vs. a census benchmark), or ``"dashboard"``
            (composite panel). Default ``"table"``.
        var: Demographic variable, required for ``"bar"`` and used
            optionally for single-variable ``"table"``. One of ``"sex"``,
            ``"race"``, ``"age_group"``, ``"education"``, ``"climate_risk"``,
            ``"region"`` (also accepts ``"age"``, ``"ibge_age_group"``,
            ``"municipality"`` -- undocumented in the R source but reachable
            the same way there).
        time_unit: Temporal resolution for ``type = "temporal"``. One of
            ``"month"`` (default), ``"epi_week"``, ``"year"``, ``"quarter"``,
            ``"semester"``.
        fill_var: Optional stratification variable for temporal plots (e.g.
            ``"sex"``, ``"age_group"``, ``"climate_risk_group"``).
        palette: Colour palette. One of ``"lancet"`` (default), ``"nature"``,
            ``"nejm"``, ``"jco"``, ``"aaas"``, ``"sus"``, ``"viridis"``,
            ``"colorblind"``, ``"science"``. climasus4py has no Python
            binding of the R ``ggsci`` package, so these are the literal
            hardcoded fallback hex values ``ggsci`` would otherwise
            override -- see IDEIAS.md.
        heatmap_row: Row variable for ``type = "heatmap"``. Defaults to
            ``"age_group"`` (auto-detected).
        heatmap_col: Column variable for ``type = "heatmap"``. Defaults to
            ``"race"`` (auto-detected). Must differ from *heatmap_row*.
        fill_metric: What to show in each ``"heatmap"`` tile. One of
            ``"pct_row"`` (default), ``"pct_col"``, ``"pct_total"``,
            ``"count"``.
        show_ci: Add 95% Poisson confidence intervals in temporal plots.
            Default ``False``.
        benchmark: Reference proportions (percent) for the race-equity plot,
            keyed by the observed race/colour category label. If ``None``
            (default), IBGE 2022 Census national proportions are used.
        interactive: Return an interactive widget instead of a static plot.
            **Not currently supported** -- ``plotly`` is not bundled with
            climasus4py; raises ``ImportError``. See IDEIAS.md.
        base_size: Base font size for the plotnine theme. Default ``11``.
        lang: Language for labels and messages: ``"pt"`` (default), ``"en"``,
            ``"es"``.
        subtitle: Figure subtitle. **Unused** -- mirrors an R quirk where
            this parameter is declared but never referenced by any internal
            plot helper. Kept for signature parity; see IDEIAS.md.
        caption: Figure caption. ``None`` auto-generates a DATASUS source
            string for the selected language.
        theme_style: Reserved for future theme variants. Currently only
            ``"publication"`` is implemented (mirrors the R source, which
            never branches on this parameter either).
        caption_suffix: Additional text appended to the auto-generated
            caption. **Only applied when** *caption* **is** ``None`` --
            mirrors an R quirk where an explicit *caption* silently ignores
            *caption_suffix*; see IDEIAS.md.
        save_path: File path to save output. ``plotnine`` plots are saved
            via ``ggplot.save()``; ``"table"`` output (a ``DataFrame``) is
            saved via ``.to_csv()``/``.to_html()`` depending on the
            extension (no ``gt``/``DT``/``htmlwidgets`` Python equivalent is
            bundled -- see IDEIAS.md). Default ``None`` (no file saved).
        width: Output width in inches. Defaults: 7 for single plots, 12 for
            the dashboard.
        height: Output height in inches. Defaults: 5 for single plots, 9 for
            the dashboard.
        dpi: Resolution for raster output. Default ``300``.
        verbose: Print progress messages. Default ``True``.
        **kwargs: Accepted for signature parity with R's ``...`` -- unused
            (the R source also never reads its own ``...`` inside the
            dispatched helpers).

    Returns:
        A ``pandas.DataFrame`` (``type="table"``), a ``plotnine.ggplot``
        object, or a plotnine composed plot (``type="dashboard"`` and the
        ``"climate"`` type when a season/month heatmap panel is available).

    Raises:
        ImportError: If ``plotnine`` is not installed
            (``pip install climasus4py[plot]``), or if ``interactive=True``.
        ValueError: If *type*, *time_unit*, or *fill_metric* is invalid, or
            a required demographic/temporal column cannot be found.

    Example:
        >>> import climasus4py as cs
        >>> p = cs.sus_data_plot_demographics(df, type="pyramid", lang="pt")
        >>> p.draw()
        >>> tbl = cs.sus_data_plot_demographics(df, type="table", lang="en")
    """
    if lang not in _I18N:
        warnings.warn(f"lang {lang!r} not supported. Using 'pt'.", UserWarning, stacklevel=2)
        lang = "pt"
    lbl = _I18N[lang]

    if type not in _VALID_TYPES:
        raise ValueError(f"type must be one of {list(_VALID_TYPES)!r}, got {type!r}.")

    if time_unit not in _VALID_TIME_UNITS:
        raise ValueError(
            f"time_unit must be one of {list(_VALID_TIME_UNITS)!r}, got {time_unit!r}."
        )

    if interactive:
        raise ImportError(
            "interactive=True requires the optional 'plotly' dependency, which "
            "climasus4py does not currently bundle (unlike climasus4r's plotly "
            "path). Install plotly manually if needed; see IDEIAS.md."
        )

    _require_plotnine()

    # Detect system before materialising (sus_meta only works on the lazy
    # DuckDB relation), mirroring R's tryCatch(sus_meta(df, "system")).
    system_id: str = "unknown"
    if not isinstance(df, pd.DataFrame):
        try:
            detected = sus_meta(df, field="system")
            system_id = str(detected) if detected else "unknown"
        except (TypeError, ValueError):
            system_id = "unknown"
        df = df.df()
    else:
        df = df.copy()

    if hasattr(df, "geometry") and "geometry" in df.columns:
        df = pd.DataFrame(df.drop(columns=["geometry"]))

    if verbose:
        print(_vl(lbl, "system_detected", system_id))

    cap_base = _vl(lbl, "source_datasus")
    cap_with_suffix = f"{cap_base} | {caption_suffix}" if caption_suffix is not None else cap_base
    resolved_caption = caption if caption is not None else f"{cap_with_suffix} | climasus4py"

    if type == "table":
        out = _vd_table(df, var, lang, lbl)
    elif type == "bar":
        out = _vd_bar(df, var, lang, palette, resolved_caption, base_size, lbl)
    elif type == "pyramid":
        out = _vd_pyramid(df, lang, palette, resolved_caption, base_size, lbl)
    elif type == "heatmap":
        out = _vd_heatmap(
            df, heatmap_row, heatmap_col, fill_metric, lang, resolved_caption, base_size, lbl
        )
    elif type == "temporal":
        out = _vd_temporal(
            df, time_unit, fill_var, show_ci, lang, palette, resolved_caption, base_size, lbl
        )
    elif type == "climate":
        out = _vd_climate(df, lang, palette, resolved_caption, base_size, lbl)
    elif type == "race_equity":
        out = _vd_race_equity(df, benchmark, lang, resolved_caption, base_size, lbl)
    else:  # "dashboard"
        out = _vd_dashboard(df, lang, palette, resolved_caption, base_size, show_ci, lbl)

    if save_path is not None:
        _vd_save(
            out,
            save_path,
            width if width is not None else (12 if type == "dashboard" else 7),
            height if height is not None else (9 if type == "dashboard" else 5),
            dpi,
            lbl,
        )

    if verbose:
        print(_vl(lbl, "done"))

    return out
