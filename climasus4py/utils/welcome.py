"""Pipeline overview banner — sus_welcome.

Prints a colour-coded overview of the climasus4py pipeline stages to the
console and, optionally, writes a self-contained HTML page and opens it in
the default browser.

Mirrors R: climasus4r::sus_welcome(). This is a display-only utility, not a
pipeline/enrichment function — it never touches a ``DuckDBPyRelation`` and
has nothing to materialize lazily.
"""

from __future__ import annotations

import os
import sys
import tempfile
import webbrowser
from html import escape as _esc
from pathlib import Path
from typing import TypedDict

from rich.console import Console

from .._version import __version__


class Stage(TypedDict):
    num: int
    color: str
    name: dict[str, str]
    fns: list[str]
    desc: dict[str, str]


_LANGS = ("pt", "en", "es")
_OUTPUTS = ("console", "html")

# color -> rich style name / CSS hex pair
_PALETTE = {
    "blue": ("bold blue", "#4c6ef5"),
    "green": ("bold green", "#2f9e44"),
    "cyan": ("bold cyan", "#0ca678"),
    "magenta": ("bold magenta", "#7950f2"),
    "yellow": ("bold yellow", "#e67700"),
    "slate": ("bold grey58", "#64748b"),
}

# Stage list mirrors R's sus_welcome() stage table, with `fns` updated to the
# function names actually exported by climasus4py's __init__.py (per the
# rename table in docs/migration-from-r-legacy.md) rather than the R names —
# a banner listing functions the user can't call would be misleading. R's
# RAP section (sus_rap_*) is dropped: no RAP builder exists in climasus4py
# yet (see IDEIAS.md).
_STAGES: list[Stage] = [
    {
        "num": 1,
        "color": "blue",
        "name": {"pt": "IMPORTAÇÃO", "en": "IMPORT", "es": "IMPORTACIÓN"},
        "fns": ["sus_data_import()", "sus_data_read()", "sus_sql()"],
        "desc": {
            "pt": "Importa dados do DATASUS com cache automático, ou entra via Parquet/SQL",
            "en": "Imports DATASUS data with automatic caching, or enters via Parquet/SQL",
            "es": "Importa datos del DATASUS con caché automático, o entra vía Parquet/SQL",
        },
    },
    {
        "num": 2,
        "color": "blue",
        "name": {"pt": "PREPARAÇÃO E LIMPEZA", "en": "DATA PREPARATION", "es": "PREPARACIÓN"},
        "fns": ["sus_data_clean_encoding()", "sus_data_standardize()"],
        "desc": {
            "pt": "Corrige encoding e padroniza nomes de colunas para o esquema "
                  "canônico do sistema",
            "en": "Fixes encoding and standardises column names to the system's "
                  "canonical schema",
            "es": "Corrige encoding y estandariza nombres de columnas al esquema "
                  "canónico del sistema",
        },
    },
    {
        "num": 3,
        "color": "blue",
        "name": {
            "pt": "FILTRAGEM E DERIVAÇÃO",
            "en": "FILTERING & VARIABLES",
            "es": "FILTRADO Y DERIVACIÓN",
        },
        "fns": ["sus_filter()", "sus_data_create_variables()"],
        "desc": {
            "pt": "Filtra por CID-10 e variáveis demográficas, cria variáveis "
                  "derivadas (idade, sazonalidade)",
            "en": "Filters by ICD-10 and demographic variables, creates derived "
                  "variables (age, seasonality)",
            "es": "Filtra por CIE-10 y variables demográficas, crea variables "
                  "derivadas (edad, estacionalidad)",
        },
    },
    {
        "num": 4,
        "color": "blue",
        "name": {"pt": "AGREGAÇÃO", "en": "AGGREGATION", "es": "AGREGACIÓN"},
        "fns": ["sus_data_aggregate()", "sus_data_quality_report()", "sus_data_ts_quality()"],
        "desc": {
            "pt": "Agrega registros por município/data, gera relatório de qualidade "
                  "e completude de séries",
            "en": "Aggregates records by municipality/date, generates a quality and "
                  "time series completeness report",
            "es": "Agrega registros por municipio/fecha, genera reporte de calidad "
                  "y completitud de series",
        },
    },
    {
        "num": 5,
        "color": "green",
        "name": {"pt": "INTEGRAÇÃO ESPACIAL", "en": "SPATIAL JOIN", "es": "INTEGRACIÓN ESPACIAL"},
        "fns": ["sus_spatial_join()"],
        "desc": {
            "pt": "Vincula dados de saúde a polígonos municipais e estaduais brasileiros",
            "en": "Links health data to Brazilian municipal and state polygons",
            "es": "Vincula datos de salud a polígonos municipales y estatales de Brasil",
        },
    },
    {
        "num": 6,
        "color": "cyan",
        "name": {"pt": "INTEGRAÇÃO CLIMÁTICA", "en": "CLIMATE DATA", "es": "DATOS CLIMÁTICOS"},
        "fns": [
            "sus_climate()", "sus_climate_aggregate()", "sus_climate_fill_inmet()",
            "sus_fill_gaps()",
            "sus_grid_era5()", "sus_grid_chirps()", "sus_grid_koppen()", "sus_grid_pdsi()",
            "sus_grid_prodes()", "sus_grid_smvi()", "sus_grid_fires()",
            "sus_grid_pollution_cams()", "sus_grid_pollution_ghap()",
            "sus_grid_pollution_merra2()",
            "sus_climate_compute_heatwaves()", "sus_climate_compute_coldwaves()",
            "sus_climate_compute_spei()", "sus_climate_compute_spi()", "sus_climate_anomaly()",
        ],
        "desc": {
            "pt": "ERA5, CHIRPS, qualidade do ar (CAMS/GHAP/MERRA-2), incêndios, "
                  "Köppen, ondas de calor/frio, SPEI/SPI",
            "en": "ERA5, CHIRPS, air quality (CAMS/GHAP/MERRA-2), fires, Köppen, "
                  "heat/cold waves, SPEI/SPI",
            "es": "ERA5, CHIRPS, calidad del aire (CAMS/GHAP/MERRA-2), incendios, "
                  "Köppen, olas de calor/frío, SPEI/SPI",
        },
    },
    {
        "num": 7,
        "color": "cyan",
        "name": {"pt": "SOCIOECONÔMICO", "en": "SOCIOECONOMIC", "es": "SOCIOECONÓMICO"},
        "fns": [
            "sus_census()", "sus_census_select()",
            "sus_socio_compute_indicators()", "sus_socio_list_indicators()",
        ],
        "desc": {
            "pt": "Adiciona indicadores do Censo IBGE e índices compostos de "
                  "vulnerabilidade socioeconômica",
            "en": "Adds IBGE Census indicators and composite socioeconomic "
                  "vulnerability indices",
            "es": "Agrega indicadores del Censo IBGE e índices compuestos de "
                  "vulnerabilidad socioeconómica",
        },
    },
    {
        "num": 8,
        "color": "magenta",
        "name": {
            "pt": "MODELAGEM EPIDEMIOLÓGICA",
            "en": "EPIDEMIOLOGICAL MODELLING",
            "es": "MODELADO EPIDEMIOLÓGICO",
        },
        "fns": [
            "sus_mod_dlnm()", "sus_mod_af()", "sus_mod_burden()", "sus_mod_casecrossover()",
            "sus_mod_excess()", "sus_mod_its()", "sus_mod_sensitivity()", "sus_mod_ml()",
            "sus_mod_spatial_weights()", "sus_mod_spatial_moran()", "sus_mod_spatial_reg()",
            "sus_mod_spatial_bayes()", "sus_mod_spacetime_bayes()",
            "sus_mod_spacetime_exceedance()",
            "sus_mod_spacetime_predict()", "sus_mod_vulnerability_index()",
        ],
        "desc": {
            "pt": "DLNM, fração atribuível, carga de doença, caso-cruzado, ITS, ML "
                  "e análise espacial/espaço-temporal",
            "en": "DLNM, attributable fraction, disease burden, case-crossover, "
                  "ITS, ML and spatial/spatiotemporal analysis",
            "es": "DLNM, fracción atribuible, carga de enfermedad, caso-cruzado, "
                  "ITS, ML y análisis espacial/espaciotemporal",
        },
    },
    {
        "num": 9,
        "color": "yellow",
        "name": {"pt": "VISUALIZAÇÃO", "en": "VISUALIZATION", "es": "VISUALIZACIÓN"},
        "fns": [
            "sus_data_plot_demographics()", "sus_data_plot_aggregate_ts()",
            "sus_data_plot_aggregate_map()",
            "sus_climate_plot_aggregate()", "sus_climate_plot_fill()",
            "sus_climate_plot_heatwaves()", "sus_climate_plot_coldwaves()",
            "sus_mod_plot_af()", "sus_mod_plot_burden()", "sus_mod_plot_sensitivity()",
            "sus_mod_plot_ml()",
            "sus_mod_plot_spacetime()", "sus_mod_plot_spatial_bayes()",
            "sus_mod_plot_spatial_moran()",
            "sus_mod_plot_vulnerability()",
        ],
        "desc": {
            "pt": "Pirâmide etária, série temporal, mapa coroplético; ondas de "
                  "calor/frio; forest plot, carga, vulnerabilidade",
            "en": "Demographic pyramid, time series, choropleth map; heat/cold "
                  "waves; forest plot, burden, vulnerability",
            "es": "Pirámide demográfica, serie temporal, mapa coroplético; olas de "
                  "calor/frío; forest plot, carga, vulnerabilidad",
        },
    },
    {
        "num": 10,
        "color": "yellow",
        "name": {"pt": "EXPORTAÇÃO", "en": "EXPORT", "es": "EXPORTACIÓN"},
        "fns": ["sus_export()", "materialize()"],
        "desc": {
            "pt": "Exporta para CSV/Parquet/DuckDB ou materializa em memória "
                  "(pandas/pyarrow/polars)",
            "en": "Exports to CSV/Parquet/DuckDB or materializes in memory "
                  "(pandas/pyarrow/polars)",
            "es": "Exporta a CSV/Parquet/DuckDB o materializa en memoria "
                  "(pandas/pyarrow/polars)",
        },
    },
    {
        "num": 0,
        "color": "slate",
        "name": {"pt": "UTILITÁRIOS", "en": "UTILITIES", "es": "UTILIDADES"},
        "fns": [
            "sus_meta()", "sus_explore()", "sus_data_cid_select()", "sus_cache_info()",
            "sus_cache_clear()", "update_climasus_data()", "sus_chat()",
        ],
        "desc": {
            "pt": "sus_meta() lê/escreve metadados do pipeline · sus_explore() "
                  "inspeciona uma relação lazy · sus_chat() abre o assistente de IA",
            "en": "sus_meta() reads/writes pipeline metadata · sus_explore() "
                  "inspects a lazy relation · sus_chat() opens the AI assistant",
            "es": "sus_meta() lee/escribe metadatos del pipeline · sus_explore() "
                  "inspecciona una relación lazy · sus_chat() abre el asistente de IA",
        },
    },
]

_TITLE = {
    "pt": "Pipeline Integrado de Análise Saúde–Clima–Ambiente no Brasil",
    "en": "Integrated Health–Climate–Environment Analysis Pipeline for Brazil",
    "es": "Pipeline Integrado de Análisis Salud–Clima–Ambiente en Brasil",
}
_LABELS = {
    "pt": {"lang": "Idioma", "docs": "Docs", "util": "Utilitários"},
    "en": {"lang": "Language", "docs": "Docs", "util": "Utilities"},
    "es": {"lang": "Idioma", "docs": "Docs", "util": "Utilidades"},
}


def sus_welcome(
    lang: str = "pt",
    output: str | tuple[str, ...] | list[str] = ("console", "html"),
    open: bool = True,
) -> str | None:
    """Display the climasus4py pipeline overview.

    Args:
        lang: Display language — one of ``"pt"`` (default), ``"en"``, ``"es"``.
        output: Outputs to produce — ``"console"``, ``"html"``, or both
            (default). Accepts a single string or a sequence.
        open: If ``True`` (default) and running in an interactive terminal,
            opens the generated HTML file in the default browser.

    Returns:
        The path to the generated HTML file, or ``None`` when ``"html"`` is
        not in *output*.

    Raises:
        ValueError: If *lang* or *output* contain an unsupported value.

    Examples:
        >>> import climasus4py as cs
        >>> cs.sus_welcome(lang="en", output="console")
    """
    if lang not in _LANGS:
        raise ValueError(f"lang must be one of {_LANGS!r}, got {lang!r}")

    outputs = (output,) if isinstance(output, str) else tuple(output)
    for o in outputs:
        if o not in _OUTPUTS:
            raise ValueError(f"output must be a subset of {_OUTPUTS!r}, got {o!r}")

    if "console" in outputs:
        _console(lang)

    html_path = None
    if "html" in outputs:
        html_path = _html(lang)
        if open and sys.stdout.isatty():
            webbrowser.open(f"file://{html_path}")

    return html_path


def _console(lang: str) -> None:
    console = Console()
    lbl = _LABELS[lang]

    console.rule(f"[bold]climasus4py {__version__}[/bold]  "
                 "[dim]github.com/climasus/climasus4py[/dim]")
    console.print(f"[italic]{_TITLE[lang]}[/italic]")
    console.rule()

    pipe_stages = [s for s in _STAGES if s["num"] > 0]
    for i, s in enumerate(pipe_stages):
        style, _hex = _PALETTE[s["color"]]
        console.print(f"[{style}]  {s['num']}. {s['name'][lang]}[/{style}]")
        console.print(f"     {'  '.join(s['fns'])}")
        console.print(f"     [dim]{s['desc'][lang]}[/dim]\n")
        if i < len(pipe_stages) - 1:
            console.print("               [dim]↓[/dim]\n")

    util_stages = [s for s in _STAGES if s["num"] == 0]
    if util_stages:
        console.rule()
        for s in util_stages:
            style, _hex = _PALETTE[s["color"]]
            console.print(f"[{style}]  ◆ {s['name'][lang]}[/{style}]")
            console.print(f"     {'  '.join(s['fns'])}")
            console.print(f"     [dim]{s['desc'][lang]}[/dim]\n")

    console.rule()
    console.print(f"{lbl['lang']}: [bold]{lang}[/bold] · Backend: duckdb (lazy end-to-end)")
    console.print(f"{lbl['docs']}: [link]https://climasus.github.io[/link]")
    console.rule()


def _html(lang: str) -> str:
    pipe_stages = [s for s in _STAGES if s["num"] > 0]
    util_stages = [s for s in _STAGES if s["num"] == 0]

    def card(s: Stage, last: bool) -> str:
        _style, hexcolor = _PALETTE[s["color"]]
        pills = "\n".join(
            f'<span class="pill" style="background:{hexcolor}">{_esc(fn)}</span>'
            for fn in s["fns"]
        )
        arrow = '<div class="arrow">&#8595;</div>' if not last else ""
        badge_content = str(s["num"]) if s["num"] else "&#9670;"
        badge = f'<span class="num" style="background:{hexcolor}">{badge_content}</span>'
        return (
            f'<div class="card" style="border-color:{hexcolor}">'
            f'<div class="card-head">{badge}'
            f'<span class="name" style="color:{hexcolor}">{_esc(s["name"][lang])}</span></div>'
            f'<div class="pills">{pills}</div>'
            f'<p class="desc">{_esc(s["desc"][lang])}</p>'
            f'</div>{arrow}'
        )

    pipe_html = "\n".join(card(s, i == len(pipe_stages) - 1) for i, s in enumerate(pipe_stages))
    util_html = "\n".join(card(s, True) for s in util_stages)
    lbl = _LABELS[lang]["lang"]

    html = f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>climasus4py v{__version__}</title>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
     background:#0f172a;color:#e2e8f0;min-height:100vh;padding:2rem 1rem}}
.wrap{{max-width:760px;margin:0 auto}}
header{{text-align:center;margin-bottom:2rem}}
.logo{{font-size:2.2rem;font-weight:800;background:linear-gradient(135deg,#4c6ef5,#0ca678,#7950f2);
      -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.sub{{color:#94a3b8;font-size:.9rem;margin-top:.3rem}}
.card{{border:2px solid;border-radius:12px;padding:1rem 1.3rem;margin-bottom:.3rem;
      background:#111827}}
.card-head{{display:flex;align-items:center;gap:.7rem;margin-bottom:.6rem}}
.num{{color:#fff;font-weight:700;font-size:.75rem;border-radius:50%;
     width:26px;height:26px;display:flex;align-items:center;justify-content:center;flex-shrink:0}}
.name{{font-weight:700;font-size:.85rem;letter-spacing:.04em;text-transform:uppercase}}
.pills{{display:flex;flex-wrap:wrap;gap:.3rem;margin-bottom:.5rem}}
.pill{{font-family:"SF Mono",Consolas,monospace;font-size:.65rem;padding:.15rem .4rem;
      border-radius:5px;color:#fff;font-weight:600;opacity:.92}}
.desc{{font-size:.78rem;line-height:1.5;color:#cbd5e1}}
.arrow{{text-align:center;font-size:1.4rem;padding:.1rem 0;opacity:.6;color:#64748b}}
footer{{text-align:center;margin-top:2rem;color:#64748b;font-size:.76rem;line-height:1.8}}
footer a{{color:#818cf8;text-decoration:none}}
</style>
</head>
<body>
<div class="wrap">
<header>
<div class="logo">climasus4py</div>
<div class="sub">{_esc(_TITLE[lang])}</div>
</header>
{pipe_html}
{util_html}
<footer>climasus4py v{__version__} &middot; {lbl}: {lang} &middot; Backend: duckdb &middot;
<a href="https://github.com/climasus/climasus4py">GitHub</a></footer>
</div>
</body>
</html>"""

    fd, path = tempfile.mkstemp(suffix=".html", prefix="climasus4py_welcome_")
    os.close(fd)
    Path(path).write_text(html, encoding="utf-8")
    return path
