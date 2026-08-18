"""Interactive disease groups explorer.

Mirrors R: sus_filter_cid_explore.R (exported as sus_data_cid_select)

Not part of the lazy DuckDB pipeline — this is a discovery/browsing helper.
It materialises a small ``pandas.DataFrame`` catalog of disease groups
(sourced from ``climasus-data``, the same source used by
``utils/disease_groups.py``) and, depending on ``output``, either opens a
self-contained interactive HTML page in the default web browser or prints a
console summary. The returned DataFrame is meant to feed
``sus_filter(groups=...)``.
"""

from __future__ import annotations

import tempfile
import webbrowser
from datetime import datetime

import pandas as pd

from .disease_groups import get_disease_group_details, list_disease_groups

_VALID_LANGS: frozenset[str] = frozenset({"pt", "en", "es"})
_VALID_OUTPUTS: frozenset[str] = frozenset({"browser", "console"})

# Category taxonomy copied verbatim from climasus4r's embedded
# ``get_disease_groups_data()`` (R source has no equivalent field in
# climasus-data yet — see IDEIAS.md). Only used for HTML/console grouping,
# never for filtering logic. Groups present in climasus-data but absent from
# the original R catalog (extras from climate_sensitive.json) fall back to
# "Other".
_CATEGORY_MAP: dict[str, str] = {
    "diarrheal": "Infectious",
    "tuberculosis": "Infectious",
    "dengue": "Infectious",
    "chikungunya": "Infectious",
    "zika": "Infectious",
    "malaria": "Infectious",
    "leishmaniasis": "Infectious",
    "chagas": "Infectious",
    "zoonotic_bacterial": "Infectious",
    "leptospirosis": "Infectious",
    "hansen": "Infectious",
    "yellow_fever": "Infectious",
    "schistosomiasis": "Infectious",
    "helminthiasis": "Infectious",
    "vector_borne": "Infectious",
    "cardiovascular": "Cardiovascular",
    "ischemic_heart": "Cardiovascular",
    "stroke": "Cardiovascular",
    "cerebrovascular": "Cardiovascular",
    "hypertension": "Cardiovascular",
    "heart_failure": "Cardiovascular",
    "arrhythmias": "Cardiovascular",
    "respiratory": "Respiratory",
    "pneumonia": "Respiratory",
    "asthma": "Respiratory",
    "copd": "Respiratory",
    "upper_respiratory": "Respiratory",
    "influenza_pneumonia": "Respiratory",
    "heat_exposure": "Injuries",
    "cold_exposure": "Injuries",
    "drowning": "Injuries",
    "injuries": "Injuries",
    "heat_related": "Composite",
    "waterborne": "Composite",
    "skin_cancer": "Neoplasms",
    "respiratory_cancer": "Neoplasms",
    "diabetes": "Endocrine",
    "malnutrition": "Endocrine",
    "mental_disorders": "Mental",
    "neurological_disorders": "Neurological",
    "digestive": "Digestive",
    "skin_infections": "Skin",
    "renal": "Genitourinary",
    "pregnancy_complications": "Pregnancy",
    "perinatal": "Perinatal",
    "congenital": "Congenital",
    "microcephaly": "Congenital",
    "ill_defined": "Ill-defined",
    "transport_accidents": "External",
    "natural_disasters": "External",
    "suicide_self_harm": "External",
    "air_pollution_related": "Climate-Health",
    "climate_sensitive_all": "Climate-Health",
    "pediatric_respiratory": "Age-Specific",
    "elderly_cardiovascular": "Age-Specific",
    "fever_syndrome": "Syndromic",
    "respiratory_syndrome": "Syndromic",
    "diarrheal_syndrome": "Syndromic",
}

# Category -> emoji icon, used only by the HTML explorer's category headers.
# NOTE (preserved R quirk, see IDEIAS.md): the R source's `get_category_icon()`
# defines "Endocrine" and "Neoplasms" twice in the same `list()` literal with
# different emoji. R's `[[` name lookup returns the *first* match for a
# duplicated name, so the second definition for each is dead code in R. A
# naive Python dict literal would silently take the *last* value instead
# (opposite tie-break), which would change the rendered icon. This map
# hardcodes the values R actually produces (first occurrence wins) rather
# than the values a literal transliteration would produce.
_CATEGORY_ICONS: dict[str, str] = {
    "Infectious": "\U0001f9a0",
    "Cardiovascular": "❤",
    "Respiratory": "\U0001fac1",
    "Injuries": "\U0001f691",
    "Composite": "\U0001f321",
    "Neoplasms": "\U0001f397",
    "Endocrine": "\U0001fa7a",
    "Mental": "\U0001f9e0",
    "Neurological": "\U0001f9e0",
    "Digestive": "\U0001fad9",
    "Skin": "\U0001f3ff",
    "Genitourinary": "\U0001f4a9",
    "Pregnancy": "\U0001f476",
    "Perinatal": "\U0001f476",
    "Congenital": "\U0001f5bc",
    "Ill-defined": "❓",
    "Climate-Health": "⛅",
    "Age-Specific": "\U0001f465",
    "Syndromic": "\U0001f9ea",
    "External": "⚠",
    "Other": "\U0001f4c4",
}

_DEFAULT_ICON = "\U0001f4c4"


def _get_category_icon(category: str) -> str:
    """Return the emoji icon for a category, with fuzzy substring fallback.

    Mirrors R's ``get_category_icon()``: exact match first, then a
    case-insensitive substring match in either direction, then a default
    clipboard icon.
    """
    if category in _CATEGORY_ICONS:
        return _CATEGORY_ICONS[category]

    cat_lower = category.lower()
    for cat_name, icon in _CATEGORY_ICONS.items():
        cat_name_lower = cat_name.lower()
        if cat_lower in cat_name_lower or cat_name_lower in cat_lower:
            return icon

    return _DEFAULT_ICON


def _get_disease_groups_data(lang: str, filter_climate: bool) -> pd.DataFrame:
    """Build the disease groups catalog from climasus-data (via disease_groups.py).

    Args:
        lang: Language for label/description text.
        filter_climate: When True, keep only climate-sensitive groups.

    Returns:
        DataFrame with columns: name, icd_codes, climate_factors,
        description, category, climate_sensitive.
    """
    groups = list_disease_groups(climate_sensitive_only=False, lang=lang)

    rows = []
    for g in groups:
        name = g["group"]
        details = get_disease_group_details(name, lang=lang)
        climate_factors = ", ".join(
            factor.replace("_", " ").capitalize()
            for factor in details.get("climate_factors", [])
        )
        rows.append(
            {
                "name": name,
                "icd_codes": ", ".join(g["codes"]),
                "climate_factors": climate_factors,
                "description": g["description"],
                "category": _CATEGORY_MAP.get(name, "Other"),
                "climate_sensitive": g["climate_sensitive"],
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "name",
            "icd_codes",
            "climate_factors",
            "description",
            "category",
            "climate_sensitive",
        ],
    )

    if filter_climate:
        df = df[df["climate_sensitive"]].reset_index(drop=True)

    return df


def sus_data_cid_select(
    lang: str = "pt",
    output: str = "browser",
    filter_climate: bool = False,
    verbose: bool = True,
) -> pd.DataFrame:
    """Interactive disease groups explorer.

    Opens an interactive HTML interface (or prints a console summary) to
    explore disease groups and climate factors for use with
    ``sus_filter(groups=...)``. Helps users discover available disease
    groups without needing to know specific ICD-10 codes.

    Args:
        lang: Language for labels/descriptions. One of ``"pt"`` (default),
            ``"en"``, ``"es"``.
        output: Output mode. ``"browser"`` (default) opens an interactive
            HTML page; ``"console"`` prints a summary to stdout.
        filter_climate: When ``True``, restricts the catalog to
            climate-sensitive disease groups only.
        verbose: When ``True`` (default), prints informative progress
            messages.

    Returns:
        DataFrame with columns ``name``, ``icd_codes``, ``climate_factors``,
        ``description``, ``category``, ``climate_sensitive`` — one row per
        disease group. Ready to use as ``sus_filter(rel, groups=df["name"])``.

    Raises:
        ValueError: If *lang* or *output* is not one of the accepted values.

    Examples::

        import climasus4py as cs

        # Open interactive explorer
        cs.sus_data_cid_select()

        # Explore only climate-sensitive diseases
        cs.sus_data_cid_select(filter_climate=True)

        # Get disease group names for programmatic use
        groups = cs.sus_data_cid_select(output="console", verbose=False)

        # Use with sus_filter
        filtered = cs.sus_filter(rel, groups=groups["name"].tolist()[:1])
    """
    if lang not in _VALID_LANGS:
        raise ValueError(
            f"sus_data_cid_select: invalid lang {lang!r}. "
            f"Choose one of {sorted(_VALID_LANGS)}."
        )
    if output not in _VALID_OUTPUTS:
        raise ValueError(
            f"sus_data_cid_select: invalid output {output!r}. "
            f"Choose one of {sorted(_VALID_OUTPUTS)}."
        )

    if verbose:
        print("Interactive Disease Groups Explorer")
        print("Loading disease groups...")

    groups_df = _get_disease_groups_data(lang, filter_climate)

    if filter_climate and verbose:
        print(f"Filtered to {len(groups_df)} climate-sensitive groups")

    if output == "console":
        if verbose:
            print("\nDisease Groups Explorer")
            print("-" * 40)
            print(f"Total groups: {len(groups_df)}")
            print(f"Categories: {', '.join(groups_df['category'].unique())}")
            print(f"Climate-sensitive: {int(groups_df['climate_sensitive'].sum())}")
            print(f"Language: {lang}")
            print("-" * 40)

            for cat in groups_df["category"].unique():
                cat_groups = groups_df[groups_df["category"] == cat]
                n_total = len(cat_groups)
                n_climate = int(cat_groups["climate_sensitive"].sum())
                sample = ", ".join(cat_groups["name"].head(3))
                print(f"\n{cat} ({n_total} groups)")
                print(f"  * Climate-sensitive: {n_climate}")
                print(f"  * Sample groups: {sample}")

            print("\nUse output='console' with filter_climate=True to narrow down")
            print("Use output='browser' for the interactive interface")

        return groups_df

    # --- browser output (HTML interface) ---
    if verbose:
        print("Generating interactive HTML interface...")

    html_content = _generate_disease_groups_html(groups_df, lang)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, encoding="utf-8"
    ) as f:
        f.write(html_content)
        temp_path = f.name

    if verbose:
        print("Opening interactive explorer in browser...")

    webbrowser.open(f"file://{temp_path}")

    return groups_df


# =============================================================================
# HTML GENERATION HELPERS
# =============================================================================


def _get_ui_text_disease_groups(lang: str) -> dict[str, str]:
    """UI translations for the HTML explorer. Mirrors R's get_ui_text_disease_groups()."""
    if lang == "pt":
        return {
            "title": "Explorador de Grupos de Doencas - CLIMASUS4PY",
            "subtitle": (
                "Explore grupos de doencas e fatores climaticos "
                "para analises de clima e saude"
            ),
            "total_groups": "Total de grupos",
            "climate_sensitive": "Sensiveis ao clima",
            "selected": "Selecionados",
            "copy_selected": "Copiar Selecionados",
            "filter_climate": "Filtrar Clima",
            "clear_filters": "Limpar Filtros",
            "ready_to_use": "Pronto para usar em sus_filter(groups=...)",
            "group_name": "Grupo",
            "icd_codes": "Codigos CID-10",
            "climate_factors": "Fatores Climaticos",
            "description": "Descricao",
            "climate_label": "Clima",
            "groups_label": "grupos",
            "copy_single": "Copiar este grupo",
            "tips_title": "Dicas de Uso",
            "tip1": "Clique em uma linha para selecionar um grupo",
            "tip2": "Use Ctrl/Cmd + Clique para selecao multipla",
            "tip3": "Filtre por fatores climaticos usando os chips coloridos",
            "footer": "CLIMASUS4PY - Climate, Health & Environmental Data Integration",
            "no_selection": "Nenhum grupo selecionado",
        }
    if lang == "es":
        return {
            "title": "Explorador de Grupos de Enfermedades - CLIMASUS4PY",
            "subtitle": (
                "Explore grupos de enfermedades y factores climaticos "
                "para analisis de clima y salud"
            ),
            "total_groups": "Total de grupos",
            "climate_sensitive": "Sensibles al clima",
            "selected": "Seleccionados",
            "copy_selected": "Copiar Seleccionados",
            "filter_climate": "Filtrar Clima",
            "clear_filters": "Limpiar Filtros",
            "ready_to_use": "Listo para usar en sus_filter(groups=...)",
            "group_name": "Grupo",
            "icd_codes": "Codigos CIE-10",
            "climate_factors": "Factores Climaticos",
            "description": "Descripcion",
            "climate_label": "Clima",
            "groups_label": "grupos",
            "copy_single": "Copiar este grupo",
            "tips_title": "Consejos de Uso",
            "tip1": "Haga clic en una fila para seleccionar un grupo",
            "tip2": "Use Ctrl/Cmd + Clic para seleccion multiple",
            "tip3": "Filtre por factores climaticos usando los chips de colores",
            "footer": "CLIMASUS4PY - Integracion de Datos de Clima, Salud y Ambiente",
            "no_selection": "Ningun grupo seleccionado",
        }
    return {
        "title": "Disease Groups Explorer - CLIMASUS4PY",
        "subtitle": "Explore disease groups and climate factors for climate-health analysis",
        "total_groups": "Total groups",
        "climate_sensitive": "Climate-sensitive",
        "selected": "Selected",
        "copy_selected": "Copy Selected",
        "filter_climate": "Filter Climate",
        "clear_filters": "Clear Filters",
        "ready_to_use": "Ready to use in sus_filter(groups=...)",
        "group_name": "Group",
        "icd_codes": "ICD-10 Codes",
        "climate_factors": "Climate Factors",
        "description": "Description",
        "climate_label": "Climate",
        "groups_label": "groups",
        "copy_single": "Copy this group",
        "tips_title": "Usage Tips",
        "tip1": "Click a row to select a group",
        "tip2": "Use Ctrl/Cmd + Click for multiple selection",
        "tip3": "Filter by climate factors using colored chips",
        "footer": "CLIMASUS4PY - Climate, Health & Environmental Data Integration",
        "no_selection": "No groups selected",
    }


def _html_escape(text: str) -> str:
    """Minimal HTML-attribute-safe escaping (stdlib only, no new dependency)."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _generate_disease_groups_html(groups_df: pd.DataFrame, lang: str) -> str:
    """Build the full self-contained HTML page. Mirrors R's generate_disease_groups_html()."""
    text = _get_ui_text_disease_groups(lang)

    table_rows = []
    current_category = None

    for _, row in groups_df.iterrows():
        if row["category"] != current_category:
            current_category = row["category"]
            category_count = int((groups_df["category"] == current_category).sum())
            table_rows.append(
                f"""<tr class="category-header">
          <td colspan="6" class="category-title">
            <div class="category-header-content">
              <span class="category-icon">{_get_category_icon(current_category)}</span>
              <span class="category-name">{_html_escape(current_category)}</span>
              <span class="category-stats">{category_count} {text['groups_label']}</span>
            </div>
          </td>
        </tr>"""
            )

        climate_chips = ""
        if row["climate_sensitive"] and row["climate_factors"] and row["climate_factors"] != "-":
            factors = [f.strip() for f in row["climate_factors"].split(",")]
            for factor in factors:
                slug = factor.lower().replace(" ", "-")
                climate_chips += (
                    f'<span class="climate-chip climate-{slug}" '
                    f'title="{_html_escape(factor)}">{_html_escape(factor)}</span>'
                )

        climate_badge = ""
        if row["climate_sensitive"]:
            climate_badge = (
                f'<span class="climate-badge" title="{_html_escape(text["climate_sensitive"])}">'
                f"\U0001f321</span>"
            )

        name = row["name"]
        description = row["description"]
        table_rows.append(
            f"""<tr class="group-row"
          data-name="{_html_escape(name)}"
          data-category="{_html_escape(row['category'])}"
          data-climate="{str(row['climate_sensitive']).lower()}">
        <td class="name-cell">
          <div class="name-container">
            <code class="group-name">{_html_escape(name)}</code>
            <span class="copy-icon" onclick="copySingleGroup('{name}')"
              title="{_html_escape(text['copy_single'])}">\U0001f4c4</span>
          </div>
        </td>
        <td class="icd-cell"><code>{_html_escape(row['icd_codes'])}</code></td>
        <td class="climate-cell">{climate_chips}</td>
        <td class="description-cell" title="{_html_escape(description[:80])}">
          {_html_escape(description)}</td>
        <td class="badge-cell">{climate_badge}</td>
      </tr>"""
        )

    table_rows_html = "\n".join(table_rows)

    total_count = len(groups_df)
    climate_count = int(groups_df["climate_sensitive"].sum())
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{_html_escape(text['title'])}</title>
    {_get_css_styles()}
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{_html_escape(text['title'])}</h1>
            <p>{_html_escape(text['subtitle'])}</p>
        </div>

        <div class="stats-bar">
            <div>{text['total_groups']}: <strong id="totalCount">{total_count}</strong></div>
            <div>{text['climate_sensitive']}:
              <strong id="climateCount">{climate_count}</strong></div>
            <div>{text['selected']}: <strong id="selectedCount">0</strong></div>
        </div>

        <div class="controls">
            <button class="btn btn-primary" onclick="copySelectedGroups()">
                <span>\U0001f4c4</span> {text['copy_selected']}
            </button>
            <button class="btn btn-secondary" onclick="filterClimate()">
                <span>\U0001f321</span> {text['filter_climate']}
            </button>
            <button class="btn btn-secondary" onclick="clearFilters()">
                <span>\U0001f504</span> {text['clear_filters']}
            </button>
        </div>

        <div id="codeHelp" class="code-help">
            <strong>{text['ready_to_use']}:</strong><br>
            <code id="codeOutput">c()</code>
        </div>

        <div class="table-container">
            <table id="groupsTable">
                <thead>
                    <tr>
                        <th width="200">{text['group_name']}</th>
                        <th width="150">{text['icd_codes']}</th>
                        <th width="250">{text['climate_factors']}</th>
                        <th>{text['description']}</th>
                        <th width="80">{text['climate_label']}</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows_html}
                </tbody>
            </table>
        </div>

        <div class="tips">
            <h3>{text['tips_title']}</h3>
            <ul>
                <li>{text['tip1']}</li>
                <li>{text['tip2']}</li>
                <li>{text['tip3']}</li>
            </ul>
        </div>

        <div class="footer">
            {text['footer']}<br>
            <small>Generated at {generated_at}</small>
        </div>
    </div>
    {_get_javascript(text)}
</body>
</html>"""


def _get_css_styles() -> str:
    """Inline CSS (Climate Forest theme), copied from R's get_css_styles()."""
    return """<style>
    :root {
        --forest-dark: #2C5530;
        --forest-medium: #4A7C59;
        --forest-light: #8FB996;
        --earth-dark: #8B4513;
        --earth-light: #D2691E;
        --sky-light: #E8F4F8;
        --text-dark: #2C3E50;
        --text-light: #5D6D7E;
        --success: #27AE60;
        --warning: #F39C12;
        --danger: #E74C3C;
    }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                     "Helvetica Neue", Arial, sans-serif;
        background: linear-gradient(135deg, var(--sky-light) 0%, #f5f9fc 100%);
        color: var(--text-dark);
        line-height: 1.6;
        padding: 20px;
        min-height: 100vh;
    }
    .container {
        max-width: 1600px;
        margin: 0 auto;
        background: white;
        border-radius: 16px;
        box-shadow: 0 10px 40px rgba(44, 83, 48, 0.15);
        overflow: hidden;
        border: 1px solid rgba(139, 69, 19, 0.1);
    }
    .header {
        background: linear-gradient(135deg, #1e3a28 0%, #2C5530 100%);
        color: #f0f7f0;
        padding: 40px;
        text-align: center;
        position: relative;
        overflow: hidden;
    }
    .header h1 {
        font-size: 2.5em;
        margin-bottom: 10px;
        font-weight: 700;
        text-shadow: 0 2px 6px rgba(0,0,0,0.4);
        color: #8FB996;
        letter-spacing: 0.5px;
    }
    .header p {
        font-size: 1.2em;
        opacity: 0.95;
        color: #8FB996;
        text-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
        font-weight: 400;
    }
    .stats-bar {
        background: linear-gradient(to right, var(--forest-light) 0%, #B8D8C0 100%);
        padding: 20px 40px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 1.1em;
        color: var(--forest-dark);
        border-bottom: 2px solid rgba(139, 69, 19, 0.1);
    }
    .stats-bar strong { color: var(--earth-dark); font-size: 1.4em; margin-left: 5px; }
    .controls {
        padding: 25px 40px;
        background: #f8faf9;
        display: flex;
        gap: 15px;
        flex-wrap: wrap;
        border-bottom: 1px solid #e8f0e8;
    }
    .btn {
        padding: 12px 24px;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        font-size: 1em;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .btn-primary {
        background: var(--forest-medium); color: white;
        border: 1px solid rgba(44, 83, 48, 0.2);
    }
    .btn-primary:hover { background: var(--forest-dark); transform: translateY(-2px); }
    .btn-secondary {
        background: var(--forest-light); color: var(--forest-dark);
        border: 1px solid rgba(44, 83, 48, 0.2);
    }
    .btn-secondary:hover { background: var(--forest-medium); color: white; }
    .code-help {
        background: linear-gradient(to right, #F1F8E9 0%, #E8F5E9 100%);
        padding: 20px;
        margin: 20px 40px;
        border-radius: 10px;
        border-left: 5px solid var(--success);
        font-family: "Consolas", "Monaco", monospace;
        font-size: 0.95em;
        display: none;
        border: 1px solid #C8E6C9;
    }
    .code-help.show { display: block; animation: fadeIn 0.3s ease; }
    .table-container {
        max-height: 700px;
        overflow-y: auto;
        padding: 0;
        margin: 0 20px;
        border-radius: 10px;
        border: 1px solid #e8f0e8;
    }
    table { width: 100%; border-collapse: collapse; background: white; }
    th {
        background: linear-gradient(to bottom, var(--forest-light) 0%, #9FC9A6 100%);
        padding: 18px 15px;
        text-align: left;
        font-weight: 600;
        color: var(--forest-dark);
        border-bottom: 3px solid var(--forest-medium);
        position: sticky;
        top: 0;
        z-index: 10;
        font-size: 0.95em;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    td { padding: 15px; border-bottom: 1px solid #f0f5f0; vertical-align: middle; }
    .category-header { background: linear-gradient(to right, #f8faf9 0%, #f0f7f0 100%) !important; }
    .category-title {
        padding: 15px !important;
        font-weight: 700;
        color: var(--forest-dark);
        border-bottom: 2px solid var(--forest-light);
        font-size: 1.1em;
    }
    .category-header-content { display: flex; align-items: center; gap: 15px; }
    .category-icon { font-size: 1.3em; }
    .category-stats {
        margin-left: auto; font-size: 0.9em;
        color: var(--earth-light); font-weight: 500;
    }
    .group-row { cursor: pointer; transition: all 0.2s ease; border-left: 4px solid transparent; }
    .group-row:hover { background: #f8faf9 !important; border-left-color: var(--forest-light); }
    .group-row.selected {
        background: #E8F5E9 !important;
        border-left-color: var(--success);
        box-shadow: inset 3px 0 0 var(--success);
    }
    .name-container { display: flex; align-items: center; gap: 10px; }
    .group-name {
        font-family: "Consolas", "Monaco", monospace;
        font-weight: 600;
        color: var(--forest-dark);
        background: #f0f7f0;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.95em;
    }
    .copy-icon { cursor: pointer; opacity: 0.5; transition: all 0.2s ease; font-size: 1.1em; }
    .copy-icon:hover { opacity: 1; transform: scale(1.2); }
    .climate-chip {
        display: inline-block;
        padding: 4px 10px;
        margin: 2px;
        border-radius: 12px;
        font-size: 0.85em;
        font-weight: 500;
        background: #E8F5E9;
        color: var(--forest-dark);
        border: 1px solid #C8E6C9;
    }
    .climate-chip.climate-temperature {
        background: #FFEBEE; color: #C62828; border-color: #EF9A9A;
    }
    .climate-chip.climate-precipitation {
        background: #E3F2FD; color: #1565C0; border-color: #90CAF9;
    }
    .climate-chip.climate-humidity {
        background: #F3E5F5; color: #6A1B9A; border-color: #CE93D8;
    }
    .climate-chip.climate-air-pollution {
        background: #FFF3E0; color: #E65100; border-color: #FFCC80;
    }
    .climate-badge { font-size: 1.3em; cursor: help; }
    .tips { padding: 30px 40px; background: #f8faf9; border-top: 1px solid #e8f0e8; }
    .tips h3 { color: var(--forest-dark); margin-bottom: 15px; font-size: 1.2em; }
    .tips ul { list-style-position: inside; color: var(--text-light); line-height: 2; }
    .tips li { margin-bottom: 8px; }
    .footer {
        text-align: center; padding: 20px;
        background: var(--forest-dark); color: #8FB996; font-size: 0.9em;
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-10px); }
        to { opacity: 1; transform: translateY(0); }
    }
  </style>"""


def _get_javascript(text: dict[str, str]) -> str:
    """Inline JS for row selection/filtering/copy-to-clipboard.

    Mirrors R's get_javascript().
    """
    no_selection = text["no_selection"]
    return f"""
    <script>
        document.querySelectorAll(".group-row").forEach(row => {{
            row.addEventListener("click", function(e) {{
                if (e.target.closest(".copy-icon")) {{
                    return;
                }}

                if (e.ctrlKey || e.metaKey) {{
                    this.classList.toggle("selected");
                }} else {{
                    this.classList.add("selected");
                    document.querySelectorAll(".group-row.selected").forEach(otherRow => {{
                        if (otherRow !== this) {{
                            otherRow.classList.remove("selected");
                        }}
                    }});
                }}

                updateSelectedCount();
                e.stopPropagation();
            }});
        }});

        function updateSelectedCount() {{
            const selected = document.querySelectorAll(".group-row.selected").length;
            document.getElementById("selectedCount").textContent = selected;

            const codeHelp = document.getElementById("codeHelp");

            if (selected > 0) {{
                showCopyHelp();
                codeHelp.classList.add("show");
            }} else {{
                codeHelp.classList.remove("show");
            }}
        }}

        function showCopyHelp() {{
            const selected = document.querySelectorAll(".group-row.selected");
            const names = Array.from(selected).map(row => row.getAttribute("data-name"));
            const codeString = 'c("' + names.join('", "') + '")';

            document.getElementById("codeOutput").textContent = codeString;
        }}

        function copySingleGroup(name) {{
            const codeString = 'c("' + name + '")';

            navigator.clipboard.writeText(codeString).then(() => {{
                showNotification("Group copied: " + name);
            }});
        }}

        function copySelectedGroups() {{
            const selected = document.querySelectorAll(".group-row.selected");
            if (selected.length === 0) {{
                showNotification("{no_selection}", "warning");
                return;
            }}

            const names = Array.from(selected).map(row => row.getAttribute("data-name"));
            const codeString = 'c("' + names.join('", "') + '")';

            navigator.clipboard.writeText(codeString).then(() => {{
                showNotification("✅ " + selected.length + " groups copied to clipboard");
                console.log("\\nReady to use in sus_filter(groups=...):\\n");
                console.log(codeString);
            }});
        }}

        function filterClimate() {{
            document.querySelectorAll(".group-row").forEach(row => {{
                const isClimate = row.getAttribute("data-climate") === "true";
                row.style.display = isClimate ? "" : "none";
            }});

            showNotification("Filtered to climate-sensitive groups");
        }}

        function clearFilters() {{
            document.querySelectorAll(".group-row").forEach(row => {{
                row.style.display = "";
            }});

            showNotification("Filters cleared");
        }}

        function showNotification(message, type = "success") {{
            const oldNotifications = document.querySelectorAll(".notification");
            oldNotifications.forEach(n => n.remove());

            const notification = document.createElement("div");
            notification.className = `notification notification-${{type}}`;
            notification.textContent = message;
            notification.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                padding: 15px 20px;
                background: ${{type === "success" ? "#27AE60" : "#F39C12"}};
                color: white;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                z-index: 1000;
                animation: fadeIn 0.3s ease;
                font-weight: 500;
            `;

            document.body.appendChild(notification);

            setTimeout(() => {{
                notification.style.opacity = "0";
                notification.style.transform = "translateX(100px)";
                setTimeout(() => notification.remove(), 300);
            }}, 3000);
        }}

        updateSelectedCount();
    </script>
  """
