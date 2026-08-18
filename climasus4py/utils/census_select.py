"""Interactive Census variables explorer.

Mirrors R: sus_census_explore.R (exported as sus_census_select)

Not part of the lazy DuckDB pipeline — this is a discovery/browsing helper.
It builds a small ``pandas.DataFrame`` catalog of Brazilian Census (IBGE)
variables and, depending on ``output``, either opens a self-contained
interactive HTML page in the default web browser, prints a console summary,
or returns just the variable codes. The returned codes are meant to feed
``sus_census(rel, variables=...)`` (the Python sibling of R's
``sus_census_join()`` / ``sus_socio_add_census()``).
"""

from __future__ import annotations

import re
import tempfile
import webbrowser
from datetime import datetime

import pandas as pd

from .data import load_json

_VALID_DATASETS: frozenset[str] = frozenset(
    {"all", "population", "households", "families", "mortality", "emigration", "tracts"}
)
_VALID_YEARS: frozenset[int] = frozenset({2000, 2010})
_VALID_LANGS: frozenset[str] = frozenset({"pt", "en", "es"})
_VALID_OUTPUTS: frozenset[str] = frozenset({"browser", "console", "codes"})

# Datasets with a dictionary loader. "tracts" is a valid `dataset=` value
# (see _VALID_DATASETS) but has no entry here — preserved R quirk, see below.
_DICTIONARY_DATASETS: tuple[str, ...] = (
    "population",
    "households",
    "families",
    "mortality",
    "emigration",
)

# lang -> climasus-data dictionary directory, matching the existing
# dictionaries/pt-{pt,en,es}/ convention used for DATASUS system dictionaries.
_LANG_DIR: dict[str, str] = {"pt": "pt-pt", "en": "pt-en", "es": "pt-es"}

# Category detection patterns, ordered (first match wins) — copied verbatim
# from R's categorize_variable(). Matched against the *translated* variable
# name (lowercased substring search), so with lang="en"/"es" the pt-only
# patterns are mostly dead but harmless, exactly as in R.
_CATEGORY_PATTERNS: dict[str, tuple[str, ...]] = {
    "demographics": (
        "idade", "age", "edad", "sexo", "sex", "raca", "race",
        "cor", "color", "nascimento", "birth", "nacimiento",
        "parentesco", "kinship",
    ),
    "education": (
        "escola", "educ", "alfabetiz", "literacy", "instrucao",
        "instruction", "instruccion", "curso", "course",
        "serie", "grade", "grado",
    ),
    "income": (
        "renda", "income", "ingreso", "salario", "wage",
        "beneficio", "benefit", "aposentadoria", "pension",
    ),
    "housing": (
        "domicilio", "household", "vivienda", "agua", "water",
        "esgoto", "sewer", "alcantarillado", "energia", "energy",
        "lixo", "garbage", "basura", "banheiro", "bathroom",
    ),
    "health": (
        "saude", "health", "salud", "deficiencia", "disability",
        "discapacidad", "enxergar", "see", "ver", "ouvir", "hear",
        "oir", "caminhar", "walk", "caminar", "morte", "death",
        "muerte", "filho", "child", "hijo",
    ),
    "migration": (
        "migracao", "migration", "migracion", "nasceu", "born",
        "nacio", "residencia", "residence", "moradia", "dwelling",
        "fixou", "settled",
    ),
    "geography": (
        "municipio", "municipality", "uf", "state",
        "estado", "regiao", "region", "area",
    ),
    "work": (
        "trabalho", "work", "trabajo", "ocupacao", "occupation",
        "ocupacion", "emprego", "employment", "empleo", "profissao",
        "profession", "profesion",
    ),
}


def _categorize_variable(name: str) -> str:
    """Assign a category to a variable name. Mirrors R's categorize_variable().

    First matching category wins, in the fixed order of ``_CATEGORY_PATTERNS``.
    Falls back to ``"other"``.
    """
    name_lower = name.lower()
    for category, patterns in _CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if pattern in name_lower:
                return category
    return "other"


def _load_census_dictionary(dataset: str, lang: str) -> dict | None:
    """Load one dataset's column/value dictionary from climasus-data.

    Mirrors R's ``load_dict()``: returns ``None`` (with a printed warning)
    instead of raising, so a missing dataset doesn't abort the whole catalog
    build when other datasets succeed.

    Args:
        dataset: One of ``_DICTIONARY_DATASETS``.
        lang: One of ``_VALID_LANGS``.

    Returns:
        Dict with ``"columns"`` (code -> name) and ``"values"``
        (code -> category labels), or ``None`` if not found.
    """
    lang_dir = _LANG_DIR[lang]
    try:
        return load_json(f"dictionaries/{lang_dir}/census_{dataset}.json")
    except FileNotFoundError as exc:
        print(f"Warning: could not load census dictionary for {dataset!r} ({lang}): {exc}")
        return None


def _build_catalog(dataset: str, year: int, lang: str, verbose: bool) -> pd.DataFrame:
    """Build the filtered variable catalog. Mirrors R's dictionary-loading block."""
    datasets_to_load = list(_DICTIONARY_DATASETS) if dataset == "all" else [dataset]

    frames: list[pd.DataFrame] = []
    for ds in datasets_to_load:
        if ds not in _DICTIONARY_DATASETS:
            # Preserved R quirk: dataset="tracts" passes validation but has no
            # dictionary function in R either — R errors trying to subset
            # NULL[["pt"]]. We raise explicitly instead of a confusing KeyError.
            raise ValueError(
                f"sus_census_select: dataset {ds!r} passed validation but has no "
                "dictionary source in climasus-data (preserved from R, where the "
                "equivalent dataset also has no dictionary function)."
            )

        dict_data = _load_census_dictionary(ds, lang)
        if dict_data is None or not dict_data.get("columns"):
            if verbose:
                print(f"Warning: no variables found for dataset: {ds}")
            continue

        columns = dict_data["columns"]
        values = dict_data.get("values", {})

        df = pd.DataFrame(
            {
                "code": list(columns.keys()),
                "name": [str(v) for v in columns.values()],
                "dataset": ds,
            }
        )
        df["type"] = df["code"].apply(
            lambda c: "imputation_marker" if re.match(r"^M", c) else "regular_variable"
        )
        df["category"] = df["name"].apply(_categorize_variable)
        df["has_categories"] = df["code"].isin(values.keys())
        df["n_categories"] = df["code"].apply(
            lambda c, _values=values: len(_values.get(c, []))
        )

        frames.append(df)
        if verbose:
            print(f"Loaded {len(df)} variables from {ds} dataset")

    if not frames:
        raise ValueError("No variables found for the specified parameters")

    vars_df = pd.concat(frames, ignore_index=True)
    vars_df["year"] = year
    vars_df = vars_df.sort_values(["dataset", "code"]).reset_index(drop=True)
    return vars_df


def sus_census_select(
    dataset: str = "all",
    year: int = 2010,
    lang: str = "pt",
    output: str = "browser",
    verbose: bool = True,
) -> pd.DataFrame | list[str]:
    """Interactive Census variables explorer.

    Opens an interactive HTML interface to explore Brazilian Census (IBGE)
    variables and copy codes for use with ``sus_census()`` (the Python
    sibling of R's ``sus_census_join()`` / ``sus_socio_add_census()``).

    Args:
        dataset: Census dataset. One of ``"all"`` (default), ``"population"``,
            ``"households"``, ``"families"``, ``"mortality"``,
            ``"emigration"``, ``"tracts"``.
        year: Census year. One of ``2000`` or ``2010`` (default). Preserved
            R quirk: the dictionaries are 2010-only regardless of *year* —
            passing ``2000`` returns the same variables labelled ``2000``.
        lang: Language for variable names. One of ``"pt"`` (default),
            ``"en"``, ``"es"``.
        output: Output mode. ``"browser"`` (default) opens an interactive
            HTML page; ``"console"`` prints a summary to stdout and returns
            the catalog; ``"codes"`` returns only the unique variable codes.
        verbose: When ``True`` (default), prints informative progress
            messages.

    Returns:
        Depending on *output*:
            - ``"browser"``/``"console"``: ``pandas.DataFrame`` with columns
              ``code``, ``name``, ``dataset``, ``type``, ``category``,
              ``has_categories``, ``n_categories``, ``year``.
            - ``"codes"``: list of unique variable code strings. Note this
              can be shorter than the full catalog when ``dataset="all"``,
              since the same code (e.g. imputation markers) can appear in
              more than one dataset.

    Raises:
        ValueError: If *dataset*, *year*, *lang*, or *output* is not one of
            the accepted values, or if no variables are found (e.g. the
            census dictionaries are not yet present in climasus-data).

    Examples::

        import climasus4py as cs

        # Open interactive explorer for all datasets
        cs.sus_census_select()

        # Explore only population variables
        cs.sus_census_select(dataset="population")

        # Get variable codes for programmatic use
        codes = cs.sus_census_select(
            dataset="population", output="codes", lang="en", verbose=False
        )

        # Use in sus_census
        out = cs.sus_census(rel, variables=codes, year=2010)
    """
    if dataset not in _VALID_DATASETS:
        raise ValueError(
            f"sus_census_select: invalid dataset {dataset!r}. "
            f"Choose one of {sorted(_VALID_DATASETS)}."
        )
    if year not in _VALID_YEARS:
        raise ValueError(
            f"sus_census_select: invalid year {year!r}. Choose one of {sorted(_VALID_YEARS)}."
        )
    if lang not in _VALID_LANGS:
        raise ValueError(
            f"sus_census_select: invalid lang {lang!r}. Choose one of {sorted(_VALID_LANGS)}."
        )
    if output not in _VALID_OUTPUTS:
        raise ValueError(
            f"sus_census_select: invalid output {output!r}. Choose one of {sorted(_VALID_OUTPUTS)}."
        )

    if verbose:
        print("Loading census dictionaries...")

    vars_df = _build_catalog(dataset, year, lang, verbose)

    if output == "codes":
        codes = vars_df["code"].unique().tolist()
        if verbose:
            print("\nCensus Variables - Codes Only")
            print("-" * 40)
            print(f"Found {len(codes)} variables")
            print(f"Dataset(s): {', '.join(vars_df['dataset'].unique())}")
            print("-" * 40)
            for c in codes[:10]:
                print(f"  * {c}")
            if len(codes) > 10:
                print(f"  ... and {len(codes) - 10} more")
        return codes

    if output == "console":
        if verbose:
            print("\nBrazilian Census Variables Explorer")
            print(f"Census {year}")
            print("-" * 40)
            print(f"Total variables: {len(vars_df)}")
            print(f"Datasets: {', '.join(vars_df['dataset'].unique())}")
            print(f"Language: {lang}")
            print("-" * 40)

            for ds in vars_df["dataset"].unique():
                ds_vars = vars_df[vars_df["dataset"] == ds]
                n_total = len(ds_vars)
                n_categorical = int(ds_vars["has_categories"].sum())
                n_imputation = int((ds_vars["type"] == "imputation_marker").sum())

                print(f"\n{ds.upper()} Dataset")
                print(f"  * Total variables: {n_total}")
                print(f"  * Categorical variables: {n_categorical}")
                print(f"  * Imputation markers: {n_imputation}")

                cats = ds_vars["category"].value_counts()
                if len(cats) > 0:
                    print("Categories:")
                    for cat, count in cats.items():
                        print(f"    {cat}: {count}")

                print("Sample variables:")
                for _, var in ds_vars.head(5).iterrows():
                    has_cat = (
                        f" ({var['n_categories']} categories)"
                        if var["has_categories"]
                        else ""
                    )
                    print(f"    {var['code']}: {var['name']}{has_cat}")
                if n_total > 5:
                    print(f"  ... and {n_total - 5} more variables")

            print("\nUse output='codes' to get all variable codes")
            print("Use output='browser' for interactive interface")

        return vars_df

    # --- browser output (HTML interface) ---
    html_content = _generate_forest_theme_html(vars_df, year, lang)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False, encoding="utf-8"
    ) as f:
        f.write(html_content)
        temp_path = f.name

    webbrowser.open(f"file://{temp_path}")

    if verbose:
        print("\nInteractive Census Explorer")
        print("Interface opened in browser!")
        print(f"  * Total variables: {len(vars_df)}")
        print(f"  * Temporary file: {temp_path}")
        print(f"  * Language: {lang}")
        print("-" * 40)
        print("Usage tips:")
        print("  1. Click on rows to select variables")
        print("  2. Use Ctrl+Click for multiple selection")
        print("  3. Click 'Copy Codes' to get code for sus_census()")
        print("  4. Export to CSV for external analysis")

    return vars_df


# =============================================================================
# HTML GENERATION HELPERS
# =============================================================================


def _get_ui_text_census(lang: str, year: int) -> dict[str, str]:
    """UI translations for the HTML explorer. Mirrors R's `texts` list."""
    if lang == "pt":
        return {
            "title": "Explorador de Variaveis do Censo Brasileiro",
            "subtitle": "Selecione variaveis para usar em sus_census()",
            "total_vars": "Total de variaveis",
            "showing": "Mostrando",
            "dataset_label": "Dataset",
            "code_label": "Codigo",
            "name_label": "Nome",
            "category_label": "Categoria",
            "type_label": "Tipo",
            "values_label": "Valores",
            "copy_btn": "\U0001F4CB  Copiar Codigos Selecionados",
            "copy_all_btn": "\U0001F4CB  Copiar Todos os Codigos",
            "export_btn": "\U0001F4BE Exportar CSV",
            "tips_title": "\U0001F4A1 Como usar:",
            "tip1": "Selecione as variaveis clicando nas linhas (use Ctrl+Clique para multiplas)",
            "tip2": "Clique em 'Copiar Codigos' para copiar os codigos no formato correto",
            "tip3": "Use os codigos copiados no argumento 'variables' de sus_census()",
            "tip4": f"Censo {year} - Interface gerada por climasus4py",
            "no_selection": "Nenhuma variavel selecionada. Clique nas linhas da tabela.",
            "regular_var": "Variavel regular",
            "imputation_var": "Marcador de imputacao",
            "categorical_var": "Variavel categorica",
        }
    if lang == "es":
        return {
            "title": "Explorador de Variables del Censo Brasileno",
            "subtitle": "Seleccione variables para usar en sus_census()",
            "total_vars": "Total de variables",
            "showing": "Mostrando",
            "dataset_label": "Dataset",
            "code_label": "Codigo",
            "name_label": "Nombre",
            "category_label": "Categoria",
            "type_label": "Tipo",
            "values_label": "Valores",
            "copy_btn": "\U0001F4CB  Copiar Codigos Seleccionados",
            "copy_all_btn": "\U0001F4CB  Copiar Todos los Codigos",
            "export_btn": "\U0001F4BE Exportar CSV",
            "tips_title": "\U0001F4A1 Como usar:",
            "tip1": (
                "Seleccione variables haciendo clic en las filas "
                "(use Ctrl+Click para multiples)"
            ),
            "tip2": "Haga clic en 'Copiar Codigos' para copiar codigos en el formato correcto",
            "tip3": "Use los codigos copiados en el argumento 'variables' de sus_census()",
            "tip4": f"Censo {year} - Interfaz generada por climasus4py",
            "no_selection": "No hay variables seleccionadas. Haga clic en las filas de la tabla.",
            "regular_var": "Variable regular",
            "imputation_var": "Marcador de imputacion",
            "categorical_var": "Variable categorica",
        }
    return {
        "title": "Brazilian Census Variables Explorer",
        "subtitle": "Select variables to use in sus_census()",
        "total_vars": "Total variables",
        "showing": "Showing",
        "dataset_label": "Dataset",
        "code_label": "Code",
        "name_label": "Name",
        "category_label": "Category",
        "type_label": "Type",
        "values_label": "Values",
        "copy_btn": "\U0001F4CB  Copy Selected Codes",
        "copy_all_btn": "\U0001F4CB  Copy All Codes",
        "export_btn": "\U0001F4BE Export CSV",
        "tips_title": "\U0001F4A1 How to use:",
        "tip1": "Select variables by clicking on rows (use Ctrl+Click for multiple)",
        "tip2": "Click 'Copy Codes' to copy codes in the correct format",
        "tip3": "Use copied codes in the 'variables' argument of sus_census()",
        "tip4": f"Census {year} - Interface generated by climasus4py",
        "no_selection": "No variables selected. Click on table rows.",
        "regular_var": "Regular variable",
        "imputation_var": "Imputation marker",
        "categorical_var": "Categorical variable",
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


def _generate_forest_theme_html(vars_df: pd.DataFrame, year: int, lang: str) -> str:
    """Build the full self-contained HTML page. Mirrors R's generate_forest_theme_html()."""
    text = _get_ui_text_census(lang, year)

    table_rows: list[str] = []
    current_dataset = None

    for _, row in vars_df.iterrows():
        if row["dataset"] != current_dataset:
            current_dataset = row["dataset"]
            n_vars_dataset = int((vars_df["dataset"] == current_dataset).sum())
            n_categorical = int(
                ((vars_df["dataset"] == current_dataset) & vars_df["has_categories"]).sum()
            )
            table_rows.append(
                f"""<tr class="dataset-header">
          <td colspan="6" class="dataset-title">
            <div class="dataset-header-content">
              <span class="dataset-icon">\U0001F4CA</span>
              <span class="dataset-name">{_html_escape(current_dataset.upper())}</span>
              <span class="dataset-stats">{n_vars_dataset} variables
              ({n_categorical} categorical)</span>
            </div>
          </td>
        </tr>"""
            )

        is_imputation = row["type"] == "imputation_marker"
        type_icon = "\U0001F3F7\U0000FE0F" if is_imputation else "\U0001F4DD"
        type_class = "type-imputation" if is_imputation else "type-regular"
        type_label = text["imputation_var"] if is_imputation else text["regular_var"]

        values_info = ""
        if row["has_categories"]:
            values_info = (
                f'<span class="categorical-badge" title="{_html_escape(text["categorical_var"])}: '
                f'{row["n_categories"]}">\U0001F4CA {row["n_categories"]}</span>'
            )

        code = row["code"]
        name = row["name"]
        display_name = name[:60]
        esc_code = _html_escape(code)
        esc_dataset = _html_escape(row["dataset"])
        esc_category = _html_escape(row["category"])
        table_rows.append(
            f"""<tr class="variable-row {row['type']}" data-code="{esc_code}"
        data-dataset="{esc_dataset}" data-category="{esc_category}">
        <td class="code-cell">
          <div class="code-container">
            <code class="variable-code">{esc_code}</code>
            <span class="copy-icon" onclick="copySingleCode('{code}')"
              title="Copy this code">\U0001F4CB</span>
          </div>
        </td>
        <td class="name-cell" title="{_html_escape(name)}">{_html_escape(display_name)}</td>
        <td class="category-cell">
          <span class="category-badge category-{row['category']}">{esc_category}</span>
        </td>
        <td class="type-cell">
          <span class="type-badge {type_class}">{type_icon} {_html_escape(type_label)}</span>
        </td>
        <td class="values-cell">{values_info}</td>
        <td class="dataset-cell">
          <span class="dataset-badge dataset-{row['dataset']}">{esc_dataset}</span>
        </td>
      </tr>"""
        )

    table_rows_html = "\n".join(table_rows)
    total_count = len(vars_df)
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
            <div>{text['total_vars']}: <strong id="totalCount">{total_count}</strong></div>
            <div>{text['showing']} <strong id="selectedCount">0</strong> selected</div>
            <div>Language: <strong>{lang}</strong> | Year: <strong>{year}</strong></div>
        </div>

        <div class="controls">
            <button class="btn btn-primary" onclick="copySelectedCodes()">
                <span>\U0001F4CB</span> {text['copy_btn']}
            </button>
            <button class="btn btn-secondary" onclick="copyAllCodes()">
                <span>\U0001F4CB</span> {text['copy_all_btn']}
            </button>
            <button class="btn btn-secondary" onclick="exportToCSV()">
                <span>\U0001F4BE</span> {text['export_btn']}
            </button>
        </div>

        <div id="codeHelp" class="code-help">
            <strong>Ready to use in sus_census():</strong><br>
            <code id="codeOutput">[]</code>
        </div>

        <div class="table-container">
            <table id="variablesTable">
                <thead>
                    <tr>
                        <th width="140">{text['code_label']}</th>
                        <th>{text['name_label']}</th>
                        <th width="120">{text['category_label']}</th>
                        <th width="140">{text['type_label']}</th>
                        <th width="80">{text['values_label']}</th>
                        <th width="100">{text['dataset_label']}</th>
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
            {text['tip4']}<br>
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
    .dataset-header { background: linear-gradient(to right, #f8faf9 0%, #f0f7f0 100%) !important; }
    .dataset-title {
        padding: 15px !important;
        font-weight: 700;
        color: var(--forest-dark);
        border-bottom: 2px solid var(--forest-light);
        font-size: 1.1em;
    }
    .dataset-header-content { display: flex; align-items: center; gap: 15px; }
    .dataset-icon { font-size: 1.3em; }
    .dataset-name { font-weight: 700; }
    .dataset-stats {
        margin-left: auto; font-size: 0.9em; color: var(--earth-light); font-weight: 500;
    }
    .variable-row {
        cursor: pointer; transition: all 0.2s ease; border-left: 4px solid transparent;
    }
    .variable-row:hover { background: #f8faf9 !important; border-left-color: var(--forest-light); }
    .variable-row.selected {
        background: #E8F5E9 !important;
        border-left-color: var(--success);
        box-shadow: inset 3px 0 0 var(--success);
    }
    .code-cell { font-family: "Consolas", "Monaco", monospace; min-width: 120px; }
    .code-container { display: flex; align-items: center; gap: 10px; }
    .variable-code {
        background: #f5f9f5;
        padding: 8px 12px;
        border-radius: 6px;
        color: var(--earth-dark);
        font-weight: 700;
        font-size: 1.1em;
        border: 1px solid #e0ede0;
        flex-grow: 1;
    }
    .copy-icon {
        cursor: pointer;
        padding: 5px;
        border-radius: 4px;
        background: var(--sky-light);
        color: var(--forest-medium);
        transition: all 0.2s;
        font-size: 0.9em;
    }
    .copy-icon:hover { background: var(--forest-light); color: white; transform: scale(1.1); }
    .category-badge {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 0.85em;
        font-weight: 600;
        text-transform: capitalize;
    }
    .category-demographics { background: #E3F2FD; color: #1565C0; }
    .category-education { background: #F3E5F5; color: #7B1FA2; }
    .category-income { background: #E8F5E9; color: #2E7D32; }
    .category-housing { background: #FFF3E0; color: #EF6C00; }
    .category-health { background: #FFEBEE; color: #C62828; }
    .category-migration { background: #E0F2F1; color: #00695C; }
    .category-geography { background: #E8EAF6; color: #3949AB; }
    .category-work { background: #FFF8E1; color: #FF8F00; }
    .category-other { background: #F5F5F5; color: #616161; }
    .type-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 12px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 500;
    }
    .type-regular { background: #E8F5E9; color: #2E7D32; }
    .type-imputation { background: #FFF3E0; color: #F57C00; }
    .dataset-badge {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 600;
        text-transform: capitalize;
        background: #f0f5f0;
        color: var(--forest-medium);
    }
    .categorical-badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 4px 10px;
        background: #E3F2FD;
        color: #1565C0;
        border-radius: 12px;
        font-size: 0.85em;
        font-weight: 500;
        cursor: help;
    }
    .tips {
        margin: 40px;
        padding: 30px;
        background: linear-gradient(135deg, #F1F8E9 0%, #E8F5E9 100%);
        border-radius: 12px;
        border-left: 6px solid var(--success);
    }
    .tips h3 {
        margin-bottom: 20px;
        color: var(--forest-dark);
        font-size: 1.3em;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .tips ul { margin-left: 25px; }
    .tips li { margin-bottom: 12px; line-height: 1.7; color: var(--text-light); }
    .footer {
        text-align: center;
        padding: 25px;
        color: var(--text-light);
        border-top: 1px solid #e8f0e8;
        background: #f8faf9;
        font-size: 0.95em;
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-10px); }
        to { opacity: 1; transform: translateY(0); }
    }
  </style>"""


def _get_javascript(text: dict[str, str]) -> str:
    """Inline JS for row selection, filtering, copy-to-clipboard, CSV export.

    Mirrors R's get_javascript(). Deviation from R: the copy-to-clipboard
    functions emit Python list syntax (``["V0001", "V0601"]``) instead of R's
    ``c("V0001", "V0601")``, since this package's users write Python — copying
    literal R syntax into a Python session would be a silent-wrong-result for
    them. See IDEIAS.md.
    """
    no_selection = text["no_selection"]
    return f"""
    <script>
        document.querySelectorAll(".variable-row").forEach(row => {{
            row.addEventListener("click", function(e) {{
                if (e.target.closest(".copy-icon")) {{
                    return;
                }}

                if (e.ctrlKey || e.metaKey) {{
                    this.classList.toggle("selected");
                }} else if (e.shiftKey) {{
                    const rows = Array.from(document.querySelectorAll(".variable-row"));
                    const currentIndex = rows.indexOf(this);
                    const selectedRows = document.querySelectorAll(".variable-row.selected");

                    if (selectedRows.length === 1) {{
                        const firstSelected = selectedRows[0];
                        const firstIndex = rows.indexOf(firstSelected);
                        const start = Math.min(firstIndex, currentIndex);
                        const end = Math.max(firstIndex, currentIndex);

                        rows.forEach(r => r.classList.remove("selected"));
                        for (let i = start; i <= end; i++) {{
                            rows[i].classList.add("selected");
                        }}
                    }} else {{
                        this.classList.add("selected");
                    }}
                }} else {{
                    this.classList.add("selected");
                    document.querySelectorAll(".variable-row.selected").forEach(otherRow => {{
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
            const selected = document.querySelectorAll(".variable-row.selected").length;
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
            const selected = document.querySelectorAll(".variable-row.selected");
            const codes = Array.from(selected).map(row => row.getAttribute("data-code"));
            document.getElementById("codeOutput").textContent = JSON.stringify(codes);
        }}

        function copySingleCode(code) {{
            navigator.clipboard.writeText(JSON.stringify([code])).then(() => {{
                showNotification("Code copied: " + code);
            }});
        }}

        function copySelectedCodes() {{
            const selected = document.querySelectorAll(".variable-row.selected");
            if (selected.length === 0) {{
                showNotification("{no_selection}", "warning");
                return;
            }}

            const codes = Array.from(selected).map(row => row.getAttribute("data-code"));
            const codeString = JSON.stringify(codes);

            navigator.clipboard.writeText(codeString).then(() => {{
                showNotification("\\u2705 " + selected.length + " codes copied to clipboard");
                console.log("\\nReady to use in sus_census():\\n");
                console.log(codeString);
            }});
        }}

        function copyAllCodes() {{
            const allRows = document.querySelectorAll(".variable-row");
            if (allRows.length === 0) {{
                showNotification("No variables available", "warning");
                return;
            }}

            const codes = Array.from(allRows).map(row => row.getAttribute("data-code"));
            const codeString = JSON.stringify(codes);

            navigator.clipboard.writeText(codeString).then(() => {{
                showNotification("\\u2705 All " + codes.length + " codes copied to clipboard");
                console.log("\\nAll variable codes:\\n");
                console.log(codeString);
            }});
        }}

        function exportToCSV() {{
            const rows = document.querySelectorAll(".variable-row");
            let csv = "code,name,category,type,dataset,has_categories,n_categories\\n";

            rows.forEach(row => {{
                const code = row.getAttribute("data-code");
                const name = row.cells[1].textContent;
                const category = row.getAttribute("data-category");
                const typeText = row.cells[3].textContent.trim();
                const dataset = row.getAttribute("data-dataset");
                const hasCategories =
                    row.cells[4].textContent.includes("\\uD83D\\uDCCA") ? "TRUE" : "FALSE";
                const nCategories = row.cells[4].textContent.match(/\\d+/)?.[0] || "0";

                csv += `"${{code}}","${{name}}","${{category}}","${{typeText}}",` +
                    `"${{dataset}}","${{hasCategories}}","${{nCategories}}"\\n`;
            }});

            const blob = new Blob([csv], {{ type: "text/csv;charset=utf-8;" }});
            const link = document.createElement("a");
            const url = URL.createObjectURL(blob);
            link.setAttribute("href", url);
            link.setAttribute("download", "census_variables.csv");
            link.style.visibility = "hidden";
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);

            showNotification("\\u2705 CSV file downloaded");
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
