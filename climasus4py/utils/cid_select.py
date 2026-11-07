"""Interactive CID-10 disease group explorer.

Mirrors R: climasus4r::sus_data_cid_select()

Three output modes:
- ``output="browser"``  — generates HTML and opens in default browser (mirrors R)
- ``output="notebook"`` — interactive ipywidgets UI (Jupyter/VSCode notebooks)
- ``output="console"``  — lists groups in terminal

Programmatic mode (no UI):
    sel = sus_data_cid_select(groups=["respiratory", "dengue"])
"""

from __future__ import annotations

import tempfile
import webbrowser
from pathlib import Path

from ..utils.disease_groups import get_disease_group_details, list_disease_groups

try:
    import ipywidgets as widgets
    from IPython.display import display as _display
    _HAS_WIDGETS = True
except ImportError:
    _HAS_WIDGETS = False

# ---------------------------------------------------------------------------
# UI text (mirrors R: get_ui_text_disease_groups)
# ---------------------------------------------------------------------------

_UI_TEXT: dict[str, dict[str, str]] = {
    "pt": {
        "title":           "Explorador de Grupos de Doenças — climasus4py",
        "subtitle":        "Selecione grupos de doenças CID-10 para usar em sus_filter()",
        "total_groups":    "Total de grupos",
        "climate_sensitive": "Sensíveis ao clima",
        "selected":        "Selecionados",
        "copy_selected":   "Copiar selecionados",
        "filter_climate":  "Filtrar climáticos",
        "clear_filters":   "Limpar filtros",
        "ready_to_use":    "Pronto para usar em sus_filter()",
        "group_name":      "Nome do Grupo",
        "icd_codes":       "Códigos CID-10",
        "climate_factors": "Fatores Climáticos",
        "description":     "Descrição",
        "climate_label":   "Clima",
        "copy_single":     "Copiar",
        "groups_label":    "grupos",
        "tips_title":      "Como usar",
        "tip1":            "Clique em uma linha para selecionar/deselecionar",
        "tip2":            "Use 'Copiar selecionados' para obter o código Python",
        "tip3":            "Grupos com 🌡 são sensíveis ao clima",
        "footer":          "climasus4py — dados de saúde e clima do Brasil",
    },
    "en": {
        "title":           "Disease Groups Explorer — climasus4py",
        "subtitle":        "Select CID-10 disease groups to use in sus_filter()",
        "total_groups":    "Total groups",
        "climate_sensitive": "Climate sensitive",
        "selected":        "Selected",
        "copy_selected":   "Copy selected",
        "filter_climate":  "Filter climate-sensitive",
        "clear_filters":   "Clear filters",
        "ready_to_use":    "Ready to use in sus_filter()",
        "group_name":      "Group Name",
        "icd_codes":       "ICD-10 Codes",
        "climate_factors": "Climate Factors",
        "description":     "Description",
        "climate_label":   "Climate",
        "copy_single":     "Copy",
        "groups_label":    "groups",
        "tips_title":      "How to use",
        "tip1":            "Click a row to select/deselect",
        "tip2":            "Use 'Copy selected' to get Python code",
        "tip3":            "Groups with 🌡 are climate-sensitive",
        "footer":          "climasus4py — Brazilian health and climate data",
    },
    "es": {
        "title":           "Explorador de Grupos de Enfermedades — climasus4py",
        "subtitle":        "Seleccione grupos de enfermedades CID-10 para usar en sus_filter()",
        "total_groups":    "Total de grupos",
        "climate_sensitive": "Sensibles al clima",
        "selected":        "Seleccionados",
        "copy_selected":   "Copiar seleccionados",
        "filter_climate":  "Filtrar climáticos",
        "clear_filters":   "Limpiar filtros",
        "ready_to_use":    "Listo para usar en sus_filter()",
        "group_name":      "Nombre del Grupo",
        "icd_codes":       "Códigos CID-10",
        "climate_factors": "Factores Climáticos",
        "description":     "Descripción",
        "climate_label":   "Clima",
        "copy_single":     "Copiar",
        "groups_label":    "grupos",
        "tips_title":      "Cómo usar",
        "tip1":            "Haga clic en una fila para seleccionar/deseleccionar",
        "tip2":            "Use 'Copiar seleccionados' para obtener el código Python",
        "tip3":            "Grupos con 🌡 son sensibles al clima",
        "footer":          "climasus4py — datos de salud y clima de Brasil",
    },
}

_CATEGORY_ICONS: dict[str, str] = {
    "Infectious": "🦠", "Cardiovascular": "❤️", "Respiratory": "🫁",
    "Injuries": "🩹", "Composite": "🔬", "Neoplasms": "🎗️",
    "Endocrine": "⚗️", "Mental": "🧠", "Neurological": "🧬",
    "Digestive": "🫙", "Skin": "🩺", "Genitourinary": "💧",
    "Pregnancy": "🤱", "Perinatal": "👶", "Congenital": "🧬",
    "Ill-defined": "❓", "External": "⚠️", "Climate-Health": "🌡️",
    "Age-Specific": "👥", "Syndromic": "📊",
}

# ---------------------------------------------------------------------------
# CSS (mirrors R: get_css_styles)
# ---------------------------------------------------------------------------

_CSS = """
    :root {
        --forest-dark: #2C5530; --forest-medium: #4A7C59;
        --forest-light: #8FB996; --earth-dark: #8B4513;
        --earth-light: #D2691E; --sky-light: #E8F4F8;
        --text-dark: #2C3E50; --text-light: #5D6D7E;
        --success: #27AE60; --warning: #F39C12; --danger: #E74C3C;
    }
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
           background:linear-gradient(135deg,var(--sky-light) 0%,#f5f9fc 100%);
           color:var(--text-dark); line-height:1.6; padding:20px; min-height:100vh; }
    .container { max-width:1600px; margin:0 auto; background:white; border-radius:16px;
                 box-shadow:0 10px 40px rgba(44,83,48,0.15); overflow:hidden;
                 border:1px solid rgba(139,69,19,0.1); }
    .header { background:linear-gradient(135deg,#1e3a28 0%,#2C5530 100%);
              color:white; padding:30px 40px; }
    .header h1 { font-size:28px; font-weight:700; margin-bottom:8px; }
    .header p { opacity:.85; font-size:16px; }
    .stats-bar { display:flex; gap:30px; padding:16px 40px;
                 background:rgba(74,124,89,0.08); border-bottom:1px solid rgba(74,124,89,0.15); }
    .stats-bar div { font-size:14px; color:var(--text-light); }
    .stats-bar strong { color:var(--forest-dark); }
    .controls { display:flex; gap:12px; padding:16px 40px;
                border-bottom:1px solid #eee; flex-wrap:wrap; }
    .btn { padding:8px 16px; border-radius:8px; border:none; cursor:pointer;
           font-size:14px; font-weight:500; display:flex; align-items:center; gap:6px; }
    .btn-primary { background:var(--forest-dark); color:white; }
    .btn-primary:hover { background:var(--forest-medium); }
    .btn-secondary { background:white; color:var(--forest-dark);
                     border:1px solid var(--forest-light); }
    .btn-secondary:hover { background:rgba(74,124,89,0.08); }
    .code-help { display:none; margin:12px 40px; padding:12px 16px;
                 background:#f8f9fa; border-radius:8px; border-left:3px solid var(--forest-dark); }
    .code-help code { font-family:monospace; font-size:13px; color:var(--forest-dark); }
    .table-container { padding:0 40px 20px; overflow-x:auto; }
    table { width:100%; border-collapse:collapse; margin-top:16px; }
    th { background:var(--forest-dark); color:white; padding:12px 16px;
         text-align:left; font-weight:600; font-size:13px; }
    .category-header td { background:rgba(74,124,89,0.06);
                          padding:10px 16px; border-bottom:1px solid rgba(74,124,89,0.15); }
    .category-header-content { display:flex; align-items:center; gap:10px; }
    .category-icon { font-size:18px; }
    .category-name { font-weight:600; color:var(--forest-dark); font-size:14px; }
    .category-stats { font-size:12px; color:var(--text-light); margin-left:auto; }
    .group-row { cursor:pointer; transition:background .15s; border-bottom:1px solid #f0f0f0; }
    .group-row:hover { background:rgba(74,124,89,0.06); }
    .group-row.selected { background:rgba(44,85,48,0.12); }
    td { padding:10px 16px; font-size:13px; vertical-align:middle; }
    .name-container { display:flex; align-items:center; gap:8px; }
    .group-name { background:rgba(44,85,48,0.08); color:var(--forest-dark);
                  padding:2px 8px; border-radius:4px; font-size:12px; }
    .copy-icon { cursor:pointer; opacity:.5; font-size:12px; }
    .copy-icon:hover { opacity:1; }
    .icd-cell code { font-family:monospace; font-size:12px; color:var(--earth-dark); }
    .climate-chip { display:inline-block; font-size:10px; padding:2px 6px;
                    border-radius:10px; margin:1px; background:rgba(74,124,89,0.1);
                    color:var(--forest-dark); }
    .climate-badge { font-size:16px; }
    .description-cell { color:var(--text-light); max-width:300px;
                        overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .tips { padding:20px 40px; background:rgba(74,124,89,0.04);
            border-top:1px solid rgba(74,124,89,0.1); }
    .tips h3 { color:var(--forest-dark); margin-bottom:10px; font-size:15px; }
    .tips ul { padding-left:20px; }
    .tips li { color:var(--text-light); font-size:13px; margin-bottom:4px; }
    .footer { text-align:center; padding:16px; font-size:12px;
              color:var(--text-light); border-top:1px solid #eee; }
"""

# ---------------------------------------------------------------------------
# JavaScript (mirrors R: get_javascript)
# ---------------------------------------------------------------------------

_JS_TEMPLATE = """
    let selectedGroups = new Set();
    let showingClimateOnly = false;

    function toggleRow(row) {
        const name = row.dataset.name;
        if (selectedGroups.has(name)) {
            selectedGroups.delete(name);
            row.classList.remove('selected');
        } else {
            selectedGroups.add(name);
            row.classList.add('selected');
        }
        document.getElementById('selectedCount').textContent = selectedGroups.size;
        updateCodeOutput();
    }

    function updateCodeOutput() {
        const help = document.getElementById('codeHelp');
        const out  = document.getElementById('codeOutput');
        if (selectedGroups.size > 0) {
            help.style.display = 'block';
            const groups = Array.from(selectedGroups).map(g => `"${g}"`).join(', ');
            out.textContent = `cs.sus_filter(rel, groups=[${groups}])`;
        } else {
            help.style.display = 'none';
        }
    }

    function copySelectedGroups() {
        if (selectedGroups.size === 0) { alert('COPY_NONE_MSG'); return; }
        const groups = Array.from(selectedGroups).map(g => `"${g}"`).join(', ');
        const code = `cs.sus_filter(rel, groups=[${groups}])`;
        navigator.clipboard.writeText(code).then(() => alert('COPY_OK_MSG'));
    }

    function copySingleGroup(name) {
        event.stopPropagation();
        navigator.clipboard.writeText(`"${name}"`).then(() => {
            alert(`COPY_SINGLE_MSG: ${name}`);
        });
    }

    function filterClimate() {
        showingClimateOnly = !showingClimateOnly;
        document.querySelectorAll('.group-row').forEach(row => {
            const isClimate = row.dataset.climate === 'true';
            row.style.display = (!showingClimateOnly || isClimate) ? '' : 'none';
        });
    }

    function clearFilters() {
        showingClimateOnly = false;
        document.querySelectorAll('.group-row').forEach(row => {
            row.style.display = '';
            row.classList.remove('selected');
        });
        selectedGroups.clear();
        document.getElementById('selectedCount').textContent = 0;
        updateCodeOutput();
    }

    document.querySelectorAll('.group-row').forEach(row => {
        row.addEventListener('click', () => toggleRow(row));
    });
"""

# ---------------------------------------------------------------------------
# HTML generator
# ---------------------------------------------------------------------------

def _generate_html(groups: list[dict], lang: str) -> str:
    text = _UI_TEXT.get(lang, _UI_TEXT["pt"])
    n_climate = sum(1 for g in groups if g.get("climate_sensitive", False))

    # build table rows
    table_rows = ""
    current_category = ""
    for g in groups:
        cat = g.get("category", "Other")
        if cat != current_category:
            current_category = cat
            cat_count = sum(1 for x in groups if x.get("category") == cat)
            icon = _CATEGORY_ICONS.get(cat, "📋")
            table_rows += (
                f'<tr class="category-header">'
                f'<td colspan="5" class="category-title">'
                f'<div class="category-header-content">'
                f'<span class="category-icon">{icon}</span>'
                f'<span class="category-name">{cat}</span>'
                f'<span class="category-stats">{cat_count} {text["groups_label"]}</span>'
                f'</div></td></tr>'
            )

        # climate chips
        factors = g.get("climate_factors", [])
        climate_chips = "".join(
            f'<span class="climate-chip">{f}</span>'
            for f in (factors if isinstance(factors, list) else [])
        )
        climate_badge = "🌡" if g.get("climate_sensitive") else ""

        # codes
        codes = g.get("codes", [])
        codes_str = ", ".join(codes) if isinstance(codes, list) else str(codes)

        # description
        desc = g.get("description", "")
        desc_text = desc.get(lang, desc.get("pt", "")) if isinstance(desc, dict) else str(desc)
        desc_short = desc_text[:80] + ("..." if len(desc_text) > 80 else "")

        name = g.get("group", g.get("name", ""))
        climate_val = str(g.get("climate_sensitive", False)).lower()

        table_rows += (
            f'<tr class="group-row" data-name="{name}" '
            f'data-category="{cat}" data-climate="{climate_val}">'
            f'<td class="name-cell">'
            f'<div class="name-container">'
            f'<code class="group-name">{name}</code>'
            f'<span class="copy-icon" onclick="copySingleGroup(\'{name}\')" '
            f'title="{text["copy_single"]}">📄</span>'
            f'</div></td>'
            f'<td class="icd-cell"><code>{codes_str}</code></td>'
            f'<td class="climate-cell">{climate_chips}</td>'
            f'<td class="description-cell" title="{desc_text}">{desc_short}</td>'
            f'<td class="badge-cell">{climate_badge}</td>'
            f'</tr>'
        )

    # JS with localized messages
    js = _JS_TEMPLATE.replace(
        "COPY_NONE_MSG", "Nenhum grupo selecionado." if lang == "pt" else "No groups selected."
    ).replace(
        "COPY_OK_MSG", "Código copiado!" if lang == "pt" else "Code copied!"
    ).replace(
        "COPY_SINGLE_MSG", "Copiado" if lang == "pt" else "Copied"
    )

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{text['title']}</title>
    <style>{_CSS}</style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{text['title']}</h1>
            <p>{text['subtitle']}</p>
        </div>
        <div class="stats-bar">
            <div>{text['total_groups']}: <strong id="totalCount">{len(groups)}</strong></div>
            <div>{text['climate_sensitive']}: <strong id="climateCount">{n_climate}</strong></div>
            <div>{text['selected']}: <strong id="selectedCount">0</strong></div>
        </div>
        <div class="controls">
            <button class="btn btn-primary" onclick="copySelectedGroups()">
                <span>📄</span> {text['copy_selected']}
            </button>
            <button class="btn btn-secondary" onclick="filterClimate()">
                <span>🌡</span> {text['filter_climate']}
            </button>
            <button class="btn btn-secondary" onclick="clearFilters()">
                <span>🔄</span> {text['clear_filters']}
            </button>
        </div>
        <div id="codeHelp" class="code-help">
            <strong>{text['ready_to_use']}:</strong><br>
            <code id="codeOutput"></code>
        </div>
        <div class="table-container">
            <table id="groupsTable">
                <thead>
                    <tr>
                        <th width="200">{text['group_name']}</th>
                        <th width="150">{text['icd_codes']}</th>
                        <th width="250">{text['climate_factors']}</th>
                        <th>{text['description']}</th>
                        <th width="60">{text['climate_label']}</th>
                    </tr>
                </thead>
                <tbody>{table_rows}</tbody>
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
        <div class="footer">{text['footer']}</div>
    </div>
    <script>{js}</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# CidSelection result object
# ---------------------------------------------------------------------------

class CidSelection:
    """Result of ``sus_data_cid_select()``.

    Attributes:
        groups: List of selected group keys (e.g. ``['respiratory']``).
        codes:  Flat list of ICD-10 codes for the selected groups.

    Ready to feed into ``sus_filter``::

        sel = sus_data_cid_select(groups=['respiratory', 'dengue'])
        sus_filter(rel, groups=sel.groups)
        sus_filter(rel, codes=sel.codes)
    """

    def __init__(self, groups: list[str], lang: str = "pt") -> None:
        self.lang   = lang
        self.groups = list(groups)
        self.codes: list[str] = []
        for g in self.groups:
            try:
                self.codes.extend(get_disease_group_details(g, lang=lang)["codes"])
            except Exception:
                pass

    def __repr__(self) -> str:
        return f"CidSelection(groups={self.groups!r}, codes={self.codes!r})"

    def __bool__(self) -> bool:
        return bool(self.groups)

# ---------------------------------------------------------------------------
# Widget helper (notebook mode)
# ---------------------------------------------------------------------------

def _format_details(group_key: str, lang: str) -> str:
    d  = get_disease_group_details(group_key, lang=lang)
    cs = ("sim" if lang == "pt" else "sí" if lang == "es" else "yes") \
         if d["climate_sensitive"] else \
         ("não" if lang == "pt" else "no")
    factors = ", ".join(d["climate_factors"]) or "—"
    codes   = ", ".join(d["codes"]) or "—"
    label   = d["label"]
    return (
        f"<b>{label}</b> <code>({group_key})</code><br>"
        f"{d['description']}<br><br>"
        f"<b>CID-10:</b> {codes}<br>"
        f"<b>Sensível ao clima:</b> {cs}<br>"
        f"<b>Fatores:</b> {factors}"
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sus_data_cid_select(
    *,
    groups: list[str] | str | None = None,
    lang: str = "pt",
    climate_sensitive_only: bool = False,
    output: str = "browser",
) -> CidSelection:
    """Interactive CID-10 disease group explorer.

    Mirrors ``climasus4r::sus_data_cid_select()``.

    Three modes:

    * **Programmatic** — pass ``groups=`` and get a ``CidSelection``
      immediately, no UI.
    * **Browser** (``output="browser"``) — generates an HTML page and
      opens it in the default browser. Works in any environment.
    * **Notebook** (``output="notebook"``) — interactive ipywidgets UI.
      Requires ``pip install ipywidgets``.
    * **Console** (``output="console"``) — lists groups in the terminal.

    Args:
        groups: Pre-selected group(s). When provided, skips UI.
        lang: Language for labels — ``"pt"`` (default), ``"en"``, ``"es"``.
        climate_sensitive_only: List only climate-sensitive groups.
        output: Output mode — ``"browser"``, ``"notebook"``, ``"console"``.

    Returns:
        ``CidSelection`` with ``.groups`` and ``.codes``.

    Example:
        >>> sel = sus_data_cid_select(groups=["respiratory", "dengue"])
        >>> sus_filter(rel, groups=sel.groups)
        >>> sus_data_cid_select(output="browser", lang="en")
        >>> sus_data_cid_select(output="console", climate_sensitive_only=True)
    """
    if lang not in ("pt", "en", "es"):
        raise ValueError(f"lang must be 'pt', 'en' or 'es', got {lang!r}")
    if output not in ("browser", "notebook", "console"):
        raise ValueError(f"output must be 'browser', 'notebook' or 'console', got {output!r}")

    # --- programmatic mode ---
    if groups is not None:
        if isinstance(groups, str):
            groups = [groups]
        return CidSelection(groups, lang=lang)

    available = list_disease_groups(
        climate_sensitive_only=climate_sensitive_only, lang=lang
    )

    # --- browser mode (mirrors R) ---
    if output == "browser":
        html = _generate_html(available, lang)
        tmp  = Path(tempfile.mktemp(suffix=".html"))
        tmp.write_text(html, encoding="utf-8")
        webbrowser.open(tmp.as_uri())
        print(f"[sus_data_cid_select] Opened in browser: {tmp}")
        return CidSelection([], lang=lang)

    # --- console mode ---
    if output == "console":
        print(f"\n{'='*60}")
        print(f"Disease Groups — {len(available)} total")
        print(f"{'='*60}")
        current_cat = ""
        for g in available:
            cat = g.get("category", "Other")
            if cat != current_cat:
                current_cat = cat
                print(f"\n  [{cat}]")
            label  = g.get("label", g.get("group", ""))
            codes  = g.get("codes", [])
            codes_str = ", ".join(codes) if isinstance(codes, list) else str(codes)
            climate = "🌡" if g.get("climate_sensitive") else "  "
            print(f"  {climate} {g['group']:<35} {codes_str}")
        print(f"\n{'='*60}\n")
        return CidSelection([], lang=lang)

    # --- notebook mode ---
    if not _HAS_WIDGETS:
        raise RuntimeError(
            "sus_data_cid_select: notebook mode requires 'ipywidgets'. "
            "Install with: pip install ipywidgets\n"
            "Or use: sus_data_cid_select(output='browser') — works everywhere.\n"
            "Available groups: " + ", ".join(g["group"] for g in available)
        )

    selection = CidSelection([], lang=lang)
    options   = [(f"{g['label']}  ({g['group']})", g["group"]) for g in available]

    picker  = widgets.SelectMultiple(
        options=options, rows=min(14, len(options)),
        layout=widgets.Layout(width="400px"),
        description="Grupos:", style={"description_width": "initial"},
    )
    details = widgets.HTML(value="<i>Selecione um grupo para ver os detalhes.</i>")
    confirm = widgets.Button(description="Confirmar seleção",
                             button_style="success", icon="check")
    status  = widgets.HTML(value="")

    def _on_change(change: dict) -> None:
        keys = change["new"]
        if not keys:
            details.value = "<i>Selecione um grupo para ver os detalhes.</i>"
            return
        details.value = "<hr>".join(_format_details(k, lang) for k in keys)

    def _on_confirm(_: widgets.Button) -> None:
        chosen = list(picker.value)
        selection.groups = chosen
        selection.codes  = []
        for g in chosen:
            try:
                selection.codes.extend(
                    get_disease_group_details(g, lang=lang)["codes"]
                )
            except Exception:
                pass
        if chosen:
            status.value = (
                f"<b style='color:green'>✓ {len(chosen)} grupo(s) selecionado(s):</b> "
                f"{', '.join(chosen)}"
            )
        else:
            status.value = "<b style='color:#b00'>Nenhum grupo selecionado.</b>"

    picker.observe(_on_change, names="value")
    confirm.on_click(_on_confirm)

    _display(widgets.VBox([
        widgets.HTML(f"<h4>{_UI_TEXT[lang]['title']}</h4>"),
        widgets.HBox([picker, widgets.Box([details],
                      layout=widgets.Layout(padding="0 0 0 16px"))]),
        confirm, status,
    ]))
    return selection
