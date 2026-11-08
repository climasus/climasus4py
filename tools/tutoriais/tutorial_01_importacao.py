"""Tutorial 1 -- Importação de dados do DATASUS: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_01_importacao.R``. Runs
the examples of ``docs/tutoriais/01-importacao.md`` for real and writes the
figures the page shows, plus the numbers its text quotes (printed at the end).

Caso condutor (all nine pipeline tutorials): temperature and health outcomes
in older adults in the São Paulo metropolitan region, 2014-2019. This one
builds the raw multi-system base: deaths (SIM-DO), hospital admissions
(SIH-RD, winter months only) and dengue notifications (SINAN-DENGUE).

    python tools/tutoriais/tutorial_01_importacao.py

Downloads several hundred MB from the DATASUS FTP on the first run (cached in
``dados/cache`` under the working directory).
"""

from __future__ import annotations

import sys
import time

import pandas as pd
from plotnine import (
    aes,
    geom_col,
    geom_line,
    geom_point,
    geom_text,
    ggplot,
    labs,
    scale_fill_manual,
    scale_x_datetime,
    scale_y_continuous,
)

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent))
from _setup_tutoriais import CF_COLORS, CF_PALETTE, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

ANOS = list(range(2014, 2020))
numeros: dict[str, object] = {}


def milhar(valores):
    """Axis labels with the Brazilian thousands separator (4.000.000)."""
    return [f"{v:,.0f}".replace(",", ".") for v in valores]


def conta(rel) -> int:
    return rel.count("*").fetchone()[0]


def bloco(nome: str):
    print(f"\n>> {nome}", flush=True)
    return time.time()


# --- Bloco 1: base bruta multi-sistema --------------------------------------
t = bloco("SIM-DO, SP, 2014-2019 (sistema anual)")
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS)
numeros["obitos"] = conta(obitos)
print(f"   {numeros['obitos']:,} linhas em {time.time() - t:.0f}s")

t = bloco("SIH-RD, SP, 2014-2019, junho a agosto (sistema mensal)")
internacoes = cs.sus_data_import(system="SIH-RD", uf="SP", year=ANOS, month=[6, 7, 8])
numeros["internacoes"] = conta(internacoes)
print(f"   {numeros['internacoes']:,} linhas em {time.time() - t:.0f}s")

t = bloco("SINAN-DENGUE, SP, 2014-2019")
dengue = cs.sus_data_import(system="SINAN-DENGUE", uf="SP", year=ANOS)
numeros["dengue"] = conta(dengue)
print(f"   {numeros['dengue']:,} linhas em {time.time() - t:.0f}s")

# --- Bloco 2: recortes espaciais ---------------------------------------------
t = bloco("SIM-DO, região Sudeste, 2019")
obitos_sudeste = cs.sus_data_import(system="SIM-DO", region="sudeste", year=2019)
numeros["obitos_sudeste"] = conta(obitos_sudeste)
print(f"   {numeros['obitos_sudeste']:,} linhas em {time.time() - t:.0f}s")

t = bloco("Município de São Paulo (IBGE 3550308), a partir de obitos")
obitos_muni_sp = cs.sus_filter(obitos, municipality="3550308")
numeros["obitos_muni_sp"] = conta(obitos_muni_sp)
print(f"   {numeros['obitos_muni_sp']:,} linhas")

# --- Bloco 3: estrutura do SIM-DO importado ----------------------------------
bloco("Estrutura do SIM-DO importado")
chave = ["DTOBITO", "DTNASC", "SEXO", "IDADE", "RACACOR", "CAUSABAS", "CODMUNRES", "CODMUNOCOR"]
chave = [c for c in chave if c in obitos.columns]
estrutura = obitos.select(", ".join(f'"{c}"' for c in chave)).limit(10).df()
for c in estrutura.columns:
    if pd.api.types.is_datetime64_any_dtype(estrutura[c]):
        estrutura[c] = estrutura[c].dt.strftime("%Y-%m-%d")
numeros["n_colunas_sim"] = len(obitos.columns)
tipos = zip(obitos.columns, obitos.dtypes, strict=True)
numeros["tipos"] = {c: str(t) for c, t in tipos if c in chave}
linhas_md = ["| " + " | ".join(chave) + " |", "|" + "---|" * len(chave)]
linhas_md += ["| " + " | ".join("" if pd.isna(v) else str(v) for v in linha) + " |"
              for linha in estrutura.itertuples(index=False)]
print("\n".join(linhas_md))

# --- Bloco 4: figura, registros por sistema ----------------------------------
bloco("Figura: registros por sistema")
registros = pd.DataFrame({
    "sistema": ["SIM-DO\n(óbitos)", "SIH-RD\n(internações)", "SINAN\n(dengue)"],
    "n": [numeros["obitos"], numeros["internacoes"], numeros["dengue"]],
})
registros["rotulo"] = registros["n"].map(lambda n: f"{n:,}".replace(",", "."))
registros["sistema"] = pd.Categorical(
    registros["sistema"], categories=registros.sort_values("n", ascending=False)["sistema"])
p = (
    ggplot(registros, aes("sistema", "n", fill="sistema"))
    + geom_col(width=0.65, show_legend=False)
    + geom_text(aes(label="rotulo"), va="bottom", nudge_y=registros["n"].max() * 0.01,
                color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=CF_COLORS)
    + scale_y_continuous(labels=milhar)
    + labs(title="Registros importados por sistema DATASUS",
           subtitle="Estado de São Paulo, 2014-2019 (SIH-RD: apenas inverno, jun-ago)",
           x="", y="Número de registros")
    + tema_climasus()
)
save_fig(p, "import_registros_por_sistema")

# --- Bloco 5: figura, série mensal de óbitos ---------------------------------
bloco("Figura: série mensal de óbitos")
tipo = dict(zip(obitos.columns, map(str, obitos.dtypes), strict=True))["DTOBITO"]
data_sql = ('"DTOBITO"' if tipo.upper().startswith(("DATE", "TIMESTAMP"))
            else "strptime(\"DTOBITO\", '%d%m%Y')")
serie = obitos.aggregate(
    f"date_trunc('month', {data_sql}) AS mes, COUNT(*) AS obitos", "1").df()
serie = serie.dropna().sort_values("mes")
numeros["serie_min"] = int(serie["obitos"].min())
numeros["serie_max"] = int(serie["obitos"].max())
numeros["mes_max"] = str(serie.loc[serie["obitos"].idxmax(), "mes"])[:7]
p = (
    ggplot(serie, aes("mes", "obitos"))
    + geom_line(color=CF_PALETTE["primary"], size=0.8)
    + geom_point(color=CF_PALETTE["primary"], size=1.1)
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + scale_y_continuous(labels=milhar)
    + labs(title="Óbitos mensais (todas as causas) - SIM-DO",
           subtitle="Estado de São Paulo, 2014-2019", x="Mês", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "import_serie_obitos_sp")

print("\n== números citados na página")
for k, v in numeros.items():
    print(f"   {k}: {v}")
