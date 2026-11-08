"""Tutorial 3 -- Filtragem: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_03_filtragem.R``. Runs
the examples of ``docs/tutoriais/03-filtragem.md`` and writes the figures the
page shows, plus the tables and numbers it quotes (printed).

Starts from the standardized SIM-DO base of Tutorial 2 (SP, 2014-2019).

    python tools/tutoriais/tutorial_03_filtragem.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from plotnine import (
    aes,
    coord_flip,
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

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_COLORS, CF_PALETTE, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402
from climasus4py.utils.data import decode_age_sql  # noqa: E402

ANOS = list(range(2014, 2020))
# Oito municípios da RMSP, como no tutorial do climasus4r (códigos IBGE de 7 dígitos).
RMSP = ["3550308", "3548708", "3509502", "3548500",
        "3547809", "3530607", "3534401", "3505708"]


def conta(rel) -> int:
    return rel.count("*").fetchone()[0]


def milhar(valores):
    return [f"{v:,.0f}".replace(",", ".") for v in valores]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


# --- Entrada: base padronizada do Tutorial 2 ------------------------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_padr = cs.sus_data_standardize(cs.sus_data_clean_encoding(obitos), lang="pt")
n_base = conta(obitos_padr)

# --- Tabela: grupos de doença de interesse ---------------------------------------
grupos = {g["group"]: g for g in cs.list_disease_groups(lang="pt")}
tab_grupos = pd.DataFrame([
    {"Grupo": f'`"{n}"`', "Códigos CID-10": ", ".join(grupos[n]["codes"]),
     "Sensível ao clima": "Sim" if grupos[n]["climate_sensitive"] else "Não",
     "Descrição": grupos[n]["label"]}
    for n in ("respiratory", "cardiovascular", "pneumonia", "ischemic_heart")])
print("\nTABELA grupos\n" + tabela_md(tab_grupos))
print(f"total de grupos: {len(grupos)} | sensíveis ao clima: "
      f"{len(cs.list_disease_groups(climate_sensitive_only=True))}")

# --- Filtros por CID-10 -------------------------------------------------------------
resp = cs.sus_filter(obitos_padr, groups="respiratory")
cardio = cs.sus_filter(obitos_padr, codes="I00-I99")
n_resp, n_cardio = conta(resp), conta(cardio)
assert n_cardio == conta(cs.sus_filter(obitos_padr, groups="cardiovascular"))

# --- Filtros demográficos -------------------------------------------------------------
resp_idosos = cs.sus_filter(resp, age_min=60, municipality=RMSP)
cardio_idosos = cs.sus_filter(cardio, age_min=60, municipality=RMSP)
n_resp_id, n_cardio_id = conta(resp_idosos), conta(cardio_idosos)
# Os dois recortes numa chamada só dão o mesmo resultado.
assert n_resp_id == conta(cs.sus_filter(obitos_padr, groups="respiratory", age_min=60,
                                        municipality=RMSP))
mulheres_sp = conta(cs.sus_filter(resp, sex="Feminino", city="São Paulo"))
sudeste = conta(cs.sus_filter(resp, region="sudeste"))
print(f"\nmulheres na cidade de SP (respiratório): {mulheres_sp:,} | sudeste: {sudeste:,}")


def pct(n):
    return f"{100 * n / n_base:.1f}".replace(".", ",")


resumo = pd.DataFrame({
    "Etapa": ["Base padronizada (Tutorial 2)",
              "CID-10: respiratório (J00–J99)",
              "CID-10: cardiovascular (I00–I99)",
              "+ demografia: respiratório, 60+, 8 municípios da RMSP",
              "+ demografia: cardiovascular, 60+, 8 municípios da RMSP"],
    "Registros": milhar([n_base, n_resp, n_cardio, n_resp_id, n_cardio_id]),
    "% da base": ["100,0", pct(n_resp), pct(n_cardio), pct(n_resp_id), pct(n_cardio_id)],
})
print("\nTABELA funil\n" + tabela_md(resumo))

# --- Figura 1: funil -----------------------------------------------------------------
funil = pd.DataFrame({
    "etapa": ["Base padronizada", "CID-10\nrespiratório",
              "+ 60 anos ou mais\n(8 municípios da RMSP)"],
    "n": [n_base, n_resp, n_resp_id]})
funil["rotulo"] = milhar(funil["n"])
funil["etapa"] = pd.Categorical(funil["etapa"], categories=funil["etapa"][::-1])
p = (
    ggplot(funil, aes("etapa", "n", fill="etapa"))
    + geom_col(width=0.6, show_legend=False)
    + geom_text(aes(label="rotulo"), ha="left", nudge_y=n_base * 0.01,
                color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=CF_COLORS[:3][::-1])
    + scale_y_continuous(labels=milhar, expand=(0, 0, 0.18, 0))
    + coord_flip()
    + labs(title="Funil de filtragem: da base ao recorte de interesse",
           subtitle="SIM-DO, SP, 2014-2019 - causas respiratórias em idosos (60+)",
           x="", y="Registros")
    + tema_climasus()
)
save_fig(p, "filt_funil_cid")

# --- Figura 2: respiratório x cardiovascular em idosos ----------------------------------
comp = pd.DataFrame({"grupo": ["Respiratório\n(J00–J99)", "Cardiovascular\n(I00–I99)"],
                     "n": [n_resp_id, n_cardio_id]})
comp["rotulo"] = milhar(comp["n"])
p = (
    ggplot(comp, aes("grupo", "n", fill="grupo"))
    + geom_col(width=0.55, show_legend=False)
    + geom_text(aes(label="rotulo"), va="bottom", nudge_y=comp["n"].max() * 0.01,
                color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=[CF_PALETTE["accent"], CF_PALETTE["primary"]])
    + scale_y_continuous(labels=milhar)
    + labs(title="Óbitos em idosos (60+) por grande grupo de causa",
           subtitle="SIM-DO, 8 municípios da RMSP, 2014-2019 - filtros CID-10 + demografia",
           x="", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "filt_resp_vs_cardio")

# --- Figura 3: distribuição etária dos óbitos respiratórios -----------------------------
idade_sql = decode_age_sql("codigo_idade", list(resp.columns))
faixas = resp.aggregate(
    f"CASE WHEN ({idade_sql}) < 20 THEN '0-19' WHEN ({idade_sql}) < 40 THEN '20-39' "
    f"WHEN ({idade_sql}) < 60 THEN '40-59' WHEN ({idade_sql}) < 80 THEN '60-79' "
    f"WHEN ({idade_sql}) IS NOT NULL THEN '80+' END AS faixa, COUNT(*) AS n",
    "1").df().dropna()
faixas["grupo"] = faixas["faixa"].map(
    lambda f: "60 anos ou mais (mantidos)" if f in ("60-79", "80+") else "Menos de 60 (removidos)")
faixas["faixa"] = pd.Categorical(faixas["faixa"],
                                 categories=["0-19", "20-39", "40-59", "60-79", "80+"])
faixas["rotulo"] = milhar(faixas["n"])
print("\nfaixas:", dict(zip(faixas["faixa"].astype(str), faixas["n"], strict=True)))
p = (
    ggplot(faixas, aes("faixa", "n", fill="grupo"))
    + geom_col(width=0.65)
    + geom_text(aes(label="rotulo"), va="bottom", nudge_y=faixas["n"].max() * 0.01,
                color=CF_PALETTE["text_main"], size=9)
    + scale_fill_manual(values=[CF_PALETTE["primary"], "#A5D6A7"])
    + scale_y_continuous(labels=milhar)
    + labs(title="Distribuição etária - óbitos respiratórios",
           subtitle="O filtro age_min=60 mantém apenas as faixas 60-79 e 80+",
           x="Faixa etária (anos)", y="Número de óbitos", fill="")
    + tema_climasus()
)
save_fig(p, "filt_piramide_idade")

# --- Figura 4: série mensal dos óbitos respiratórios em idosos ---------------------------
serie = resp_idosos.aggregate(
    "date_trunc('month', data_obito) AS mes, COUNT(*) AS n", "1").df().dropna().sort_values("mes")
mes_max = serie.loc[serie["n"].idxmax()]
print(f"\nsérie: mínimo {serie['n'].min()} | máximo {serie['n'].max()} "
      f"em {str(mes_max['mes'])[:7]}")
p = (
    ggplot(serie, aes("mes", "n"))
    + geom_line(color=CF_PALETTE["primary"], size=0.8)
    + geom_point(color=CF_PALETTE["primary"], size=1.1)
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + scale_y_continuous(labels=milhar)
    + labs(title="Óbitos respiratórios mensais em idosos (60+)",
           subtitle="SIM-DO, 8 municípios da RMSP, 2014-2019 - após filtros CID-10 + demografia",
           x="Mês", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "filt_serie_idosos")
