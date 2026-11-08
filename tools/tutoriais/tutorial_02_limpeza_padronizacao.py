"""Tutorial 2 -- Limpeza e Padronização: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_02_encoding_padronizacao.R``.
Runs the examples of ``docs/tutoriais/02-limpeza-padronizacao.md`` and writes
the figures the page shows, plus the tables and numbers it quotes (printed).

Starts from the SIM-DO base of Tutorial 1 (SP, 2014-2019), read from the
DATASUS cache that tutorial filled.

    python tools/tutoriais/tutorial_02_limpeza_padronizacao.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from plotnine import (
    aes,
    coord_flip,
    geom_col,
    geom_text,
    ggplot,
    labs,
    scale_fill_manual,
    scale_y_continuous,
)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_COLORS, CF_PALETTE, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402
from climasus4py.utils.data import load_json  # noqa: E402

ANOS = list(range(2014, 2020))


def conta(rel) -> int:
    return rel.count("*").fetchone()[0]


def milhar(valores):
    return [f"{v:,.0f}".replace(",", ".") for v in valores]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


# --- Entrada: a base do Tutorial 1 -------------------------------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
n_bruto, c_bruto = conta(obitos), len(obitos.columns)

# --- Passo 1: limpeza ----------------------------------------------------------
obitos_limpo = cs.sus_data_clean_encoding(obitos)
n_limpo = conta(obitos_limpo)
n_sem_idade = conta(cs.sus_data_clean_encoding(obitos, dedup=False))
print(f"\nimportado {n_bruto:,} | depois da limpeza {n_limpo:,} "
      f"(repetidas {n_bruto - n_sem_idade:,}... ver abaixo)")
n_repetidas = n_bruto - conta(obitos.distinct())
print(f"linhas 100% iguais: {n_repetidas:,} | idades fora de 0-120: "
      f"{n_bruto - n_repetidas - n_limpo:,}")

# --- Passo 2: padronização --------------------------------------------------------
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
print(f"\nsistema: {cs.sus_meta(obitos_padr, 'system')} | "
      f"estágio: {cs.sus_meta(obitos_padr, 'stage')} | colunas: {len(obitos_padr.columns)}")

# Os mesmos campos nos três idiomas.
campos = ["DTOBITO", "SEXO", "RACACOR", "CAUSABAS", "CODMUNRES", "LOCOCOR"]
idiomas = {lang: list(cs.sus_data_standardize(obitos_limpo.limit(1), lang=lang).columns)
           for lang in ("pt", "en", "es")}
pos = [list(obitos_limpo.columns).index(c) for c in campos]
nomes = pd.DataFrame({"Original (DATASUS)": [f"`{c}`" for c in campos],
                      **{lang: [f"`{idiomas[lang][i]}`" for i in pos] for lang in idiomas}})
print("\nTABELA idiomas\n" + tabela_md(nomes))

# Amostra antes/depois.
amostra = obitos_padr.select(
    '"data_obito", "sexo", "raca", "causa_basica", "codigo_municipio_residencia", '
    '"local_ocorrencia_obito"').limit(5).df()
amostra["data_obito"] = pd.to_datetime(amostra["data_obito"]).dt.strftime("%Y-%m-%d")
print("\nTABELA amostra padronizada\n" + tabela_md(amostra))

# --- Tabela: dicionário de tradução (pt) ---------------------------------------------
cat = load_json("dictionaries/pt-pt/categories.json")["COMMON"]
linhas = []
for var, chave in [("SEXO → sexo", "sexo"), ("RACACOR → raca", "raca"),
                   ("LOCOCOR → local_ocorrencia_obito", "local_ocorrencia_obito")]:
    vistos = set()
    for cod, rot in cat[chave].items():
        if len(cod) == 1 and cod.isdigit() and (cod, rot) not in vistos:
            vistos.add((cod, rot))
            linhas.append({"Variável": var, "Código": cod, "Rótulo": rot})
dic = pd.DataFrame(linhas)
dic = dic[~((dic["Variável"].str.startswith("SEXO")) & (dic["Código"] == "3"))]  # 3 is SIH's
print("\nTABELA dicionario\n" + tabela_md(dic))

# --- Figura: sexo ------------------------------------------------------------------
sexo = obitos_padr.aggregate('"sexo", COUNT(*) AS n', '"sexo"').df().dropna()
sexo["rotulo"] = milhar(sexo["n"])
ordem = sexo.sort_values("n", ascending=False)["sexo"]
sexo["sexo"] = pd.Categorical(sexo["sexo"], categories=ordem)
print("\nsexo:", dict(zip(sexo["sexo"].astype(str), sexo["n"], strict=True)))
p = (
    ggplot(sexo, aes("sexo", "n", fill="sexo"))
    + geom_col(width=0.6, show_legend=False)
    + geom_text(aes(label="rotulo"), va="bottom", nudge_y=sexo["n"].max() * 0.01,
                color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=CF_COLORS)
    + scale_y_continuous(labels=milhar)
    + labs(title="Óbitos por sexo após padronização",
           subtitle="Rótulos legíveis (1 → Masculino, 2 → Feminino) - SIM-DO, SP, 2014-2019",
           x="", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "padr_sexo")

# --- Figura: raça/cor ----------------------------------------------------------------
raca = obitos_padr.aggregate('"raca", COUNT(*) AS n', '"raca"').df()
raca["raca"] = raca["raca"].fillna("Não informado")
raca["rotulo"] = milhar(raca["n"])
raca["raca"] = pd.Categorical(raca["raca"], categories=raca.sort_values("n")["raca"])
print("raca:", dict(zip(raca["raca"].astype(str), raca["n"], strict=True)))
cores = (CF_COLORS * 2)[: len(raca)]
p = (
    ggplot(raca, aes("raca", "n", fill="raca"))
    + geom_col(width=0.65, show_legend=False)
    + geom_text(aes(label="rotulo"), ha="left", nudge_y=raca["n"].max() * 0.01,
                color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=cores)
    + scale_y_continuous(labels=milhar, expand=(0, 0, 0.15, 0))
    + coord_flip()
    + labs(title="Óbitos por raça/cor após padronização",
           subtitle="Códigos decodificados (1 → Branca, 4 → Parda, ...) - SIM-DO, SP, 2014-2019",
           x="", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "padr_raca")

# --- Tabela-resumo ---------------------------------------------------------------------
resumo = pd.DataFrame({
    "Indicador": ["Linhas (importado)", "Linhas (depois da limpeza)", "Colunas (importado)",
                  "Colunas (padronizado)", "Sistema detectado", "Estágio (`sus_meta`)",
                  "Idioma de saída"],
    "Valor": [milhar([n_bruto])[0], milhar([n_limpo])[0], str(c_bruto),
              str(len(obitos_padr.columns)), str(cs.sus_meta(obitos_padr, "system")),
              f"`{cs.sus_meta(obitos_padr, 'stage')}`", "pt"],
})
print("\nTABELA resumo\n" + tabela_md(resumo))
