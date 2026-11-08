"""Tutorial 6 -- Vínculo Socioeconômico: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_06_socioeconomico.R``.
Runs the examples of ``docs/tutoriais/06-socioeconomico.md`` and writes the
figures the page shows, plus the tables and numbers it quotes (printed).

The census link is real (IBGE Census 2010 from climasus-data). The four
indicators of the R tutorial need counts the census table does not carry
(elderly, young, working-age people; households with sewage and piped water,
M255), so -- like the R script -- they are computed on illustrative columns
drawn at random around each municipality's real population.

    python tools/tutoriais/tutorial_06_socioeconomico.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from plotnine import (aes, coord_flip, geom_col, geom_errorbar, geom_point, geom_text,  # noqa: E402
                      ggplot, labs, scale_x_continuous, scale_y_continuous)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_PALETTE, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

warnings.filterwarnings("ignore", message=".*materializada.*")

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]
CATEGORIAS = {
    "demographic": "Demográficos", "socioeconomic": "Socioeconômicos",
    "mortality": "Mortalidade", "morbidity": "Morbidade",
    "maternal_child": "Materno-infantil", "health_resources": "Recursos de saúde",
}


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def milhar(valores):
    return [f"{v:,.0f}".replace(",", ".") for v in valores]


def decimal(v: float, casas: int = 1) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


# --- Entrada: o caso espacial do Tutorial 5 ---------------------------------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_limpo = cs.sus_data_clean_encoding(obitos)
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
resp_idosos = cs.sus_filter(obitos_padr, groups="respiratory", age_min=60, city=RMSP)
obitos_vars = cs.sus_data_create_variables(
    resp_idosos, age_breaks=[60, 70, 80, 999], age_labels=["60-69", "70-79", "80+"],
    climate_region="sudeste", lang="pt", verbose=False)
serie = cs.sus_data_aggregate(obitos_vars, time_unit="month", group_by=["faixa_etaria"],
                              join_col="residencia", lang="pt", verbose=False)
caso_espacial = cs.sus_spatial_join(serie, geo_level="municipality", join_col="residencia",
                                    lang="pt")

# --- Passo 1: descobrir variáveis do censo -----------------------------------------------
codigos = cs.sus_census_select(dataset="population", year=2010, output="codes", lang="pt",
                               verbose=False)
print(f"\nsus_census_select: {len(codigos)} códigos; primeiros: {codigos[:6]}")

# --- Passo 2: vincular o censo (real) ----------------------------------------------------
caso_censo = cs.sus_census(caso_espacial, year=2010, variables=["sample_weight_sum", "n_records"])
censo = caso_censo.df()
print(f"sus_census: linhas {len(serie.df()):,} -> {len(censo):,} | colunas novas: "
      f"{[c for c in censo.columns if c not in caso_espacial.columns]}")

# --- Passo 3: catálogo -------------------------------------------------------------------
catalogo = cs.sus_socio_list_indicators(lang="pt")
mortalidade = cs.sus_socio_list_indicators(lang="pt", category="mortality")
print(f"catálogo: {len(catalogo)} indicadores | mortalidade: {len(mortalidade)} | "
      f"por categoria: {catalogo['category'].value_counts().to_dict()}")

# --- Passo 4: indicadores (colunas ilustrativas, como no R) --------------------------------
municipios = censo.drop_duplicates("code_muni_7")[["code_muni_7", "spatial_name",
                                                   "sample_weight_sum"]].reset_index(drop=True)
sorteio = np.random.default_rng(2026)
n = len(municipios)
populacao = municipios["sample_weight_sum"]
municipios["idosos"] = (populacao * sorteio.uniform(0.09, 0.16, n)).round()
municipios["jovens"] = (populacao * sorteio.uniform(0.16, 0.24, n)).round()
municipios["ativos"] = populacao - municipios["idosos"] - municipios["jovens"]
municipios["domicilios"] = (populacao / sorteio.uniform(2.8, 3.4, n)).round()
municipios["domicilios_esgoto"] = (municipios["domicilios"] * sorteio.uniform(0.70, 0.99, n)).round()
municipios["domicilios_agua"] = (municipios["domicilios"] * sorteio.uniform(0.85, 0.99, n)).round()

caso_socio = cs.sus_socio_compute_indicators(
    municipios,
    indicators=["aging_index", "dependency_ratio", "sewage_connection_rate",
                "water_connection_rate"],
    col_mapping={"pop_elderly": "idosos", "pop_young": "jovens", "pop_working": "ativos",
                 "total_hh": "domicilios", "hh_sewage": "domicilios_esgoto",
                 "hh_water": "domicilios_agua"},
    confidence_level=0.95, add_ci=True, lang="pt", verbose=False)
print("colunas de indicador:", [c for c in caso_socio.columns if c.startswith("ind_")])
print("meta:", {k: caso_socio.attrs["sus_meta"].get(k) for k in ("stage", "type")})

# --- Tabelas -------------------------------------------------------------------------------
usados = ["aging_index", "dependency_ratio", "sewage_connection_rate", "water_connection_rate",
          "urbanization_rate", "homicide_rate", "respiratory_hospitalization_rate",
          "beds_per_capita"]
t1 = catalogo.set_index("id").loc[usados].reset_index()
t1 = pd.DataFrame({
    "id": "`" + t1["id"] + "`", "Nome": t1["name"],
    "Categoria": t1["category"].map(CATEGORIAS), "Fórmula": "`" + t1["formula"] + "`",
    "Multiplicador": milhar(t1["multiplier"]), "Incerteza": t1["uncertainty_method"],
})
print("\nTABELA 1\n" + tabela_md(t1))

s = caso_socio.sort_values("ind_aging_index", ascending=False)
t2 = pd.DataFrame({
    "Município": s["spatial_name"],
    "População (Censo 2010)": milhar(s["sample_weight_sum"]),
    "Envelhecimento": [decimal(v) for v in s["ind_aging_index"]],
    "Dependência": [decimal(v) for v in s["ind_dependency_ratio"]],
    "Esgoto (%)": [decimal(v) for v in s["ind_sewage_connection_rate"]],
    "IC 95% esgoto": [f"{decimal(a, 2)}–{decimal(b, 2)}" for a, b in
                      zip(s["ind_sewage_connection_rate_low"],
                          s["ind_sewage_connection_rate_high"], strict=True)],
    "Água (%)": [decimal(v) for v in s["ind_water_connection_rate"]],
})
print("\nTABELA 2\n" + tabela_md(t2))

# --- Figura 1: catálogo por categoria (real) ------------------------------------------------
cat = catalogo["category"].map(CATEGORIAS).value_counts().rename_axis("categoria").reset_index(name="n")
cat["categoria"] = pd.Categorical(cat["categoria"], categories=cat.sort_values("n")["categoria"])
p = (
    ggplot(cat, aes("categoria", "n"))
    + geom_col(fill=CF_PALETTE["primary"], width=0.65)
    + geom_text(aes(label="n"), ha="left", nudge_y=0.1, color=CF_PALETTE["text_main"],
                fontweight="bold")
    + scale_y_continuous(expand=(0, 0, 0.12, 0))
    + coord_flip()
    + labs(title="Indicadores disponíveis por categoria",
           subtitle="Catálogo de sus_socio_compute_indicators() (sus_socio_list_indicators)",
           x="", y="Número de indicadores")
    + tema_climasus()
)
save_fig(p, "socio_catalogo_categorias")

ROTULO_ILUSTRATIVO = "Valores ilustrativos (proporções sorteadas)"

# --- Figura 2: ranking do índice de envelhecimento (ilustrativo) -------------------------------
rk = caso_socio[["spatial_name", "ind_aging_index"]].copy()
rk["spatial_name"] = pd.Categorical(rk["spatial_name"],
                                    categories=rk.sort_values("ind_aging_index")["spatial_name"])
rk["rotulo"] = [decimal(v) for v in rk["ind_aging_index"]]
p = (
    ggplot(rk, aes("spatial_name", "ind_aging_index"))
    + geom_col(fill=CF_PALETTE["secondary"], width=0.65)
    + geom_text(aes(label="rotulo"), ha="left", nudge_y=0.8, color=CF_PALETTE["text_main"])
    + scale_y_continuous(expand=(0, 0, 0.12, 0))
    + coord_flip()
    + labs(title="Índice de envelhecimento por município (RMSP)",
           subtitle=f"Idosos por 100 jovens - {ROTULO_ILUSTRATIVO}",
           x="", y="Índice de envelhecimento")
    + tema_climasus(base_size=11)
)
save_fig(p, "socio_vulnerabilidade")

# --- Figura 3: cobertura de esgoto com IC (ilustrativo) -----------------------------------------
ic = caso_socio[["spatial_name", "ind_sewage_connection_rate", "ind_sewage_connection_rate_low",
                 "ind_sewage_connection_rate_high"]].copy()
ic["spatial_name"] = pd.Categorical(
    ic["spatial_name"], categories=ic.sort_values("ind_sewage_connection_rate")["spatial_name"])
p = (
    ggplot(ic, aes("spatial_name", "ind_sewage_connection_rate"))
    + geom_errorbar(aes(ymin="ind_sewage_connection_rate_low",
                        ymax="ind_sewage_connection_rate_high"),
                    width=0.3, color=CF_PALETTE["accent"], size=0.8)
    + geom_point(color=CF_PALETTE["primary"], size=3)
    + coord_flip()
    + labs(title="Cobertura de esgoto com intervalo de confiança de 95%",
           subtitle=f"Escore de Wilson (add_ci=True) - {ROTULO_ILUSTRATIVO}",
           x="", y="Domicílios ligados à rede de esgoto (%)")
    + tema_climasus(base_size=11)
)
save_fig(p, "socio_indicador_ic")

# --- Figura 4: envelhecimento x esgoto (ilustrativo) ----------------------------------------------
p = (
    ggplot(caso_socio, aes("ind_aging_index", "ind_sewage_connection_rate"))
    + geom_point(color=CF_PALETTE["primary"], size=3)
    + geom_text(aes(label="spatial_name"), va="bottom", nudge_y=0.6, size=8,
                color=CF_PALETTE["text_main"])
    + scale_x_continuous(expand=(0.12, 0))
    + labs(title="Envelhecimento e cobertura de esgoto nos municípios da RMSP",
           subtitle=ROTULO_ILUSTRATIVO,
           x="Índice de envelhecimento", y="Cobertura de esgoto (%)")
    + tema_climasus(base_size=11)
)
save_fig(p, "socio_dispersao_idosos")
