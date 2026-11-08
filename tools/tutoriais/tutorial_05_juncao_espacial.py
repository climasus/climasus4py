"""Tutorial 5 -- Junção Espacial: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_05_juncao_espacial.R``.
Runs the examples of ``docs/tutoriais/05-juncao-espacial.md`` and writes the
figures the page shows, plus the tables and numbers it quotes (printed).

Starts from the monthly series by age band of Tutorial 4, counted here by the
municipality of residence, so that the geometries are those of the eight RMSP
municipalities of the guiding case.

    python tools/tutoriais/tutorial_05_juncao_espacial.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from plotnine import (aes, geom_col, geom_line, geom_text, ggplot, labs,  # noqa: E402
                      scale_fill_manual, scale_x_datetime, scale_y_continuous)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_PALETTE, FIG_DIR, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def milhar(valores):
    return [f"{v:,.0f}".replace(",", ".") for v in valores]


# --- Entrada: a série do Tutorial 4, por município de residência ------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_limpo = cs.sus_data_clean_encoding(obitos)
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
resp_idosos = cs.sus_filter(obitos_padr, groups="respiratory", age_min=60, city=RMSP)
obitos_vars = cs.sus_data_create_variables(
    resp_idosos, age_breaks=[60, 70, 80, 999], age_labels=["60-69", "70-79", "80+"],
    climate_region="sudeste", lang="pt", verbose=False)
serie = cs.sus_data_aggregate(obitos_vars, time_unit="month", group_by=["faixa_etaria"],
                              join_col="residencia", lang="pt", verbose=False)
n_antes = serie.count("*").fetchone()[0]

# --- Junção espacial -----------------------------------------------------------------
caso_espacial = cs.sus_spatial_join(serie, geo_level="municipality", join_col="residencia",
                                    lang="pt")
esp = caso_espacial.df()
caso_estado = cs.sus_spatial_join(serie, geo_level="state", join_col="residencia", lang="pt")
est = caso_estado.df()
print(f"\nlinhas antes {n_antes:,} | depois {len(esp):,} | municípios {esp['code_muni_7'].nunique()}"
      f" | geometrias município {esp['geometry_wkt'].nunique()} | estado {est['geometry_wkt'].nunique()}"
      f" ({est['spatial_name'].iloc[0]})")
print("colunas novas:", [c for c in esp.columns if c not in serie.columns])
tipos = esp["geometry_wkt"].str.split(" ").str[0].value_counts().to_dict()
print("tipos de geometria:", tipos)

# --- sus_grid_join ilustrativo (grade inventada; o clima real entra no Tutorial 8) -------------
grade = esp[["code_muni_7", "date"]].drop_duplicates().reset_index(drop=True)
rng = np.random.default_rng(7)
grade["t2m"] = np.round(rng.normal(21, 4, len(grade)), 1)
caso_clima = cs.sus_grid_join(esp, grade, by=["code_muni_7", "date"], lang="pt", verbose=False)
print(f"grid join ilustrativo: {len(caso_clima):,} linhas | t2m vazia em "
      f"{int(caso_clima['t2m'].isna().sum())} | estágio {caso_clima.attrs['sus_meta']['stage']}")

# --- Tabelas -----------------------------------------------------------------------------
resumo = pd.DataFrame({
    "Item": ["Função", "Nível (`geo_level`)", "Coluna de junção (`join_col`)",
             "Sistema de referência", "Linhas antes da junção", "Linhas depois da junção",
             "Municípios distintos", "Tipo de geometria"],
    "Valor": ["`sus_spatial_join()`", "`municipality` (município)",
              "`residencia` (`codigo_municipio_residencia`)",
              "SIRGAS 2000 (EPSG:4674), o do IBGE", milhar([n_antes])[0], milhar([len(esp)])[0],
              str(esp["code_muni_7"].nunique()),
              ", ".join(sorted(tipos)) + " (texto WKT, coluna `geometry_wkt`)"],
})
print("\nTABELA resumo\n" + tabela_md(resumo))
amostra = (esp[["code_muni_7", "spatial_name", "abbrev_state", "date", "faixa_etaria", "n_obitos"]]
           .sort_values(["date", "spatial_name", "faixa_etaria"]).head(8).copy())
amostra["date"] = pd.to_datetime(amostra["date"]).dt.strftime("%Y-%m-%d")
print("\nTABELA amostra\n" + tabela_md(amostra))

# --- Figura 1: mapa coroplético dos óbitos por município ------------------------------------
import geopandas as gpd  # noqa: E402
from shapely import wkt  # noqa: E402

total = (esp.groupby(["code_muni_7", "spatial_name", "geometry_wkt"], as_index=False)["n_obitos"]
         .sum())
gdf = gpd.GeoDataFrame(total, geometry=[wkt.loads(g) for g in total["geometry_wkt"]], crs=4674)
print("\nóbitos por município:", dict(zip(total["spatial_name"], total["n_obitos"], strict=True)))
fig, ax = plt.subplots(figsize=(8, 5))
gdf.plot(column="n_obitos", cmap="YlOrRd", edgecolor="white", linewidth=0.6, ax=ax, legend=True,
         norm=matplotlib.colors.LogNorm(vmin=gdf["n_obitos"].min(), vmax=gdf["n_obitos"].max()),
         legend_kwds={"label": "Óbitos (escala log)", "shrink": 0.7})
from matplotlib import patheffects  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

barra = ax.get_figure().axes[-1]
barra.yaxis.set_major_locator(matplotlib.ticker.FixedLocator([2000, 5000, 10000, 20000, 50000]))
barra.yaxis.set_major_formatter(FuncFormatter(lambda v, _: milhar([v])[0]))
barra.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
for _, linha in gdf.iterrows():
    p = linha.geometry.representative_point()
    ax.annotate(linha["spatial_name"], (p.x, p.y), ha="center", fontsize=7, color="#1B1B1B",
                path_effects=[patheffects.withStroke(linewidth=2.5, foreground="white")])
ax.set_title("Óbitos respiratórios por município - após a junção espacial", loc="left",
             fontsize=13, fontweight="bold", color=CF_PALETTE["text_main"], pad=20)
ax.text(0, 1.02, "Idosos (60+), 8 municípios da RMSP, 2014-2019 (sus_spatial_join, nível município)",
        transform=ax.transAxes, fontsize=10, color=CF_PALETTE["secondary"])
ax.axis("off")
FIG_DIR.mkdir(parents=True, exist_ok=True)
fig.savefig(FIG_DIR / "geo_mapa_obitos.png", dpi=150, bbox_inches="tight", facecolor="white")
plt.close(fig)
print("   figura: geo_mapa_obitos.png")

# --- Figura 2: a série mensal continua a mesma -------------------------------------------------
mensal = (esp.assign(date=pd.to_datetime(esp["date"])).groupby("date", as_index=False)["n_obitos"]
          .sum())
p = (
    ggplot(mensal, aes("date", "n_obitos"))
    + geom_line(color=CF_PALETTE["primary"], size=0.8)
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + scale_y_continuous(labels=milhar)
    + labs(title="Série mensal preservada após a junção espacial",
           subtitle="A junção acrescenta geometria sem alterar a estrutura temporal (RMSP)",
           x="Mês", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "geo_serie")

# --- Figura 3: cobertura da junção --------------------------------------------------------------
entrada = set(esp["codigo_municipio_residencia"].astype(str).str[:6])
com_geo = set(esp.loc[esp["geometry_wkt"].notna(), "codigo_municipio_residencia"]
              .astype(str).str[:6])
cob = pd.DataFrame({"status": ["Com geometria", "Sem correspondência"],
                    "n": [len(entrada & com_geo), len(entrada - com_geo)]})
cob["status"] = pd.Categorical(cob["status"], categories=cob["status"])
print("cobertura:", dict(zip(cob["status"].astype(str), cob["n"], strict=True)))
p = (
    ggplot(cob, aes("status", "n", fill="status"))
    + geom_col(width=0.6, show_legend=False)
    + geom_text(aes(label="n"), va="bottom", color=CF_PALETTE["primary"], fontweight="bold")
    + scale_fill_manual(values=[CF_PALETTE["primary"], CF_PALETTE["accent"]])
    + labs(title="Cobertura da junção espacial (códigos municipais)",
           subtitle="Municípios da série que encontraram geometria",
           x="", y="Municípios distintos")
    + tema_climasus()
)
save_fig(p, "geo_cobertura")
