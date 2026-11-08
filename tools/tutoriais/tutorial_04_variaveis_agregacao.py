"""Tutorial 4 -- Variáveis e Agregação: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_04_variaveis_agregacao.R``.
Runs the examples of ``docs/tutoriais/04-variaveis-agregacao.md`` and writes
the figures the page shows, plus the tables and numbers it quotes (printed).

Starts from the respiratory deaths of older adults filtered in Tutorial 3.

    python tools/tutoriais/tutorial_04_variaveis_agregacao.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import pandas as pd  # noqa: E402
from plotnine import aes, geom_line, ggplot, labs, scale_color_manual, scale_x_datetime  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_COLORS, FIG_DIR, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def milhar(v) -> str:
    return f"{v:,.0f}".replace(",", ".")


def como_df(serie) -> pd.DataFrame:
    return serie.df() if hasattr(serie, "df") else serie


# --- Entrada: os passos dos tutoriais 1 a 3 -----------------------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_limpo = cs.sus_data_clean_encoding(obitos)
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
resp_idosos = cs.sus_filter(obitos_padr, groups="respiratory", age_min=60, city=RMSP)

# --- Variáveis derivadas ----------------------------------------------------------
obitos_vars = cs.sus_data_create_variables(
    resp_idosos, create_age_groups=True, age_breaks=[60, 70, 80, 999],
    age_labels=["60-69", "70-79", "80+"], create_calendar_vars=True,
    create_climate_vars=True, climate_region="sudeste", hemisphere="south",
    lang="pt", verbose=False)

amostra = obitos_vars.select(
    "data_obito, idade_anos, faixa_etaria, grupo_risco_climatico, faixa_etaria_ibge, "
    "ano, mes, trimestre, estacao_climatica, estacao_seca_chuvosa").limit(5).df()
amostra["data_obito"] = pd.to_datetime(amostra["data_obito"]).dt.strftime("%Y-%m-%d")
print("\nTABELA amostra\n" + tabela_md(amostra))
print("\nfaixas:", dict(obitos_vars.aggregate("faixa_etaria, COUNT(*)", "faixa_etaria")
                    .order("1").fetchall()))

# --- Agregações -------------------------------------------------------------------
serie_diaria = como_df(cs.sus_data_aggregate(
    obitos_vars, time_unit="day", fun="count", join_col="ocorrencia",
    complete_dates=True, lang="pt", verbose=False))
serie_mensal = como_df(cs.sus_data_aggregate(
    obitos_vars, time_unit="month", fun="count", group_by=["faixa_etaria"],
    join_col="ocorrencia", complete_dates=True, lang="pt", verbose=False))
serie_muni = cs.sus_data_aggregate(
    obitos_vars, time_unit="month", fun="count", join_col="residencia",
    lang="pt", verbose=False)


def resumo_de(d: pd.DataFrame, rotulo: str, unidade: str) -> dict:
    datas = pd.to_datetime(d["date"])
    return {"Série": rotulo, "Unidade": unidade, "Períodos": milhar(datas.nunique()),
            "Linhas": milhar(len(d)), "Total de óbitos": milhar(d["n_obitos"].sum()),
            "Início": datas.min().strftime("%Y-%m-%d"), "Fim": datas.max().strftime("%Y-%m-%d")}


resumo = pd.DataFrame([resumo_de(serie_diaria, "Diária", "dia"),
                       resumo_de(serie_mensal, "Mensal, por faixa etária", "mês")])
print("\nTABELA resumo\n" + tabela_md(resumo))
print("municípios de ocorrência na diária:", serie_diaria["codigo_municipio_ocorrencia"].nunique(),
      "| de residência no mapa:", como_df(serie_muni)["codigo_municipio_residencia"].nunique())

# --- Figura 1: curva epidêmica diária ------------------------------------------------
FIG_DIR.mkdir(parents=True, exist_ok=True)
cs.sus_data_plot_aggregate_ts(serie_diaria, plot_type="epidemic", smooth_method="loess",
                              palette="lancet", lang="pt", verbose=False,
                              save_path=str(FIG_DIR / "agg_serie_diaria.png"),
                              width=8, height=5)
print("   figura: agg_serie_diaria.png")

# --- Figura 2: sazonalidade ------------------------------------------------------------
cs.sus_data_plot_aggregate_ts(serie_diaria, plot_type="seasonal", palette="lancet",
                              lang="pt", verbose=False,
                              save_path=str(FIG_DIR / "agg_sazonalidade.png"),
                              width=8, height=5)
print("   figura: agg_sazonalidade.png")

# --- Figura 3: série mensal por faixa etária (desenhada aqui, como no R) ------------------
faixa = (serie_mensal.assign(date=pd.to_datetime(serie_mensal["date"]))
         .groupby(["date", "faixa_etaria"], as_index=False)["n_obitos"].sum())
p = (
    ggplot(faixa, aes("date", "n_obitos", color="faixa_etaria", group="faixa_etaria"))
    + geom_line(size=0.7)
    + scale_color_manual(values=CF_COLORS, name="Faixa etária")
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + labs(title="Óbitos respiratórios mensais por faixa etária",
           subtitle="Idosos (60+), 8 municípios da RMSP, 2014-2019",
           x="Mês", y="Número de óbitos")
    + tema_climasus()
)
save_fig(p, "agg_serie_faixa")

# --- Figura 4: mapa por município de residência ----------------------------------------------
cs.sus_data_plot_aggregate_map(serie_muni, map_type="bubble", log_scale=True, palette="YlOrRd",
                               lang="pt", verbose=False,
                               save_path=str(FIG_DIR / "agg_mapa_municipios.png"),
                               width=8, height=6)
print("   figura: agg_mapa_municipios.png")
