"""Tutorial 7 -- Clima por Estações (INMET): generator script.

Python counterpart of climasus4r's ``scripts/tutorial_07_clima_estacoes.R``.
Runs the examples of ``docs/tutoriais/07-clima-estacoes.md`` and writes the
figures the page shows, plus the tables and numbers it quotes (printed).

Real data throughout: hourly series of INMET station A701 (São Paulo --
Mirante de Santana), 2014-2019; INMET 1991-2020 normals of the same site
(conventional station 83781); the daily respiratory-death series of the
guiding case.

    python tools/tutoriais/tutorial_07_clima_estacoes.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from plotnine import (aes, geom_line, geom_point, ggplot, labs, scale_color_manual,  # noqa: E402
                      scale_linetype_manual, scale_x_datetime)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_PALETTE, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

warnings.filterwarnings("ignore", message=".*Santo André.*")

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]
MESES = ["janeiro", "fevereiro", "marco", "abril", "maio", "junho", "julho", "agosto",
         "setembro", "outubro", "novembro", "dezembro"]
ROTULO_MES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def milhar(v: float) -> str:
    return f"{v:,.0f}".replace(",", ".")


def decimal(v: float, casas: int = 2) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


# --- 1. Importar a série da estação A701 -------------------------------------------------
clima_horario = cs.sus_climate_inmet(years=ANOS, uf="SP", station_code="A701", lang="pt",
                                     verbose=False)
clima_df = clima_horario.df()
temp = clima_df.set_index("date")["tair_dry_bulb_c"].sort_index()
falta = temp.isna().to_numpy()
lacunas, inicio = [], None
for i, f in enumerate(falta):
    if f and inicio is None:
        inicio = i
    if not f and inicio is not None:
        lacunas.append((temp.index[inicio], i - inicio))
        inicio = None
if inicio is not None:
    lacunas.append((temp.index[inicio], len(falta) - inicio))
maior = max(lacunas, key=lambda x: x[1])
print(f"\nhoras {len(clima_df):,} | sem temperatura {int(falta.sum())} | lacunas {len(lacunas)} | "
      f"maior {maior[1]} h a partir de {maior[0]}")

# --- 2. Preencher lacunas ----------------------------------------------------------------
aval = cs.sus_climate_fill_inmet(clima_df, target_var="tair_dry_bulb_c", quality_threshold=0.4,
                                 run_evaluation=True, gap_percentage=0.2, lang="pt", verbose=False)
metricas = aval["tair_dry_bulb_c"]["metrics"]
print("métricas:\n", metricas)
clima_preenchido = cs.sus_climate_fill_inmet(clima_df, target_var="tair_dry_bulb_c",
                                             quality_threshold=0.4, run_evaluation=False,
                                             keep_features=False, lang="pt", verbose=False)
print("preenchidas:", int(clima_preenchido["is_imputed_tair_dry_bulb_c"].sum()))

# --- 4. Normais ----------------------------------------------------------------------------
normais = cs.sus_climate_normals(period="1991-2020", target_var=["t_max", "t_min", "t_mean_comp"],
                                 lang="pt", verbose=False)
mirante = normais[normais["codigo"].astype(str) == "83781"]
print("normais:", normais.shape, "| Mirante:", mirante["nome_estacao"].iloc[0], len(mirante))

# --- 5. Alinhar clima e saúde ----------------------------------------------------------------
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_limpo = cs.sus_data_clean_encoding(obitos)
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
resp_idosos = cs.sus_filter(obitos_padr, groups="respiratory", age_min=60, city=RMSP)
obitos_vars = cs.sus_data_create_variables(
    resp_idosos, age_breaks=[60, 70, 80, 999], age_labels=["60-69", "70-79", "80+"],
    climate_region="sudeste", lang="pt", verbose=False)
serie_diaria = cs.sus_data_aggregate(obitos_vars, time_unit="day", join_col="residencia",
                                     lang="pt", verbose=False)
saude_espacial = cs.sus_spatial_join(serie_diaria, geo_level="municipality",
                                     join_col="residencia", lang="pt")
saude_exact = cs.sus_climate_aggregate(saude_espacial, clima_preenchido,
                                       temporal_strategy="exact",
                                       climate_vars=["tair_dry_bulb_c"], verbose=False)
saude_movel = cs.sus_climate_aggregate(saude_espacial, clima_preenchido,
                                       temporal_strategy="moving_window", window_days=7,
                                       climate_vars=["tair_dry_bulb_c"], verbose=False)
ex = saude_exact.df()
mv = saude_movel.df()
print(f"alinhamento: linhas {len(ex):,} | óbitos {int(ex['n_obitos'].sum()):,} | "
      f"colunas exact {[c for c in ex.columns if c.startswith('tair')]} | "
      f"móvel {[c for c in mv.columns if c.startswith('tair')]} | "
      f"vazias móvel {int(mv['tair_dry_bulb_c_mean_w7'].isna().sum())}")
print(ex.sort_values(["date", "spatial_name"])[["date", "spatial_name", "n_obitos",
                                                 "tair_dry_bulb_c"]].head(4).to_string())

# --- Tabelas -------------------------------------------------------------------------------
t1 = pd.DataFrame({
    "Item": ["Estação", "Período", "Horas na série", "Horas sem temperatura",
             "Lacunas (trechos contínuos)", "Maior lacuna"],
    "Valor": ["A701 — São Paulo (Mirante de Santana)",
              f"{temp.index.min():%d/%m/%Y} a {temp.index.max():%d/%m/%Y}",
              milhar(len(clima_df)),
              f"{int(falta.sum())} ({decimal(100 * falta.mean())}%)",
              str(len(lacunas)),
              f"{maior[1]} h (cerca de {decimal(maior[1] / 24, 1)} dias), a partir de "
              f"{maior[0]:%d/%m/%Y}"],
})
print("\nTABELA 1\n" + tabela_md(t1))
m0 = metricas.iloc[0]
t2 = pd.DataFrame({
    "Estação": [m0["station"]], "MAE (°C)": [decimal(m0["mae"])],
    "RMSE (°C)": [decimal(m0["rmse"])], "R²": [decimal(m0["r2"], 3)],
    "Horas avaliadas": [milhar(m0["n"])],
})
print("\nTABELA 2\n" + tabela_md(t2))

# --- Figura 1: temperatura média diária observada ---------------------------------------------
diaria = temp.resample("D").mean().rename("t").reset_index()
p = (
    ggplot(diaria, aes("date", "t"))
    + geom_line(color=CF_PALETTE["primary"], size=0.4)
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + labs(title="Temperatura média diária - estação A701",
           subtitle="São Paulo (Mirante de Santana), INMET, 2014-2019 - média das horas observadas",
           x="", y="Temperatura (°C)")
    + tema_climasus()
)
save_fig(p, "inmet_serie")

# --- Figura 2: a maior lacuna, aproximada (função do pacote) -----------------------------------
janela = (clima_preenchido["date"] >= "2014-07-14") & (clima_preenchido["date"] < "2014-08-04")
p = cs.sus_climate_plot_fill(clima_preenchido[janela], clima_df[janela.to_numpy()],
                             target_var="tair_dry_bulb_c", lang="pt")
p = p + labs(title="Preenchimento da maior lacuna: 20 a 28 de julho de 2014 (A701)",
             x="", y="Temperatura (°C)")
p.save(str(Path(__file__).resolve().parents[2] / "docs" / "tutoriais" / "figuras" /
           "inmet_fill.png"), width=10, height=4.5, dpi=150, verbose=False)
print("   figura: inmet_fill.png")

# --- Figura 3: normais do Mirante x 2014-2019 ---------------------------------------------------
nm = mirante[mirante["mes"].isin(MESES)].copy()
nm["mes_n"] = nm["mes"].map({m: i + 1 for i, m in enumerate(MESES)})
nome_var = {"t_max": "Máxima", "t_mean_comp": "Média", "t_min": "Mínima"}
nm["variavel"] = pd.Categorical(nm["var_code"].map(nome_var), categories=["Máxima", "Média", "Mínima"])
nm["fonte"] = "Normal 1991-2020"
obs = clima_df.assign(mes_n=clima_df["date"].dt.month)
dia = obs.set_index("date")[["tair_max_c", "tair_dry_bulb_c", "tair_min_c"]].resample("D").agg(
    {"tair_max_c": "max", "tair_dry_bulb_c": "mean", "tair_min_c": "min"})
dia["mes_n"] = dia.index.month
mensal = dia.groupby("mes_n").mean().reset_index().melt(id_vars="mes_n", var_name="col",
                                                         value_name="valor")
mensal["variavel"] = pd.Categorical(mensal["col"].map(
    {"tair_max_c": "Máxima", "tair_dry_bulb_c": "Média", "tair_min_c": "Mínima"}),
    categories=["Máxima", "Média", "Mínima"])
mensal["fonte"] = "Observado 2014-2019 (A701)"
junto = pd.concat([nm[["mes_n", "valor", "variavel", "fonte"]],
                   mensal[["mes_n", "valor", "variavel", "fonte"]]])
from plotnine import scale_x_continuous  # noqa: E402

p = (
    ggplot(junto, aes("mes_n", "valor", color="variavel", linetype="fonte"))
    + geom_line(size=0.9)
    + geom_point(size=1.6)
    + scale_x_continuous(breaks=list(range(1, 13)), labels=ROTULO_MES)
    + scale_color_manual(values=[CF_PALETTE["accent"], CF_PALETTE["primary"], "#1565C0"])
    + scale_linetype_manual(values=["solid", "dashed"])
    + labs(title="Normais climatológicas - Mirante de Santana (SP)",
           subtitle="Normais 1991-2020 (estação 83781) e observado 2014-2019 (estação A701)",
           x="", y="Temperatura (°C)", color="", linetype="")
    + tema_climasus()
)
save_fig(p, "inmet_normais")

# --- Figura 4: mesmo dia x janela móvel de 7 dias, inverno de 2014 ---------------------------
sp_ex = ex[ex["spatial_name"] == "São Paulo"][["date", "tair_dry_bulb_c"]]
sp_mv = mv[mv["spatial_name"] == "São Paulo"][["date", "tair_dry_bulb_c_mean_w7"]]
lag = sp_ex.merge(sp_mv, on="date")
lag["date"] = pd.to_datetime(lag["date"])
lag = lag[(lag["date"] >= "2014-06-01") & (lag["date"] <= "2014-08-31")]
lag = lag.melt(id_vars="date", var_name="estrategia", value_name="t")
lag["estrategia"] = lag["estrategia"].map({
    "tair_dry_bulb_c": "Mesmo dia (exact)",
    "tair_dry_bulb_c_mean_w7": "Média móvel de 7 dias (moving_window)"})
p = (
    ggplot(lag, aes("date", "t", color="estrategia"))
    + geom_line(size=0.9)
    + scale_color_manual(values=[CF_PALETTE["accent"], CF_PALETTE["primary"]])
    + scale_x_datetime(date_breaks="2 weeks", date_labels="%d/%m")
    + labs(title="Temperatura por estratégia temporal - inverno de 2014",
           subtitle="Município de São Paulo, estação A701 (sus_climate_aggregate)",
           x="", y="Temperatura (°C)", color="")
    + tema_climasus()
)
save_fig(p, "inmet_lag")
print("dias de inverno com valor nas duas:", int(lag.dropna()["date"].nunique()))
_ = np  # keep numpy import for readers extending the script
