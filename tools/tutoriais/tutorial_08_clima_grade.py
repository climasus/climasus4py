"""Tutorial 8 -- Clima em Grade e Dados Ambientais: generator script.

Python counterpart of climasus4r's ``scripts/tutorial_08_clima_grade.R``.
Runs the examples of ``docs/tutoriais/08-clima-grade.md`` and writes the
figures the page shows, plus the tables and numbers it quotes (printed).

Real data throughout. ERA5-Land is distributed as one ~95 MB file per
variable and month (all of Latin America), so the grid part uses one month
(January 2014, mean temperature and precipitation, ~190 MB) and compares it
with station A701. The climate indices need years of data -- SPI/SPEI at
least 24 months, heatwaves a multi-year baseline -- and are computed on the
2014-2019 A701 series of Tutorial 7.

    python tools/tutoriais/tutorial_08_clima_grade.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from plotnine import (aes, geom_col, geom_hline, geom_line, geom_point, ggplot, labs,  # noqa: E402
                      scale_color_manual, scale_fill_manual, scale_x_datetime)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_PALETTE, FIG_DIR, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

warnings.filterwarnings("ignore", message=".*(baseline curto|sus_climate_fill_gap|materializ).*")

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
MESES = ["janeiro", "fevereiro", "marco", "abril", "maio", "junho", "julho", "agosto",
         "setembro", "outubro", "novembro", "dezembro"]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def dec(v: float, casas: int = 1) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


# --- Série de saúde dos Tutoriais 1 a 5 (residência, diária) -----------------------------
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]
obitos = cs.sus_data_import(system="SIM-DO", uf="SP", year=ANOS, verbose=False)
obitos_limpo = cs.sus_data_clean_encoding(obitos)
obitos_padr = cs.sus_data_standardize(obitos_limpo, lang="pt")
resp_idosos = cs.sus_filter(obitos_padr, groups="respiratory", age_min=60, city=RMSP)
obitos_vars = cs.sus_data_create_variables(
    resp_idosos, age_breaks=[60, 70, 80, 999], age_labels=["60-69", "70-79", "80+"],
    climate_region="sudeste", lang="pt", verbose=False)
serie_diaria = cs.sus_data_aggregate(obitos_vars, time_unit="day", join_col="residencia",
                                     lang="pt", verbose=False)
caso_espacial = cs.sus_spatial_join(serie_diaria, geo_level="municipality",
                                    join_col="residencia", lang="pt")

# --- 1. ERA5-Land para o município de São Paulo (janeiro de 2014) ----------------------
tabela = caso_espacial.df()
sp = tabela[tabela["spatial_name"] == "São Paulo"].drop_duplicates("code_muni_7")
sp_geom = gpd.GeoDataFrame({"code_muni": sp["code_muni_7"]},
                           geometry=gpd.GeoSeries.from_wkt(sp["geometry_wkt"]), crs="EPSG:4674")
era5_sp = cs.sus_grid_era5(years=2014, months=[1], vars=["t2m", "tp"], municipalities=sp_geom,
                           agg_fun="mean", lang="pt", verbose=False)
print("\nERA5:", era5_sp.shape, list(era5_sp.columns), era5_sp.attrs["sus_meta"]["stage"],
      era5_sp.attrs["sus_meta"]["type"])

# --- Estação A701 (Tutorial 7) --------------------------------------------------------------
clima_horario = cs.sus_climate_inmet(years=ANOS, uf="SP", station_code="A701", lang="pt",
                                     verbose=False)
clima_df = clima_horario.df()
clima_preenchido = cs.sus_climate_fill_inmet(clima_df, target_var="tair_dry_bulb_c",
                                             lang="pt", verbose=False)

# Comparação diária ERA5 x A701 em janeiro de 2014 (dias em UTC nos dois)
a701_dia = (clima_preenchido.set_index("date")
            .resample("D").agg({"tair_dry_bulb_c": "mean", "rainfall_mm": "sum"}).reset_index())
a701_jan = a701_dia[(a701_dia["date"] >= "2014-01-01") & (a701_dia["date"] < "2014-02-01")]
comp = era5_sp[["date", "tair_dry_bulb_c", "rainfall_mm"]].merge(
    a701_jan, on="date", suffixes=("_era5", "_a701"))
vies = (comp["tair_dry_bulb_c_era5"] - comp["tair_dry_bulb_c_a701"]).mean()
corr = comp["tair_dry_bulb_c_era5"].corr(comp["tair_dry_bulb_c_a701"])
corr_chuva = comp["rainfall_mm_era5"].corr(comp["rainfall_mm_a701"])
# The ERA5-Land daily precipitation is dated the day AFTER it fell (the file for
# January runs 02/01..01/02): the total of day D is stamped at 00 UTC of D+1.
chuva_era5 = era5_sp[["date", "rainfall_mm"]].dropna().copy()
chuva_era5["date"] = pd.to_datetime(chuva_era5["date"]) - pd.DateOffset(days=1)
chuva = chuva_era5.merge(a701_jan[["date", "rainfall_mm"]], on="date", suffixes=("_era5", "_a701"))
corr_chuva_ajustada = chuva["rainfall_mm_era5"].corr(chuva["rainfall_mm_a701"])
print(f"chuva: datas ERA5 {era5_sp.dropna(subset=['rainfall_mm'])['date'].min():%d/%m}-"
      f"{era5_sp.dropna(subset=['rainfall_mm'])['date'].max():%d/%m} | r na data do arquivo "
      f"{corr_chuva:.2f} | r com a data corrigida {corr_chuva_ajustada:.2f} | "
      f"total corrigido ERA5 {chuva['rainfall_mm_era5'].sum():.1f} x A701 {chuva['rainfall_mm_a701'].sum():.1f}")
print(f"comparação: {len(comp)} dias | viés {vies:.2f} C | r temp {corr:.2f} | r chuva {corr_chuva:.2f}")

# --- 2. Anomalia mensal (à mão, como o script do R) ------------------------------------------
normais = cs.sus_climate_normals(period="1991-2020", target_var=["t_mean_comp"], lang="pt",
                                 verbose=False)
normal_mirante = normais[(normais["codigo"] == "83781") & (normais["mes"].isin(MESES))].copy()
normal_mirante["mes"] = normal_mirante["mes"].map({m: i + 1 for i, m in enumerate(MESES)})
mensal_obs = a701_dia.assign(ano=a701_dia["date"].dt.year, mes=a701_dia["date"].dt.month)
mensal_obs = mensal_obs.groupby(["ano", "mes"], as_index=False)["tair_dry_bulb_c"].mean()
anomalia = mensal_obs.merge(normal_mirante[["mes", "valor"]], on="mes")
anomalia["anomalia_c"] = anomalia["tair_dry_bulb_c"] - anomalia["valor"]
anomalia["date"] = pd.to_datetime(dict(year=anomalia["ano"], month=anomalia["mes"], day=1))
print(f"anomalia: {len(anomalia)} meses | positivos {int((anomalia['anomalia_c'] > 0).sum())} | "
      f"média {anomalia['anomalia_c'].mean():.2f} C | maior {anomalia['anomalia_c'].max():.2f} "
      f"({anomalia.loc[anomalia['anomalia_c'].idxmax(), 'date']:%m/%Y})")

# --- 3. Indicadores bioclimáticos (demonstração, janeiro de 2014) ---------------------------
jan_horario = clima_df[(clima_df["date"] >= "2014-01-01") & (clima_df["date"] < "2014-02-01")]
indicadores = cs.sus_climate_compute_indicators(jan_horario, indicators=["wbgt", "hi", "utci"],
                                                region="atlantic_forest", lang="pt",
                                                verbose=False).df()
print("indicadores (jan/2014):", {c: round(float(indicadores[c].mean()), 1)
                                   for c in ("wbgt_c", "hi_c", "utci_c")},
      "| HI vazio em", int(indicadores["hi_c"].isna().sum()), "de", len(indicadores), "h")

# --- 4. Ondas de calor --------------------------------------------------------------------------
hw = cs.sus_climate_compute_heatwaves(clima_df, method=["WHO", "WMO", "INMET"],
                                      baseline_start="2014-01-01", baseline_end="2019-12-31",
                                      percentile=90, lang="pt", verbose=False)
ev = hw["events"]
print("ondas de calor:", ev.groupby("method").size().to_dict(), "| daily", hw["daily"].shape)
p = cs.sus_climate_plot_heatwaves(hw, type="trend", interactive=False, lang="pt")
p.save(str(FIG_DIR / "grid_ondas_calor.png"), width=9, height=5, dpi=150, verbose=False)
print("   figura: grid_ondas_calor.png")

# --- 5. SPI e SPEI ------------------------------------------------------------------------------
mensal = clima_preenchido.assign(date=clima_preenchido["date"].dt.to_period("M").dt.to_timestamp())
mensal = mensal.groupby(["station_code", "date"], as_index=False).agg(
    rainfall_mm=("rainfall_mm", "sum"), tair_dry_bulb_c=("tair_dry_bulb_c", "mean"))
mensal = mensal.rename(columns={"station_code": "code_muni"})
spi = cs.sus_climate_compute_spi(mensal, var="rainfall_mm", scales=[1, 3, 6], min_n=24,
                                 lang="pt", verbose=False)
spei = cs.sus_climate_compute_spei(spi, rain_var="rainfall_mm", pet_method="thornthwaite",
                                   temp_var="tair_dry_bulb_c", scales=[1, 3, 6], min_n=24,
                                   lang="pt", verbose=False)
seco = spei[spei["spi_3mo"] <= -1]
print(f"SPI/SPEI: {len(spei)} meses | colunas {[c for c in spei.columns if c.startswith('sp')]} | "
      f"SPI-3 <= -1 em {len(seco)} meses: {[f'{d:%m/%Y}' for d in seco['date']]}")

# --- 6. sus_grid_join: ERA5 + série de saúde (São Paulo, janeiro de 2014) --------------------
saude_sp = tabela[tabela["spatial_name"] == "São Paulo"][["date", "codigo_municipio_residencia",
                                                          "n_obitos"]]
saude_sp = saude_sp.rename(columns={"codigo_municipio_residencia": "code_muni"})
saude_sp["date"] = pd.to_datetime(saude_sp["date"])
saude_jan = saude_sp[saude_sp["date"] < "2014-02-01"]
saude_grade = cs.sus_grid_join(saude_jan, era5_sp, by=["code_muni", "date"], lang="pt",
                               verbose=False)
print("grid_join:", saude_grade.shape, list(saude_grade.columns),
      "| t2m vazia", int(saude_grade["tair_dry_bulb_c"].isna().sum()),
      "| óbitos", int(saude_grade["n_obitos"].sum()))

# --- Tabelas --------------------------------------------------------------------------------------
t1 = pd.DataFrame({
    "Item": ["Período", "Dias", "Temperatura média — ERA5-Land (°C)",
             "Temperatura média — A701 (°C)", "Diferença média ERA5 − A701 (°C)",
             "Correlação diária da temperatura", "Chuva no mês — ERA5-Land (mm)",
             "Chuva no mês — A701 (mm)", "Correlação diária da chuva (data do arquivo)",
             "Correlação diária da chuva (data corrigida, −1 dia)"],
    "Valor": ["01/01/2014 a 31/01/2014", str(len(comp)),
              dec(comp["tair_dry_bulb_c_era5"].mean()), dec(comp["tair_dry_bulb_c_a701"].mean()),
              dec(vies), dec(corr, 2), dec(chuva["rainfall_mm_era5"].sum()),
              dec(chuva["rainfall_mm_a701"].sum()), dec(corr_chuva, 2),
              dec(corr_chuva_ajustada, 2)],
})
print("\nTABELA 1\n" + tabela_md(t1))
amostra = ev.sort_values("start_date").head(8)
t2 = pd.DataFrame({
    "Método": amostra["method"], "Início": pd.to_datetime(amostra["start_date"]).dt.strftime("%d/%m/%Y"),
    "Fim": pd.to_datetime(amostra["end_date"]).dt.strftime("%d/%m/%Y"),
    "Dias": amostra["duration_days"].astype(int),
    "Pico (°C)": [dec(v) for v in amostra["temp_peak"]],
    "Anomalia média (°C)": [dec(v) for v in amostra["anomaly_mean"]],
})
print("\nTABELA 2\n" + tabela_md(t2))

# --- Figura 1: ERA5-Land x A701, janeiro de 2014 ----------------------------------------------
f1 = comp.melt(id_vars="date", value_vars=["tair_dry_bulb_c_era5", "tair_dry_bulb_c_a701"],
               var_name="fonte", value_name="t")
f1["fonte"] = f1["fonte"].map({"tair_dry_bulb_c_era5": "ERA5-Land (média do município)",
                               "tair_dry_bulb_c_a701": "Estação A701 (Mirante de Santana)"})
p = (
    ggplot(f1, aes("date", "t", color="fonte"))
    + geom_line(size=0.9)
    + geom_point(size=1.4)
    + scale_color_manual(values=[CF_PALETTE["primary"], CF_PALETTE["accent"]])
    + scale_x_datetime(date_breaks="1 week", date_labels="%d/%m")
    + labs(title="Grade x estação - temperatura média diária, janeiro de 2014",
           subtitle="Município de São Paulo: ERA5-Land (~10 km) e estação A701",
           x="", y="Temperatura (°C)", color="")
    + tema_climasus()
)
save_fig(p, "grid_serie")

# --- Figura 2: anomalia mensal ------------------------------------------------------------------
anomalia["sinal"] = np.where(anomalia["anomalia_c"] > 0, "Mais quente que a normal",
                             "Mais frio que a normal")
p = (
    ggplot(anomalia, aes("date", "anomalia_c", fill="sinal"))
    + geom_col(width=25)
    + geom_hline(yintercept=0, color="#555555")
    + scale_fill_manual(values={"Mais quente que a normal": CF_PALETTE["accent"],
                                "Mais frio que a normal": "#1565C0"})
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + labs(title="Anomalia mensal de temperatura - Mirante de Santana",
           subtitle="Média mensal da A701 (2014-2019) menos a normal 1991-2020 (estação 83781)",
           x="", y="Anomalia (°C)", fill="")
    + tema_climasus()
)
save_fig(p, "grid_anomalia")

# --- Figura 4: SPI-3 x SPEI-3 -------------------------------------------------------------------
f4 = spei.melt(id_vars="date", value_vars=["spi_3mo", "spei_3mo"], var_name="indice",
               value_name="valor").dropna()
f4["indice"] = f4["indice"].map({"spi_3mo": "SPI-3", "spei_3mo": "SPEI-3"})
p = (
    ggplot(f4, aes("date", "valor", color="indice"))
    + geom_hline(yintercept=[-1, 1], linetype="dashed", color="#888888")
    + geom_hline(yintercept=0, color="#555555")
    + geom_line(size=0.9)
    + scale_color_manual(values={"SPI-3": "#1565C0", "SPEI-3": CF_PALETTE["accent"]})
    + scale_x_datetime(date_breaks="1 year", date_labels="%Y")
    + labs(title="SPI-3 e SPEI-3 mensais - estação A701",
           subtitle="Linhas tracejadas: ±1 (seca ou umidade moderada); 2014-2019, ilustrativo",
           x="", y="Índice padronizado", color="")
    + tema_climasus()
)
save_fig(p, "grid_spi_spei")
