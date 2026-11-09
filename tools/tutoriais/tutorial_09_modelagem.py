"""Tutorial 9 -- Modelagem Epidemiológica (DLNM): generator script.

Python counterpart of climasus4r's ``scripts/tutorial_09_modelagem.R``.
Runs the examples of ``docs/tutoriais/09-modelagem.md`` and writes the
figures the page shows, plus the tables and numbers it quotes (printed).

Real data throughout: daily respiratory deaths of the elderly (60+) in the
eight RMSP municipalities by residence, 2014-2019, and the daily mean
temperature of station A701 (Tutorial 7), joined with
``sus_climate_aggregate(temporal_strategy="distributed_lag")``.

    python tools/tutoriais/tutorial_09_modelagem.py
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd
from plotnine import (aes, geom_col, geom_errorbar, geom_text, ggplot, labs,  # noqa: E402
                      scale_fill_manual, scale_y_continuous)

sys.path.insert(0, str(Path(__file__).parent))
from _setup_tutoriais import CF_PALETTE, FIG_DIR, save_fig, tema_climasus  # noqa: E402

import climasus4py as cs  # noqa: E402

warnings.filterwarnings("ignore", message=".*Santo André.*")

ANOS = [2014, 2015, 2016, 2017, 2018, 2019]
RMSP = ["São Paulo", "Guarulhos", "São Bernardo do Campo", "Santo André",
        "Osasco", "Mauá", "Carapicuíba", "Diadema"]


def tabela_md(df: pd.DataFrame) -> str:
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    linhas += ["| " + " | ".join(str(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join(linhas)


def dec(v: float, casas: int = 2) -> str:
    return f"{v:.{casas}f}".replace(".", ",")


def milhar(v: float) -> str:
    return f"{v:,.0f}".replace(",", ".")


# --- Entrada: saúde (Tutoriais 1 a 5) e clima (Tutorial 7) -----------------------------
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
clima_horario = cs.sus_climate_inmet(years=ANOS, uf="SP", station_code="A701", lang="pt",
                                     verbose=False)
clima_preenchido = cs.sus_climate_fill_inmet(clima_horario.df(), target_var="tair_dry_bulb_c",
                                             lang="pt", verbose=False)

# --- 1. Lags distribuídos ---------------------------------------------------------------------
df_dl = cs.sus_climate_aggregate(saude_espacial, clima_preenchido,
                                 temporal_strategy="distributed_lag",
                                 lag_days=list(range(22)), climate_vars=["tair_dry_bulb_c"],
                                 verbose=False)
entrada = df_dl.df()
print(f"\nentrada: {entrada.shape} | lags: {sum('_lag' in c for c in entrada.columns)}")

# --- 2. DLNM ------------------------------------------------------------------------------------
fit = cs.sus_mod_dlnm(entrada, outcome_col="n_obitos", climate_col="tair_dry_bulb_c", lag_max=21,
                      argvar={"fun": "ns", "df": 4}, arglag={"fun": "ns", "df": 3},
                      family="quasipoisson", dof_per_year=6, lang="pt", verbose=False)
er = fit["exposure_response"]
diag = fit["diagnostics"]
cen = float(er.loc[er["pct"] == 0.5, "exposure"].iloc[0])
print("dias", len(fit["data_daily"]), "| óbitos", int(fit["data_daily"]["y"].sum()),
      "| ref", round(cen, 2), "| diag", {k: diag[k] for k in ("disp_ratio", "autocorr_pval")})
print(er.to_string())
curva = pd.DataFrame({"x": fit["pred"]["predvar"], "rr": fit["pred"]["allRRfit"]})
print("RR no extremo frio:", curva.iloc[0].round(3).to_dict(), "| quente:", curva.iloc[-1].round(3).to_dict())
lr = fit["lag_response"]
print("lag-resposta (P75):", lr[["lag", "rr", "rr_cum"]].iloc[[0, 3, 7, 14, 21]].round(4).to_dict("records"))

# --- 3. Fração atribuível -------------------------------------------------------------------------
af = cs.sus_mod_af(fit, by="month", nsim=200, lang="pt", verbose=False, seed=2026)
tot = af["total"]
print(tot.to_string())

# --- Tabelas ------------------------------------------------------------------------------------
p75 = er[er["pct"] == 0.75].iloc[0]
p99 = er[er["pct"] == 0.99].iloc[0]
p25 = er[er["pct"] == 0.25].iloc[0]
t1 = pd.DataFrame({
    "Item": ["Período / dias analisados", "Óbitos analisados", "Exposição",
             "Base da exposição × lag", "Lag máximo", "Família / sazonalidade",
             "Referência (RR = 1)", "RR no P25 (frio)", "RR no P75", "RR no P99 (calor)",
             "Razão de dispersão", "Autocorrelação residual (Ljung-Box)"],
    "Valor": [f"2014–2019 / {milhar(len(fit['data_daily']))}", milhar(fit["data_daily"]["y"].sum()),
              "Temperatura média diária, estação A701", "ns (4 gl) × ns (3 gl)", "21 dias",
              "quasi-Poisson / 6 gl por ano", f"{dec(cen, 1)} °C (mediana)",
              f"{dec(p25['rr'])} [{dec(p25['lo'])}–{dec(p25['hi'])}] a {dec(p25['exposure'], 1)} °C",
              f"{dec(p75['rr'])} [{dec(p75['lo'])}–{dec(p75['hi'])}] a {dec(p75['exposure'], 1)} °C",
              f"{dec(p99['rr'])} [{dec(p99['lo'])}–{dec(p99['hi'])}] a {dec(p99['exposure'], 1)} °C",
              dec(diag["disp_ratio"]), f"p = {dec(diag['autocorr_pval'], 3)}"],
})
print("\nTABELA 1\n" + tabela_md(t1))
nomes = {"total": "Total", "heat": "Calor", "cold": "Frio"}
t2 = pd.DataFrame({
    "Componente": tot["component"].map(nomes),
    "FA (%)": [f"{dec(a)} [{dec(lo)}–{dec(hi)}]" for a, lo, hi in
               zip(tot["af_pct"], tot["af_pct_lo"], tot["af_pct_hi"], strict=True)],
    "Óbitos atribuíveis": [f"{milhar(a)} [{milhar(lo)}–{milhar(hi)}]" for a, lo, hi in
                           zip(tot["an"], tot["an_lo"], tot["an_hi"], strict=True)],
})
print("\nTABELA 2\n" + tabela_md(t2))

# --- Figuras 1-3: funções do pacote ---------------------------------------------------------------
for tipo, nome in [("contour", "mod_dlnm_surface"), ("overall", "mod_dlnm_overall"),
                   ("lag", "mod_dlnm_lag")]:
    g = cs.sus_mod_plot_dlnm(fit, type=tipo, lang="pt")
    g.save(str(FIG_DIR / f"{nome}.png"), width=9, height=5, dpi=150, verbose=False)
    print(f"   figura: {nome}.png")

# --- Figura 4: fração atribuível ---------------------------------------------------------------
f4 = tot.assign(componente=tot["component"].map(nomes))
f4["componente"] = pd.Categorical(f4["componente"], categories=["Total", "Calor", "Frio"])
f4["rotulo"] = [f"{dec(v, 1)}%" for v in f4["af_pct"]]
p = (
    ggplot(f4, aes("componente", "af_pct", fill="componente"))
    + geom_col(width=0.6, show_legend=False)
    + geom_errorbar(aes(ymin="af_pct_lo", ymax="af_pct_hi"), width=0.2, color="#333333")
    + geom_text(aes(label="rotulo", y="af_pct_hi"), va="bottom", nudge_y=0.3,
                color=CF_PALETTE["text_main"], fontweight="bold")
    + scale_fill_manual(values=[CF_PALETTE["primary"], CF_PALETTE["accent"], "#1565C0"])
    + scale_y_continuous(expand=(0, 0, 0.12, 0))
    + labs(title="Fração de óbitos atribuível à temperatura",
           subtitle="Idosos (60+), óbitos respiratórios, RMSP 2014-2019 - IC 95% por Monte Carlo",
           x="", y="Fração atribuível (%)")
    + tema_climasus()
)
save_fig(p, "mod_af")
