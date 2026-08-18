# Changelog

## [Não lançado]

### Added — sus_mod_swot / sus_mod_plot_dlnm / sus_mod_plot_swot: SWOT climático-saúde e visualizações DLNM/SWOT

- Adicionado `sus_mod_swot()` (`enrichment/mod_swot.py`), portando `sus_mod_swot.R`: sintetiza os dicts de `sus_mod_vulnerability_index`/`sus_mod_af`/`sus_mod_burden`/`sus_mod_dlnm`/`sus_mod_sensitivity` em um framework SWOT (Forças/Fraquezas/Oportunidades/Ameaças), com pontuação 0–100 por quadrante/entidade e categorização por cortes (`breaks`/`labels`, padrão `pt`/`en`/`es`).
- Adicionado `sus_mod_plot_dlnm()` (`viz/mod_plot_dlnm.py`), portando `sus_mod_plot_dlnm.R`: 7 tipos de gráfico (`overall`, `lag`, `surface`→`contour`, `contour`, `slice`, `distribution`, `series`) a partir do dict de `sus_mod_dlnm()`, mais tabela de resumo estatístico (`output_type="table"`).
- Adicionado `sus_mod_plot_swot()` (`viz/mod_plot_swot.py`), portando `sus_mod_plot_swot.R`: 3 tipos (`radar`, `matrix`, `bar`) a partir do dict de `sus_mod_swot()`.
- **Correção de classificação:** a tabela `no-port-deps.md` da skill `climasus4py-convert` listava essas 3 funções (e `sus_mod_af`/`sus_mod_dlnm`/`sus_mod_excess`, já portadas por outra via) como bloqueadas por `dlnm`/`mvmeta`. Checagem linha a linha (`grep "dlnm::\|mvmeta::"`) confirmou que `sus_mod_swot.R`/`sus_mod_plot_dlnm.R` nunca chamam essas libs — só consomem objetos já portados. Tabela de referência corrigida; ver `IDEIAS.md`.
- **Simplificações deliberadas** (documentadas em `IDEIAS.md`): sem sub-painel de histograma `patchwork` no tipo `"overall"` de `sus_mod_plot_dlnm`; `type="surface"` sem 3-D cai no render de `"contour"` (mesmo fallback do R); paleta `ggsci` substituída por paleta fixa própria; tabelas retornam `dict[str, pandas.DataFrame]` em vez de `gt`; `interactive=True` não suportado em nenhuma das duas (sem dependência `plotly`).

### Added — sus_mod_metaregression / sus_mod_pool / sus_mod_spatial_scan (e 2 plots dependentes): stubs por falta de porte fiel de dlnm/mvmeta/SpatialEpi

- Adicionados como stubs (assinatura completa igual ao R, corpo lança `NotImplementedError` imediatamente): `sus_mod_metaregression()` (`enrichment/mod_metaregression.py`), `sus_mod_pool()` (`enrichment/mod_pool.py`), `sus_mod_spatial_scan()` (`enrichment/mod_spatial_scan.py`), `sus_mod_plot_pool()` (`viz/mod_plot_pool.py`), `sus_mod_plot_spatial_scan()` (`viz/mod_plot_spatial_scan.py`). Motivo: `sus_mod_metaregression()`/`sus_mod_pool()` chamam `dlnm::crosspred()` e `mvmeta::mvmeta()`/`blup()`/`qtest()` diretamente (pooling multivariado de coeficientes DLNM entre cidades, sem porte Python); `sus_mod_spatial_scan()` chama `SpatialEpi::kulldorff()` diretamente (estatística de varredura espacial de Kulldorff, sem lib Python equivalente). Os 2 plots dependentes só consomem os objetos que essas funções produziriam. Ver `IDEIAS.md` para o detalhamento e a revisão do coordenador pendente antes de qualquer implementação aproximada.

### Added — sus_welcome: banner de visão geral do pipeline

- Adicionado `sus_welcome()` (`utils/welcome.py`), portando `sus_welcome.R`: imprime uma visão geral colorida (via `rich`, já dependência do projeto) das etapas do pipeline no console e, opcionalmente, gera uma página HTML autocontida em arquivo temporário (`output=("console","html")`, `lang="pt"/"en"/"es"`, `open=True` abre no browser padrão quando o terminal é interativo — mesmo padrão de `sus_chat()`).
- **Divergência não-comportamental (nomes de função exibidos):** a lista `fns` de cada etapa usa os nomes públicos **reais** do `climasus4py` (ex.: `sus_spatial_join()`, `sus_census()`, `sus_filter()` unificado) em vez de copiar os nomes do R (`sus_join_spatial()`, `sus_socio_add_census()`, `sus_data_filter_cid()`+`sus_data_filter_demographics()`) — ver tabela de renomeação em `docs/migration-from-r-legacy.md`. Um banner listando funções que o usuário não pode chamar seria enganoso.
- **Seção RAP omitida:** o R lista `sus_rap_*` na seção de utilitários; nenhum RAP builder existe ainda em `climasus4py` (decisão de arquitetura pendente, ver `IDEIAS.md`), então a seção foi removida do banner Python em vez de listar funções inexistentes.
- **Etapa 8 (modelagem) e 9 (visualização) refletem apenas o que já está portado:** `sus_mod_pool()`/`sus_mod_metaregression()` (R) e seus respectivos plots ainda não têm porte Python — omitidos da lista em vez de aparecerem como "disponíveis".

### Added — sus_mod_plot_af / sus_mod_plot_sensitivity: visualizações de FA e sensibilidade

- Adicionado `sus_mod_plot_af()` (`viz/mod_plot_af.py`), portando `sus_mod_plot_af.R`: gráficos de barra (FA% por componente total/calor/frio, com IC), forest plot (NA ± IC por componente) e barra por faixa de percentil de exposição (`fit["by_quantile"]`), a partir do dict retornado por `sus_mod_af()`.
- Adicionado `sus_mod_plot_sensitivity()` (`viz/mod_plot_sensitivity.py`), portando `sus_mod_plot_sensitivity.R`: sobreposição de curvas exposição-resposta por estrato, dispersão RR calor vs. RR frio (tamanho = índice de sensibilidade) e forest plot horizontal de RR calor/frio por estrato, a partir do dict retornado por `sus_mod_sensitivity()`.
- Ambas seguem o mesmo padrão já estabelecido em `sus_mod_plot_burden()`: `plotnine` (extra opcional `[plot]`), `interactive=True` não suportado (`plotly` não é dependência empacotada — levanta `ImportError`, ver `IDEIAS.md`), `save_plot` via `plotnine.ggplot.save()`.
- **Divergência de estilo não-comportamental:** `scale_fill_manual(..., guide=FALSE)`/`guide="none"` do ggplot2/R não tem equivalente direto na versão do `plotnine` instalada (0.15.7) — a legenda é ocultada via `theme(legend_position="none")` em vez do parâmetro `guide` da escala. Resultado visual idêntico.

### Added — sus_mod_sensitivity: comparação de sensibilidade climática entre estratos

- Adicionado `sus_mod_sensitivity()` (`enrichment/mod_sensitivity.py`), portando `sus_mod_sensitivity.R`: compara curvas de exposição-resposta e RR cumulativo entre estratos populacionais (faixas etárias, sexo, município, etc.) a partir de um dict nomeado (ou lista, rotulada automaticamente `"Stratum 1"`, `"Stratum 2"`, ...) de ajustes de `sus_mod_dlnm()`. Extrai o RR nos percentis "quente" e "frio" de cada estrato via interpolação linear na grade de 100 pontos já pré-computada em `fit["pred"]` (equivalente a `stats::approx(..., rule=2)` do R — `numpy.interp` já satura no valor de fronteira fora da grade, com aviso de extrapolação). Produz tabela ranqueada por índice de sensibilidade (soma dos log-RR quente+frio) e curvas completas por estrato para plotagem.
- **Diferente de `sus_mod_af`/`sus_mod_excess`: esta função nunca chama `dlnm::crosspred()` de novo** — só lê a grade já computada em cada `fit["pred"]`, que é intrinsecamente bem-ordenada (grade de 100 pontos crescente por construção). O bug de desalinhamento por `sort(unique(at))` documentado para as duas funções anteriores **não se aplica aqui**. Validado numericamente contra `climasus4r` real a menos de 1e-8 (dois ajustes DLNM em metades sobrepostas do dataset sintético fixo).

### Added — sus_mod_excess: excesso de mortalidade/morbidade (3 métodos de baseline)

- Adicionado `sus_mod_excess()` (`enrichment/mod_excess.py`), portando `sus_mod_excess.R`: estima contagem em excesso (observado menos esperado) para uma série diária de saúde, com três métodos de baseline: `"from_dlnm"` (contrafactual a partir de um ajuste DLNM de `sus_mod_dlnm()`), `"spline"` (GLM quasi-Poisson com spline natural sobre o tempo calendário, ajustado no período de controle) e `"serfling"` (regressão harmônica de Serfling). Suporta quebra temporal opcional (`by="year"/"month"/"season"`) e flag de excesso por z-score.
- Validado numericamente contra `climasus4r`/`dlnm` reais: os métodos `"spline"` e `"serfling"` batem **exatamente** (diferença zero) com o R no dataset sintético fixo, incluindo o recorte por `control_period`/`study_period` e a quebra mensal. O método `"from_dlnm"` bate a menos de 0,03 (nível de arredondamento de exibição do R) contra o valor R **corrigido** — ver o bug abaixo.
- **Segunda ocorrência do mesmo bug de correctness silencioso já documentado para `sus_mod_af` (regra 4/§3.3 do CLAUDE.md):** `.sex_baseline_dlnm()` no R chama `dlnm::crosspred(cb, model, at = x, cen = cen)` com `x` em ordem cronológica — e `dlnm::crosspred()` ordena/deduplica `at` internamente, então `fitted_vals / rr_obs` no R pareia o valor ajustado de um dia com o RR de exposição de OUTRO dia (ordenado por exposição, não por data), sempre que a série não é monotônica. O port em Python reutiliza `mod_dlnm._allrr_fit`/`_crosspred` (já corretamente alinhados por linha) — não replica o bug, e foi validado contra o valor R corrigido (RR realinhado por nome antes da divisão), não contra a saída bruta do R. Ver `IDEIAS.md`.
- Preservado (não é bug, é quirk documentável): o baseline `"from_dlnm"` sempre reporta IC a 95% (default do próprio `dlnm::crosspred()`), ignorando o `alpha` passado pelo usuário a `sus_mod_excess()` — mesmo comportamento do caminho delta de `sus_mod_af()`. Os métodos `"spline"`/`"serfling"` respeitam `alpha` corretamente (não usam `crosspred()`).

### Fixed — sus_mod_af: bug de correctness silencioso do R (RR desalinhado por dia) não replicado

- Adicionado `sus_mod_af()` (`enrichment/mod_af.py`), portando `sus_mod_af.R`: fração e número atribuível (AF/AN) a partir de um ajuste DLNM (`sus_mod_dlnm()`), com decomposição calor/frio, tabela por faixa de percentil, quebra temporal opcional (mês/ano/estação) e IC via simulação Monte Carlo (`numpy.random.Generator.multivariate_normal`, análogo direto de `MASS::mvrnorm`) ou método delta (`nsim=0`).
- **Bug real encontrado no R, corrigido no Python (regra 4/§3.3 do CLAUDE.md — correctness silencioso, não um "quirk" a preservar):** `dlnm::crosspred(cb, model, at = x, cen = cen)` faz `at <- sort(unique(at))` internamente — o vetor de RR retornado (`allRRfit`) vem em **ordem crescente de exposição**, não na ordem original de `x`. `.saf_component()` no R então faz `cases * (1 - 1/rr_obs)` — uma multiplicação **posicional**, pareando o número de casos de cada dia (ordem cronológica) com o RR de um dia *diferente* (ordem ordenada por exposição), sempre que a série de exposição não está pré-ordenada (ou seja, sempre, para uma série temporal real). Verificado numericamente: em um dataset sintético fixo, o R "como escrito" retorna FA total = -5,03%; o valor correto (RR realinhado por nome/posição antes da multiplicação) é FA total = -5,32% — uma diferença de ~6% relativa, não um erro de arredondamento. O port em Python nunca introduz esse reordenamento (o RR é calculado já alinhado à ordem de entrada) — portanto o Python está correto e diverge deliberadamente do output bruto do R nesse ponto. Validado numericamente contra o valor R corrigido (realinhado por nome) a menos de 1e-6 em `tests/test_mod_af.py`. Ver `IDEIAS.md` para o detalhamento completo e a recomendação de reportar o bug upstream ao `climasus4r`.
- `nsim=0` usa IC pelo método delta, sempre no nível de 95% (`dlnm::crosspred()`'s próprio default) — um quirk do R (`alpha` do usuário é ignorado nesse caminho) preservado deliberadamente, já que não é um bug de correctness silencioso (é consistente e documentável, apenas surpreendente).

### Added — sus_mod_dlnm: Distributed Lag Non-linear Model, reimplementado do zero

- Adicionado `sus_mod_dlnm()` (`enrichment/mod_dlnm.py`), portando `sus_mod_dlnm.R`: modelo de defasagem distribuída não-linear (DLNM, Gasparrini et al. 2010/2011/2014) para associação exposição climática × desfecho de saúde diário. O pacote R `dlnm` (`crossbasis`/`crosspred`) não tem binding Python — em vez de aproximar, a base spline natural (mesma regra de nós de `splines::ns()`), a construção da cross-basis bidimensional (exposição × lag, igual a `dlnm::crossbasis`) e a grade de predição centrada com RR cumulativo/por-lag e IC de Wald (igual a `dlnm::crosspred`) foram **reimplementadas do zero em NumPy/statsmodels e validadas numericamente contra a saída real do R** (`climasus4r`/`dlnm` 2.4.10 instalados localmente): RR, IC, RR cumulativo por lag, razão de dispersão, AIC e p-valor de Ljung-Box batem com o R a menos de 1e-6 em um dataset sintético fixo (fixture `tests/fixtures/dlnm_synthetic_input.csv`, pinado como regressão em `tests/test_mod_dlnm.py`). Os coeficientes do GLM em si **não** são numericamente iguais aos do R (parametrização diferente, mas equivalente, do mesmo espaço de spline natural) — só os valores preditos/RR importam para paridade, e esses batem exatamente (ver docstring do módulo e `IDEIAS.md`).
- Suporta `argvar`/`arglag` com `fun` em `"ns"`/`"lin"`/`"poly"` (as três opções documentadas no R); `fun="strata"`/`"integer"`/`"thr"` não implementadas (levantam `ValueError` explícito) — não usadas nos exemplos documentados de `sus_mod_dlnm`.
- **Atenção:** a função assume o contrato documentado do R (`date` + colunas `{climate_col}_lagN`), que o `sus_climate_aggregate()` real em Python ainda não produz (`temporal_strategy="distributed_lag"` não implementado) — mesma categoria de divergência já registrada no achado prioritário no topo de `IDEIAS.md`.
- É a função fundacional de 6 outras funções do R (`sus_mod_af`, `sus_mod_excess`, `sus_mod_metaregression`, `sus_mod_pool`, `sus_mod_sensitivity`, `sus_mod_swot`, todas bloqueadas pelo `dlnm` até agora), que consomem o objeto `climasus_dlnm` retornado aqui — porte planejado em lotes subsequentes.

### Stub — Funções bloqueadas por INLA/CARBayes (sem porte fiel em Python)

- Adicionados como stubs (assinatura completa igual ao R, corpo lança `NotImplementedError` imediatamente): `sus_mod_spacetime_bayes()` (`enrichment/mod_spacetime_bayes.py`, porta `sus_mod_spacetime_bayes.R`), `sus_mod_spatial_bayes()` (`enrichment/mod_spatial_bayes.py`, porta `sus_mod_spatial_bayes.R`), `sus_mod_spacetime_exceedance()` (`enrichment/mod_spacetime_exceedance.py`, porta `sus_mod_spacetime_exceedance.R`), `sus_mod_spacetime_predict()` (`enrichment/mod_spacetime_predict.py`, porta `sus_mod_spacetime_predict.R`), `sus_mod_plot_spacetime()` (`viz/mod_plot_spacetime.py`, porta `sus_mod_plot_spacetime.R`) e `sus_mod_plot_spatial_bayes()` (`viz/mod_plot_spatial_bayes.py`, porta `sus_mod_plot_spatial_bayes.R`). Motivo: `sus_mod_spacetime_bayes()` e `sus_mod_spatial_bayes()` dependem do `INLA` (inferência Bayesiana por aproximação de Laplace) e/ou do `CARBayes` (amostrador MCMC para priors CAR/BYM/Leroux) — nenhum dos dois tem equivalente Python; uma reimplementação via PyMC/MCMC usaria um algoritmo de inferência diferente, não seria um porte fiel. As outras quatro funções (`sus_mod_spacetime_exceedance`, `sus_mod_spacetime_predict`, `sus_mod_plot_spacetime`, `sus_mod_plot_spatial_bayes`) só consomem o objeto retornado por uma das duas anteriores — como esse objeto nunca existe em Python, foram stubadas junto. Decisão já validada com o usuário em conversa de escopo prévia; ver detalhamento em `IDEIAS.md` e revisão do coordenador pendente antes de qualquer implementação aproximada.

### Added — Seleção de dados censitários, CID-10, indicadores socioeconômicos e visualizações

- Adicionado `sus_census_select()` (`utils/census_select.py`), portando `sus_census_explore.R`: navegador HTML interativo do catálogo de variáveis censitárias (Censo 2010/2022 do IBGE) por dataset (`"demografia"`, `"domicilios"`, `"renda"`, etc.), com filtro por idioma. **Atenção:** o dicionário de variáveis censitárias (`dictionaries/{pt-pt,pt-en,pt-es}/census_<dataset>.json`) ainda não existe em `climasus-data` — a função lança erro explícito em vez de hardcodear os nomes das variáveis (ver `IDEIAS.md`).
- Adicionado `sus_data_cid_select()` (`utils/cid_select.py`), portando `sus_filter_cid_explore.R`: navegador HTML interativo dos grupos de doenças sensíveis ao clima (CID-10), com filtro `filter_climate` e suporte a `lang`. O mapa nome→categoria (58 entradas) foi portado 1:1 do R como stopgap — candidato a migração para `climasus-data` (ver `IDEIAS.md`).
- Adicionado `sus_socio_compute_indicators()` e `sus_socio_list_indicators()` (`enrichment/socio_indicators.py`), portando `sus_socio_compute_indicators.R`: calcula 22 indicadores socioeconômicos/epidemiológicos (razão de dependência, taxa de urbanização, mortalidade infantil/materna, indicadores de saneamento, etc.) a partir de colunas mapeadas via `col_mapping`, com IC de Poisson/binomial opcional. Catálogo de 22 fórmulas portado 1:1 do R (mesmo hardcode que o R já usa internamente) — candidato a migração para `climasus-data` (ver `IDEIAS.md`).
- Adicionado `sus_data_plot_aggregate_map()` (`viz/data_plot_aggregate_map.py`), portando `sus_data_plot_aggregate_map.R`: mapas coropléticos/de bolha por município via `plotnine`+`geopandas`. Adiciona um parâmetro novo `municipalities` (não presente no R, necessário para fornecer a geometria) — pendente de aprovação do coordenador, mesmo precedente já registrado para `sus_mod_plot_spatial_moran` (ver `IDEIAS.md`).
- Adicionado `sus_data_plot_aggregate_ts()` (`viz/data_plot_aggregate_ts.py`), portando `sus_data_plot_aggregate_ts.R`: quatro tipos de gráfico de série temporal (`epidemic`, `seasonal`, `heatmap`, `trend`) via `plotnine`. **Quirk preservado do R:** o parâmetro `plot_type`, se omitido, expande para os 4 tipos simultaneamente via `match.arg(..., several.ok = TRUE)` — não é apenas o primeiro da lista. **Atenção:** implementado contra o contrato documentado do R (`date` + coluna de desfecho nomeada), que diverge do contrato real de `sus_data_aggregate()` em Python — mesma categoria de divergência já registrada para `sus_climate_aggregate`/`sus_climate_plot_aggregate` (ver `IDEIAS.md`). O tipo `epidemic` com `smooth_method="loess"` (padrão) requer o pacote opcional `scikit-misc` (mesma dependência já flagada para `sus_climate_plot_aggregate`).
- Adicionado `sus_data_plot_demographics()` (`viz/data_plot_demographics.py`), portando `sus_data_plot_demographics.R`: visualizações demográficas (barras, pirâmide etária, mapa de calor, série temporal, clima, equidade racial, dashboard) via `plotnine`. Não decodifica códigos brutos de sexo/raça/escolaridade para rótulos (mesmo comportamento do R) — `decode_labels=True` sugerido como opt-in futuro, não implementado unilateralmente (ver `IDEIAS.md`).

### Added — Normais climatológicas INMET

- `sus_climate_normals()` e `sus_climate_normals_meta()` ([core/climate_normals.py](climasus4py/core/climate_normals.py)) — porte de `sus_climate_normals.R`/`sus_climate_normals_meta.R` do `climasus4r` legacy. Baixa normais climatológicas de 30 anos do INMET (1961-1990, 1981-2010, 1991-2020) por variável, com cache em disco (Parquet) e cabeçalho Excel com células mescladas por mês/década, retornando um `pandas.DataFrame` no formato longo (metadados em `df.attrs["sus_meta"]`).
- O catálogo de variáveis (`normal_meta` no R) é lido de `climasus-data` (`metadata/inmet_normals.json`) em vez de replicar a cadeia bundled→cache→GitHub do R — ver observação em [`IDEIAS.md`](IDEIAS.md).
- Adicionado `sus_climate_uniplu()` (`core/climate_uniplu.py`), portando `sus_climate_uniplu.R`: importa o UNIPLU-BR (base unificada de chuva no Brasil, 21 mil+ estações, 1885–2025), com download/cache do ZIP do Zenodo, join e padronização de colunas via DuckDB, filtros por ano/UF/rede e agregação temporal opcional (dia/mês/ano).

### Added — Índices de seca (SPI/SPEI)

- Adicionado `sus_climate_compute_spi()` (`enrichment/climate_spi.py`), portando `sus_climate_compute_spi.R`: Standardized Precipitation Index (McKee et al., 1993) em múltiplas escalas temporais, com ajuste de distribuição gama por município (método dos momentos) e mistura de probabilidade de zero. Requer `scipy` (não é dependência base — ver `IDEIAS.md`).
- Adicionado `sus_climate_compute_spei()` (`enrichment/climate_spei.py`), portando `sus_climate_compute_spei.R`: Standardized Precipitation-Evapotranspiration Index (Vicente-Serrano et al., 2010), com balanço hídrico `P - PET` e PET opcional via Thornthwaite (1948). Requer `scipy` para a transformação normal — ver `IDEIAS.md`.

### Added — Anomalias climáticas

- Adicionado `sus_climate_anomaly()` (`enrichment/climate_anomaly.py`), portando `sus_climate_anomaly.R`: compara dados observados (`sus_climate_inmet`/`sus_climate_aggregate`) contra normais climatológicas (`sus_climate_normals`), com métodos `absolute`/`relative`/`standardized`/`all` e escala `monthly`/`decadal`.

### Added — Detecção de ondas de calor e de frio

- Adicionado `sus_climate_compute_heatwaves()` + `hw_get_events()`/`hw_count_by_year()`/`hw_active_days()` (`enrichment/climate_heatwaves.py`), portando `sus_climate_compute_heatwaves.R`: até sete métodos de detecção (WHO, WMO, INMET, EHF, UTCI, WBGT, HI) sobre dados horários/sub-diários, com baseline circular por dia-do-ano (janela ±15 dias), extração de eventos e resumo anual. Retorna um `dict` de três `pandas.DataFrame` (`events`/`daily`/`summary`), cada um com metadados em `.attrs["sus_meta"]`.
- Adicionado `sus_climate_compute_coldwaves()` + `cw_get_events()`/`cw_count_by_year()`/`cw_active_days()` (`enrichment/climate_coldwaves.py`), portando `sus_climate_compute_coldwaves.R`: mesmos sete métodos com sinais invertidos (limiares P10 por padrão, Excess Cold Factor em vez de Excess Heat Factor).

### Added — Qualidade de séries temporais

- Adicionado `sus_data_ts_quality()` (`utils/ts_quality.py`), portando `sus_data_ts_quality.R`: avalia completude, outliers mensais (cerca de Tukey) e lacunas temporais por município, com score composto (0-100) e recomendação de inclusão/exclusão. O teste de quebra estrutural (`strucchange::sctest`, OLS-CUSUM) não tem porte Python adotado — `has_break`/`break_pval` são sempre `None`, com aviso emitido (ver `IDEIAS.md`).

### Added — Visualização exploratória clima-saúde

- Adicionado `sus_climate_plot_aggregate()` (`viz/climate_plot_aggregate.py`), portando `sus_climate_plot_aggregate.R`: seis tipos de gráfico exploratório via `plotnine` (`timeseries`, `scatter`, `ccf`, `distribution`, `corr_matrix`, `seasonal`) para tabelas diárias clima-saúde. Requer o extra opcional `[plot]`. **Atenção:** esta função espera uma tabela com `date` + desfecho + colunas climáticas (o contrato real do `sus_climate_aggregate()` do R) — divergência importante registrada em `IDEIAS.md`.
- Adicionado `sus_climate_plot_heatwaves()` (`viz/climate_plot_heatwaves.py`), portando `sus_climate_plot_heatwaves.R`: visualiza o `dict` retornado por `sus_climate_compute_heatwaves()` em quatro tipos de gráfico (`timeline`, `calendar`, `intensity`, `trend`), com paleta ggsci "npg" e rótulos multilíngues.
- Adicionado `sus_climate_plot_coldwaves()` (`viz/climate_plot_coldwaves.py`), portando `sus_climate_plot_coldwaves.R`: mesmos quatro tipos de gráfico para o `dict` retornado por `sus_climate_compute_coldwaves()`.
- Adicionado `sus_mod_plot_burden()` (`viz/mod_plot_burden.py`), portando `sus_mod_plot_burden.R`: três tipos de gráfico (`lollipop`, `lorenz`, `stacked`) para o `dict` retornado por `sus_mod_burden()`.
- Adicionado `sus_mod_plot_ml()` (`viz/mod_plot_ml.py`), portando `sus_mod_plot_ml.R`: três tipos de gráfico (`importance`, `fit`, `cv_log`) para o `dict` retornado por `sus_mod_ml()`.
- Adicionado `sus_mod_plot_vulnerability()` (`viz/mod_plot_vulnerability.py`), portando `sus_mod_plot_vulnerability.R`: três tipos de gráfico (`ranking`, `pillars`, `lorenz`) para o `dict` retornado por `sus_mod_vulnerability_index()`; funciona tanto com `normalize="minmax"` (score limitado) quanto `normalize="zscore"` (score ilimitado, pode ser negativo).

### Fixed — Bugs de renderização plotnine (não detectados pelos testes originais dos agentes)

- **`sus_mod_plot_ml()`**: `cv_log` não tem coluna `"iter"` (o número da rodada de boosting é só o índice do DataFrame retornado por `xgboost.cv()`) — `KeyError` real ao chamar com `type="cv_log"`. Corrigido usando o índice (1-based, igual à convenção `best_nrounds` já usada em `mod_ml.py`) em vez de uma coluna inexistente.
- **`sus_mod_plot_ml()` / `sus_mod_plot_vulnerability()` / `sus_mod_plot_burden()`**: uso de nomes de cor no estilo R/ggplot2 (`"gray30"`, `"gray40"`, `"grey50"` etc.) que não existem na paleta de cores nomeadas do `mizani`/`plotnine` instalado (`ValueError: Unknown name 'gray30' for a color.`) — só disparava ao efetivamente renderizar o plot (`.draw()`/`.save()`), não na construção do objeto `ggplot`, então os testes originais (que só verificavam `type(p).__name__ == "ggplot"`) não pegaram o bug. Substituído por códigos hex equivalentes (`gray30` → `#4D4D4D` etc.) nos quatro módulos afetados (`mod_plot_ml.py`, `mod_plot_vulnerability.py`, `mod_plot_burden.py`, `mod_plot_spatial_moran.py`).
- **`sus_mod_plot_burden()`**: usava `geom_errorbar` (que exige `ymin`/`ymax`, geometria vertical) com estética `xmin`/`xmax` mapeada — `PlotnineError` real ao renderizar a barra de erro horizontal do lollipop. Corrigido para `geom_errorbarh` (a variante horizontal correta do plotnine).
- **Lição de processo:** os testes desta sessão para módulos de plot precisam chamar `.draw()` (ou `.save()`) para forçar a renderização real via matplotlib — checar apenas `type(p).__name__ == "ggplot"` não exercita a montagem de camadas/geometrias e deixa passar erros de parâmetro/cor que só aparecem no render de fato. Auditoria retroativa com `.draw()` real nos módulos `viz/` já mesclados antes desta sessão não encontrou bugs adicionais — apenas uma dependência opcional não declarada: `scikit-misc`, exigida por `plotnine.geom_smooth(method="loess")` em `sus_climate_plot_aggregate(plot_type="scatter")` (ver `IDEIAS.md`).

### Added — Dados ambientais em grade (grid)

- Adicionado `sus_grid_join()` (`enrichment/grid_join.py`), portando `sus_grid_join.R`: ponte entre o pipeline de dados ambientais em grade (`sus_grid_*`) e o pipeline de saúde, unindo por `code_muni`/`date` (ou apenas `code_muni` para dados anuais como PRODES).
- Adicionado `sus_grid_era5()` (`enrichment/grid_era5.py`), portando `sus_grid_era5.R`: agregados diários ERA5-Land para a América Latina (Zenodo, DOI 10.5281/zenodo.10013254), com cache em disco e agregação zonal por município via `exactextract`.
- Adicionado `sus_grid_chirps()` (`enrichment/grid_chirps.py`), portando `sus_grid_chirps.R`: precipitação CHIRPS v2.0 (UCSB/CHC), resoluções `monthly`/`daily`/`annual`, cache em disco (raster + Parquet) e agregação zonal via `exactextract`.
- Adicionado `sus_grid_fires()` (`enrichment/grid_fires.py`), portando `sus_grid_fires.R`: focos de calor via API do INPE Queimadas (padrão) ou NASA FIRMS, com agregação município x dia por join ponto-em-polígono (`geopandas`) quando `municipalities` é informado.
- Adicionado `sus_grid_pdsi()` (`enrichment/grid_pdsi.py`), portando `sus_grid_pdsi.R`: Índice de Severidade de Seca de Palmer via TerraClimate (padrão) ou NOAA PSL/Dai, com agregação zonal via `exactextract`.
- Adicionado `sus_grid_koppen()` (`enrichment/grid_koppen.py`), portando `sus_grid_koppen.R`: classificação climática de Köppen-Geiger por município (Alvares et al. 2013) — modo `"approx"` (padrão, sem dependências novas) ou `"exact"` via junção espacial contra um shapefile fornecido pelo usuário.
- Adicionado `sus_grid_smvi()` (`enrichment/grid_smvi.py`), portando `sus_grid_smvi.R`: catálogo global de eventos de flash drought — Soil Moisture Volatility Index (Osman et al. 2024, DOI 10.1038/s41597-024-03809-9) via HydroShare, com agregação anual/mensal por município via junção espacial ponto-em-polígono (`geopandas`, sem raster/`exactextract`).
- Adicionado `sus_grid_prodes()` (`enrichment/grid_prodes.py`), portando `sus_grid_prodes.R`: desmatamento anual do PRODES/INPE via WFS do TerraBrasilis (6 biomas), com interseção espacial (`geopandas`) contra municípios e agregação município x ano. Corrigido o mesmo bug de colisão de cache Parquet já identificado em `sus_grid_pdsi` (cache tagueado só por `uf`, não por `municipalities` — adicionado `muni_hash`).
- Adicionado `sus_grid_pollution_cams()` (`enrichment/grid_pollution_cams.py`), portando `sus_grid_pollution_cams.R`: poluição atmosférica CAMS (PM2.5, PM10, CO, O3, NO2, SO2) pré-processada por município (2003-2024, Zenodo) — arquivos já vêm agregados por município, sem necessidade de `geopandas`/raster.
- Adicionado `sus_grid_pollution_ghap()` (`enrichment/grid_pollution_ghap.py`), portando `sus_grid_pollution_ghap.R`: rasters GHAP (GlobalHighAirPollutants v2, Zenodo) de PM2.5/O3/CO, recorte para o Brasil e agregação zonal via `exactextract`. Corrigido bug de metadado (`temporal.unit` mapeado incorretamente pelo R) e o mesmo bug de colisão de cache de `sus_grid_pdsi`.
- Adicionado `sus_grid_pollution_merra2()` (`enrichment/grid_pollution_merra2.py`), portando `sus_grid_pollution_merra2.R`: aerossóis/poluição atmosférica NASA MERRA-2 (GES DISC, requer login Earthdata gratuito via `EARTHDATA_USER`/`EARTHDATA_PASSWORD` ou `.netrc`), variáveis `pm25`/`aod`/`so2` (experimental), cache em disco (NetCDF + Parquet) e agregação zonal via `exactextract`.
  - **Correção de correctness silencioso (regra 4/§3.3 do CLAUDE.md), não apenas replicado:** o R junta os resultados de múltiplos poluentes com `full_join()` linha a linha do manifesto (`sus_grid_pollution_merra2.R:433`), o que — para dois meses do *mesmo* poluente — nunca casa a chave `(code_muni, date)` e produz colunas duplicadas com sufixo `.x`/`.y` em vez do contrato documentado de "uma coluna por poluente". O port faz `concat` dentro de cada poluente primeiro, depois `merge` externo entre poluentes — honrando o contrato de retorno documentado (e o próprio `@return` do R). O caminho de cache Parquet 100%-hit tem o mesmo defeito, agravado: o R usa `rbind()` (que erraria com colunas diferentes por poluente), mas um `pd.concat()` literal em Python empilharia silenciosamente com NaN e linhas `(code_muni, date)` duplicadas em vez de falhar — corrigido com o mesmo agrupamento por poluente.
  - **Correção do mesmo bug de colisão de cache já identificado em `sus_grid_pdsi`:** o nome do arquivo Parquet de cache (`{pollutant}_{resolution}_{ano}{mês}.parquet`) não inclui hash do conjunto de `municipalities` no R — corrigido com o mesmo `muni_hash` md5 já usado em `grid_chirps.py`/`grid_pdsi.py`.
  - Demais divergências (arquivo idêntico para `resolution="daily"`/`"monthly"`, `so2` provavelmente inválido, limitação de auth em redirecionamento cross-host, engine NetCDF não declarado) preservadas do R e documentadas em `IDEIAS.md`.
- **Novas dependências opcionais pendentes de aval do coordenador** (usadas por `sus_grid_era5`/`sus_grid_chirps`/`sus_grid_pdsi`/`sus_grid_pollution_merra2`/`sus_grid_pollution_ghap`/`sus_grid_prodes`): `rioxarray`, `xarray`, `exactextract` (binding Python do mesmo motor C++ isciences que a `exactextractr` do R usa — não `rasterstats`, que só aproxima a ponderação fracionária pixel-polígono), além de um engine NetCDF (`netcdf4` ou `h5netcdf`, necessário por `xarray.open_dataset()` em arquivos `.nc4`/`.nc`). `sus_grid_fires`/`sus_grid_koppen`/`sus_grid_smvi`/`sus_grid_pollution_cams` não precisam de raster, só de `geopandas` (já no extra `[spatial]`) ou de nada além dos utilitários base. Ver `IDEIAS.md`.

### Added — Modelagem epidemiológica e espacial

- Adicionado `sus_mod_spatial_weights()` (`enrichment/mod_spatial_weights.py`), portando `sus_mod_spatial_weights.R`: pesos espaciais de contiguidade (Queen/Rook) a partir de polígonos municipais, base para `sus_mod_spatial_moran()`/`sus_mod_spatial_reg()`. Usa `libpysal` em vez de `spdep`; estilos `W`/`B`/`S` mapeiam para transforms nativos do `libpysal` (`R`/`B`/`V`), estilos `C`/`U`/`minmax` são calculados manualmente. **Nova dependência opcional pendente de aval do coordenador**: `libpysal` (+ `esda`, usado pelas funções consumidoras) — ver `IDEIAS.md`.
- Adicionado `sus_mod_spatial_moran()` (`enrichment/mod_spatial_moran.py`), portando `sus_mod_spatial_moran.R`: I de Moran global e LISA local (Anselin 1995) via `esda.Moran`/`esda.Moran_Local`. Corrige um risco de correção silenciosa do R (realinha `df` explicitamente a `W["listw"].id_order` por `code_muni`, em vez de assumir que a ordem já coincide). `p.adjust` (fdr/bonferroni/none) reimplementado à mão, replicando o comportamento de `n` do R (conta só p-valores não-NA), verificado contra uma sessão real do R.
- Adicionado `sus_mod_plot_spatial_moran()` (`viz/mod_plot_spatial_moran.py`), portando `sus_mod_plot_spatial_moran.R`: mapa coroplético por quadrante LISA e/ou dispersão de Moran para o `dict` retornado por `sus_mod_spatial_moran()`.
- Adicionado `sus_mod_spatial_reg()` (`enrichment/mod_spatial_reg.py`), portando `sus_mod_spatial_reg.R`: regressão espacial por máxima verossimilhança (SAR/lag, SEM/error, SDM/durbin) via `spreg.ML_Lag`/`spreg.ML_Error`, com impactos diretos/indiretos/totais (LeSage & Pace) e teste de Moran nos resíduos. `model="sac"` e `method="Chebyshev"`/`"MC"` levantam `NotImplementedError` — `spreg` só oferece um estimador GMM (`GM_Combo`) para SAC, não equivalente ao `spatialreg::sacsarlm()` (ML) do R. **Nova dependência opcional**: `spreg`.
- Adicionado `sus_mod_casecrossover()` (`enrichment/mod_casecrossover.py`), portando `sus_mod_casecrossover.R`: análise case-crossover estratificada no tempo (Maclure, 1991; Levy et al., 2001). Métodos `"conditional_poisson"` (`statsmodels`, GLM com efeitos fixos de estrato) e `"clogit"` (`lifelines`, via o truque de Breslow & Day 1980 que o próprio `survival::clogit()` do R usa internamente). Retorna `CaseCrossoverResult`. **Novas dependências opcionais**: `statsmodels`, `lifelines` — ver `IDEIAS.md`.
- Adicionado `sus_mod_its()` (`enrichment/mod_its.py`), portando `sus_mod_its.R`: séries temporais interrompidas (ITS) via regressão segmentada quasi-Poisson/Poisson (`statsmodels.GLM`), com termos de nível/inclinação por data de intervenção, harmônicos sazonais e projeção contrafactual. Retorna `ClimasusITS`. O R original não tem correção de autocorrelação (nem `gls`, nem `arima`) — o port não inventa uma.
- Adicionado `sus_mod_ml()` + `sus_mod_ml_predict()` (`enrichment/mod_ml.py`), portando `sus_mod_ml.R`/`predict.climasus_ml()`: modelo XGBoost para prever desfechos de saúde a partir de variáveis climáticas/socioeconômicas, com validação cruzada k-fold (opcionalmente *group-aware* por município), predições out-of-fold, importância de variáveis e métricas de desempenho. Usa os extras `[ml]`/`[xgboost]` já existentes — nenhuma dependência nova.
- Adicionado `sus_mod_burden()` (`enrichment/mod_burden.py`), portando `sus_mod_burden.R`: tabela de carga de doença ranqueada entre cidades/estratos a partir de resultados pré-computados de fração atribuível (AF) ou excesso, com curva de concentração estilo Lorenz. A função em si não depende de `dlnm`, mas seus produtores reais no R (`sus_mod_af`/`sus_mod_excess`/`sus_mod_dlnm`) dependem e não foram portados — `fits` aceita tabelas fornecidas diretamente pelo usuário.
- Adicionado `sus_mod_vulnerability_index()` (`enrichment/mod_vulnerability_index.py`), portando `sus_mod_vulnerability_index.R`: índice composto de vulnerabilidade climática IPCC AR6 (Exposição + Sensibilidade − Capacidade Adaptativa), normalização min-max/z-score, pesos por indicador/pilar e coeficiente de Gini.

### Fixed

- `sus_grid_pdsi()`: chave de cache Parquet agora inclui um hash das municipalities (`code_muni` ordenados), corrigindo uma colisão de cache silenciosa presente no `sus_grid_pdsi.R` original (chamadas com os mesmos `years` mas municípios diferentes reaproveitariam o cache Parquet errado, resultado incorreto sem aviso) — corrigido por ser correctness silencioso (§3.3 exceção do CLAUDE.md), não apenas replicado.
- `utils/data.py::load_json()`: passou a abrir arquivos com `encoding="utf-8-sig"` em vez de `"utf-8"` puro — `climasus-data`'s `geo/municipios.json` tem BOM e causava `JSONDecodeError`. Mudança retrocompatível (lê corretamente com ou sem BOM).

## [0.2.0a4] - 2026-05-26

### Fixed - INMET header correctness

- `parse_inmet_csv()` agora detecta o cabeçalho real de dados pelo token `HORA`, evitando o falso match em metadados como `DATA DE FUNDAÇÃO (YYYY-MM-DD)`. Isso impede schemas explodidos quando várias estações INMET são unidas.
- Fixtures reais latin-1 foram adicionadas para FLORIANOPOLIS/A806 e ERECHIM/A828 cobrindo formatos INMET com metadados antes do bloco horário.

### Changed - INMET lazy backend

- O parser INMET agora retorna `duckdb.DuckDBPyRelation`; a API pública `sus_climate_inmet()` continua retornando `pd.DataFrame` por compatibilidade.
- Parsing, renomeação canônica, casts numéricos, QC físico, QC dew-point e QC solar noturno foram migrados para DuckDB SQL.
- `_process_year` deixou de usar `pd.concat` e `pa.Table.from_pandas`; a união é feita por `UNION ALL` DuckDB e o cache Parquet/Zstd é escrito com `COPY`.
- A saída canônica usa `wmo_code` e inclui `date`, `year`, 8 metadados de estação e as colunas de medição documentadas, sem colunas raw extras.

## [0.2.0a3] - 2026-05-24

> Hotfix release implementando o plano [`2026-05-24-py-correcoes-revisao-OWASP-correctness.md`](../governanca/3-planos/em-execucao/2026-05-24-py-correcoes-revisao-OWASP-correctness.md). Todos os itens entram nas **exceções da diretriz de paridade** (OWASP + correctness silencioso + bugs com evidência empírica de crash em produção). Demais achados da revisão estrutural de 2026-05-24 foram registrados em [`ideias-climasus4py-v2.md`](../governanca/6-instancia/ideias-climasus4py-v2.md) para o v2.0.

### Fixed — SQL injection / OWASP

- **`sus_filter(date_start=, date_end=)`** ([core/filter.py](climasus4py/core/filter.py)) — datas embutidas via `_sql.sql_string()` em vez de `f'\'{date_start}\''`. Mesma normalização aplicada em filtros de sex, race, ICD e `drop_ignored`.
- **`sus_export()`** ([io/export.py](climasus4py/io/export.py)) — `_copy_to` reescrito para passar o destino via `sql_string()` e registrar a relação sob view com sufixo UUID (em vez de depender da resolução implícita de locais `rel`). Adicionada allowlist para `compress` (`snappy`/`zstd`/`gzip`/`none`/`lz4`).
- **`sus_data_quality_report()`** ([utils/quality.py](climasus4py/climasus4py/utils/quality.py)) — nomes de coluna passados por `quote_ident()`, migrado para o padrão `rel.query(alias, sql)` (não polui mais o namespace global da conexão singleton).
- **`sus_pipeline` fast path** ([core/pipeline.py](climasus4py/climasus4py/core/pipeline.py)) — paths dos parquets embutidos via `sql_string()`; `age_min`/`age_max` coagidos com `int()`.

### Fixed — Correctness silencioso

- **`sus_pipeline` fast path: truncamento silencioso de CID** — antes, `prefixes[:200]` descartava prefixos extras sem aviso, produzindo resultados divergentes do staged pipeline. Agora, quando a lista de prefixos excede 200, o fast path retorna `None` e o staged (que usa `SEMI JOIN`) toma o controle.
- **`sus_pipeline` fast path: fallback silencioso** — `except Exception` agora emite `UserWarning` em vez de `logging.debug`, expondo quando o fast path falhou e o staged está sendo usado.
- **`sus_pipeline` fast path: nome da coluna geográfica** — `geo_alias` agora é sempre `"state"` ou `"municipality"` (antes mudava conforme a coluna detectada no source).
- **`codes_for_groups(group_names)`** ([utils/cid.py](climasus4py/climasus4py/utils/cid.py)) — grupos desconhecidos agora levantam `KeyError` listando os disponíveis. Antes, typos retornavam lista vazia silenciosamente e produziam zero linhas downstream sem aviso.
- **`expand_city_to_codes()`** ([utils/data.py](climasus4py/climasus4py/utils/data.py)) — normalização agora é **realmente** accent-insensitive (NFKD + strip de combining marks). A versão anterior usava NFC, que **mantém** acentos — "São Paulo" não batia com "Sao Paulo". Docstring corrigida.
- **`sus_data_clean_encoding(fix_enc=)`** ([core/clean.py](climasus4py/climasus4py/core/clean.py)) — argumento era no-op silencioso (nunca aplicou correção de encoding). Agora documentado como `deprecated` e emite `DeprecationWarning`; mantido apenas para retrocompatibilidade.
- **`sus_climate_fill_inmet` quality filter** ([enrichment/climate_fill.py](climasus4py/climasus4py/enrichment/climate_fill.py)) — exclusão de estações por threshold de missing values agora considera **todas** as `vars_to_fill`, não apenas `vars_to_fill[0]`. A estação só é excluída quando todas as variáveis-alvo excedem o threshold.
- **`sus_data_import` agora registra stage** ([core/importer.py](climasus4py/climasus4py/core/importer.py)) — chama `set_stage("import", system=..., rel_type="health")`. O exemplo do docstring de `sus_meta` agora funciona; docstring de `sus_meta` ([core/meta.py](climasus4py/climasus4py/core/meta.py)) atualizada para explicar honestamente a limitação do `WeakKeyDictionary` (transformações criam objetos novos).
- **`sus_census` legacy path** ([enrichment/census.py](climasus4py/climasus4py/enrichment/census.py)) — emite `UserWarning` ao materializar `DuckDBPyRelation` em DataFrame para o pandas merge.

### Fixed — OOM em `sus_climate_inmet` (BUG-2026-05-24-A)

Crash reportado em uso real: chamada padrão materializava o dataset nacional INMET (~5-10 GB/ano em pandas), com `parallel=True` mantendo múltiplos DataFrames de anos simultaneamente, resultando em OOM em máquinas com < 32 GB RAM. Cinco mitigações combinadas ([core/climate_inmet.py](climasus4py/climasus4py/core/climate_inmet.py)):

- **Parsing por UF (correção principal):** quando o usuário supre `uf=`, apenas os CSVs cujo nome contém o código da UF são parseados. A versão anterior parseava o conjunto nacional inteiro antes de aplicar o filtro UF, o que era a causa raiz do OOM no cache miss.
- **Cache em Hive partition por UF:** cada UF tem agora seu próprio sub-diretório (`year=<YYYY>/UF=<XX>/data.parquet`); chamadas subsequentes para uma UF diferente só baixam/parseiam o subset novo. O layout legado nacional (`year=<YYYY>/data.parquet`) ainda é aceito na leitura. Helpers `_year_cache_covers` + `_read_year_cache_filtered` cobrem ambos os layouts.
- Default `parallel=False` (era `True`). Documentado no docstring; opt-in explícito para máquinas com folga de RAM.
- `UserWarning` quando `uf=None`, alertando sobre o tamanho do dataset nacional e o número de anos solicitados.
- `gc.collect()` explícito entre anos no path sequencial.
- O refactor estrutural (`sus_climate_inmet` retornar `DuckDBPyRelation` lazy, eliminando a materialização interna por design) foi registrado em [`ideias-climasus4py-v2.md`](../governanca/6-instancia/ideias-climasus4py-v2.md) para o v2.0.

**Validação empírica (2026-05-24, smoke real com download INMET):**

| Cenário | Antes (v0.2.0a1) | Depois (v0.2.0a3) | Redução |
|---|---|---|---|
| Pico RSS cache miss (`uf="SP", years=2023`) | 11.240 MB | **713 MB** | **-94%** |
| Duração cache miss | 215 s | 24 s | -89% |
| Pico RSS cache hit | 2.627 MB | **487 MB** | -81% |
| Layout do cache | `year=YYYY/data.parquet` (nacional) | `year=YYYY/UF=XX/data.parquet` (Hive) | — |

### Fixed — `ImportError: numpy._core.multiarray` em Colab (BUG-2026-05-24-B)

Erro reportado em Colab: `pip install climasus4py` puxava `pandas` 3.x preview que disparava `cannot load module more than once per process` por incompatibilidade ABI com `numpy` pré-carregado. Mitigações:

- Pin conservador em [pyproject.toml](climasus4py/pyproject.toml): `pandas>=2.0,<3.0`, `numpy>=1.26,<3`, `pyarrow>=12.0,<20`. Revisar quando pandas 3.0 sair estável.
- Nova seção **"Notebooks (Colab / Jupyter)"** no [README.md](climasus4py/README.md) explicando o restart de kernel necessário após `pip install` e fornecendo comando alternativo de pin.

### Plumbing

- `_version.py` e `pyproject.toml` em sincronia (`0.2.0a3`).
- Plano formal registrado em [`governanca/3-planos/em-execucao/2026-05-24-py-correcoes-revisao-OWASP-correctness.md`](../governanca/3-planos/em-execucao/2026-05-24-py-correcoes-revisao-OWASP-correctness.md).
- Backlog v2 atualizado em [`governanca/6-instancia/ideias-climasus4py-v2.md`](../governanca/6-instancia/ideias-climasus4py-v2.md) com 7 entradas cobrindo refactor lazy de `sus_climate_inmet`/`sus_fill_gaps`, padronização `rel.query()`, cobertura de testes, fragilidade do `_stage_map` e itens menores agregados.

### Validação

- `pytest tests/` → 504 passed, 16 skipped, 30 warnings (todas intencionais: `DeprecationWarning` para `fix_enc`, `UserWarning` para INMET sem `uf` e census legacy).
- `ruff check` → All checks passed.
- Smoke `import climasus4py as cs` → OK.

---

## [0.2.0a1] - 2026-05-08

> **Renumeração:** as tags `v0.3.0`, `v0.3.1` e `v0.3.2` **nunca foram publicadas**.
> Esta release reseta a numeração para `v0.2.0a1` (PEP 440 alpha 1) — primeiro
> Alpha público da nova arquitetura paridade `climasus4r` legacy. Os entries
> históricos `[0.3.x]` abaixo são preservados como registro técnico do trabalho
> que precedeu este Alpha.

### Fixed — Bioclimatic indicators (BUG-01..BUG-04)

- **BUG-01 — `consecutive_hot_days` é agora um run length verdadeiro** (não
  uma janela 7-day). Reescrito com técnica gaps-and-islands em CTE: para
  cada dia hot, retorna o número de dias consecutivos terminando hoje
  (compatível com convenção ETCCDI CDD).
- **BUG-02 — `heat_wave` flagga TODOS os dias do episódio.** A versão
  anterior usava `LAG(Tmax,1)` + `LAG(Tmax,2)` e perdia os 2 primeiros
  dias de cada onda — viés de 67% em ondas de 3 dias. Reescrito para
  contar o tamanho da run completa via `COUNT(*) OVER (PARTITION BY
  run_id)` e flaggar todos os dias quando ≥ 3.
- **BUG-03 — `wbgt` (Wet-Bulb Globe Temperature) implementado.** O
  docstring documentava WBGT (Liljegren 2008), mas a chave não existia
  em `_INDICATOR_DEFS`. Adicionada implementação simplificada outdoor
  (`0.67 * Twb + 0.33 * Tdb`) com Twb estimado de T+RH via Stull (2011).
  Paridade com `climasus4r::sus_climate_compute_indicators`.
- **BUG-04 — `heat_index` com guarda de domínio.** A regressão Rothfusz
  é definida apenas para T ≥ 27°C e RH ≥ 40%. Fora desse domínio o
  polinômio retornava valores < T (biologicamente absurdo). Agora
  retorna `NULL` fora do domínio.

### Fixed — Cache and security (BUG-05..BUG-06)

- **BUG-05 — `hashlib.md5` com `usedforsecurity=False`.** Compatibilidade
  com Python ≥ 3.9 em modo FIPS (que rejeitava MD5 sem essa flag).
- **BUG-06 — Cache de modelos XGBoost com validação de features.** Nome
  do arquivo agora inclui versão do pacote + hash das `feature_cols`.
  Mudança em `_engineer_features` invalida automaticamente o cache.
  Carga adicional valida `n_features_in_` como segunda barreira.

### Fixed — Release plumbing (BUG-07)

- **BUG-07 — `_version.py` e `pyproject.toml` em sincronia.** Wheel
  publicado com metadado consistente com `climasus4py.__version__`.

### Notes

- `consecutive_hot_days` e `heat_wave` **não existem no `climasus4r`
  legacy** — são adições do `climasus4py` registradas como divergência
  intencional em [`DECISOES.md`](../governanca/6-instancia/DECISOES.md).

---

## [0.3.2] - 2026-05-09

### Fixed

- `mypy --ignore-missing-imports` agora passa com 0 erros (eram 14).
- `utils/data.py`: `load_json` tipado como `dict[str, Any]` (era `Any`).
- `utils/cid.py`: variáveis `raw` anotadas como `list[str]` para resolver
  cascata de `Any | None` em `expand_cid_ranges`.
- `core/variables.py`: `cast(dict[str, Any], ...)` em `presets` e `patterns`
  para resolver `object not indexable` e `in object`.
- `utils/quality.py`: `cast(int, fetchone_scalar(...))` em `total_rows`;
  `assert isinstance(data, pd.DataFrame)` no branch `else` para narrowing.
- `io/materialize.py`: `cast(int, fetchone_scalar(...))` em `count` para
  resolver comparação `int <= object`.
- `enrichment/climate.py`: `fetchone()[0]` substituído por `fetchone_scalar()`
  para evitar indexação de `tuple | None`.
- `core/sus_sql.py`: `cast(DuckDBPyRelation, rel)` antes de `register_relation`
  para resolver invariância de `str | DuckDBPyRelation`.
- `core/importer.py:525`: sentinela `[None]` tipado como `list[int | None]`.
- `core/engine.py`: `read_parquets` assinatura mudada para `Sequence[str | Path]`
  (covariante) — resolve `list[Path]` vs `list[str | Path]`.

### Changed

- CI já executa `mypy climasus4py --ignore-missing-imports` como step obrigatório.
- `pyproject.toml`: adicionado `[[tool.mypy.overrides]]` para `requests`
  (`ignore_missing_imports = true`) — suprime `[import-untyped]` em `climate_inmet.py`.

### Note

- Sem mudança de API pública. Migração de v0.3.1 → v0.3.2 é transparente.
- `tests/test_spatial_enrichment.py`: removido — testava o modo eager `shapefile=`
  descontinuado no v0.3.1 (mesmo padrão de `test_climate_enrichment.py`);
  cobertura lazy mantida em `tests/test_lazy_enrichments.py`.

## [0.3.1] - 2026-05-07

### Fixed

- `sus_climate`: restaurado contrato lazy estrito — retorna `DuckDBPyRelation` e faz JOIN automático com `climasus-data/inmet_observations_*.parquet` via DuckDB SQL. Modo eager `climate=<DataFrame>` introduzido durante o porte do Sub-plano B foi removido (não tem equivalente no `climasus4r` legacy).
- `sus_spatial_join`: restaurado contrato lazy estrito — retorna `DuckDBPyRelation` e faz JOIN automático com `climasus-data/spatial/municipalities.parquet`. Modo eager `shapefile=<GeoDataFrame>` removido pelo mesmo motivo.
- `sus_data_aggregate`: corrigido deadlock de recurso DuckDB ao usar `rel.query()` em vez de `conn.register()` + `conn.sql()`.
- `materialize(how="pandas")`: removido auto-upgrade implícito para `GeoDataFrame` — use `how="geopandas"` explicitamente.
- `core/engine.py`: adicionados `TYPE_CHECKING` imports de `pd` e `pa` para resolver `F821`.
- Importações reordenadas e deduplicadas em todo o pacote (`F401`, `I001`).
- Exceções relançadas com `raise ... from err` (`B904`).
- `zip()` sem `strict=` adicionado `strict=False` (`B905`).
- Linhas longas (E501) anotadas com `# noqa: E501` ou encurtadas.
- `_migrate_layout.py` movido para `tools/_layout_migration_2026-05-06.py`.

### Removed

- **BREAKING (interno, v0.3.0 nunca foi publicada):** `sus_climate(climate=...)` e `sus_spatial_join(shapefile=...)` foram removidos. Esses parâmetros foram introduzidos no porte do Sub-plano B mas violavam o princípio "lazy ponta a ponta" e não tinham equivalente no `climasus4r` legacy.
- `tests/test_climate_enrichment.py`: removido (testava apenas o modo eager descontinuado; cobertura lazy mantida em `tests/test_lazy_enrichments.py`).

## [0.3.0] - 2026-05-06

### Added — Parâmetros avançados em `sus_filter` (Sub-plano D)

- `match_type="starts_with"|"exact"` — controle de precisão no match CID-10; `"exact"` exige código completo (ex: `"J189"`), `"starts_with"` (padrão) usa prefixo de 3 caracteres. Paridade: `climasus4r::sus_data_filter_cid(match_type=)`
- `education` — filtra por escolaridade; auto-detecta coluna entre `education`, `education_2010`, `ESC`, `ESC2010`. Paridade: `climasus4r::sus_data_filter_demographics(education=)`
- `city` — filtra por nome de município; resolve para código IBGE via `climasus-data/spatial/municipalities.parquet`; emite `UserWarning` quando o nome casa múltiplos municípios. Paridade: `climasus4r::sus_data_filter_demographics(city=)`
- `drop_ignored=False` — quando `True`, remove linhas com valores codificados como ignorado/desconhecido (`9`, `99`, `Ignorado`, `Unknown`, etc.) em colunas demográficas detectáveis. Paridade: `climasus4r::sus_data_filter_demographics(drop_ignored=)`

### Added — Metadados de pipeline e grupos de doenças (Sub-plano C)

- `sus_meta(rel, field=None, add_history=None)` — introspecção de metadados da relação DuckDB (sistema, etapa, tipo, histórico). Paridade: `climasus4r::sus_meta()`
- `list_disease_groups(climate_sensitive_only, lang)` — lista grupos de doenças de `climasus-data/disease_groups/core.json` + `climate_sensitive.json` com suporte a PT/EN/ES. Paridade: `climasus4r::sus_list_disease_groups()`
- `get_disease_group_details(group_name, lang)` — detalhes completos de um grupo (label, description, codes, climate_sensitive, climate_factors). Paridade: `climasus4r::sus_disease_group_details()`
- `_stage.py` expandido: `_stage_map` agora armazena `{stage, system, type, history}`; `set_stage()` aceita `system=` e `rel_type=`; nova `get_meta()` retorna o dict completo.

### Added — Suíte climática avançada (paridade com `climasus4r` legacy)

- `sus_climate_aggregate` — agregação climática lazy em DuckDB SQL (mensal/sazonal/anual, 10 estatísticas)
- `sus_climate_compute_indicators` — 8 indicadores bioclimáticos via SQL macros (HI, THI, AT, VP, DPD, DTR, CHD, HWD) — fontes: Rothfusz (1990), Thom (1959), Magnus-Tetens
- `sus_climate_fill_inmet` — imputação por XGBoost por estação (opt-in `pip install climasus4py[xgboost]`); fallback linear com `UserWarning`; cache de modelos em `~/.climasus4py/models/`
- `sus_climate_plot_fill` — visualização ggplot do antes/depois via plotnine (opt-in `pip install climasus4py[plot]`)
- Extras opcionais `[xgboost]` e `[plot]` declarados em `pyproject.toml`

### BREAKING CHANGES — Paridade com `climasus4r` legacy

Os nomes públicos de 9 funções mudaram para alinhar com o pacote R `climasus4r`. Script de migração automática: `tools/migrate-from-v0.2.py`.

| v0.2.x (antigo) | v0.3.0 (novo) |
|-----------------|---------------|
| `sus_import` | `sus_data_import` |
| `sus_clean` | `sus_data_clean_encoding` |
| `sus_standardize` | `sus_data_standardize` |
| `sus_variables` | `sus_data_create_variables` |
| `sus_aggregate` | `sus_data_aggregate` |
| `sus_read` | `sus_data_read` |
| `sus_quality` | `sus_data_quality_report` |
| `sus_spatial` | `sus_spatial_join` |
| `sus_chat_ai` | `sus_chat` (renomeado em 2026-05-05) |

Sem aliases de deprecação — código que usa os nomes antigos quebra com `AttributeError`.

## [0.2.1] - 2026-05-05

### Corrigido
- **Hotfix semana epidemiológica (SVS):** `sus_aggregate` e `sus_variables` agora usam formato SVS (`"%U"` domingo-primeiro, ex: `02/2023`) por padrão; formato ISO legado preservado via `week_format="iso"`.
- **Aviso de dados sintéticos no censo 2010:** `sus_census(year=2010)` emite `UserWarning` orientando uso de dados IBGE oficiais.
- **`fetchone_scalar` helper:** nova função utilitária que evita `AttributeError` em relações DuckDB vazias em `quality.py` e `materialize.py`.
- **Escrita atômica de Parquet:** `_write_parquet_atomic` em `importer.py` evita corrupção de cache em workers paralelos (escreve em `.tmp_<hex>` e renomeia).

### Segurança (OWASP)
- **Injeção SQL prevenida em `filter.py`:** parâmetros `race`, `uf` e `municipality` agora usam `sql_string()` para escaping correto.
- **Path traversal prevenido em `importer.py`:** URLs com `%2e%2e` ou `../` são rejeitadas via `unquote()` + `Path.resolve()` antes do cache.
- Testes de regressão de segurança adicionados em `test_filter.py`, `test_importer.py` e `test_guards.py`.

### CI/CD
- `--cov-fail-under=75` adicionado ao gate de cobertura no CI (78% alcançado).
- Step `mypy climasus4py --ignore-missing-imports` adicionado ao CI.
- `publish-pypi.yml` reescrito com Trusted Publisher OIDC, ações SHA-pinadas, ambientes separados (`testpypi` / `pypi`). **Releases de tag vão apenas para TestPyPI Alpha**; PyPI de produção requer `workflow_dispatch` manual.

### Testes
- +129 novos testes; suite em 343 passed, 1 skipped (excluindo fixtures de dados reais).
- Módulos com cobertura notável: `pipeline.py` 85%, `inmet_parser.py` 92%, `climate_inmet.py` 50%+, `importer.py` 71%.

### Benchmarks
- `bench_lazy_10m.py` adicionado: benchmark opt-in de RAM para 10M linhas SIM-DO no pipeline lazy (alvo ≤ 500 MB).

## [0.2.0] - 2026-05-02

- Remove `sus_to_lazy` do contrato publico.
- Adiciona `sus_read()` para Parquet/GeoParquet lazy.
- Adiciona `sus_sql()` como entrada SQL e transformacao com `{data}`.
- Adiciona `materialize(how=...)` como saida explicita em RAM.
- Reescreve `sus_spatial`, `sus_census` e `sus_climate` para joins SQL lazy.
- Reescreve `sus_fill_gaps` com `linear` e `locf` lazy; `spline` e `xgboost` ficam opt-in com warning de RAM.
- Adiciona stage tracking, guards sem rotas de contorno e testes do novo contrato.
- Remove `collect_arrow` da API publica; use `materialize(how="pyarrow")`.
- `sus_export()` passa a aceitar apenas `DuckDBPyRelation`; para DataFrame, use APIs nativas de pandas/pyarrow.
- Adiciona testes de integracao com `fixture_reais` gerados pelo `climasus4r`.
- Integra com `climasus-data>=1.1.0` para assets em `assets/spatial/`, `assets/climate/` e `assets/census/`.
- `sus_spatial()` aceita `spatial_path` para geometria customizada em Parquet/GeoParquet.
