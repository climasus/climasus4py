# Changelog

## [Unreleased]

### Added — API pública da cadeia de clima

12 funções que já existiam no pacote passaram a ser exportadas em `climasus4py/__init__.py`. Antes elas só eram alcançáveis por import direto do submódulo (`from climasus4py.enrichment.climate_heatwaves import ...`); `cs.<função>` levantava `AttributeError`. O `__all__` foi de 39 para 51 nomes.

- **Normais climatológicas e anomalias** — `sus_climate_normals()`, `sus_climate_normals_meta()`, `sus_climate_anomaly()`.
- **Ondas de calor e de frio** — `sus_climate_compute_heatwaves()`, `sus_climate_compute_coldwaves()` e os seis auxiliares `hw_get_events()`, `hw_active_days()`, `hw_count_by_year()`, `cw_get_events()`, `cw_active_days()`, `cw_count_by_year()`.
- **Fonte alternativa de chuva** — `sus_climate_uniplu()`.

### Notes — validação da Fase 2 contra o `climasus4r`

Comparação R vs Python com SIM-DO / SP / 2023, parquets gerados pelos dois lados.

**Paridade confirmada.** `sus_climate_normals_meta()` é 100% idêntica (36 × 5, todos os valores). `sus_climate_compute_heatwaves()` e `sus_climate_compute_coldwaves()` produzem resultados idênticos aos do R — 103 e 128 eventos, 14.600 diários, 37 no summary, 309 e 384 dias ativos —, e `hw_active_days`/`hw_count_by_year`/`cw_active_days`/`cw_count_by_year` batem em 100% das colunas. Em `hw_get_events`/`cw_get_events`, 13–14 das 20 colunas são exatamente iguais; as numéricas divergentes são arredondamento float32 vs float64 (|diff| máx. 7,6e-06) e a única divergência real é o contador de `event_id`.

**Divergência de cobertura.** `sus_climate_compute_indicators()` devolve 36 colunas contra 64 do R, com só 15 em comum: faltam 8 índices (`cdd_c`, `gdd_c`, `hdd_c`, `et_c`, `pet_c`, `utci_c`, `wcet_c`, `wct_c`), 30 flags de confiança e 8 classificações — consequência dos parâmetros ausentes na assinatura (`confidence_flags`, `verify_physics`, `region`, `apply_validity_mask`). Em contrapartida calcula 4 que o R não tem (`at_c`, `consecutive_hot_days`, `dpd_c`, `heat_wave`). Registrado como **M12**.

**Bloqueio de nomenclatura (M10).** `sus_climate_compute_heatwaves()` e `sus_climate_compute_coldwaves()` exigem `station_code`, mas a saída canônica usa `wmo_code` (decisão registrada em [0.2.0a4]) — falham com `ValueError: Missing required columns: ['station_code']` e não expõem `station_col` para contornar. Renomear a coluna resolve, então o nome é o único bloqueio; com os seis auxiliares são oito funções afetadas. `sus_climate_anomaly()` sofre a versão branda: tem `station_col`, mas o default está errado para a saída do próprio pacote. Não corrigido aqui — escolher entre reverter para `station_code` (paridade com o R) ou atualizar os consumidores é decisão de paridade.

**Correctness silencioso (M14).** Com `baseline_start="2023"` em vez de `"2023-01-01"`, `sus_climate_compute_heatwaves()` devolve zero eventos sem erro nem aviso; a mesma chamada com data completa devolve 103. O R recusa a entrada malformada com `"No data available in the specified baseline period."`

**Dependências opcionais.** `sus_climate_fill_inmet()` exige o extra `[ml]`/`[xgboost]` e `sus_climate_normals()` o extra `[excel]`. Ambos já estavam declarados no `pyproject.toml` — nenhuma dependência nova foi adicionada. O R depende simetricamente dos pacotes `xgboost` e `readxl`, ausentes no ambiente de teste, então essas duas funções não puderam ser comparadas.

**Fonte externa.** `sus_climate_uniplu()` falha nos dois lados com `HTTP 403` no download do UNIPLU-BR no Zenodo — falhar identicamente confirma problema da fonte, não de código.

### Status de verificação das 13 funções exportadas

| Função | Verificada contra o R? |
|---|---|
| `sus_climate_normals_meta` | Sim — 100% idêntica (36 × 5, todos os valores) |
| `sus_climate_normals` | Sim — 105.438 × 11 nos dois lados, 99,91% das células (ver M22) |
| `sus_climate_anomaly` | Sim — 480 × 16 nos dois lados |
| `sus_climate_compute_heatwaves` | Sim — 103 eventos nos dois lados. **Bloqueada por M10** |
| `sus_climate_compute_coldwaves` | Sim — 128 eventos nos dois lados. **Bloqueada por M10** |
| `hw_active_days`, `hw_count_by_year` | Sim — 100% das colunas idênticas |
| `cw_active_days`, `cw_count_by_year` | Sim — 100% das colunas idênticas |
| `hw_get_events`, `cw_get_events` | Sim — só o contador de `event_id` difere |
| `sus_data_ts_quality` | Sim — mesmos 4 componentes, assinatura idêntica |
| **`sus_climate_uniplu`** | **Não — nunca executada em nenhum dos dois lados** |

`sus_climate_uniplu()` está exportada para ficar alcançável, mas **seu comportamento é desconhecido**: o download do Zenodo retorna `HTTP 403` e não foi possível executá-la nem no R nem no Python. Deve ser revalidada quando a fonte voltar, antes de ser considerada em paridade.

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
