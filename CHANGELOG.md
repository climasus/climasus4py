# Changelog

## [0.3.1] - 2026-05-07

### Fixed

- `sus_climate`: restaurada compatibilidade backward com API eager legada — ao passar `climate=<DataFrame>`, materializa e faz merge pandas (retorna `pd.DataFrame`); nova API lazy continua sem mudança.
- `sus_spatial_join`: restaurada compatibilidade backward com API eager legada — ao passar `shapefile=<GeoDataFrame>`, faz merge pandas e retorna `gpd.GeoDataFrame`; nova API lazy continua sem mudança.
- `sus_data_aggregate`: corrigido deadlock de recurso DuckDB ao usar `rel.query()` em vez de `conn.register()` + `conn.sql()`.
- `materialize(how="pandas")`: removido auto-upgrade implícito para `GeoDataFrame` — use `how="geopandas"` explicitamente.
- `core/engine.py`: adicionados `TYPE_CHECKING` imports de `pd` e `pa` para resolver `F821`.
- Importações reordenadas e deduplicadas em todo o pacote (`F401`, `I001`).
- Exceções relançadas com `raise ... from err` (`B904`).
- `zip()` sem `strict=` adicionado `strict=False` (`B905`).
- Linhas longas (E501) anotadas com `# noqa: E501` ou encurtadas.
- `_migrate_layout.py` movido para `tools/_layout_migration_2026-05-06.py`.

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
