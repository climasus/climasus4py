# Changelog

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
