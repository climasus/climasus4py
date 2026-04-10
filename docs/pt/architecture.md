# Arquitetura

## Visao geral

`climasus4py` usa DuckDB para manter o pipeline lazy ate a materializacao final.

Fluxo principal:

1. Importacao e cache em parquet
2. Limpeza e padronizacao
3. Filtros epidemiologicos
4. Variaveis derivadas
5. Agregacao temporal/geografica
6. Exportacao

## Modulos

- `src/climasus/core`: pipeline e transformacoes centrais
- `src/climasus/io`: exportacao e cache
- `src/climasus/enrichment`: clima, censo, espacial, gap fill
- `src/climasus/utils`: CID, metadados, qualidade e exploracao

## Fast path do pipeline

`su_pipeline` tenta um caminho otimizado (SQL unico) quando:

- `age_group is None`
- `epi_week is False`
- `time` em `year|quarter|month|week|day`
- `geo` em `state|municipality`

Se nao atender, usa o pipeline por etapas.

## Cache local

- Diretorio padrao: `dados/cache/<SYSTEM>/<UF>_<YEAR>_<MONTH|all>.parquet`
- Reutilizacao automatica quando `cache=True`

## Metadados compartilhados

Vem de `climasus-data` (fora do pacote):

- grupos CID
- dicionarios de colunas
- codigos UF e regioes

Sem esse repositorio local, partes da API nao funcionam.
