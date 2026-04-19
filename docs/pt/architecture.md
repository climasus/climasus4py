# Arquitetura

Esta página descreve a estrutura interna do `climasus4py`: como o DuckDB é usado, o sistema de cache, o fast path SQL e a organização dos módulos.

---

## Visão geral do fluxo

```
DATASUS FTP
    │
    ▼
sus_import()  ──── Download .dbc ──── Converte para .parquet ──── Cache local
    │                                                                    │
    │◄───────────────────────── cache hit ──────────────────────────────┘
    │
    ▼ DuckDBPyRelation (lazy)
sus_clean()       — corrige encoding, tipos, datas DATASUS
    │
    ▼ DuckDBPyRelation (lazy)
sus_standardize() — renomeia colunas (DATASUS → legível, em pt/en/es)
    │
    ▼ DuckDBPyRelation (lazy)
sus_filter()      — filtragem por CID-10, faixa etária, sexo, município, data
    │
    ▼ DuckDBPyRelation (lazy)
sus_variables()   — faixas etárias, semana epi, estação, trimestre
    │
    ▼ DuckDBPyRelation (lazy)
sus_aggregate()   — GROUP BY tempo + geografia + extras
    │
    ▼ materialização (apenas aqui)
sus_export()  /  .df()  /  .fetchall()
```

Nenhuma linha de dado é lida para memória RAM até a etapa de materialização. DuckDB executa todas as transformações como consultas SQL compostas sobre os arquivos Parquet em disco.

---

## Fast path SQL

Quando as condições abaixo são todas satisfeitas, `sus_pipeline` constrói **uma única query SQL com CTE** — sem materialização intermediária entre etapas:

| Condição | Valor aceito |
|----------|-------------|
| `age_group` | `None` |
| `epi_week` | `False` |
| `time` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `"state"` ou `"municipality"` |

O fast path é equivalente ao `rc_a` do pacote R. Para os demais casos, o pipeline por etapas é acionado automaticamente (fallback). O usuário não precisa escolher — o código decide em tempo de execução.

---

## Sistema de cache

Os arquivos DBC baixados do FTP do DATASUS são convertidos para Parquet e armazenados localmente:

```
dados/cache/
└── SIM-DO/
    ├── SP_2022_all.parquet
    ├── SP_2023_all.parquet
    ├── RJ_2022_all.parquet
    └── ...
```

Convenção de nome: `{UF}_{ANO}_{MÊS_ou_all}.parquet`

O diretório padrão é `dados/cache/` (relativo ao diretório de trabalho). Pode ser sobrescrito pelo parâmetro `cache_dir` em `sus_import` e `sus_pipeline`, ou globalmente por variável de ambiente.

```python
cs.sus_cache_info()   # lista arquivos em cache com tamanho e data
cs.sus_cache_clear()  # remove todos os arquivos de cache
```

---

## Cadeia de leitura DBC

`sus_import` tenta ler os arquivos `.dbc` com a seguinte cadeia de fallback:

1. **`readdbc`** (pure Python, sem compilação — `climasus_readdbc_py`)
2. **`pyreaddbc`** (bindings C opcionais)
3. **`pysus`** (fallback alternativo)
4. **`dbc2dbf` CLI** (ferramenta externa, último recurso)

---

## Coerção de tipos DATASUS

Antes de salvar em Parquet, `sus_import` aplica coerções automáticas:

| Tipo de coluna | Exemplo | Transformação |
|----------------|---------|---------------|
| Datas | `DTOBITO = "12032022"` | `pd.to_datetime(..., format="%d%m%Y")` |
| Numérico | `CONTADOR`, `PESO` | `pd.to_numeric(..., errors="coerce")` |
| String | Todos os demais | `.str.strip()`, `""` → `None` |

---

## Detecção automática de colunas

`sus_filter`, `sus_aggregate` e `sus_variables` não dependem de nomes fixos de colunas. Funções em `utils/data.py` detectam automaticamente:

| Função | O que detecta |
|--------|--------------|
| `detect_date_column` | `DTOBITO`, `death_date`, `date`, `DTNASC`… |
| `detect_geo_column` | `CODMUNRES`, `municipality_code`, `UF`, `state`… |
| `detect_age_column` | `IDADE`, `age`, `age_years`… |
| `detect_cause_column` | `CAUSABAS`, `cause_icd10`, `diag_princ`… |
| `detect_sex_column` | `SEXO`, `sex`, `gender`… |

Isso permite que o pipeline funcione com colunas brutas DATASUS **e** com colunas já padronizadas por `sus_standardize`.

---

## Estrutura de módulos

```
src/climasus/
├── __init__.py          # API pública — todos os exports
├── _version.py
├── core/
│   ├── pipeline.py      # sus_pipeline — orquestração + fast path
│   ├── importer.py      # sus_import — download FTP + cache parquet
│   ├── clean.py         # sus_clean — encoding, tipos, datas
│   ├── standardize.py   # sus_standardize — renomeia colunas (pt/en/es)
│   ├── filter.py        # sus_filter — CID-10, idade, sexo, geo, data
│   ├── variables.py     # sus_variables — age_group, epi_week, season
│   ├── aggregate.py     # sus_aggregate — GROUP BY tempo + geo
│   └── engine.py        # conexão DuckDB + helpers (collect, schema)
├── enrichment/
│   ├── climate.py       # sus_climate — join saúde + clima (INMET)
│   ├── spatial.py       # sus_spatial — join geoespacial
│   ├── census.py        # sus_census — variáveis do censo
│   └── fill_gaps.py     # sus_fill_gaps — imputação ML (xgboost)
├── io/
│   ├── export.py        # sus_export — parquet / csv / xlsx
│   └── cache.py         # sus_cache_info, sus_cache_clear
└── utils/
    ├── cid.py           # expand_cid_ranges, codes_for_groups
    ├── data.py          # detect_*, resolve_uf, decode_age_sql
    ├── encoding.py      # correção encoding DATASUS (cp1252/latin-1)
    ├── explore.py       # sus_explore — resumo exploratório
    └── quality.py       # sus_quality — relatório de qualidade
```

---

## Metadados externos (`climasus-data`)

O pacote depende de metadados mantidos no repositório separado `climasus-data`:

```
climasus-data/
├── disease_groups/
│   ├── core.json           # grupos de doenças → códigos CID-10
│   └── climate_sensitive.json
├── dictionaries/           # mapeamentos DATASUS → nomes legíveis
└── geo/                    # códigos de UF, municípios, regiões
```

O caminho é configurado na variável `CLIMASUS_DATA_DIR` ou usando `cs.update_climasus_data()`.

---

## Paralelismo

O `sus_import` suporta download paralelo de múltiplos arquivos DBC quando `parallel=True` e `workers=N` são passados (via `**kwargs` para `sus_pipeline`). DuckDB já paraleliza internamente a leitura de múltiplos Parquet via `read_parquet([...], union_by_name=True)`.
