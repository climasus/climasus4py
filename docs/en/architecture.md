# Architecture

This page describes the internal structure of `climasus4py`: how DuckDB is used, the cache system, the SQL fast path, and the module organisation.

---

## Pipeline flow

```
DATASUS FTP
    │
    ▼
sus_import()  ──── Download .dbc ──── Convert to .parquet ──── Local cache
    │                                                                    │
    │◄─────────────────────── cache hit ────────────────────────────────────────────────┘
    │
    ▼ DuckDBPyRelation (lazy)
sus_clean()       — fix encoding, types, DATASUS date formats
    │
    ▼ DuckDBPyRelation (lazy)
sus_standardize() — rename columns (DATASUS codes → readable, in pt/en/es)
    │
    ▼ DuckDBPyRelation (lazy)
sus_filter()      — filter by ICD-10, age, sex, municipality, date range
    │
    ▼ DuckDBPyRelation (lazy)
sus_variables()   — age bands, epi week, season, quarter
    │
    ▼ DuckDBPyRelation (lazy)
sus_aggregate()   — GROUP BY time + geography + extras
    │
    ▼ materialisation (only here)
sus_export()  /  .df()  /  .fetchall()
```

No data rows are read into RAM until the materialisation step. DuckDB executes all transformations as composed SQL queries over the Parquet files on disk.

---

## SQL fast path

When all conditions below are met, `sus_pipeline` builds **a single SQL CTE query** — no intermediate materialisation between stages:

| Condition | Accepted value |
|-----------|---------------|
| `age_group` | `None` |
| `epi_week` | `False` |
| `time` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `"state"` or `"municipality"` |

The fast path is equivalent to the `rc_a` path in the R package. For all other cases the staged pipeline is triggered automatically. The user does not need to choose.

---

## Cache system

DBC files downloaded from the DATASUS FTP are converted to Parquet and cached locally:

```
dados/cache/
└── SIM-DO/
    ├── SP_2022_all.parquet
    ├── SP_2023_all.parquet
    ├── RJ_2022_all.parquet
    └── ...
```

File naming convention: `{UF}_{YEAR}_{MONTH_or_all}.parquet`

The default directory is `dados/cache/` (relative to the working directory). Override with the `cache_dir` parameter in `sus_import` / `sus_pipeline`.

```python
cs.sus_cache_info()   # list cached files with size and timestamp
cs.sus_cache_clear()  # remove all cached files
```

---

## DBC reader chain

`sus_import` tries to read `.dbc` files with the following fallback chain:

1. **`readdbc`** (pure Python, no compilation — `climasus_readdbc_py`)
2. **`pyreaddbc`** (optional C bindings)
3. **`pysus`** (alternative fallback)
4. **`dbc2dbf` CLI** (external tool, last resort)

---

## DATASUS type coercion

Before saving to Parquet, `sus_import` applies automatic type coercions:

| Column type | Example | Transformation |
|-------------|---------|----------------|
| Dates | `DTOBITO = "12032022"` | `pd.to_datetime(..., format="%d%m%Y")` |
| Numeric | `CONTADOR`, `PESO` | `pd.to_numeric(..., errors="coerce")` |
| String | All others | `.str.strip()`, `""` → `None` |

---

## Automatic column detection

`sus_filter`, `sus_aggregate`, and `sus_variables` do not depend on fixed column names. Functions in `utils/data.py` detect the correct column automatically:

| Function | What it detects |
|----------|----------------|
| `detect_date_column` | `DTOBITO`, `death_date`, `date`, `DTNASC`… |
| `detect_geo_column` | `CODMUNRES`, `municipality_code`, `UF`, `state`… |
| `detect_age_column` | `IDADE`, `age`, `age_years`… |
| `detect_cause_column` | `CAUSABAS`, `cause_icd10`, `diag_princ`… |
| `detect_sex_column` | `SEXO`, `sex`, `gender`… |

This allows the pipeline to work with raw DATASUS columns **and** with columns already standardised by `sus_standardize`.

---

## Module structure

```
src/climasus/
├── __init__.py          # public API — all exports
├── _version.py
├── core/
│   ├── pipeline.py      # sus_pipeline — orchestration + fast path
│   ├── importer.py      # sus_import — FTP download + parquet cache
│   ├── clean.py         # sus_clean — encoding, types, dates
│   ├── standardize.py   # sus_standardize — rename columns (pt/en/es)
│   ├── filter.py        # sus_filter — ICD-10, age, sex, geo, date
│   ├── variables.py     # sus_variables — age_group, epi_week, season
│   ├── aggregate.py     # sus_aggregate — GROUP BY time + geo
│   └── engine.py        # DuckDB connection + helpers (collect, schema)
├── enrichment/
│   ├── climate.py       # sus_climate — health + climate (INMET) join
│   ├── spatial.py       # sus_spatial — geospatial join
│   ├── census.py        # sus_census — census variables
│   └── fill_gaps.py     # sus_fill_gaps — ML imputation (xgboost)
├── io/
│   ├── export.py        # sus_export — parquet / csv / xlsx
│   └── cache.py         # sus_cache_info, sus_cache_clear
└── utils/
    ├── cid.py           # expand_cid_ranges, codes_for_groups
    ├── data.py          # detect_*, resolve_uf, decode_age_sql
    ├── encoding.py      # DATASUS encoding fixes (cp1252/latin-1)
    ├── explore.py       # sus_explore — exploratory summary
    └── quality.py       # sus_quality — quality report
```
