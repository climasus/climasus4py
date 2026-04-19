# Getting Started

This guide covers installation, initial setup, and the most common usage patterns — from the full pipeline to the step-by-step API.

---

## 1. Installation

=== "Basic install"

    ```bash
    pip install climasus4py
    ```

=== "With optional extras"

    ```bash
    # Spatial data (geobr, geopandas)
    pip install "climasus4py[spatial]"

    # ML gap filling (xgboost)
    pip install "climasus4py[ml]"

    # Excel export
    pip install "climasus4py[excel]"

    # Everything
    pip install "climasus4py[spatial,ml,excel]"
    ```

!!! info "Python requirement"
    Python 3.10 or higher. The package uses DuckDB internally — no database installation or configuration required.

---

## 2. Set up `climasus-data`

Part of the API depends on local metadata: disease groups (ICD-10), DATASUS column dictionaries, UF and region codes. These metadata are maintained in the [`climasus-data`](https://github.com/climasus/climasus-data) repository.

```python
import climasus as cs

# Download/update metadata to the default directory (~/.climasus_data/)
cs.update_climasus_data()
```

Or specify a custom directory:

```bash
git clone https://github.com/climasus/climasus-data.git /my/local/path
```

```python
import os
os.environ["CLIMASUS_DATA_DIR"] = "/my/local/path/climasus-data"
```

!!! warning
    Without the metadata configured, functions like `sus_filter(groups=...)` and `sus_standardize()` will not work correctly.

---

## 3. Full pipeline (`sus_pipeline`)

`sus_pipeline` is the main entry point: it automatically downloads DBC files from DATASUS, converts them to Parquet (cache), and runs the full clean → standardise → filter → variables → aggregate chain.

```python
import climasus as cs

result = cs.sus_pipeline(
    system="SIM-DO",           # DATASUS system
    uf="SP",                   # State (or list, or "all", or region)
    year=[2021, 2022, 2023],   # Year(s)
    groups="respiratory",      # Disease group (optional)
    age_min=18,                # Minimum age filter (optional)
    age_max=80,                # Maximum age filter (optional)
    time="month",              # Temporal granularity
    geo="state",               # Geographic level
    lang="en",                 # Output column language
)

# Lazy relation — materialises only here:
df = result.df()
print(df.shape)
print(df.head())
```

### `uf` options

```python
# Single state
sus_pipeline("SIM-DO", uf="SP", year=2023)

# Multiple states
sus_pipeline("SIM-DO", uf=["SP", "RJ", "MG"], year=2023)

# All states nationwide
sus_pipeline("SIM-DO", uf="all", year=2023)

# By region
sus_pipeline("SIM-DO", uf="Southeast", year=2023)
```

### Save results automatically

```python
sus_pipeline(
    "SIM-DO", "SP", 2023,
    groups="dengue",
    output="output/dengue_sp_2023.parquet",  # .parquet, .csv, or .xlsx
)
```

---

## 4. Step-by-step API

For finer-grained control, each pipeline stage can be run separately. All functions return `duckdb.DuckDBPyRelation` — the relation stays lazy.

```python
import climasus as cs

# 1. Import (download + cache + lazy read)
rel = cs.sus_import("SIM-DO", uf="RJ", year=[2022, 2023])
print(f"Columns: {rel.columns}")

# 2. Clean encoding and types (DATASUS string/numeric/date fields)
rel = cs.sus_clean(rel)

# 3. Standardise column names (DATASUS codes → readable names)
rel = cs.sus_standardize(rel, lang="en", system="SIM-DO")

# 4. Filter — by named ICD-10 groups, direct codes, age range, sex
rel = cs.sus_filter(
    rel,
    groups=["dengue", "zika_chikungunya"],
    age_min=0,
    age_max=14,     # children
    sex="F",
)

# 5. Create derived variables
rel = cs.sus_variables(
    rel,
    age_group="who",  # WHO age bands
    epi_week=True,
    season=True,      # Southern Hemisphere seasons
)

# 6. Aggregate
rel = cs.sus_aggregate(
    rel,
    time="month",
    geo="municipality",
    extra_groups=["sex", "age_group"],
)

# 7. Materialise
df = rel.df()

# 8. Export
cs.sus_export(rel, "output/result.parquet")
```

---

## 5. Advanced filters

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", uf="SP", year=2023)
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, lang="en", system="SIM-DO")

# Filter by direct ICD-10 codes (including ranges)
rel = cs.sus_filter(rel, codes=["A90", "A91", "B50-B54"])

# Filter by municipality (IBGE 7-digit code)
rel = cs.sus_filter(rel, municipality=["3550308"])  # São Paulo city

# Filter by date range
rel = cs.sus_filter(rel, date_start="2023-06-01", date_end="2023-12-31")

# Count without full materialisation
print(f"Records: {rel.count('*').fetchone()[0]:,}")
```

---

## 6. Climate enrichment

```python
import climasus as cs
import pandas as pd

# Climate data from INMET (must have: municipality_code, date + variable columns)
climate_df = pd.read_parquet("data/inmet_sp_2023.parquet")

rel = cs.sus_pipeline("SIM-DO", "SP", 2023, geo="municipality")

# Join with climate data + 7 and 14-day lags
enriched = cs.sus_climate(
    rel,
    climate_df,
    lags=[7, 14],  # adds temp_lag7d, prec_lag14d, etc.
)
print(enriched.filter(like="_lag7d").columns.tolist())
```

---

## Quick parameter reference

| Parameter | Type | Accepted values |
|-----------|------|-----------------|
| `system` | `str` | `"SIM-DO"`, `"SINASC"`, `"SIH"`, `"SIA"` |
| `uf` | `str` or `list[str]` | `"SP"`, `["SP", "RJ"]`, `"all"`, `"Southeast"` |
| `year` | `int` or `list[int]` | `2023`, `[2020, 2021, 2022]` |
| `time` | `str` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `str` | `"state"`, `"municipality"`, `"region"`, `"country"` |
| `lang` | `str` | `"pt"`, `"en"`, `"es"` |
| `groups` | `str` or `list[str]` | See [disease groups table](index.md#available-disease-groups) |
| `age_group` | `str` or `list[int]` | `"who"`, `"decadal"`, `[0, 18, 65]` |
```

You can also set `CLIMASUS_DATA_DIR`.

## 3. Full pipeline

```python
import climasus as cs

rel = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2021, 2022, 2023],
    lang="en",
    groups=["dengue"],
    time="month",
    geo="state",
)

print(rel.columns)
pdf = rel.df()
```

## 4. Stage-by-stage usage

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", "SP", [2022, 2023])
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, lang="en", system="SIM-DO")
rel = cs.sus_filter(rel, codes=["A90", "A91"], age_min=0, age_max=80)
rel = cs.sus_variables(rel, age_group="who", epi_week=True)
rel = cs.sus_aggregate(rel, time="month", geo="state")
cs.sus_export(rel, "output/dengue_sp.parquet")
```
