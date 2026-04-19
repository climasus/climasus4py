# climasus4py — English Documentation

`climasus4py` is a Python package for analysing Brazilian SUS (public health system) microdata, with built-in support for INMET climate data enrichment. It uses DuckDB as the query engine, keeping the entire pipeline **lazy** until final materialisation — allowing you to process full national datasets with low memory usage.

---

## What you will find here

<div class="grid cards" markdown>

- :rocket: **[Getting Started](getting-started.md)**

    Installation, `climasus-data` setup, and your first complete pipeline step by step.

- :books: **[API Reference](../reference/)**

    Auto-generated documentation for all public functions, with parameters, types, and examples.

- :building_construction: **[Architecture](architecture.md)**

    How DuckDB lazy evaluation works, the fast path, the cache system, and the module structure.

- :wrench: **[Development](development.md)**

    Local setup, running tests, benchmarks, and contributing to the project.

</div>

---

## Quick example: respiratory mortality in São Paulo (2020–2023)

```python
import climasus as cs

# Full pipeline — auto-download + cache + filter + aggregate
result = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2020, 2021, 2022, 2023],
    groups="respiratory",      # ICD-10 group: J00-J99
    age_min=18,                # adults only
    time="month",              # monthly aggregation
    geo="state",               # state level
    lang="en",                 # English column names
)

# The relation is lazy — materialises only here:
df = result.df()
print(df.head(10))
```

---

## Available disease groups

| Group | Description |
|-------|-------------|
| `respiratory` | Respiratory diseases (J00–J99) |
| `cardiovascular` | Cardiovascular diseases |
| `dengue` | Dengue fever (A90, A91) |
| `covid19` | COVID-19 (U07.1, U07.2) |
| `diabetes` | Diabetes mellitus (E10–E14) |
| `neoplasms` | Neoplasms (C00–D48) |
| `external_causes` | External causes (V01–Y98) |
| `maternal_causes` | Maternal causes (O00–O99) |
| `malaria` | Malaria (B50–B54) |
| `tuberculosis_respiratory` | Pulmonary tuberculosis |
| `zika_chikungunya` | Zika and Chikungunya |
| `climate_sensitive` | Climate-sensitive disease bundle |

---

## Supported DATASUS systems

| Code | System |
|------|--------|
| `SIM-DO` | Death Certificates (mortality) |
| `SINASC` | Live Births |
| `SIH` | Hospital Admissions |
| `SIA` | Outpatient Visits |
