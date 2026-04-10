# climasus4py

[![PyPI](https://img.shields.io/pypi/v/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Fast SUS and climate workflows for Brazil in Python.
DOCUMENTATION: [https://climasus.github.io/climasus4py_documentation/](https://climasus.github.io/climasus4py_documentation/)
## Features

- DuckDB lazy pipeline (`sus_import -> sus_clean -> sus_standardize -> sus_filter -> sus_variables -> sus_aggregate`)
- Local Parquet cache for reproducible runs
- Optional enrichment: climate, census, and spatial joins
- Trilingual docs (PT, EN, ES) with MkDocs

## Installation

```bash
pip install climasus4py
```

Optional extras:

```bash
pip install climasus4py[datasus]  # PySUS integration
pip install climasus4py[spatial]  # geopandas + geobr
pip install climasus4py[ml]       # xgboost + scikit-learn
pip install climasus4py[excel]    # openpyxl
pip install climasus4py[all]      # all extras
```

## Quick Start

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

print(rel.df().head())
```

## Required Local Metadata (`climasus-data`)

Some features need a local checkout of `climasus-data` (group dictionaries, column translations, metadata exploration).

```python
from climasus import update_climasus_data
update_climasus_data()
```

Or clone manually:

```bash
git clone https://github.com/ClimaHealth/climasus-data.git
```

Set custom location if needed:

```bash
# Windows PowerShell
$env:CLIMASUS_DATA_DIR="C:\path\to\climasus-data"

# Linux/macOS
export CLIMASUS_DATA_DIR=/path/to/climasus-data
```

## Documentation (MkDocs)

- Source: `docs/`
- Config: `mkdocs.yml`

Run locally:

```bash
pip install -r requirements-docs.txt
mkdocs serve
```

Build static site:

```bash
mkdocs build
```

## License

MIT
