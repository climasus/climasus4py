# climasus4py

[![PyPI](https://img.shields.io/pypi/v/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Fast SUS and climate workflows for Brazil in Python.

__DOCUMENTATION AGE__ : [https://climasus.github.io/climasus4py_documentation/](https://climasus.github.io/climasus4py_documentation/)

## Features

- DuckDB lazy pipeline (`sus_import -> sus_clean -> sus_standardize -> sus_filter -> sus_variables -> sus_aggregate`)
- Local Parquet cache for reproducible runs
- Optional enrichment: climate, census, and spatial joins
- Trilingual docs (PT, EN, ES) with MkDocs


