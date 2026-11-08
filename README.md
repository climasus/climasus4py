# climasus4py

[![PyPI](https://img.shields.io/pypi/v/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![Python Versions](https://img.shields.io/pypi/pyversions/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Fast SUS and climate data workflows for Brazil - Python edition.

Documentation: https://climasus.github.io/climasus4py

## Installation

Requires **Python 3.12 or newer**. Install the latest version from GitHub with one
command, which also installs **climasus-data** (the dictionaries and metadata the
package reads):

```bash
pip install "climasus-data @ https://github.com/climasus/climasus-data/archive/refs/heads/main.zip" "climasus4py[all] @ https://github.com/climasus/climasus4py/archive/refs/heads/main.zip"
```

Git is not required. The releases on PyPI are older than this code (climasus-data
0.2.0 predates the metadata it reads), so install from GitHub as above.

### Notebooks (Colab / Jupyter)

Após `pip install`, **reinicie o kernel** antes do primeiro `import climasus4py`.
O Colab pré-carrega `numpy`/`pandas` na inicialização do runtime; se o `pip` faz
upgrade dessas dependências in-place, o processo Python fica com dois conjuntos
de extensões C carregadas e o import falha com
`ImportError: numpy._core.multiarray failed to import` /
`cannot load module more than once per process`.

No Colab: `Runtime → Restart runtime` (atalho `Ctrl+M .`). No JupyterLab:
`Kernel → Restart Kernel`. Em caso de problema persistente, pinar as versões
antes da instalação resolve:

```bash
pip install "numpy<3" "pandas<3"
```

and then the installation command above.

## Quick Example

```python
import climasus4py as cs

result = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2021, 2022, 2023],
    lang="en",
    groups=["dengue"],
    time="month",
    geo="state",
)

print(result.df().head())
```

## Step-by-step Example

```python
import climasus4py as cs

# 1. Import and cache
x = cs.sus_data_import("SIM-DO", "SP", [2021, 2022])

# 2. Clean
x = cs.sus_data_clean_encoding(x)

# 3. Standardize
x = cs.sus_data_standardize(x, lang="en")

# 4. Filter
x = cs.sus_filter(x, groups=["dengue"], age_min=0, age_max=80)

# 5. Create variables
x = cs.sus_data_create_variables(x, age_group="who", epi_week=True)

# 6. Aggregate
x = cs.sus_data_aggregate(x, time="month", geo="state")

# 7. Export
cs.sus_export(x, "output/dengue_sp.parquet")
```

To preserve the original DATASUS `.dbc` files for audit or reuse, enable the raw
cache explicitly:

```python
x = cs.sus_data_import(
    "SINAN-DENGUE",
    "SP",
    2024,
    store_raw=True,
    raw_cache_dir="dados/cache/raw",
)
```

## Shared Metadata

climasus4py consumes metadata from `climasus-data` (disease groups, dictionaries,
UFs, regions, DATASUS FTP sources and SINAN disease codes). It is installed by the
command in [Installation](#installation). To get newer metadata, reinstall it:

```bash
pip install --force-reinstall --no-deps "climasus-data @ https://github.com/climasus/climasus-data/archive/refs/heads/main.zip"
```

The larger files (municipality and state polygons, census tables) are
downloaded on first use and kept in `~/.climasus4py_cache`.

## Contributing

- Pull requests and issues are welcome.
- Run tests locally before opening PRs.
- Include a minimal reproducible example for bug reports.

## License

MIT
