# climasus4py

[![PyPI](https://img.shields.io/pypi/v/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![Python Versions](https://img.shields.io/pypi/pyversions/climasus4py.svg)](https://pypi.org/project/climasus4py/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Fast SUS and climate data workflows for Brazil - Python edition.

Documentation: https://climasus.github.io/climasus4py

## Installation

Install from PyPI:

```bash
pip install climasus4py
```

Install with optional extras:

```bash
pip install "climasus4py[all]"
```

Install latest from GitHub:

```bash
pip install climasus4py.git
```

## Quick Example

```python
import climasus as cs

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
import climasus as cs

# 1. Import and cache
x = cs.sus_import("SIM-DO", "SP", [2021, 2022])

# 2. Clean
x = cs.sus_clean(x)

# 3. Standardize
x = cs.sus_standardize(x, lang="en")

# 4. Filter
x = cs.sus_filter(x, groups=["dengue"], age_min=0, age_max=80)

# 5. Create variables
x = cs.sus_variables(x, age_group="who", epi_week=True)

# 6. Aggregate
x = cs.sus_aggregate(x, time="month", geo="state")

# 7. Export
cs.sus_export(x, "output/dengue_sp.parquet")
```

## Shared Metadata

climasus4py consumes metadata from `climasus-data` (disease groups, dictionaries,
UFs, regions). This dependency is installed automatically.

You can force update local metadata when needed:

```python
from climasus import update_climasus_data
update_climasus_data()
```

## Contributing

- Pull requests and issues are welcome.
- Run tests locally before opening PRs.
- Include a minimal reproducible example for bug reports.

## License

MIT
