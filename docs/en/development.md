# Development

This guide covers local setup, running tests, benchmarks, and project conventions.

---

## Local setup

### 1. Clone and create a virtual environment

```bash
git clone https://github.com/climasus/climasus4py.git
cd climasus4py
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.\.venv\Scripts\Activate.ps1     # Windows PowerShell
```

### 2. Install in editable mode with dev dependencies

```bash
pip install -e ".[dev]"
# or, if no dev extras defined:
pip install -e .
pip install pytest ruff pytest-cov
```

### 3. Configure `climasus-data`

```bash
python -c "import climasus as cs; cs.update_climasus_data()"
```

---

## Tests

```bash
# Run the full suite
pytest -q

# With coverage
pytest --cov=climasus --cov-report=term-missing

# A specific module
pytest tests/test_filter.py -v

# Filter by test name
pytest -k "test_pipeline" -v
```

!!! info
    Tests that require DATASUS FTP downloads are marked `@pytest.mark.network` and skipped in CI by default. To run them locally:
    ```bash
    pytest -m network
    ```

---

## Code quality

```bash
# Linting and formatting (ruff)
ruff check src/ tests/
ruff format src/ tests/
```

---

## Benchmarks

```bash
python benchmarks_climasus/python/benchmark_pipeline.py
```

Results are saved in `benchmarks_climasus/python/results/` and the consolidated report in `BENCHMARK_REPORT.md`.

---

## Local documentation

```bash
pip install -r requirements-docs.txt
mkdocs serve      # local server at http://127.0.0.1:8000
mkdocs build      # static build in site/
```

---

## Project conventions

### Public functions
- Exported in `src/climasus/__init__.py`
- `sus_` prefix for consistency with the R API (`climasus4r`)
- Return `duckdb.DuckDBPyRelation` (lazy) whenever possible
- Return `pd.DataFrame` only when materialisation is required (e.g. external joins)

### Commits
Following [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add SIAB system support
fix: fix date column detection in SINASC
docs: update getting-started with filter examples
refactor: extract decode_age_sql to utils/data
test: add ICD-10 group test cases
```

---

## Contributing

1. Open an issue describing the bug or feature
2. Fork and create a branch: `git checkout -b feat/my-feature`
3. Implement + tests
4. `ruff check` and `pytest` passing
5. Open PR against `main`

## Recommended conventions

- Keep public API exports in `src/climasus/__init__.py`
- Prefer lazy DuckDB operations
- Avoid early pandas materialization
- Keep PT/EN/ES docs in sync when API changes
