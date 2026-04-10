# Development

## Local setup

```bash
pip install -e .
pip install pytest ruff mkdocs
```

## Run tests

```bash
pytest -q
```

## Run benchmarks

```bash
python benchmark/benchmark_pipeline.py
```

Current benchmark report: `benchmark/BENCHMARK_REPORT.md`.

## Build docs

```bash
mkdocs serve
mkdocs build
```

## Recommended conventions

- Keep public API exports in `src/climasus/__init__.py`
- Prefer lazy DuckDB operations
- Avoid early pandas materialization
- Keep PT/EN/ES docs in sync when API changes
