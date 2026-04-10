# Desarrollo

## Setup local

```bash
pip install -e .
pip install pytest ruff mkdocs
```

## Ejecutar tests

```bash
pytest -q
```

## Ejecutar benchmark

```bash
python benchmark/benchmark_pipeline.py
```

Reporte actual: `benchmark/BENCHMARK_REPORT.md`.

## Construir documentacion

```bash
mkdocs serve
mkdocs build
```

## Convenciones recomendadas

- Mantener exports de API publica en `src/climasus/__init__.py`
- Priorizar operaciones lazy con DuckDB
- Evitar materializacion temprana en pandas
- Mantener sincronizados PT/EN/ES cuando cambie la API
