# Desenvolvimento

## Setup local

```bash
pip install -e .
pip install pytest ruff mkdocs
```

## Testes

```bash
pytest -q
```

## Benchmark

```bash
python benchmark/benchmark_pipeline.py
```

Relatorio atual: `benchmark/BENCHMARK_REPORT.md`.

## Documentacao MkDocs

```bash
mkdocs serve
mkdocs build
```

## Convencoes recomendadas

- Manter funcoes publicas exportadas em `src/climasus/__init__.py`
- Priorizar operacoes lazy com DuckDB
- Evitar materializacao em pandas antes do necessario
- Atualizar docs trilingues quando houver mudanca de API
