# Desarrollo

## Configuración local

```bash
git clone https://github.com/climasus/climasus4py.git
cd climasus4py
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
python -c "import climasus as cs; cs.update_climasus_data()"
```

---

## Pruebas

```bash
pytest -q                                     # suite completa
pytest --cov=climasus --cov-report=term-missing  # con cobertura
pytest tests/test_filter.py -v               # módulo específico
pytest -m network                            # pruebas con descarga FTP
```

---

## Calidad de código

```bash
ruff check src/ tests/
ruff format src/ tests/
```

---

## Benchmarks

```bash
python benchmarks_climasus/python/benchmark_pipeline.py
```

---

## Documentación local

```bash
pip install -r requirements-docs.txt
mkdocs serve   # servidor local en http://127.0.0.1:8000
mkdocs build   # build estático en site/
```

---

## Convenciones

- Funciones públicas exportadas en `src/climasus/__init__.py` con prefijo `sus_`
- Retornan `duckdb.DuckDBPyRelation` (lazy) siempre que sea posible
- Commits siguiendo [Conventional Commits](https://www.conventionalcommits.org/)
- PRs contra `main`, con `ruff check` y `pytest` pasados

## Convenciones recomendadas

- Mantener exports de API publica en `src/climasus/__init__.py`
- Priorizar operaciones lazy con DuckDB
- Evitar materializacion temprana en pandas
- Mantener sincronizados PT/EN/ES cuando cambie la API
