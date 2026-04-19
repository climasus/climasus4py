# Desenvolvimento

Este guia cobre setup local, como rodar os testes, benchmarks, e as convenções do projeto.

---

## Setup local

### 1. Clonar e criar ambiente virtual

```bash
git clone https://github.com/climasus/climasus4py.git
cd climasus4py
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.\.venv\Scripts\Activate.ps1     # Windows PowerShell
```

### 2. Instalar em modo editável com dependências de desenvolvimento

```bash
pip install -e ".[dev]"
# ou, se não houver extras dev:
pip install -e .
pip install pytest ruff pytest-cov
```

### 3. Configurar `climasus-data`

```bash
python -c "import climasus as cs; cs.update_climasus_data()"
```

---

## Testes

```bash
# Rodar toda a suíte
pytest -q

# Com cobertura
pytest --cov=climasus --cov-report=term-missing

# Um módulo específico
pytest tests/test_filter.py -v

# Filtrar por nome de teste
pytest -k "test_pipeline" -v
```

!!! info
    Os testes que fazem download do FTP do DATASUS estão marcados com `@pytest.mark.network` e são pulados por padrão em CI. Para rodá-los localmente:
    ```bash
    pytest -m network
    ```

---

## Qualidade de código

```bash
# Linting e formatação (ruff)
ruff check src/ tests/
ruff format src/ tests/

# Verificação de tipos (se mypy instalado)
mypy src/climasus
```

---

## Benchmarks

O diretório `benchmarks_climasus/python/` contém o script de benchmark:

```bash
python benchmarks_climasus/python/benchmark_pipeline.py
```

O resultado é salvo em `benchmarks_climasus/python/results/` e o relatório consolidado em `BENCHMARK_REPORT.md`.

---

## Documentação local

```bash
pip install -r requirements-docs.txt
mkdocs serve            # servidor local em http://127.0.0.1:8000
mkdocs build            # build estático em site/
```

O build usa `gen_ref_pages.py` para gerar a referência de API automaticamente a partir das docstrings Google-style nos arquivos `.py`.

---

## Convenções do projeto

### Funções públicas

- Exportadas em `src/climasus/__init__.py`
- Prefixo `sus_` para consistência com a API R (`climasus4r`)
- Retornam `duckdb.DuckDBPyRelation` (lazy) quando possível
- Retornam `pd.DataFrame` apenas quando a materialização é necessária (ex.: joins com dados externos)

### Docstrings

Estilo Google:

```python
def sus_exemplo(rel, *, param: str = "valor") -> duckdb.DuckDBPyRelation:
    """Título curto da função.

    Descrição de 2-4 linhas explicando o comportamento,
    incluindo detalhes sobre lazy evaluation quando relevante.

    Args:
        rel: Relação DuckDB de entrada.
        param: Descrição do parâmetro com tipo e valores aceitos.

    Returns:
        Descrição do que é retornado.

    Raises:
        ValueError: Quando ``param`` não é um valor aceito.

    Example:
        >>> result = sus_exemplo(rel, param="valor")
        >>> result.df().head()
    """
```

### Commits

Seguimos [Conventional Commits](https://www.conventionalcommits.org/pt-br/):

```
feat: adiciona suporte a sistema SIAB
fix: corrige detecção de coluna de data no SINASC
docs: atualiza getting-started com exemplos de filtro
refactor: extrai decode_age_sql para utils/data
test: adiciona casos de teste para grupos CID-10
```

---

## Estrutura de testes

```
tests/
├── test_pipeline.py      # sus_pipeline end-to-end
├── test_importer.py      # sus_import + cache
├── test_filter.py        # sus_filter com vários predicados
├── test_aggregate.py     # sus_aggregate time/geo
├── test_variables.py     # sus_variables age_group/epi_week
├── test_cid.py           # expand_cid_ranges, codes_for_groups
└── conftest.py           # fixtures compartilhadas (rel de teste)
```

---

## Como contribuir

1. Abra uma *issue* descrevendo o problema ou feature
2. Faça fork e crie um branch: `git checkout -b feat/minha-feature`
3. Implemente + testes
4. `ruff check` e `pytest` passando
5. Abra PR contra `main`
