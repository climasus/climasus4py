> **Nota histórica (v0.3.0):** Este relatório usa os nomes vigentes em v0.2.x. Os nomes públicos de 9 funções mudaram em v0.3.0 para paridade com `climasus4r` legacy. Tabela de equivalência: ver [`CHANGELOG.md`](CHANGELOG.md).

# Relatório de Revisão Multi-Agente — `climasus4py` v0.2.0

> **Data:** 03/05/2026  
> **Revisores:** Analista de Projeto · Engenheiro de Dados · Cientista de Dados · Revisor de Código Python · Engenheiro de Pacotes · Revisor de Documentação  
> **Versão revisada:** `0.2.0` (Alpha)  
> **Localização:** `climasus4py/`

> **Validação externa (Claude Opus, 03/05/2026):** Spot-check confirmou 5 dos 8 bugs críticos diretamente no código. Validação resultou em 3 ajustes de prioridade incorporados nesta versão do relatório: BUG-2 elevado para RCE, IMP-4 e IMP-7 promovidos ao Sprint 1.
>
> **Plano de execução Sprint 1:** [`governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md`](../governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md)

---

## Índice

1. [Panorama Geral](#1-panorama-geral)
2. [Bugs Críticos](#2-bugs-críticos)
3. [Issues Importantes](#3-issues-importantes)
4. [Melhorias Relevantes](#4-melhorias-relevantes)
5. [Diagnóstico por Agente](#5-diagnóstico-por-agente)
   - [5.1 Analista de Projeto](#51-analista-de-projeto)
   - [5.2 Engenheiro de Dados](#52-engenheiro-de-dados)
   - [5.3 Cientista de Dados](#53-cientista-de-dados)
   - [5.4 Revisor de Código Python](#54-revisor-de-código-python)
   - [5.5 Engenheiro de Pacotes](#55-engenheiro-de-pacotes)
   - [5.6 Revisor de Documentação](#56-revisor-de-documentação)
6. [Plano de Ação](#6-plano-de-ação)

---

## 1. Panorama Geral

| Dimensão | Nota | Avaliação Resumida |
|---|:---:|---|
| Arquitetura | 9/10 | Lazy-first coerente, stage tracking, guards — design de referência |
| Pipeline de Dados | 6/10 | 3 bugs críticos de integridade; fallback chain robusto |
| Qualidade Analítica | 5/10 | 2 bugs metodológicos confirmados; lacunas epidemiológicas graves |
| Qualidade do Código | 5/10 | 4 vulnerabilidades de SQL injection; memory leaks identificados |
| Empacotamento (PyPI) | 5/10 | 3 bloqueadores para publicação segura |
| Documentação | 5/10 | README com import quebrado; 10 funções públicas sem docstring útil |
| **Maturidade Geral** | **6/10** | **Alpha avançado — funcional internamente, não pronto para PyPI público** |

### Pontos Fortes Transversais

- **Arquitetura lazy-first coerente** — `DuckDBPyRelation` propagado por todas as etapas; materialização apenas em `materialize()` ou `sus_export()`
- **Stage tracking com guardrails** — `_stage.py` detecta ordem incorreta e emite `UserWarning` acionável; `_guards.py` lança `TypeError` com mensagem de migração
- **API de materialização flexível** — `materialize(how=auto/pandas/geopandas/polars/pyarrow)` com detecção automática por coluna `geometry_wkt`
- **`sus_sql()` dual-mode** — funciona como entrypoint e como transformador de pipeline via `.pipe()`
- **Trusted Publisher OIDC** — `publish-pypi.yml` sem tokens hardcoded
- **Fallback chain para .dbc** — 5 backends em cascata: `climasus_readdbc_py → climasus_readdbc → pyreaddbc → pysus → dbc2dbf CLI`
- **Benchmarks excelentes** — 0.36s / 334K rows; 0.76s / 2M rows; ~0.1MB memória
- **Fixtures de paridade R↔Python** — `test_real_fixtures.py` valida compatibilidade entre os dois pacotes

---

## 2. Bugs Críticos

> ⚠️ **BUG-1 e BUG-2 são vulnerabilidades de segurança** — devem ser corrigidos imediatamente, independentemente do cronograma de release.

---

### 🔴 BUG-1 — SQL Injection em `sus_filter` e `sus_quality`

**Agentes:** Revisor de Código, Engenheiro de Dados  
**Arquivos:** `src/climasus4py/core/filter.py`, `src/climasus4py/utils/quality.py`  
**Severidade:** Crítica (segurança)

**Problema:**

Valores fornecidos pelo usuário são interpolados diretamente em f-strings SQL sem nenhum escapamento.

```python
# filter.py — linha ~113 — VULNERÁVEL
sex_val = sex.upper()
rel = rel.filter(f'"{sex_col}" = \'{sex_val}\'')

# filter.py — linha ~120 — VULNERÁVEL
vals = ", ".join(f"'{r}'" for r in race_list)
rel = rel.filter(f'"{candidate}" IN ({vals})')

# quality.py — linha ~36 — VULNERÁVEL
non_null = conn.sql(
    f'SELECT COUNT("{col}") FROM data WHERE "{col}" IS NOT NULL'
).fetchone()[0]
```

**Exploração:**
```python
# Entrada maliciosa — injetada diretamente na query
sus_filter(rel, sex="M' OR '1'='1")
```

**Correção:**

Usar as funções `quote_ident()` e `sql_string()` já existentes em `src/climasus4py/core/_sql.py`:

```python
# filter.py — CORREÇÃO
from climasus4py.core._sql import sql_string, quote_ident

# Para sex
rel = rel.filter(f'{quote_ident(sex_col)} = {sql_string(sex_val)}')

# Para race
vals = ", ".join(sql_string(r) for r in race_list)
rel = rel.filter(f'{quote_ident(candidate)} IN ({vals})')

# quality.py — CORREÇÃO
from climasus4py.core._sql import quote_ident, register_relation

view = register_relation(conn, data, "quality_input")
non_null = conn.sql(
    f"SELECT COUNT({quote_ident(col)}) FROM {view} "
    f"WHERE {quote_ident(col)} IS NOT NULL"
).fetchone()[0]
```

---

### 🔴 BUG-2 — Path Traversal em `_raw_cache_path`

**Agente:** Revisor de Código  
**Arquivo:** `src/climasus4py/core/importer.py` linha ~265  
**Severidade:** Crítica (segurança)

**Problema:**

Segmentos de path extraídos de URLs não são validados. `..` não é filtrado, permitindo que o arquivo de cache seja resolvido fora do diretório esperado.

```python
def _raw_cache_path(url: str, raw_cache_dir: Path) -> Path:
    parsed = urlparse(url)
    parts = [parsed.netloc, *[part for part in parsed.path.split("/") if part]]
    # "if part" remove vazios, mas NÃO remove ".."
    return raw_cache_dir.joinpath(*parts)  # ← path traversal possível
```

**Exploração:**
```
URL: ftp://datasus.ftp.gov.br/../../../tmp/evil.dbc
Resultado: raw_cache_dir / ".." / ".." / ".." / "tmp" / "evil.dbc"
           → resolve fora do cache_dir
```

O arquivo resultante é passado para `subprocess.run([dbc2dbf, str(path), dbf_path])`. Isso **eleva o impacto de Path Traversal para Remote Code Execution (RCE) potencial**: um atacante que controle a URL (ex: via redirect de DNS ou resposta FTP forjada) controla onde o binário externo `dbc2dbf` **escreve** — podendo sobrescrever scripts de sistema, arquivos de configuração ou criar um `.dbf` em local arbitrário. OWASP A01:2021 (Broken Access Control) + A03:2021 (Injection).

**Correção:**

```python
def _raw_cache_path(url: str, raw_cache_dir: Path) -> Path:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p and p != "."]
    if ".." in parts or any(Path(p).is_absolute() for p in parts):
        raise ValueError(f"URL com path suspeito bloqueada: {url!r}")
    return raw_cache_dir.joinpath(parsed.netloc, *parts)
```

---

### 🔴 BUG-3 — Escrita de Parquet não atômica (corrupção em concorrência)

**Agente:** Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/importer.py` linha ~335  
**Severidade:** Crítica (integridade de dados)

**Problema:**

Dois workers simultâneos (ex: jobs do Dagster para o mesmo UF/ano) ambos passam na verificação `target.is_file()` (ambos veem `False`), e então escrevem concorrentemente no mesmo arquivo — Parquet corrompido silenciosamente.

```python
# PROBLEMA: dois processos simultâneos escrevem no mesmo target
if not target.is_file():
    pq.write_table(pa.Table.from_pandas(df), target)  # ← corrida
```

**Correção — escrita atômica via `rename`:**

```python
import uuid

def _write_parquet_atomic(table: pa.Table, target: Path) -> None:
    """Escreve Parquet atomicamente via rename para evitar corrupção concorrente."""
    tmp = target.with_suffix(f".tmp_{uuid.uuid4().hex[:8]}.parquet")
    try:
        pq.write_table(table, tmp)
        tmp.replace(target)  # rename atômico no mesmo filesystem
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
```

---

### 🔴 BUG-4 — Descarte silencioso de CIDs no fast path (`prefixes[:200]`)

**Agente:** Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/pipeline.py` linha ~125  
**Severidade:** Crítica (integridade de dados)

**Problema:**

O fast path SQL corta silenciosamente qualquer prefixo CID além do índice 200. Com múltiplos grupos combinados, análises retornam contagens menores que o esperado — sem nenhum erro ou warning.

```python
# Descarta prefixos sem aviso algum
codes_str = ", ".join(f"'{c}'" for c in prefixes[:200])
```

**Correção — tabela temporária para conjuntos grandes:**

```python
if len(prefixes) > 200:
    codes_sql = ", ".join(f"('{c}')" for c in prefixes)
    conn.execute(
        f"CREATE OR REPLACE TEMP TABLE __cid_filter AS VALUES {codes_sql}"
    )
    where_parts.append(
        f'SUBSTR({quote_ident(cause_col)}, 1, 3) IN '
        f'(SELECT column0 FROM __cid_filter)'
    )
else:
    codes_str = ", ".join(f"'{c}'" for c in prefixes)
    where_parts.append(
        f'SUBSTR({quote_ident(cause_col)}, 1, 3) IN ({codes_str})'
    )
```

---

### 🔴 BUG-5 — Semana Epidemiológica com dia de início errado

**Agente:** Cientista de Dados  
**Arquivo:** `src/climasus4py/core/variables.py`  
**Severidade:** Crítica (incorreção metodológica)

**Problema:**

`%W` no DuckDB conta semanas começando na **segunda-feira** (padrão ISO 8601). A semana epidemiológica SVS/SINAN/DATASUS começa no **domingo** (convenção Pan-Americana OPAS). Análises de dengue, arboviroses e SRAG ficam deslocadas por até 6 dias em relação aos boletins oficiais do Ministério da Saúde.

O formato de saída `%Y-W%W` também não coincide com a notação padrão SVS (`SEXX/YYYY`).

```python
# ERRADO — %W começa na segunda-feira
projections.append(f"STRFTIME({date_cast}, '%Y-W%W') AS epi_week")
```

**Correção — `%U` começa no domingo no DuckDB:**

```python
# CORRETO — %U começa no domingo; formato SVS: SE{WW}/{YYYY}
epi_expr = (
    f"LPAD(CAST(STRFTIME({date_cast}, '%U') AS VARCHAR), 2, '0')"
    f" || '/' || STRFTIME({date_cast}, '%Y')"
)
projections.append(f"{epi_expr} AS epi_week")
```

---

### 🔴 BUG-6 — Filtro CID-10 com comportamento inconsistente para listas grandes

**Agente:** Cientista de Dados  
**Arquivo:** `src/climasus4py/core/filter.py` linhas ~113–130  
**Severidade:** Crítica (reprodutibilidade analítica)

**Problema:**

O comportamento do filtro muda dependendo do número de códigos: com ≤100 o match é **exato (4 chars)**, com >100 é por **prefixo de 3 chars**. Contagens para o mesmo grupo de doenças são inconsistentes e não-reprodutíveis dependendo de quais outros grupos são usados na mesma chamada.

```python
if len(unique_codes) <= 100:
    # Match EXATO — captura "J189" mas não "J180"
    rel = rel.filter(f'"{cause_col}" IN ({codes_str})')
else:
    # Match por PREFIXO — captura "J18x" completo
    rel = rel.query(..., f'SEMI JOIN _icd_filter f ON SUBSTR(..., 1, 3) = f.code')
```

**Correção — sempre usar prefixo de 3 caracteres:**

```python
prefixes = sorted(set(c[:3] for c in unique_codes))
prefixes_str = ", ".join(f"'{p}'" for p in prefixes)
rel = rel.filter(
    f'SUBSTR({quote_ident(cause_col)}, 1, 3) IN ({prefixes_str})'
)
```

> **Nota:** Se precisão de subcategoria for necessária (ex: diferenciar J180 de J189), adicionar `exact_match: bool = False` como parâmetro explícito.

---

### 🔴 BUG-7 — `import climasus` no README causa `ModuleNotFoundError`

**Agente:** Revisor de Documentação  
**Arquivo:** `README.md`, `docs/index.md`, `docs/pt/index.md`, `docs/en/index.md`, `docs/es/index.md`  
**Severidade:** Crítica (onboarding quebrado)

**Problema:**

O README e todas as páginas de índice dos docs usam:
```python
import climasus as cs  # ← ModuleNotFoundError
```

O pacote se chama `climasus4py`. As páginas internas dos docs (`querying.md`, `enrichments.md`) já usam o nome correto. Qualquer pesquisador que copie o Quick Start recebe erro imediato.

**Correção:**
```python
import climasus4py as cs  # ← correto
```

Adicionalmente, o README contém um comando pip inválido:
```bash
# ERRADO
pip install climasus4py.git

# CORRETO
pip install git+https://github.com/climasus/climasus4py.git
```

---

### 🔴 BUG-8 — Artefatos `dist/` são da versão 0.1.4 (versão atual: 0.2.0)

**Agente:** Engenheiro de Pacotes  
**Arquivo:** `dist/`  
**Severidade:** Crítica (publicação incorreta)

**Problema:**

O diretório `dist/` contém `climasus4py-0.1.4-py3-none-any.whl` e `climasus4py-0.1.4.tar.gz`. O `pyproject.toml` declara `version = "0.2.0"`. Publicar sem rebuildar enviaria a versão errada ao PyPI.

**Correção:**
```bash
# Limpar dist/ e rebuildar
rm -rf dist/
python -m build
# Verificar antes de publicar
twine check dist/*
```

---

## 3. Issues Importantes

### 🟠 IMP-1 — Arquivo temporário `.dbf` vaza para disco

**Agente:** Revisor de Código, Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/importer.py` linha ~204

O backend `dbc2dbf` cria um arquivo temporário com `delete=False` e nunca o remove. Em pipelines de longa duração, `.dbf` temporários acumulam no disco.

```python
# PROBLEMA — sem cleanup em caso de sucesso
with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
    dbf_path = tmp.name
subprocess.run([dbc2dbf, str(path), dbf_path], check=True, ...)
table = dbfread.DBF(dbf_path, ...)
return pd.DataFrame(iter(table))
# ← dbf_path NUNCA É DELETADO

# CORREÇÃO
try:
    subprocess.run([dbc2dbf, str(path), dbf_path], check=True, capture_output=True)
    table = dbfread.DBF(dbf_path, encoding="latin1")
    return pd.DataFrame(iter(table))
finally:
    Path(dbf_path).unlink(missing_ok=True)
```

---

### 🟠 IMP-2 — Singleton DuckDB não é thread-safe

**Agente:** Revisor de Código  
**Arquivo:** `src/climasus4py/core/engine.py` linha ~22

Race condition: dois threads podem criar conexões separadas. Views registradas em uma conexão não são visíveis na outra.

```python
# PROBLEMA — sem lock
_conn: duckdb.DuckDBPyConnection | None = None

def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")
    return _conn

# CORREÇÃO
import threading
_lock = threading.Lock()

def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    with _lock:
        if _conn is None:
            _conn = duckdb.connect(":memory:")
    return _conn
```

---

### 🟠 IMP-3 — Views DuckDB acumulam sem limpeza (memory leak)

**Agente:** Revisor de Código, Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/_sql.py` linha ~31

Cada chamada a `sus_spatial`, `sus_climate`, `sus_census` registra uma nova view via `register_relation()` que nunca é removida com `conn.unregister()`. Em sessões interativas longas ou notebooks, dezenas de views se acumulam.

```python
# PROBLEMA — sem unregister
def register_relation(conn, rel, prefix) -> str:
    name = f"__climasus_{prefix}_{id(rel)}"
    conn.register(name, rel)   # ← sem conn.unregister(name) em lugar algum
    return name
```

---

### 🟠 IMP-4 — `_STAGES` global cresce indefinidamente + ID reuse

> ⚠️ **Reclassificado pelo revisor externo (Claude Opus):** promovido ao Sprint 1. O stage tracking é parte do contrato público com o usuário — falsos positivos nos guardrails quebram a confiança na API principal do pacote.

**Agente:** Revisor de Código, Analista de Projeto  
**Arquivo:** `src/climasus4py/core/_stage.py` linha ~20

`dict[int, str]` usando `id(rel)` nunca é limpo. Pior: `id()` de um objeto Python é reutilizado após garbage collection — uma nova relação pode herdar o stage de uma relação já destruída, gerando falsos positivos nos guardrails.

```python
# PROBLEMA
_STAGES: dict[int, str] = {}

def set_stage(rel, stage):
    _STAGES[id(rel)] = stage  # ← id() pode ser reutilizado após GC

# SOLUÇÃO MÍNIMA — não retém o objeto vivo
import weakref
_STAGES: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
```

---

### 🟠 IMP-5 — Download sem retry e carregando arquivo inteiro na RAM

**Agente:** Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/importer.py` linha ~228

O FTP do DATASUS é notoriamente instável. Arquivos SIH-SP mensais chegam a 100–500MB. `response.read()` carrega o arquivo inteiro na RAM de uma vez, sem streaming e sem retry.

```python
# PROBLEMA
with urllib.request.urlopen(url, timeout=timeout) as response:
    dest.write_bytes(response.read())  # ← 500MB na RAM; sem retry

# CORREÇÃO — streaming com retry exponencial
import time

def _download_ftp(url: str, dest: Path, timeout: int = 120,
                  max_retries: int = 3) -> bool:
    for attempt in range(max_retries):
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(url, timeout=timeout) as response:
                with open(dest, "wb") as f:
                    while chunk := response.read(4 * 1024 * 1024):  # 4MB chunks
                        f.write(chunk)
            return True
        except Exception as e:
            if dest.exists():
                dest.unlink()
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # backoff: 1s, 2s, 4s
                continue
            return False
    return False
```

---

### 🟠 IMP-6 — `sus_quality` executa N queries SQL (uma por coluna)

**Agente:** Revisor de Código, Engenheiro de Dados  
**Arquivo:** `src/climasus4py/utils/quality.py` linha ~52

Para 100 colunas, executa 100 queries DuckDB separadas. Trivialmente resolvível com uma única query de agregação.

```python
# PROBLEMA — 100 colunas = 100 queries
for col in columns:
    non_null = conn.sql(
        f'SELECT COUNT("{col}") FROM data WHERE "{col}" IS NOT NULL'
    ).fetchone()[0]

# CORREÇÃO — single query
agg_exprs = ", ".join(f'COUNT({quote_ident(col)}) AS {quote_ident(col)}' for col in columns)
row = conn.sql(f"SELECT {agg_exprs} FROM {view}").fetchone()
completeness = {
    col: round(val / max(total_rows, 1) * 100, 1)
    for col, val in zip(columns, row)
}
```

---

### � IMP-7 — Linkagem clima↔saúde produz NULLs silenciosos pós-agregação mensal

> ⚠️ **Reclassificado pelo revisor externo (Claude Opus):** promovido de "Issue Importante" (🟠) para **crítico** (🔴) e movido ao Sprint 1. Justificativa: `sus_climate` é a **feature central do pacote ClimaSUS** — um pacote sobre correlação clima↔saúde com a feature principal silenciosamente quebrada para o caso de uso mais comum (`time="month"`) equivale a produto não funcional.

**Agente:** Cientista de Dados  
**Arquivo:** `src/climasus4py/enrichment/climate.py`

O `sus_climate` exige `assert_after(..., "aggregate")`. Após agregar com `time="month"`, a coluna de data se torna `"2023-01"` (string). O join tenta `CAST("2023-01" AS DATE)` — que retorna NULL no DuckDB. O join climático produz apenas NULLs silenciosamente para qualquer resolução diferente de `time="day"`. Este comportamento não está documentado.

**Arquitetura correta para estudos de série temporal clima-saúde:**
```
1. Dados de saúde individuais (DTOBITO por CODMUNRES)
2. Dados climáticos diários por município (IDW já aplicado)
3. Agregar AMBOS para a mesma resolução (ex: mês×município)
4. Join por chave temporal×geográfica
```

Requer nova função `sus_climate_preaggregate()` ou refatoração do estágio de enriquecimento para operar antes de `sus_aggregate`.

---

### 🟠 IMP-8 — Caminho absoluto da máquina do desenvolvedor nos testes

**Agente:** Analista de Projeto, Engenheiro de Pacotes  
**Arquivo:** `tests/test_pipeline_e2e.py`

```python
# Caminho da máquina de um único desenvolvedor no código do repositório público
PARQUET = Path(r"c:\Users\Readone\Desktop\CLIMA_SUS_4_R\dados\cache\SIM-DO\SP_2023_all.parquet")
```

O `skipif` mitiga o problema no CI, mas o caminho não deve estar no código. Qualquer clone por outro colaborador jamais executará esses testes.

```python
# CORREÇÃO
PARQUET = Path(os.getenv("CLIMASUS_TEST_PARQUET", ""))

@pytest.mark.skipif(
    not PARQUET.is_file(),
    reason="CLIMASUS_TEST_PARQUET não definida ou arquivo ausente"
)
```

---

### 🟠 IMP-9 — Dependências `climasus-data` e `climasus_readdbc_py` podem não estar no PyPI

**Agente:** Engenheiro de Pacotes

Ambas são deps **obrigatórias** em `pyproject.toml`. Se não estiverem publicadas no índice PyPI, `pip install climasus4py` falhará para qualquer usuário externo. A presença de `allow-direct-references = true` sugere que podem usar referências diretas (`dep @ git+https://...`).

**Ação:** verificar antes de publicar:
```bash
pip index versions climasus-data
pip index versions climasus_readdbc_py
```

---

### 🟠 IMP-10 — Exceções silenciadas mascaram falhas reais no `_read_dbc`

**Agente:** Revisor de Código, Engenheiro de Dados  
**Arquivo:** `src/climasus4py/core/importer.py`

O padrão `except Exception: pass` repete-se 4 vezes. Se um backend disponível **tenta mas falha** (arquivo corrompido, OOM), o erro desaparece e o usuário vê apenas `ImportError: Cannot read .dbc files` sem indicação do erro real.

```python
# CORREÇÃO — acumular erros para diagnóstico
import logging
_log = logging.getLogger(__name__)

errors: list[str] = []
try:
    return readdbc.read_dbc(path)
except Exception as e:
    errors.append(f"climasus_readdbc_py: {e}")

# ... outros backends ...

if errors:
    _log.debug("_read_dbc tentativas falhas: %s", "; ".join(errors))
raise ImportError(
    f"Não foi possível ler {path.name}.\n"
    + (f"Erros encontrados:\n  " + "\n  ".join(errors) + "\n" if errors else "")
    + "Instale: pip install climasus_readdbc_py"
)
```

---

## 4. Melhorias Relevantes

| # | Melhoria | Agente | Arquivo | Esforço |
|---|---|---|---|:---:|
| M-1 | Adicionar `ruff check .` ao CI antes de `pytest` | Analista de Projeto | `.github/workflows/ci.yml` | ⬛ Baixo |
| M-2 | Coverage threshold `--cov-fail-under=75` no CI | Analista de Projeto | `pyproject.toml` | ⬛ Baixo |
| M-3 | Consolidar versão via `[tool.hatch.version]` (single source of truth) | Analista + Eng. Pacotes | `pyproject.toml` | ⬛ Baixo |
| M-4 | Download paralelo para `uf="all"` com `ThreadPoolExecutor` | Engenheiro de Dados | `importer.py` | ⬛⬛ Médio |
| M-5 | Função `sus_rates()` — contagens → taxas por 100k hab. | Cientista de Dados | novo módulo | ⬛⬛ Médio |
| M-6 | Expandir `sus_quality` com validações epidemiológicas | Cientista de Dados | `quality.py` | ⬛⬛ Médio |
| M-7 | Completar docstrings das 10 funções sem cobertura adequada | Revisor de Docs | vários | ⬛ Baixo |
| M-8 | Corrigir estilo docstring `sus_climate_inmet` (NumPy → Google) | Revisor de Docs | `climate.py` | ⬛ Baixo |
| M-9 | Adicionar `environment: pypi` no `publish-pypi.yml` | Eng. Pacotes | `publish-pypi.yml` | ⬛ Baixo |
| M-10 | Adicionar marker `pysus; sys_platform != 'win32'` | Eng. Pacotes | `pyproject.toml` | ⬛ Baixo |
| M-11 | Remover/renomear `src/climasus/` (diretório fantasma quebra build dos docs) | Revisor de Docs | `src/climasus/` | ⬛ Baixo |
| M-12 | Matrix CI com runner Windows | Analista de Projeto | `ci.yml` | ⬛⬛ Médio |
| M-13 | `conftest.py` com reset de conexão DuckDB entre testes | Analista de Projeto | `tests/conftest.py` | ⬛ Baixo |
| M-14 | Detecção de ondas de calor (N dias consecutivos > limiar) | Cientista de Dados | novo módulo | ⬛⬛⬛ Alto |
| M-15 | `@lru_cache(maxsize=1)` em `_datasus_date_cols()` | Revisor de Código | `importer.py` | ⬛ Baixo |
| M-16 | `pin` de SHA fixo na action `gh-action-pypi-publish` | Eng. Pacotes | `publish-pypi.yml` | ⬛ Baixo |
| M-17 | Adicionar URLs ao PyPI (`Changelog`, `Documentation`) | Eng. Pacotes | `pyproject.toml` | ⬛ Baixo |
| M-18 | Criar ao menos 1 tutorial executável por idioma nos docs | Revisor de Docs | `docs/` | ⬛⬛⬛ Alto |

---

## 5. Diagnóstico por Agente

---

### 5.1 Analista de Projeto

**Nota: 7,0 / 10**

#### Estado Atual

O pacote está em **Alpha avançado** (classificador PyPI correto). Passou por uma reescrita significativa em 02/05/2026 (v0.2.0), migrando de uma API mista para um contrato totalmente lazy-first baseado em `DuckDBPyRelation`. A arquitetura central é sólida. Os maiores riscos são operacionais: testes E2E com caminhos absolutos hardcoded, ausência de linting no CI, e documentação trilíngue incipiente.

#### Maturidade por Dimensão

| Dimensão | Nota | Justificativa |
|---|:---:|---|
| Arquitetura | 9/10 | Lazy-first, stage tracking, guards — design de referência |
| Testes | 6/10 | 20 arquivos, boa largura; mas sem coverage threshold, E2E com path hardcoded |
| CI/CD | 6/10 | 3 workflows presentes, OIDC configurado; sem lint no pipeline e sem Windows |
| Documentação | 5/10 | README com typo, CHANGELOG com 1 entrada, docs trilíngues incipientes |
| Segurança de release | 8/10 | Trusted Publisher OIDC, checklist detalhado, TestPyPI step definido |
| Operação | 7/10 | Fallback chain robusto, cache de Parquet, env var override; singleton sem isolamento |

#### Ações Prioritárias

1. **[Imediato]** Corrigir caminho hardcoded em `test_pipeline_e2e.py`
2. **[Imediato]** Adicionar `ruff check .` ao CI
3. **[Curto prazo]** Adicionar coverage threshold (`--cov-fail-under=75`)
4. **[Curto prazo]** `conftest.py` com fixture de reset de conexão DuckDB
5. **[Curto prazo]** Consolidar versão com `[tool.hatch.version]`
6. **[Médio prazo]** Runner Windows no CI
7. **[Médio prazo]** Expandir docs trilíngues além do `index.md`

---

### 5.2 Engenheiro de Dados

**Nota: 6/10**

#### Pontos Fortes do Pipeline

| Aspecto | Avaliação |
|---|---|
| Lazy evaluation | Excelente — `DuckDBPyRelation` propagado corretamente |
| Fast path SQL | Sólido — CTE único replica otimização do R `rc_a` |
| Schema evolution | `union_by_name=True` resolve diferenças entre anos |
| Cache de Parquet | Bem implementado com manifest JSON + MD5 |
| Chain de backends .dbc | Boa resiliência com 5 backends em cascata |
| Stage tracking | `_stage.py` guia o usuário com warnings claros |
| Coerção de tipos | `_coerce_datasus_types` antes de gravar no Parquet |

#### Bugs Críticos de Pipeline

- **BUG-3**: Escrita de Parquet não atômica → corrupção em concorrência (ver §2)
- **BUG-4**: `prefixes[:200]` descarta CIDs silenciosamente (ver §2)
- **IMP-10**: Exceções silenciadas no `_read_dbc` (ver §3)

#### Issues de Robustez

- **IMP-5**: Download sem retry e sem streaming — falha em arquivos grandes com FTP instável
- **IMP-6**: `sus_quality` executa N queries individuais — ineficiente para tabelas largas
- **IMP-1**: Arquivo `.dbf` temporário nunca deletado

#### Inconsistência de Datas

`_coerce_datasus_types` converte `DTOBITO` → `datetime64` antes de gravar no Parquet. Depois, `sus_standardize` tenta reconverter de `'%d%m%Y'`. Ao ler do cache, `CAST(date AS VARCHAR)` produz `'2023-01-15'` e o `TRY_STRPTIME(..., '%d%m%Y')` retorna NULL — salvo pelo `TRY_CAST`. A lógica está acidentalmente correta, mas é frágil.

**Recomendação:** centralizar conversão de datas exclusivamente em `_coerce_datasus_types`.

#### Melhoria de Performance: Download Paralelo

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=min(4, len(needed))) as pool:
    futures = {
        pool.submit(_download_and_cache, **item): item
        for item in needed
    }
    for future in as_completed(futures):
        result = future.result()
        if result:
            parquet_paths.append(result)
```

---

### 5.3 Cientista de Dados

**Nota: 5/10**

#### Cobertura Analítica

| Componente | Status | Avaliação |
|---|---|---|
| Decodificação IDADE DATASUS | ✅ Implementado | Correto |
| Filtragem CID-10 por grupos | ⚠️ Implementado | Bug de inconsistência (BUG-6) |
| Faixas etárias (WHO, decadal, custom) | ✅ Implementado | OK |
| Limpeza e deduplicação | ✅ Implementado | OK |
| Padronização de colunas | ✅ Implementado | OK |
| Agregação tempo×espaço | ✅ Implementado | OK |
| Semana epidemiológica SVS | ❌ Bug | Dia de início errado (BUG-5) |
| Enriquecimento climático IDW | ⚠️ Implementado | NULLs silenciosos pós-agregação (IMP-7) |
| Enriquecimento espacial | ✅ Implementado | OK |
| Enriquecimento censitário | ✅ Implementado | OK |
| **Taxas de mortalidade/incidência** | ❌ Ausente | Lacuna crítica |
| **Modelos de defasagem distribuída (DLNM)** | ❌ Ausente | Lacuna crítica |
| **Mortalidade excessiva / baseline** | ❌ Ausente | Lacuna crítica |
| **Indicadores SINASC** | ❌ Ausente | Lacuna crítica |
| **Detecção de ondas de calor** | ❌ Ausente | Lacuna crítica |

#### Má Prática — `sus_quality` Epidemiologicamente Vazio

`sus_quality` retorna apenas `total_rows`, `total_cols` e completude por coluna. Para uso epidemiológico é insuficiente. Não verifica:

- Proporção de `CAUSABAS` inválidos (não-CID10, `"000"`, ignorados)
- Consistência `DTOBITO >= DTNASC`
- `CODMUNRES` impossíveis (ex: 999999, 000000)
- Taxa de campos "ignorado" (código 9/99/999) em `SEXO`, `RACACOR`, `ESCMAE`

#### Má Prática — RACACOR sem Rótulos

A filtragem aceita `race=["1", "4"]` sem documentar os significados: 1=Branca, 2=Preta, 3=Amarela, 4=Parda, 5=Indígena. Induz erros de interpretação em análises de saúde racial.

#### O Que Falta para Pesquisa Epidemiológica Séria

| Lacuna | Impacto | Referência Metodológica |
|---|---|---|
| **Modelos DLNM** | Crítico | Gasparrini et al. (2010), padrão ouro clima-saúde |
| **Mortalidade excessiva / baseline** | Alto | Análise de ondas de calor, p-score |
| **Taxas padronizadas por idade (TEP)** | Alto | Comparabilidade entre populações |
| **Autocorrelação espacial (Moran's I)** | Médio | Clusterização de eventos clima-saúde |
| **Índice de calor / Temperatura aparente** | Médio | Exposição humana efetiva ao calor |
| **Detecção de ondas de calor** | Alto | Definição algoritmos WHO, Perkins |
| **Testes de tendência temporal** | Médio | Verificar mudança climática no período |
| **Indicadores SINASC** (Kotelchuck, peso) | Alto | Desfechos perinatais |
| **Linkagem probabilística SIM-SINASC** | Avançado | Estudos de coorte passivo |
| **Quality report epidemiológico** | Alto | Completude de campos críticos por UF/ano |

#### Proposta `sus_rates()`

```python
# Após sus_census(rel, variables=["population_2022"])
rel = sus_rates(rel, numerator="count", denominator="population_2022", per=100_000)
# Adiciona: rate_per_100k = count / population_2022 * 100_000
```

---

### 5.4 Revisor de Código Python

**Nota: 5/10**

#### Vulnerabilidades de Segurança

- **BUG-1**: SQL injection em `filter.py` e `quality.py` (ver §2)
- **BUG-2**: Path traversal em `_raw_cache_path` (ver §2)

#### Problemas de Correctness

- **IMP-4**: `_STAGES` com `id()` reusável após GC — stage pode ser atribuído ao objeto errado
- **IMP-2**: Singleton DuckDB sem `threading.Lock` — race condition real em ambientes async
- **IMP-10**: Exceções silenciadas mascaram falhas reais no `_read_dbc`

#### Type Safety

- `dict` não parametrizado em `_download_pysus` (linha ~373): `kwargs: dict` → `kwargs: dict[str, Any]`
- `sus_sql._install_pipe()` usa monkey-patch com `type: ignore[attr-defined]` — sem teste que detecte quebra após upgrade do DuckDB

#### Problemas de Manutenibilidade

| Issue | Arquivo | Descrição |
|---|---|---|
| DRY violado | `clean.py` | Decodificação de `IDADE` duplica `decode_age_sql()` de `utils/data.py` |
| `_datasus_date_cols()` sem cache | `importer.py` | Recria `frozenset` a cada chamada; decorar com `@lru_cache(maxsize=1)` |
| Warnings em português | `_stage.py` | Em pacote multilíngue, warnings ao usuário deveriam ser em inglês |
| `assert_after` usa `warnings.warn` | `_stage.py` | Pipeline mal-ordenado → resultados incorretos; deveria ser pelo menos `UserWarning` com `stacklevel` correto |
| Extra `metadata = []` vazio | `pyproject.toml` | Sem deps, sem propósito documentado |

#### Avaliação Geral

O código tem uma base sólida (type hints razoáveis, uso idiomático de DuckDB, abstrações bem pensadas), mas os 2 bugs de segurança são graves e os memory leaks do singleton DuckDB precisam de atenção antes de qualquer uso em produção.

---

### 5.5 Engenheiro de Pacotes

**Nota: 5/10**

#### Bloqueadores para Publicação PyPI

| # | Issue | Impacto |
|---|---|---|
| B1 | `publish-pypi.yml` sem `environment:` declarado no job | OIDC token pode ser rejeitado |
| B2 | Deps obrigatórias `climasus-data` e `climasus_readdbc_py` podem não estar no PyPI | `pip install` falha para todos os usuários |
| B3 | `dist/` contém artefatos da v0.1.4, não da v0.2.0 | Publica versão errada |

**Correção B1:**
```yaml
jobs:
  build-and-publish:
    runs-on: ubuntu-latest
    environment: pypi          # ← adicionar
```

#### Issues Importantes de Packaging

| # | Issue | Correção |
|---|---|---|
| I1 | Versão duplicada (`pyproject.toml` + `_version.py`) sem automação | `[tool.hatch.version] path = "src/climasus4py/_version.py"` |
| I2 | Path Windows hardcoded em `test_pipeline_e2e.py` | `PARQUET = Path(os.getenv("CLIMASUS_TEST_PARQUET", ""))` |
| I3 | CI instala editable, nunca testa o wheel buildado | Adicionar smoke test com `pip install dist/*.whl` |
| I4 | `pysus` sem platform marker para Windows | `pysus>=1.0; sys_platform != 'win32'` |
| I5 | `gh-action-pypi-publish@release/v1` é tag mutável | Usar SHA fixo (supply chain hardening) |
| I6 | Condição do job publish ambígua para `workflow_dispatch` | Adicionar condição explícita para `inputs.target == 'pypi'` |

#### Recomendações de Boas Práticas

| Item | Situação | Recomendação |
|---|---|---|
| sdist contents | Sem `[tool.hatch.build.targets.sdist]` | Declarar `include` para excluir `.venv/`, `site/`, `dados/` |
| Classifiers | Bons, incompletos | Adicionar `OS :: OS Independent`, `Natural Language :: Portuguese (Brazilian)` |
| URLs do projeto | Incompletas | Adicionar `Changelog` e `Documentation` URLs |
| `allow-direct-references = true` | Suspeito | Remover quando deps estiverem no PyPI |
| Extra `dev` | Sem `build` nem `hatch` | Adicionar `build>=1.0` e `hatch>=1.9` para contribuidores |

#### Estado de Prontidão

O pacote tem boa estrutura (`src/` layout, `__all__`, `hatchling`, matrix CI). Com os 3 bloqueadores resolvidos, a nota sobe para ~8/10.

---

### 5.6 Revisor de Documentação

**Nota: 5/10**

#### Cobertura de Docstrings (Funções Públicas)

| Função | Estado |
|---|---|
| `sus_pipeline` | ✅ Completa (Args, Returns, Examples) |
| `sus_import` | ✅ Completa (Args, Returns, Raises, Examples) |
| `sus_clean` | ✅ Completa (Args, Returns, Examples) |
| `sus_filter` | ✅ Completa (Args, Returns, Examples) |
| `sus_aggregate` | ✅ Completa (Args, Returns, Examples) |
| `sus_cache_info` | ✅ Completa |
| `sus_cache_clear` | ✅ Completa |
| `sus_explore` | ✅ Completa (Args, Returns, Raises, Examples) |
| `sus_quality` | ✅ Completa |
| `update_climasus_data` | ✅ Completa em PT |
| `sus_climate_inmet` | ⚠️ Completa mas estilo NumPy (mkdocs.yml define Google) |
| `sus_spatial` | ⚠️ Prosa parcial, sem Args/Returns/Example |
| `sus_export` | ⚠️ Duas frases, sem Args/Returns/Raises |
| `sus_sql` | ⚠️ Minimal |
| `sus_standardize` | ❌ Uma linha apenas |
| `sus_variables` | ❌ Uma linha apenas |
| `sus_read` | ❌ Uma linha apenas |
| `materialize` | ❌ Uma linha apenas |
| `sus_climate` | ❌ Uma linha apenas |
| `sus_census` | ❌ Uma linha apenas |
| `sus_fill_gaps` | ❌ Uma linha apenas |

#### Issues Críticos

1. **BUG-7**: `import climasus` no README e todas as páginas de índice (ver §2)
2. **Diretório fantasma `src/climasus/`**: o script `gen_ref_pages.py` tentará gerar `reference/climasus/core/importer.md` com `::: climasus.core.importer` — o `mkdocs build` falhará pois `climasus` não é um módulo instalável
3. **10 funções públicas sem docstring** adequada para `mkdocstrings`

#### Issues Importantes

- **Tabela de sistemas**: `docs/en/index.md` lista `SIA` (Ambulatorial) como suportado, mas não está implementado
- **Assimetria multilíngue**: páginas `querying.md`, `enrichments.md`, `materialize.md` existem só em PT; EN e ES têm apenas `index.md`
- **CHANGELOG monolíngue e incompleto**: v0.1.0 não documentada; breaking changes da v0.2.0 ausentes
- **Nenhum tutorial executável**: notebooks existem em `jupyter_notebooks/` do workspace raiz mas não estão vinculados aos docs do pacote
- **Assets externos frágeis**: logo e CSS carregados do `design-system` GitHub Pages — se o Pages cair, o site fica sem branding sem erro no CI
- **`gen_ref_pages.py` expõe módulos internos**: `engine.py`, `inmet_parser.py` aparecem na referência de API mesmo não estando em `__all__`

#### Melhorias Sugeridas

1. Adicionar seção "Pré-requisitos e Windows" ao README explicando a chain de backends `.dbc`
2. Expandir `pipeline-order.md` com diagrama Mermaid do fluxo de dados
3. Criar pelo menos 1 tutorial end-to-end por idioma (ex: mortalidade por dengue AM 2019–2023)
4. Configurar `gen_ref_pages.py` para documentar apenas símbolos em `__all__`
5. Unificar estilo de docstrings (Google) e documentar em `CONTRIBUTING`

---

## 6. Plano de Ação

### Sprint 1 — Antes do próximo push para `main` (segurança e correctness)

| # | Ação | Arquivo(s) | Responsável |
|---|---|---|---|
| 1 | Corrigir SQL injection em `sus_filter` e `sus_quality` | `filter.py`, `quality.py` | Dev |
| 2 | Corrigir path traversal em `_raw_cache_path` | `importer.py` | Dev |
| 3 | Corrigir semana epidemiológica (`%W` → `%U` + formato SVS) | `variables.py` | Dev |
| 4 | Corrigir `import climasus` no README e páginas de docs | `README.md`, `docs/*/index.md` | Dev |
| 5 | Remover caminho hardcoded de `test_pipeline_e2e.py` | `tests/test_pipeline_e2e.py` | Dev |
| 6 | Adicionar `ruff check .` ao CI | `.github/workflows/ci.yml` | Dev |
| 7 | Uniformizar filtro CID-10 (sempre 3 chars) | `filter.py` | Dev |
| 8 | Corrigir linkagem climática (IMP-7): `sus_climate` produz NULLs pós-agregação mensal | `enrichment/climate.py` | Dev |
| 9 | Corrigir `_STAGES` id() reuse com `WeakValueDictionary` (IMP-4) | `core/_stage.py` | Dev |

### Sprint 2 — Antes de publicar no PyPI (bloqueadores de release)

| # | Ação | Arquivo(s) | Responsável |
|---|---|---|---|
| 8 | Escrita atômica de Parquet via `rename` | `importer.py` | Dev |
| 9 | Corrigir corte silencioso de CIDs `prefixes[:200]` | `pipeline.py` | Dev |
| 10 | Deletar arquivo `.dbf` temporário com `try/finally` | `importer.py` | Dev |
| 11 | Adicionar `environment: pypi` no publish workflow | `publish-pypi.yml` | Dev |
| 12 | Confirmar `climasus-data` e `climasus_readdbc_py` no PyPI | — | Dev |
| 13 | Rebuildar `dist/` com `python -m build` | — | Dev |
| 14 | Completar docstrings das 10 funções sem cobertura | vários | Dev |
| 15 | Remover/renomear `src/climasus/` | `src/climasus/` | Dev |
| 16 | Corrigir estilo docstring `sus_climate_inmet` (NumPy → Google) | `climate.py` | Dev |
| 17 | Consolidar versão via `[tool.hatch.version]` | `pyproject.toml` | Dev |

### Sprint 3 — Qualidade e features pós-release

| # | Ação | Arquivo(s) | Responsável |
|---|---|---|---|
| 18 | Download paralelo + retry com streaming | `importer.py` | Dev |
| 19 | ~~Corrigir linkagem climática~~ → *movido ao Sprint 1 (item 8)* | — | — |
| 20 | Acumular erros reais no `_read_dbc` | `importer.py` | Dev |
| 21 | `threading.Lock` no singleton DuckDB | `engine.py` | Dev |
| 22 | ~~`WeakValueDictionary` em `_STAGES`~~ → *movido ao Sprint 1 (item 9)* | — | — |
| 23 | `sus_quality` com validações epidemiológicas | `quality.py` | Dev |
| 24 | Função `sus_rates()` — taxas por 100k habitantes | novo arquivo | Dev |
| 25 | Matrix Windows no CI | `ci.yml` | Dev |
| 26 | Coverage threshold `--cov-fail-under=75` | `pyproject.toml` | Dev |
| 27 | Indicadores SINASC (peso ao nascer, Kotelchuck) | `variables.py` | Dev |
| 28 | Detecção de ondas de calor (algoritmos WHO, Perkins) | novo módulo | Dev |
| 29 | Criar tutoriais executáveis por idioma nos docs | `docs/` | Dev |
| 30 | Expandir CHANGELOG com histórico v0.1.0 e breaking changes v0.2.0 | `CHANGELOG.md` | Dev |

---

> **Nota:** Os itens 1 e 2 (SQL injection e path traversal — BUG-2 com potencial RCE) são vulnerabilidades de segurança OWASP e devem ser tratados com prioridade máxima, antes de qualquer outro trabalho.

---

## 7. Integração com o Plano Lazy em Andamento

Este relatório cobre a versão `0.2.0` entregue pelo plano [`2026-05-02-py-pipeline-lazy-completo.md`](../governanca/3-planos/em-execucao/2026-05-02-py-pipeline-lazy-completo.md). Os bugs encontrados têm duas origens:

- **Implementação anterior ao plano lazy** (`sus_filter`, `sus_quality`, `_raw_cache_path`, `_STAGES`): funções que o plano lazy não tocou e carregam dívida técnica da versão 0.1.x.
- **Funções criadas/modificadas no plano lazy** que ainda não atingiram completude: `sus_climate` (IMP-7), `pipeline.py` fast path (BUG-4).

**Recomendação de integração:**

| Sprint | Ação |  
|---|---|
| Sprint 1 deste relatório | Executar como sub-tarefa de segurança/correctness do plano lazy (não cria conflito) |
| Sprint 2 deste relatório | Absorver como **Fase 4** do plano lazy (`2026-05-02`) |
| Sprint 3 deste relatório | Absorver como **Fase 5** do plano lazy (features epidemiológicas) |

O plano de execução para Sprint 1 está em: [`governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md`](../governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md)
