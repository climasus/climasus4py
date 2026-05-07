> **Nota histórica (v0.3.0):** Este relatório usa os nomes vigentes em v0.2.x. Os nomes públicos de 9 funções mudaram em v0.3.0 para paridade com `climasus4r` legacy. Tabela de equivalência: ver [`CHANGELOG.md`](CHANGELOG.md).

# Relatório de Revisão Multi-Agente — `climasus4py` v0.2.0 (pós-Sprint 1)

> **Data:** 03/05/2026 (sessão 2)
> **Revisores:** Arquiteto de Software · Revisor de Código Python · Cientista de Dados · Engenheiro de Pacotes
> **Versão revisada:** `0.2.0` (Alpha)
> **Localização:** `climasus4py/`
> **Contexto:** Segunda revisão. Sprint 1 (9 itens de segurança/correctness) concluído. Fase 0 do plano lazy concluída. Avaliação do estado atual e definição de prioridades para Sprint 2.

> **Relatório anterior:** [`climasus4py/REVIEW_REPORT.md`](REVIEW_REPORT.md) — revisão inicial de 03/05/2026
> **Sprint 1 concluído:** [`governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md`](../governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md)
> **Plano lazy:** [`governanca/3-planos/em-execucao/2026-05-02-py-pipeline-lazy-completo.md`](../governanca/3-planos/em-execucao/2026-05-02-py-pipeline-lazy-completo.md)

---

## Índice

1. [Panorama Geral](#1-panorama-geral)
2. [O que mudou desde o relatório anterior](#2-o-que-mudou-desde-o-relatório-anterior)
3. [Issues Ativos](#3-issues-ativos)
4. [Diagnóstico por Agente](#4-diagnóstico-por-agente)
   - [4.1 Arquiteto de Software](#41-arquiteto-de-software)
   - [4.2 Revisor de Código Python](#42-revisor-de-código-python)
   - [4.3 Cientista de Dados](#43-cientista-de-dados)
   - [4.4 Engenheiro de Pacotes](#44-engenheiro-de-pacotes)
5. [Plano de Ação — Sprint 2](#5-plano-de-ação--sprint-2)

---

## 1. Panorama Geral

| Dimensão | Nota anterior | Nota atual | Delta | Resumo |
|---|:---:|:---:|:---:|---|
| Arquitetura | 9/10 | **6,5/10** | ↓ | Risco por módulos grandes e não testados; design geral permanece sólido |
| Pipeline de Dados | 6/10 | **7/10** | ↑ | Joins 6→7 dígitos corrigidos; 3 enriquecimentos com cobertura ≥ 88% |
| Qualidade Analítica | 5/10 | **5,5/10** | ↑ | SE SVS corrigida, guard de granularidade; mas dados climáticos reais ausentes |
| Qualidade do Código | 5/10 | **6,4/10** | ↑ | ruff limpo, `filter.py` 100%; 15 erros mypy residuais |
| Empacotamento (PyPI) | 5/10 | **5,5/10** | ↑ | 4 docs centrais reescritos, CI mais robusto; bloqueadores de deps persistem |
| Documentação | 5/10 | **6,5/10** | ↑ | 4 docs de produção; multilíngue e docstrings ainda pendentes |
| **Maturidade Geral** | **6/10** | **6,2/10** | ↑ | Fundação segura, lazy ponta a ponta; cobertura e dados são os gargalos |

### Estado da suíte de testes

```
189 passed, 8 skipped | cobertura: 61% | ruff: All checks passed | mypy: 15 erros em 8 arquivos
```

### Arquivos auxiliares `climasus-data` (estado atual)

| Asset | Linhas | Dado Real? |
|---|---|---|
| `spatial/municipalities.parquet` | 5.570 | ✅ Real (IBGE) |
| `spatial/states.parquet` | 27 | ✅ Real (IBGE) |
| `spatial/regions.parquet` | 5 | ✅ Real (IBGE) |
| `climate/inmet_stations.parquet` | ? | ✅ Real (INMET) |
| `climate/inmet_observations_2023.parquet` | ? | ⚠️ **1 estação** (A701/SP) |
| `climate/idw_weights_municipality.parquet` | 5.570 | ⚠️ **Pesos fictícios** (1 estação) |
| `census/census_2022.parquet` | 5.570 | ✅ Real (IBGE) |
| `census/census_2010.parquet` | 5.570 | ❌ **Sintético** (seed=2010) |

### Funções públicas (22 exportadas via `__init__.py`)

`sus_pipeline` · `sus_import` · `sus_clean` · `sus_standardize` · `sus_filter` · `sus_variables` · `sus_aggregate` · `sus_read` · `sus_sql` · `materialize` · `sus_export` · `sus_cache_info` · `sus_cache_clear` · `sus_climate_inmet` · `sus_climate` · `sus_spatial` · `sus_census` · `sus_fill_gaps` · `sus_explore` · `sus_quality` · `update_climasus_data`

---

## 2. O que mudou desde o relatório anterior

### Sprint 1 — Segurança e Correctness (✅ concluído)

| # | Item | Status |
|---|---|---|
| 1 | SQL Injection em `sus_filter` e `sus_quality` | ✅ Corrigido |
| 2 | Path Traversal + RCE em `_raw_cache_path` | ✅ Corrigido |
| 3 | Semana epidemiológica com início errado (segunda → domingo) | ✅ Corrigido |
| 4 | `import climasus` quebrado em README/docs | ✅ Corrigido |
| 5 | Path hardcoded `C:\Users\Readone\...` nos testes | ✅ Corrigido |
| 6 | ruff ausente do CI | ✅ Corrigido |
| 7 | CID-10 uniformização (prefixo Y) | ✅ Corrigido |
| 8 | `sus_climate` sem guard de granularidade mensal | ✅ Corrigido |
| 9 | `_STAGES` id() reuse → `WeakKeyDictionary` | ✅ Corrigido |

### Fase 0 do Plano Lazy (✅ concluída)

| Item | Status |
|---|---|
| `climasus-data/` populado com 8 parquets auxiliares | ✅ |
| `census_2010.parquet` sintético gerado (seed=2010) | ✅ (marcado) |
| Enriquecimentos reescritos como JOIN lazy DuckDB | ✅ |
| `materialize(how=auto/pandas/geopandas/polars/pyarrow)` | ✅ |
| Rollback de `to_lazy.py` | ✅ |
| Stage tracking com `WeakKeyDictionary` | ✅ |
| 4 docs de pipeline reescritos | ✅ |
| 25 novos testes (164 → 189 passed) | ✅ |
| mypy adicionado ao `[dev]` | ✅ |

### Bug crítico de join corrigido (esta sessão)

Normalização 6→7 dígitos nos 3 módulos de enriquecimento via `LEFT(CAST(...AS VARCHAR), 6)` em ambos os lados dos joins:

- `enrichment/climate.py` (2 joins: IDW + direct)
- `enrichment/census.py`
- `enrichment/spatial.py`

Validação end-to-end: `sus_climate` para SP 2023 → `temp_mean ≈ 17–19°C`, zero NULLs.

### Pendente no plano lazy (❌)

| Critério de aceite | Status |
|---|---|
| Cobertura ≥ 80% | ❌ 61% |
| Benchmark RAM 10M linhas (≤ 500 MB) | ❌ não executado |
| Docs gov (3 arquivos + PLANO_v002b) | ❌ |
| `census_2010` com dados reais IBGE | ❌ |

---

## 3. Issues Ativos

> Issues ordenados por severidade. **BUG-1** e **IMP-1** são os dois mais urgentes.

---

### 🔴 IMP-1 — Dados climáticos de 1 estação invalidam todos os resultados de `sus_climate`

**Agente:** Cientista de Dados
**Severidade:** Crítica (científica)

O `inmet_observations_2023.parquet` contém dados reais de apenas 1 estação INMET (A701 — IAG/USP, São Paulo). O `idw_weights_municipality.parquet` foi construído com pesos `1.0` para todos os 5.570 municípios. O resultado é que **todos os municípios do Brasil recebem os dados meteorológicos de São Paulo** — o IDW com estação única não é interpolação, é cópia. Análises clima-saúde publicadas com esse dado são metodologicamente inválidas.

**Mitigação imediata:**
- Emitir `UserWarning` ao executar `sus_climate()` indicando cobertura limitada de estações
- Documentar claramente a limitação em `docs/enrichments.md` (já foi feito)

**Solução definitiva:** Obter dataset INMET completo (~500 estações automáticas, 2010–2023), reexecutar `scripts/build_climate.py` e `scripts/build_idw_weights.py`.

---

### 🔴 IMP-2 — `census_2010.parquet` sintético sem aviso em runtime

**Agente:** Cientista de Dados
**Severidade:** Alta

`sus_census(year=2010)` executa sem qualquer warning. Os dados sintéticos têm estrutura estatística plausível mas valores individuais arbitrários. Um usuário que use `income_per_capita` ou `gini` como covariável em modelo de Poisson produzirá coeficientes não interpretáveis sem perceber.

**Correção:** `warnings.warn(...)` em `census.py` quando `year == 2010`:

```python
import warnings
if year == 2010:
    warnings.warn(
        "census_2010.parquet contém dados SINTÉTICOS (seed=2010). "
        "Não use em análises ou publicações. "
        "Substitua por dados reais do IBGE antes de qualquer uso científico.",
        UserWarning,
        stacklevel=2,
    )
```

---

### 🟡 BUG-1 — Pattern `fetchone()[0]` sem guard de nulidade em 3 módulos

**Agente:** Revisor de Código Python
**Severidade:** Média (crash em runtime com datasets vazios)

`fetchone()` retorna `tuple[Any, ...] | None`. Os três módulos abaixo acessam `[0]` diretamente:

- `utils/quality.py:48`
- `io/materialize.py:62`
- `enrichment/fill_gaps.py:163`

Com um dataset vazio (ex: filtro muito restritivo), `fetchone()` retorna `None` e o `[0]` lança `TypeError`. Afeta diretamente o workflow do usuário: filtrar dados demográficos raros + agregar + materializar.

**Correção:** Criar helper `_fetchone_scalar` em `core/_sql.py`:

```python
def fetchone_scalar(rel, fallback=0):
    """Executa .fetchone() e retorna o primeiro campo ou fallback."""
    row = rel.fetchone()
    return row[0] if row is not None else fallback
```

---

### 🟡 IMP-3 — `climate_inmet.py` e `inmet_parser.py` com 9% e 15% de cobertura

**Agente:** Revisor de Código Python / Arquiteto de Software
**Severidade:** Média (módulo público sem testes)

`sus_climate_inmet` é exportada publicamente mas tem 278 statements com 9% de cobertura. O parser `inmet_parser.py` depende do formato textual dos CSVs do INMET (8 linhas de cabeçalho de metadados + dados) que muda sem versionamento. Qualquer quebra silenciosa de contrato da API INMET passa despercebida.

**Ação imediata:** Criar 2–3 fixtures de arquivo representando versões do formato INMET e testar o parser sem rede.

---

### 🟡 IMP-4 — `pipeline.py` com 19% de cobertura no orquestrador principal

**Agente:** Arquiteto de Software / Revisor de Código Python
**Severidade:** Média

`sus_pipeline()` é o ponto de entrada de alto nível. Com 80 dos 99 statements descobertos, os caminhos de composição de estágios, detecção de erro de sequência e interações multi-módulo não têm garantia de comportamento. O benchmark de 10M linhas (critério de aceite do plano lazy) executado sobre esse módulo pode passar por razões erradas.

---

### 🟡 IMP-5 — Normalização 6→7 dígitos replicada em 3 módulos sem extração

**Agente:** Arquiteto de Software
**Severidade:** Baixa-Média (manutenibilidade)

A lógica `LEFT(CAST(... AS VARCHAR), 6)` está replicada em `census.py`, `climate.py` e `spatial.py`. Qualquer mudança futura no padrão IBGE (ex: código de 8 dígitos) exige 3 pontos de modificação sincronizados.

**Recomendação:** Criar constante ou helper SQL em `core/_sql.py`:
```python
def normalize_municipality_join(left_col: str, right_col: str) -> str:
    return f"LEFT(CAST({left_col} AS VARCHAR), 6) = LEFT(CAST({right_col} AS VARCHAR), 6)"
```

---

### 🟡 IMP-6 — Dependências internas não verificadas no PyPI

**Agente:** Engenheiro de Pacotes
**Severidade:** Bloqueadora para publicação

`climasus-data>=1.1.0` e `climasus_readdbc_py>=0.2.1` estão listadas como dependências obrigatórias em `[dependencies]`. Se não estiverem publicadas no PyPI, `pip install climasus4py` falha imediatamente em ambiente limpo.

---

### 🟡 IMP-7 — `sus_aggregate(time="week")` usa `%W` (segunda-feira) mas `sus_variables` usa `%U` (domingo)

**Agente:** Cientista de Dados
**Severidade:** Média (inconsistência metodológica)

`sus_variables` gera `epi_week` com início no domingo (`%U`, formato SVS). `sus_aggregate(time="week")` usa `STRFTIME(..., '%Y-W%W')` — `%W` conta semanas com início na **segunda-feira**. Usuários que usam `sus_aggregate(time="week")` diretamente (sem `sus_variables`) ficam desalinhados com boletins SVS.

---

### 🟢 IMP-8 — mypy: 15 erros residuais em 8 arquivos (não bloqueante, documentado)

**Agente:** Revisor de Código Python
**Severidade:** Baixa (debt técnico acumulado)

Top 5 por impacto real em runtime:
1. `fetchone()[0]` sem guard (`quality.py`, `materialize.py`, `fill_gaps.py`) — duplicado de BUG-1
2. `load_json` retorna tipo opaco (`variables.py`) — acesso de dict unsafe
3. `sus_sql.py` assinatura conflitante com o tipo aceito
4. `importer.py:497` — `None` como sentinel em `list[int]`
5. `utils/cid.py` — `Any | None` passado como `list[str]`

---

## 4. Diagnóstico por Agente

### 4.1 Arquiteto de Software

**Nota: 6,5 / 10**

A escolha de DuckDB lazy (`DuckDBPyRelation`) como motor central é arquiteturalmente sólida e justifica a separação clara entre as camadas `core → enrichment → io`. O projeto demonstra maturidade ao corrigir anti-padrões reais (`id()` reuse → `WeakKeyDictionary`; `pandas.merge` → JOIN DuckDB). No entanto, a presença de `climate_inmet.py` (278 statements, 9% de cobertura) combinada com cobertura geral de 61% indica que a arquitetura ainda não está consolidada — há superfície pública não testada.

#### Riscos arquiteturais ativos

| # | Risco | Severidade |
|---|---|---|
| 1 | `climate_inmet.py` — 278 statements, 9% de cobertura. Exportado publicamente. | **Alta** |
| 2 | `pipeline.py` — 19% de cobertura no orquestrador de alto nível. | **Alta** |
| 3 | Normalização 6→7 dígitos replicada em 3 módulos de enriquecimento. | **Média** |
| 4 | `census_2010.parquet` sintético em produção sem sinal de qualidade na API. | **Média** |
| 5 | Fallback chain de 5 backends para `.dbc` opaca ao chamador. | **Baixa** |

#### Pontos fortes arquiteturais (não-óbvios)

- **`WeakKeyDictionary` no stage tracking** — considera o ciclo de vida real dos objetos Python, não apenas o caminho feliz. Evita vazamento de memória em sessões longas.
- **`climasus-data/` como pacote auxiliar desacoplado** — dados de referência com `manifest.json` independente permitem versionamento separado do código.
- **Trusted Publisher OIDC** — eliminação de uma classe inteira de débito de segurança de supply chain.

#### Recomendações

1. Decidir o destino de `climate_inmet.py`: deprecar, marcar como experimental com `warnings.warn`, ou investir em cobertura mínima de 60%.
2. Centralizar normalização de código municipal em `utils/cid.py` como função canônica.
3. Elevar cobertura de `pipeline.py` para ≥ 60% antes de executar o benchmark de 10M linhas.

---

### 4.2 Revisor de Código Python

**Nota: 6,4 / 10**

| Dimensão | Nota | Justificativa |
|---|---|---|
| Lint / estilo | 10/10 | ruff sem erros — base limpa |
| Cobertura de testes | 5/10 | 61% total; os 4 piores módulos concentram lógica arriscada |
| Corretude de tipos | 5/10 | 15 erros mypy; 3 são riscos reais de crash |
| Idiomaticidade Python | 8/10 | Fallbacks explícitos, uso correto de features DuckDB |
| Abstração / clareza | 7/10 | Separação de responsabilidades boa; SQL gerado em `fill_gaps.py` é potente mas difícil de depurar |

#### Déficit de cobertura — módulos críticos

| Módulo | Cover | Por que importa |
|---|---|---|
| `core/climate_inmet.py` | 9% | Integração HTTP INMET; qualquer quebra de schema da API passa despercebida |
| `utils/inmet_parser.py` | 15% | Parser de CSVs históricos INMET; bugs contaminam toda a cadeia de enriquecimento climático |
| `core/pipeline.py` | 19% | Orquestrador principal; comportamento de composição de stages não validado |
| `core/importer.py` | 44% | Download FTP + multi-UF/multi-ano; caminhos reais do usuário não cobertos |

#### Padrões positivos de código

- **Interpolação linear lazy via CTEs em `fill_gaps.py`** — sem materialização intermediária, usando window functions DuckDB (`LAST_VALUE ... IGNORE NULLS`).
- **`sus_quality()` dual-mode** — aceita `DuckDBPyRelation | pd.DataFrame` com contrato de retorno uniforme.
- **Defaults embutidos em `variables.py`** — `except FileNotFoundError` com fallback para constantes internas; funciona off-line sem stacktrace.

#### Débitos técnicos priorizados

| Prioridade | Débito |
|---|---|
| Alta | Pattern `fetchone()[0]` sem guard repetido em 3 módulos — criar `_fetchone_scalar` em `_sql.py` |
| Alta | `climate_inmet.py` + `inmet_parser.py` sem fixtures de arquivo do formato INMET |
| Média | `load_json` com tipo opaco `dict[str, object]` — `TypedDict` ou `dict[str, Any]` nos consumidores |
| Média | `[None]` como sentinel em `list[int]` em `importer.py:497` — refatorar para `Optional[list[int]]` |

---

### 4.3 Cientista de Dados

**Nota: 5,5 / 10**

O pacote tem design técnico correto e primitivas epidemiológicas bem implementadas (SE SVS, grupos CID-10, guard de granularidade). Porém, sua adequação analítica real está **severamente limitada** por deficiências de dados nos módulos de enriquecimento. O produto atual é utilizável para análise descritiva de mortalidade/internação (SIM/SIH), mas **não** para análises de exposição climática publicáveis.

#### Riscos metodológicos ativos

| # | Risco | Severidade |
|---|---|---|
| R1 | **IDW com 1 estação** — todos os municípios recebem dados de São Paulo; resultado não é interpolação, é cópia | **Crítica** |
| R2 | **Census 2010 sintético** sem warning em runtime — covariáveis de controle são ruído artificial | **Alta** |
| R3 | **`sus_aggregate(time="week")` usa `%W`** (segunda-feira) enquanto SE SVS começa no domingo | **Moderada** |
| R4 | **Ausência de denominador populacional** — contagens brutas sem `sus_rates` convidam a comparações espúrias | **Alta** |

#### Funcionalidades analíticas ausentes críticas

| Funcionalidade | Justificativa epidemiológica |
|---|---|
| `sus_rates` | Epidemiologia opera em taxas. Sem denominador, comparações espaciais são inválidas |
| DLNM | Efeito de temperatura tem latência 1–21 dias não-linear; lags fixos subestimam RR em ondas de calor |
| Detecção de ondas de calor | Padrão metodológico de epidemiologia ambiental; necessário para o contexto climático brasileiro |
| Índices de vulnerabilidade climática | Essencial para estudos de equidade em saúde ambiental (Amazônia, Nordeste) |

#### Pontos positivos de design analítico

- **Guards de estágio refletem lógica epidemiológica** — `filter → variables → aggregate → enrichment` é a sequência analiticamente correta; o guard impede o erro de juntar clima com dados individuais não-agregados.
- **Semana epidemiológica SVS com `SE{WW}/{YYYY}`** — alinha com InfoDengue e AlertaDengue; comparabilidade com boletins MS.
- **Guard de granularidade diária em `sus_climate`** — previne NULLs silenciosos em join clima-saúde com agregação mensal.

---

### 4.4 Engenheiro de Pacotes

**Nota: 5,5 / 10**

| Critério | Status |
|---|---|
| Metadados (`pyproject.toml`) | ✅ OK |
| Build system (hatchling) | ✅ OK |
| CI (ruff + pytest, 3.10–3.13) | ✅ OK |
| Trusted Publisher OIDC | ✅ OK |
| Cobertura | ⚠️ 61% (meta: 80%) |
| Documentação central | ✅ 4 docs de produção |
| Documentação multilíngue | ❌ `pt/`, `en/`, `es/` apenas com `index.md` |
| Deps internas no PyPI | ❓ Não verificado |
| Workflow de publicação | ❌ Não existe |

O pacote **não está pronto para PyPI público**. Pode ser publicado no TestPyPI como Alpha privado com aviso explícito.

#### Bloqueadores para publicação

| # | Bloqueador | Critério de aceite |
|---|---|---|
| B1 | Deps internas no PyPI | `pip install climasus-data>=1.1.0 climasus_readdbc_py>=0.2.1` funciona em ambiente limpo |
| B2 | Cobertura < 80% | `pytest --cov-fail-under=80` passa no CI |
| B3 | `allow-direct-references = true` no hatch | Remover e confirmar build limpo |
| B4 | mypy ausente do CI | Step `mypy src/climasus4py --ignore-missing-imports` no workflow sem erros `error:` |

#### Estado da documentação

| Seção | Status |
|---|---|
| `docs/querying.md` | ✅ Reescrita — 3 entradas, encadeamento, saídas |
| `docs/pipeline-order.md` | ✅ Reescrita — diagrama Mermaid, tabelas |
| `docs/materialize.md` | ✅ Reescrita — todos os formatos com exemplos em tabs |
| `docs/enrichments.md` | ✅ Reescrita — 4 enriquecimentos completos |
| `docs/index.md` | ⚠️ Não revisado nesta sprint |
| `docs/en/`, `docs/pt/`, `docs/es/` | ❌ Apenas `index.md` em cada |
| `gen_ref_pages.py` | ⚠️ 10+ funções sem docstring → páginas de referência vazias |
| `CHANGELOG.md` | ✅ Entrada 0.2.0 presente |

#### Gaps de CI/CD

1. **Sem `--cov-fail-under`** no step de pytest — CI passa com 61%.
2. **mypy não executa no CI** — verificação de tipos apenas local/manual.
3. **Sem `publish.yml`** — Trusted Publisher configurado, mas trigger de upload não existe.

---

## 5. Plano de Ação — Sprint 2

> **Tema:** Qualidade analítica, cobertura de testes e prontidão para publicação

### Prioridade 1 — Dados reais INMET multi-estação *(desbloqueador científico)*

**Impacto:** Invalida os resultados de `sus_climate` sem isso.

- [ ] Executar `scripts/build_climate.py` com CSVs históricos INMET (~500 estações automáticas)
- [ ] Executar `scripts/build_idw_weights.py` para recalcular pesos IDW com distâncias reais
- [ ] Reconstruir `inmet_observations_YYYY.parquet` para 2010–2023
- [ ] Atualizar `manifest.json` com novos MD5s

### Prioridade 2 — Warning em `sus_census(year=2010)` *(baixo custo, alto impacto científico)*

- [ ] `warnings.warn(...)` com texto explícito em `enrichment/census.py`
- [ ] Teste: verificar que `pytest -W error::UserWarning` captura o warning

### Prioridade 3 — Cobertura ≥ 75% (threshold progressivo) *(critério de aceite do plano lazy)*

**Módulos prioritários:**

| Módulo | Cobertura atual | Meta Sprint 2 | Estratégia |
|---|---|---|---|
| `core/climate_inmet.py` | 9% | 50% | Mockar `requests`, fixtures de resposta JSON |
| `utils/inmet_parser.py` | 15% | 60% | Fixtures de arquivos CSV INMET histórico |
| `core/pipeline.py` | 19% | 60% | Mockar `sus_import` internamente |
| `core/importer.py` | 44% | 65% | Mockar urllib/FTP, DBC → DBF offline |
| `io/materialize.py` | 73% | 85% | Testes para alertas de tamanho |

### Prioridade 4 — Correção do pattern `fetchone()[0]` *(crash em runtime)*

- [ ] Criar `_fetchone_scalar(rel, fallback=0)` em `core/_sql.py`
- [ ] Adotar em `utils/quality.py`, `io/materialize.py`, `enrichment/fill_gaps.py`
- [ ] Teste com dataset propositalmente vazio após filtro restritivo

### Prioridade 5 — `sus_aggregate(time="week")` — alinhar com SVS *(correctness)*

- [ ] Verificar e corrigir `%W` → `%U` em `aggregate.py` para `time="week"`
- [ ] Teste com datas de referência do calendário SVS 2025

### Prioridade 6 — CI/CD: enforcement de cobertura + mypy *(qualidade de release)*

- [ ] Adicionar `--cov-fail-under=75` ao step de pytest no CI
- [ ] Adicionar step mypy ao CI (`--ignore-missing-imports`)
- [ ] Criar `publish.yml` com trigger em tag `v*` via Trusted Publisher OIDC

### Ordem de execução recomendada

```
Dia 1 — Dados + warnings:
  P1: script de dados INMET (pode rodar em background)
  P2: sus_census warning (30 min)

Dia 2 — Cobertura (maior esforço):
  P3: testes climate_inmet + inmet_parser (com fixtures)
  P3: testes pipeline.py + importer.py (com mocks)

Dia 3 — Correctness + CI:
  P4: fetchone_scalar helper
  P5: sus_aggregate week fix
  P6: CI enforcement + publish.yml
```

---

## Links de referência

- Relatório anterior: [`climasus4py/REVIEW_REPORT.md`](REVIEW_REPORT.md)
- Sprint 1 (concluído): [`governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md`](../governanca/3-planos/em-execucao/2026-05-03-sprint1-seguranca-correctness.md)
- Plano lazy (em execução): [`governanca/3-planos/em-execucao/2026-05-02-py-pipeline-lazy-completo.md`](../governanca/3-planos/em-execucao/2026-05-02-py-pipeline-lazy-completo.md)
- Docs pipeline: [`docs/querying.md`](docs/querying.md) · [`docs/pipeline-order.md`](docs/pipeline-order.md) · [`docs/materialize.md`](docs/materialize.md) · [`docs/enrichments.md`](docs/enrichments.md)
