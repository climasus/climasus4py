# REVIEW REPORT v3 — `climasus4py` v0.3.0

*Data: 8 maio 2026 · Agentes: SoftwareArchitect, PythonCodeReviewer, DataScientist, Package Engineer*

---

## Panorama Geral

| Dimensão | v0.2.0 | v0.3.0 | Δ |
|---|---|---|---|
| Testes | 189 passed, 8 skip | **494 passed, 19 skip** | +305 |
| Cobertura | 61% | **80%** | +19 pp |
| mypy erros | 15 em 8 arquivos | **0 erros** | zerado |
| ruff | limpo | limpo | = |
| Funções públicas (`__all__`) | 22 | **31** | +9 |
| Versão | 0.2.0 | 0.3.0 / **0.3.2** (pyproject) | divergente |
| publish.yml (OIDC) | ausente | **presente** | + |
| CI gates (cov + mypy) | ausentes | **presentes** | + |

### Notas por agente

| Agente | v0.2.0 | v0.3.0 | Δ |
|---|---|---|---|
| SoftwareArchitect | 6,5/10 | **7,5/10** | +1,0 |
| PythonCodeReviewer | 6,4/10 | **7,6/10** | +1,2 |
| DataScientist | 5,5/10 | **6,8/10** | +1,3 |
| Package Engineer | 5,5/10 | **6,5/10** | +1,0 |
| **Média** | **5,98** | **7,10** | **+1,12** |

---

## O que mudou desde o relatório anterior (v0.2.0)

### Conquistado

| Item | Status |
|---|---|
| mypy 15 erros → 0 | ✅ zerado |
| Cobertura 61% → 80% | ✅ meta atingida |
| `inmet_parser.py` 15% → 100% | ✅ crítico corrigido |
| `pipeline.py` 19% → 85% | ✅ crítico corrigido |
| `importer.py` 44% → 75% | ✅ melhora substancial |
| `fill_gaps.py` 78% → 100% | ✅ |
| `spatial.py` 88% → 100% | ✅ |
| 9 novos módulos com lazy ponta a ponta | ✅ disciplina mantida |
| `sus_meta()` — rastreabilidade de pipeline | ✅ novo |
| `sus_climate_aggregate()` — séries temporais | ✅ novo |
| `sus_climate_compute_indicators()` — bioclimáticos | ✅ novo |
| `sus_climate_fill_inmet()` — gap-fill XGBoost/linear | ✅ novo |
| `list_disease_groups()` / `get_disease_group_details()` | ✅ novo |
| `sus_chat()` — paridade climasus4r | ✅ novo |
| Warning em `sus_census(year=2010)` (dado sintético) | ✅ |
| `sus_aggregate(time="week")` alinhado com SE SVS (`%U`) | ✅ |
| `publish.yml` com OIDC | ✅ |
| CI gates (cov + mypy) | ✅ |

### API renomeada (breaking changes)

| v0.2.0 | v0.3.0 |
|---|---|
| `sus_import` | `sus_data_import` |
| `sus_clean` | `sus_data_clean_encoding` |
| `sus_standardize` | `sus_data_standardize` |
| `sus_aggregate` | `sus_data_aggregate` |
| `sus_variables` | `sus_data_create_variables` |
| `sus_spatial` | `sus_spatial_join` |
| `sus_quality` | `sus_data_quality_report` |
| `sus_read` | `sus_data_read` |

Sem aliases de compatibilidade. Semver pré-1.0 (`0.x`) permite breaking em minor bump — justificável e documentado no CHANGELOG.

---

## Issues Ativos (classificados por criticidade)

### 🔴 BLOQUEADORES (impedem publicação ou comprometem integridade científica)

#### BLK-1 — Divergência de versão `_version.py` vs `pyproject.toml`
```
pyproject.toml → version = "0.3.2"
_version.py    → __version__ = "0.3.0"
```
O wheel gerado terá metadado `0.3.2` mas `climasus4py.__version__` retornará `"0.3.0"` em runtime. Quebra verificações em logs e `importlib.metadata`. Correção trivial.

#### BLK-2 — `climasus-data` e `climasus_readdbc_py` não verificados no PyPI público
`pip install climasus4py` em ambiente limpo tentará resolver essas deps. Se não estiverem no índice padrão, a instalação falhará com `ResolutionImpossible`. O `allow-direct-references = true` confirma uso de `dep @ git+https://...` — não permitido em releases PyPI. Verificar:
```bash
pip index versions climasus-data
pip index versions climasus_readdbc_py
```

#### BLK-3 — CHD implementado incorretamente (densidade ≠ consecutividade)
```sql
-- implementação atual — ERRADA
SUM(CASE WHEN Tmax > 32 THEN 1 ELSE 0 END)
OVER (PARTITION BY station ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```
A sequência `Q, F, Q, Q, Q, Q, Q` retorna 6 mas não há consecutividade. O índice correto ETCCDI *CDD* exige contagem de *runs* via LAG acumulado. O nome `consecutive_hot_days` é **epidemiologicamente enganoso** — análises de risco relativo construídas sobre esse campo estarão infladas em períodos com dias alternados de calor.

#### BLK-4 — HWD underflags o início de episódios (67% de subnotificação em ondas de 3 dias)
```sql
-- implementação atual — INCOMPLETA
CASE WHEN Tmax > 35 AND LAG(Tmax,1) > 35 AND LAG(Tmax,2) > 35 THEN 1 ELSE 0 END
```
Detecta apenas o 3º+ dia; **os dois primeiros dias de todo episódio retornam 0**. Em estudos de carga de mortalidade por calor (Gasparrini & Armstrong, *Epidemiology* 2011), isso gera viés negativo sistemático nas ondas curtas. Uma onda de 3 dias → apenas 1 dia flagged = 67% de subnotificação do evento.

#### BLK-5 — WBGT documentado mas não implementado
O docstring do módulo `climate_indicators.py` documenta "WBGT — Liljegren et al. (2008)" mas não existe chave `wbgt` em `_INDICATOR_DEFS`. Nenhum output de WBGT é gerado. Discrepância documentação vs. implementação é risco de integridade científica.

---

### 🟡 DÉBITOS TÉCNICOS NOVOS (introduzidos em v0.3.0)

#### DEB-1 — `hashlib.md5` sem `usedforsecurity=False` (FIPS)
```python
# climate_fill.py:151
key = hashlib.md5(f"{station}_{target_var}".encode()).hexdigest()[:12]
```
Em Python ≥ 3.9 com FIPS habilitado, levanta `ValueError`. Correção:
```python
key = hashlib.md5(f"{station}_{target_var}".encode(), usedforsecurity=False).hexdigest()
```
Adicionalmente, hash truncado a 12 hex chars (48 bits) cria risco teórico de colisão — usar `.hexdigest()` completo.

#### DEB-2 — Cache de modelos sem invalidação por versão de features
```python
# climate_fill.py
if cache_file.exists():
    model = joblib.load(cache_file)  # sem verificar se feature_cols mudou
```
Se `_engineer_features` for alterado em versão futura, modelos antigos em `~/.climasus4py/models/` serão carregados com features incompatíveis — erro silencioso que produz predições incorretas. Estratégia mínima: incluir hash da versão do pacote ou das `feature_cols` no nome do arquivo de cache.

#### DEB-3 — `_detect_datetime_col` frágil em DataFrames enriquecidos
```python
for col in df.columns:
    low = col.lower()
    if "date" in low or "time" in low or low == "dt":
        return col  # primeira coluna que satisfaz vence
```
Com DataFrames enriquecidos (múltiplas colunas com "date" no nome), o comportamento é não-determinístico e depende da ordem das colunas. Priorizar dtype `datetime64` antes de recorrer à heurística de nome.

#### DEB-4 — `allow-direct-references = true` incompatível com PyPI
```toml
[tool.hatch.metadata]
allow-direct-references = true
```
Sinaliza má prática e bloqueia publicação no PyPI público. Remover após resolver BLK-2.

---

### 🟡 LIMITAÇÕES CIENTÍFICAS PERSISTENTES

#### LIM-1 — Cobertura climática de 1 estação INMET (A701/SP)
Qualquer análise de associação clima-saúde com dados de múltiplos municípios ou regiões climáticas distintas estará sujeita a **erro de exposição ecológica grave** (*exposure misclassification*). A estação A701 tem altitude ~760 m e microclima urbano de SP — não representativa sequer do estado de SP. Inviabiliza análises nacionais.

#### LIM-2 — `census_2010.parquet` ainda sintético
Warning agora emitido em runtime — positivo. Mas qualquer análise de taxa padronizada por idade/sexo, indicadores de deprivação ou estrutura etária usada para standardização de mortalidade permanece baseada em ficção estatística.

#### LIM-3 — `climate_fill.py` quebra a laziness (RAM unbounded)
`climate_fill.py` converte `DuckDBPyRelation` para `pandas.DataFrame` antes de qualquer operação, materializando todo o dataset em RAM. Para séries com múltiplas estações após expansão INMET, o risco é proibitivo. Cria assimetria: indicadores são lazy, mas os dados de entrada para eles podem estar em RAM após fill.

---

### 🟡 RISCOS ARQUITETURAIS (SoftwareArchitect)

#### ARQ-1 — 4 funções fora do padrão de nomeação
| Função atual | Padrão esperado |
|---|---|
| `sus_filter` | `sus_data_filter` |
| `sus_explore` | `sus_data_explore` |
| `collect_arrow` | `sus_collect_arrow` ou remover de `__all__` |
| `materialize` | `sus_materialize` ou remover de `__all__` |

`collect_arrow` e `materialize` são utilitários de baixo nível que vazam para a API pública sem o prefixo `sus_`. **Custo zero agora (nenhuma versão publicada); alto após qualquer release.**

#### ARQ-2 — Model cache sem ciclo de vida (`~/.climasus4py/models/`)
`sus_cache_clear()` existe mas cobre apenas cache de dados, não modelos. Em uso operacional com múltiplas estações e variáveis, o diretório cresce indefinidamente. Side effect implícito em função de enriquecimento é o padrão mais difícil de depurar em pipelines automatizados.

#### ARQ-3 — Heat Index sem guarda de domínio
A equação Rothfusz tem domínio restrito: T ≥ 27°C e UR ≥ 40%. Sem guarda no SQL, valores fora do domínio geram `hi_c < T` — biologicamente absurdo. Solução:
```sql
CASE WHEN T >= 27 AND RH >= 40 THEN <fórmula> ELSE NULL END
```

#### ARQ-4 — `WeakKeyDictionary` em `_stage.py` pode perder metadados silenciosamente
Se DuckDB retornar um novo objeto Python para a mesma relação lógica (após `.filter()`, `.project()`, etc.), os metadados são perdidos — `sus_meta()` retorna `None` sem aviso. A hashabilidade de `DuckDBPyRelation` não é garantida entre versões do duckdb.

---

### 🟡 RESSALVAS CIENTÍFICAS MENORES

#### RSV-1 — `APPROX_QUANTILE` inadequado para limiares de classificação epidemiológica
Os percentis p10/p90 em `sus_climate_aggregate` usam *reservoir sampling* (t-digest). O erro relativo nos extremos pode atingir 0,5–2% — aceitável para exploração, mas **não para limiares de classificação** (ex: tercil de exposição em estudos caso-crossover). Documentar e oferecer `exact_quantile=True`.

#### RSV-2 — Velocidade do vento para Apparent Temperature (referência de altura)
A coluna mapeada é `ws_2_m_s`, mas a fórmula BOM foi derivada para velocidade a 10 m (*Steadman 1994*). INMET mede a 10 m, mas a nomenclatura interna cria risco de confusão. Diferenças 2 m→10 m representam 15–30% no perfil logarítmico, **superestimando AT** quando o dado real for a 2 m.

---

## Diagnóstico por Agente

### SoftwareArchitect — 7,5/10 (+1,0 vs v0.2.0)

**Pontos fortes novos:**
1. mypy zerado com tipagem real, não supressões — contratos reais para novos contribuidores
2. Lazy ponta a ponta mantido em `climate_aggregate` e `climate_indicators` sob pressão de window functions complexas
3. Cobertura de 80% com 494 testes tem valor de regressão real, não apenas nominal

**Riscos ativos:** ARQ-1 a ARQ-4 acima.

**Recomendações:**
1. Completar padrão `sus_data_*` / `sus_*` antes de qualquer publicação (ARQ-1) — custo zero agora
2. Adicionar `sus_cache_clear(type="models")` para ciclo de vida do model cache (ARQ-2)
3. Elevar `climate_fill.py` (51%) e `viz/climate_plot.py` (32%) para 70%+ no próximo ciclo

---

### PythonCodeReviewer — 7,6/10 (+1,2 vs v0.2.0)

**Conquistas objetivas:**
1. Eliminação total de erros de tipagem (15 → 0) — reduz surpresas em produção
2. +305 testes com fixtures determinísticos (`np.random.default_rng(seed)`), `pytest.importorskip`, `pytest.warns(UserWarning, match=...)` — qualidade acima de cobertura nominal
3. Arquitetura de dep opcional com fallback explícito (XGBoost/linear + `[plot]`) — padrão reproduzível

**Déficits de cobertura residuais:**
| Módulo | Cobertura | Estratégia |
|---|---|---|
| `core/climate_inmet.py` | 45% | Fixtures CSV sintéticos + mock HTTP (`responses`/`pytest-httpx`) |
| `enrichment/climate_fill.py` | 51% | Testes XGBoost reais via `pytest.importorskip("xgboost")` + fixture multi-estação |
| `viz/climate_plot.py` | 32% | Validar camadas do objeto `ggplot` sem `draw()` (inspecionar `p.layers`, `p.mapping`) |

**Débitos novos:** DEB-1, DEB-2, DEB-3 acima.

**Padrão positivo a replicar:** I18n via dict estático `_I18N` com chave de locale em `climate_plot.py` — replicar para os `UserWarning` em `climate_fill.py` (hoje apenas inglês).

---

### DataScientist — 6,8/10 (+1,3 vs v0.2.0)

**O que a v0.3.0 habilitou de novo:**
1. Séries mensais de p10/p90 de temperatura + HI + AT — insumos diretos de estudos caso-crossover com DLNM (Gasparrini et al., *Stat Med* 2010)
2. Detecção de episódios de calor extremo linkáveis ao SIM-DO — base de estudos de carga de mortalidade atribuível a ondas de calor (metodologia EuroMOMO/WHO-PAHO)
3. Classificação nosológica `climate_sensitive_only=True` — reduz risco de erro de especificação do desfecho em estudos epidemiológicos

**Erros metodológicos críticos:** BLK-3 (CHD), BLK-4 (HWD), BLK-5 (WBGT ausente), ARQ-3 (Heat Index sem guarda).

**Limitações persistentes:** LIM-1, LIM-2, LIM-3 acima.

---

### Package Engineer — 6,5/10 (+1,0 vs v0.2.0)

**Melhorias concretas:**
- publish.yml com OIDC — caminho para PyPI via Trusted Publisher
- CI gates de cobertura (80%) e mypy agora presentes
- API renomeada documentada no CHANGELOG com bloco de migração

**Bloqueadores:** BLK-1 (versão), BLK-2 (deps PyPI), BLK-4 (allow-direct-references).

**Documentação a atualizar:**

| Documento | Estado |
|---|---|
| `docs/querying.md` | ✅ válido (nomes v0.3.0) |
| `docs/pipeline-order.md` | ✅ válido |
| `docs/materialize.md` | Verificar referência a `sus_data_read` |
| `docs/enrichments.md` | ✅ válido (cobre novos módulos climáticos) |
| `README.md` (PyPI page) | ❌ Faltam `[xgboost]` e `[plot]` na seção de extras |
| `docs/index.md` | ❌ Idem — adicionar opt-ins com requisitos de sistema |

---

## Plano de Ação — Sprint 3

### P0 — Corretude científica (antes de qualquer análise)

| Ação | Arquivo | Esforço |
|---|---|---|
| Corrigir CHD para contagem de *runs* reais (BLK-3) | `enrichment/climate_indicators.py` | M |
| Corrigir HWD para flagear **todos** os dias do episódio (BLK-4) | `enrichment/climate_indicators.py` | P |
| Implementar WBGT ou remover do docstring (BLK-5) | `enrichment/climate_indicators.py` | M |
| Adicionar guarda de domínio no Heat Index (ARQ-3) | `enrichment/climate_indicators.py` | P |

### P1 — Bloqueadores de publicação

| Ação | Arquivo | Esforço |
|---|---|---|
| Corrigir `_version.py` → `"0.3.2"` (BLK-1) | `climasus4py/_version.py` | P |
| Verificar `climasus-data` e `climasus_readdbc_py` no PyPI (BLK-2) | — | P |
| Remover `allow-direct-references` após confirmar deps (DEB-4) | `pyproject.toml` | P |
| Atualizar README com `[xgboost]` e `[plot]` | `README.md`, `docs/index.md` | P |

### P2 — Segurança e robustez

| Ação | Arquivo | Esforço |
|---|---|---|
| `hashlib.md5(..., usedforsecurity=False)` (DEB-1) | `enrichment/climate_fill.py` | P |
| Cache de modelos com hash de versão de features (DEB-2) | `enrichment/climate_fill.py` | M |
| `_detect_datetime_col` — priorizar dtype datetime64 (DEB-3) | `enrichment/climate_fill.py` | P |
| Completar padrão de nomeação antes de publicar (ARQ-1) | `__init__.py` + renameações | M |
| `sus_cache_clear(type="models")` para model cache (ARQ-2) | `io/cache.py` | M |

### P3 — Cobertura e qualidade de testes

| Ação | Meta | Módulo |
|---|---|---|
| `climate_fill.py` 51% → 70% | Mock HTTP + fixture multi-estação XGBoost | `enrichment/climate_fill.py` |
| `viz/climate_plot.py` 32% → 60% | Inspeção de `p.layers` + `p.mapping` | `viz/climate_plot.py` |
| `climate_inmet.py` 45% → 60% | Fixtures CSV offline + mock HTTP | `core/climate_inmet.py` |

### P4 — Desbloqueador científico crítico (próxima sprint)

| Ação | Impacto |
|---|---|
| Ampliar dados INMET para 1 estação/UF (27 estações mínimo) | Habilita análises nacionais reais |
| `census_2010.parquet` com dados reais IBGE | Remove limitação LIM-2 |
| `APPROX_QUANTILE` → oferecer `exact_quantile=True` (RSV-1) | Consistência para estudos caso-crossover |

---

## Referências Científicas Relevantes

- Rothfusz (1990) — Heat Index: *NWS Tech. Attachment SR/SSD 90-23*
- Thom (1959) — THI: *Weatherwise* 12(2):57–59
- Steadman (1994) — Apparent Temperature: *J. Appl. Meteor.* 33:1674–1674
- Liljegren et al. (2008) — WBGT: *J Occup Environ Hyg* 5(10):645–655
- Gasparrini & Armstrong (2011) — DLNM para mortalidade por calor: *Epidemiology* 22(6):793–803
- Donat et al. (2013) — Índices ETCCDI: *J Geophys Res* 118:2098–2118
- Gandin (1965) — Optimal interpolation para dados meteorológicos
