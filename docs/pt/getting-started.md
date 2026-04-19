# Primeiros Passos

Este guia cobre instalação, configuração inicial e os fluxos de uso mais comuns — do pipeline completo à API por etapas.

---

## 1. Instalação

=== "Instalação básica"

    ```bash
    pip install climasus4py
    ```

=== "Com extras opcionais"

    ```bash
    # Dados espaciais (geobr, geopandas)
    pip install "climasus4py[spatial]"

    # ML e preenchimento de lacunas (xgboost)
    pip install "climasus4py[ml]"

    # Exportação para Excel
    pip install "climasus4py[excel]"

    # Tudo
    pip install "climasus4py[spatial,ml,excel]"
    ```

!!! info "Requisito Python"
    Python 3.10 ou superior. O pacote usa DuckDB internamente — não é necessário instalar ou configurar banco de dados.

---

## 2. Configurar `climasus-data`

Parte da API depende de metadados locais: grupos de doenças (CID-10), dicionários de colunas DATASUS, códigos de UF e regiões. Esses metadados estão no repositório [`climasus-data`](https://github.com/climasus/climasus-data).

```python
import climasus as cs

# Baixa/atualiza os metadados no diretório padrão (~/.climasus_data/)
cs.update_climasus_data()
```

Ou, se preferir um diretório personalizado:

```bash
git clone https://github.com/climasus/climasus-data.git /caminho/local
```

```python
import os
os.environ["CLIMASUS_DATA_DIR"] = "/caminho/local/climasus-data"
```

!!! warning "Atenção"
    Sem os metadados configurados, funções como `sus_filter(groups=...)` e `sus_standardize()` não funcionarão corretamente.

---

## 3. Pipeline completo (`sus_pipeline`)

`sus_pipeline` é o ponto de entrada principal: faz download automático dos arquivos DBC do DATASUS, converte para Parquet (cache), e executa todo o fluxo de limpeza → padronização → filtragem → variáveis → agregação.

```python
import climasus as cs

result = cs.sus_pipeline(
    system="SIM-DO",           # Sistema DATASUS
    uf="SP",                   # UF (ou lista, ou "all", ou região)
    year=[2021, 2022, 2023],   # Ano(s)
    groups="respiratory",      # Grupo de doenças (opcional)
    age_min=18,                # Filtro de idade mínima (opcional)
    age_max=80,                # Filtro de idade máxima (opcional)
    time="month",              # Granularidade temporal
    geo="state",               # Nível geográfico
    lang="pt",                 # Idioma das colunas de saída
)

# Relação lazy — materializa apenas aqui:
df = result.df()
print(df.shape)
print(df.head())
```

### Opções de `uf`

```python
# UF única
sus_pipeline("SIM-DO", uf="SP", year=2023)

# Múltiplas UFs
sus_pipeline("SIM-DO", uf=["SP", "RJ", "MG"], year=2023)

# Todas as UFs do país
sus_pipeline("SIM-DO", uf="all", year=2023)

# Por região
sus_pipeline("SIM-DO", uf="Sudeste", year=2023)
```

### Salvar resultado automaticamente

```python
sus_pipeline(
    "SIM-DO", "SP", 2023,
    groups="dengue",
    output="output/dengue_sp_2023.parquet",  # ou .csv, .xlsx
)
```

---

## 4. Fluxo por etapas

Para maior controle, cada etapa do pipeline pode ser executada separadamente. Todas retornam `duckdb.DuckDBPyRelation` — a relação permanece lazy.

```python
import climasus as cs

# 1. Importar (download + cache + leitura lazy)
rel = cs.sus_import("SIM-DO", uf="RJ", year=[2022, 2023])
print(f"Colunas: {rel.columns}")

# 2. Limpar encoding e tipos (campos string/numérico/data DATASUS)
rel = cs.sus_clean(rel)

# 3. Padronizar nomes de colunas (DATASUS → nomes legíveis)
rel = cs.sus_standardize(rel, lang="pt", system="SIM-DO")
print(f"Colunas padronizadas: {rel.columns[:5]}")

# 4. Filtrar — por CID-10 nomeado, código direto, faixa etária, sexo
rel = cs.sus_filter(
    rel,
    groups=["dengue", "zika_chikungunya"],  # grupos nomeados
    age_min=0,
    age_max=14,                             # crianças
    sex="F",                                # sexo feminino
)

# 5. Criar variáveis derivadas
rel = cs.sus_variables(
    rel,
    age_group="who",    # faixas etárias WHO
    epi_week=True,      # semana epidemiológica
    season=True,        # estação do ano (hemisfério sul)
)

# 6. Agregar
rel = cs.sus_aggregate(
    rel,
    time="month",
    geo="municipality",
    extra_groups=["sex", "age_group"],  # grupos extras
)

# 7. Materializar
df = rel.df()
print(df.head())

# 8. Exportar
cs.sus_export(rel, "output/resultado.parquet")
```

---

## 5. Filtros avançados

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", uf="SP", year=2023)
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, lang="pt", system="SIM-DO")

# Filtrar por códigos CID-10 diretos (incluindo faixas)
rel = cs.sus_filter(rel, codes=["A90", "A91", "B50-B54"])

# Filtrar por município específico (código IBGE 7 dígitos)
rel = cs.sus_filter(rel, municipality=["3550308"])  # São Paulo capital

# Filtrar por data
rel = cs.sus_filter(rel, date_start="2023-06-01", date_end="2023-12-31")

# Contar sem materializar completamente
print(f"Registros: {rel.count('*').fetchone()[0]:,}")
```

---

## 6. Enriquecimento com dados climáticos

```python
import climasus as cs
import pandas as pd

# Dados climáticos do INMET (deve ter: municipality_code, date + variáveis)
climate_df = pd.read_parquet("dados/inmet_sp_2023.parquet")

rel = cs.sus_pipeline("SIM-DO", "SP", 2023, geo="municipality")

# Juntar com dados climáticos + lags de 7 e 14 dias
enriched = cs.sus_climate(
    rel,
    climate_df,
    lags=[7, 14],   # adiciona colunas temp_lag7d, prec_lag14d, etc.
)
print(enriched.filter(like="_lag7d").columns.tolist())
```

---

## 7. Cache e gestão de dados

```python
import climasus as cs

# Ver o que está em cache (tamanho, timestamp, UF, sistema)
info = cs.sus_cache_info()
print(info)

# Limpar cache de um sistema específico
cs.sus_cache_clear(system="SIM-DO")

# Limpar todo o cache
cs.sus_cache_clear()
```

---

## 8. Exploração rápida de dados

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", uf="SP", year=2023)

# Resumo rápido do dataset bruto (distribuição por coluna)
cs.sus_explore(rel)

# Relatório de qualidade — missing values, inconsistências
report = cs.sus_quality(rel)
print(report)
```

---

## Referência rápida de parâmetros

| Parâmetro | Tipo | Valores aceitos |
|-----------|------|-----------------|
| `system` | `str` | `"SIM-DO"`, `"SINASC"`, `"SIH"`, `"SIA"` |
| `uf` | `str` ou `list[str]` | `"SP"`, `["SP", "RJ"]`, `"all"`, `"Sudeste"` |
| `year` | `int` ou `list[int]` | `2023`, `[2020, 2021, 2022]` |
| `time` | `str` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `str` | `"state"`, `"municipality"`, `"region"`, `"country"` |
| `lang` | `str` | `"pt"`, `"en"`, `"es"` |
| `groups` | `str` ou `list[str]` | Ver [tabela de grupos](index.md#grupos-de-doenças-disponíveis) |
| `age_group` | `str` ou `list[int]` | `"who"`, `"decadal"`, `[0, 18, 65]` |
