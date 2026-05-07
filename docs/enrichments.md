# Enriquecimentos lazy

Os quatro enriquecimentos do `climasus4py` adicionam dimensões externas aos
dados de saúde usando `JOIN` em SQL no DuckDB. Todos retornam
`DuckDBPyRelation` — sem materialização interna nos modos padrão.

!!! info "Dependência: `climasus-data`"
    Os enriquecimentos leem Parquets auxiliares de `climasus-data/`.  
    Por padrão, o pacote procura o diretório `climasus-data/` subindo a partir
    do seu arquivo de dados. Para apontar explicitamente:  
    `export CLIMASUS_DATA_DIR=/caminho/para/climasus-data`

!!! note "Códigos de município: DATASUS vs IBGE"
    O DATASUS usa códigos de 6 dígitos (ex: `355030`). Os assets de
    `climasus-data` usam 7 dígitos IBGE (ex: `3550308`). Os joins normalizam
    automaticamente via `LEFT(..., 6)` em ambos os lados.

## `sus_spatial_join` — geometrias e territórios

Adicioa `geometry_wkt` e nome geográfico ao resultado. Necessário para
mapas com `materialize(how="geopandas")`.

```python
rel = cs.sus_spatial_join(rel)                              # nível padrão: municipality
rel = cs.sus_spatial_join(rel, geo_level="state")
rel = cs.sus_spatial_join(rel, geo_level="region")
```

Após o join, a relação terá as colunas `spatial_name` e `geometry_wkt`.

### Asset personalizado

Passe um Parquet próprio com as colunas mínimas do nível:

```python
rel = cs.sus_spatial_join(
    rel,
    geo_level="municipality",
    spatial_path="meus_assets/municipios_custom.parquet",
)
```

| Nível | Chave de join | Colunas mínimas |
|---|---|---|
| `municipality` | `code_muni` | `name`, `geometry_wkt` |
| `state` | `state` | `name`, `geometry_wkt` |
| `region` | `region` | `name`, `geometry_wkt` |

### Exemplo: mapa de mortalidade

```python
rel = cs.sus_data_aggregate(rel, time="year", geo="municipality")
rel = cs.sus_spatial_join(rel)
gdf = cs.materialize(rel, how="geopandas")
gdf.plot(column="count", scheme="quantiles", legend=True)
```

---

## `sus_census` — indicadores socioeconômicos IBGE

Junta indicadores do Censo Demográfico ao nível de município.

```python
rel = cs.sus_census(rel, year=2022)
rel = cs.sus_census(rel, year=2010, variables=["population_2010", "income_per_capita"])
```

### Variáveis disponíveis

=== "Censo 2022"

    | Variável | Descrição |
    |---|---|
    | `municipality_name` | Nome do município |
    | `state_code` | Sigla do estado |
    | `is_capital` | É capital estadual? |
    | `population_2021` | Estimativa populacional 2021 |
    | `population_2025` | Estimativa populacional 2025 |
    | `latitude`, `longitude` | Coordenadas do centroide |

=== "Censo 2010"

    | Variável | Descrição |
    |---|---|
    | `population_2010` | Estimativa populacional 2010 |
    | `pct_urban` | % população urbana |
    | `pct_literacy` | Taxa de alfabetização |
    | `income_per_capita` | Renda per capita (R$) |
    | `gini` | Índice de Gini |
    | `pct_sanitation` | % acesso a saneamento básico |

!!! warning "Dados sintéticos — Censo 2010"
    O asset `census_2010.parquet` atual contém dados sintéticos (seed=2010)
    gerados para desenvolvimento. Substitua pelo dado real do IBGE quando
    disponível.

---

## `sus_climate` — variáveis climáticas INMET

Junta observações diárias de estações meteorológicas INMET usando pesos IDW
(Inverse Distance Weighting) pré-computados por município.

```python
rel = cs.sus_climate(rel, variables=["temp_mean", "precipitation"], years=[2023])
```

!!! warning "Granularidade obrigatória: dia"
    A coluna de data deve ter granularidade diária (`YYYY-MM-DD`).  
    Se a coluna estiver em formato mensal (`YYYY-MM`), um `ValueError` é
    levantado. Agregue primeiro e depois junte: use `sus_data_aggregate(time="day")`
    ou filtre os dados antes.

### Parâmetros

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `variables` | `["temp_mean", "precipitation"]` | Variáveis climáticas a juntar |
| `years` | Todos disponíveis | Anos das observações INMET |
| `lags` | `[]` | Defasagens em dias (ex: `[1, 7]` → `temp_mean_lag1d`, `temp_mean_lag7d`) |
| `idw` | `True` | Usa pesos IDW; `False` usa join direto (obs com `municipality_code`) |

### Variáveis climáticas disponíveis

| Variável | Unidade |
|---|---|
| `temp_mean` | °C (média diária) |
| `temp_max` | °C (máxima diária) |
| `temp_min` | °C (mínima diária) |
| `precipitation` | mm (acumulado diário) |
| `humidity` | % (umidade relativa média) |
| `pressure` | hPa (pressão atmosférica média) |
| `wind_speed` | m/s (velocidade do vento média) |
| `radiation` | MJ/m² (radiação solar global) |

### Exemplo com defasagem

```python
# Temperatura média do dia e dos 7 dias anteriores
rel = cs.sus_climate(
    rel,
    variables=["temp_mean", "precipitation"],
    years=[2023],
    lags=[1, 7],
)
# Colunas adicionadas: temp_mean, precipitation, temp_mean_lag1d,
# precipitation_lag1d, temp_mean_lag7d, precipitation_lag7d
```

!!! info "Cobertura atual de estações"
    O asset atual contém observações reais de 1 estação INMET (A701 —
    IAG/USP, São Paulo). Todos os municípios recebem os valores desta estação
    com peso 1.0. Para cobertura nacional, execute `scripts/build_climate.py`
    com os CSVs públicos do INMET.

---

## `sus_fill_gaps` — interpolação de lacunas

Preenche valores ausentes em séries temporais. Após o join climático, é
comum haver lacunas em municípios sem observação próxima.

```python
rel = cs.sus_fill_gaps(rel, method="linear")
rel = cs.sus_fill_gaps(rel, method="locf")
```

### Modos

| Método | Implementação | RAM | Custo |
|---|---|---|---|
| `"linear"` | Window function DuckDB | Zero | Baixo |
| `"locf"` | Window function DuckDB | Zero | Baixo |
| `"spline"` | `scipy` (opt-in) | Materializa | Médio |
| `"xgboost"` | `xgboost` (opt-in) | Materializa | Alto |

!!! warning "Modos opt-in materializam em RAM"
    `spline` e `xgboost` emitem `UserWarning` e coletam a relação em
    `pandas` internamente. Para bases grandes (> 500k linhas), prefira
    `"linear"` ou `"locf"`.

### Parâmetros adicionais

```python
rel = cs.sus_fill_gaps(
    rel,
    method="linear",
    group_col="municipality_code",   # agrupar por município (padrão)
    date_col="date",                  # coluna de data (padrão)
    columns=["temp_mean", "precipitation"],  # colunas a interpolar (padrão: todas numéricas)
    max_gap=7,                        # máximo de lacunas consecutivas a preencher
)
```

---

## Combinando enriquecimentos

Enriquecimentos podem ser encadeados — cada um retorna `DuckDBPyRelation`:

```python
rel = cs.sus_data_aggregate(rel, time="month", geo="municipality")
rel = cs.sus_census(rel, year=2022, variables=["population_2021"])
rel = cs.sus_climate(rel, variables=["temp_mean", "precipitation"], years=[2023])
rel = cs.sus_fill_gaps(rel, method="linear")
rel = cs.sus_spatial_join(rel)   # por último, para não carregar geometry em todos os joins

gdf = cs.materialize(rel, how="geopandas")
```
