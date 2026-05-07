# Como consultar dados com o climasus4py

O contrato central do `climasus4py` é `DuckDBPyRelation`. Todas as funções do
pipeline constroem uma consulta lazy — nenhuma linha de dado chega à RAM até
que você chame [`materialize()`](materialize.md) ou [`sus_export()`](#saidas).

## Três portas de entrada

=== "API canônica (DATASUS)"

    Baixa, cacheia e abre dados do DATASUS como DuckDB lazy:

    ```python
    import climasus4py as cs

    rel = cs.sus_import("SIM-DO", "SP", 2023)
    ```

    Também aceita múltiplos anos:

    ```python
    rel = cs.sus_import("SIM-DO", "SP", [2020, 2021, 2022, 2023])
    ```

=== "Parquet local (`sus_read`)"

    Abre qualquer Parquet ou GeoParquet local como relação lazy:

    ```python
    rel = cs.sus_read("dados/cache/SIM-DO/SP_2023_all.parquet")
    ```

    Aceita lista ou glob:

    ```python
    rel = cs.sus_read(["dados/SIM_SP_2022.parquet", "dados/SIM_SP_2023.parquet"])
    rel = cs.sus_read("dados/SIM_SP_*.parquet")
    ```

=== "SQL bruto (`sus_sql`)"

    Executa SQL DuckDB arbitrário como ponto de entrada:

    ```python
    rel = cs.sus_sql("""
        SELECT * FROM read_parquet('dados/externo.parquet')
        WHERE UF = 'SP'
    """)
    ```

## Encadeamento de etapas

Após a entrada, o pipeline segue a [ordem canônica](pipeline-order.md):

```python
rel = cs.sus_import("SIM-DO", "SP", 2023)
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, system="SIM-DO")
rel = cs.sus_filter(rel, codes=["J00-J99"], age_min=15, age_max=64, uf="SP")
rel = cs.sus_variables(rel, age_group="epidemiological_default", epi_week=True)
rel = cs.sus_aggregate(rel, time="month", geo="municipality")
```

Todo o bloco acima é **zero RAM** — apenas SQL sendo construído.

## Transformações SQL no meio do pipeline

Use `.pipe(cs.sus_sql, ...)` para injetar SQL arbitrário em qualquer ponto.
O marcador `{data}` é substituído pelo nome da relação atual:

```python
rel = cs.sus_aggregate(rel, time="month", geo="municipality")
rel = rel.pipe(
    cs.sus_sql,
    "SELECT *, count / population AS rate FROM {data}",
)
```

Para transformações DuckDB nativas (sem precisar de `sus_sql`):

```python
rel = rel.filter("count > 5")
rel = rel.select("municipality_code, month, count")
```

## Enriquecimentos

Depois do `aggregate`, aplique enriquecimentos opcionais. Todos retornam
`DuckDBPyRelation` — sem RAM até o fim:

```python
rel = cs.sus_spatial(rel)                                   # adiciona geometry_wkt
rel = cs.sus_census(rel, year=2022, variables=["population_2021"])  # indicadores IBGE
rel = cs.sus_climate(rel, variables=["temp_mean"], years=[2023])    # INMET
rel = cs.sus_fill_gaps(rel, method="linear")                # interpolação lazy
```

Veja o [guia completo de enriquecimentos](enrichments.md).

## Saídas

### Para disco (`sus_export`)

Escreve sem coletar em memória. Preferido para bases grandes:

```python
cs.sus_export(rel, "resultado/sim_sp_2023.parquet")   # Parquet (padrão)
cs.sus_export(rel, "resultado/sim_sp_2023.csv")       # CSV
```

### Para RAM (`materialize`)

Carrega o resultado em um formato in-memory. Use quando precisar de análise
interativa ou integração com outras bibliotecas:

```python
df     = cs.materialize(rel)                    # auto: pandas ou GeoDataFrame
df     = cs.materialize(rel, how="pandas")
gdf    = cs.materialize(rel, how="geopandas")   # requer geometry_wkt
table  = cs.materialize(rel, how="pyarrow")
polars = cs.materialize(rel, how="polars")
```

Veja a [referência completa do materialize](materialize.md).

## Exemplo completo

```python
import climasus4py as cs

# 1. Entrada
rel = cs.sus_import("SIM-DO", "SP", 2023)

# 2. Pipeline core (lazy)
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, system="SIM-DO")
rel = cs.sus_filter(rel, groups="respiratory", age_min=0, age_max=14)
rel = cs.sus_variables(rel, age_group="epidemiological_default")
rel = cs.sus_aggregate(rel, time="month", geo="municipality")

# 3. Enriquecimento (lazy)
rel = cs.sus_climate(rel, variables=["temp_mean", "precipitation"], years=[2023])

# 4. Saída — escolha uma:
cs.sus_export(rel, "sim_resp_criancas_sp_2023.parquet")  # disco
df = cs.materialize(rel)                                  # RAM
```
