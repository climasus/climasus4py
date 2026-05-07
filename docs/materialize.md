# Materialize — trazendo dados para a memória

`cs.materialize(rel)` é o comando explícito de fim de pipeline quando o destino
é a RAM. Ele aceita qualquer `DuckDBPyRelation` — independente de quantas
etapas lazy foram encadeadas — e devolve o formato solicitado.

## Sintaxe

```python
result = cs.materialize(rel, how="auto", quiet=False)
```

| Parâmetro | Tipo | Padrão | Descrição |
|---|---|---|---|
| `rel` | `DuckDBPyRelation` | — | Relação lazy a materializar |
| `how` | `str` | `"auto"` | Formato de saída (ver tabela abaixo) |
| `quiet` | `bool` | `False` | Suprime alertas de tamanho |

## Formatos de saída

| `how` | Tipo retornado | Dependência extra | Notas |
|---|---|---|---|
| `"auto"` | `GeoDataFrame` ou `DataFrame` | — | Auto-detecta `geometry_wkt` |
| `"pandas"` | `pandas.DataFrame` | — | Sempre disponível |
| `"geopandas"` | `geopandas.GeoDataFrame` | `pip install climasus4py[spatial]` | Exige coluna `geometry_wkt` |
| `"polars"` | `polars.DataFrame` | `pip install climasus4py[polars]` | Via Arrow |
| `"pyarrow"` | `pyarrow.Table` | — | Sempre disponível |

## Exemplos

=== "pandas (padrão)"

    ```python
    import climasus4py as cs

    rel = cs.sus_aggregate(rel, time="month", geo="municipality")
    df = cs.materialize(rel)          # how="auto" → pandas quando não há geometry
    df = cs.materialize(rel, how="pandas")

    print(df.head())
    ```

=== "geopandas"

    ```python
    rel = cs.sus_spatial(rel)         # adiciona geometry_wkt
    gdf = cs.materialize(rel, how="geopandas")

    gdf.plot(column="count", legend=True)
    ```

    Requer `pip install climasus4py[spatial]`.

=== "polars"

    ```python
    import polars as pl

    df = cs.materialize(rel, how="polars")
    df.filter(pl.col("count") > 10)
    ```

    Requer `pip install climasus4py[polars]`.

=== "pyarrow"

    ```python
    import pyarrow as pa

    table = cs.materialize(rel, how="pyarrow")
    pa.parquet.write_table(table, "resultado.parquet")
    ```

## Alertas de tamanho

Por padrão, `materialize` emite um `UserWarning` quando a relação tem muitas
linhas, orientando o usuário a usar `sus_export` para evitar esgotamento de
memória:

| Linhas | Nível de alerta |
|---|---|
| < 100.000 | Silencioso |
| 100.000 – 999.999 | Nota informativa |
| 1.000.000 – 9.999.999 | Warning com sugestão de `sus_export` |
| ≥ 10.000.000 | Warning forte de risco de OOM |

Para suprimir os alertas quando a materialização é intencional:

```python
df = cs.materialize(rel, quiet=True)
```

## Quando usar `sus_export` em vez de `materialize`

Use `sus_export` quando:

- O dataset tem mais de 1 milhão de linhas.
- O destino é um arquivo (ETL, pipeline de dados).
- Você não precisa manipular os dados em memória.

```python
cs.sus_export(rel, "saida/sim_sp_2023.parquet")   # Parquet
cs.sus_export(rel, "saida/sim_sp_2023.csv")       # CSV
```

`sus_export` nunca coleta dados em `pandas` — usa streaming DuckDB direto
para disco.

## `auto` — detecção de formato

`how="auto"` inspeciona as colunas da relação:

- Se houver `geometry_wkt` → retorna `geopandas.GeoDataFrame` (CRS: EPSG:4674)
- Caso contrário → retorna `pandas.DataFrame`

```python
rel_sem_geo = cs.sus_aggregate(rel, time="month", geo="municipality")
df = cs.materialize(rel_sem_geo)    # → pandas.DataFrame

rel_com_geo = cs.sus_spatial(rel_sem_geo)
gdf = cs.materialize(rel_com_geo)   # → geopandas.GeoDataFrame
```
