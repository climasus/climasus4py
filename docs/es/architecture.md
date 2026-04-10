# Arquitectura

## Resumen

`climasus4py` usa relaciones DuckDB para mantener transformaciones lazy hasta materializar.

Flujo principal:

1. Importacion y cache parquet
2. Limpieza y estandarizacion
3. Filtros epidemiologicos
4. Variables derivadas
5. Agregacion temporal/geografica
6. Exportacion

## Modulos

- `src/climasus/core`: pipeline y transformaciones centrales
- `src/climasus/io`: exportacion y cache
- `src/climasus/enrichment`: clima, censo, espacial, gap fill
- `src/climasus/utils`: CID, metadatos, calidad y exploracion

## Fast path del pipeline

`sus_pipeline` intenta una consulta SQL optimizada cuando:

- `age_group is None`
- `epi_week is False`
- `time` en `year|quarter|month|week|day`
- `geo` en `state|municipality`

Si no cumple, usa el flujo por etapas.

## Cache local

- Ruta por defecto: `dados/cache/<SYSTEM>/<UF>_<YEAR>_<MONTH|all>.parquet`
- Reutilizacion automatica con `cache=True`

## Metadatos compartidos

Se cargan desde `climasus-data` externo:

- grupos CID
- diccionarios de columnas
- UF y regiones

Sin ese repositorio local, partes de la API no funcionan.
