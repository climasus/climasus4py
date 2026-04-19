# Arquitectura

Esta página describe la estructura interna de `climasus4py`: cómo se usa DuckDB, el sistema de caché, el fast path SQL y la organización de módulos.

---

## Flujo del pipeline

```
FTP DATASUS
    │
    ▼
sus_import()  ──── Descarga .dbc ──── Convierte a .parquet ──── Caché local
    │                                                                    │
    │◄─────────────────────── cache hit ────────────────────────────────────────────────┘
    │
    ▼ DuckDBPyRelation (lazy)
sus_clean()       — corrige encoding, tipos, fechas DATASUS
    │
    ▼ DuckDBPyRelation (lazy)
sus_standardize() — renombra columnas (DATASUS → legible, en pt/en/es)
    │
    ▼ DuckDBPyRelation (lazy)
sus_filter()      — filtros por CIE-10, edad, sexo, municipio, fecha
    │
    ▼ DuckDBPyRelation (lazy)
sus_variables()   — grupos de edad, semana epi, estación, trimestre
    │
    ▼ DuckDBPyRelation (lazy)
sus_aggregate()   — GROUP BY tiempo + geografía + extras
    │
    ▼ materialización (sólo aquí)
sus_export()  /  .df()  /  .fetchall()
```

---

## Fast path SQL

Cuando se cumplen todas las condiciones siguientes, `sus_pipeline` construye **una única consulta SQL con CTE** — sin materialización intermedia:

| Condición | Valor aceptado |
|-----------|---------------|
| `age_group` | `None` |
| `epi_week` | `False` |
| `time` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `"state"` o `"municipality"` |

En los demás casos, el pipeline por etapas se activa automáticamente.

---

## Sistema de caché

```
dados/cache/
└── SIM-DO/
    ├── SP_2022_all.parquet
    ├── SP_2023_all.parquet
    └── ...
```

Convención: `{UF}_{AÑO}_{MES_o_all}.parquet`

```python
cs.sus_cache_info()   # listar archivos en caché
cs.sus_cache_clear()  # borrar caché
```

---

## Estructura de módulos

```
src/climasus/
├── __init__.py          # API pública
├── core/
│   ├── pipeline.py      # sus_pipeline — orquestación + fast path
│   ├── importer.py      # sus_import — descarga FTP + caché parquet
│   ├── clean.py         # sus_clean — encoding, tipos, fechas
│   ├── standardize.py   # sus_standardize — renombrar columnas
│   ├── filter.py        # sus_filter — CIE-10, edad, sexo, geo, fecha
│   ├── variables.py     # sus_variables — age_group, epi_week, season
│   ├── aggregate.py     # sus_aggregate — GROUP BY tiempo + geo
│   └── engine.py        # conexión DuckDB + helpers
├── enrichment/
│   ├── climate.py       # sus_climate — unión salud + clima (INMET)
│   ├── spatial.py       # sus_spatial — unión geoespacial
│   ├── census.py        # sus_census — variables del censo
│   └── fill_gaps.py     # sus_fill_gaps — imputación ML (xgboost)
├── io/
│   ├── export.py        # sus_export — parquet / csv / xlsx
│   └── cache.py         # sus_cache_info, sus_cache_clear
└── utils/
    ├── cid.py           # expand_cid_ranges, codes_for_groups
    ├── data.py          # detect_*, resolve_uf, decode_age_sql
    ├── encoding.py      # corrección encoding DATASUS
    ├── explore.py       # sus_explore
    └── quality.py       # sus_quality
```

Se cargan desde `climasus-data` externo:

- grupos CID
- diccionarios de columnas
- UF y regiones

Sin ese repositorio local, partes de la API no funcionan.
