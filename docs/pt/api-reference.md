# Referência da API

Documentação completa gerada automaticamente a partir das docstrings do código.

---

## Pipeline

::: climasus.core.pipeline
    options:
      members: [sus_pipeline]
      show_source: true

---

## Core

::: climasus.core.importer
    options:
      members: [sus_import]

::: climasus.core.clean
    options:
      members: [sus_clean]

::: climasus.core.standardize
    options:
      members: [sus_standardize]

::: climasus.core.filter
    options:
      members: [sus_filter]

::: climasus.core.variables
    options:
      members: [sus_variables]

::: climasus.core.aggregate
    options:
      members: [sus_aggregate]

::: climasus.core.engine
    options:
      members: [collect_arrow]

---

## I/O e Cache

::: climasus.io.export
    options:
      members: [sus_export]

::: climasus.io.cache
    options:
      members: [sus_cache_info, sus_cache_clear]

---

## Enriquecimento

::: climasus.enrichment.climate
    options:
      members: [sus_climate]

::: climasus.enrichment.spatial
    options:
      members: [sus_spatial]

::: climasus.enrichment.census
    options:
      members: [sus_census]

::: climasus.enrichment.fill_gaps
    options:
      members: [sus_fill_gaps]

---

## Utilitários

::: climasus.utils.explore
    options:
      members: [sus_explore]

::: climasus.utils.quality
    options:
      members: [sus_quality]
- Retorno: `duckdb.DuckDBPyRelation` (lazy).

## Core

- `sus_import(system, uf, year, month=None, ..., backend="auto")`
- `sus_clean(rel, fix_enc=True, dedup=True, dedup_cols=None, age_range=(0, 120))`
- `sus_standardize(rel, lang="en", system=None)`
- `sus_filter(rel, groups=None, codes=None, age_min=None, age_max=None, sex=None, race=None, uf=None, municipality=None, date_start=None, date_end=None)`
- `sus_variables(rel, age_group=None, epi_week=False, season=False, quarter=False, month_name=False, day_of_week=False)`
- `sus_aggregate(rel, time="month", geo="state", extra_groups=None)`

## I/O e cache

- `sus_export(data, path, fmt=None, overwrite=True, compress="snappy")`
- `sus_cache_info(cache_dir="dados/cache")`
- `sus_cache_clear(cache_dir="dados/cache", system=None, uf=None, before=None)`
- `collect_arrow(rel)`

## Enriquecimento

- `sus_climate(data, climate, time_window=0, lags=None)`
- `sus_spatial(data, shapefile=None, geo_level="municipality", join_type="left")`
- `sus_census(data, census)`
- `sus_fill_gaps(data, method="linear", group_col="municipality_code", date_col="date", max_gap=None)`

## Utilitarios

- `sus_explore(what="systems")`
- `sus_quality(data)`
- `update_climasus_data(...)`

## Observacoes importantes

- `groups=` em `sus_filter` depende de `climasus-data` local.
- `lang="en"` ou `lang="es"` em `sus_standardize` tambem depende de dicionarios locais.
- `sus_census(..., census=None)` ainda nao faz autoload no Python.
