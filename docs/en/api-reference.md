# API Reference

## Pipeline

- `sus_pipeline(...)` -> runs import, clean, standardize, filter, variables, aggregate.
- Returns: `duckdb.DuckDBPyRelation` (lazy).

## Core

- `sus_import(system, uf, year, month=None, ..., backend="auto")`
- `sus_clean(rel, fix_enc=True, dedup=True, dedup_cols=None, age_range=(0, 120))`
- `sus_standardize(rel, lang="en", system=None)`
- `sus_filter(rel, groups=None, codes=None, age_min=None, age_max=None, sex=None, race=None, uf=None, municipality=None, date_start=None, date_end=None)`
- `sus_variables(rel, age_group=None, epi_week=False, season=False, quarter=False, month_name=False, day_of_week=False)`
- `sus_aggregate(rel, time="month", geo="state", extra_groups=None)`

## I/O and cache

- `sus_export(data, path, fmt=None, overwrite=True, compress="snappy")`
- `sus_cache_info(cache_dir="dados/cache")`
- `sus_cache_clear(cache_dir="dados/cache", system=None, uf=None, before=None)`
- `collect_arrow(rel)`

## Enrichment

- `sus_climate(data, climate, time_window=0, lags=None)`
- `sus_spatial(data, shapefile=None, geo_level="municipality", join_type="left")`
- `sus_census(data, census)`
- `sus_fill_gaps(data, method="linear", group_col="municipality_code", date_col="date", max_gap=None)`

## Utilities

- `sus_explore(what="systems")`
- `sus_quality(data)`
- `update_climasus_data(...)`

## Important notes

- `groups=` in `sus_filter` requires local `climasus-data`.
- `lang="en"` and `lang="es"` in `sus_standardize` also require local dictionaries.
- `sus_census(..., census=None)` is not auto-loaded yet in Python.
