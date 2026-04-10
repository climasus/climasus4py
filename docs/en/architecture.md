# Architecture

## Overview

`climasus4py` uses DuckDB relations to keep transformations lazy until materialization.

Main flow:

1. Import and parquet cache
2. Cleaning and standardization
3. Epidemiological filtering
4. Derived variables
5. Time/geo aggregation
6. Export

## Modules

- `src/climasus/core`: pipeline and core transforms
- `src/climasus/io`: export and cache helpers
- `src/climasus/enrichment`: climate, census, spatial, gap fill
- `src/climasus/utils`: CID, metadata, quality, exploration

## Fast path in pipeline

`sus_pipeline` tries a single optimized SQL query when:

- `age_group is None`
- `epi_week is False`
- `time` is one of `year|quarter|month|week|day`
- `geo` is `state` or `municipality`

If constraints are not met, it falls back to staged execution.

## Local cache

- Default: `dados/cache/<SYSTEM>/<UF>_<YEAR>_<MONTH|all>.parquet`
- Reused automatically with `cache=True`

## Shared metadata

Loaded from external `climasus-data`:

- CID groups
- column dictionaries
- UF and region metadata

Without it, parts of the API are unavailable.
