# climasus4py (EN)

Fast SUS + climate workflows for Brazil with a lazy DuckDB pipeline.

## What is included

- Getting started: [getting-started](getting-started.md)
- Public API: [api-reference](api-reference.md)
- Internals: [architecture](architecture.md)
- Contributing flow: [development](development.md)

## Short example

```python
import climasus as cs

rel = cs.sus_pipeline(system="SIM-DO", uf="SP", year=2023, time="month", geo="state")
print(rel.df().head())
```
