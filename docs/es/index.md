# climasus4py (ES)

Flujo rapido de datos SUS y clima para Brasil con pipeline lazy en DuckDB.

## Contenido

- Primeros pasos: [getting-started](getting-started.md)
- API publica: [api-reference](api-reference.md)
- Arquitectura: [architecture](architecture.md)
- Desarrollo: [development](development.md)

## Ejemplo corto

```python
import climasus as cs

rel = cs.sus_pipeline(system="SIM-DO", uf="SP", year=2023, time="month", geo="state")
print(rel.df().head())
```
