# climasus4py — Documentación en Español

`climasus4py` es un paquete Python para analizar microdatos del Sistema Único de Salud (SUS) de Brasil, con soporte integrado para enriquecimiento con datos climáticos del INMET. Usa DuckDB como motor de consultas, manteniendo todo el pipeline **lazy** hasta la materialización final — lo que permite procesar datos nacionales completos con bajo uso de memoria.

---

## Contenido

<div class="grid cards" markdown>

- :rocket: **[Primeros Pasos](getting-started.md)**

    Instalación, configuración de `climasus-data` y tu primer pipeline completo paso a paso.

- :books: **[Referencia de API](../reference/)**

    Documentación autogenerada de todas las funciones públicas, con parámetros, tipos y ejemplos.

- :building_construction: **[Arquitectura](architecture.md)**

    Cómo funciona el DuckDB lazy evaluation, el fast path, el sistema de caché y la estructura de módulos.

- :wrench: **[Desarrollo](development.md)**

    Configuración local, cómo ejecutar pruebas, benchmarks y cómo contribuir.

</div>

---

## Ejemplo rápido: mortalidad respiratoria en SP (2020–2023)

```python
import climasus as cs

# Pipeline completo — descarga automática + caché + filtrado + agregación
result = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2020, 2021, 2022, 2023],
    groups="respiratory",      # grupo ICD-10: J00-J99
    age_min=18,
    time="month",
    geo="state",
    lang="es",                 # nombres de columnas en español
)

df = result.df()
print(df.head(10))
```

---

## Grupos de enfermedades disponibles

| Grupo | Descripción |
|-------|-------------|
| `respiratory` | Enfermedades respiratorias (J00–J99) |
| `cardiovascular` | Enfermedades cardiovasculares |
| `dengue` | Dengue (A90, A91) |
| `covid19` | COVID-19 (U07.1, U07.2) |
| `diabetes` | Diabetes mellitus (E10–E14) |
| `neoplasms` | Neoplasias (C00–D48) |
| `external_causes` | Causas externas (V01–Y98) |
| `maternal_causes` | Causas maternas (O00–O99) |
| `malaria` | Malaria (B50–B54) |
| `tuberculosis_respiratory` | Tuberculosis pulmonar |
| `zika_chikungunya` | Zika y Chikungunya |
| `climate_sensitive` | Conjunto de enfermedades sensibles al clima |
