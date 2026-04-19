# Primeros Pasos

Esta guía cubre instalación, configuración inicial y los flujos de uso más comunes.

---

## 1. Instalación

=== "Instalación básica"

    ```bash
    pip install climasus4py
    ```

=== "Con extras opcionales"

    ```bash
    pip install "climasus4py[spatial]"   # datos espaciales
    pip install "climasus4py[ml]"        # relleno de brechas con ML
    pip install "climasus4py[excel]"     # exportar a Excel
    pip install "climasus4py[spatial,ml,excel]"  # todo
    ```

!!! info "Requisito Python"
    Python 3.10 o superior.

---

## 2. Configurar `climasus-data`

```python
import climasus as cs

# Descarga/actualiza los metadatos al directorio por defecto (~/.climasus_data/)
cs.update_climasus_data()
```

O directorio personalizado:

```bash
git clone https://github.com/climasus/climasus-data.git /mi/ruta/local
```

```python
import os
os.environ["CLIMASUS_DATA_DIR"] = "/mi/ruta/local/climasus-data"
```

---

## 3. Pipeline completo

```python
import climasus as cs

result = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2021, 2022, 2023],
    groups="respiratory",
    age_min=18,
    age_max=80,
    time="month",
    geo="state",
    lang="es",
)

df = result.df()
print(df.head())
```

### Opciones de `uf`

```python
sus_pipeline("SIM-DO", uf="SP", year=2023)              # una UF
sus_pipeline("SIM-DO", uf=["SP", "RJ", "MG"], year=2023) # varias UFs
sus_pipeline("SIM-DO", uf="all", year=2023)              # todo el país
sus_pipeline("SIM-DO", uf="Sudeste", year=2023)          # por región
```

---

## 4. API por etapas

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", uf="RJ", year=[2022, 2023])
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, lang="es", system="SIM-DO")
rel = cs.sus_filter(rel, groups=["dengue", "zika_chikungunya"],
                    age_min=0, age_max=14, sex="F")
rel = cs.sus_variables(rel, age_group="who", epi_week=True, season=True)
rel = cs.sus_aggregate(rel, time="month", geo="municipality",
                       extra_groups=["sex", "age_group"])
df = rel.df()
cs.sus_export(rel, "output/resultado.parquet")
```

---

## 5. Filtros avanzados

```python
rel = cs.sus_filter(rel, codes=["A90", "A91", "B50-B54"])      # códigos CIE-10
rel = cs.sus_filter(rel, municipality=["3550308"])              # São Paulo capital
rel = cs.sus_filter(rel, date_start="2023-06-01",
                    date_end="2023-12-31")                       # rango de fechas
```

---

## Referencia rápida de parámetros

| Parámetro | Tipo | Valores aceptados |
|-----------|------|-------------------|
| `system` | `str` | `"SIM-DO"`, `"SINASC"`, `"SIH"`, `"SIA"` |
| `uf` | `str` o `list[str]` | `"SP"`, `["SP", "RJ"]`, `"all"`, `"Sudeste"` |
| `year` | `int` o `list[int]` | `2023`, `[2020, 2021, 2022]` |
| `time` | `str` | `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"` |
| `geo` | `str` | `"state"`, `"municipality"`, `"region"`, `"country"` |
| `lang` | `str` | `"pt"`, `"en"`, `"es"` |
| `groups` | `str` o `list[str]` | Ver [tabla de grupos](index.md#grupos-de-enfermedades-disponibles) |
| `age_group` | `str` o `list[int]` | `"who"`, `"decadal"`, `[0, 18, 65]` |
```

Tambien puedes definir `CLIMASUS_DATA_DIR`.

## 3. Pipeline completo

```python
import climasus as cs

rel = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2021, 2022, 2023],
    lang="en",
    groups=["dengue"],
    time="month",
    geo="state",
)

print(rel.columns)
pdf = rel.df()
```

## 4. Flujo por etapas

```python
import climasus as cs

rel = cs.sus_import("SIM-DO", "SP", [2022, 2023])
rel = cs.sus_clean(rel)
rel = cs.sus_standardize(rel, lang="en", system="SIM-DO")
rel = cs.sus_filter(rel, codes=["A90", "A91"], age_min=0, age_max=80)
rel = cs.sus_variables(rel, age_group="who", epi_week=True)
rel = cs.sus_aggregate(rel, time="month", geo="state")
cs.sus_export(rel, "output/dengue_sp.parquet")
```
