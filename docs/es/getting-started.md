# Primeros Pasos

## 1. Instalacion

```bash
pip install climasus4py
```

Extras opcionales:

```bash
pip install climasus4py[datasus]
pip install climasus4py[spatial]
pip install climasus4py[ml]
pip install climasus4py[excel]
```

## 2. Configurar `climasus-data`

Algunas partes de la API usan metadatos locales (grupos CID, diccionarios de columnas, UF/regiones).

```python
from climasus import update_climasus_data
update_climasus_data()
```

O manualmente:

```bash
git clone https://github.com/ClimaHealth/climasus-data.git
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
