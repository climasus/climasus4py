# Migração do `climasus4r` legacy para `climasus4py` v0.3.0

> **Português** | [English](#migration-from-climasus4r-legacy-to-climasus4py-v030) | [Español](#migración-del-climasus4r-legacy-a-climasus4py-v030)

---

## PT — Migração do `climasus4r` legacy para `climasus4py` v0.3.0

### Por que migrar?

`climasus4py` v0.3.0 atinge paridade funcional completa com o `climasus4r` legacy. Todos os nomes de função foram sincronizados — código R e Python agora compartilham o mesmo vocabulário.

### Tabela de renames — v0.2.x → v0.3.0

| `climasus4r` (R) | `climasus4py` v0.2.x (antigo) | `climasus4py` v0.3.0 (novo) |
|---|---|---|
| `sus_data_import()` | `sus_import()` | **`sus_data_import()`** |
| `sus_data_clean_encoding()` | `sus_clean()` | **`sus_data_clean_encoding()`** |
| `sus_data_standardize()` | `sus_standardize()` | **`sus_data_standardize()`** |
| `sus_data_create_variables()` | `sus_variables()` | **`sus_data_create_variables()`** |
| `sus_data_aggregate()` | `sus_aggregate()` | **`sus_data_aggregate()`** |
| `sus_data_read()` | `sus_read()` | **`sus_data_read()`** |
| `sus_data_quality_report()` | `sus_quality()` | **`sus_data_quality_report()`** |
| `sus_spatial_join()` | `sus_spatial()` | **`sus_spatial_join()`** |
| `sus_chat()` | `sus_chat_ai()` | **`sus_chat()`** |

> **Atenção:** não há aliases de compatibilidade retroativa. Código que usa os nomes antigos levanta `AttributeError` imediatamente.

### Migração automática

```bash
# Atualiza todos os arquivos .py no diretório atual
python tools/migrate-from-v0.2.py --path ./meus_scripts/

# Apenas verifica, sem alterar arquivos
python tools/migrate-from-v0.2.py --path ./meus_scripts/ --dry-run
```

### Funções novas em v0.3.0 (sem equivalente em v0.2.x)

#### `sus_meta` — metadados de pipeline

```python
# R legacy
meta <- sus_meta(rel)
sus_meta(rel, field = "stage")
sus_meta(rel, add_history = "Filtrado por CID J")

# Python v0.3.0
import climasus4py as cs

meta = cs.sus_meta(rel)
stage = cs.sus_meta(rel, field="stage")
rel2 = cs.sus_meta(rel, add_history="Filtrado por CID J")
```

#### `list_disease_groups` — catálogo de grupos de doenças

```python
# R legacy
grupos <- sus_list_disease_groups(climate_sensitive_only = TRUE, lang = "pt")

# Python v0.3.0
grupos = cs.list_disease_groups(climate_sensitive_only=True, lang="pt")
# Retorna DataFrame com colunas: group_name, label, climate_sensitive, n_codes
```

#### `get_disease_group_details` — detalhes de um grupo CID-10

```python
# R legacy
detalhes <- sus_disease_group_details("respiratory", lang = "pt")

# Python v0.3.0
detalhes = cs.get_disease_group_details("respiratory", lang="pt")
# Retorna dict com: label, description, codes, climate_sensitive, climate_factors
```

#### `sus_filter` — parâmetros novos em v0.3.0

```python
# R legacy
sus_data_filter_demographics(rel, education = "complete_high_school",
                              drop_ignored = TRUE)
sus_data_filter_cid(rel, groups = "J", match_type = "exact")

# Python v0.3.0 (tudo em sus_filter unificado)
cs.sus_filter(rel,
    education="complete_high_school",
    city="São Paulo",
    drop_ignored=True,
    match_type="exact")   # "starts_with" (padrão) ou "exact"
```

### Exemplo completo: pipeline SIM-DO

```python
# R legacy
library(climasus4r)
rel <- sus_data_import("SIM-DO", uf = "SP", year = 2023)
rel <- sus_data_clean_encoding(rel)
rel <- sus_data_standardize(rel)
rel <- sus_data_filter_cid(rel, groups = "respiratory")
rel <- sus_data_create_variables(rel, epi_week = TRUE)
resultado <- sus_data_aggregate(rel, time = "month", geo = "state")
df <- collect(resultado)

# Python v0.3.0 — mesma sequência
import climasus4py as cs

rel = cs.sus_data_import("SIM-DO", uf="SP", year=2023)
rel = cs.sus_data_clean_encoding(rel)
rel = cs.sus_data_standardize(rel)
rel = cs.sus_filter(rel, groups="respiratory")
rel = cs.sus_data_create_variables(rel, epi_week=True)
rel = cs.sus_data_aggregate(rel, time="month", geo="state")
df = rel.df()
```

---

## EN — Migration from `climasus4r` legacy to `climasus4py` v0.3.0

### Why migrate?

`climasus4py` v0.3.0 achieves full functional parity with the `climasus4r` legacy package. All function names have been synchronized — R and Python code now share the same vocabulary.

### Rename table — v0.2.x → v0.3.0

| `climasus4r` (R) | `climasus4py` v0.2.x (old) | `climasus4py` v0.3.0 (new) |
|---|---|---|
| `sus_data_import()` | `sus_import()` | **`sus_data_import()`** |
| `sus_data_clean_encoding()` | `sus_clean()` | **`sus_data_clean_encoding()`** |
| `sus_data_standardize()` | `sus_standardize()` | **`sus_data_standardize()`** |
| `sus_data_create_variables()` | `sus_variables()` | **`sus_data_create_variables()`** |
| `sus_data_aggregate()` | `sus_aggregate()` | **`sus_data_aggregate()`** |
| `sus_data_read()` | `sus_read()` | **`sus_data_read()`** |
| `sus_data_quality_report()` | `sus_quality()` | **`sus_data_quality_report()`** |
| `sus_spatial_join()` | `sus_spatial()` | **`sus_spatial_join()`** |
| `sus_chat()` | `sus_chat_ai()` | **`sus_chat()`** |

> **Warning:** no backward-compatibility aliases exist. Code using old names raises `AttributeError` immediately.

### Automatic migration

```bash
# Updates all .py files in the given directory
python tools/migrate-from-v0.2.py --path ./my_scripts/

# Dry run — check only, no file changes
python tools/migrate-from-v0.2.py --path ./my_scripts/ --dry-run
```

### New functions in v0.3.0 (no equivalent in v0.2.x)

#### `sus_meta` — pipeline metadata

```python
# R legacy
meta <- sus_meta(rel)
sus_meta(rel, field = "stage")
sus_meta(rel, add_history = "Filtered by CID J")

# Python v0.3.0
import climasus4py as cs

meta = cs.sus_meta(rel)
stage = cs.sus_meta(rel, field="stage")
rel2 = cs.sus_meta(rel, add_history="Filtered by CID J")
```

#### `list_disease_groups` — disease group catalog

```python
# R legacy
groups <- sus_list_disease_groups(climate_sensitive_only = TRUE, lang = "en")

# Python v0.3.0
groups = cs.list_disease_groups(climate_sensitive_only=True, lang="en")
# Returns DataFrame with columns: group_name, label, climate_sensitive, n_codes
```

#### `get_disease_group_details` — ICD-10 group details

```python
# R legacy
details <- sus_disease_group_details("respiratory", lang = "en")

# Python v0.3.0
details = cs.get_disease_group_details("respiratory", lang="en")
# Returns dict with: label, description, codes, climate_sensitive, climate_factors
```

#### `sus_filter` — new parameters in v0.3.0

```python
# R legacy (two separate functions)
sus_data_filter_demographics(rel, education = "complete_high_school",
                              drop_ignored = TRUE)
sus_data_filter_cid(rel, groups = "J", match_type = "exact")

# Python v0.3.0 (unified sus_filter)
cs.sus_filter(rel,
    education="complete_high_school",
    city="São Paulo",
    drop_ignored=True,
    match_type="exact")   # "starts_with" (default) or "exact"
```

### Full example: SIM-DO pipeline

```python
# R legacy
library(climasus4r)
rel <- sus_data_import("SIM-DO", uf = "SP", year = 2023)
rel <- sus_data_clean_encoding(rel)
rel <- sus_data_standardize(rel)
rel <- sus_data_filter_cid(rel, groups = "respiratory")
rel <- sus_data_create_variables(rel, epi_week = TRUE)
resultado <- sus_data_aggregate(rel, time = "month", geo = "state")
df <- collect(resultado)

# Python v0.3.0 — same sequence
import climasus4py as cs

rel = cs.sus_data_import("SIM-DO", uf="SP", year=2023)
rel = cs.sus_data_clean_encoding(rel)
rel = cs.sus_data_standardize(rel)
rel = cs.sus_filter(rel, groups="respiratory")
rel = cs.sus_data_create_variables(rel, epi_week=True)
rel = cs.sus_data_aggregate(rel, time="month", geo="state")
df = rel.df()
```

---

## ES — Migración del `climasus4r` legacy a `climasus4py` v0.3.0

### ¿Por qué migrar?

`climasus4py` v0.3.0 alcanza paridad funcional completa con el paquete legacy `climasus4r`. Todos los nombres de función fueron sincronizados — el código R y Python ahora comparten el mismo vocabulario.

### Tabla de renombrado — v0.2.x → v0.3.0

| `climasus4r` (R) | `climasus4py` v0.2.x (antiguo) | `climasus4py` v0.3.0 (nuevo) |
|---|---|---|
| `sus_data_import()` | `sus_import()` | **`sus_data_import()`** |
| `sus_data_clean_encoding()` | `sus_clean()` | **`sus_data_clean_encoding()`** |
| `sus_data_standardize()` | `sus_standardize()` | **`sus_data_standardize()`** |
| `sus_data_create_variables()` | `sus_variables()` | **`sus_data_create_variables()`** |
| `sus_data_aggregate()` | `sus_aggregate()` | **`sus_data_aggregate()`** |
| `sus_data_read()` | `sus_read()` | **`sus_data_read()`** |
| `sus_data_quality_report()` | `sus_quality()` | **`sus_data_quality_report()`** |
| `sus_spatial_join()` | `sus_spatial()` | **`sus_spatial_join()`** |
| `sus_chat()` | `sus_chat_ai()` | **`sus_chat()`** |

> **Atención:** no existen aliases de compatibilidad retroactiva. El código que usa nombres antiguos lanza `AttributeError` inmediatamente.

### Migración automática

```bash
# Actualiza todos los archivos .py en el directorio indicado
python tools/migrate-from-v0.2.py --path ./mis_scripts/

# Solo verificación, sin modificar archivos
python tools/migrate-from-v0.2.py --path ./mis_scripts/ --dry-run
```

### Nuevas funciones en v0.3.0 (sin equivalente en v0.2.x)

#### `sus_meta` — metadatos del pipeline

```python
# R legacy
meta <- sus_meta(rel)
sus_meta(rel, field = "stage")
sus_meta(rel, add_history = "Filtrado por CID J")

# Python v0.3.0
import climasus4py as cs

meta = cs.sus_meta(rel)
stage = cs.sus_meta(rel, field="stage")
rel2 = cs.sus_meta(rel, add_history="Filtrado por CID J")
```

#### `list_disease_groups` — catálogo de grupos de enfermedades

```python
# R legacy
grupos <- sus_list_disease_groups(climate_sensitive_only = TRUE, lang = "es")

# Python v0.3.0
grupos = cs.list_disease_groups(climate_sensitive_only=True, lang="es")
# Retorna DataFrame con columnas: group_name, label, climate_sensitive, n_codes
```

#### `get_disease_group_details` — detalles de un grupo CID-10

```python
# R legacy
detalles <- sus_disease_group_details("respiratory", lang = "es")

# Python v0.3.0
detalles = cs.get_disease_group_details("respiratory", lang="es")
# Retorna dict con: label, description, codes, climate_sensitive, climate_factors
```

#### `sus_filter` — nuevos parámetros en v0.3.0

```python
# R legacy (dos funciones separadas)
sus_data_filter_demographics(rel, education = "complete_high_school",
                              drop_ignored = TRUE)
sus_data_filter_cid(rel, groups = "J", match_type = "exact")

# Python v0.3.0 (sus_filter unificado)
cs.sus_filter(rel,
    education="complete_high_school",
    city="São Paulo",
    drop_ignored=True,
    match_type="exact")   # "starts_with" (predeterminado) o "exact"
```

### Ejemplo completo: pipeline SIM-DO

```python
# R legacy
library(climasus4r)
rel <- sus_data_import("SIM-DO", uf = "SP", year = 2023)
rel <- sus_data_clean_encoding(rel)
rel <- sus_data_standardize(rel)
rel <- sus_data_filter_cid(rel, groups = "respiratory")
rel <- sus_data_create_variables(rel, epi_week = TRUE)
resultado <- sus_data_aggregate(rel, time = "month", geo = "state")
df <- collect(resultado)

# Python v0.3.0 — misma secuencia
import climasus4py as cs

rel = cs.sus_data_import("SIM-DO", uf="SP", year=2023)
rel = cs.sus_data_clean_encoding(rel)
rel = cs.sus_data_standardize(rel)
rel = cs.sus_filter(rel, groups="respiratory")
rel = cs.sus_data_create_variables(rel, epi_week=True)
rel = cs.sus_data_aggregate(rel, time="month", geo="state")
df = rel.df()
```
