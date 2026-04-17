# climasus4py

[![PyPI](https://img.shields.io/pypi/v/climasus4py.svg)](https://pypi.org/project/climasus4py/) [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Fast SUS and climate data workflows for Brazil — Python edition.**

<<<<<<< HEAD
Port Python do [climasus4r](https://github.com/ClimaHealth/climasus4r), usando DuckDB para processamento lazy e PySUS para downloads do DATASUS.
=======
__DOCUMENTATION AGE__ : [https://climasus.github.io/climasus4py_documentation/](https://climasus.github.io/climasus4py_documentation/)

## Features
>>>>>>> 6e991b71a17e4383fb30a13222c5d76bf68dcf42

---

<<<<<<< HEAD
## Instalação

```bash
pip install git+https://github.com/climasus/climasus4py.git

```

---

## Exemplo rápido

```python
import climasus as cs

# Pipeline completo: importar → limpar → padronizar → filtrar → agregar
df = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2021, 2022, 2023],
    lang="en",
    groups=["dengue"],
    time="month",
    geo="state",
).df()
print(df)
```

---

## Uso passo a passo

```python
import climasus as cs

# 1. Importa e faz cache (parquet)
data = cs.sus_import("SIM-DO", "SP", [2021, 2022])
# 2. Limpa (dedup, encoding, idade)
data = cs.sus_clean(data)
# 3. Padroniza (colunas PT→EN, tipos)
data = cs.sus_standardize(data, lang="en")
# 4. Filtra por grupo de doença
data = cs.sus_filter(data, groups=["dengue"], age_min=0, age_max=80)
# 5. Cria variáveis derivadas
data = cs.sus_variables(data, age_group="who", epi_week=True)
# 6. Agrega
data = cs.sus_aggregate(data, time="month", geo="state")
# 7. Exporta
cs.sus_export(data, "output/dengue_sp.parquet")
```

---

## Arquitetura

```
climasus/
├── core/           # Engine (DuckDB, pipeline)
├── io/             # Entrada/Saída (parquet, cache)
├── enrichment/     # Enriquecimento (clima, censo, espacial)
└── utils/          # Utilitários (CID-10, encoding, etc)
```

- **Processamento:** DuckDB (SQL lazy, alta performance)
- **Cache:** Parquet local, reutilizável
- **Enriquecimento:** Clima, censo, espacial, interpolação

---

## Dados compartilhados

Usa [climasus-data](https://github.com/climasus/climasus-data) para metadados (grupos de doenças, dicionários, UFs, regiões). 
A instalação desses arquivos é automática
### Atualização dos metadados

Para baixar ou atualizar o catálogo localmente:

```python
from climasus import update_climasus_data
update_climasus_data()  # Clona ou faz pull do repositório climasus-data
```

Se preferir, faça manualmente:

```bash
git clone https://github.com/climasus/climasus-data.git
# ou, para atualizar:
cd climasus-data && git pull
```


## Contribuindo

- Pull Requests e Issues são bem-vindos!
- Siga o padrão PEP8 e mantenha testes automatizados.
- Para bugs, inclua exemplo mínimo reproduzível.

---

## Licença

MIT
=======
