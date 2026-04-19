# climasus4py

**Workflows rápidos com dados SUS e clima para o Brasil — em Python.**

`climasus4py` processa microdados DATASUS com DuckDB: downloads automáticos do FTP, cache em Parquet, filtragem por CID-10, agregação temporal/geográfica e enriquecimento com dados climáticos e censitários — tudo em uma API lazy de alto desempenho.

---

## Instalação rápida

```bash
pip install climasus4py
```

## Pipeline em 3 linhas

```python
import climasus as cs

result = cs.sus_pipeline("SIM-DO", uf="SP", year=[2021, 2022, 2023],
                          groups="respiratory", time="month", geo="state")
result.df().head()
```

---

## Documentação por idioma

- :flag_br: **[Português](pt/index.md)**
- :flag_us: **[English](en/index.md)**
- :flag_es: **[Español](es/index.md)**

---

## Links

- [Referência da API](reference/)
- [GitHub](https://github.com/climasus/climasus4py)
- [PyPI](https://pypi.org/project/climasus4py/)
- [climasus4r (versão R)](https://climasus.github.io/climasus4r/)
