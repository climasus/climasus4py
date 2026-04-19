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

## Escolha o idioma da documentação

<div class="grid cards" markdown>

- :flag_br: **[Português](pt/index.md)**
  
    Documentação completa em português — primeiros passos, referência de API e arquitetura.

- :flag_us: **[English](en/index.md)**
  
    Full documentation in English — getting started, API reference and architecture.

- :flag_es: **[Español](es/index.md)**
  
    Documentación completa en español — primeros pasos, referencia de API y arquitectura.

</div>

---

## Sistemas DATASUS suportados

| Sistema | Descrição |
|---------|-----------|
| `SIM-DO` | Declarações de Óbito (mortalidade) |
| `SINASC` | Sistema de Informação sobre Nascidos Vivos |
| `SIH` | Sistema de Informações Hospitalares |
| `SIA` | Sistema de Informações Ambulatoriais |

---

## Links

- [GitHub](https://github.com/climasus/climasus4py)
- [PyPI](https://pypi.org/project/climasus4py/)
- [climasus4r (versão R)](https://climasus.github.io/climasus4r/)
