# climasus4py (PT)

Pipeline rapido para dados SUS e clima no Brasil, com DuckDB lazy.

## O que voce encontra aqui

- Primeiros passos: [getting-started](getting-started.md)
- API publica: [api-reference](api-reference.md)
- Estrutura interna: [architecture](architecture.md)
- Fluxo de contribuicao: [development](development.md)

## Exemplo curto

```python
import climasus as cs

rel = cs.sus_pipeline(system="SIM-DO", uf="SP", year=2023, time="month", geo="state")
print(rel.df().head())
```
