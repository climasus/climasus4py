# climasus4py — Documentação em Português

`climasus4py` é um pacote Python para análise de microdados do Sistema Único de Saúde (SUS), com suporte a dados climáticos do INMET. Usa DuckDB como motor de consulta, mantendo o pipeline **lazy** até a materialização final — o que permite processar anos inteiros de dados nacionais com baixo uso de memória.

---

## Navegação

- :books: **[Referência da API](../reference/)** — Documentação gerada automaticamente de todas as funções públicas.
- :house: **[Página inicial](../index.md)**

## Exemplo: mortalidade respiratória em SP (2020–2023)

```python
import climasus as cs

# Pipeline completo — download automático + cache + filtragem + agregação
result = cs.sus_pipeline(
    system="SIM-DO",
    uf="SP",
    year=[2020, 2021, 2022, 2023],
    groups="respiratory",      # grupo CID-10: J00-J99
    age_min=18,                # adultos
    time="month",              # agregação mensal
    geo="state",               # nível estadual
    lang="pt",                 # nomes de colunas em português
)

# A relação é lazy — materializa apenas quando necessário
df = result.df()
print(df.head(10))
```

```
  time_group UF_residencia  contagem
0    2020-01            SP      1243
1    2020-02            SP      1189
...
```

---

## Grupos de doenças disponíveis

Os grupos são mantidos em `climasus-data` e resolvem automaticamente os CID-10 correspondentes:

| Grupo | Descrição |
|-------|-----------|
| `respiratory` | Doenças respiratórias (J00–J99) |
| `cardiovascular` | Doenças cardiovasculares |
| `dengue` | Dengue (A90, A91) |
| `covid19` | COVID-19 (U07.1, U07.2) |
| `diabetes` | Diabetes mellitus (E10–E14) |
| `neoplasms` | Neoplasias (C00–D48) |
| `external_causes` | Causas externas (V01–Y98) |
| `maternal_causes` | Causas maternas (O00–O99) |
| `malaria` | Malária (B50–B54) |
| `tuberculosis_respiratory` | Tuberculose pulmonar |
| `zika_chikungunya` | Zika e Chikungunya |
| `climate_sensitive` | Conjunto sensível ao clima |

---

## Filosofia do pacote

- **Lazy por padrão**: todas as transformações retornam `duckdb.DuckDBPyRelation` — os dados só são lidos do disco quando você chama `.df()`, `.fetchall()` ou exporta.
- **Cache automático**: os arquivos DBC do DATASUS são baixados uma vez e convertidos para Parquet. Execuções subsequentes reutilizam o cache.
- **Fast path SQL**: quando possível, todo o pipeline vira uma única query SQL (sem materialização intermediária).
- **Trilingue**: nomes de colunas disponíveis em `"pt"`, `"en"` e `"es"`.

---

## Sistemas DATASUS suportados

| Código | Sistema |
|--------|---------|
| `SIM-DO` | Declarações de Óbito |
| `SINASC` | Nascidos Vivos |
| `SIH` | Internações Hospitalares |
| `SIA` | Atendimentos Ambulatoriais |
