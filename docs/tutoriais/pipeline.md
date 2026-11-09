# Trilha do Pipeline em 9 Etapas

[← Voltar para Tutoriais](index.md)

Esta trilha percorre o fluxo analítico do **climasus4py**, da preparação dos dados do DATASUS até
a integração ambiental e a modelagem epidemiológica. Cada etapa corresponde a uma decisão
metodológica auditável: origem dos dados, padronização, filtros, unidade espacial, exposição
climática, janela temporal e modelo estatístico.

| Etapa | Título | Funções principais |
|---:|---|---|
| 1 | Importação | `sus_data_import()` |
| 2 | Limpeza e Padronização | `sus_data_clean_encoding()`, `sus_data_standardize()` |
| 3 | Filtragem | `sus_filter()` |
| 4 | Variáveis e Agregação | `sus_data_create_variables()`, `sus_data_aggregate()` |
| 5 | Junção Espacial | `sus_spatial_join()` |
| 6 | Vínculo Socioeconômico | `sus_census()`, `sus_socio_compute_indicators()` |
| 7 | Clima por Estações (INMET) | `sus_climate_inmet()`, `sus_climate_fill_inmet()` |
| 8 | Clima em Grade e Ambiental | `sus_grid_era5()`, `sus_grid_chirps()`, `sus_grid_join()` |
| 9 | Modelagem Epidemiológica | `sus_mod_dlnm()`, `sus_mod_plot_*()` |

<div class="grid cards" markdown>

-   **Etapa 1 · Importação**

    Baixe e leia dados de saúde do DATASUS (SIM, SIH, SINAN, SINASC), preparando a base do
    pipeline.

    [Começar agora →](01-importacao.md)

-   **Etapa 2 · Limpeza e Padronização**

    Remova registros repetidos e idades impossíveis, e padronize nomes e valores das variáveis
    para um formato consistente.

    [Ver tutorial →](02-limpeza-padronizacao.md)

-   **Etapa 3 · Filtragem**

    Selecione casos por código CID-10 e por critérios demográficos (idade, sexo) para definir a
    população de estudo.

    [Ver tutorial →](03-filtragem.md)

-   **Etapa 4 · Variáveis e Agregação**

    Crie variáveis derivadas e agregue os registros no tempo e no espaço para formar séries
    analíticas.

    [Ver tutorial →](04-variaveis-agregacao.md)

-   **Etapa 5 · Junção Espacial**

    Vincule os dados a malhas geográficas (municípios, estados) para análises e mapeamentos
    espaciais.

    [Ver tutorial →](05-juncao-espacial.md)

-   **Etapa 6 · Vínculo Socioeconômico**

    Integre indicadores do Censo e dados socioeconômicos para contextualizar vulnerabilidades da
    população.

    [Ver tutorial →](06-socioeconomico.md)

-   **Etapa 7 · Clima por Estações (INMET)**

    Obtenha séries meteorológicas das estações do INMET, com controle de qualidade e
    preenchimento de falhas.

    [Ver tutorial →](07-clima-estacoes.md)

-   **Etapa 8 · Clima em Grade e Ambiental**

    Use dados climáticos em grade (ERA5-Land) e camadas ambientais como alternativa e complemento
    às estações.

    [Ver tutorial →](08-clima-grade.md)

-   **Etapa 9 · Modelagem Epidemiológica**

    Relacione exposição climática e desfechos de saúde com modelos epidemiológicos (ex.: DLNM) e
    estime efeitos.

    [Ver tutorial →](09-modelagem.md)

</div>

[← Voltar para Tutoriais](index.md)
