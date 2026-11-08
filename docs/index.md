# climasus4py { .titulo-oculto }

<p align="center">
<img src="assets/climasus4py_logo.png" alt="climasus4py logo" width="190">
</p>

> **Análises Espaço-temporais Integradas de Saúde, Clima e Ambiente no Brasil**

---

O **climasus4py** é a versão em Python do [**climasus4r**](https://bymaxanjos.github.io/climasus4r/pt/index.html),
um conjunto integrado de ferramentas desenvolvido para otimizar a análise de dados de saúde,
clima e ambiente no Brasil. O pacote foi concebido no âmbito do projeto INCT Conexão – Amazônia
e tem como objetivo automatizar e padronizar etapas críticas do fluxo de trabalho em pesquisas
epidemiológicas e ambientais, promovendo reprodutibilidade, eficiência e escalabilidade.

Os microdados são baixados diretamente do servidor do DATASUS e processados com o
[DuckDB](https://duckdb.org/): os dados ficam em disco até o último passo, então anos inteiros
de dados nacionais cabem num computador comum. As etapas e os nomes das funções seguem os do
climasus4r — quem usa um pacote sabe usar o outro.

## 🌐 Outros Idiomas

- [English](en/index.md) | [Español](es/index.md)

---

## Instalação

O **climasus4py** encontra-se em desenvolvimento ativo. A versão mais recente é instalada
diretamente do GitHub, com um único comando, que instala também o **climasus-data** (os
dicionários e metadados usados pelo pacote).

Requer **Python 3.12 ou mais recente**.

```bash
pip install "climasus-data @ https://github.com/climasus/climasus-data/archive/refs/heads/main.zip" "climasus4py[all] @ https://github.com/climasus/climasus4py/archive/refs/heads/main.zip"
```

!!! tip "Qual é o meu Python?"
    No terminal, rode `python --version`. O Google Colab já usa o Python 3.12.

!!! tip "Colab e Jupyter"
    Depois de instalar, **reinicie o kernel** antes do primeiro `import climasus4py`
    (no Colab: *Ambiente de execução → Reiniciar sessão*).

!!! info "O que é baixado durante o uso, e onde fica"
    Os dados são baixados na primeira vez que uma função precisa deles e reaproveitados
    nas vezes seguintes:

    - **Dados do DATASUS:** na pasta `dados/cache`, dentro da pasta onde você está trabalhando.
    - **Clima (INMET), grades ambientais, mapas e censo:** na pasta `.climasus4py_cache`,
      dentro da sua pasta de usuário.

---

## Visão Geral das Funções

São **109 funções exportadas**, organizadas pelas etapas do pipeline.

| Etapa | Funções | Principais |
|---|---:|---|
| Importar | 3 | `sus_data_import`, `sus_data_read`, `sus_pipeline` |
| Limpar | 1 | `sus_data_clean_encoding` |
| Padronizar | 1 | `sus_data_standardize` |
| Filtrar | 4 | `sus_filter`, `sus_data_cid_select`, `list_disease_groups` |
| Criar variáveis | 1 | `sus_data_create_variables` |
| Agregar | 2 | `sus_data_aggregate`, `sus_fill_gaps` |
| Qualidade e visualização | 6 | `sus_data_quality_report`, `sus_data_plot_aggregate_ts`, `sus_explore` |
| Clima | 24 | `sus_climate_inmet`, `sus_climate_aggregate`, `sus_climate_compute_heatwaves` |
| Ambiente (grades) | 11 | `sus_grid_era5`, `sus_grid_chirps`, `sus_grid_fires` |
| Socioeconômico e espacial | 5 | `sus_census`, `sus_socio_compute_indicators`, `sus_spatial_join` |
| Modelar | 33 | `sus_mod_dlnm`, `sus_mod_casecrossover`, `sus_mod_spatial_bayes` |
| Exportar | 7 | `sus_data_export`, `materialize`, `sus_as_arrow` |
| Reprodutibilidade | 4 | `sus_rap_recipe`, `sus_rap_run` |
| Utilitários | 7 | `sus_meta`, `sus_cache_info`, `sus_sql` |

---

## Sistemas Suportados

O **climasus4py** permite o acesso simplificado e padronizado aos principais sistemas de
informação do DATASUS. Os arquivos são baixados diretamente do servidor do DATASUS e lidos pelo
[climasus_readdbc_py](https://github.com/climasus/climasus_readdbc_py), o leitor de arquivos
`.dbc` do próprio projeto, escrito em Python. A partir desses dados, o climasus4py organiza,
limpa e estrutura as informações, transformando bases complexas do DATASUS em conjuntos de dados
prontos para análise estatística e estudos espaço-temporais.

#### **1. SIM (Sistema de Informação sobre Mortalidade)**

- `"SIM-DO"`: Declarações de Óbito (Dataset completo)
- `"SIM-DOFET"`: Óbitos Fetais
- `"SIM-DOEXT"`: Óbitos por Causas Externas
- `"SIM-DOINF"`: Óbitos Infantis
- `"SIM-DOMAT"`: Óbitos Maternos

#### **2. SIH (Sistema de Informação Hospitalar)**

- `"SIH-RD"`: AIH (Autorizações de Internação Hospitalar) - Geral
- `"SIH-RJ"`: AIH - Específico para o Rio de Janeiro
- `"SIH-SP"`: AIH - Específico para São Paulo
- `"SIH-ER"`: Prontuários de Emergência

#### **3. SINAN (Sistema de Informação de Agravos de Notificação)**

- `"SINAN-DENGUE"`: Casos de Dengue
- `"SINAN-CHIKUNGUNYA"`: Casos de Chikungunya
- `"SINAN-ZIKA"`: Casos de Zika vírus
- `"SINAN-MALARIA"`: Casos de Malária
- `"SINAN-CHAGAS"`: Casos de Doença de Chagas
- `"SINAN-LEISHMANIOSE-VISCERAL"`: Leishmaniose Visceral
- `"SINAN-LEISHMANIOSE-TEGUMENTAR"`: Leishmaniose Tegumentar
- `"SINAN-LEPTOSPIROSE"`: Casos de Leptospirose

#### **4. SIA (Sistema de Informação Ambulatorial)**

- `"SIA-AB"`: Atenção Básica
- `"SIA-ABO"`: Procedimentos Odontológicos
- `"SIA-ACF"`: Assistência Farmacêutica
- `"SIA-AD"`: Alta Complexidade/Diferenciada
- `"SIA-AN"`: Atenção Domiciliar
- `"SIA-AM"`: Ambulatório de Especialidades
- `"SIA-AQ"`: Ações Estratégicas
- `"SIA-AR"`: Regulação
- `"SIA-ATD"`: Urgência/Emergência
- `"SIA-PA"`: Procedimentos Ambulatoriais em Hospital
- `"SIA-PS"`: Atenção Psicossocial
- `"SIA-SAD"`: Atenção Especializada

#### **5. CNES (Cadastro Nacional de Estabelecimentos de Saúde)**

- `"CNES-LT"`: Leitos
- `"CNES-ST"`: Profissionais de Saúde
- `"CNES-DC"`: Equipamentos (Detalhado)
- `"CNES-EQ"`: Equipamentos (Resumo)
- `"CNES-SR"`: Serviços Especializados
- `"CNES-HB"`: Leitos Hospitalares
- `"CNES-PF"`: Pessoal Físico (Profissionais)
- `"CNES-EP"`: Participantes do Ensino
- `"CNES-RC"`: Classificação Hospitalar
- `"CNES-IN"`: Indicadores Hospitalares
- `"CNES-EE"`: Entidades de Ensino
- `"CNES-EF"`: Instalações de Ensino
- `"CNES-GM"`: Gestão e Apoio

#### **6. SINASC (Sistema de Informação sobre Nascidos Vivos)**

- `"SINASC"`: Declarações de Nascidos Vivos

---

## Início Rápido

Óbitos por doenças respiratórias em São Paulo, em 2023:

```python
import climasus4py as cs

# Pipeline completo: dados prontos para análise
df_analise = cs.sus_data_import(system="SIM-DO", uf="SP", year=2023)
df_analise = cs.sus_data_clean_encoding(df_analise)
df_analise = cs.sus_data_standardize(df_analise, lang="pt")
df_analise = cs.sus_filter(df_analise, groups="respiratory")
df_analise = cs.sus_data_create_variables(df_analise, create_age_groups=True, lang="pt")

df_analise.df()   # 42.275 óbitos por doenças respiratórias
```

Até o `.df()`, nada é carregado na memória: cada etapa só acrescenta um passo à consulta, e o
DuckDB executa tudo de uma vez no final.

---

## Infraestrutura de Dados

A fase de infraestrutura do **climasus4py** fornece um pipeline completo, de ponta a ponta, para
a preparação de dados de saúde, da aquisição bruta até dados prontos para análise. Com 8 funções
principais, você transforma dados do DATASUS em séries temporais agregadas, padronizadas e
prontas para modelagem.

```text
DADOS BRUTOS (DATASUS)
    ↓
[1] sus_data_import()            → Aquisição direta do DATASUS, com cache
    ↓
[2] sus_data_clean_encoding()    → Correção de encoding
    ↓
[3] sus_data_standardize()       → Padronização multilíngue
    ↓
[4] sus_filter()                 → Filtragem por doença e perfil demográfico
    ↓
[5] sus_data_create_variables()  → Criação de variáveis
    ↓
[6] sus_data_quality_report()    → Verificação de qualidade
    ↓
[7] sus_data_aggregate()         → Agregação temporal
    ↓
[8] sus_data_export()            → Exportação com metadados
    ↓
DADOS PRONTOS PARA ANÁLISE
```

---

## Tem sugestões ou achou um erro?

Abra uma [issue no GitHub](https://github.com/climasus/climasus4py/issues). Um exemplo pequeno
que reproduza o problema ajuda muito.

---

## Sobre

- **Autores:** Marlon Resende Faria, Thauã Menezes, Max Anjos e Andrey Araújo
- **Mantenedor:** Max Anjos
- **Licença:** [MIT](https://github.com/climasus/climasus4py/blob/main/LICENSE)
- **Links:**
[Código no GitHub](https://github.com/climasus/climasus4py) ·
[climasus4r (versão R)](https://bymaxanjos.github.io/climasus4r/pt/index.html) ·
[Novidades (CHANGELOG)](https://github.com/climasus/climasus4py/blob/main/CHANGELOG.md)
