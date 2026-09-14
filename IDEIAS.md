# IDEIAS.md — backlog para a v2.0

Registro de observações, débitos técnicos e melhorias identificadas durante o trabalho — **anotadas, não executadas**. A diretriz vigente é replicar o comportamento do `climasus4r` legacy; o que merece mudar entra aqui e é revisado periodicamente com o coordenador.

Formato de cada entrada:

```
## AAAA-MM-DD — título curto
- **Onde:** função/arquivo afetado
- **O quê:** descrição da observação (bug herdado do R, nome confuso, lentidão, etc.)
- **Por que não agora:** paridade / fora de escopo / precisa de decisão do coordenador
```

---

## 2026-09-03 — `assert_after()` existe mas nunca é chamado

- **Onde:** `climasus4py/core/_stage.py` (definição) — nenhum chamador no pacote
- **O quê:** o módulo define `assert_after(rel, stage)` para levantar `ValueError` quando uma etapa é executada fora da ordem de `CANONICAL_STAGES`. Uma varredura no pacote encontrou **0 ocorrências fora do próprio `_stage.py`** — o guard nunca entra em ação. Na prática, chamar `sus_data_aggregate()` sobre uma relação que não passou por `sus_data_standardize()` não levanta erro: apenas produz resultado errado, silenciosamente. É exatamente a classe de falha que o guard foi escrito para pegar.
- **Por que não agora:** ativar o guard muda o comportamento em runtime de funções da API pública — código de usuário que hoje roda (mesmo que produzindo resultado errado) passaria a levantar exceção. Precisa de decisão do coordenador e de verificação de paridade: confirmar se o `climasus4r` aplica a mesma checagem, para não divergir.

## 2026-09-03 — estágios `climate` e `enrichment` fora de `CANONICAL_STAGES`

- **Onde:** `climasus4py/core/_stage.py` (lista `CANONICAL_STAGES`) vs. chamadas `set_stage()` no pacote
- **O quê:** `CANONICAL_STAGES` lista apenas os seis estágios do track de saúde (`import → clean → standardize → filter → variables → aggregate`), mas o código grava também `"climate"` (2 ocorrências) e `"enrichment"` (3 ocorrências) via `set_stage()`. Como `assert_after()` retorna sem checar quando o estágio não está na lista (`except ValueError: return`), nenhum erro aparece hoje. Consequência: se o guard da entrada anterior for ativado, **todo o track de clima/enriquecimento passaria batido** — o guard daria uma falsa sensação de cobertura.
- **Por que não agora:** acoplado à decisão anterior — só faz sentido resolver junto. Exige definir se clima/enriquecimento formam uma cadeia ordenada própria (com sua própria lista de estágios) ou se são ramos paralelos ao track de saúde, e conferir como o `climasus4r` modela isso.

## 2026-09-09 — o R devolve NA de idade quando `DTNASC` é nulo, mesmo com `IDADE` válida  ·  **M6**

- **Onde:** `climasus4r::sus_data_create_variables()` (coluna de idade) — lado R
- **O quê:** medido no SIM-DO SP 2023 durante a correção do M6. As divergências de `age_years` entre os dois lados eram **471**, e a conta fecha exatamente em duas direções opostas: **335** registros onde o Python errava (o sentinela `IDADE='999'`, "idade desconhecida", virava 999 anos — corrigido em 09/09) e **136** registros onde o **R** erra: `DTNASC` nulo e `IDADE` trazendo valor válido codificado (ex. `IDADE='454'` = 54 anos). O Python decodifica e acerta; o R devolve `NA`, descartando idade que está no dado. Ou seja, depois da nossa correção a divergência residual esperada contra o R é de 136 linhas, **todas a favor do Python**.
- **Por que não agora:** é bug do R, e pelo princípio de paridade do `CLAUDE.md` bugs do R são apontados e não replicados nem corrigidos daqui. Vale levar ao coordenador porque afeta o `climasus4r` publicado: 136 em 334.303 é 0,04%, mas são idades que existem no dado e estão sendo perdidas. Registrado também no M6.

## 2026-09-09 — `dlnm::crosspred(at = x)` ordena `x`, e o `.saf_component` do R pareia posicionalmente  ·  **M24**

- **Onde:** `climasus4r::sus_mod_af()` → `.saf_component()` — lado R. Propaga para `sus_mod_excess()` e `sus_mod_swot()`.
- **O quê:** o `sus_mod_af` faz `pred_obs <- dlnm::crosspred(cb, model, at = x, cen = cen)` e usa `rr_obs <- as.numeric(pred_obs$allRRfit)`. Mas o `crosspred` devolve `allRRfit` na ordem **crescente** de `x`, não na ordem em que `x` foi passado — verificado: `identical(predvar, sort(predvar))` é `TRUE` enquanto `identical(x, sort(x))` é `FALSE`. O `.saf_component` então pareia esse vetor **posicionalmente** com `cases` e com `in_range`, que estão em ordem de **data**. Como os valores de `x` são contínuos e todos distintos, os comprimentos coincidem (1812 = 1812) e **o R não emite aviso nenhum**: cada dia recebe silenciosamente o RR de outro dia.
- **Prova:** com a mesma série sintética de 5 anos nos dois lados, o Python (pareamento correto) dá af total 0,056669 / heat −0,007568 / cold 0,064237 e o R dá 0,054892 / 0,032898 / 0,021994. Embaralhando o RR do Python do mesmo jeito — `rr[argsort(x)]` — reproduzem-se os números do R **até a sexta casa decimal**. Isso identifica a causa sem margem: é o desalinhamento, não diferença de método, de base spline (as curvas de RR são idênticas até a 7ª casa em toda a faixa), de centragem (`cen` idêntico) nem de entrada (`nrow`, `n_cases`, `x` e a contagem de dias de cada lado do `cen` todos idênticos).
- **Por que importa mais que o normal:** a fração atribuível é o número publicável desta biblioteca. O total sai próximo do correto (~3%) porque os óbitos diários são homogêneos, o que faz a soma embaralhada ficar perto da certa — mas a **decomposição calor/frio fica arbitrária**, e foi por isso que o achado original (M24) via a ordem heat/cold invertida e desvios de até +306% por faixa de percentil. Um relatório de carga de calor construído sobre isso atribui ao frio o que é do calor.
- **Correção sugerida ao R:** `crosspred` aceita `at` e devolve `predvar`; basta reordenar por `match(x, pred_obs$predvar)` antes de usar, ou computar o RR por dia sem passar pelo grid ordenado. Vale também um `stopifnot(identical(as.numeric(pred_obs$predvar), as.numeric(x)))` como guarda.
- **Por que não agora:** é bug do R, e pelo princípio de paridade do `CLAUDE.md` bugs do R são apontados e não replicados nem corrigidos daqui. O lado Python está correto e tem `tests/test_af_pairing.py` impedindo que alguém o alinhe ao comportamento defeituoso numa comparação futura. **Levar ao coordenador com prioridade**: afeta o `climasus4r` publicado, no número que vai para publicação.

## 2026-09-14 — `sus_mod_metaregression()` fatia os coeficientes do `mvmeta` na ordem errada  ·  **M67**

- **Onde:** `climasus4r::sus_mod_metaregression()` linhas 134 e 136, e o helper `.mr_wald_tests()` — lado R.
- **O quê:** o `mvmeta` devolve o vetor de coeficientes em ordem **outcome-major intercalada**, e o R fatia como se cada moderador ocupasse um bloco contíguo. Verificado com `p=5` resultados e uma covariável: `names(coef(fit))` sai `y1.(Intercept)`, `y1.x`, `y2.(Intercept)`, `y2.x`, … O R faz `as.numeric(coef(active_fit))[seq_len(n_coef)]` e chama isso de bloco do intercepto.
- **Prova numérica:** com interceptos verdadeiros `0,1 0,2 0,3 0,4 0,5` e inclinações `0,9 0,8 0,7 0,6 0,5`, a fatia do R devolve **`0,08 0,9226 0,2217 0,7913 0,3404`** — alternando intercepto e inclinação, cobrindo só os três primeiros resultados. A extração correta (passo `q`, começando no moderador desejado) devolve `0,08 0,2217 0,3404 0,4176 0,5426`, que são os interceptos.
- **O mesmo erro no Wald:** `.mr_wald_tests()` usa `idx <- (j * n_coef + 1L):((j + 1L) * n_coef)`. Com `p=5`, `q=2`, a covariável 1 pega as posições 6..10 = `y3.x, y4.(Intercept), y4.x, y5.(Intercept), y5.x` — nada a ver com o bloco dela. Então os testes de significância das covariáveis também saem de um subvetor arbitrário.
- **Alcance:** atinge **toda** saída da função sempre que houver ao menos uma covariável — ou seja, sempre, já que é meta-regressão. `pooled_pred`, `pooled_curve`, `exposure_response` e os BLUPs vêm todos de `coef_intercept`/`vcov_intercept`. O `vcov` é fatiado com o mesmo `seq_len(n_coef)`, então o bloco de covariância também está errado. O `.mr_heterogeneity()` está correto — usa `qtest()` direto.
- **Por que o `sus_mod_pool` escapa:** lá `q = 1`, e com um único moderador as duas ordenações coincidem. O defeito aparece só quando entram covariáveis.
- **No Python:** implementado `MvmetaFit.block(moderador)`, que acessa por passo `n_moderators`. Há dois testes em `tests/test_mod_metaregression.py` fixando o comportamento, sendo um deles o contraexemplo explícito de que a fatia ingênua **não** é o bloco do intercepto.
- **Por que não agora (no R):** é bug do R e, por decisão do Andrey em 14/09/2026, bugs do R são anotados aqui e feitos corretamente no Python. **Levar ao coordenador com prioridade** — junto com o bug de pareamento da AF, é o segundo defeito que corrompe número publicável no `climasus4r`.

## 2026-09-14 — `wct_c` do R converte o vento com o fator errado, e o próprio R prova  ·  **M69**

- **Onde:** `climasus4r:::.compute_wct()` — lado R.
- **O quê:** `ws_mph <- pmax(ws * 0.621371, 0.01)`. O `0.621371` converte **km/h → mph**; a coluna de entrada é `ws_2_m_s`, em **m/s**. O fator correto é `2.2369362920544`. Subestima o vento por 3,6×.
- **A prova não precisa de referência externa.** O R implementa a mesma grandeza física duas vezes: `wcet_c` (Environment Canada, vento em km/h, conversão `ws*3.6` **correta**) e `wct_c` (NWS, em mph). As duas regressões publicadas concordam entre si a menos de ~0,03 °C — então a inconsistência aparece na saída do próprio pacote.
- **Medido:** em 4.000 linhas no domínio válido (1.838 com as duas colunas preenchidas), `wct_c − wcet_c` dá média **+4,602 °C**, mediana +4,560, faixa +1,83 a +7,67. Em 200 mil pontos: `|wct_R − wcet|` média 4,503 (máx 7,49) contra `|wct_corrigido − wcet|` média **0,024** (máx 0,04).
- **A direção importa:** subestimar o vento subestima a sensação de frio, então `wct_c` sai **sempre** mais quente que a verdade (verificado em 100% das linhas). Para onda de frio é o lado perigoso — reporta menos risco do que existe.
- **No Python:** `wct_c` replica o R por paridade (decisão do Andrey, para a apresentação ao coordenador ser defensável), com a constante nomeada `_MPH_PER_KMH` para não parecer erro de digitação. A fórmula correta fica **executável e testada** em `_wct_correct_units()` — função, não comentário, justamente para não se perder. Quatro testes fixam o achado.
- **Por que não agora:** bug do R; anotado e não replicado como correção. **Levar ao coordenador** — é o achado mais fácil de verificar dos três, e o único que não depende de julgamento.

## 2026-09-14 — `koppen_humidity` do R classifica umidade **ausente** como `"Perhumid"`  ·  **M70**

- **Onde:** `climasus4r:::.compute_koppen_humidity()` — lado R.
- **O quê:** é um `case_when` de quatro faixas terminando em `TRUE ~ "Perhumid"`. Esse ramo captura também o `NA`: com `rh_mean_porc` ausente, as três condições anteriores avaliam `NA`, nenhuma casa, e a linha vira **"Perhumid"** — a faixa *mais úmida* das quatro.
- **Medido:** nas 4.000 linhas da fixture, 40 têm umidade nula e as 40 saem `"Perhumid"`. Onde a umidade existe, R e Python concordam em **3.960 de 3.960**. As 40 são as únicas divergências.
- **Por que é diferente do `wct`:** lá é um número errado numa coluna numérica; aqui o R **inventa uma categoria** para dado que não existe. Uma contagem por classe de umidade soma as ausências na faixa mais úmida, e nada sinaliza isso.
- **Estado:** o Python devolve `NULL`. A diretriz de replicar-e-anotar foi dada no contexto de *escolha de fórmula* (o WBGT), não de fabricação de valor ausente — por isso o comportamento conservador ficou e a decisão foi levada ao Andrey. Um teste nomeado fixa a divergência como **escolha**, não descuido.
- **Por que não agora:** aguarda decisão.

## 2026-09-14 — o WBGT do R não implementa as referências que a própria documentação cita  ·  **M71**

- **Onde:** `climasus4r:::.compute_wbgt()` e a página de ajuda de `sus_climate_compute_indicators` — lado R.
- **O que a doc declara:** *"WBGT uses a dual wet-bulb estimate (**Liljegren + Bernard/Pourmoghani**) averaged for numerical robustness"*, com Liljegren et al. (2008) *JOEH* 5(10):645-655 e Bernard & Pourmoghani (1999) *AIHAJ* 60(1):32-37 nas referências. **A média de dois termos é deliberada** — corrijo aqui uma leitura minha anterior que a tratava como suspeita.
- **O que o código faz:** `tnw1 <- airT * atan(0.16 * sqrt(pmax(e_a,0.01) + 0.1)) + 3` tem a forma `T·atan(c·√x)+k`, que é a de **Stull (2011)** — cujo termo principal é `T·atan(0.151977·√(RH + 8.313659))` e espera **RH em porcentagem**. O R alimenta com `e_a`, pressão de vapor em **kPa**: com T=30/RH=60 o `atan` recebe 0,26 onde a forma pede 1,26. E `tnw2 <- airT + 0.33*(rh/100)*exp(0.0514*airT) - 4` tem a forma da **temperatura aparente australiana** (Steadman/BOM) sem o termo de vento — não um bulbo úmido. Nem Stull nem Steadman estão nas referências declaradas, e Liljegren (2008) é modelo iterativo de balanço de energia, sem forma fechada.
- **Medido** contra tabela psicrométrica (T=30 °C, RH=60%, referência ≈ 23,9 °C): Stull, que o Python usa, dá **24,00 °C** (erro 0,10); o `tnw` do R dá **18,78 °C** (erro 5,12).
- **Segundo sintoma, independente:** o WBGT existe para capturar carga solar, mas a temperatura de globo do R vai de **28,00 a 28,84 °C** com a radiação indo de 0 a 1000 W/m² — amplitude de 0,84 °C, praticamente inerte. Em sol pleno um globo passa de 45 °C na literatura.
- **Consequência medida:** as duas colunas `wbgt_c` diferem em média **3,35 °C** e discordam no limiar de 31 °C (calor extremo, ISO 7243) em **15,5%** dos casos — e os limiares que o R aplica (31/28/25) são os do WBGT *externo*.
- **Ressalva honesta:** o desvio de 5 °C contra a tabela e a inércia solar são medições sólidas; a atribuição a uma troca de unidade é **inferência** minha, forte mas inferência. O coordenador conhece a origem do código e resolve isso em minutos.
- **Plano acordado:** `wbgt_c` replica o R (paridade) e `wbgt_stull_c` entra como indicador **separado** com a fórmula validada, para as duas saírem lado a lado e a diferença virar dado em vez de afirmação. Ainda não implementado.
- **Por que não agora:** precisa de decisão do coordenador sobre o lado R.

---

### Nota de 2026-09-14 — decisões sobre as três entradas acima

Por decisão do Andrey, o Python passou a **replicar o R** nos três casos, para a apresentação ao coordenador ser defensável. O mérito dos defeitos segue **aberto** — o que foi decidido é o comportamento do Python, não que o R esteja certo.

- **`wct_c`** replica o R, e a fórmula correta ficou em `_wct_correct_units()` — função executável e testada, não comentário.
- **`koppen_humidity`** passou a classificar umidade ausente como `"Perhumid"`, igual ao R.
- **`wbgt_c`** replica o R (diferença 0,000e+00 em 3.920 valores), e `wbgt_stull_c` entrou como **coluna separada** com a fórmula validada, para a divergência sair como dado na mesma tabela.

**Correção a um número que eu reportei.** Eu havia dito que as duas versões do WBGT diferiam em média 3,35 °C com 15,5% de discordância no limiar. Aquela medição amostrava vento de 0,2 a 4 m/s; na fixture o vento vai a 18 e o termo de globo do R é dividido por `ws^0,2`. Medido na fixture de 4.000 linhas: média **−0,363 °C**, mediana −0,625, **faixa de −7,98 a +8,07**. Não é viés constante — as duas divergem em até 8 °C nos **dois** sentidos, e uma média engana. Eu não devia ter apresentado o 3,35 como geral.

O que sobrevive a qualquer distribuição são dois fatos: o **ponto fixo psicrométrico** (a 30 °C/60%, Stull erra 0,10 °C e o `tnw` do R erra 5,12) e a **consequência no limiar** (acima de 31 °C o `wbgt_c` marca 207 linhas e o `wbgt_stull_c` marca 449, mais que o dobro).

**Efeito colateral da paridade:** `wbgt_c` agora exige os quatro insumos (T, RH, SR, WS) onde antes bastavam T e RH. Em série sem radiação solar ele deixa de sair, e o `wbgt_stull_c` passa a ser o único disponível — o que na prática o torna o mais útil nas séries curtas do INMET.

## 2026-09-14 — das 30 colunas de flag do R, 16 nunca disparam e 2 estão invertidas  ·  **M72**

- **Onde:** `climasus4r:::.add_confidence_flags()` e o registry de limiares — lado R.
- **Como funciona:** três booleanos por indicador que declare limiar (`_flag_extreme`, `_flag_high`, `_flag_low`), e uma **cadeia de prioridade** escolhe qual limiar declarado alimenta cada um. Quando nenhum nome casa, a coluna sai cheia de `FALSE` em vez de ser omitida.

**Problema 1 — 16 das 30 colunas nunca disparam.** Não por falta de limiar declarado, mas porque o **nome** declarado não está na cadeia. O `diurnal_range` declara `high=15`, `moderate=10`, `low=5` e a cadeia procura `high_stress`/`warning_low`: as três flags dele são mortas. O `vapor_pressure` é igual. O `pet` declara **seis** limiares e só o `extreme_heat` é lido, porque ele escreve `slight_cold` onde a cadeia quer `slight_cold_stress` — quase acerto que custa duas colunas. Por indicador: `wbgt` 0, `hi` 1, `thi` 2, `wcet` 2, `wct` 2, `et` 1, `utci` 0, `pet` 2, `diurnal_range` 3, `vapor_pressure` 3.

**Problema 2 — as flags de frio estão invertidas.** `wcet` e `wct` declaram `high_risk = -35`, que cai na cadeia do **extreme**, e essa flag dispara em `valor > limiar`. Para sensação térmica de frio, **mais frio é pior**. Medido numa grade de −40 a 60: a flag é TRUE em **190 de 201** pontos — TRUE a −30 °C e a +20 °C, FALSE a −40 °C. Na fixture de 4.000 linhas é TRUE em 1.657 de 1.838 linhas com valor, e no valor **mais frio** da amostra (−43,61 °C, risco de vida) está **FALSE**.

- **Conta final:** das 30 colunas emitidas, **16 constantes FALSE, 2 invertidas, 12 com informação correta**.
- **No Python:** replicado integralmente, com `confidence_flags` (default `True`, igual ao R). A conta não fica em comentário: `flag_threshold(indicador, flag)` devolve qual limiar alimenta cada flag ou `None` quando é constante FALSE, e há teste parametrizado nomeando as 16. Verificado contra a saída real do `.add_confidence_flags` numa grade de −40 a 60: **todas** batem, incluindo as mortas e a inversão.
- **Ressalva sobre o ganho:** as flags fecham 30 das 46 colunas que faltavam, resolvendo a paridade de *forma* da tabela. Mas 18 delas não carregam informação — o ganho de contagem é maior que o de utilidade, e isso deve ir ao coordenador junto.
- **Por que não agora:** bug do R. **A inversão é o pedaço mais sério** — uma flag de frio extremo que não dispara no frio extremo é pior que nenhuma flag.

## 2026-09-14 — `diurnal_range` do R agrupa por dia e não por estação  ·  **M76**

- **Onde:** `climasus4r:::.compute_diurnal_range()` — lado R.
- **O quê:** monta `day_key <- as.character(as.Date(df[["date"]]))` e agrega com `tapply(t, day_key, max)` — **por dia apenas**, sem agrupar por estação. Numa entrada multi-estação, a "amplitude diurna" de cada linha passa a ser o máximo menos o mínimo de **todas** as estações naquele dia, ou seja incorpora a diferença *entre* estações, que não é amplitude diurna. O Python faz `MAX(T) OVER (PARTITION BY estação, data) − MIN(T) OVER (...)`, que é a definição.
- **Medido** com as 2 estações da fixture: **3.936 de 4.000** linhas diferem. A magnitude que medi (média 3,9 °C, máx 34) está **inflada** porque a fixture usa temperatura aleatória e não ciclo diário real — o que vale é a estrutura, não esse número.
- **Segunda divergência no mesmo helper, ainda não tratada:** o R tem um ramo **primário** que usa `tair_max_c`/`tair_min_c` quando existem *e* o desvio-padrão entre dias de (max − min) passa de 0,5; só cai para `max(T) − min(T)` como **fallback**. O Python implementa sempre o fallback. Em entrada que traga max/min, os dois calculam coisas diferentes.
- **Não alinhado ao R por ora:** replicar significaria misturar estações de propósito, e a decisão de replicar-e-anotar foi dada para *escolha de fórmula*, não para agrupamento que mistura unidades de observação.
- **Por que não agora:** aguarda decisão, junto com o M70.

---

### Nota de 2026-09-14 — a auditoria que gerou M74, M75 e M76

Ao conferir se cada indicador dado como portado estava **de fato** em paridade, descobri que `thi`, `diurnal_range` e `vapor_pressure` existiam no Python desde antes deste trabalho e **nunca tinham sido comparados contra o R**. Eu vinha tratando os três como portados sem evidência. **Três dos quatro divergiam.**

O pior era meu: o `hi_c` chegava a **151,6 °C** por falta do teto de 60 °C do Rothfusz (**M75**) — corrigido, e agora em paridade exata. Registro também que meu primeiro número para esse achado ("média 9,875 °C de diferença") estava dominado por extrapolação fora do domínio do ajuste; os dois polinômios são a mesma regressão, com mediana 0,000 °C.

**Lição de método que vale além deste módulo:** adotei `ROUND_EVEN` em todos os indicadores depois de descobrir que a paridade exata dos anteriores foi **sorte da fixture** não ter caído em nenhum caso de meio exato — não garantia estrutural. O `round()` do R 4.x desempata conforme a representação binária, e nenhum modo do DuckDB reproduz isso: `ROUND_EVEN` acerta 99,3% e é o melhor disponível.


---

### Nota de 2026-09-14 — as variantes corretas, e uma armadilha de método  ·  **M77, M76**

A diretriz passou a ser: **seguir o R sempre**, inclusive quando ele contraria a própria documentação ou os artigos que cita; anotar para conversar com o criador; e **guardar o código correto** quando o Python já o tinha, para não ter que reescrever.

Auditei os cinco casos em que seguimos o R e só **dois** tinham o correto preservado — uma função auxiliar para o `wct`, uma coluna para o `wbgt`. Nos outros três o correto foi sobrescrito: o `NULL` do koppen, a forma clássica de Thom, a partição por estação do `diurnal_range`.

Agora existe mecanismo nomeado: `CORRECTED_INDICATORS`. Cada divergência em que este pacote acredita que o R erra ganha uma **variante correta** — indicador de verdade, coluna própria, limiares herdados da base, teste. Ficam **fora** de `indicators="all"`, para o padrão entregar exatamente o conjunto de colunas do R, e são pedidas por nome. São cinco: `thi_classic`, `koppen_humidity_strict`, `diurnal_range_station`, `wct_ms` e `wbgt_stull`.

**A armadilha de método, que vale além deste módulo.** Eu havia medido que as duas formas do `diurnal_range` divergem em "3.936 de 4.000 linhas". Errado: eu comparava arrays **por posição**, e a função de **janela** do DuckDB **reordena** as linhas de saída — ao contrário dos indicadores escalares, que preservam a ordem da entrada. Verificado nos dois casos. Medido com junção pela chave `(estação, datetime)`: **2.896 de 4.000 (72,4%)**, diferença média 4,118 °C, e sempre na mesma direção — misturar estações só pode aumentar a amplitude.

As comparações de fixture deste módulo estão a salvo porque usam apenas indicadores escalares. Mas qualquer teste futuro que compare um indicador de janela posicionalmente cai na mesma armadilha, e há agora um teste que registra explicitamente a diferença de comportamento.

## 2026-09-14 — `et_c` do R é NA para vento abaixo de 0,2 m/s  ·  **M78**

- **Onde:** `climasus4r:::.compute_et()` — lado R.
- **O quê:** põe um piso no vento com `ws_safe <- pmax(ws, 0.04)` e em seguida calcula `1.1 * (ws_safe - 0.2)^0.5`. Abaixo de 0,2 m/s o radicando fica **negativo** e o R devolve `NaN` — o piso de 0,04 é anulado pela subtração de 0,2 que vem depois. Parece intenção frustrada, não decisão.
- **Medido no R** (T=30, RH=60): vento de 0, 0.04, 0.10 e 0.19 **todos** dão NA; 0.20 dá 26,8. E o dispatcher zera vento ausente antes de chamar, então vento faltante cai no mesmo ramo.
- **Não é canto raro:** 76 das 4.000 linhas da fixture ficam abaixo de 0,2 m/s — e noite de calma é exatamente quando uma temperatura *efetiva* importaria, porque com ar parado a sensação se afasta **mais** da temperatura seca, não menos.
- **No Python:** `et_c` replica o R (paridade exata, nulos 154/154). A variante `et_calm` põe o piso no **radicando**, recuperando 74 das 76 linhas de calma; acima de 0,2 m/s as duas batem exato.

## 2026-09-14 — `heat_stress_risk` do R rotula ausência como `"None"`, igual ao frio  ·  **M79**

- **Onde:** `climasus4r:::.compute_heat_stress_risk()` — lado R.
- **O quê:** mesma forma do M70 — o `case_when` termina em `TRUE ~ "None"` e esse ramo captura o `NA`. Verificado: T ausente → `"None"`, RH ausente → `"None"`.
- **Pior que o M70, por uma razão específica:** `"None"` é **também** a resposta legítima para tempo frio — um WBGT de 15 °C é corretamente `"None"`. Então "sem risco" e "sem dado" viram a mesma string. No koppen o rótulo fabricado era `"Perhumid"`, que chama atenção; aqui ele se esconde no **valor mais comum da coluna**: 3.049 de 4.000 linhas saem `"None"`, e 80 delas não têm dado.
- **No Python:** paridade de 4.000/4.000. A variante `heat_stress_risk_strict` devolve `NULL` quando falta T ou RH; fora dessas linhas concorda com a do R, e a diferença na contagem de `"None"` é exatamente 80.
- **Defeito meu corrigido no caminho, e o mecanismo vale registrar:** eu substituía a expressão **crua** do WBGT na classificação, e o R classifica o que `.compute_wbgt()` **devolve** — já arredondado em duas casas. Três linhas caíram uma faixa acima, cada uma exatamente sobre um limiar (20,00, 28,00, 30,00), porque um WBGT de 19,997 cru é `"Low"` e arredondado é `"None"`. O arredondamento passou para dentro do fragmento `_WBGT_EXPR`, e o `wbgt_c` reusa o mesmo fragmento — então as faixas de risco não podem mais divergir da coluna que classificam.

## 2026-09-14 — `utci_c` é aproximação de seis termos, não o polinômio UTCI  ·  **M80**

- **Onde:** `climasus4r:::.compute_utci()` — replicado no Python.
- **O quê:** a ajuda do R descreve como *"Fiala-polynomial-inspired multi-term regression"* e cita Bröde et al. (2012), o artigo do UTCI. **O hedge é justo** — o UTCI publicado é polinômio de sexta ordem com 210 termos, e o R diz *"inspired"*, não *"implements"*. Por isso isto **não** é tratado como defeito; é nota de interpretação.
- **Medido** rodando o `.compute_utci` do R (T=30, RH=60, ws=2): resposta ao sol de **4,97 °C** ao longo de 0 a 1000 W/m² (30,39 no escuro, 35,36 em sol pleno), onde o UTCI publicado passa de 10 °C acima do ar. Resposta ao vento de **−3,01 °C** de 0,5 a 20 m/s. As duas subestimam.
- **O teto de 50 °C atua em dado real**, não é limite defensivo distante: na fixture o máximo é exatamente 50,00, truncando 2 linhas. O piso de −60 não foi alcançado.
- **No Python:** paridade exata, 0,00e+00 em 3.920 valores.

## 2026-09-14 — PET lê uma coluna chamada `date` e ignora o `datetime_col`  ·  **M81**

- **Onde:** `climasus4r:::.compute_pet()` e o dispatch — lado R, replicado.
- **O quê:** o PET aplica um ajuste sazonal de vestuário derivado do mês, e para achar o mês o dispatch passa `df[["date"]]` — **nome fixo**, não o `datetime_col` que a função aceita como parâmetro e detecta sozinha. Sem coluna chamada literalmente `date`, o `inherits()` falha e o mês vira **6 para todas as linhas**: o ajuste documentado fica inerte, em silêncio.
- **Verificado:** a fixture chama a coluna de `datetime`, e a saída do R sobre ela é **idêntica** ao ramo do mês 6. Numa série do `sus_climate_inmet` — cuja coluna de tempo não se chama `date` — o recurso nunca entra.
- **O efeito é pequeno:** janeiro 31,25 contra julho 31,13, diferença de **0,12 °C**. Então o que se perde é um *recurso documentado*, não muita exatidão — e é essa a razão de registrar em vez de corrigir: o conserto mudaria número publicado para ganhar 0,12 °C.
- **Detalhe adicional:** o PET **não** tem limite, ao contrário do UTCI, que o R limita em [−60, 50]. Medido: T=60 → 63,60; T=−50 com vento 30 → −59,17.
- **No Python:** replicado, coluna `date` e tudo. Paridade de 99,82% — 7 de 3.920 linhas diferem por 0,01, todas casos de meio exato, o mesmo desempate do `thi_c`.
