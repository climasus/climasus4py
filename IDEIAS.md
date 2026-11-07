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

## 2026-09-14 — três dos doze parâmetros de bioma são declarados e nunca lidos  ·  **M82**

- **Onde:** `climasus4r:::.region_params_table()` — lado R.
- **O quê:** a tabela declara 12 parâmetros para cada um dos 8 biomas. Varrendo o corpo de **todas** as funções internas do pacote *exceto a própria tabela* — onde os nomes são definidos — três não aparecem em lugar nenhum: **`wbgt_high_warning`, `wbgt_extreme` e `metabolic_factor`**. Zero ocorrências fora da tabela; os outros nove aparecem entre 1 e 5 vezes, todos no `.dispatch_indicator`.
- **Importa mais no `wbgt_extreme`:** ele vai de **31 na Amazônia a 33 na Caatinga**, e a documentação destaca *"regionally adapted thresholds"* como ponto de robustez científica. Mas as flags são alimentadas por `reg$thresholds` — do **registry** de indicadores, com valores fixos 31/28/25 — e não pelos `region_params`. O limiar por bioma é declarado, documentado e nunca chega ao cálculo.
- Somado ao **M72**, são **duas rotas independentes** pelas quais limiares declarados não alcançam as flags.
- **No Python:** os nove lidos foram portados; os três não, de propósito, com teste que falha se alguém os adicionar sem ligar o uso.

## 2026-09-14 — com os defaults, o R **desliga** a máscara de validade  ·  **M83**

- **Onde:** corpo de `sus_climate_compute_indicators` — lado R, replicado.
- **O quê:** `apply_mask = apply_validity_mask && !use_region`, com `use_region <- region != "none"`. Como o default é `region="auto"`, sai `apply_mask = TRUE && !TRUE = FALSE`: a máscara fica **desligada no caminho padrão**, e as regressões de HI, WCET e WCT extrapolam para fora do domínio em que foram ajustadas. É preciso pedir `region="none"` explicitamente para ligá-la — `apply_validity_mask=TRUE` sozinho **não basta**, o que inverte a leitura natural do nome do parâmetro.
- **Medido, rodando os helpers do R:** com T=20 e RH=90 o índice de **calor** devolve **18,48 °C** — *abaixo* da temperatura do ar. Na fixture o `hi_c` desce a **−16,38 °C**, um índice de calor abaixo de zero. E o `wcet_c`, fórmula de sensação térmica de **frio**, chega a **+53,39 °C**; com T=25 e vento 5 m/s devolve 26,34 em vez de NA. O `wct_c` vai a +51,10.
- **A documentação do R está correta aqui** — ela diz explicitamente *"When region==\"none\", masks HI, WCET, WCT outside their valid meteorological domains"*. Então não é divergência doc/código como no M71: é que o **default** entrega o caminho sem máscara, e os valores resultantes não têm significado físico.
- **Correção de uma falha na minha verificação.** Todas as comparações que eu fiz antes disso para `hi_c`, `wcet_c` e `wct_c` chamaram os helpers do R com `apply_mask=TRUE` — configuração que o R só usa com `region="none"`. Verifiquei paridade contra um caminho **não padrão** e reportei como "paridade exata" sem qualificar. Agora há **duas fixtures**, e os dois caminhos batem com diferença máxima 0,00e+00 e os nulos nas mesmas linhas.

## 2026-09-14 — a varredura de Kulldorff mede distância em **graus**  ·  **M84**

O `sus_mod_spatial_scan` faz `st_centroid(st_transform(municipalities, 4326))` e entrega as coordenadas em graus ao `SpatialEpi::kulldorff()`. O `zones()` do SpatialEpi aplica `dist()` — distância euclidiana simples — sobre o que receber.

- **As janelas não são círculos.** Um grau de longitude é mais curto que um grau de latitude fora do equador, então as janelas são **elipses achatadas no sentido leste-oeste**, e o achatamento varia com a latitude: no Brasil a razão vai de ~1,00 na fronteira norte a ~0,84 no extremo sul. Um mesmo raio em graus cobre áreas fisicamente diferentes conforme a região.
- **O efeito é limitado, não nulo.** A distância só serve para **ordenar** vizinhos em torno de cada centro; não entra na verossimilhança. Mas muda quais municípios entram em quais zonas candidatas, e portanto pode mudar o aglomerado encontrado.
- **O próprio SpatialEpi exporta `latlong2grid()`** exatamente para essa conversão, e documenta como o passo anterior ao `kulldorff()`. O `climasus4r` não a chama.
- **No Python:** replicado igual, com o motivo no docstring da função. Não corrigido, para manter paridade.

## 2026-09-14 — o C++ do SpatialEpi **trunca os totais** do estudo  ·  **M85**

O `computeAllLogLkhd()` do SpatialEpi trunca para inteiro os **totais** do estudo — total de casos, de esperados e de população — enquanto as somas **por zona** seguem em dupla precisão. Determinado empiricamente: numa grade de 300 configurações de duas áreas, a fórmula com os totais truncados bate em **300/300** com diferença máxima 1,8e-12; sem o truncamento não bate em nenhuma.

- **Importa porque contagem esperada quase nunca é inteira** — ela é taxa × população. O caminho `expected=<coluna>` do `sus_mod_spatial_scan` é exatamente o caso afetado: o modelo nulo passa a ser ajustado contra um total que falta até um caso.
- **Medido nos dois extremos.** Na fixture de 42 municípios, com 4090,66 casos esperados, truncar para 4090 desloca a log-verossimilhança do aglomerado mais provável de 401,0385 para 400,9031 — **0,03%**. Num estudo pequeno o efeito é grande: com total esperado 12,6 truncado para 12, a estatística cai de 1,3643 para 1,1126, **18%**.
- **Correção de uma suposição minha.** Eu supus que o erro relativo encolheria com o tamanho do estudo e **medi que não**: na grade de 300 casos a correlação entre a fração descartada e o erro relativo é praticamente zero (−0,01), e o erro chega a 1,8% com totais na casa dos milhares. O que a medição mostra é que o erro é maior onde a **estatística é pequena** (correlação −0,5 entre os logs) — justamente onde a decisão de significância se decide.
- **A origem é o SpatialEpi, não o `climasus4r`** — vale reportar upstream.
- **No Python:** replicado, com a chave de módulo `SPATIALEPI_TRUNCATES_TOTALS`. Em `False` sai a estatística correta, sem paridade; o caminho corrigido já está escrito e testado, para não precisar ser reescrito depois.

## 2026-09-14 — a partir do 10º aglomerado secundário o mapa de categorias **quebra**  ·  **M86**

No ramo `show_rr=FALSE`, o R monta as cores com `palette_pool[seq_len(min(n_sec, 9))]` — no máximo 9 — e depois chama `setNames(c("firebrick", sec_colors, "grey80"), cluster_levels)`, onde `cluster_levels` tem `1 + n_sec + 1` entradas. Com 10 ou mais secundários há mais nomes que valores e o `setNames` aborta: *"'names' attribute [n] must be the same length as the vector [11]"*. O gráfico não sai.

- **Verificado de ponta a ponta** contra o pacote instalado, montando objetos `climasus_spatial_scan` com N secundários e forçando `ggplot_build`: com 3 e com 9 desenha, com 10 e com 12 aborta.
- O ramo `show_rr=TRUE`, que é o default, não usa paleta e **não é afetado**.
- **É alcançável.** O `sus_mod_spatial_scan` colhe secundários sem limite superior — só para quando um deles passa do alpha. Num estudo nacional, passar de 9 aglomerados disjuntos e significativos é plausível.
- **No Python: divergência deliberada.** A paleta é ciclada, então o 10º reusa a cor do 1º em vez de matar a figura. O motivo de não replicar: **não existe saída a que ser fiel**, porque o R não produz nenhuma — diferente dos outros casos, onde replicar preserva um valor comparável. Se você preferir paridade estrita, é trocar o `itertools.cycle` por um erro.

## 2026-09-14 — com um único aglomerado, o mapa de RR pinta o aglomerado de **branco**  ·  **M87**

O ramo default usa `scale_fill_gradient2(low="navy", mid="white", high="red", midpoint=1)`, que reescala a cor pelo **intervalo dos dados**. Municípios fora de aglomerado têm RR `NA` e vão para o cinza; os de dentro recebem o RR do seu aglomerado. Quando a varredura acha **um aglomerado só**, todos os municípios coloridos dividem o mesmo RR e o intervalo é **degenerado** — e tanto o `scales::rescale_mid` do R quanto o `mizani.bounds.rescale_mid` do plotnine tratam faixa nula devolvendo o ponto médio (0,5), que é a cor do **meio**: branco.

- **O aglomerado sai da mesma cor que "risco igual ao nulo"** — o oposto do que o achado significa — e a legenda degenera para um único tick sem rampa. Só o contorno preto denuncia que ali há alguma coisa.
- **Medido** na fixture de 42 municípios: os 4 municípios do aglomerado têm RR = 2,2347 e o R os renderiza com fill `#FFFFFF`, limites da escala `[2,234701, 2,234701]`. No Python o mizani devolve `#fffefe`, visualmente idêntico. Com dois RR distintos (1,2 e 2,8) as duas bibliotecas pintam certo, o maior indo a vermelho pleno.
- **A gravidade está na frequência:** `show_rr=TRUE` é o **default** e um aglomerado único é o caso **comum** — na própria fixture o scan devolve zero secundários. O gráfico padrão de um resultado típico se lê ao contrário.
- **Correção possível** (não aplicada, para manter paridade): fixar os limites da escala em algo como `c(min(1, min(RR)), max(RR))`, ou simétrico em torno de 1, para que a rampa exista mesmo com um valor único.
- **Paridade sai de graça:** as duas bibliotecas fazem a mesma coisa, então não houve decisão a tomar.
- PNGs do R e do Python lado a lado em `docs/plots_para_conferir/` — **conferência visual pendente**.

## 2026-09-14 — a tabela de efeitos fixos perde o intercepto e ganha uma variância  ·  **M88**

O `sus_mod_spatial_bayes` escolhe quais linhas do `summary.results` do CARBayes são efeito fixo por casamento de padrão:

```r
coef_pattern <- "^(Intercept|[A-Za-z_][A-Za-z0-9_\.]*)"
excl_pattern <- "(tau2|nu2|rho|Sigma|phi|psi|delta|gamma)"
```

**Instalei o CARBayes 6.1.1 e rodei um ajuste de verdade** (BYM, Poisson, offset de esperados, 42 municípios, 6000 iterações). O CARBayes devolve `(Intercept)`, `temp_media`, `tau2`, `sigma2`. O filtro erra nas **duas pontas**:

| linha | casa coef | casa excl | resultado | correto? |
|---|---|---|---|---|
| `(Intercept)` | não | não | **descartado** | ✗ |
| `temp_media` | sim | não | mantido | ✓ |
| `tau2` | sim | sim | descartado | ✓ |
| `sigma2` | sim | **não** | **mantido** | ✗ |

- **O intercepto some** porque os parênteses de `(Intercept|...)` são um **grupo de alternância**, não caracteres literais — o padrão exige começar com a palavra `Intercept` ou com letra. O nome real começa com `(`. Que a intenção era outra fica claro pelo próprio padrão começar por `Intercept|`.
- **O `sigma2` fica** porque a exclusão escreve `Sigma` com S maiúsculo e o `grepl` é sensível a caso. O `tau2` é corretamente excluído — o que prova que a intenção era tirar variância.
- **No gráfico** `type="coef"` isso vira uma **variância** (sempre positiva, nunca cruza a referência no zero) ao lado de uma razão de taxas em log, no mesmo eixo, e sem o intercepto. No ajuste de teste sobram `temp_media` (0,1397; IC 0,0765–0,2063) e `sigma2` (0,0332; IC 0,0043–0,0740).
- **Os dois erros se compensam na contagem** — entram 2 linhas, saem 2 linhas — o que provavelmente explica ter passado despercebido.
- **De quebra:** o `summary.results` do CARBayes não tem coluna `SD` (tem `Mean`, `2.5%`, `97.5%`, `n.sample`, `% accept`, `n.effective`, `Geweke.diag`). O R já trata isso com um `if`, mas o efeito é que a coluna `sd` fica **sempre NA** em ajuste de CARBayes.
- **Correção sugerida:** `"^\(?Intercept\)?$"` para o intercepto e `ignore.case = TRUE` (ou incluir `sigma`) na exclusão.
- **No Python:** o ajustador segue stub, então não há o que corrigir; o `plot_spatial_bayes` exibe o que recebe, com o defeito no docstring e fixado em teste sobre a fixture de ajuste real.

## 2026-09-14 — `sus_mod_spacetime_bayes` **não roda no Windows**  ·  **M89**

A função escreve o grafo de vizinhança num arquivo temporário e **interpola o caminho desse arquivo numa string de fórmula**, que depois passa por `as.formula()` — isto é, por *parse de código R*:

```r
graph_file <- tempfile(fileext = ".graph")
spatial_term <- paste0("f(.area_idx_st, model='bym2', graph='", graph_file, "', ...")
fml <- stats::as.formula(fml_str, env = parent.frame())
```

No Windows o `tempfile()` devolve caminho com **barra invertida**. Medido nesta máquina: `C:\Users\KUJOJO~1\AppData\Local\Temp\Rtmp8GQMeb\file6543ea16260.graph`. Dentro de uma string literal de R cada barra invertida inicia um escape.

**Dois regimes, os dois medidos:**

1. **Erro duro** — e é o caso do caminho padrão. Com `C:\Users\...` o parse aborta em `'\U' used without hex digits in character string`. Também falham `C:\temp\x.graph` (`\x` sem hex) e `C:\dados\` (`\d` unrecognized). A função **simplesmente não executa**.
2. **Corrupção silenciosa** — pior. Quando todas as barras calham de formar escapes **válidos**, o parse passa e o caminho vira outro. Medido: `C:\temp\afile.graph`, 19 caracteres, parseia sem erro e produz valor de **17** caracteres, bytes `67 58 **9** 101 109 112 **7** 102 105 108 101 46 103 114 97 112 104` — o byte 9 é TAB (de `\t`) e o 7 é BEL (de `\a`). O INLA recebe um arquivo inexistente e falha depois, com mensagem que não aponta para a causa.

- **`TMPDIR` não contorna:** o R normaliza para barra invertida de qualquer jeito — com `TMPDIR=C:/inlatmp` o `tempdir()` devolve `C:\inlatmp\RtmpkXwrG4`.
- **Correção: uma linha** — `graph_file <- gsub("\\\\", "/", tempfile(fileext = ".graph"))`. Confirmado: com essa única alteração a função ajusta normalmente (17,8 s para 42 áreas × 8 períodos, BYM2 + RW1, Poisson). Melhor ainda seria montar a fórmula como expressão em vez de string.
- Foi assim que a fixture de spacetime deste repositório foi gerada: com uma cópia da função carregando exatamente essa correção de uma linha, e nada mais.
- **INLA 26.8.7 instalado** em 14/09/2026 (`inla.r-inla-download.org/R/stable`).

## 2026-09-14 — o `time_idx` é índice mas a exceedência o lê como **ano**  ·  **M90**

As duas funções discordam sobre o que a coluna significa. O `sus_mod_spacetime_bayes` escreve em `fit$rr$time_idx` um **índice de base 1** (1, 2, 3, …), não o rótulo temporal original. O `sus_mod_spacetime_exceedance`, ao agregar, faz `as.Date(paste0(as.integer(time_idx), "-01-01"))` — trata o índice como **ano do calendário**.

Medido sobre ajuste real (painel 2015–2022, 42 municípios × 8 anos):

1. **Os rótulos saem `"0001"`…`"0008"`** em vez de `"2015"`…`"2022"`. Os anos de verdade desaparecem.
2. **A agregação não agrega nada.** Cada índice já é único dentro de cada município, então cada "ano" tem exatamente uma célula: entram 336, saem 336. O `aggregate_time` não faz o que promete em **nenhum** ajuste vindo do `spacetime_bayes`. Verifiquei nos dois sentidos: com `time_idx` sintético de anos repetidos a agregação funciona e reduz 8 períodos para 4.

- **Correção:** o ajustador precisa preservar o rótulo temporal original (ou devolver uma coluna `time_label`), e a exceedência usá-la.
- **Observação separada, que eu não classifiquei como defeito:** ao agregar, o sigma do log-RR é a **média dos sigmas** por período, não o sigma da média. Agregar *n* períodos portanto **não estreita** o intervalo — o valor agregado carrega a incerteza típica de um período. Com RW1 as células são correlacionadas, então parte da conservação se justifica; o quanto não está derivado nem documentado. Vale perguntar ao criador se foi intencional.
- **No Python:** replicado exato, inclusive os rótulos `"0001"`. O ramo numérico é formatado aritmeticamente e não via timestamp, porque o ano 1 está fora do alcance do `datetime64[ns]` do pandas (piso em 1677) — e o ano 1 é exatamente o que um índice de base 1 produz.

## 2026-09-14 — a tabela de interação espaço-tempo vem **mal rotulada**  ·  **M91**

O slot `interaction_re` do `sus_mod_spacetime_bayes` tem os rótulos de município e período **desalinhados dos valores**. Medido num ajuste real com `interaction_type="II"`, 42 municípios × 8 períodos = 336 células:

| | |
|---|---|
| linhas devolvidas | **2688** |
| células únicas | 336 |
| células que aparecem 1 vez | 328 |
| células que aparecem **295 vezes** | **8 — todas do município `3506005`** |

**2360 dos 2688 valores estão atribuídos a um único município.** E não é duplicação de um mesmo número: dentro de uma dessas células os 295 `gamma_mean` são **todos diferentes** (295 valores únicos, de −1,0109 a 0,2116) — são 295 efeitos aleatórios distintos rotulados como a mesma célula.

- **Sinal adicional:** `gamma_sd` fica entre 6,34 e 7,22 em toda a tabela — implausível para efeito aleatório em escala log, onde isso é um IC95 de e^±13.
- **No gráfico:** o `geom_tile` sobrepõe as repetidas, então o azulejo visível é o que desenhar por último, arbitrariamente; e a ordenação das linhas (o R ordena pela média de gamma por município) fica distorcida, porque esse município média 2360 valores e cada outro média 8.
- **No Python:** replicado, com a medição fixada em teste. O defeito é a montante.

## 2026-09-14 — todo rótulo do `plot_spacetime` sai como a **chave interna**  ·  **M92**

O helper `.stl()` procura os rótulos em `.st_msgs` — que é a tabela de mensagens do `spacetime_bayes` e **não contém nenhuma chave de gráfico**. Quando não acha, devolve a própria chave.

A tabela certa existe: `.st_plot_labels`, **32 chaves nas três línguas**. Verifiquei nos dois sentidos: `rr_map_title` não está em `.st_msgs`, está em `.st_plot_labels`, e a varredura do corpo de **todas** as funções do pacote procurando `.st_plot_labels` não retorna **nenhuma**. A tabela é completamente órfã.

Medido na saída renderizada:

| type | título | eixo x | eixo y |
|---|---|---|---|
| `rr_map` | `rr_map_title` | — | — |
| `temporal` | `temporal_title` | `x_time` | `y_rr` |
| `coef` | `coef_title` | `coef_x` | `coef_y` |

- **O `lang` não tem efeito nenhum** nesta função — as três línguas dão os mesmos rótulos, porque a chave é a mesma.
- **Correção: uma linha** — o `.stl` deve ler `.st_plot_labels`.
- **No Python: divergência deliberada**, atrás de `USE_INTENDED_LABELS`. Por padrão usa a tabela pretendida, copiada verbatim (inclusive o typo *"Excedaência"* no título em português, para que ligar/desligar a chave mude só qual tabela é consultada e nunca o texto). Em `False` reproduz o R.

## 2026-09-14 — no mapa de RR a escala de cor está **invertida**: risco alto sai azul  ·  **M93**

O `.st_plot_rr_map` passa `brewer.pal(11, palette)[1]` como extremo **low** e `[11]` como **high**. Para a `RdYlBu` padrão, `[1] = #A50026` (vermelho escuro) e `[11] = #313695` (azul escuro) — então **risco baixo é vermelho e risco alto é azul**.

Confirmado na saída renderizada, não só no fonte: a escala reporta `low=#A50026 / high=#313695`, e o município de RR mais alto da fixture (1,879) recebe o preenchimento `#313695`.

- **É inconsistente com os próprios irmãos**, o que reforça que não foi intencional: o `.st_plot_exceedance`, no mesmo arquivo, vai de branco a `#a50026` conforme a probabilidade **sobe**; o `sus_mod_plot_spatial_bayes` vai de navy a red3 conforme o risco **sobe**. Só o `rr_map` anda ao contrário.
- Causa provável: supor que o `brewer.pal` devolve do frio para o quente — vale para as sequenciais (`Blues`, `Reds`), não vale para as divergentes que começam no quente.
- **No Python: replicado** por padrão, atrás de `INVERT_RR_PALETTE`. Portadas as oito paletas divergentes de 11 classes, com os hex da própria ColorBrewer nos extremos.

## 2026-09-14 — `type="exceedance"` **sempre aborta**: lê uma coluna que ninguém produz  ·  **M94**

O `.st_plot_exceedance` monta o mapa a partir de uma coluna `exceedance_prob`. Varri o corpo de todas as funções do pacote: `exceedance_prob` aparece em **exatamente um lugar — o próprio painel**. Nenhuma função a escreve. O `sus_mod_spacetime_exceedance` produz `p_gt_1`, `p_gt_1_5`, `p_gt_2`.

Medido: com a saída da exceedência, aborta em *"objeto 'exceedance_prob' não encontrado"*; com o próprio ajuste, aborta antes em `err_exceedance_type`. **O tipo não funciona com nenhuma entrada possível.**

- O painel também trata `x$exceedance` como se pudesse ser uma **lista indexada por limiar** (`exc_list[[thr_key]]`, com mensagem `err_threshold_not_found`), mas a função devolve um único data frame — vestígio de um formato anterior.
- **No Python: divergência deliberada**, atrás de `EXCEEDANCE_COLUMN_FROM_THRESHOLD`. Seleciona `p_gt_<limiar>` usando o parâmetro `threshold` que já existe e que a própria legenda descreve (`P(RR > {thr})`). Mesmo critério do M86: não há saída a que ser fiel.

### Critério usado nas divergências desta função

Replicar quando a saída do R **carrega a informação em forma ruim** (M91, M93 — quem lê a legenda ainda interpreta). Corrigir quando **não carrega informação nenhuma** (M92, um nome de variável não é um título; M94, não há saída).

## 2026-09-15 — a semana começava na **segunda**; no R começa no **domingo**  ·  **M95**

O R agrega com `lubridate::floor_date(date, unit = time_unit)` e **não passa `week_start`**, herdando o default do lubridate — `getOption("lubridate.week.start", 7)`, isto é **7 = domingo**. Conferido no lubridate instalado: o formal é literalmente esse e a opção está `NULL`. O Python usava `DATE_TRUNC('week', ...)` do DuckDB, que é ISO e começa na **segunda**.

**O efeito não era de borda.** Sobre seis datas espalhadas por 2023, **todas as seis** caíam em bucket diferente:

| data | dia | Python (ISO) | R (lubridate) |
|---|---|---|---|
| 2023-01-01 | dom | 2022-12-26 | **2023-01-01** |
| 2023-01-08 | dom | 2023-01-02 | **2023-01-08** |
| 2023-06-15 | qui | 2023-06-12 | **2023-06-11** |
| 2023-12-25 | seg | 2023-12-25 | **2023-12-24** |
| 2023-03-19 | dom | 2023-03-13 | **2023-03-19** |
| 2023-09-03 | dom | 2023-08-28 | **2023-09-03** |

Toda série semanal do Python saía deslocada, sem aviso — e o deslocamento **não é constante**, depende do dia da semana da observação.

- **Domingo é também a convenção certa aqui:** a semana epidemiológica da SVS vai de domingo a sábado, e o caminho de variáveis do R usa `lubridate::epiweek()`, que é baseada em domingo. Um agregado semanal com fronteira de segunda não corresponde a nenhuma semana epidemiológica publicada.
- **Corrigido** (correção silenciosa, §3): `DATE_TRUNC('week', d + INTERVAL 1 DAY) - INTERVAL 1 DAY`. Verificado contra o lubridate em 8 de 8 datas, mais teste de propriedade varrendo os 365 dias de 2023.
- **Como ficou escondido:** os 9 testes do arquivo falhavam desde a migração da API — chamavam `time=` em vez de `time_unit=` — e **nunca chegavam na asserção da semana**. O teste original até documentava a intenção certa: *"2023-01-01 is a Sunday → SVS week 01/2023"*. Um erro de nome de parâmetro escondeu um bug de semântica.

## 2026-09-15 — o `sus_climate_inmet` calculava o metadado e não anexava  ·  **M97**

O R anexa um `sus_meta` descritivo: `system, stage, type="inmet", spatial, temporal{start,end}, created, modified, years, ufs, station_codes, n_stations, n_observations, history, user`. O Python anexava só o núcleo de estágio — `stage, system, type, stages, history` — com `type` valendo `None`. **Faltavam dez chaves.**

O que torna isso claramente lacuna e não decisão: a função **já calculava** `n_stations` (COUNT DISTINCT) e `temporal` (MIN/MAX da data), e usava os dois **apenas** para compor a string de history e a linha do verbose. Os valores estavam na mão e não chegavam ao metadado.

- **Corrigido:** o `set_stage` ganhou `extra=` e o INMET passa as dez chaves, com os **nomes do R** (`temporal`, não `temporal_coverage` — este era invenção do Python antigo, e o teste velho o cobrava).
- **De quebra, uma correção no `_stage`:** o `set_stage` reconstruía o dicionário a partir das cinco chaves de núcleo, então qualquer campo descritivo anexado por uma função de leitura era **descartado** na próxima vez que um estágio fosse registrado. Agora as chaves desconhecidas são carregadas adiante.
- **Sobre o mock, que não é defeito do pacote:** o monkeypatch devolvia `pandas.DataFrame` onde o `_download_inmet` real é anotado `-> DuckDBPyRelation`. Os testes sem `station_code` passavam por acidente; o filtro chama `.filter()` com SQL e quebrava. O mock passou a honrar o contrato real.

## 2026-09-15 — o validador aceitava colunas que o detector não resolvia  ·  **M96**

O `_validate_health_data` do `climate_aggregate` conferia a coluna de município contra a sua `_MUNI_CANDIDATES` e a de data contra `_DATE_CANDIDATES`. Mas o join **não usava essas listas** — pedia a coluna ao `detect_geo_column`/`detect_date_column`, cujas listas eram **outras**. Onde divergiam, o dado passava na validação, o detector devolvia `None`, e o `None` era interpolado direto no SQL:

```
Binder Error: Values list "h" does not have a column named "None"
```

— que não nomeia a coluna culpada nem sugere o que fazer.

**Aceitos pelo validador e invisíveis ao detector:**

| tipo | nomes |
|---|---|
| município | `code_muni`, `notification_municipality_code`, `MUNI_RES` |
| data | **`DT_NOTIFIC`**, **`DT_INTER`** |

Reproduzido de ponta a ponta: `CODMUNRES`+`DTOBITO` funcionava; as outras quatro combinações quebravam todas com o mesmo erro.

- **Importava de verdade:** `DT_NOTIFIC` é a data de notificação do **SINAN** — o caminho de dengue, caso de uso central de uma biblioteca de clima e saúde. `DT_INTER` é a de internação do **SIH**. Os dois sistemas estavam inalcançáveis nesta função.
- **Corrigido** nos dois lugares, acrescentando no **fim** de cada lista para não mexer na precedência: os três de município no `detect_geo_column` (hardcoded no Python) e os dois de data no `role_priority.date` do `climasus-data` (de onde o `detect_date_column` lê). Diff de **uma linha** no `climasus-data`.

### O problema de fundo, que eu não corrigi

Existem **três** listas que deveriam concordar e não concordam:

| fonte | nomes de município |
|---|---|
| `climasus-data` `role_priority.municipality` | 3 |
| `detect_geo_column` (hardcoded) | 10 (era 7) |
| `climate_aggregate._MUNI_CANDIDATES` | 7 |

O `detect_date_column` lê do `climasus-data`, mas o `detect_geo_column` **ignora** o `role_priority.municipality` e mantém lista própria. São duas fontes de verdade para a mesma coisa, e foi essa divergência que produziu o bug.

Unificar muda a **precedência** de escolha de coluna — que é exatamente o que o M21 mostrou ser sensível: residência contra ocorrência são recortes epidemiológicos diferentes. Não fiz em silêncio; é decisão do coordenador.

## 2026-09-15 — chaves de categoria na língua errada não traduzem  ·  **M7** (ampliado)

O achado original era **uma** chave órfã no dicionário inglês (`mother_education_level` em vez de `mother_education`), já corrigida. Varrendo os três dicionários de forma sistemática, o problema é muito maior — e não estava no inglês.

**Como medir certo:** a tradução de categoria é aplicada **depois** do rename de coluna, e a busca é `col in cat_map` com `col` sendo o nome **já traduzido**. Então cada `categories.json` precisa ter as chaves **na sua própria língua**.

> Minha primeira tentativa comparou as chaves **entre** as línguas e acusou 563 assimetrias. Era artefato: as chaves não devem casar entre línguas. A verificação válida cruza, por língua, os nomes traduzidos do `columns.json` com as chaves do `categories.json` daquela língua.

| dicionário | chaves órfãs |
|---|---|
| `pt-en` | 7 (uma é `_meta`, legítima) |
| `pt-es` | **48** |
| `pt-pt` | 18 |

**31 das 48 do `pt-es` eram nomes ingleses** deixados no dicionário espanhol — `autopsy`, `marital_status`, `work_accident`, `death_type`, `education_2010`, `death_location`, `information_source` e mais 24. Os **valores já estavam corretos em espanhol** (`Soltero/a`, `Sí`, `Ignorado`); só a chave estava errada, então cada conserto foi um renome puro, com o destino tirado do próprio `columns.json` da língua.

**Medido antes e depois**, numa relação SIM-DO com `lang="es"`:

| coluna | antes | depois |
|---|---|---|
| `estado_civil` | `'1'` | `Soltero/a` |
| `accidente_trabajo` | `'1'` | `Sí` |
| `autopsia` | `'1'` | `Sí` |
| `asistencia_medica` | `'1'` | `Sí` |
| `tipo_muerte` | `'1'` | `Fetal` |

Usuário de espanhol recebia **código do DATASUS** nessas colunas, em silêncio.

Corrigido no commit `aff7b9d`, por substituição textual das linhas de chave — CRLF e formatação de dois espaços preservados byte a byte, as 213 chaves sobrevivem e todo mapa de valor é idêntico. Órfãs do `pt-es` caem de 48 para 17.

### O que eu disse que sobrava — e estava errado

> Aqui eu registrei que as órfãs restantes eram **6 duplicatas de escolha de conteúdo, 11 nomes sem rota e 18 do `pt-pt` não resolvíveis automaticamente**, e que dependiam de decisão do Andrey.
>
> **Não era isso.** Todas eram mecânicas, e havia autoridade para decidir o conteúdo: o `categories.json` não é a fonte da verdade. Ver o fechamento abaixo.

## 2026-09-15 — um dos quinze dicionários de censo usa a chave `categories`  ·  **M98**

Das quinze funções de dicionário de censo do R, **catorze** devolvem `columns` + `values`. **Uma** — `get_translate_dictionary_pt_households` — devolve `columns` + **`categories`**. O `load_dict` do `sus_census_select` lê `dict_data$values` e nada mais, então para essa combinação o slot vem `NULL` e os rótulos são descartados em silêncio. **Os dados estão lá, sob o nome errado.**

Medido chamando as quinze: 14 usam `values`, 1 usa `categories`, e as três línguas têm as mesmas 49 colunas nesse dataset — o que descarta a hipótese de o português simplesmente não ter as categorias.

**Medido na saída do R:**

| | variáveis categorizadas | rótulos |
|---|---|---|
| `households`, pt | **0** | **0** |
| `households`, en/es | 31 | 152 |
| `all`, pt | 148 | 615 |
| `all`, en/es | 179 | 767 |

O usuário de **português** — a língua nativa do público do pacote — perde 31 variáveis e 152 rótulos, sem aviso.

- **No Python: divergência deliberada.** Ao extrair os dicionários (M38) normalizei a chave para `values`, então o Python devolve 179/767 nas **três** línguas. Fica mais completo que o R em pt. O motivo de não replicar: o dado existe, a chave errada não carrega informação nenhuma, e replicar exigiria escrever de propósito um arquivo incompleto.
- **Correção no R:** renomear a chave, ou `dict_data$values %||% dict_data$categories` no `load_dict`.

## 2026-09-15 — o `manifest.json` está 48% obsoleto e ninguém o lê  ·  **M99**

O `manifest.json` se apresenta como o inventário versionado do repositório compartilhado — `version`, `schema_version`, `last_updated` e uma lista `files` com `path`, `size_bytes`, `md5`, `rows`. O M38 alertava, com razão, que mexer nele exige revisão do coordenador por ser contrato consumido pelas duas linguagens.

**Medido: das 23 entradas originais, 12 conferem e 11 não** — 48% do inventário está desatualizado. O `last_updated` estava em 2026-05-02.

**Não fui eu.** Dos dois arquivos que editei nesta sessão, os dois **já** divergiam antes: `datasus_columns.json` tinha `8049804c` e o manifest dizia `4725c633`; `pt-es/categories.json` tinha `50a916c9` e o manifest dizia `d87c0a92`.

**A causa provável está medida também: nenhuma função das duas linguagens lê esse arquivo em tempo de execução.** No `climasus4py` as únicas menções de "manifest" são o `_manifest.jsonl` do cache de download bruto; no R, as 13 funções que mencionam manifest são manifests locais de cache de grade. Um contrato que ninguém valida envelhece sem sinal — e foi o que aconteceu.

**O que eu fiz e o que não fiz:** acrescentei as 15 entradas dos dicionários de censo com MD5 correto e atualizei o `last_updated`, porque são arquivos novos que eu introduzi e deixá-los fora pioraria a cobertura. **Não** reescrevi os 11 MD5 divergentes — decidir se o arquivo ou o manifest está certo é uma afirmação sobre conteúdo que eu não revisei, e regenerar apagaria justamente a evidência da deriva.

**Decisão pendente:** (a) regenerar a partir dos arquivos, assumindo-os como fonte de verdade; (b) investigar as 11 divergências uma a uma; (c) se é para ser contrato de verdade, **adicionar validação em tempo de carga** em uma das linguagens — sem isso ele volta a envelhecer.

## 2026-09-15 — o SPEI não replicava o `rightmost.closed=TRUE`, e o extremo úmido saía espelhado  ·  **M43**

O `_spei_transform` **diz** replicar `findInterval(x, sorted_calib, rightmost.closed = TRUE)`. O `searchsorted(side="right")` sozinho **não é** essa função: as duas concordam em todo lugar menos no único ponto para o qual a flag existe — quando `x` é igual ao **maior** valor de calibração, o `rightmost.closed=TRUE` trata o último intervalo como fechado e devolve `n-1`; o `searchsorted` devolve `n`.

Conferido no R, inclusive o empate:

```
vec = c(1,2,3,4,4)
x=4    TRUE -> 4    FALSE -> 5
x=4.5  TRUE -> 5    FALSE -> 5     (a flag não atua acima do máximo)
```

**O espelhamento era consequência, não causa.** Com rank `n` a probabilidade de Hazen do máximo vira `(n-0.5)/n`, que é o espelho aritmético exato de `0.5/n`, a do mínimo. Daí a média sair **exatamente** `0.000000` e a amplitude perfeitamente simétrica — que era o sintoma visível.

Medido sobre 96 meses × 5 municípios, mesma entrada nos dois lados:

| | antes | depois |
|---|---|---|
| divergências por escala | 5 | **0** |
| difmax | 0,4078 | **8,9e-16** |
| máximo `spei_1mo` | +2,561682 | **+2,153875** (= R) |
| média | −0,000000 | **−0,004248** (= R) |

### Uma correção ao achado original

Ele registrava como sintoma que *"as 5 linhas recebem o mesmo valor no Python"*. **Não era.** O R também dá o mesmo valor aos cinco: a posição de plotagem depende só do rank e de *n*, e com 96 meses por município todo máximo cai em `(n-1-0.5)/n`. O que estava errado era o **valor** compartilhado, não o compartilhamento. Escrevi um teste afirmando o contrário, ele falhou, e corrigi.

O **SPI não era afetado** porque usa outro caminho — ajuste de gama por momentos, não a ECDF empírica de Hazen. É o que explica ele estar exato desde sempre.

Prioridade: estava como **Média** e eu subiria. É correção silenciosa num índice publicado, e a simetria perfeita disfarçava de propriedade do índice.

## 2026-09-15 — os 8 anos do `sus_climate_inmet` passam: a medição que faltava  ·  **M44**

O M44 não era bug em aberto — era **dívida de verificação**. A causa-raiz (seis `SET` vazando na conexão singleton, rebaixando a sessão em 68× de memória) já estava corrigida desde 07/09 pelo `duckdb_settings`. O que ficou sem medir foi a afirmação original: que a função estoura com 8 anos. Na época não havia cache de download.

Os zips de 2016 a 2024 estão na máquina desde aquela sessão (4,5 GB), e o cache parquet foi escrito em 07/09 16:18–16:21. Reprodução exata, `uf='SP'`:

| anos | linhas | tempo | antes |
|---|---|---|---|
| 1 (2023) | 350.400 | 0,7 s | OK |
| 3 (2021–23) | 1.051.200 | 1,6 s | OK |
| **8 (2016–23)** | **2.744.928** | **4,6 s** | `OutOfMemoryException` |

Os dois primeiros números batem exatamente com os do achado. O de 8 anos materializa 2.744.928 × 27 em 7,4 s, ocupando 796 MB em pandas. A conexão também não é mais rebaixada: 6,2 GiB / 8 threads antes, ~6,0 GiB / 8 threads depois das três chamadas.

**Corroboração do M54 de tabela:** o `memory_limit` derivou 6,2 → 6,1 → 6,0 GiB ao longo das chamadas e estabilizou — exatamente a perda de formatação de ~3% que o docstring do `duckdb_settings` previa ao restaurar com `SET` em vez de `RESET`. Comportou-se como documentado.

**Escopo, para não superafirmar:** o que foi exercitado é a leitura dos 8 anos do cache parquet, a união das 8 relações, o `order("date")` global e a materialização completa — precisamente a etapa sobre a qual o achado dizia *"isso é inferência, não medição"*. O parse a frio dos CSVs sob o orçamento de 96 MB não foi reexercitado; aquele caminho é por ano e já havia sido medido em 07/09.

### Duas hipóteses minhas que a medição derrubou

Antes de ler o achado inteiro eu testei duas coisas erradas, e vale registrar as duas:

1. **Supus que faltava `temp_directory`** para spill em disco — a própria proposta do achado. **Não falta:** vale `.tmp` por default, tanto na conexão do pacote quanto numa crua, e defini-lo explicitamente não resolve um OOM sintético.
2. **Supus acumulação de temp table por ano** — o `_materialize_relation_temp` só é alcançado quando a escrita do cache **falha**; no caminho normal cada ano volta como leitura lazy do parquet.

## 2026-09-15 — radiação solar 33% menor: uma regra de QC tratava hora UTC como local  ·  **M28**

O `utils/inmet_parser.py` tinha uma regra de QC inventada pelo porte:

```sql
CASE WHEN sr_kj_m2 IS NOT NULL
 AND (EXTRACT(HOUR FROM date) >= 18 OR EXTRACT(HOUR FROM date) < 6)
 THEN 0.0 ELSE sr_kj_m2 END
```

Zerava toda radiação das 18h às 05h, por entender que são horas de noite. **Os timestamps do INMET são UTC** — o próprio docstring do módulo declara `date (UTC)` — e o Brasil é UTC−3, então a janela cobria **15h–02h local** e jogava fora a tarde inteira.

Uma linha concreta: estação **A701** (São Paulo Mirante), `2023-01-01 19:00 UTC` — 16h local, janeiro, pico de verão. O CSV cru traz **2604,3 kJ/m²**; o Python devolvia **0**.

Medido com join por `(station_code, date)` sobre SP 2023, 350.400 linhas:

| | Python | R |
|---|---|---|
| média `sr_kj_m2` | 958,29 | 1270,22 |
| zeros | **71.568** | 6.519 |
| linhas com py = 0 e R > 0 | **65.049** | — |
| nessas, o R vale | — | média 881, máx **6471** |

### Por que a assinatura enganava

Isso responde o *"detalhe estranho"* do registro original. A contagem de não-nulos ficava **idêntica** porque a regra transforma valor em **zero**, não em nulo; o mínimo ficava idêntico porque zero já existia nos dois; e o máximo ficava idêntico porque 6670,3 ocorre às 13–17h UTC, **fora** da janela destruída. Batiam as bordas e a contagem, e diferia o meio — exatamente o que o achado descreveu sem conseguir explicar.

Das três hipóteses que ele listou, a certa era a primeira — "tratamento distinto dos zeros noturnos" — só que **na direção inversa**: o Python *criava* os zeros.

### O R não tem regra equivalente

É por isso que **remover** é o que restaura a paridade. O `.verify_solar_radiation` do R calcula a irradiância extraterrestre a partir de latitude, dia do ano e hora usando geometria solar de verdade (declinação, ângulo horário, seno da elevação) e **apenas avisa** quando um valor passa de 110% dela — nunca edita o dado.

Depois da correção: média **1270,2198** contra 1270,2199; 183.819 não-nulos, 6.519 zeros e mediana 1042,9 nos dois; no join valor a valor, **diferença relativa máxima 4,77e-08** — abaixo do epsilon do float32 — e **zero** linhas com Python em 0 e R positivo.

### Três coisas que vieram junto

- **O cache parquet de 2016 a 2022 guardava os valores zerados** e foi invalidado. Sem isso, serviria dado corrompido em silêncio.
- **Um teste afirmava o defeito.** O `test_qc_solar_nighttime_zero` exigia que 125,5 virasse 0,0 — e 125,5 é justamente o valor das 22h UTC da A701 em 2023-01-01, ou seja o teste foi escrito a partir de dado real e petrificou a regra errada.
- **Observação sobre o R, de menor porte:** ele calcula `ha_rad = (hour_h - 12) * 15` usando a hora do timestamp como se fosse solar/local, quando é UTC. Como só **avisa** com isso, a consequência é aviso mal apontado, não dado corrompido — mas o viés de 3h está lá. Vale mencionar ao criador junto do M28.
- **Latente, não exercitada neste dado:** o R manda negativo para 0 e o Python manda para NULL (os limites físicos são `BETWEEN 0 AND 40000`). Em SP 2023 não há negativo, então não aparece.

## 2026-09-15 — o default do Python desacopla a máscara de validade do `region`  ·  **M83**

Decisão do Andrey: seguir a documentação do R em vez do comportamento dele. O `apply_validity_mask` passou a governar a máscara **por conta própria**, independente do `region`. A chave `R_COUPLES_MASK_TO_REGION` (default `False`) reproduz o R exato quando ligada, e é o que as fixtures de paridade usam.

**Divergência deliberada**, pelo mesmo critério do M92 e do M94: os valores que o caminho do R produz — índice de calor a **−16,38 °C**, sensação térmica de frio a **+53,39 °C** — não têm significado físico, então replicá-los fielmente seria entregar um default sabidamente ruim. Não é o caso do M87 e do M93, onde a saída carrega a informação em forma ruim e a replicação foi mantida.

Verificado: no default do Python o `wcet_c` passa a ter **2.162 nulos com qualquer `region`** (inclusive `auto`, `southeast`, `amazon`, `south`), contra 40 antes; o `hi_c` não desce mais abaixo de zero e o `wcet_c` não passa de 50; e `apply_validity_mask=False` continua sendo o jeito de desligar.

Conferi também que **desacoplar a máscara não desacoplou os parâmetros de bioma** — as bases de graus-dia seguem mudando por `region`.

**Um tropeço:** usei `monkeypatch` de escopo de módulo na fixture de paridade, e ele só desfaz no teardown do arquivo — a chave ficava ligada para as classes seguintes, que testam justamente o oposto, e três testes do default falharam. Troquei por set/restore explícito com `try/finally`.

No R segue aberto: a correção lá é desacoplar do mesmo jeito, ou mudar o default de `region`.

## 2026-09-15 — o aviso de fallback do `sus_pipeline` deixou de prometer equivalência  ·  **M52**

O próprio achado delimitava o que era possível sem o coordenador: *"enquanto não houver decisão, o aviso de fallback não deveria prometer equivalência."* A frase literal era **"Results should be equivalent but slower"** — falsa nas três dimensões que o registro já tinha isolado.

Prometer equivalência num aviso é **pior que não avisar**, porque quem lê para de conferir.

O aviso passou a nomear cada diferença com o número que a sustenta:

| dimensão | fast | staged |
|---|---|---|
| esquema | `time_group / state / count` | `date / <detectada> / n_deaths` |
| geografia | estado da **residência** | município de **ocorrência** |
| total (SP 2023) | 334.303 | 333.968 |

Os 335 de diferença são exatamente o que o `sus_data_clean_encoding` deduplica — o staged roda, o fast, sendo CTE único sobre o parquet, não.

**Também documentado na assinatura:** o docstring ganhou a seção *"The two paths do not produce the same table"*. Importa porque **qual caminho roda não é escolha do usuário** — é decidido a partir dos argumentos — então quem lê a API precisa saber antes de cair no fallback.

11 testes novos, entre eles uma guarda parametrizada que exige o aviso citar `time_group`, `n_deaths`, `occurrence`, `clean_encoding`, os dois totais e o próprio M52, e uma que falha se a frase voltar.

**Segue aberto, e é o corpo do achado:** unificar o esquema pede um parâmetro `geo` no `sus_data_aggregate`, cujas formals hoje espelham o R (que não tem `geo`). Mudança de API pública — §8, decisão do coordenador.

## 2026-09-15 — o R **não lê** os `categories.json`: as 39 órfãs fechadas contra o dicionário dele  ·  **M7** (fechado)

A pergunta que destravou tudo: *quem é a autoridade de paridade?* Eu vinha tratando o `categories.json` como a fonte, e por isso as órfãs pareciam exigir decisão de conteúdo.

O R **não usa esses arquivos**. Ele tem dicionários embutidos no pacote — `get_translation_dict_<lang>[_<sistema>]()$values` — e eles são indexados pelo **campo de origem do DATASUS**:

```r
get_translation_dict_es()$values[["LOCOCOR"]]
#  1 Hospital | 2 Otro establecimiento de salud | 3 Domicilio | 4 Via publica | ...
```

Isso explica a classe de defeito inteira. **No R ela não pode existir:** não há rename entre a chave e a busca. O porte reindexou por *nome traduzido de coluna*, e é dessa reindexação que nascem as órfãs — uma chave cujo nome nenhuma coluna assume depois do rename nunca é consultada, e o código cru (`9`, `4`, `3`) chega ao usuário sem erro e sem aviso.

### Como cada órfã foi roteada

Duas provas exatas, nunca semelhança de nome:

- **P1, pivô de língua** — a órfã é nome de coluna válido em outra língua; o campo de origem comum dá o nome certo nesta. Foi assim que `causa_modificada` → `alteracao_causa` (via `ALTCAUSA`).
- **P2, pivô de rótulo** — os rótulos da órfã, desacentuados, batem com os do R para um campo específico. `local_obito` casou 7 de 7 rótulos com `LOCOCOR`, cujo destino é `local_ocorrencia_obito`.

> **Casar por conjunto de códigos não funciona, e eu tentei.** Quase todo dicionário usa `{1, 2, 9}` — então tudo casa com tudo. A heurística chegou a propor `birth_location` → `escolaridad`. O **texto** dos rótulos serve de impressão digital; os códigos não.

O P2 também corrigiu um nome enganoso: `momento_obito_puerperio` tem conteúdo `{Antes, Durante, Após, Ignorado}`, que é o livro do `MORTEPARTO`, não de puerpério — e idêntico ao de `momento_obito_parto`. Era duplicata sob nome errado.

### Resultado

| ação | quantas | exemplo |
|---|---|---|
| renomeadas | 14 | `escolaridade_agregada_falecido` → `escolaridade_falecido_agregada` (`ESCFALAGR1`) |
| fundidas em chave viva | 11 | `congenital_anomaly` → `anomalia_congenita`, trazendo o `Sí` acentuado |
| chaves novas do R | 20 | `mother_education_level`, `tumor_staging`, `radiotherapy_purpose` |
| códigos novos do R | 43 | `education_level` ganhou `0`, `5`–`8`, `10` |
| mantidas | 14 | fantasmas do próprio R |

**Medido** — SIM-DO/SP/2023, 334.303 óbitos, o mesmo `sus_data_standardize` contra o dicionário antes e depois:

| língua | células que mudaram | colunas |
|---|---|---|
| `pt` | **1.674.124** | 13 |
| `es` | 756.185 | 8 |
| `en` | 4.824 | 1 |

O maior ganho isolado é `local_ocorrencia_obito`: 334.303 linhas saindo de `'1'` para `Hospital` — o conjunto inteiro.

### Uma regressão minha

`ESCMAE` vira `mother_education` no **SIM** e `mother_education_level` no **SINASC**. O commit `aff7b9d` renomeou uma na outra: consertou o SIM e **emudeceu o SINASC**. As duas chaves precisam existir. Restaurada.

### As 14 que ficam são fantasmas do R

`TERCEIRO`, `YES_NO_FLAG`, `UNIDADE_IDADE`, `SI_NO_FLAG`, `SIM_NAO_FLAG`, `GESTOR_TP`, `VINCPREV` — o dicionário embutido do R tem entradas sob nomes que **não são campos do DATASUS**, inalcançáveis lá também. O P2 prova isso: casam 100% com um pseudo-campo cujo conjunto de destinos é vazio. Replicadas de propósito, e **listadas no teste** para que uma órfã *nova* apareça como falha.

### O guarda que faltava

`TestCategoryKeysReachable` em `tests/test_metadata_external.py`: toda chave de categoria tem de ser nome de coluna, a lista de fantasmas não pode envelhecer, e `ESCMAE` tem de traduzir nos dois sistemas. **Verificado que falha** — reintroduzi `local_obito` de propósito e ele acusou pelo nome.

Commits `3287c93` e `2ac4f1c` no `climasus-data`, ramo `andrey`.

## 2026-09-15 — dois erros de conteúdo achados no caminho  ·  **M100**, **M101**

**`ESC2010` com o livro de códigos trocado** no `pt-pt` e no `pt-es`. O DATASUS é `1 Fundamental I, 2 Fundamental II, 3 Médio, 4 Superior incompleto, 5 Superior completo`. O arquivo trazia uma escada *incompleto/completo* deslocada em um grau:

| código | o arquivo dizia | o que é |
|---|---|---|
| `3` | Fundamental II incompleto (5ª a 8ª série) | **Médio** |
| `4` | Fundamental II completo (8ª série) | **Superior incompleto** |
| `5` | Médio incompleto | **Superior completo** |

Não é redação, é outro livro: escolaridade **superior incompleta** era publicada como **fundamental II completo** — erra para baixo em dois níveis de ensino, e nada no dado denuncia. **259.617 de 334.303 registros** afetados em um único estado-ano. O `pt-en` já estava certo, que é a evidência mais limpa de que o erro era do dado.

**`CS_SEXO` código `I`**: os três dicionários diziam *Indeterminado*; no DATASUS é **Ignorado**. Conceitos distintos — um afirma que o sexo foi apurado e não se enquadrou, o outro diz apenas que não se sabe. Quem filtra `Ignorado` para descartar registro sem informação deixava esses passar.

Nos dois casos o rótulo de substituição veio **do próprio R**, não redigido por mim: a correção é sobre *qual código significa o quê*, não sobre a redação.

## 2026-09-15 — o R é ASCII puro, e aqui o Python fica melhor  ·  **M102**

Dump dos 33 getters, verificado byte a byte: **3.059 células de tradução, zero com caractere acentuado**, e nenhum escape `\uXXXX` escondendo acento. O R publica `Indigena`, `Via publica`, `Domicilio`, `Si`, `Cesarea`, `Nao`.

Isso fazia **108 células** parecerem divergência de paridade na comparação crua. Desacentuando os dois lados:

- **53 eram só acento** (25 no `pt-es`, 28 no `pt-pt`) — o Python está certo e o R errado. Ficam como estão.
- **55 divergem em palavra**, e quase todas são paráfrase inócua: `Outro`/`Outros`, `Após`/`Depois`, `Tripla ou mais`/`Tripla e mais`, `Codificador`/`Codificadora`.
- **2 eram erro de verdade** — viraram M100 e M101.

**Pendência que o M7 criou:** os rótulos trazidos do R entraram sem acento. Restaurei acento só onde era mecanicamente derivável do vocabulário do próprio arquivo — 7 rótulos, via casamento exato da forma desacentuada. O resto ficou como o R escreveu, em vez de eu redigir. Vale uma passagem de acentuação aprovada por quem fala a língua.

> A primeira versão dessa regra caiu no próprio rabo: eu construí o vocabulário do arquivo **já corrigido**, então os rótulos ASCII que eu tinha acabado de inserir "provavam" que a forma sem acento era legítima. Zero trocas. Passei a montar o vocabulário da versão anterior ao patch.

Achado menor do mesmo lote: `get_translation_dict_pt_sia` traduz `PA_SEXO` `'F'` como **`Femenino`** — espanhol dentro do dicionário português.

## 2026-09-15 — o R tem dois livros de raça/cor contraditórios  ·  **M103**

Para o **mesmo** campo de dois dígitos:

| dicionário do R | `03` | `04` |
|---|---|---|
| `PA_RACACOR@sia`, `AP_RACACOR@sia_{ad,am,aq,ar}`, `RACACOR@sia_ps` | Parda | Amarela |
| `RACA_COR@sih` | **Amarela** | **Parda** |

Os mesmos dois códigos, trocados — não é redação, aponta para categoria diferente. Confirmado nas três línguas. No R não colide porque ele escolhe o dicionário por sistema; o efeito é que **um dos dois sistemas rotula raça errado**.

O livro de **um** dígito (`RACACOR` no SIM e SINASC: `3` Amarela, `4` Parda) é outro e não está em dúvida — o DATASUS realmente usa ordens diferentes em campo de um e de dois dígitos, o que explica como a troca passou despercebida.

No Python os dois livros já conviviam desde antes, em chaves separadas (`race_color` com o do SIH, `patient_race_color` com o do SIA). **O `pt-pt` é o único onde colidem de fato**, porque seu `columns.json` manda `RACA_COR` e `PA_RACACOR` para a mesma coluna `raca`.

### O que eu quase publiquei

O patch do M7 preencheu `03` e `04` em `pt-pt/raca` **escolhendo por ordem de iteração de dicionário**. Rastreei: `PA_RACACOR@sia` escreveu primeiro e o `RACA_COR@sih`, que queria o inverso, perdeu para o `setdefault`.

Auditei as **219 células** que o patch acrescentou contra a lista de células onde o R se contradiz em *sentido* (não em grafia). Foram exatamente **2** — essas duas. Revertidas: `03` e `04` voltaram a passar sem tradução, como antes, sem regressão e sem rótulo errado publicado. `01`, `02`, `05` e `99` ficaram, porque todos os dicionários do R concordam neles.

**Precisa decisão em duas frentes:** qual livro está certo (para o R), e se o `columns.json` do `pt-pt` deve separar `RACA_COR` de `PA_RACACOR` como as outras duas línguas fazem — isso resolveria de vez, mas renomeia coluna de saída (§5/§8).

## 2026-09-15 — o `categories.json` é plano e o R não é  ·  **M104**

O R mantém um dicionário **por sistema**. O `categories.json` tem **uma** entrada por nome de coluna traduzido. Quando dois sistemas alimentam o mesmo nome e discordam, o arquivo plano só cabe um rótulo.

Levantei 16 células de sentido divergente no `pt-en`, mais as de `es` e `pt`. A maioria é paráfrase, mas duas classes são livro de códigos de verdade:

1. **`education_level`** recebe `INSTRU` do SIH (`1` Analfabeto, `2` Fundamental, `3` Médio, `4` Superior) **e** `CS_ESCOL_N` do SINAN (`1` a `4` = séries do fundamental). Os códigos `1`–`4` significam coisas diferentes nos dois sistemas e a chave guarda o do SIH — então **dado de SINAN sai rotulado com a escala do SIH**. Já era assim antes deste trabalho.
2. Raça/cor de dois dígitos, que é o M103.

O detalhe que faz disso um conserto viável: **`sus_data_standardize` já recebe e detecta `system`**, e já usa a seção por sistema do `columns.json` para renomear. A dimensão existe no rename e falta na tradução de valor — o conserto é dar ao `categories.json` a mesma estrutura por seção que o `columns.json` tem. É mudança de formato do `climasus-data`, então é do coordenador.

Achado lateral: o `columns.json` do `pt-es` e do `pt-pt` **não tem as seções `SIA-AD`, `SIA-AM`, `SIA-AQ`, `SIA-AR` e `SIA-PS`** que o `pt-en` tem, então 29 a 31 campos que o R traduz nessas línguas não têm destino nenhum — entre eles `AP_RACACOR`, `AQ_ESTADI` e `AR_FINALI`. É por isso que o `pt-en` ganhou 15 chaves novas no M7 e o `pt-es` apenas 4.

## 2026-09-15 — decisão do M90: replicar o R e anotar  ·  **M90**

Decidido pelo Andrey: **manter igual ao R**. O Python já replicava; a decisão fecha a dúvida e evita que alguém "conserte" o consumidor sem mexer no produtor.

Eu havia proposto fazer a função **recusar** em vez de emitir rótulo `"0001"`. Não é o caminho: casar com o R é o contrato, e o defeito é do produtor — `sus_mod_spacetime_bayes`, que em Python ainda é stub, então nem existe aqui para consertar.

Registrado no código: seção própria no docstring público de `sus_mod_spacetime_exceedance`, com a medição (336 células entram, 336 saem) e com a saída pela qual o usuário escapa — passar `time_idx` com anos reais, que agrega normalmente. O caminho quebrado e o que funciona estão os dois fixados em teste.

**Segue aberto no R:** o ajustador precisa preservar o rótulo temporal original em vez do índice de base 1. Um parâmetro `time_labels` resolveria melhor, mas amplia API pública (§5/§8).

## 2026-09-16 — o `verify_physics` **edita** o dado, e eu havia registrado o contrário  ·  **M12**

Ontem escrevi no M28 que o `.verify_solar_radiation` do R *"apenas avisa quando um valor passa de 110% dela — nunca edita o dado"*. A primeira linha do corpo dele desmente:

```r
df_work[["sr_kj_m2"]] <- ifelse(!is.na(sr) & sr < 0, 0, sr)
```

**Grampeia radiação negativa em zero.** A parte do aviso estava certa; o "nunca edita" não. E é exatamente ali que morava a divergência que eu havia classificado como *latente* no M28 — o R manda negativo para `0`, o Python mandava para `NULL` pelo filtro de faixa física. Agora o parâmetro existe e o caminho padrão bate.

Medido contra o R sobre 4.000 linhas com 39 negativos, 25 nulos e 30 leituras impossíveis de 9000 kJ/m²: **grampeamento idêntico** (0 negativos, 39 zeros, 25 nulos nos dois lados) e **contagem de aviso idêntica em 2.347**.

### Duas armadilhas no caminho

**Fuso.** A primeira comparação deu R=2.339 contra Python=2.347, e eu quase registrei divergência. Não era o porte: o `arrow::read_parquet` do R devolve POSIXct com `tzone=NULL`, que **localiza** para `America/Sao_Paulo`, enquanto o DuckDB mantém o timestamp ingênuo. A primeira linha virava `2019-12-31 21:00` no R e `2020-01-01 00:00` no Python — três horas de deslocamento no ângulo horário. Com `attr(tzone) <- "UTC"` no lado R, os números coincidem.

> Fica a regra: **comparação R/Python sobre timestamp exige fixar o fuso**, senão o R localiza e o DuckDB não.

**`GREATEST`.** Terceira vez que essa aparece neste módulo. O `pmax(sin_elev, 0)` do R propaga `NA`; o `GREATEST` do DuckDB **ignora** `NULL`. Com latitude nula ele devolveria `0`, poria `G0` em zero e faria **qualquer** leitura positiva contar como excedência. Usei `CASE` explícito, com teste exigindo zero excedência nesse caso.

## 2026-09-16 — o RNG do R, bit a bit  ·  **M12**

O `compute_uncertainty` parecia ter um limite de paridade intransponível: o `.compute_mc_uncertainty` roda sob `set.seed(2024L)` fixo, então os intervalos do R são **determinísticos** — e reproduzi-los exige reproduzir o fluxo do R. O NumPy não serve: semeia o MT19937 de outro jeito e converte uniforme em normal por outro método. `RandomState(2024)` dá uma sequência sem relação nenhuma.

Reimplementei em `climasus4py/utils/r_random.py`. Duas peças, as duas tiradas do fonte C do R:

- **`qnorm` por AS 241 de Wichura** (`src/nmath/qnorm.c`). Necessário porque a normal do R sai de **inverter** a CDF, então qualquer diferença na inversa aparece em *todo* sorteio. Medido contra o R em 35 probabilidades de `1e-10` a `1-1e-10`: **AS 241 acertou as 35 exatamente**, e o `ndtri` do scipy (Cephes) errou por até `1,8e-15`.
- **Mersenne-Twister com o embaralhamento do `set.seed`** e o `norm_rand` por inversão (`src/main/RNG.c`) — inclusive os **50 passos** que antecedem o preenchimento do estado, sem os quais nada bate, e o `fixup` que impede `0` e `1` de saírem.

**Verificado contra o R 4.6.0: 4.200 valores em quatro combinações de tipo e semente, todos idênticos bit a bit.** Referência congelada em `tests/fixtures/r_random/referencia_r.parquet`.

### O defeito que só apareceu depois da palavra 622

Vetorizei o *twist* do MT lendo tudo do estado anterior. Mas o laço do C é **auto-referente**: o feed `mt[i-227]`, para `i >= 454`, foi reescrito pelo próprio laço.

O sintoma foi preciso e traiçoeiro:

| posição | resultado |
|---|---|
| 1 … 622 | certas |
| **623** | **errada** |
| 624 … 1246 | certas |
| 1247 em diante | **erradas** |

Se eu tivesse conferido só os 8 primeiros valores — que era o meu teste inicial — teria dado o módulo por pronto. Foi o teste que **atravessa a fronteira do bloco** que pegou.

Corrigido processando em três blocos de 227, onde cada bloco lê apenas palavras já finalizadas por um bloco anterior, sem abrir mão da vetorização. Há teste nas posições exatas 623, 624, 625, 1247, 1248, 1873 e 2000.

**Desempenho:** `rnorm` de 4.000 em 5,1 ms; as 200 simulações × 3 variáveis × 4.000 linhas em 2,7 s. O gargalo do `compute_uncertainty` vai ser avaliar o indicador 200 vezes, não o sorteio.

### O que falta, e a decisão de projeto embutida

O laço de Monte Carlo em si: perturbar `tair`, `rh` (limitado a 0–100), `ws` (piso 0) e `sr` (multiplicativo, piso 0) **na ordem em que o R chama `rnorm`**, porque a ordem consome o fluxo; despachar o indicador com `apply_mask=FALSE`; e tirar os quantis 2,5% e 97,5% por linha (tipo 7, que é o método linear padrão do NumPy) arredondados a 2 casas.

Os indicadores do Python são SQL do DuckDB, então as 200 avaliações serão **200 passagens de SQL** (fiel, mas 200 idas ao banco por indicador) ou uma **reimplementação das fórmulas em NumPy** (rápida, mas duplica fórmula e abre espaço para as duas versões divergirem). Prefiro as 200 passagens, justamente para não ter duas verdades.

## 2026-09-17 — as 13 colunas de estação que faltavam, e o join que **dobra linhas**  ·  **M106**, **M107**

O `sus_climate_compute_indicators` do R termina com uma linha que eu havia subestimado:

```r
station_meta <- get_spatial_station_cache(...) %>% sf::st_drop_geometry()
result <- dplyr::left_join(result, station_meta, by = c("station_code"))
```

São **13 das 60 colunas** que ele devolve por padrão: identidade da estação mais cinco classificações climáticas. Eu havia anotado que `use_cache`/`cache_dir` *"pertencem a outras partes da função"* — verdade quanto à detecção de região, mas eles alimentam justamente esse join. O join é **incondicional**; eles só decidem se o download fica em cache.

Portei a tabela para o `climasus-data` (`assets/climate/inmet_station_meta.parquet`), o que resolve o M106 e torna os dois parâmetros **sem objeto** em Python: não há download a guardar. Conferido contra o R para a A701 (SÃO PAULO - MIRANTE): os 13 valores idênticos, acentos inclusos.

### O que eu encontrei ao olhar a tabela

**636 linhas para 609 estações.** 26 códigos aparecem duas vezes — e **nenhum dos 26 pares é duplicata de verdade**:

| coluna | em quantos dos 26 conflita |
|---|---|
| `id_link`, `zona_climatica` | **26** |
| `tipo_umidade`, `distr_umidade`, `temperatura_id`, `descricao` | 8 |
| identidade (região, UF, nome, lat, lon, altitude, fundação) | **0** |

A forma da discordância diz o que aconteceu: os pares se dividem entre uma classe de terra e `Massa d'água` ou `Zona Econômica Exclusiva`. E as **18** estações cujas *duas* candidatas são água ficam no litoral ou no mar — SALINÓPOLIS (PA), JOÃO PESSOA (PB), CALCANHAR (RN), e ARQ. SÃO PEDRO E SÃO PAULO, que está de fato no meio do Atlântico. São pontos caindo na fronteira dos polígonos climáticos, então o join espacial que gerou a tabela emitiu uma linha por polígono.

### E aí está o defeito, medido no R

```
A701 (código único)      100 linhas -> 100
A134 (código repetido)   100 linhas -> 200   <<<
A215                     100 linhas -> 200   <<<
A229                     100 linhas -> 200   <<<
A302                     100 linhas -> 200   <<<
```

Uma estação com um ano de leitura horária vai de 8.760 para 17.520 linhas, e **toda contagem, média ou soma sobre essas 26 estações sai dobrada**. Em silêncio: o dplyr não avisa, ninguém confere o número de linhas, e as duas cópias só diferem em colunas de classificação que quem analisa temperatura nem olha.

**Não replico.** Dobrar linha de observação não é "informação em forma ruim" — as linhas extra são fabricação, e cai no §3. A tabela vai para o `climasus-data` colapsada por uma regra sem juízo meu nenhum: **onde a fonte concorda, o valor fica; onde ela se contradiz, fica nulo.** Identidade preservada nas 609; as 26 perdem só o que a própria fonte não soube resolver. Arbitrar um lado seria afirmar que uma estação de terra está dentro de um corpo de água — ou, no caso do arquipélago, que ela não está.

### Dois defeitos meus no caminho

**A ordem das linhas.** O hash join do DuckDB não preserva ordem; o `left_join` do dplyr preserva a do lado esquerdo. Uma entrada de 4 linhas voltou como `[1200, 0, 500, 0]` em vez de `[0, 500, 0, 1200]`. Para série horária isso é pior que coluna faltando. Corrigido carregando um `row_number` através do join e ordenando por ele — com teste sobre 50 linhas, porque com 4 eu poderia ter tido sorte.

**Colisão de nome.** No R o `result` só tem id e indicadores nesse ponto, então as 13 chegam limpas. Aqui as colunas da entrada sobrevivem (M105), e o `sus_climate_inmet` já emite `region`, `station_name`, `latitude`, `longitude` e `altitude`. Preservo as cinco — o cabeçalho do próprio arquivo do INMET é no mínimo tão autoritativo quanto a tabela — e acrescento só as 8 ausentes.

### O M99 se pagando

O manifest pegou o arquivo novo **sozinho**: 45 arquivos, 1 acrescentado, 0 checksums obsoletos. Antes do conserto de ontem ele teria ignorado o `.parquet` novo e estampado a data como se tivesse conferido.

## 2026-09-17 — uma falha intermitente que eu não consegui reproduzir  ·  **M108**

Registro porque não quero que vire ruído aceito.

Duas vezes em dois dias, em **arquivos diferentes**, um `TestRenderizacao::test_desenha_sem_erro` falhou na suíte completa: em 16/09 o `test_mod_plot_spatial_bayes.py[rr]`, em 17/09 o `test_mod_plot_spacetime.py[rr_map]`. Nos dois casos passa isolado e passa na rodada seguinte.

Tentei reproduzir de propósito: rodar só os 168 testes com `plot` no nome passa; a suíte completa falhou em 2 de 4 tentativas. **Nas duas vezes que instrumentei para capturar o traceback, a rodada passou.** Então não tenho o traceback.

Hipótese, não diagnóstico: não existe `conftest.py` no projeto e não há nenhuma chamada a `plt.close()` em teste nenhum, então as figuras do matplotlib acumulam pela sessão inteira. É causa conhecida de falha intermitente quando muitos testes de renderização rodam juntos — mas falta a prova.

Por que não ignorar: **"1283 passed" deixa de significar o que deveria** se uma falha em 2 de 4 rodadas for tratada como ruído.
