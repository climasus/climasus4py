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

## 2026-09-09 — o R devolve NA de idade quando `DTNASC` é nulo, mesmo com `IDADE` válida

- **Onde:** `climasus4r::sus_data_create_variables()` (coluna de idade) — lado R
- **O quê:** medido no SIM-DO SP 2023 durante a correção do M6. As divergências de `age_years` entre os dois lados eram **471**, e a conta fecha exatamente em duas direções opostas: **335** registros onde o Python errava (o sentinela `IDADE='999'`, "idade desconhecida", virava 999 anos — corrigido em 09/09) e **136** registros onde o **R** erra: `DTNASC` nulo e `IDADE` trazendo valor válido codificado (ex. `IDADE='454'` = 54 anos). O Python decodifica e acerta; o R devolve `NA`, descartando idade que está no dado. Ou seja, depois da nossa correção a divergência residual esperada contra o R é de 136 linhas, **todas a favor do Python**.
- **Por que não agora:** é bug do R, e pelo princípio de paridade do `CLAUDE.md` bugs do R são apontados e não replicados nem corrigidos daqui. Vale levar ao coordenador porque afeta o `climasus4r` publicado: 136 em 334.303 é 0,04%, mas são idades que existem no dado e estão sendo perdidas. Registrado também no M6.

## 2026-09-09 — `dlnm::crosspred(at = x)` ordena `x`, e o `.saf_component` do R pareia posicionalmente

- **Onde:** `climasus4r::sus_mod_af()` → `.saf_component()` — lado R. Propaga para `sus_mod_excess()` e `sus_mod_swot()`.
- **O quê:** o `sus_mod_af` faz `pred_obs <- dlnm::crosspred(cb, model, at = x, cen = cen)` e usa `rr_obs <- as.numeric(pred_obs$allRRfit)`. Mas o `crosspred` devolve `allRRfit` na ordem **crescente** de `x`, não na ordem em que `x` foi passado — verificado: `identical(predvar, sort(predvar))` é `TRUE` enquanto `identical(x, sort(x))` é `FALSE`. O `.saf_component` então pareia esse vetor **posicionalmente** com `cases` e com `in_range`, que estão em ordem de **data**. Como os valores de `x` são contínuos e todos distintos, os comprimentos coincidem (1812 = 1812) e **o R não emite aviso nenhum**: cada dia recebe silenciosamente o RR de outro dia.
- **Prova:** com a mesma série sintética de 5 anos nos dois lados, o Python (pareamento correto) dá af total 0,056669 / heat −0,007568 / cold 0,064237 e o R dá 0,054892 / 0,032898 / 0,021994. Embaralhando o RR do Python do mesmo jeito — `rr[argsort(x)]` — reproduzem-se os números do R **até a sexta casa decimal**. Isso identifica a causa sem margem: é o desalinhamento, não diferença de método, de base spline (as curvas de RR são idênticas até a 7ª casa em toda a faixa), de centragem (`cen` idêntico) nem de entrada (`nrow`, `n_cases`, `x` e a contagem de dias de cada lado do `cen` todos idênticos).
- **Por que importa mais que o normal:** a fração atribuível é o número publicável desta biblioteca. O total sai próximo do correto (~3%) porque os óbitos diários são homogêneos, o que faz a soma embaralhada ficar perto da certa — mas a **decomposição calor/frio fica arbitrária**, e foi por isso que o achado original (M24) via a ordem heat/cold invertida e desvios de até +306% por faixa de percentil. Um relatório de carga de calor construído sobre isso atribui ao frio o que é do calor.
- **Correção sugerida ao R:** `crosspred` aceita `at` e devolve `predvar`; basta reordenar por `match(x, pred_obs$predvar)` antes de usar, ou computar o RR por dia sem passar pelo grid ordenado. Vale também um `stopifnot(identical(as.numeric(pred_obs$predvar), as.numeric(x)))` como guarda.
- **Por que não agora:** é bug do R, e pelo princípio de paridade do `CLAUDE.md` bugs do R são apontados e não replicados nem corrigidos daqui. O lado Python está correto e tem `tests/test_af_pairing.py` impedindo que alguém o alinhe ao comportamento defeituoso numa comparação futura. **Levar ao coordenador com prioridade**: afeta o `climasus4r` publicado, no número que vai para publicação.
