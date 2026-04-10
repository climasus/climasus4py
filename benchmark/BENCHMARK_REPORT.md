# Benchmark: climasus4py vs climasus4r

**Data**: 07/04/2026 | **Ambiente**: Python 3.11.9 / R 4.x | DuckDB 1.5.1 | Windows 10 | 8 cores

## Dados de Teste

| Cenario | Sistema | UF | Anos | Linhas | Parquet |
|---------|---------|----|----- |--------|---------|
| Pequeno | SIM-DO | SP | 2023 | 334.303 | 15.8 MB |
| Medio | SIM-DO | SP | 2021-23 | 1.119.975 | 52.3 MB |
| Grande | SIM-DO | SP | 2018-23 | 2.074.113 | 96.4 MB |

Pipeline: import → clean → standardize → filter(cardiovascular I10-I99) → aggregate(month) → export(parquet)

---

## Resultado Principal: Tempo Total do Pipeline

| Versao | Pequeno | Medio | Grande |
|--------|---------|-------|--------|
| **R v0.0.1** | 7.18s | 21.83s | 38.97s |
| **R v0.0.2** | 8.01s | 25.25s | 47.15s |
| **R rc_a (DuckDB)** | 0.37s | 0.43s | 0.44s |
| **Python pipeline** | **0.36s** | **0.64s** | **0.76s** |
| **Python raw SQL** | **0.16s** | **0.27s** | **0.28s** |

### Speedup vs R v0.0.1

| Versao | Pequeno | Medio | Grande |
|--------|---------|-------|--------|
| R v0.0.1 | 1.0x | 1.0x | 1.0x |
| R v0.0.2 | 0.9x | 0.9x | 0.8x |
| R rc_a | **19.4x** | **50.8x** | **88.6x** |
| Python pipeline | **19.9x** | **34.3x** | **51.3x** |
| Python raw SQL | **44.9x** | **80.9x** | **140.1x** |

---

## Detalhamento por Etapa (Python)

### Cenario Pequeno (334K rows)

| Etapa | Tempo | % |
|-------|-------|---|
| import | 3.5ms | 0.9% |
| clean | 4.7ms | 1.2% |
| standardize | 18.2ms | 4.7% |
| filter | 6.9ms | 1.8% |
| aggregate | 10.3ms | 2.7% |
| **export** | **341ms** | **88.7%** |
| **TOTAL** | **385ms** | 100% |

### Cenario Grande (2M rows)

| Etapa | Tempo | % |
|-------|-------|---|
| import | 6.7ms | 0.9% |
| clean | 8.2ms | 1.1% |
| standardize | 22.0ms | 2.9% |
| filter | 9.5ms | 1.3% |
| aggregate | 13.3ms | 1.8% |
| **export** | **688ms** | **92.0%** |
| **TOTAL** | **748ms** | 100% |

> **Achado**: O export (write_parquet) domina ~90% do tempo. Import→aggregate inteiro leva <60ms mesmo com 2M linhas, graças a evaluacao lazy do DuckDB.

---

## Memoria

| Versao | Pequeno | Grande |
|--------|---------|--------|
| R v0.0.1 | 572.6 MB | 2.555 MB |
| R rc_a | 21.4 MB | N/A |
| **Python pipeline** | **0.1 MB** | **0.03 MB** |

> Python usa ~5.700x menos memoria que R v0.0.1 (avaliacao 100% lazy, zero materialização).

### Custo de Materialização (se necessario)

| Destino | Pequeno (334K) | Medio (1.1M) | Grande (2M) |
|---------|----------------|--------------|-------------|
| → pandas | 5.9s / 901 MB | 19.4s / 3.016 MB | 42.6s / 5.624 MB |
| → Arrow | 27ms / ~0 MB | 41ms / ~0 MB | 272ms / ~0 MB |

> Arrow é ~200x mais rapido que pandas para materialização. Usar `.arrow()` quando precisar materializar.

---

## Escalabilidade

| Metrica | Pequeno→Grande (6.2x dados) |
|---------|------------------------------|
| R v0.0.1 | 7.18s → 38.97s (5.4x) |
| R rc_a | 0.37s → 0.44s (1.2x) |
| Python pipeline | 0.36s → 0.76s (2.1x) |
| Python raw SQL | 0.16s → 0.28s (1.8x) |

> R rc_a escala melhor (fast path unico SQL). Python pipeline tem overhead do orchestrator em Python, mas ainda sub-linear.

---

## Arquitetura Comparada

```
R v0.0.1/v0.0.2 (materializa a cada etapa):
  parquet → DataFrame → clean → DataFrame → standardize → DataFrame → ...
  Resultado: ~573 MB pico, 7-47s

R rc_a (DuckDB lazy via duckplyr):
  parquet → duckplyr_df → lazy chain → materializa no final
  Resultado: ~21 MB pico, 0.37-0.44s

Python (DuckDB nativo):
  parquet → DuckDBPyRelation → lazy chain → write_parquet direto
  Resultado: ~0.1 MB pico, 0.36-0.76s

Python raw SQL:
  parquet → SQL unico → resultado
  Resultado: ~0 MB, 0.16-0.28s
```

---

## Conclusoes

1. **Python pipeline e R rc_a tem performance similar** no cenario pequeno (~0.37s ambos)
2. **R rc_a e ligeiramente mais rapido** nos cenarios maiores (0.44s vs 0.76s) devido ao fast path SQL unico sem overhead Python
3. **Python raw SQL e 2-3x mais rapido** que qualquer outra abordagem — e o piso teorico
4. **Ambos sao 20-90x mais rapidos** que R v0.0.1/v0.0.2 (que materializam a cada etapa)
5. **Python usa virtualmente zero memoria** (~0.1 MB vs 573 MB no R v0.0.1)
6. **O gargalo do Python e o export** (~90% do tempo) — import→aggregate leva <60ms
7. **Materialização para pandas e cara** — preferir Arrow ou manter lazy
