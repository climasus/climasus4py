"""Testes do duckdb_settings — o escopo dos ajustes da conexão compartilhada.

Contexto (M44): a conexão de ``get_connection()`` é uma singleton do processo.
``_process_year`` do INMET aplicava ``SET memory_limit='96MB'`` e ``SET
threads=1`` nela e nunca desfazia, então uma chamada a ``sus_climate_inmet()``
deixava a sessão inteira em 91,5 MiB e uma thread — e funções sem relação
nenhuma passavam a falhar em poucos milhares de linhas.
"""

from __future__ import annotations

import duckdb
import pytest

from climasus4py.core.engine import duckdb_settings, get_connection

# Ajustes que o pacote mexe de fato, nos dois pontos que existiam.
AJUSTES = [
    "memory_limit",
    "threads",
    "preserve_insertion_order",
    "enable_external_file_cache",
    "allocator_flush_threshold",
]

# Consulta que não cabe em 96 MB mas cabe no orçamento cheio. Serve para medir
# o limite REALMENTE aplicado, que é diferente do valor relatado por
# current_setting -- ver test_restores_the_enforced_limit_not_just_the_reading.
PESADA = (
    "SELECT count(*) FROM ("
    "  SELECT i, repeat(md5(i::VARCHAR), 40) AS s FROM range(400000) t(i)"
    "  ORDER BY s"
    ")"
)


def _ler(conn, chaves=AJUSTES) -> dict[str, object]:
    return {
        k: conn.sql(f"SELECT current_setting('{k}')").fetchone()[0] for k in chaves
    }


def test_applies_inside_the_block():
    conn = get_connection()
    with duckdb_settings(memory_limit="96MB", threads=1):
        dentro = _ler(conn, ["memory_limit", "threads"])
    assert dentro["memory_limit"] == "91.5 MiB"   # 96 MB base 10
    assert dentro["threads"] == 1


def test_restores_every_setting_on_exit():
    conn = get_connection()
    antes = _ler(conn)
    with duckdb_settings(
        memory_limit="96MB",
        threads=1,
        preserve_insertion_order=False,
        enable_external_file_cache=False,
        allocator_flush_threshold="1MB",
    ):
        pass
    depois = _ler(conn)

    # memory_limit perde precisao no round-trip de texto (6.2 GiB volta como
    # 6.1 GiB); e limitado e converge, entao aqui exijo igualdade exata apenas
    # nos ajustes que nao sao tamanho.
    for k in ("threads", "preserve_insertion_order", "enable_external_file_cache"):
        assert depois[k] == antes[k], f"{k} nao voltou: {antes[k]} -> {depois[k]}"
    assert depois["allocator_flush_threshold"] == antes["allocator_flush_threshold"]


def test_restores_after_exception():
    """O bloco tem de restaurar mesmo quando o corpo levanta."""
    conn = get_connection()
    antes = _ler(conn, ["threads", "preserve_insertion_order"])
    with pytest.raises(RuntimeError):
        with duckdb_settings(threads=1, preserve_insertion_order=False):
            raise RuntimeError("falha simulada")
    assert _ler(conn, ["threads", "preserve_insertion_order"]) == antes


def test_restores_the_enforced_limit_not_just_the_reading():
    """Regressao do jeito errado de restaurar.

    No DuckDB 1.5.3, ``RESET memory_limit`` devolve o valor RELATADO ao default
    mas NAO desfaz o limite aplicado: uma consulta depois disso ainda falha com
    ``(91.5 MiB/91.5 MiB used)`` embora current_setting diga ``6.2 GiB``. Por
    isso duckdb_settings restaura com SET explicito. Este teste falha se
    alguem trocar de volta para RESET, o que nenhuma leitura de
    current_setting detectaria.
    """
    conn = get_connection()

    conn.sql(PESADA).fetchone()          # cabe no orcamento cheio

    with duckdb_settings(memory_limit="96MB", threads=1):
        with pytest.raises(duckdb.Error):
            conn.sql(PESADA).fetchone()  # nao cabe em 91,5 MiB

    # e o que importa: volta a caber depois do bloco
    conn.sql(PESADA).fetchone()


def test_unknown_setting_is_ignored_not_fatal():
    """Ajuste inexistente nesta build nao deve derrubar o bloco."""
    conn = get_connection()
    antes = _ler(conn, ["threads"])
    with duckdb_settings(threads=1, nao_existe_esse_ajuste="x"):
        pass
    assert _ler(conn, ["threads"]) == antes


def test_process_year_does_not_leak_its_budget():
    """M44: o orcamento apertado do INMET nao pode escapar da funcao.

    Nao baixa nada -- so confere que _process_year devolve a conexao ao estado
    anterior. Passa por um cache_dir vazio, entao a funcao sai cedo; o ponto e
    que os SET do topo dela ja aconteceram antes disso.
    """
    from climasus4py.core import climate_inmet as mod

    conn = get_connection()
    antes = _ler(conn)
    try:
        mod._process_year(
            year=1900,                     # ano sem dado: sai cedo
            uf=["SP"],
            cache_dir=mod.Path("dados/cache/_nao_existe_"),
            use_cache=True,
            parallel=False,
            workers=1,
            verbose=False,
        )
    except Exception:
        pass                               # falhar e aceitavel; vazar nao e

    depois = _ler(conn)
    for k in ("threads", "preserve_insertion_order", "enable_external_file_cache"):
        assert depois[k] == antes[k], (
            f"_process_year vazou {k}: {antes[k]} -> {depois[k]}"
        )
