"""Testes de sus_as_arrow / sus_as_duckdb — a travessia da metadata.

Estas duas existem por um motivo so: fazer o ``sus_meta`` sobreviver a troca de
formato. ``collect_arrow()`` entrega o dado e descarta a procedencia; estas
entregam os dois. Os testes centrais sao os de round-trip, porque o valor delas
esta em o leitor existente conseguir ler de volta o que elas escreveram.
"""

from __future__ import annotations

import json

import duckdb
import pandas as pd
import pyarrow.parquet as pq
import pytest

import climasus4py as cs
from climasus4py.core._stage import set_stage
from climasus4py.core.engine import get_connection
from climasus4py.core.meta import META_SCHEMA_KEY, META_TABLE_SUFFIX


@pytest.fixture
def rel_com_meta():
    """Relacao pequena com stage e historico gravados."""
    conn = get_connection()
    df = pd.DataFrame({"code": ["350010", "330455"], "n": [3, 7]})
    conn.register("_conv_fix", df)
    rel = conn.sql("SELECT * FROM _conv_fix")
    return set_stage(rel, "standardize")


@pytest.fixture
def rel_sem_meta():
    return get_connection().sql("SELECT 1 AS x")


# ---------------------------------------------------------------------------
# sus_as_arrow
# ---------------------------------------------------------------------------

class TestSusAsArrow:
    def test_embeds_meta_in_schema(self, rel_com_meta):
        tbl = cs.sus_as_arrow(rel_com_meta)
        md = dict(tbl.schema.metadata or {})
        assert META_SCHEMA_KEY.encode() in md
        assert json.loads(md[META_SCHEMA_KEY.encode()])["stage"] == "standardize"

    def test_keeps_the_data(self, rel_com_meta):
        tbl = cs.sus_as_arrow(rel_com_meta)
        assert tbl.num_rows == 2
        assert set(tbl.column_names) == {"code", "n"}

    def test_no_meta_means_no_key(self, rel_sem_meta):
        """Nao estampar payload vazio: um leitor tomaria por procedencia real."""
        md = dict(cs.sus_as_arrow(rel_sem_meta).schema.metadata or {})
        assert META_SCHEMA_KEY.encode() not in md

    def test_collect_arrow_still_does_not_embed(self, rel_com_meta):
        """A funcao antiga fica como era -- a nova nao muda o contrato dela."""
        md = dict(cs.collect_arrow(rel_com_meta).schema.metadata or {})
        assert META_SCHEMA_KEY.encode() not in md

    def test_rejects_dataframe(self):
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            cs.sus_as_arrow(pd.DataFrame({"a": [1]}))

    def test_round_trip_through_the_existing_reader(self, rel_com_meta, tmp_path):
        """O que sus_as_arrow escreve, sus_meta(from_parquet=) le de volta."""
        destino = tmp_path / "t.parquet"
        pq.write_table(cs.sus_as_arrow(rel_com_meta), destino)

        volta = cs.sus_meta(from_parquet=destino)
        assert cs.sus_meta(volta, "stage") == "standardize"
        assert len(volta.df()) == 2


# ---------------------------------------------------------------------------
# sus_as_duckdb
# ---------------------------------------------------------------------------

class TestSusAsDuckdb:
    def test_creates_table_and_meta_companion(self, rel_com_meta):
        conn = get_connection()
        cs.sus_as_duckdb(rel_com_meta, name="_t_conv")

        n = conn.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?",
            [f"_t_conv{META_TABLE_SUFFIX}"],
        ).fetchone()[0]
        assert n == 1, "a tabela companheira de metadata nao foi criada"
        assert len(conn.sql('SELECT * FROM "_t_conv"').df()) == 2

    def test_returned_relation_carries_meta(self, rel_com_meta):
        out = cs.sus_as_duckdb(rel_com_meta, name="_t_conv2")
        assert cs.sus_meta(out, "stage") == "standardize"

    def test_appends_history(self, rel_com_meta):
        antes = len(cs.sus_meta(rel_com_meta, "history") or [])
        out = cs.sus_as_duckdb(rel_com_meta, name="_t_conv3")
        depois = cs.sus_meta(out, "history")
        assert len(depois) == antes + 1
        assert "_t_conv3" in depois[-1]

    def test_overwrite_false_protects(self, rel_com_meta):
        cs.sus_as_duckdb(rel_com_meta, name="_t_conv4")
        with pytest.raises(ValueError, match="already exists"):
            cs.sus_as_duckdb(rel_com_meta, name="_t_conv4", overwrite=False)

    def test_overwrite_true_replaces(self, rel_com_meta):
        cs.sus_as_duckdb(rel_com_meta, name="_t_conv5")
        out = cs.sus_as_duckdb(rel_com_meta, name="_t_conv5", overwrite=True)
        assert len(out.df()) == 2

    def test_rejects_dataframe(self):
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            cs.sus_as_duckdb(pd.DataFrame({"a": [1]}))

    def test_round_trip_through_a_file_connection(self, rel_com_meta, tmp_path):
        """Conexao diferente: exercita o fallback por Arrow e o leitor de arquivo.

        Uma relacao pertence a conexao que a produziu, entao registrar numa
        outra falha e a funcao tem de transferir via Arrow. Depois disso,
        sus_meta(from_duckdb=) precisa achar tanto o dado quanto a metadata.
        """
        destino = tmp_path / "conv.duckdb"
        alvo = duckdb.connect(str(destino))
        try:
            cs.sus_as_duckdb(rel_com_meta, alvo, name="saude")
        finally:
            alvo.close()

        volta = cs.sus_meta(from_duckdb=destino, table="saude")
        assert cs.sus_meta(volta, "stage") == "standardize"
        assert len(volta.df()) == 2


# ---------------------------------------------------------------------------
# M19 — o sus_export descarta a metadata e precisa dizer isso
# ---------------------------------------------------------------------------

class TestExportAvisaSobreMetadata:
    def test_warns_when_meta_would_be_lost(self, rel_com_meta, tmp_path):
        with pytest.warns(UserWarning, match="sus_meta"):
            cs.sus_export(rel_com_meta, tmp_path / "x.parquet")

    def test_silent_when_there_is_nothing_to_lose(self, rel_sem_meta, tmp_path):
        import warnings

        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_export(rel_sem_meta, tmp_path / "y.parquet")
        assert not [
            w for w in capturados if "sus_export" in str(w.message)
        ], "avisou sobre metadata inexistente"

    def test_csv_does_not_warn(self, rel_com_meta, tmp_path):
        """O aviso e sobre Parquet, que TEM onde guardar metadata. CSV nao tem."""
        import warnings

        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            cs.sus_export(rel_com_meta, tmp_path / "z.csv")
        assert not [w for w in capturados if "sus_export" in str(w.message)]
