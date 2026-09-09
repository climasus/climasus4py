"""Tests for sus_meta and the expanded _stage.py metadata tracking."""

import duckdb
import pytest

from climasus4py.core._stage import get_meta, get_stage, set_stage
from climasus4py.core.meta import sus_meta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_rel() -> duckdb.DuckDBPyRelation:
    """Return a minimal DuckDB relation with no metadata."""
    con = duckdb.connect()
    return con.sql("SELECT 1 AS x")


def _staged_rel(stage: str, system: str = "SIM-DO", rel_type: str = "health"):
    rel = _fresh_rel()
    set_stage(rel, stage, system=system, rel_type=rel_type)
    return rel


# ---------------------------------------------------------------------------
# _stage.py — set_stage / get_stage / get_meta
# ---------------------------------------------------------------------------

class TestSetGetStage:
    def test_set_and_get_stage(self):
        rel = _fresh_rel()
        set_stage(rel, "import")
        assert get_stage(rel) == "import"

    def test_get_stage_unknown_returns_none(self):
        rel = _fresh_rel()
        assert get_stage(rel) is None

    def test_set_stage_returns_same_rel(self):
        rel = _fresh_rel()
        returned = set_stage(rel, "clean")
        assert returned is rel

    def test_meta_has_all_keys(self):
        rel = _staged_rel("standardize", system="SINAN", rel_type="notificacao")
        meta = get_meta(rel)
        # "stages" entrou quando history foi dividido em dois -- ver a nota em
        # test_stages_accumulate abaixo.
        assert set(meta.keys()) == {"stage", "system", "type", "stages", "history"}

    def test_meta_system_and_type(self):
        rel = _staged_rel("filter", system="SIM-DO", rel_type="health")
        meta = get_meta(rel)
        assert meta["system"] == "SIM-DO"
        assert meta["type"] == "health"

    def test_stages_accumulate(self):
        """A trilha de etapas vive em "stages" (M53, atualizado 09/09/2026).

        Estes testes esperavam ``history == ["import", "clean", "filter"]``,
        de quando ``history`` guardava NOMES DE ETAPA. Hoje sao duas coisas
        distintas, e a separacao segue o R:

        - ``stages``  -> nomes de etapa, alimentado por ``set_stage``
        - ``history`` -> mensagens TIMESTAMPADAS legiveis, alimentado por
          ``add_history``, equivalente a ``sus_meta(add_history=)`` do R

        Ou seja ``set_stage`` nao escreve mais em ``history``, e por isso a
        assercao antiga via lista vazia. Nao e defeito: e a trilha de auditoria
        do R, que precisa de texto com hora e nao so do nome da etapa.
        """
        rel = _fresh_rel()
        set_stage(rel, "import")
        set_stage(rel, "clean")
        set_stage(rel, "filter")
        meta = get_meta(rel)
        assert meta["stages"] == ["import", "clean", "filter"]
        assert meta["stage"] == "filter", "stage guarda a etapa corrente"
        assert meta["history"] == [], "set_stage nao escreve em history"

    def test_stages_no_duplicate_consecutive(self):
        rel = _fresh_rel()
        set_stage(rel, "import")
        set_stage(rel, "import")  # mesma etapa duas vezes
        assert get_meta(rel)["stages"] == ["import"]

    def test_repeated_stage_after_another_is_kept(self):
        """So o consecutivo e colapsado: import -> clean -> import mantem os tres.

        Sem isso o teste anterior passaria com uma implementacao que apenas
        deduplica a lista inteira, o que perderia um reprocessamento legitimo.
        """
        rel = _fresh_rel()
        set_stage(rel, "import")
        set_stage(rel, "clean")
        set_stage(rel, "import")
        assert get_meta(rel)["stages"] == ["import", "clean", "import"]

    def test_history_holds_timestamped_messages(self):
        """A contrapartida: history recebe texto com hora, via add_history."""
        from climasus4py.core._stage import add_history

        rel = _fresh_rel()
        set_stage(rel, "import")
        add_history(rel, "Imported DATASUS SIM-DO (SP) via FTP")
        historico = get_meta(rel)["history"]
        assert len(historico) == 1
        assert historico[0].startswith("[")
        assert "Imported DATASUS" in historico[0]

    def test_meta_system_preserved_across_stages(self):
        rel = _fresh_rel()
        set_stage(rel, "import", system="SIM-DO")
        set_stage(rel, "clean")  # no system arg
        assert get_meta(rel)["system"] == "SIM-DO"


# ---------------------------------------------------------------------------
# sus_meta — introspection API
# ---------------------------------------------------------------------------

class TestSusMeta:
    def test_returns_none_for_untracked_rel(self):
        rel = _fresh_rel()
        assert sus_meta(rel) is None

    def test_returns_full_dict(self):
        rel = _staged_rel("aggregate")
        result = sus_meta(rel)
        assert isinstance(result, dict)
        assert result["stage"] == "aggregate"

    def test_field_stage(self):
        rel = _staged_rel("filter")
        assert sus_meta(rel, field="stage") == "filter"

    def test_field_system(self):
        rel = _staged_rel("import", system="SIM-DO")
        assert sus_meta(rel, field="system") == "SIM-DO"

    def test_field_stages(self):
        """A trilha de etapas sai por field="stages" -- ver test_stages_accumulate."""
        rel = _fresh_rel()
        set_stage(rel, "import", system="SIM-DO")
        set_stage(rel, "clean")
        assert sus_meta(rel, field="stages") == ["import", "clean"]

    def test_invalid_field_raises_valueerror(self):
        rel = _staged_rel("import")
        with pytest.raises(ValueError, match="unknown field"):
            sus_meta(rel, field="nope")

    def test_non_relation_raises_typeerror(self):
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            sus_meta("not a relation")

    def test_add_history_does_not_mutate_stored(self):
        """A intencao original vale; os valores mudaram com a divisao stages/history.

        ``set_stage`` nao alimenta mais ``history``, entao a relacao comeca com
        history vazio e a entrada nova e a unica. O que este teste protege
        continua sendo o mesmo: ``add_history`` anota a COPIA devolvida sem
        tocar no que esta guardado.
        """
        rel = _staged_rel("import")
        extended = sus_meta(rel, add_history="Cleaned encoding")
        stored = sus_meta(rel, field="history")
        assert len(extended["history"]) == 1
        assert extended["history"][0].endswith("Cleaned encoding")
        assert stored == [], "o history guardado nao pode ter mudado"

    def test_add_history_with_field(self):
        rel = _staged_rel("import")
        result = sus_meta(rel, field="history", add_history="Cleaned encoding")
        assert len(result) == 1
        assert result[0].endswith("Cleaned encoding")

    def test_add_history_is_timestamped_like_the_internal_writer(self):
        """Uma so forma de entrada no log (M53, 09/09/2026).

        O ``sus_meta(add_history=)`` publico anexava a string CRUA enquanto o
        ``_stage.add_history`` interno punha timestamp, entao a trilha de
        auditoria acabava com dois formatos na mesma lista -- metade
        parseavel por hora e metade nao. Agora os dois passam por
        ``format_history_entry``.
        """
        from climasus4py.core._stage import add_history as add_interno

        rel = _staged_rel("import")
        add_interno(rel, "pelo escritor interno")
        combinado = sus_meta(rel, add_history="pelo publico")["history"]

        assert len(combinado) == 2
        assert all(e.startswith("[") for e in combinado), combinado

    def test_add_history_skips_a_consecutive_repeat(self):
        """Repetir a mesma mensagem em seguida nao duplica.

        A comparacao tem de ser pela MENSAGEM: duas entradas iguais gravadas
        com um segundo de diferenca sao strings diferentes, e comparar as
        strings finais nunca casaria.
        """
        rel = _staged_rel("import")
        uma = sus_meta(rel, add_history="mesma coisa")
        # simula o segundo pedido sobre um estado que ja tem a entrada
        rel2 = _staged_rel("import")
        from climasus4py.core._stage import add_history as add_interno

        add_interno(rel2, "mesma coisa")
        duas = sus_meta(rel2, add_history="mesma coisa")

        assert len(uma["history"]) == 1
        assert len(duas["history"]) == 1, duas["history"]

    def test_none_when_no_meta_and_field_requested(self):
        rel = _fresh_rel()
        assert sus_meta(rel, field="stage") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
