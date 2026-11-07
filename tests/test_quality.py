"""Tests for sus_data_quality_report — data quality profiling."""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.utils.quality import sus_data_quality_report


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


# Atualizado em 09/09/2026 (M53). O relatorio deixou de ser um dict raso e
# passou a ter secoes, espelhando as do relatorio do R (0. auditoria do
# pipeline, 1. visao geral, 2. completude, ...). O mapeamento:
#   total_rows          -> overview.n_rows
#   total_cols          -> overview.n_cols
#   completeness[col]   -> missing.by_column, com pct_missing por coluna
# Nao e perda: a estrutura nova diz por coluna quantos faltam, a porcentagem e
# um quality_flag, onde antes havia um numero solto.


def _pct_faltante(relatorio: dict) -> dict[str, float]:
    """Achata missing.by_column para {coluna: pct_missing}.

    ATENCAO ao contrato: ``by_column`` lista apenas as colunas que TEM valor
    ausente. Coluna completa nao aparece -- e nao aparecer e o sinal de que
    esta completa. Quem quiser a contagem de completas usa
    ``n_complete_cols``.
    """
    return {
        c["column"]: c["pct_missing"] for c in relatorio["missing"]["by_column"]
    }


class TestQualityRelation:
    def test_basic_quality(self):
        rel = _make_rel({
            "A": [1, 2, None],
            "B": ["x", None, "z"],
        })
        result = sus_data_quality_report(rel, verbose=False)
        assert result["overview"]["n_rows"] == 3
        assert result["overview"]["n_cols"] == 2
        assert set(_pct_faltante(result)) == {"A", "B"}

    def test_full_completeness(self):
        rel = _make_rel({"A": [1, 2, 3], "B": [4, 5, 6]})
        result = sus_data_quality_report(rel, verbose=False)
        assert _pct_faltante(result) == {}, "coluna completa nao entra em by_column"
        assert result["missing"]["completeness_score"] == 100.0
        assert result["missing"]["n_complete_cols"] == 2
        assert result["missing"]["overall_pct_missing"] == 0.0

    def test_empty_relation(self):
        rel = _make_rel({"A": pd.Series([], dtype="int64")})
        result = sus_data_quality_report(rel, verbose=False)
        assert result["overview"]["n_rows"] == 0

    def test_partial_completeness_is_measured(self):
        """Um terco faltando tem de sair 33.3, nao 0 nem 100."""
        rel = _make_rel({"A": [1, 2, None], "B": [4, 5, 6]})
        result = sus_data_quality_report(rel, verbose=False)
        assert _pct_faltante(result) == {"A": 33.3}
        assert result["missing"]["n_complete_cols"] == 1
        assert result["missing"]["overall_pct_missing"] == 16.7


class TestQualityDataFrame:
    def test_dataframe_quality(self):
        df = pd.DataFrame({
            "A": [1, None, 3, None],
            "B": ["x", "y", "z", "w"],
        })
        result = sus_data_quality_report(df, verbose=False)
        assert result["overview"]["n_rows"] == 4
        assert _pct_faltante(result) == {"A": 50.0}
        assert result["missing"]["n_complete_cols"] == 1


def _pipeline_rel(*passos: tuple[str, str]):
    """A relation whose metadata went through *passos* (stage, history)."""
    from climasus4py.core._stage import add_history, set_stage

    rel = _make_rel({"sex": ["Male", "Female"], "age_group": ["0-4", "65+"]})
    for i, (stage, msg) in enumerate(passos):
        rel = set_stage(rel, stage, system="SIM-DO" if i == 0 else None,
                        rel_type="health" if i == 0 else None)
        rel = add_history(rel, msg)
    return rel


_ATE_VARIABLES = [
    ("import", "Imported DATASUS SIM-DO (SP) via FTP (climasus4py)"),
    ("clean", "Cleaned character encoding (UTF-8)"),
    ("standardize", "Standardized column names and types to EN"),
    ("variables", "Created derived variables (EN): age_group"),
]


class TestPipelineAudit:
    """M235: the report speaks R's stage names; this package's metadata
    uses its own, and stages_reached came out empty for every pipeline."""

    def test_stages_reached_translates_this_package_stage_names(self):
        rep = sus_data_quality_report(_pipeline_rel(*_ATE_VARIABLES), verbose=False)
        assert rep["pipeline"]["stages_reached"] == ["import", "clean", "stand", "derive"]
        assert rep["derived"]["stage_at_least_derive"] is True

    def test_filters_that_never_ran_are_not_reported(self):
        """climasus4r marks every stage before the current one as reached,
        so it lists both filters after create_variables even when neither
        ran (M236). Here only what the metadata recorded counts."""
        rep = sus_data_quality_report(_pipeline_rel(*_ATE_VARIABLES), verbose=False)
        fns = rep["pipeline"]["functions_applied"]
        assert "sus_data_filter_cid()" not in fns
        assert "sus_data_filter_demographics()" not in fns
        assert fns == ["sus_data_import()", "sus_data_clean_encoding()",
                       "sus_data_standardize()", "sus_data_create_variables()"]

    @pytest.mark.parametrize("criterios, esperado", [
        ("sex='Male'; age=[15,64]", ["filter_demo"]),
        ("groups='respiratory'", ["filter_cid"]),
        ("groups='respiratory'; sex='Male'", ["filter_cid", "filter_demo"]),
    ])
    def test_sus_filter_maps_to_the_r_filter_it_amounts_to(self, criterios, esperado):
        rel = _pipeline_rel(*_ATE_VARIABLES, ("filter", f"Filtered: {criterios}"))
        reached = sus_data_quality_report(rel, verbose=False)["pipeline"]["stages_reached"]
        assert [s for s in reached if s.startswith("filter")] == esperado

    def test_without_a_visit_log_falls_back_to_the_stage_prefix(self):
        from climasus4py.utils.quality import _stages_reached
        assert _stages_reached("variables", None, []) == [
            "import", "clean", "stand", "filter_cid", "filter_demo", "derive"]
        assert _stages_reached("unknown", None, []) == []

    def test_plain_data_has_no_stage(self):
        rep = sus_data_quality_report(pd.DataFrame({"sex": ["Male"]}), verbose=False)
        assert rep["pipeline"]["stages_reached"] == []
        assert rep["derived"]["stage_at_least_derive"] is None

    def test_derived_variables_match_r_keys(self):
        rep = sus_data_quality_report(_pipeline_rel(*_ATE_VARIABLES), verbose=False)
        assert list(rep["derived"]["variables"]) == [
            "age_group", "ibge_age_group", "sex_label", "climate_risk_grp", "month",
            "year", "quarter", "epi_week", "season", "dry_rainy"]
        assert rep["derived"]["variables"]["sex_label"] == {"present": True, "col": "sex"}

    def test_console_lists_pending_stages_and_history(self, capsys):
        sus_data_quality_report(_pipeline_rel(*_ATE_VARIABLES), lang="en", verbose=False)
        out = capsys.readouterr().out
        assert "Stages not yet applied" in out
        assert "aggregate" in out.split("Stages not yet applied")[1]
        assert "History entries: 4" in out
        assert "Created derived variables (EN): age_group" in out


_DADO_SIM = pd.DataFrame({
    "sex": ["Male", "Female", "Male", None],
    "race": ["White", "Brown", "White", "Black"],
    "age_years": [70, 1, 45, None],
    "death_date": pd.to_datetime(["2023-01-02", "2023-02-03", "2023-03-04", "2023-04-05"]),
    "underlying_cause": ["J189", "I219", "J189", "C349"],
    "residence_municipality_code": ["355030", "355030", "350950", "351880"],
})


class TestFileFormats:
    """M237: the markdown and HTML files carry sections 3-6, as R writes them.
    They went straight from completeness to derived variables before."""

    def test_markdown_has_the_four_data_sections(self, tmp_path):
        arq = tmp_path / "r.md"
        sus_data_quality_report(_DADO_SIM, output_format="markdown",
                                output_file=str(arq), lang="en", verbose=False)
        md = arq.read_text(encoding="utf-8")
        for titulo in ("## 3. Demographic Variables", "## 4. Date Validation",
                       "## 5. ICD-10 Code Quality", "## 6. Geographic Coverage"):
            assert titulo in md
        assert md.index("## 2. Completeness") < md.index("## 3.") < md.index("## 7.")
        assert "| Male | 2 | 50.0 |" in md
        assert "| NA | 1 | 25.0 |" in md               # missing sex, as R's NA row
        assert "| J189 | 2 | 50.0 |" in md
        assert "| 355030 | 2 | 50.0 |" in md
        assert "- Range: 1.0 - 70.0" in md
        assert "2023-01-02 - 2023-04-05" in md

    def test_html_has_the_four_data_sections(self, tmp_path):
        arq = tmp_path / "r.html"
        sus_data_quality_report(_DADO_SIM, output_format="html",
                                output_file=str(arq), lang="en", verbose=False)
        html = arq.read_text(encoding="utf-8")
        for titulo in ("3. Demographic Variables", "4. Date Validation",
                       "5. ICD-10 Code Quality", "6. Geographic Coverage"):
            assert f"<h2>{titulo}</h2>" in html
        assert "<td>J189</td><td>2</td><td>50.0</td>" in html
        assert "Municipalities (n=3)" in html

    def test_html_escapes_category_text(self, tmp_path):
        arq = tmp_path / "r.html"
        dado = _DADO_SIM.assign(race=["<b>x</b>", "a & b", "White", "Black"])
        sus_data_quality_report(dado, output_format="html", output_file=str(arq),
                                lang="en", verbose=False)
        html = arq.read_text(encoding="utf-8")
        assert "&lt;b&gt;x&lt;/b&gt;" in html and "a &amp; b" in html


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
