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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
