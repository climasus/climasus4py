"""Tests for sus_data_aggregate — time/geo grouping and summarisation."""

import re

import pandas as pd
import pytest

from climasus4py.core.aggregate import sus_data_aggregate
from climasus4py.core.engine import get_connection
from climasus4py.core.variables import sus_data_create_variables


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


class TestTimeAggregation:
    @pytest.fixture
    def rel(self):
        return _make_rel({
            "DTOBITO": pd.to_datetime([
                "2023-01-10", "2023-01-20", "2023-02-15",
                "2023-06-01", "2023-06-15", "2023-12-25",
            ]),
            "CODMUNRES": ["355030", "330455", "355030",
                          "310620", "355030", "330455"],
            "CAUSABAS": ["J189"] * 6,
        })

    def test_month_aggregation(self, rel):
        result = sus_data_aggregate(rel, time="month", geo="municipality")
        df = result.df()
        assert "time_group" in df.columns
        assert "count" in df.columns
        # At least 4 distinct months
        assert len(df["time_group"].unique()) >= 4

    def test_year_aggregation(self, rel):
        result = sus_data_aggregate(rel, time="year", geo="municipality")
        df = result.df()
        # All same year
        assert len(df["time_group"].unique()) == 1

    def test_quarter_aggregation(self, rel):
        result = sus_data_aggregate(rel, time="quarter", geo="municipality")
        df = result.df()
        assert len(df["time_group"].unique()) >= 3

    def test_extra_groups(self, rel):
        """extra_groups should add grouping columns."""
        result = sus_data_aggregate(rel, time="month", geo="municipality", extra_groups=["CAUSABAS"])  # noqa: E501
        df = result.df()
        assert "CAUSABAS" in df.columns

    def test_no_date_col_returns_count(self):
        """Without date/geo columns, should return total count."""
        rel = _make_rel({"VALUE": [1, 2, 3]})
        result = sus_data_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "count" in df.columns
        assert df["count"].iloc[0] == 3


class TestGeoAggregation:
    def test_state_geo(self):
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "UF": ["SP", "RJ", "SP"],
        })
        result = sus_data_aggregate(rel, time="month", geo="state")
        df = result.df()
        assert "UF" in df.columns


class TestWeekFormatSVS:
    """P1 Sprint 2 — sus_data_aggregate week format aligned with SVS."""

    @pytest.fixture
    def rel_week(self):
        # 2023-01-01 is a Sunday → SVS week 01/2023
        return _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-08"]),
        })

    def test_default_is_svs_format(self, rel_week):
        """Default week_format='svs' → 'WW/YYYY' pattern."""
        df = sus_data_aggregate(rel_week, time="week", geo="state").df()
        for val in df["time_group"].dropna():
            assert re.fullmatch(r"\d{2}/\d{4}", val), f"Expected WW/YYYY, got {val!r}"

    def test_iso_format_option(self, rel_week):
        """week_format='iso' preserves old 'YYYY-WXX' pattern."""
        df = sus_data_aggregate(rel_week, time="week", geo="state", week_format="iso").df()
        for val in df["time_group"].dropna():
            assert re.fullmatch(r"\d{4}-W\d{2}", val), f"Expected YYYY-WXX, got {val!r}"

    def test_aggregate_and_variables_aligned(self):
        """sus_data_aggregate(time='week') and sus_data_create_variables(epi_week=True) must return identical values."""  # noqa: E501
        dates = pd.to_datetime([
            "2023-01-01", "2023-06-15", "2023-12-25",
            "2023-03-19", "2023-09-03",
        ])
        rel = _make_rel({"DTOBITO": dates})
        agg_weeks = set(sus_data_aggregate(rel, time="week", geo="state").df()["time_group"].dropna())  # noqa: E501
        var_weeks = set(sus_data_create_variables(rel, epi_week=True).df()["epi_week"].dropna())
        assert agg_weeks == var_weeks, (
            f"Mismatch between aggregate and variables week values:\n"
            f"aggregate: {sorted(agg_weeks)}\nvariables: {sorted(var_weeks)}"
        )


# ---------------------------------------------------------------------------
# M21 — o system vem do sus_meta quando nao e passado
# ---------------------------------------------------------------------------

class TestSystemFromMeta:
    """A coluna geografica deixa de ser escolhida por ordem default (M21).

    Para o SIM, geo_candidates["SIM"] poe OCORRENCIA primeiro e
    geo_candidates["common"] poe RESIDENCIA. Sem ``system=``, caia no
    "common" -- entao o usuario recebia uma serie de mortalidade por local de
    residencia pensando estar vendo por local de ocorrencia, sem nenhum sinal.
    Sao recortes epidemiologicos diferentes: onde a pessoa morava contra onde
    morreu.

    O agravante era que o ``system`` ja estava na relacao, no sus_meta, e a
    funcao nao lia.
    """

    @pytest.fixture
    def rel_dois_municipios(self):
        """Relacao com AS DUAS colunas candidatas, que e quando a escolha importa."""
        return _make_rel({
            "death_date": pd.to_datetime(["2023-01-10", "2023-01-20", "2023-02-05"]),
            "occurrence_municipality_code": [355030, 355030, 330455],
            "residence_municipality_code": [350010, 330020, 310620],
            "sex": ["Male", "Female", "Male"],
        })

    def test_reads_system_from_meta(self, rel_dois_municipios):
        from climasus4py.core._stage import set_stage

        set_stage(rel_dois_municipios, "standardize", system="SIM-DO")
        cols = sus_data_aggregate(
            rel_dois_municipios, time_unit="month", verbose=False
        ).df().columns
        assert "occurrence_municipality_code" in cols
        assert "residence_municipality_code" not in cols

    def test_explicit_argument_still_wins(self, rel_dois_municipios):
        """Metadado e o padrao, nao uma imposicao: o argumento tem precedencia."""
        from climasus4py.core._stage import set_stage

        set_stage(rel_dois_municipios, "standardize", system="SIM-DO")
        cols = sus_data_aggregate(
            rel_dois_municipios, time_unit="month", system="SINAN-DENGUE", verbose=False
        ).df().columns
        assert "occurrence_municipality_code" not in cols

    def test_warns_when_nothing_says_the_system(self, rel_dois_municipios):
        """Sem system em nenhum lugar, a ordem default decide -- e isso se diz."""
        with pytest.warns(UserWarning, match="no system given"):
            sus_data_aggregate(rel_dois_municipios, time_unit="month", verbose=False)

    def test_no_warning_when_only_one_candidate(self):
        """Havendo uma coluna so, nao ha escolha a sinalizar: silencio.

        Sem esta contrapartida o aviso viraria ruido em toda agregacao de dado
        que tem uma unica coluna geografica, e ruido se aprende a ignorar.
        """
        import warnings

        rel = _make_rel({
            "death_date": pd.to_datetime(["2023-01-10", "2023-02-05"]),
            "residence_municipality_code": [350010, 330020],
        })
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            sus_data_aggregate(rel, time_unit="month", verbose=False)
        assert not [w for w in capturados if "no system given" in str(w.message)]

    def test_count_name_also_follows_the_system(self, rel_dois_municipios):
        """O system nomeia a contagem tambem: n_deaths para SIM, n no generico."""
        from climasus4py.core._stage import set_stage

        set_stage(rel_dois_municipios, "standardize", system="SIM-DO")
        cols = sus_data_aggregate(
            rel_dois_municipios, time_unit="month", verbose=False
        ).df().columns
        assert "n_deaths" in cols


class TestStandardizeRecordsSystem:
    """O standardize passa a gravar o system que resolveu (M21, 09/09/2026).

    So o sus_data_import gravava. Uma cadeia que comecasse de um parquet cru
    perdia o system MESMO quando o usuario o informava ao standardize -- e ai
    o sus_data_aggregate nao tinha o que ler.
    """

    def test_explicit_system_is_recorded(self):
        import climasus4py as cs

        rel = _make_rel({"DTOBITO": ["01012023"], "CAUSABAS": ["J189"]})
        std = cs.sus_data_standardize(rel, system="SIM-DO")
        assert cs.sus_meta(std, "system") == "SIM-DO"

    def test_detected_system_is_recorded(self):
        """Tambem quando vem da deteccao automatica, nao so do argumento."""
        import climasus4py as cs

        rel = _make_rel({
            "DTOBITO": ["01012023"], "CAUSABAS": ["J189"], "CODMUNRES": ["355030"],
        })
        std = cs.sus_data_standardize(rel)
        assert cs.sus_meta(std, "system") is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
