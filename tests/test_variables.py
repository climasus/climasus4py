"""Tests for sus_data_create_variables — age groups, epi weeks, season, quarter, etc.

Focuses on:
  - Age group bucketing with IDADE decoding
  - Temporal derived columns (epi_week, season, quarter, month_name, day_of_week)
"""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.core.variables import (
    _age_breaks_for_preset,
    sus_data_create_variables,
)


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


# ---------------------------------------------------------------------------
# Age group with IDADE decoding
# ---------------------------------------------------------------------------

class TestAgeGroup:
    @pytest.fixture
    def rel_with_idade(self):
        return _make_rel({
            "IDADE": ["405", "420", "450", "468", "490", "500", "301", "110"],
            # decoded:   5,   20,   50,   68,   90,  100,    0,    0
            "DTOBITO": ["01012023"] * 8,
            "CAUSABAS": ["J189"] * 8,
        })

    # Atualizado em 09/09/2026 (M53). Estes testes chamavam
    # ``age_group="decadal"`` / ``age_group=[0, 18, 65]``, argumento que nao
    # existe: a assinatura -- e a do R, conferida nas formals -- e
    # ``create_age_groups`` mais ``age_breaks``. O preset se resolve por
    # ``_age_breaks_for_preset``. O que os testes verificavam de substantivo
    # (a decodificacao de IDADE do DATASUS, "405" -> 5 anos) esta preservado.

    def test_decadal_groups(self, rel_with_idade):
        """Decadal age groups should use decoded IDADE."""
        result = sus_data_create_variables(
            rel_with_idade, age_breaks=_age_breaks_for_preset("decadal"), verbose=False
        )
        df = result.df()
        assert "age_group" in df.columns
        groups = df["age_group"].tolist()
        # 5→0-9, 20→20-29, 50→50-59, 68→60-69, 90→90+, 100→90+, 0→0-9, 0→0-9
        assert groups.count("0-9") == 3
        assert "20-29" in groups
        assert "50-59" in groups
        assert "60-69" in groups
        assert groups.count("90+") == 2

    def test_who_groups(self, rel_with_idade):
        result = sus_data_create_variables(
            rel_with_idade, age_breaks=_age_breaks_for_preset("who"), verbose=False
        )
        df = result.df()
        assert "age_group" in df.columns
        groups = set(df["age_group"].tolist())
        assert "0-0" in groups   # infants (age 0)
        assert "20-24" in groups  # age 20

    def test_custom_breaks(self, rel_with_idade):
        result = sus_data_create_variables(
            rel_with_idade, age_breaks=[0, 18, 65], verbose=False
        )
        df = result.df()
        groups = df["age_group"].tolist()
        # 5→0-17, 20→18-64, 50→18-64, 68→65+, 90→65+, 100→65+, 0→0-17, 0→0-17
        assert groups.count("0-17") == 3
        assert groups.count("18-64") == 2
        assert groups.count("65+") == 3

    def test_open_top_band_when_breaks_omit_the_sentinel(self, rel_with_idade):
        """Faixa de topo aberta mesmo sem o 999 explicito (corrigido 09/09/2026).

        Este e o teste que salvou um defeito real. ``age_breaks=[0, 18, 65]``
        produzia "0-17", "18-64" e NULL para 68, 90 e 100 -- os idosos, que sao
        o grupo de interesse em estudo de clima e saude, sumiam em silencio e
        depois ficavam indistinguiveis de idade faltante. Todos os presets do
        catalogo terminam no sentinela, entao so uma lista escrita a mao caia
        nisso -- e ``sus_pipeline(age_group=[...])`` passa lista.
        """
        df = sus_data_create_variables(
            rel_with_idade, age_breaks=[0, 18, 65], verbose=False
        ).df()
        assert df["age_group"].notna().all(), (
            f"idade sem grupo: {df.loc[df['age_group'].isna(), 'age_years'].tolist()}"
        )
        assert set(df["age_group"]) == {"0-17", "18-64", "65+"}

    def test_default_breaks_are_not_mutated(self, rel_with_idade):
        """A normalizacao acima nao pode tocar na constante compartilhada.

        ``age_breaks`` tem AGE_BREAKS_DEFAULT como default mutavel, entao
        acrescentar o sentinela no lugar contaminaria todas as chamadas
        seguintes do processo.
        """
        from climasus4py.core.variables import AGE_BREAKS_DEFAULT

        antes = list(AGE_BREAKS_DEFAULT)
        sus_data_create_variables(rel_with_idade, verbose=False)
        sus_data_create_variables(rel_with_idade, age_breaks=[0, 30], verbose=False)
        assert AGE_BREAKS_DEFAULT == antes

    def test_no_age_group(self, rel_with_idade):
        """Sem faixas quando create_age_groups=False.

        A premissa inverteu: antes o default era NAO criar (``age_group=None``),
        hoje o default e criar, como no R (``create_age_groups = TRUE``). Pedir
        para nao criar agora e explicito.
        """
        result = sus_data_create_variables(
            rel_with_idade, create_age_groups=False, verbose=False
        )
        df = result.df()
        assert "age_group" not in df.columns


# ---------------------------------------------------------------------------
# Temporal variables
# ---------------------------------------------------------------------------

class TestTemporalVariables:
    @pytest.fixture
    def rel_with_dates(self):
        return _make_rel({
            "DTOBITO": pd.to_datetime([
                "2023-01-15", "2023-04-20", "2023-07-10", "2023-12-25",
            ]),
            "CAUSABAS": ["J189", "I219", "A90", "E149"],
        })

    # Atualizado em 09/09/2026 (M53). Nao havia flag por variavel para portar:
    # o R tambem nao tem uma -- tem ``create_calendar_vars``, tudo ou nada,
    # igual ao Python de hoje. Os testes antigos fixavam uma granularidade que
    # nunca existiu no R. Dois nomes de coluna tambem mudaram: epi_week ->
    # epidemiological_week e season -> astronomical_season, este ultimo porque
    # ha tambem a estacao CLIMATICA (seca/chuvosa), que e outra variavel.

    @pytest.fixture
    def calendario(self, rel_with_dates):
        """Chama uma vez o que antes eram cinco chamadas separadas."""
        return sus_data_create_variables(
            rel_with_dates, create_age_groups=False, verbose=False
        ).df()

    def test_epi_week(self, calendario):
        assert "epidemiological_week" in calendario.columns
        assert calendario["epidemiological_week"].notna().all()

    def test_season(self, calendario):
        """Southern hemisphere seasons."""
        assert "astronomical_season" in calendario.columns
        # Jan→Summer, Apr→Autumn, Jul→Winter, Dec→Summer
        assert calendario["astronomical_season"].tolist() == [
            "Summer", "Autumn", "Winter", "Summer",
        ]

    def test_northern_hemisphere_inverts_the_seasons(self, rel_with_dates):
        """Contrapartida do teste acima: sem ela, 'south' podia estar cravado."""
        df = sus_data_create_variables(
            rel_with_dates, create_age_groups=False, hemisphere="north", verbose=False
        ).df()
        assert df["astronomical_season"].tolist() == [
            "Winter", "Spring", "Summer", "Winter",
        ]

    @pytest.mark.parametrize(
        "coluna", ["quarter", "month_name", "day_of_week", "year", "month"]
    )
    def test_calendar_columns_present(self, calendario, coluna):
        assert coluna in calendario.columns
        assert calendario[coluna].notna().all()

    def test_no_calendar_vars_when_disabled(self, rel_with_dates):
        """Contrapartida: create_calendar_vars=False nao cria nenhuma delas."""
        df = sus_data_create_variables(
            rel_with_dates,
            create_age_groups=False,
            create_calendar_vars=False,
            create_climate_vars=False,
            verbose=False,
        ).df()
        for coluna in ("quarter", "month_name", "epidemiological_week",
                       "astronomical_season"):
            assert coluna not in df.columns

    def test_climate_vars_require_calendar_vars(self, rel_with_dates):
        """As variaveis de clima derivam das de calendario, e a funcao diz isso."""
        with pytest.raises(ValueError, match="create_calendar_vars"):
            sus_data_create_variables(
                rel_with_dates,
                create_age_groups=False,
                create_calendar_vars=False,
                verbose=False,
            )

    def test_missing_age_column_is_rejected(self):
        """Sem coluna de idade e com create_age_groups ligado, recusa.

        Substitui ``test_no_date_col_rejects_temporal``, que casava a mensagem
        com "date column". A relacao daquele teste nao tinha NEM idade NEM
        data, e hoje a idade e verificada primeiro -- a mensagem fala de
        ``Age column``. Fixo o que a funcao de fato garante, com o ponteiro
        para o sus_data_standardize que ela mesma sugere.
        """
        rel = _make_rel({"VALUE": [1, 2, 3]})
        with pytest.raises(ValueError, match="Age column not found"):
            sus_data_create_variables(rel, verbose=False)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
