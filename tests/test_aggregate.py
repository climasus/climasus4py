"""Tests for sus_data_aggregate — time/geo grouping and summarisation.

Estes testes foram escritos contra uma API Python ANTERIOR a paridade
com o R -- `time=`, `geo=`, `extra_groups=`, `week_format=`, colunas
`time_group` e `count`. Nada disso existe no climasus4r, cuja assinatura
e `df, time_unit, fun, group_by, value_col, complete_dates, date_col,
backend, lang, verbose`. A funcao foi realinhada ao R e os testes
ficaram para tras; foram migrados em 14/09/2026.

O que mudou, para quem for ler os dois lados:

  time=            -> time_unit=
  geo="municipality"  -> nada: a coluna geografica e detectada sozinha
  geo="state"      -> nao existe (ver TestGeoAggregation)
  extra_groups=    -> group_by=
  week_format=     -> nao existe; a semana sai como DATA de inicio
  coluna time_group -> date
  coluna count     -> n

A migracao expos um bug de verdade no eixo da semana -- ver
TestSemanaComecaNoDomingo (M95).
"""

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
        df = sus_data_aggregate(rel, time_unit="month", verbose=False).df()

        assert "date" in df.columns
        assert "n" in df.columns
        assert len(df["date"].unique()) >= 4

    def test_year_aggregation(self, rel):
        df = sus_data_aggregate(rel, time_unit="year", verbose=False).df()

        assert len(df["date"].unique()) == 1

    def test_quarter_aggregation(self, rel):
        df = sus_data_aggregate(rel, time_unit="quarter", verbose=False).df()

        assert len(df["date"].unique()) >= 3

    def test_group_by_adiciona_colunas(self, rel):
        """Era `extra_groups=`; hoje `group_by=`, como no R."""
        df = sus_data_aggregate(
            rel, time_unit="month", group_by=["CAUSABAS"], verbose=False
        ).df()

        assert "CAUSABAS" in df.columns

    def test_a_coluna_geografica_e_detectada_sozinha(self, rel):
        """Era `geo="municipality"`; hoje sai do proprio dado.

        O CODMUNRES esta na lista de candidatos, entao entra no
        agrupamento sem ninguem pedir.
        """
        df = sus_data_aggregate(rel, time_unit="month", verbose=False).df()

        assert "CODMUNRES" in df.columns

    def test_sem_coluna_de_data_levanta(self):
        """Antes o teste esperava a contagem total; hoje a funcao exige
        uma coluna de data e diz o que fazer."""
        rel = _make_rel({"VALUE": [1, 2, 3]})

        with pytest.raises(ValueError, match="[Dd]ate column not found"):
            sus_data_aggregate(rel, time_unit="month", verbose=False)

    def test_a_soma_das_contagens_e_o_total(self, rel):
        df = sus_data_aggregate(rel, time_unit="month", verbose=False).df()

        assert df["n"].sum() == 6


class TestGeoAggregation:
    def test_uf_nao_e_candidato_geografico(self):
        """A deteccao automatica so olha codigo de MUNICIPIO.

        Os candidatos de `_agg_geo_candidates` sao todos municipais --
        residencia, ocorrencia, ID_MUNICIP. Nao ha nenhum de UF, entao
        uma relacao que so tem `UF` agrega no tempo e a coluna nao
        aparece no resultado.

        Isso NAO e perda de capacidade, e foi o que eu supus primeiro:
        o `sus_data_aggregate` do R tambem nao tem argumento `geo=`, e
        quem oferece nivel estadual no pacote e o `sus_pipeline`, que
        tem `geo="state"` como default e deriva o codigo de UF a partir
        do municipal. O `geo=` que estes testes usavam era invencao do
        Python antigo nesta funcao.

        (O `sus_pipeline` honra `geo="state"` so no caminho rapido; o
        staged avisa que nao conseguiu. Isso e o M52, ja registrado.)
        """
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "UF": ["SP", "RJ", "SP"],
        })
        df = sus_data_aggregate(rel, time_unit="month", verbose=False).df()

        assert "UF" not in df.columns
        assert list(df.columns) == ["date", "n"]
        assert df["n"].iloc[0] == 3

    def test_mas_da_para_agrupar_por_uf_via_group_by(self):
        """O caminho que existe hoje para o mesmo resultado."""
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "UF": ["SP", "RJ", "SP"],
        })
        df = sus_data_aggregate(
            rel, time_unit="month", group_by=["UF"], verbose=False
        ).df()

        assert "UF" in df.columns
        assert set(df["UF"]) == {"SP", "RJ"}
        assert df.loc[df["UF"] == "SP", "n"].iloc[0] == 2


class TestSemanaComecaNoDomingo:
    """A semana comeca no DOMINGO, nao na segunda (M95).

    O R agrega com `lubridate::floor_date(date, unit = time_unit)` e NAO
    passa `week_start`, entao herda o default do lubridate --
    `getOption("lubridate.week.start", 7)`, isto e 7 = domingo. O
    `DATE_TRUNC('week', ...)` do DuckDB e ISO e comeca na SEGUNDA.

    O Python usava o DATE_TRUNC cru, e o efeito nao era de borda: das
    seis datas de teste espalhadas por 2023, TODAS AS SEIS caiam num
    bucket diferente do R. Toda serie semanal saia deslocada, sem aviso.

    Domingo tambem e a convencao certa aqui: a semana epidemiologica da
    SVS vai de domingo a sabado. A expressao corrigida desloca um dia
    para frente, trunca na segunda e volta um dia.

    Os valores esperados abaixo foram medidos rodando o lubridate.
    """

    #: data -> inicio de semana que o lubridate produz.
    ESPERADO = {
        "2023-01-01": "2023-01-01",   # domingo: e o proprio inicio
        "2023-01-08": "2023-01-08",   # domingo
        "2023-06-15": "2023-06-11",   # quinta -> domingo anterior
        "2023-12-25": "2023-12-24",   # segunda -> domingo anterior
        "2023-03-19": "2023-03-19",   # domingo
        "2023-09-03": "2023-09-03",   # domingo
    }

    def test_bate_com_o_lubridate(self):
        rel = _make_rel({"DTOBITO": pd.to_datetime(list(self.ESPERADO))})
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()
        got = sorted(df["date"].astype(str).str[:10])

        assert got == sorted(set(self.ESPERADO.values()))

    @pytest.mark.parametrize(("data", "inicio"), list(ESPERADO.items()))
    def test_cada_data_no_seu_domingo(self, data, inicio):
        rel = _make_rel({"DTOBITO": pd.to_datetime([data])})
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()

        assert str(df["date"].iloc[0])[:10] == inicio

    def test_todo_inicio_de_semana_cai_num_domingo(self):
        """A propriedade que define a correcao."""
        rel = _make_rel({
            "DTOBITO": pd.date_range("2023-01-01", "2023-12-31", freq="D")
        })
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()

        assert set(pd.to_datetime(df["date"]).dt.dayofweek) == {6}   # 6 = domingo

    def test_o_domingo_e_a_segunda_seguinte_ficam_juntos(self):
        """Com ISO eles caiam em semanas diferentes."""
        rel = _make_rel({"DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-02"])})
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()

        assert len(df) == 1
        assert df["n"].iloc[0] == 2
        assert str(df["date"].iloc[0])[:10] == "2023-01-01"

    def test_o_sabado_fecha_a_semana(self):
        """Domingo a sabado num bucket; o domingo seguinte, noutro."""
        rel = _make_rel({
            "DTOBITO": pd.to_datetime(["2023-01-01", "2023-01-07", "2023-01-08"])
        })
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()
        por_semana = dict(zip(df["date"].astype(str).str[:10], df["n"]))

        assert por_semana == {"2023-01-01": 2, "2023-01-08": 1}


class TestSemanaContraCreateVariables:
    """O `epi_week=True` nao existe mais no create_variables.

    O teste original comparava
    `sus_data_aggregate(time="week")` com
    `sus_data_create_variables(epi_week=True)` e exigia valores
    identicos. Hoje o create_variables tem `create_calendar_vars` e nao
    `epi_week`, e o que ele devolve nao e um rotulo de semana -- entao a
    comparacao nao tem mais os dois lados.

    Fica registrado o que sobrou de verificavel: o R usa
    `lubridate::epiweek()` no caminho de variaveis (numeracao com base
    em domingo) e `floor_date` no de agregacao, e os dois concordam na
    fronteira porque ambos usam domingo. Depois da correcao do M95 o
    Python tambem usa domingo nos dois, mas o rotulo do create_variables
    ainda nao foi portado -- ver M97.
    """

    def test_o_parametro_epi_week_nao_existe_mais(self):
        import inspect

        params = inspect.signature(sus_data_create_variables).parameters

        assert "epi_week" not in params
        assert "create_calendar_vars" in params

    def test_o_aggregate_ja_usa_a_fronteira_de_domingo(self):
        """A metade do alinhamento que da para afirmar hoje."""
        rel = _make_rel({"DTOBITO": pd.to_datetime(["2023-01-01"])})
        df = sus_data_aggregate(rel, time_unit="week", verbose=False).df()

        assert str(df["date"].iloc[0])[:10] == "2023-01-01"


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
