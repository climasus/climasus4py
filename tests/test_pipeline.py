"""Tests for sus_pipeline — orchestração, composição de stages e fast-path.

Cobre pipeline.py e _stage.py com mocks de sus_data_import para evitar I/O real.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from climasus4py.core._stage import (
    CANONICAL_STAGES,
    assert_after,
    get_stage,
    set_stage,
)
from climasus4py.core.engine import get_connection

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _synthetic_sim_do(n: int = 5) -> pd.DataFrame:
    """DataFrame mínimo com colunas típicas de SIM-DO."""
    return pd.DataFrame(
        {
            "DTOBITO": ["01012022"] * n,
            "IDADE": ["420"] * n,
            "SEXO": ["1"] * n,
            "CAUSABAS": ["J189"] * n,
            "CODMUNRES": ["355030"] * n,
        }
    )


def _make_import_mock(df: pd.DataFrame):
    """Retorna função substituta para sus_data_import que devolve relação sintética."""

    def _fake_import(system, uf, year, **kwargs):
        conn = get_connection()
        rel = conn.from_df(df)
        set_stage(rel, "import")
        return rel

    return _fake_import


def _make_passthrough(name: str, calls: list[str]):
    """Stage mock que registra chamada e retorna rel sem alteração."""

    def _pass(rel, **kwargs):
        calls.append(name)
        return rel

    return _pass


def _patch_all_stages(monkeypatch, mod, calls: list[str]) -> None:
    """Monkeypatcha todos os stages do pipeline com passthroughs rastreáveis."""
    _stage_map = [
        ("sus_data_clean_encoding", "clean"),
        ("sus_data_standardize", "standardize"),
        ("sus_filter", "filter"),
        ("sus_data_create_variables", "variables"),
        ("sus_data_aggregate", "aggregate"),
    ]
    for attr, name in _stage_map:
        monkeypatch.setattr(mod, attr, _make_passthrough(name, calls))


# ---------------------------------------------------------------------------
# _stage.py — set_stage / get_stage / assert_after
# ---------------------------------------------------------------------------

class TestStageTracking:
    def test_set_and_get_stage(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "import")
        assert get_stage(rel) == "import"

    def test_overwrite_stage(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "import")
        set_stage(rel, "clean")
        assert get_stage(rel) == "clean"

    def test_get_stage_unknown_returns_none(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        assert get_stage(rel) is None

    def test_assert_after_raises_when_current_before_required(self):
        """Relation em 'import' não pode ser usada onde 'clean' é mínimo."""
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "import")
        with pytest.raises(ValueError, match="before required stage"):
            assert_after(rel, "clean")

    def test_assert_after_raises_multiple_stages_behind(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "import")
        with pytest.raises(ValueError):
            assert_after(rel, "aggregate")

    def test_assert_after_passes_when_same_stage(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "clean")
        assert_after(rel, "clean")  # mesmo nível — não levanta

    def test_assert_after_passes_when_current_is_after_required(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        set_stage(rel, "standardize")
        assert_after(rel, "import")  # standardize > import — ok

    def test_assert_after_passes_when_no_stage_set(self):
        """Sem stage definido, assert_after não deve levantar."""
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        assert_after(rel, "clean")  # get_stage → None → retorna cedo

    def test_canonical_stages_order(self):
        assert CANONICAL_STAGES == [
            "import", "clean", "standardize", "filter", "variables", "aggregate"
        ]

    def test_set_stage_returns_same_relation(self):
        rel = get_connection().from_df(pd.DataFrame({"x": [1]}))
        returned = set_stage(rel, "import")
        assert returned is rel


# ---------------------------------------------------------------------------
# _can_fast_path
# ---------------------------------------------------------------------------

class TestCanFastPath:
    def test_default_params_enables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, False, "month", "state") is True

    def test_age_group_disables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path("who", False, "month", "state") is False

    def test_epi_week_disables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, True, "month", "state") is False

    def test_invalid_time_disables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, False, "decade", "state") is False

    def test_invalid_geo_disables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, False, "month", "region") is False

    @pytest.mark.parametrize("time", ["year", "quarter", "month", "week", "day"])
    def test_all_valid_times_enable_fast_path(self, time):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, False, time, "state") is True

    @pytest.mark.parametrize("geo", ["state", "municipality"])
    def test_all_valid_geos_enable_fast_path(self, geo):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path(None, False, "month", geo) is True

    def test_age_group_list_disables_fast_path(self):
        from climasus4py.core.pipeline import _can_fast_path
        assert _can_fast_path([0, 15, 60], False, "month", "state") is False


# ---------------------------------------------------------------------------
# sus_pipeline — composição de stages via mocked sus_data_import
# ---------------------------------------------------------------------------

class TestPipelineStaging:
    """Testa lógica de orquestração com sus_data_import e stages mockados."""

    def test_pipeline_returns_result(self, monkeypatch, tmp_path):
        """sus_pipeline deve retornar algo quando stages são passthrough."""
        from climasus4py.core import pipeline as mod

        calls: list[str] = []
        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        _patch_all_stages(monkeypatch, mod, calls)

        result = mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )
        assert result is not None

    def test_staged_path_calls_all_stages_in_order(self, monkeypatch, tmp_path):
        """epi_week=True força staged path; todos os stages devem ser chamados na ordem."""
        from climasus4py.core import pipeline as mod

        calls: list[str] = []
        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        _patch_all_stages(monkeypatch, mod, calls)

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert calls == ["clean", "standardize", "filter", "variables", "aggregate"]

    def test_clean_called_before_standardize(self, monkeypatch, tmp_path):
        """clean deve ser chamado antes de standardize."""
        from climasus4py.core import pipeline as mod

        calls: list[str] = []
        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        _patch_all_stages(monkeypatch, mod, calls)

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert calls.index("clean") < calls.index("standardize")

    def test_groups_string_forwarded_as_list_to_filter(self, monkeypatch, tmp_path):
        """groups='respiratory' deve chegar em sus_filter como ['respiratory']."""
        from climasus4py.core import pipeline as mod

        filter_kwargs: dict = {}
        calls: list[str] = []

        def capturing_filter(rel, **kwargs):
            filter_kwargs.update(kwargs)
            calls.append("filter")
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", capturing_filter)
        monkeypatch.setattr(mod, "sus_data_create_variables", _make_passthrough("variables", calls))
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            groups="respiratory",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert filter_kwargs.get("groups") == ["respiratory"]

    def test_groups_list_forwarded_unchanged(self, monkeypatch, tmp_path):
        """groups=['respiratory', 'circulatory'] deve chegar intacto."""
        from climasus4py.core import pipeline as mod

        filter_kwargs: dict = {}
        calls: list[str] = []

        def capturing_filter(rel, **kwargs):
            filter_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", capturing_filter)
        monkeypatch.setattr(mod, "sus_data_create_variables", _make_passthrough("variables", calls))
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            groups=["respiratory", "circulatory"],
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert filter_kwargs.get("groups") == ["respiratory", "circulatory"]

    def test_age_min_max_forwarded_to_filter(self, monkeypatch, tmp_path):
        """age_min e age_max devem chegar em sus_filter."""
        from climasus4py.core import pipeline as mod

        filter_kwargs: dict = {}
        calls: list[str] = []

        def capturing_filter(rel, **kwargs):
            filter_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", capturing_filter)
        monkeypatch.setattr(mod, "sus_data_create_variables", _make_passthrough("variables", calls))
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            age_min=18,
            age_max=65,
            age_group="who",  # forces staged path
            cache_dir=tmp_path,
            verbose=False,
        )

        assert filter_kwargs.get("age_min") == 18
        assert filter_kwargs.get("age_max") == 65

    def test_variables_receives_epi_week(self, monkeypatch, tmp_path):
        """sus_data_create_variables deve receber epi_week=True."""
        from climasus4py.core import pipeline as mod

        variables_kwargs: dict = {}
        calls: list[str] = []

        def capturing_variables(rel, **kwargs):
            variables_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", _make_passthrough("filter", calls))
        monkeypatch.setattr(mod, "sus_data_create_variables", capturing_variables)
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        # Os mocks aceitam **kwargs, então nome de argumento errado não
        # levanta erro aqui -- é assim que o pipeline chamava
        # ``age_group=``/``epi_week=``, que não existem, e quebrava com
        # TypeError só em uso real. Conferir contra a assinatura verdadeira.
        import inspect

        from climasus4py.core.variables import (
            sus_data_create_variables as real_variables,
        )

        inspect.signature(real_variables).bind(object(), **variables_kwargs)

        # A semana epidemiológica sai de create_calendar_vars (ligado por
        # padrão); não existe argumento ``epi_week`` para repassar.
        assert "epi_week" not in variables_kwargs
        assert variables_kwargs.get("lang") == "pt"

    def test_variables_receives_age_breaks_from_age_group(self, monkeypatch, tmp_path):
        """age_group do pipeline vira age_breaks (lista) em create_variables."""
        import inspect

        from climasus4py.core import pipeline as mod
        from climasus4py.core.variables import (
            sus_data_create_variables as real_variables,
        )

        variables_kwargs: dict = {}
        calls: list[str] = []

        def capturing_variables(rel, **kwargs):
            variables_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", _make_passthrough("filter", calls))
        monkeypatch.setattr(mod, "sus_data_create_variables", capturing_variables)
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            age_group=[0, 18, 65],
            cache_dir=tmp_path,
            verbose=False,
        )

        inspect.signature(real_variables).bind(object(), **variables_kwargs)
        assert variables_kwargs.get("age_breaks") == [0, 18, 65]
        assert "age_group" not in variables_kwargs

    def test_aggregate_receives_time_unit(self, monkeypatch, tmp_path):
        """sus_data_aggregate recebe time_unit; nao existe argumento geo."""
        from climasus4py.core import pipeline as mod

        aggregate_kwargs: dict = {}
        calls: list[str] = []

        def capturing_aggregate(rel, **kwargs):
            aggregate_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", _make_passthrough("filter", calls))
        monkeypatch.setattr(mod, "sus_data_create_variables", _make_passthrough("variables", calls))
        monkeypatch.setattr(mod, "sus_data_aggregate", capturing_aggregate)

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            time="year",
            geo="municipality",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        import inspect

        from climasus4py.core.aggregate import sus_data_aggregate as real_aggregate

        inspect.signature(real_aggregate).bind(object(), **aggregate_kwargs)

        assert aggregate_kwargs.get("time_unit") == "year"
        assert "time" not in aggregate_kwargs
        # sus_data_aggregate nao tem argumento geo: detecta a coluna
        # geografica a partir dos dados, ajudada por ``system``.
        assert "geo" not in aggregate_kwargs


# ---------------------------------------------------------------------------
# sus_pipeline — multi-UF e multi-year
# ---------------------------------------------------------------------------

class TestPipelineMultiInput:
    def test_multi_uf_list_passed_to_sus_import(self, monkeypatch, tmp_path):
        """sus_data_import deve receber a lista de UFs sem modificação."""
        from climasus4py.core import pipeline as mod

        received_uf = []
        calls: list[str] = []

        def fake_import(system, uf, year, **kwargs):
            received_uf.append(uf)
            conn = get_connection()
            rel = conn.from_df(_synthetic_sim_do())
            set_stage(rel, "import")
            return rel

        monkeypatch.setattr(mod, "sus_data_import", fake_import)
        _patch_all_stages(monkeypatch, mod, calls)

        mod.sus_pipeline(
            "SIM-DO", ["SP", "RJ"], 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert received_uf == [["SP", "RJ"]]

    def test_multi_year_list_passed_to_sus_import(self, monkeypatch, tmp_path):
        """sus_data_import deve receber lista de anos."""
        from climasus4py.core import pipeline as mod

        received_year = []
        calls: list[str] = []

        def fake_import(system, uf, year, **kwargs):
            received_year.append(year)
            conn = get_connection()
            rel = conn.from_df(_synthetic_sim_do())
            set_stage(rel, "import")
            return rel

        monkeypatch.setattr(mod, "sus_data_import", fake_import)
        _patch_all_stages(monkeypatch, mod, calls)

        mod.sus_pipeline(
            "SIM-DO", "SP", [2020, 2021, 2022],
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert received_year == [[2020, 2021, 2022]]

    def test_single_year_int_passed_to_sus_import(self, monkeypatch, tmp_path):
        """Ano como int deve chegar sem transformação em sus_data_import."""
        from climasus4py.core import pipeline as mod

        received_year = []
        calls: list[str] = []

        def fake_import(system, uf, year, **kwargs):
            received_year.append(year)
            conn = get_connection()
            rel = conn.from_df(_synthetic_sim_do())
            set_stage(rel, "import")
            return rel

        monkeypatch.setattr(mod, "sus_data_import", fake_import)
        _patch_all_stages(monkeypatch, mod, calls)

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert received_year == [2022]

    def test_none_groups_forwarded_as_none(self, monkeypatch, tmp_path):
        """groups=None deve chegar como None em sus_filter."""
        from climasus4py.core import pipeline as mod

        filter_kwargs: dict = {}
        calls: list[str] = []

        def capturing_filter(rel, **kwargs):
            filter_kwargs.update(kwargs)
            return rel

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        monkeypatch.setattr(mod, "sus_data_clean_encoding", _make_passthrough("clean", calls))
        monkeypatch.setattr(mod, "sus_data_standardize", _make_passthrough("standardize", calls))
        monkeypatch.setattr(mod, "sus_filter", capturing_filter)
        monkeypatch.setattr(mod, "sus_data_create_variables", _make_passthrough("variables", calls))
        monkeypatch.setattr(mod, "sus_data_aggregate", _make_passthrough("aggregate", calls))

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            groups=None,
            epi_week=True,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert filter_kwargs.get("groups") is None


# ---------------------------------------------------------------------------
# _date_parse_sql — geração de SQL de parsing de datas
# ---------------------------------------------------------------------------

class TestDateParseSql:
    def test_returns_case_expression(self):
        from climasus4py.core.pipeline import _date_parse_sql
        sql = _date_parse_sql("DTOBITO")
        assert "CASE" in sql.upper()
        assert "DTOBITO" in sql

    def test_includes_ddmmyyyy_format(self):
        from climasus4py.core.pipeline import _date_parse_sql
        sql = _date_parse_sql("DTOBITO")
        assert "%d%m%Y" in sql

    @pytest.mark.parametrize(
        ("literal", "esperado"),
        [
            # DDMMYYYY, o formato cru do DATASUS
            ("'01022023'", "2023-02-01"),
            ("'2023-02-01'", "2023-02-01"),
            # ISO com hora no fim: é assim que uma coluna DATE/TIMESTAMP
            # aparece depois de CAST(... AS VARCHAR). '%Y-%m-%d' rejeitava,
            # zerando toda data já tipada e devolvendo tabela vazia.
            ("'2023-02-01 00:00:00'", "2023-02-01"),
            ("DATE '2023-02-01'", "2023-02-01"),
            ("TIMESTAMP '2023-02-01 03:00:00'", "2023-02-01"),
            # Barra é ordem brasileira: 01/02 é 1º de fevereiro, não 2 de janeiro
            ("'01/02/2023'", "2023-02-01"),
        ],
    )
    def test_parses_every_supported_format(self, literal, esperado):
        from climasus4py.core.engine import get_connection
        from climasus4py.core.pipeline import _date_parse_sql

        sql = _date_parse_sql("x").replace(
            'CAST("x" AS VARCHAR)', f"CAST({literal} AS VARCHAR)"
        )
        obtido = get_connection().sql(f"SELECT {sql} AS d").fetchone()[0]
        assert obtido is not None, f"{literal} nao foi parseado"
        assert obtido.strftime("%Y-%m-%d") == esperado

    def test_includes_br_format(self):
        from climasus4py.core.pipeline import _date_parse_sql
        sql = _date_parse_sql("DATA")
        assert "%d/%m/%Y" in sql

    def test_column_name_quoted_in_sql(self):
        """Nome da coluna deve aparecer entre aspas duplas no SQL."""
        from climasus4py.core.pipeline import _date_parse_sql
        sql = _date_parse_sql("DTOBITO")
        assert '"DTOBITO"' in sql


# ---------------------------------------------------------------------------
# _build_fast_sql — geração de SQL de fast path
# ---------------------------------------------------------------------------

def _make_sim_do_parquet(path: Path, n: int = 3) -> None:
    """Cria parquet mínimo com colunas SIM-DO no formato real (DTOBITO=DDMMYYYY)."""
    df = pd.DataFrame({
        "DTOBITO": ["15012022", "20022022", "05032022"][:n],   # formato real DATASUS
        "CODMUNRES": [355030, 330455, 355030][:n],
        "CAUSABAS": ["J189", "I219", "K920"][:n],
    })
    pq.write_table(pa.Table.from_pandas(df), path)


def _make_sim_do_parquet_typed(path: Path, n: int = 3) -> None:
    """Igual ao anterior, mas com DTOBITO já TIMESTAMP.

    É o formato que o cache de ``sus_data_import`` realmente grava; o fixture
    de string DDMMYYYY acima nunca exercitou esse caso, e por isso o fast path
    passou a devolver zero linha sem que nenhum teste notasse.
    """
    df = pd.DataFrame({
        "DTOBITO": pd.to_datetime(["2022-01-15", "2022-02-20", "2022-03-05"][:n]),
        "CODMUNRES": [355030, 330455, 355030][:n],
        "CAUSABAS": ["J189", "I219", "K920"][:n],
    })
    pq.write_table(pa.Table.from_pandas(df), path)


class TestFastSqlRunsOnTypedDates:
    """Regressão: o fast path precisa devolver linhas, não SQL bem-formado."""

    @pytest.mark.parametrize(
        "fabrica", [_make_sim_do_parquet, _make_sim_do_parquet_typed]
    )
    def test_counts_all_rows(self, tmp_path, fabrica):
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        fabrica(p)

        sql = _build_fast_sql([p], None, None, None, "month", "state")
        assert sql is not None

        df = get_connection().sql(sql).df()
        assert df["count"].sum() == 3, (
            "toda data virou NULL e o WHERE __date IS NOT NULL zerou o resultado"
        )
        assert set(df["state"]) == {"35", "33"}


class TestBuildFastSql:
    def test_returns_sql_with_count(self, tmp_path):
        """_build_fast_sql deve gerar SQL com COUNT(*)."""
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        _make_sim_do_parquet(p)

        sql = _build_fast_sql([p], None, None, None, "month", "state")

        assert sql is not None
        assert "COUNT" in sql.upper()

    def test_returns_sql_with_group_by(self, tmp_path):
        """SQL deve ter GROUP BY."""
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        _make_sim_do_parquet(p)

        sql = _build_fast_sql([p], None, None, None, "month", "state")

        assert sql is not None
        assert "GROUP BY" in sql.upper()

    def test_returns_none_when_no_date_column(self, tmp_path):
        """Retorna None quando parquet não tem coluna de data reconhecida."""
        from climasus4py.core.pipeline import _build_fast_sql

        df = pd.DataFrame({"COL_SEM_DATA": [1, 2, 3], "OUTRO": ["a", "b", "c"]})
        p = tmp_path / "nodate.parquet"
        pq.write_table(pa.Table.from_pandas(df), p)

        sql = _build_fast_sql([p], None, None, None, "month", "state")
        assert sql is None

    def test_year_time_expr_in_sql(self, tmp_path):
        """time='year' deve usar EXTRACT(YEAR ...) no SQL."""
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        _make_sim_do_parquet(p)

        sql = _build_fast_sql([p], None, None, None, "year", "state")

        assert sql is not None
        assert "YEAR" in sql.upper()

    def test_quarter_time_expr_in_sql(self, tmp_path):
        """time='quarter' deve usar QUARTER no SQL."""
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        _make_sim_do_parquet(p)

        sql = _build_fast_sql([p], None, None, None, "quarter", "state")

        assert sql is not None
        assert "QUARTER" in sql.upper()

    def test_age_min_adds_filter(self, tmp_path):
        """age_min deve adicionar filtro >= no SQL."""
        from climasus4py.core.pipeline import _build_fast_sql

        df = pd.DataFrame({
            "DTOBITO": pd.to_datetime(["2022-01-01"]),
            "CODMUNRES": [355030],
            "IDADE": ["420"],
        })
        p = tmp_path / "data.parquet"
        pq.write_table(pa.Table.from_pandas(df), p)

        sql = _build_fast_sql([p], None, 18, None, "month", "state")

        assert sql is not None
        assert ">= 18" in sql

    def test_age_max_adds_filter(self, tmp_path):
        """age_max deve adicionar filtro <= no SQL."""
        from climasus4py.core.pipeline import _build_fast_sql

        df = pd.DataFrame({
            "DTOBITO": pd.to_datetime(["2022-01-01"]),
            "CODMUNRES": [355030],
            "IDADE": ["420"],
        })
        p = tmp_path / "data.parquet"
        pq.write_table(pa.Table.from_pandas(df), p)

        sql = _build_fast_sql([p], None, None, 65, "month", "state")

        assert sql is not None
        assert "<= 65" in sql

    def test_generated_sql_is_executable(self, tmp_path):
        """SQL gerado deve ser executável e retornar resultados."""
        from climasus4py.core.engine import get_connection
        from climasus4py.core.pipeline import _build_fast_sql

        p = tmp_path / "data.parquet"
        _make_sim_do_parquet(p)

        sql = _build_fast_sql([p], None, None, None, "month", "state")
        assert sql is not None

        conn = get_connection()
        result_df = conn.sql(sql).df()
        assert len(result_df) > 0
        assert "count" in result_df.columns


# ---------------------------------------------------------------------------
# sus_pipeline — fast path com parquet real e branch output
# ---------------------------------------------------------------------------

class TestPipelineFastPath:
    def test_fast_path_executes_when_parquet_exists(self, tmp_path, monkeypatch):
        """Fast path deve executar SQL quando parquet existe no cache."""
        from climasus4py.core import pipeline as mod

        df = pd.DataFrame({
            "DTOBITO": ["15012022", "20022022"],   # formato real DATASUS
            "CODMUNRES": [355030, 330455],
        })

        cache_dir = tmp_path / "cache"
        parquet_path = cache_dir / "SIM-DO" / "SP_2022_all.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df), parquet_path)

        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(pd.DataFrame(df)))
        monkeypatch.setattr("climasus4py.utils.data.resolve_uf", lambda uf: ["SP"])

        result = mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            time="month",
            geo="state",
            epi_week=False,
            age_group=None,
            cache_dir=cache_dir,
            verbose=False,
        )

        assert result is not None

    def test_fast_path_fallback_to_staged_when_no_parquet(self, tmp_path, monkeypatch):
        """Sem parquet no cache, staged pipeline deve ser chamado."""
        from climasus4py.core import pipeline as mod

        calls: list[str] = []
        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        _patch_all_stages(monkeypatch, mod, calls)
        monkeypatch.setattr("climasus4py.utils.data.resolve_uf", lambda uf: ["SP"])

        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            time="month",
            geo="state",
            epi_week=False,
            age_group=None,
            cache_dir=tmp_path / "empty_cache",
            verbose=False,
        )

        assert "clean" in calls


class TestPipelineStagedOutput:
    def test_output_param_calls_sus_export(self, monkeypatch, tmp_path):
        """Quando output é definido no staged path, sus_export deve ser chamado."""
        from climasus4py.core import pipeline as mod

        export_calls: list[str] = []

        def mock_export(rel, path, **kwargs):
            export_calls.append(str(path))

        calls: list[str] = []
        monkeypatch.setattr(mod, "sus_data_import", _make_import_mock(_synthetic_sim_do()))
        _patch_all_stages(monkeypatch, mod, calls)
        monkeypatch.setattr(mod, "sus_export", mock_export)

        output_path = tmp_path / "result.parquet"
        mod.sus_pipeline(
            "SIM-DO", "SP", 2022,
            lang="pt",
            epi_week=True,
            output=output_path,
            cache_dir=tmp_path,
            verbose=False,
        )

        assert len(export_calls) == 1
        assert str(output_path) in export_calls[0]
