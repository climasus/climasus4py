"""D6/M96: uma lista de coluna de municipio, com recorte explicito.

Havia CINCO listas, nao as tres que o briefing contava:

1. `climasus-data` -> `role_priority.municipality`   (3 nomes)
2. `detect_geo_column`, cravada no Python            (10 nomes)
3. `climate_aggregate._MUNI_CANDIDATES`              (7 nomes)
4. `plot_aggregate_map._MAP_MUNI_CANDIDATES`         (8 nomes)
5. `plot_aggregate_ts._TS_DATE_CANDIDATES`           (data, 7 nomes)

E divergiam tambem na ORDEM, que e o que decide, porque a primeira que
casa vence. O `detect_date_column` sempre leu do metadado; o de geografia
tinha dicionario proprio -- a assimetria que fez `DT_NOTIFIC` (SINAN) e
`DT_INTER` (SIH) passarem na validacao e morrerem no SQL com
`Binder Error: ... does not have a column named "None"`.

Em 21/09/2026 a D6 fechou a causa. O metadado virou fonte unica, com
`municipality_by_basis` declarando de que recorte e cada nome, e o
`sus_climate_aggregate` ganhou `geo_basis` -- porque residencia e
ocorrencia sao recortes epidemiologicos DIFERENTES e a ordem de uma lista
nao deveria decidir isso em silencio (M21).
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection
from climasus4py.enrichment import climate_aggregate as agg
from climasus4py.utils.data import (
    detect_date_column,
    detect_geo_column,
    load_datasus_columns_spec,
    municipality_candidates,
)

RECORTES = ("residence", "occurrence", "notification", "unspecified")

WKT = ("POLYGON((-46.7 -23.6,-46.6 -23.6,-46.6 -23.5,"
       "-46.7 -23.5,-46.7 -23.6))")


@pytest.fixture
def clima():
    return get_connection().from_df(pd.DataFrame({
        "station_code": ["A"] * 40,
        "date": pd.date_range("2023-01-01", periods=40),
        "latitude": [-23.55] * 40,
        "longitude": [-46.63] * 40,
        "tair_dry_bulb_c": [25.0] * 40,
    }))


def _saude(**cols):
    base = {"DTOBITO": pd.to_datetime(
        ["2023-01-05", "2023-01-12", "2023-02-03"]),
        "geometry_wkt": [WKT] * 3}
    return get_connection().from_df(pd.DataFrame({**cols, **base}))


# --------------------------------------------------------------------------
# A fonte unica
# --------------------------------------------------------------------------

class TestFonteUnica:

    def test_o_metadado_declara_os_recortes(self):
        spec = load_datasus_columns_spec()
        assert int(spec["schema_version"]) >= 3
        porbase = spec["municipality_by_basis"]
        for r in RECORTES:
            assert porbase[r], f"recorte {r} vazio"
        assert porbase["_fallback_order"] == list(RECORTES)

    def test_a_uniao_e_a_concatenacao_dos_recortes(self):
        """`role_priority.municipality` nao pode divergir dos recortes.

        Se divergisse, voltariamos a ter duas listas -- agora dentro do
        mesmo arquivo, que e pior.
        """
        spec = load_datasus_columns_spec()
        porbase = spec["municipality_by_basis"]
        esperado = [n for r in porbase["_fallback_order"] for n in porbase[r]]
        assert spec["role_priority"]["municipality"] == esperado

    def test_nenhum_nome_em_dois_recortes(self):
        porbase = load_datasus_columns_spec()["municipality_by_basis"]
        vistos: dict[str, str] = {}
        for r in RECORTES:
            for n in porbase[r]:
                assert n not in vistos, f"{n} em {r} e em {vistos[n]}"
                vistos[n] = r

    def test_as_cinco_listas_viraram_a_mesma(self):
        """A garantia estrutural da D6.

        Conteudo em acordo e bom; a MESMA chamada e melhor, porque nao
        depende de alguem lembrar de atualizar as duas.
        """
        do_metadado = municipality_candidates()
        assert agg._muni_candidates() == do_metadado
        for nome in do_metadado:
            assert detect_geo_column([nome], level="municipality") == nome

    def test_os_graficos_tambem(self):
        from climasus4py.viz.plot_aggregate_map import _map_detect_muni_col
        from climasus4py.viz.plot_aggregate_ts import _ts_date_candidates

        for nome in municipality_candidates():
            assert _map_detect_muni_col([nome]) == nome

        declaradas = load_datasus_columns_spec()["role_priority"]["date"]
        assert set(declaradas) <= set(_ts_date_candidates())
        # Os dois que so o grafico conhecia seguem cobertos.
        assert {"data", "DT_COMPET"} <= set(_ts_date_candidates())

    def test_o_validador_e_o_detector_nao_podem_discordar(self):
        """O nucleo do M96, agora impossivel por construcao."""
        for nome in agg._muni_candidates():
            assert detect_geo_column([nome], level="municipality") is not None
        for nome in agg._date_candidates():
            assert detect_date_column([nome]) is not None


# --------------------------------------------------------------------------
# O recorte explicito
# --------------------------------------------------------------------------

class TestRecorteExplicito:

    @pytest.mark.parametrize(("basis", "esperado"), [
        (None, "CODMUNRES"),
        ("residence", "CODMUNRES"),
        ("occurrence", "CODMUNOCOR"),
        ("notification", "ID_MUNICIP"),
        ("unspecified", "municipality_code"),
    ])
    def test_escolhe_a_coluna_do_recorte(self, basis, esperado):
        cols = ["CODMUNRES", "CODMUNOCOR", "ID_MUNICIP", "municipality_code"]
        assert detect_geo_column(cols, "municipality", basis=basis) == esperado

    def test_o_default_e_residencia(self):
        """Nao muda comportamento: as duas listas antigas do Python ja
        preferiam residencia. O que muda e a ordem estar DECLARADA."""
        porbase = load_datasus_columns_spec()["municipality_by_basis"]
        assert porbase["_fallback_order"][0] == "residence"
        assert municipality_candidates()[0] == porbase["residence"][0]

    def test_recorte_invalido_diz_os_validos(self):
        with pytest.raises(ValueError, match="Unknown geographic basis"):
            municipality_candidates("umbigo")

    def test_recorte_so_vale_para_municipio(self):
        with pytest.raises(ValueError, match="only applies to"):
            detect_geo_column(["state"], level="state", basis="residence")

    def test_estado_e_regiao_seguem_funcionando(self):
        assert detect_geo_column(["SG_UF"], level="state") == "SG_UF"
        assert detect_geo_column(["region"], level="region") == "region"
        assert detect_geo_column(["country"], level="country") == "country"
        assert detect_geo_column(["nada"], level="municipality") is None


class TestGeoBasisNoAggregate:

    def test_escolhe_o_recorte_pedido(self, clima):
        """Com as duas colunas presentes, o recorte decide.

        Este e o caso que importa: um obito num hospital de referencia e
        contado no municipio de RESIDENCIA do paciente por um recorte e no
        do hospital pelo outro, e para exposicao climatica a resposta
        certa e onde a pessoa MORAVA.
        """
        saude = _saude(CODMUNRES=["3550308"] * 3,
                       CODMUNOCOR=["3509502"] * 3)
        for basis, esperada in (("residence", "CODMUNRES"),
                                ("occurrence", "CODMUNOCOR")):
            tok = agg._GEO_BASIS.set(basis)
            try:
                assert agg._detect_muni(saude.columns) == esperada
            finally:
                agg._GEO_BASIS.reset(tok)

    def test_roda_ponta_a_ponta_com_recorte(self, clima):
        saude = _saude(CODMUNRES=["3550308"] * 3)
        out = cs.sus_climate_aggregate(saude, clima, geo_basis="residence",
                                       verbose=False)
        assert out.count("*").fetchone()[0] == 3

    def test_o_default_segue_igual_ao_de_antes(self, clima):
        """Sem geo_basis, nada muda -- e o que protege quem ja chamava."""
        saude = _saude(CODMUNRES=["3550308"] * 3)
        a = cs.sus_climate_aggregate(saude, clima, verbose=False).df()
        b = cs.sus_climate_aggregate(saude, clima, geo_basis="residence",
                                     verbose=False).df()
        pd.testing.assert_frame_equal(a, b)

    def test_recorte_invalido_falha_antes_do_trabalho(self, clima):
        saude = _saude(CODMUNRES=["3550308"] * 3)
        with pytest.raises(ValueError, match="Unknown geographic basis"):
            cs.sus_climate_aggregate(saude, clima, geo_basis="umbigo",
                                     verbose=False)

    def test_recorte_que_o_dado_nao_tem_nomeia_o_que_falta(self, clima):
        saude = _saude(CODMUNRES=["3550308"] * 3)
        with pytest.raises(ValueError, match="no municipality column"):
            cs.sus_climate_aggregate(saude, clima, geo_basis="occurrence",
                                     verbose=False)

    def test_o_contexto_volta_ao_normal(self, clima):
        """ContextVar e nao global, e tem de ser restaurado.

        Se vazasse, a proxima chamada herdaria o recorte da anterior --
        um efeito colateral entre chamadas que ninguem veria.
        """
        saude = _saude(CODMUNRES=["3550308"] * 3)
        assert agg._GEO_BASIS.get() is None
        cs.sus_climate_aggregate(saude, clima, geo_basis="residence",
                                 verbose=False)
        assert agg._GEO_BASIS.get() is None

    def test_o_contexto_volta_ao_normal_mesmo_com_erro(self, clima):
        saude = _saude(CODMUNRES=["3550308"] * 3)
        with pytest.raises(ValueError):
            cs.sus_climate_aggregate(saude, clima, geo_basis="occurrence",
                                     verbose=False)
        assert agg._GEO_BASIS.get() is None


class TestCompatibilidadeDeMetadado:

    def test_metadado_antigo_avisa_e_usa_as_listas_do_pacote(self,
                                                             monkeypatch):
        """Com climasus-data anterior ao schema 3, o M96 nao pode voltar.

        A lista antiga tinha 3 nomes e nenhum recorte: `code_muni` e
        `MUNI_RES` voltariam a passar na validacao e a nao ser detectados.
        """
        from climasus4py.utils import data as mod

        antigo = {
            "schema_version": 2,
            "all_date_columns": ["DTOBITO"],
            "all_numeric_columns": ["CONTADOR"],
            "all_identifier_columns": {"CODESTAB": 7},
            "all_categorical_columns": ["LOCOCOR"],
            "system_signatures": {},
            "role_priority": {
                "date": ["DTOBITO"],
                "municipality": ["municipality_code", "CODMUNRES",
                                 "ID_MUNICIP"],
            },
        }
        monkeypatch.setattr(mod, "load_json", lambda _p: antigo)
        mod.load_datasus_columns_spec.cache_clear()
        try:
            with pytest.warns(UserWarning, match="municipality_by_basis"):
                spec = mod.load_datasus_columns_spec()
            # Compared against the bundled list, not a literal count.
            # It used to say `== 11`, and the number went stale the day
            # the M123 translations were added — a magic number that
            # fails for a change that was correct.
            assert (spec["role_priority"]["municipality"]
                    == mod._FALLBACK_DATASUS_COLUMNS["role_priority"]["municipality"])
            assert spec["municipality_by_basis"]["occurrence"]
            assert mod.detect_geo_column(["MUNI_RES"], "municipality") \
                == "MUNI_RES"
        finally:
            mod.load_datasus_columns_spec.cache_clear()


# ---------------------------------------------------------------------------
# M11 (a) — a divergencia da chuva deixou de ser silenciosa
# ---------------------------------------------------------------------------

class TestDivergenciaDaChuvaAvisa:
    """A divergencia era DELIBERADA e SILENCIOSA; agora so deliberada.

    Decidida em 09/09/2026 a favor deste pacote: precipitacao acumula,
    entao somar a janela de 14 dias responde a pergunta epidemiologica
    usual, e media diaria de chuva na janela nao responde nada em
    particular. O que ficou aberto foi o silencio -- a coluna sai
    `rainfall_mm_sum_w14` em vez de `rainfall_mm_mean_w14` e nenhum dos
    dois pacotes dizia nada.

    E o terreno e mais firme do que o registro dizia. O R NAO "faz
    media": o R se contradiz, que e a mesma forma de prova do M69.
    Conferido no climasus4r instalado:

    * `.build_agg_rules` declara `rainfall_mm ~ "sum"` -- regra identica
      a deste modulo;
    * `.join_window`, `.join_lag`, `.join_offset_window`,
      `.join_weighted_window`, `.join_discrete_lag` e
      `.join_distributed_lag` consultam essa regra e a honram;
    * `.join_moving_window` NAO MENCIONA `agg_rule` nem `agg_type`. Faz
      `avg = mean(value, na.rm = TRUE)` sem condicao e nomeia a coluna
      `paste0(var, "_mean_w", window_days)`.

    Uma das sete funcoes de janela do R ignora a regra do proprio R, e
    este pacote concorda com as outras seis.
    """

    @staticmethod
    def _clima(com_chuva=True):
        cols = {"station_code": ["A"] * 90,
                "date": pd.date_range("2023-01-01", periods=90),
                "latitude": [-23.55] * 90, "longitude": [-46.63] * 90,
                "tair_dry_bulb_c": [25.0] * 90}
        if com_chuva:
            cols["rainfall_mm"] = [5.0] * 90
        return get_connection().from_df(pd.DataFrame(cols))

    @classmethod
    def _chama(cls, estrategia, com_chuva=True, limpar=True, **kw):
        from climasus4py.enrichment import climate_aggregate as agg

        if limpar:
            agg._AVISOU_MW.clear()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cs.sus_climate_aggregate(
                _saude(CODMUNRES=["3550308"] * 3), cls._clima(com_chuva),
                temporal_strategy=estrategia, verbose=False, **kw)
        return [str(x.message) for x in w if "M11" in str(x.message)]

    def test_moving_window_com_chuva_avisa(self):
        msgs = self._chama("moving_window", window_days=14)
        assert len(msgs) == 1
        assert "_sum_w" in msgs[0] and "_mean_w" in msgs[0]
        assert "DELIBERATE" in msgs[0]

    def test_o_aviso_diz_que_o_R_se_contradiz(self):
        """Nao basta avisar: o aviso tem de dizer por que estamos certos."""
        msgs = self._chama("moving_window", window_days=14)
        assert "six of its seven window functions" in msgs[0]
        assert ".join_moving_window" in msgs[0]
        assert "60.35" in msgs[0] and "4.11" in msgs[0]

    def test_avisa_uma_vez_por_processo(self):
        """Aviso repetido em laco e ruido, e ruido esconde aviso de verdade."""
        from climasus4py.enrichment import climate_aggregate as agg

        primeiro = self._chama("moving_window", window_days=14)
        segundo = self._chama("moving_window", limpar=False, window_days=14)
        assert len(primeiro) == 1
        assert segundo == []
        agg._AVISOU_MW.clear()

    def test_sem_variavel_acumulavel_nao_avisa(self):
        assert self._chama("moving_window", com_chuva=False,
                           window_days=14) == []

    def test_estrategia_que_nao_diverge_nao_avisa(self):
        """Avisar onde nao ha divergencia gastaria a atencao a toa."""
        assert self._chama("exact") == []

    def test_a_regra_por_variavel_e_a_mesma_do_R(self):
        """`c("rainfall_mm", "sr_kj_m2") ~ "sum"`, byte a byte.

        Se alguem mexer nesta lista, a divergencia deixa de ser a que o
        aviso descreve.
        """
        from climasus4py.enrichment.climate_aggregate import (
            _CIRC_VARS,
            _SUM_VARS,
            _build_agg_rules,
        )

        assert _SUM_VARS == {"rainfall_mm", "sr_kj_m2"}
        assert _CIRC_VARS == {"wd_degrees"}
        assert _build_agg_rules(
            ["rainfall_mm", "sr_kj_m2", "wd_degrees", "tair_dry_bulb_c"]
        ) == {"rainfall_mm": "sum", "sr_kj_m2": "sum",
              "wd_degrees": "mean_circular", "tair_dry_bulb_c": "mean"}

    def test_a_coluna_sai_somada_de_fato(self):
        """O aviso descreve o que a saida faz, e nao o contrario.

        A data de obito e de FEVEREIRO de proposito: a `_saude` deste
        modulo usa 5 de janeiro, e com o clima comecando em 1 de janeiro
        nao ha 14 dias anteriores, entao o `min_obs` nao e atingido e a
        coluna sai NaN. A primeira versao deste teste caiu nisso -- e o
        NaN e comportamento correto, nao defeito.
        """
        saude = get_connection().from_df(pd.DataFrame({
            "CODMUNRES": ["3550308"] * 2,
            "DTOBITO": pd.to_datetime(["2023-02-15", "2023-02-20"]),
            "geometry_wkt": [WKT] * 2}))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out = cs.sus_climate_aggregate(
                saude, self._clima(), temporal_strategy="moving_window",
                window_days=14, verbose=False).df()
        assert "rainfall_mm_sum_w14" in out.columns
        assert out["rainfall_mm_sum_w14"].iloc[0] == pytest.approx(75.0), (
            "5 mm/dia numa janela de 14+1 dias: soma 75, media daria 5")
