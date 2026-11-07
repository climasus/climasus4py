"""sus_mod_plot_spacetime: os cinco paineis de um ajuste espaco-temporal.

Fixtures: dois ajustes REAIS do INLA 26.8.7 sobre 42 municipios x 8
periodos (BYM2 + RW1, Poisson com offset), um sem interacao e um com
interacao tipo II. As referencias sao os data frames que o ggplot do R
de fato monta em cada painel.

Quatro defeitos do R ficam registrados aqui -- M91, M92, M93 e M94 --
dos quais dois sao replicados e dois corrigidos, cada um atras de uma
chave de modulo.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.viz import mod_plot_spacetime as mps
from climasus4py.viz.mod_plot_spacetime import _BREWER_ENDS, VALID_TYPES

FIX = Path(__file__).parent / "fixtures" / "spacetime"
FIX_GEO = Path(__file__).parent / "fixtures" / "spatial_scan"


@pytest.fixture(scope="module")
def municipios() -> gpd.GeoDataFrame:
    return gpd.read_file(FIX_GEO / "scan_muni.gpkg", layer="muni")


@pytest.fixture(scope="module")
def ajuste() -> dict:
    return {
        "rr": pd.read_parquet(FIX / "st_rr.parquet"),
        "fixed": pd.read_parquet(FIX / "st_fixed.parquet"),
        "spatial_re": pd.read_parquet(FIX / "st_spatial_re.parquet"),
        "temporal_re": pd.read_parquet(FIX / "st_temporal_re.parquet"),
    }


@pytest.fixture(scope="module")
def com_interacao() -> dict:
    return {
        "rr": pd.read_parquet(FIX / "st_rr_ia.parquet"),
        "interaction_re": pd.read_parquet(FIX / "st_interaction_re.parquet"),
    }


@pytest.fixture(scope="module")
def excedencia(ajuste) -> dict:
    return cs.sus_mod_spacetime_exceedance({"rr": ajuste["rr"]}, verbose=False)


class TestPainelTemporal:
    def test_paridade_com_o_r(self, ajuste):
        got = cs.sus_mod_plot_spacetime(ajuste, type="temporal").data
        ref = pd.read_parquet(FIX / "pst_temporal.parquet")

        assert len(got) == len(ref) == 8
        for col in ("time_idx", "rr_mean", "rr_lower95", "rr_upper95"):
            assert np.abs(got[col].to_numpy(float)
                          - ref[col].to_numpy(float)).max() < 1e-12

    def test_time_range_filtra(self, ajuste):
        got = cs.sus_mod_plot_spacetime(
            ajuste, type="temporal", time_range=(3, 6)
        ).data
        ref = pd.read_parquet(FIX / "pst_temporal_range.parquet")

        assert len(got) == len(ref) == 4
        assert got["time_idx"].tolist() == [3, 4, 5, 6]

    def test_a_tendencia_sobe(self, ajuste):
        """O excesso plantado cresce no tempo, entao o RR medio sobe."""
        d = cs.sus_mod_plot_spacetime(ajuste, type="temporal").data

        assert d["rr_mean"].iloc[-1] > d["rr_mean"].iloc[0]
        assert d["rr_mean"].is_monotonic_increasing or \
            d["rr_mean"].iloc[7] > d["rr_mean"].iloc[5]

    def test_dispensa_geometria(self, ajuste):
        assert cs.sus_mod_plot_spacetime(ajuste, type="temporal") is not None


class TestPainelRrMap:
    def test_paridade_agregada(self, ajuste, municipios):
        got = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios
        ).data.sort_values("code_muni")
        ref = pd.read_parquet(FIX / "pst_rrmap.parquet").sort_values("code_muni")

        for col in ("rr_mean", "rr_lower95", "rr_upper95"):
            assert np.abs(got[col].to_numpy(float)
                          - ref[col].to_numpy(float)).max() < 1e-12
        assert int(got["sig_elevated"].sum()) == int(ref["sig_elevated"].sum()) == 4

    def test_paridade_num_periodo(self, ajuste, municipios):
        got = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios, time_point=3
        ).data.sort_values("code_muni")
        ref = pd.read_parquet(FIX / "pst_rrmap_t3.parquet").sort_values("code_muni")

        assert np.abs(got["rr_mean"].to_numpy(float)
                      - ref["rr_mean"].to_numpy(float)).max() < 1e-12
        assert int(got["sig_elevated"].sum()) == 4

    def test_o_titulo_nomeia_o_periodo(self, ajuste, municipios):
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios, time_point=3
        )

        assert "periodo 3" in p.labels.title

    def test_facet_time_mantem_todas_as_celulas(self, ajuste, municipios):
        """Sem facet o R agrega para 42 linhas; com facet mantem 42x8."""
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios, facet_time=True
        )

        assert len(p.data) == 42 * 8

    def test_facet_time_ignora_time_point(self, ajuste, municipios):
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios,
            time_point=3, facet_time=True,
        )

        assert len(p.data) == 42 * 8

    def test_a_marca_e_unilateral(self, ajuste, municipios):
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios
        )
        reduzidos = p.data["rr_upper95"] < 1

        assert not p.data.loc[reduzidos, "sig_elevated"].any()


class TestPaletaInvertida:
    """No rr_map o risco ALTO sai azul e o BAIXO vermelho (M93).

    O R passa brewer.pal(11, palette)[1] como extremo BAIXO da escala e
    [11] como ALTO. Para a RdYlBu padrao isso e #A50026 (vermelho
    escuro) no risco baixo e #313695 (azul escuro) no risco alto --
    invertido em relacao aos dois paineis irmaos do proprio pacote: o de
    excedencia vai de branco a vermelho escuro conforme a probabilidade
    sobe, e o sus_mod_plot_spatial_bayes vai de navy a red3 conforme o
    risco sobe.

    Confirmado na saida renderizada do R: a escala reporta
    low=#A50026 / high=#313695, e o municipio de RR mais alto (1,879) e
    pintado de #313695.

    Replicado por padrao; INVERT_RR_PALETTE=False troca os extremos.
    """

    def test_os_extremos_da_rdylbu(self):
        assert _BREWER_ENDS["RdYlBu"] == ("#A50026", "#313695")

    @staticmethod
    def _extremos(p) -> tuple[str, str]:
        """Cores que a escala de fato atribui aos extremos.

        Os `low`/`high` passados ficam no closure da paleta, nao em
        atributos do objeto (`escala.low` devolve o default da classe),
        entao le-se pelo `map()`. Treinada em (0, 2) com ponto medio 1,
        a reescala manda 0 para o extremo baixo e 2 para o alto.
        """
        escala = [s for s in p.scales if "fill" in s.aesthetics][0]
        escala.train(pd.Series([0.0, 2.0]))
        baixo, alto = escala.map(pd.Series([0.0, 2.0]))
        return str(baixo).upper(), str(alto).upper()

    def test_por_padrao_alto_e_azul(self, ajuste, municipios):
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios
        )
        baixo, alto = self._extremos(p)

        assert baixo == "#A50026"      # risco BAIXO -> vermelho escuro
        assert alto == "#313695"       # risco ALTO  -> azul escuro

    def test_a_chave_troca_os_extremos(self, ajuste, municipios, monkeypatch):
        monkeypatch.setattr(mps, "INVERT_RR_PALETTE", False)
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios
        )
        baixo, alto = self._extremos(p)

        assert baixo == "#313695"
        assert alto == "#A50026"

    def test_os_irmaos_vao_no_sentido_certo(self):
        """Prova de que a inversao e especifica deste painel."""
        from climasus4py.viz.mod_plot_spacetime import _EXC_COLORS
        from climasus4py.viz.mod_plot_spatial_bayes import _HIGH, _LOW

        assert _EXC_COLORS[0] == "#FFFFFF" and _EXC_COLORS[-1] == "#a50026"
        assert _LOW == "#000080" and _HIGH == "#CD0000"

    @pytest.mark.parametrize("nome", sorted(_BREWER_ENDS))
    def test_as_oito_paletas_divergentes(self, ajuste, municipios, nome):
        p = cs.sus_mod_plot_spacetime(
            ajuste, type="rr_map", municipalities=municipios, palette=nome
        )

        assert p is not None

    def test_recusa_paleta_desconhecida(self, ajuste, municipios):
        with pytest.raises(ValueError, match="[Pp]aleta|[Pp]alette"):
            cs.sus_mod_plot_spacetime(
                ajuste, type="rr_map", municipalities=municipios, palette="Viridis"
            )


class TestRotulosOrfaos:
    """Em R todo rotulo sai como a propria chave interna (M92).

    O `.stl` procura os rotulos numa tabela que nao os contem
    (`.st_msgs`), e quando nao acha devolve a chave. A tabela certa --
    `.st_plot_labels`, 32 chaves em pt/en/es -- existe no pacote e
    NENHUMA funcao a le. Verificado nos dois sentidos: a chave
    'rr_map_title' nao esta em `.st_msgs`, esta em `.st_plot_labels`, e
    o grep por `.st_plot_labels` no corpo de todas as funcoes do pacote
    nao retorna nada.

    Consequencia no R: os titulos saem literalmente 'rr_map_title',
    'temporal_title', 'coef_x', e o parametro `lang` nao tem efeito
    nenhum nesta funcao.

    Esta porta usa a tabela pretendida; USE_INTENDED_LABELS=False
    reproduz o comportamento do R.
    """

    def test_por_padrao_os_rotulos_sao_de_verdade(self, ajuste):
        p = cs.sus_mod_plot_spacetime(ajuste, type="temporal")

        assert p.labels.title == "Evolucao Temporal do Risco Relativo"
        assert p.labels.x == "Indice de Tempo"

    def test_a_chave_reproduz_o_r(self, ajuste, monkeypatch):
        monkeypatch.setattr(mps, "USE_INTENDED_LABELS", False)
        p = cs.sus_mod_plot_spacetime(ajuste, type="temporal")

        assert p.labels.title == "temporal_title"
        assert p.labels.x == "x_time"
        assert p.labels.y == "y_rr"

    def test_os_erros_seguem_legiveis_com_a_chave_desligada(self, ajuste, monkeypatch):
        """Mensagem de erro nao e rotulo de grafico: continua traduzida."""
        monkeypatch.setattr(mps, "USE_INTENDED_LABELS", False)
        with pytest.raises(ValueError, match="municipalities"):
            cs.sus_mod_plot_spacetime(ajuste, type="rr_map")

    @pytest.mark.parametrize(("lang", "esperado"), [
        ("pt", "Evolucao Temporal do Risco Relativo"),
        ("en", "Temporal Trend of Relative Risk"),
        ("es", "Evolucion Temporal del Riesgo Relativo"),
    ])
    def test_o_lang_funciona_aqui(self, ajuste, lang, esperado):
        """Em R este parametro nao muda nada nesta funcao."""
        p = cs.sus_mod_plot_spacetime(ajuste, type="temporal", lang=lang)

        assert p.labels.title == esperado


class TestPainelExcedencia:
    """Em R este painel SEMPRE aborta (M94).

    `.st_plot_exceedance` le uma coluna chamada `exceedance_prob`. Esse
    nome aparece em exatamente um lugar no pacote -- o proprio painel --
    e nenhuma funcao o escreve, entao o painel falha com "objeto
    'exceedance_prob' nao encontrado" para qualquer entrada. O
    sus_mod_spacetime_exceedance produz `p_gt_1`, `p_gt_1_5`, ...

    Esta porta seleciona `p_gt_<limiar>`, que e o que a propria legenda
    do painel descreve ("P(RR > {thr})").
    """

    def test_funciona_com_a_saida_da_excedencia(self, excedencia, municipios):
        p = cs.sus_mod_plot_spacetime(
            excedencia, type="exceedance", municipalities=municipios
        )

        assert "p_gt_1" in p.data.columns
        assert len(p.data) == 42          # uma media por municipio

    def test_media_sobre_os_periodos(self, excedencia, municipios):
        p = cs.sus_mod_plot_spacetime(
            excedencia, type="exceedance", municipalities=municipios
        )
        esperado = (
            excedencia["exceedance"].groupby("code_muni")["p_gt_1"].mean()
        )
        got = p.data.set_index("code_muni")["p_gt_1"]

        assert np.abs(got.loc[esperado.index].to_numpy()
                      - esperado.to_numpy()).max() < 1e-12

    def test_o_limiar_escolhe_a_coluna(self, excedencia, municipios):
        p = cs.sus_mod_plot_spacetime(
            excedencia, type="exceedance", municipalities=municipios,
            threshold=1.5,
        )

        assert "p_gt_1_5" in p.data.columns
        assert "P(RR > 1.5)" in p.labels.title

    def test_limiar_sem_coluna_avisa_quais_existem(self, excedencia, municipios):
        with pytest.raises(ValueError, match="p_gt_1"):
            cs.sus_mod_plot_spacetime(
                excedencia, type="exceedance", municipalities=municipios,
                threshold=7,
            )

    def test_a_chave_volta_a_exigir_exceedance_prob(
        self, excedencia, municipios, monkeypatch
    ):
        """Com a chave desligada, falha como o R falha."""
        monkeypatch.setattr(mps, "EXCEEDANCE_COLUMN_FROM_THRESHOLD", False)
        with pytest.raises(ValueError):
            cs.sus_mod_plot_spacetime(
                excedencia, type="exceedance", municipalities=municipios
            )

    def test_recusa_um_ajuste_no_lugar(self, ajuste, municipios):
        """O fit nao tem 'exceedance': o R aborta aqui tambem."""
        with pytest.raises(ValueError, match="sus_mod_spacetime_exceedance"):
            cs.sus_mod_plot_spacetime(
                ajuste, type="exceedance", municipalities=municipios
            )


class TestPainelInteracao:
    """A tabela de interacao vem mal rotulada do ajustador (M91).

    Medido no ajuste real tipo II: 2688 linhas para 336 celulas
    municipio-periodo -- 328 celulas aparecendo uma vez e 8 celulas,
    todas de UM municipio, aparecendo 295 vezes cada, com 295 valores
    DIFERENTES em cada. Ou seja 2360 dos 2688 valores estao atribuidos
    a um unico municipio: os rotulos estao desalinhados de um vetor de
    nos de interacao mais longo.

    O defeito e a montante; este painel desenha o que recebe, e a
    paridade com o R e exata inclusive na ordenacao das linhas.
    """

    def test_paridade_de_linhas_e_ordem(self, com_interacao):
        p = cs.sus_mod_plot_spacetime(com_interacao, type="interaction")
        ref = pd.read_parquet(FIX / "pst_interaction.parquet")

        assert len(p.data) == len(ref) == 2688
        assert list(p.data["code_muni"].cat.categories[:5]) == \
            ["3500004", "3504004", "3502001", "3500002", "3504002"]
        assert p.data["code_muni"].cat.categories[-1] == "3505004"

    def test_a_tabela_esta_mal_rotulada(self, com_interacao):
        """A medicao que sustenta o M91."""
        ia = com_interacao["interaction_re"]
        por_celula = ia.groupby(["code_muni", "time_idx"]).size()

        assert len(ia) == 2688
        assert len(por_celula) == 336
        assert sorted(por_celula.value_counts().to_dict().items()) == \
            [(1, 328), (295, 8)]
        repetidas = por_celula[por_celula > 1]
        assert len({i[0] for i in repetidas.index}) == 1      # um municipio so

    def test_os_valores_repetidos_sao_distintos(self, com_interacao):
        """Nao e duplicacao de um valor: sao 295 efeitos diferentes."""
        ia = com_interacao["interaction_re"]
        alvo = ia.groupby(["code_muni", "time_idx"]).size().idxmax()
        sub = ia[(ia["code_muni"] == alvo[0]) & (ia["time_idx"] == alvo[1])]

        assert len(sub) == 295
        assert sub["gamma_mean"].nunique() == 295

    def test_recusa_ajuste_sem_interacao(self, ajuste):
        with pytest.raises(ValueError, match="interaction"):
            cs.sus_mod_plot_spacetime(ajuste, type="interaction")

    def test_aceita_o_nome_curto(self, com_interacao):
        """O R chega nessa tabela por casamento parcial do $; aqui os
        dois nomes funcionam."""
        curto = {"interaction": com_interacao["interaction_re"]}

        assert cs.sus_mod_plot_spacetime(curto, type="interaction") is not None

    def test_time_range_filtra(self, com_interacao):
        p = cs.sus_mod_plot_spacetime(
            com_interacao, type="interaction", time_range=(2, 4)
        )

        assert set(p.data["time_idx"].unique()) == {2, 3, 4}


class TestPainelCoef:
    def test_ordena_por_media(self, ajuste):
        p = cs.sus_mod_plot_spacetime(ajuste, type="coef")

        assert list(p.data["term"].cat.categories) == ["temp_media", "(Intercept)"]

    def test_os_valores(self, ajuste):
        d = cs.sus_mod_plot_spacetime(ajuste, type="coef").data.set_index("term")

        assert d.loc["temp_media", "mean"] == pytest.approx(-0.02365516, abs=1e-7)
        assert d.loc["(Intercept)", "mean"] == pytest.approx(0.59153507, abs=1e-7)

    def test_recusa_sem_efeitos_fixos(self, ajuste):
        with pytest.raises(ValueError, match="[Nn]enhum efeito fixo"):
            cs.sus_mod_plot_spacetime({"fixed": pd.DataFrame()}, type="coef")


class TestEntradas:
    def test_recusa_x_que_nao_e_dict(self, municipios):
        with pytest.raises(TypeError, match="sus_mod_spacetime"):
            cs.sus_mod_plot_spacetime([1, 2], type="temporal")

    def test_recusa_type_invalido(self, ajuste):
        with pytest.raises(ValueError, match="invalido|invalid"):
            cs.sus_mod_plot_spacetime(ajuste, type="mapa")

    @pytest.mark.parametrize("tipo", ["rr_map", "exceedance"])
    def test_os_mapas_exigem_geometria(self, excedencia, tipo):
        with pytest.raises(ValueError, match="municipalities"):
            cs.sus_mod_plot_spacetime(excedencia, type=tipo)

    def test_recusa_dataframe_sem_geometria(self, ajuste, municipios):
        plano = pd.DataFrame({"code_muni": municipios["code_muni"]})
        with pytest.raises(TypeError, match="GeoDataFrame"):
            cs.sus_mod_plot_spacetime(ajuste, type="rr_map", municipalities=plano)

    def test_recusa_sem_rr(self, municipios):
        with pytest.raises(ValueError, match="'rr'"):
            cs.sus_mod_plot_spacetime({"fixed": pd.DataFrame({"a": [1]})},
                                      type="temporal")

    def test_time_range_fora_de_contexto_avisa(self, ajuste, municipios):
        with pytest.warns(UserWarning, match="time_range"):
            cs.sus_mod_plot_spacetime(
                ajuste, type="rr_map", municipalities=municipios,
                time_range=(2, 5),
            )

    def test_lingua_desconhecida_avisa(self, ajuste):
        with pytest.warns(UserWarning, match="nao suportado"):
            cs.sus_mod_plot_spacetime(ajuste, type="temporal", lang="tupi")

    def test_interactive_avisa_e_devolve_estatico(self, ajuste):
        from plotnine import ggplot

        with pytest.warns(UserWarning, match="plotly"):
            p = cs.sus_mod_plot_spacetime(ajuste, type="temporal", interactive=True)

        assert isinstance(p, ggplot)

    def test_aceita_e_ignora_kwargs(self, ajuste):
        assert cs.sus_mod_plot_spacetime(ajuste, type="temporal", algo=1) is not None

    def test_os_cinco_tipos(self):
        assert VALID_TYPES == (
            "rr_map", "temporal", "interaction", "exceedance", "coef",
        )


class TestRenderizacao:
    @pytest.mark.parametrize("tipo", ["rr_map", "temporal", "coef"])
    def test_desenha_sem_erro(self, ajuste, municipios, tipo, tmp_path):
        p = cs.sus_mod_plot_spacetime(ajuste, type=tipo, municipalities=municipios)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p.save(tmp_path / f"s_{tipo}.png", width=5, height=4, dpi=60,
                   verbose=False)

        assert (tmp_path / f"s_{tipo}.png").exists()

    def test_desenha_a_excedencia(self, excedencia, municipios, tmp_path):
        p = cs.sus_mod_plot_spacetime(
            excedencia, type="exceedance", municipalities=municipios
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p.save(tmp_path / "s_exc.png", width=5, height=4, dpi=60, verbose=False)

        assert (tmp_path / "s_exc.png").exists()
