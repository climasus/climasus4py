"""Tests para sus_mod_plot_pool -- graficos do agrupamento multi-cidade.

Nao se compara o OBJETO grafico contra o R (o plotly nao esta instalado e
um ggplot nao e comparavel a um plotnine de qualquer forma). O que se
testa e o que de fato pode sair errado sem ninguem notar: qual tabela
alimenta cada tipo, a ordenacao do forest, o recuo do BLUP, e os dois
avisos que separam um grafico legivel de um grafico enganoso.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.enrichment.mod_pool import sus_mod_pool
from climasus4py.viz.mod_plot_pool import _blup_curves, sus_mod_plot_pool

pytest.importorskip("statsmodels")
pytest.importorskip("plotnine")

LAG = 4
N_CIDADES = 5


@pytest.fixture(scope="module")
def pool():
    """Agrupamento de cinco cidades com a MESMA exposicao (base compartilhada)."""
    dias = pd.date_range("2018-01-01", "2021-12-31", freq="D")
    base = np.random.default_rng(1)
    temp = (24 + 6 * np.sin(2 * np.pi * (dias.dayofyear - 20) / 365)
            + base.normal(0, 2.0, len(dias)))

    def cidade(seed: int, beta: float):
        r = np.random.default_rng(seed)
        risco = 18 * (1 + beta * np.clip(temp - 26, 0, None)
                      + 0.02 * np.clip(18 - temp, 0, None))
        df = pd.DataFrame({"date": dias, "n_obitos": r.poisson(risco)})
        s = pd.Series(temp)
        for lag in range(LAG + 1):
            df[f"tair_dry_bulb_c_lag{lag}"] = s.shift(lag).values
        return cs.sus_mod_dlnm(
            df.dropna().reset_index(drop=True), outcome_col="n_obitos",
            lag_max=LAG, argvar={"fun": "ns", "df": 3},
            arglag={"fun": "ns", "df": 2}, verbose=False,
        )

    fits = {f"cidade_{i}": cidade(60 + i, 0.004 + 0.016 * i)
            for i in range(N_CIDADES)}
    return sus_mod_pool(fits, n_grid=40, verbose=False, method="reml")


@pytest.fixture(scope="module")
def pool_sem_blup(pool):
    """O mesmo agrupamento, com os BLUPs removidos."""
    return {**pool, "blup_preds": None,
            "city_table": pool["city_table"].assign(
                blup_rr=np.nan, blup_lo=np.nan, blup_hi=np.nan)}


# ---------------------------------------------------------------------------
# Validacao
# ---------------------------------------------------------------------------

class TestValidacao:
    def test_entrada_que_nao_e_saida_de_pool(self):
        with pytest.raises(TypeError, match="sus_mod_pool"):
            sus_mod_plot_pool({"nada": 1})

    def test_tipo_desconhecido(self, pool):
        with pytest.raises(ValueError, match="overall"):
            sus_mod_plot_pool(pool, type="violino")

    def test_output_type_desconhecido(self, pool):
        with pytest.raises(ValueError, match="output_type"):
            sus_mod_plot_pool(pool, output_type="latex")

    def test_interactive_levanta_import_error(self, pool):
        """plotly nao e empacotado; melhor recusar que devolver estatico calado."""
        with pytest.raises(ImportError, match="plotly"):
            sus_mod_plot_pool(pool, interactive=True)

    def test_curva_vazia(self, pool):
        vazio = {**pool, "exposure_curve": pd.DataFrame()}
        with pytest.raises(ValueError, match="curva agrupada"):
            sus_mod_plot_pool(vazio, type="overall")

    def test_city_table_vazia(self, pool):
        vazio = {**pool, "city_table": pd.DataFrame()}
        with pytest.raises(ValueError, match="por cidade"):
            sus_mod_plot_pool(vazio, type="forest")

    def test_idioma_invalido_cai_para_pt(self, pool, capsys):
        sus_mod_plot_pool(pool, lang="fr")
        assert "nao suportado" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Os tres tipos
# ---------------------------------------------------------------------------

class TestTipos:
    @pytest.mark.parametrize("tipo", ["overall", "forest", "spaghetti"])
    def test_devolve_um_ggplot(self, pool, tipo):
        from plotnine import ggplot

        assert isinstance(sus_mod_plot_pool(pool, type=tipo), ggplot)

    @pytest.mark.parametrize("lang", ["pt", "en", "es"])
    def test_os_tres_idiomas_desenham(self, pool, lang):
        for tipo in ("overall", "forest", "spaghetti"):
            assert sus_mod_plot_pool(pool, type=tipo, lang=lang) is not None

    def test_salva_arquivo(self, pool, tmp_path):
        alvo = tmp_path / "forest.png"
        sus_mod_plot_pool(pool, type="forest", save_plot=str(alvo))

        assert alvo.exists()
        assert alvo.stat().st_size > 5000


# ---------------------------------------------------------------------------
# Forest
# ---------------------------------------------------------------------------

class TestForest:
    def test_ordenado_por_rr_com_o_menor_primeiro(self, pool):
        """O R ordena por rr e o primeiro nivel vai para BAIXO no eixo y.

        Ordenar deixa o gradiente de efeito legivel; sem isso as cidades
        saem na ordem em que os ajustes chegaram, que nao diz nada.
        """
        p = sus_mod_plot_pool(pool, type="forest")
        niveis = list(p.data["city"].cat.categories)
        esperado = list(pool["city_table"].sort_values("rr")["city"])

        assert niveis == esperado

    def test_cidade_e_categorica_ordenada(self, pool):
        """Sem categoria ORDENADA o plotnine reordena alfabeticamente."""
        p = sus_mod_plot_pool(pool, type="forest")

        assert isinstance(p.data["city"].dtype, pd.CategoricalDtype)
        assert p.data["city"].cat.ordered

    def test_sem_blup_o_forest_ainda_desenha(self, pool_sem_blup):
        """Sem BLUP o forest perde uma camada, mas nao a razao de existir."""
        assert sus_mod_plot_pool(pool_sem_blup, type="forest") is not None


# ---------------------------------------------------------------------------
# Spaghetti e o recuo para overall
# ---------------------------------------------------------------------------

class TestSpaghetti:
    def test_uma_curva_por_cidade(self, pool):
        curvas = _blup_curves(pool)

        assert set(curvas["city"]) == set(pool["meta"]["city_names"])
        assert len(curvas) == N_CIDADES * len(pool["meta"]["expo_grid"])

    def test_sem_blup_recua_para_overall_com_aviso(self, pool_sem_blup, capsys):
        """Igual ao R: avisa e mostra a curva agrupada, em vez de quebrar."""
        p = sus_mod_plot_pool(pool_sem_blup, type="spaghetti")

        assert "Sem BLUPs" in capsys.readouterr().out
        assert p is not None

    def test_o_recuo_nao_troca_a_tabela_pedida(self, pool_sem_blup):
        """O grafico recua para overall, mas quem pediu spaghetti pediu city_table.

        O R devolve exposure_curve so para type='overall'; o recuo muda o
        desenho, nao a pergunta. Dai a tabela sair de `type` e nao do tipo
        efetivamente desenhado.
        """
        tabela = sus_mod_plot_pool(pool_sem_blup, type="spaghetti",
                                   output_type="table")

        assert "city" in tabela.columns
        assert "exposure" not in tabela.columns

    def test_blup_curves_vazio_quando_nao_ha_preds(self, pool_sem_blup):
        assert _blup_curves(pool_sem_blup).empty


# ---------------------------------------------------------------------------
# output_type
# ---------------------------------------------------------------------------

class TestSaida:
    def test_overall_devolve_a_curva(self, pool):
        tabela = sus_mod_plot_pool(pool, type="overall", output_type="table")
        assert list(tabela.columns) == ["exposure", "rr", "lo", "hi"]

    def test_forest_devolve_a_city_table(self, pool):
        tabela = sus_mod_plot_pool(pool, type="forest", output_type="table")
        assert list(tabela["city"]) == list(pool["city_table"]["city"])

    def test_all_traz_plot_table_e_data(self, pool):
        todo = sus_mod_plot_pool(pool, type="forest", output_type="all")

        assert set(todo) == {"plot", "table", "data"}
        assert todo["plot"] is not None
        assert todo["table"] is todo["data"]

    def test_table_nao_gera_grafico(self, pool):
        """output_type='table' nao deve pagar o custo de desenhar."""
        assert sus_mod_plot_pool(pool, output_type="table") is not None


# ---------------------------------------------------------------------------
# O aviso que separa grafico legivel de grafico enganoso
# ---------------------------------------------------------------------------

class TestAvisoDeBase:
    def test_base_nao_compartilhada_avisa_na_hora_de_plotar(self, pool, capsys):
        """M65: a curva so e interpretavel se as cidades usaram a mesma base.

        O sus_mod_pool ja avisa quando calcula, mas quem abre o grafico
        depois nao viu aquele aviso -- e e olhando a curva que a pessoa
        tira a conclusao.
        """
        divergente = {**pool, "meta": {**pool["meta"], "shared_basis": False}}
        sus_mod_plot_pool(divergente, type="overall")

        assert "M65" in capsys.readouterr().out

    def test_base_compartilhada_nao_avisa(self, pool, capsys):
        sus_mod_plot_pool(pool, type="overall")
        assert "M65" not in capsys.readouterr().out
