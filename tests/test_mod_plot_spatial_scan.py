"""sus_mod_plot_spatial_scan: coroplético dos aglomerados de Kulldorff.

Consome a saída do sus_mod_spatial_scan e a cola nos polígonos. Os
testes cobrem a montagem da tabela (qual município recebe qual id de
aglomerado, em que ordem os níveis entram na legenda) e dois defeitos
do R achados ao portar: M86 e M87.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.viz.mod_plot_spatial_scan import (
    _BACKGROUND,
    _MLC_FILL,
    _PALETTE_POOL,
    _cluster_colours,
)

FIXTURES = Path(__file__).parent / "fixtures" / "spatial_scan"


@pytest.fixture(scope="module")
def municipios() -> gpd.GeoDataFrame:
    return gpd.read_file(FIXTURES / "scan_muni.gpkg", layer="muni")


@pytest.fixture(scope="module")
def codigos(municipios) -> list[str]:
    return municipios["code_muni"].astype(str).tolist()


def cluster(ids, rr=1.8, p=0.001) -> dict:
    return {"location_ids": list(ids), "observed": 100.0, "expected": 50.0,
            "RR": rr, "log_lik": 10.0, "p_value": p}


@pytest.fixture
def varredura(codigos):
    """Monta uma saída de varredura com N aglomerados secundários."""
    def _mk(n_sec: int, p_mlc: float = 0.001, rr_mlc: float = 2.5):
        sec = [cluster([codigos[i + 1]], rr=1.2 + i / 10) for i in range(n_sec)]
        return {
            "most_likely_cluster": cluster([codigos[0]], rr=rr_mlc, p=p_mlc),
            "secondary_clusters": sec,
            "n_clusters": n_sec + 1,
            "data": pd.DataFrame({"code_muni": codigos}),
            "meta": {},
        }
    return _mk


@pytest.fixture(scope="module")
def real(municipios) -> dict:
    df = pd.read_parquet(FIXTURES / "scan_df.parquet")
    return cs.sus_mod_spatial_scan(
        df, "obitos", "pop", municipios, n_simulations=99, verbose=False
    )


class TestMontagemDaTabela:
    """Quem recebe qual id, e em que ordem os níveis entram."""

    def test_os_ids_seguem_a_numeracao_do_r(self, varredura, municipios, codigos):
        p = cs.sus_mod_plot_spatial_scan(varredura(3), municipios, show_rr=False)
        d = p.data.set_index("code_muni")

        assert d.loc[codigos[0], "cluster_id"] == 1        # mais provável
        assert d.loc[codigos[1], "cluster_id"] == 2        # 1o secundário
        assert d.loc[codigos[3], "cluster_id"] == 4        # 3o secundário
        assert d.loc[codigos[10], "cluster_id"] == 0       # fundo

    def test_os_niveis_ficam_na_ordem_do_r(self, varredura, municipios):
        p = cs.sus_mod_plot_spatial_scan(varredura(2), municipios, show_rr=False)

        assert list(p.data["cluster_label"].cat.categories) == [
            "Mais provavel", "Aglomerado 2", "Aglomerado 3", "Plano de fundo",
        ]

    def test_o_rr_e_carimbado_por_aglomerado(self, varredura, municipios, codigos):
        p = cs.sus_mod_plot_spatial_scan(varredura(2), municipios)
        d = p.data.set_index("code_muni")

        assert d.loc[codigos[0], "RR"] == pytest.approx(2.5)
        assert d.loc[codigos[1], "RR"] == pytest.approx(1.2)
        assert np.isnan(d.loc[codigos[10], "RR"])

    def test_todos_os_municipios_entram(self, varredura, municipios):
        p = cs.sus_mod_plot_spatial_scan(varredura(2), municipios)

        assert len(p.data) == len(municipios)

    def test_alpha_decide_o_contorno_nao_a_cor(self, varredura, municipios):
        """p=0.20 continua desenhado, mas deixa de ser significativo."""
        p = cs.sus_mod_plot_spatial_scan(
            varredura(0, p_mlc=0.20), municipios, show_rr=True
        )

        assert (p.data["cluster_id"] == 1).sum() == 1      # ainda aparece
        assert not p.data["is_significant"].any()          # sem contorno

    def test_alpha_mais_frouxo_reabilita(self, varredura, municipios):
        p = cs.sus_mod_plot_spatial_scan(
            varredura(0, p_mlc=0.20), municipios, alpha=0.25
        )

        assert p.data["is_significant"].sum() == 1


class TestPaletaEstourada:
    """A partir do 10o aglomerado secundário o R quebra (M86).

    O R fatia a paleta com `palette_pool[seq_len(min(n, 9))]` e depois
    nomeia o resultado contra um nível por aglomerado. Do décimo em
    diante ele entrega ao `setNames()` mais nomes que valores e o erro é
    "'names' attribute [n] must be the same length as the vector [11]" --
    o gráfico não sai. Verificado de ponta a ponta contra o pacote
    instalado: nove desenham, dez abortam.

    Esta porta cicla a paleta em vez disso. É divergência deliberada:
    não há saída a que ser fiel, porque o R não produz nenhuma.
    """

    def test_ate_nove_sao_todas_distintas(self):
        cores = _cluster_colours(9)

        assert len(cores) == 9
        assert len(set(cores)) == 9
        assert cores == list(_PALETTE_POOL)

    def test_a_partir_do_decimo_recicla(self):
        cores = _cluster_colours(12)

        assert len(cores) == 12
        assert cores[9] == cores[0]
        assert cores[11] == cores[2]

    def test_doze_aglomerados_renderizam(self, varredura, municipios):
        """O caso que derruba o R."""
        p = cs.sus_mod_plot_spatial_scan(varredura(12), municipios, show_rr=False)

        assert len(p.data["cluster_label"].cat.categories) == 14
        assert p.data["cluster_id"].max() == 13

    def test_o_fundo_e_o_mais_provavel_nao_se_confundem(self, varredura, municipios):
        p = cs.sus_mod_plot_spatial_scan(varredura(12), municipios, show_rr=False)
        escala = [s for s in p.scales if "fill" in s.aesthetics][0]
        niveis = p.data["cluster_label"].cat.categories
        escala.train(pd.Series(p.data["cluster_label"]))
        cores = dict(zip(niveis, escala.map(pd.Series(pd.Categorical(
            niveis, categories=niveis)))))

        assert cores["Mais provavel"] == _MLC_FILL
        assert cores["Plano de fundo"] == _BACKGROUND
        assert cores["Aglomerado 11"] == cores["Aglomerado 2"]   # ciclou


class TestEscalaDegenerada:
    """Com um só aglomerado, o mapa de RR pinta o aglomerado de BRANCO (M87).

    `scale_fill_gradient2` reescala pelo intervalo dos dados. Quando
    todos os municípios de aglomerado dividem o mesmo RR -- o que
    acontece sempre que a varredura acha um aglomerado só, que é o caso
    comum -- o intervalo é degenerado. Tanto o `scales` do R quanto o
    `mizani` do plotnine tratam faixa nula devolvendo o ponto médio, que
    é a cor do meio: branco. Ou seja o aglomerado sai da mesma cor que
    "risco igual ao nulo", que é o oposto do achado.

    O `show_rr=True` é o DEFAULT e um aglomerado só é o caso comum,
    então o gráfico padrão de um resultado típico se lê ao contrário.
    A paridade aqui sai de graça: as duas bibliotecas fazem o mesmo.
    """

    def test_um_rr_unico_vira_branco(self):
        from plotnine import scale_fill_gradient2

        sc = scale_fill_gradient2(low="#000080", mid="#FFFFFF",
                                  high="#FF0000", midpoint=1)
        sc.train(pd.Series([2.2347]))
        cor = str(sc.map(pd.Series([2.2347]))[0]).lower()

        # #fffefe: indistinguível do branco do ponto médio
        assert cor.startswith("#ff")
        assert int(cor[3:5], 16) > 250 and int(cor[5:7], 16) > 250

    def test_dois_rr_distintos_pintam_direito(self):
        from plotnine import scale_fill_gradient2

        sc = scale_fill_gradient2(low="#000080", mid="#FFFFFF",
                                  high="#FF0000", midpoint=1)
        sc.train(pd.Series([1.2, 2.8]))
        cores = [str(c).lower() for c in sc.map(pd.Series([1.2, 2.8]))]

        assert cores[1] == "#ff0000"       # o maior vai a vermelho pleno
        assert cores[0] != cores[1]

    def test_a_varredura_real_cai_nesse_caso(self, real):
        """Confirma que o caso comum e o caso degenerado sao o mesmo."""
        assert len(real["secondary_clusters"]) == 0
        # um unico RR entre os municipios de aglomerado
        assert real["most_likely_cluster"]["RR"] > 2


class TestEntradas:
    def test_recusa_x_que_nao_e_varredura(self, municipios):
        with pytest.raises(TypeError, match="sus_mod_spatial_scan"):
            cs.sus_mod_plot_spatial_scan({"algo": 1}, municipios)

    def test_recusa_municipios_sem_geometria(self, varredura, municipios):
        plano = pd.DataFrame({"code_muni": municipios["code_muni"]})
        with pytest.raises(TypeError, match="GeoDataFrame"):
            cs.sus_mod_plot_spatial_scan(varredura(1), plano)

    def test_recusa_sem_code_muni(self, varredura, municipios):
        with pytest.raises(ValueError, match="code_muni"):
            cs.sus_mod_plot_spatial_scan(
                varredura(1), municipios.drop(columns="code_muni")
            )

    def test_lingua_desconhecida_avisa_e_cai_no_pt(self, varredura, municipios):
        with pytest.warns(UserWarning, match="nao suportado"):
            p = cs.sus_mod_plot_spatial_scan(
                varredura(1), municipios, lang="tupi"
            )

        assert "Mais provavel" in list(p.data["cluster_label"].cat.categories)

    def test_avisa_quando_nada_e_significativo(self, varredura, municipios):
        with pytest.warns(UserWarning, match="[Nn]enhum aglomerado significativo"):
            cs.sus_mod_plot_spatial_scan(varredura(0, p_mlc=0.9), municipios)

    @pytest.mark.parametrize(("lang", "esperado"), [
        ("pt", "Mais provavel"), ("en", "Most likely"), ("es", "Mas probable"),
    ])
    def test_as_tres_linguas(self, varredura, municipios, lang, esperado):
        p = cs.sus_mod_plot_spatial_scan(
            varredura(1), municipios, show_rr=False, lang=lang
        )

        assert esperado in list(p.data["cluster_label"].cat.categories)

    def test_aceita_e_ignora_kwargs(self, varredura, municipios):
        """Espelha o `...` do R."""
        p = cs.sus_mod_plot_spatial_scan(
            varredura(1), municipios, algo_qualquer=123
        )

        assert p is not None


class TestRenderizacao:
    @pytest.mark.parametrize("show_rr", [True, False])
    def test_devolve_um_ggplot(self, real, municipios, show_rr):
        from plotnine import ggplot

        p = cs.sus_mod_plot_spatial_scan(real, municipios, show_rr=show_rr)

        assert isinstance(p, ggplot)

    @pytest.mark.parametrize("show_rr", [True, False])
    def test_desenha_sem_erro(self, real, municipios, show_rr, tmp_path):
        p = cs.sus_mod_plot_spatial_scan(real, municipios, show_rr=show_rr)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p.save(tmp_path / f"m_{show_rr}.png", width=5, height=4, dpi=60,
                   verbose=False)

        assert (tmp_path / f"m_{show_rr}.png").exists()

    def test_o_titulo_pode_ser_trocado(self, real, municipios):
        p = cs.sus_mod_plot_spatial_scan(real, municipios, title="Meu titulo")

        assert p.labels.title == "Meu titulo"

    def test_o_titulo_padrao_vem_da_lingua(self, real, municipios):
        p = cs.sus_mod_plot_spatial_scan(real, municipios, lang="en")

        assert p.labels.title == "Kulldorff Spatial Clusters"
