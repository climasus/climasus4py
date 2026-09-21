"""M99 (c): o manifest.json passou a ser conferido na carga.

O gerador ja havia sido consertado em 16/09/2026 -- passou a hashear tudo
o que lista e a varrer os diretorios, e ficou idempotente. O que sobrava
era que NADA conferia o conteudo: o arquivo era lido, mas so como
marcador para achar a raiz do repositorio, e qualquer edicao honesta de
um `.json` produzia divergencia que nada nunca consertava.

A decisao, de 21/09/2026 (D7): **conferir na carga e AVISAR, nunca
abortar.** Se e contrato, tem de ser conferido; mas um climasus-data que
se recusa a carregar porque um checksum esta velho transforma problema de
inventario em pacote quebrado, e quem sente e o usuario final que nao
editou nada. `CLIMASUS_DATA_STRICT=1` eleva a erro, para CI.

E a pergunta lateral foi respondida por evidencia, nao por gosto: o
diretorio `viz/` ENTROU no manifest, porque
`climasus4py/viz/plot_aggregate_map.py` e `plot_aggregate_ts.py` leem
`viz/viz_labels.json` e `viz/viz_config.json` em tempo de execucao. Sao
dado consumido, e deixa-los fora punha dois arquivos que o pacote le
fora do contrato de integridade.
"""

from __future__ import annotations

import os
import warnings

import pytest

climasus_data = pytest.importorskip("climasus_data")


@pytest.fixture
def limpa_cache():
    """Zera o cache por arquivo antes e depois.

    O modulo hasheia e reclama uma vez por caminho por processo, de
    proposito; sem zerar, um teste veria o veredito de outro.
    """
    climasus_data._check_file.cache_clear()
    climasus_data._JA_AVISADO.clear()
    yield
    climasus_data._check_file.cache_clear()
    climasus_data._JA_AVISADO.clear()


@pytest.fixture
def arquivo_alterado(limpa_cache):
    """Altera um json do climasus-data e restaura no fim.

    Escolhido `metadata/regions.json` por ser pequeno e nao ser lido por
    nenhum outro teste desta suite. O byte acrescentado e um newline, que
    nao muda o JSON e muda o md5 -- exatamente o caso que interessa: o
    arquivo continua valido e o inventario ficou velho.
    """
    alvo = climasus_data.data_root() / "metadata" / "regions.json"
    original = alvo.read_bytes()
    alvo.write_bytes(original + b"\n")
    climasus_data._check_file.cache_clear()
    climasus_data._JA_AVISADO.clear()
    try:
        yield "metadata/regions.json"
    finally:
        alvo.write_bytes(original)
        climasus_data._check_file.cache_clear()
        climasus_data._JA_AVISADO.clear()


class TestCheckoutIntegro:

    def test_o_repositorio_esta_integro(self, limpa_cache):
        """Se este falhar, rode scripts/update_manifest.py."""
        rel = climasus_data.verify_integrity()
        assert not rel["mismatched"], f"md5 divergente: {rel['mismatched']}"
        assert not rel["missing"], f"listado e ausente: {rel['missing']}"
        assert len(rel["ok"]) >= 47

    def test_carregar_nao_avisa_quando_esta_tudo_certo(self, limpa_cache):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            climasus_data.get_path("metadata/datasus_columns.json")
            climasus_data.load_json("metadata/datasus_systems.json")

    def test_o_viz_entrou_no_manifest(self, limpa_cache):
        """D7: sao dado que o climasus4py le em tempo de execucao."""
        listados = {i["path"] for i in climasus_data.manifest()["files"]}
        assert "viz/viz_labels.json" in listados
        assert "viz/viz_config.json" in listados

    def test_quem_le_o_viz_de_fato_existe(self):
        """A contrapartida: a justificativa do escopo tem de ser verdade.

        Se ninguem mais lesse esses arquivos, a inclusao no manifest
        perderia o motivo e este teste avisaria.
        """
        from climasus4py.viz import plot_aggregate_map, plot_aggregate_ts

        assert plot_aggregate_map._load_viz_labels()
        assert plot_aggregate_map._load_viz_config()
        assert plot_aggregate_ts._load_viz_labels()


class TestDivergenciaAvisa:

    def test_avisa_e_devolve_o_caminho(self, arquivo_alterado):
        """Avisar e nao abortar e a decisao da D7.

        Devolver o caminho importa tanto quanto avisar: quem chamou
        precisa do dado, e o inventario velho nao e culpa dele.
        """
        with pytest.warns(UserWarning, match="does not match manifest"):
            caminho = climasus_data.get_path(arquivo_alterado)
        assert caminho.is_file()

    def test_load_json_tambem_avisa_e_carrega(self, arquivo_alterado):
        with pytest.warns(UserWarning, match="does not match manifest"):
            dados = climasus_data.load_json(arquivo_alterado)
        assert dados, "o conteudo tem de vir mesmo com o md5 velho"

    def test_a_mensagem_diz_o_que_fazer(self, arquivo_alterado):
        with pytest.warns(UserWarning) as rec:
            climasus_data.get_path(arquivo_alterado)
        msg = str(rec[0].message)
        assert arquivo_alterado in msg, "tem de nomear o arquivo"
        assert "update_manifest.py" in msg, "tem de dizer o conserto"
        assert "corrupt" in msg, "tem de admitir a outra causa possivel"

    def test_avisa_uma_vez_por_arquivo(self, arquivo_alterado):
        """Um aviso por acesso viraria ruido num laco.

        E ruido e como um aviso de verdade passa batido. O cache do
        _check_file so evita re-hashear; a contagem depende do
        _JA_AVISADO.
        """
        contagens = []
        for _ in range(3):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                climasus_data.get_path(arquivo_alterado)
            contagens.append(len(w))
        assert contagens == [1, 0, 0], contagens

    def test_verify_integrity_devolve_a_lista(self, arquivo_alterado):
        """Devolve em vez de so avisar, para CI poder falhar a build."""
        rel = climasus_data.verify_integrity()
        assert rel["mismatched"] == [arquivo_alterado]
        assert not rel["missing"]

    def test_verify_integrity_aceita_um_subconjunto(self, arquivo_alterado):
        rel = climasus_data.verify_integrity([arquivo_alterado])
        assert rel["mismatched"] == [arquivo_alterado]
        assert rel["ok"] == []

    def test_verify_false_nao_confere(self, arquivo_alterado):
        """O proprio gerador do manifest precisa disso.

        Ele tem de ler um arquivo JUSTAMENTE porque o checksum dele esta
        velho -- conferir ali seria circular.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            climasus_data.get_path(arquivo_alterado, verify=False)


class TestModoEstrito:

    @pytest.fixture
    def estrito(self):
        anterior = os.environ.get(climasus_data.STRICT_ENV)
        os.environ[climasus_data.STRICT_ENV] = "1"
        try:
            yield
        finally:
            if anterior is None:
                os.environ.pop(climasus_data.STRICT_ENV, None)
            else:
                os.environ[climasus_data.STRICT_ENV] = anterior

    def test_levanta_em_vez_de_avisar(self, arquivo_alterado, estrito):
        with pytest.raises(ValueError, match="does not match manifest"):
            climasus_data.get_path(arquivo_alterado)

    def test_levanta_toda_vez_e_nao_so_a_primeira(self, arquivo_alterado,
                                                  estrito):
        """Excecao nao e ruido, e engolir a segunda esconderia o problema
        de quem capturou a primeira e seguiu adiante."""
        for _ in range(3):
            with pytest.raises(ValueError):
                climasus_data.get_path(arquivo_alterado)

    def test_o_checkout_integro_nao_levanta_nem_no_estrito(self, limpa_cache,
                                                           estrito):
        climasus_data.get_path("metadata/datasus_columns.json")

    @pytest.mark.parametrize("valor,deve_levantar", [
        ("1", True), ("true", True), ("yes", True), ("TRUE", True),
        ("0", False), ("false", False), ("no", False), ("", False),
    ])
    def test_os_valores_que_ligam_o_estrito(self, arquivo_alterado, valor,
                                            deve_levantar):
        anterior = os.environ.get(climasus_data.STRICT_ENV)
        os.environ[climasus_data.STRICT_ENV] = valor
        try:
            if deve_levantar:
                with pytest.raises(ValueError):
                    climasus_data.get_path(arquivo_alterado)
            else:
                with pytest.warns(UserWarning):
                    climasus_data.get_path(arquivo_alterado)
        finally:
            if anterior is None:
                os.environ.pop(climasus_data.STRICT_ENV, None)
            else:
                os.environ[climasus_data.STRICT_ENV] = anterior


class TestArquivoAusente:

    def test_listado_e_fora_do_disco_e_reportado(self, limpa_cache):
        """Sem mexer em arquivo de verdade: finge o manifest."""
        original = climasus_data._expected_md5.cache_clear
        climasus_data._expected_md5.cache_clear()
        real = climasus_data._expected_md5
        try:
            climasus_data._expected_md5 = lambda: {
                "nao/existe.json": "0" * 32}
            climasus_data._check_file.cache_clear()
            rel = climasus_data.verify_integrity()
            assert rel["missing"] == ["nao/existe.json"]
            assert not rel["mismatched"]
        finally:
            climasus_data._expected_md5 = real
            original()
            climasus_data._check_file.cache_clear()

    def test_arquivo_que_o_manifest_nao_lista_nao_e_erro(self, limpa_cache):
        """O manifest cobre os diretorios publicados, nao tudo.

        Um chamador pode legitimamente pedir outra coisa -- o proprio
        manifest.json, por exemplo.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            climasus_data.get_path("manifest.json")
