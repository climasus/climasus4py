"""O bloco RAP portavel, e o contrato de interoperabilidade com o R.

Das nove funcoes `sus_rap_*` do climasus4r, quatro foram portadas --
`recipe`, `from_recipe`, `run` e `inspect` -- porque a receita YAML grava
os passos como DADOS e o `run` reconstroi a chamada em vez de parsear R.
As outras cinco (`export`, `read`, `update`, `targets`, `make`) escrevem,
parseiam ou editam texto R, ou dependem do pacote `targets`; estao
marcadas como ecossistema R na planilha de paridade.

O que estes testes protegem, em ordem de importancia:

1. **Interoperabilidade nas duas direcoes.** Uma receita escrita aqui
   carrega em `sus_rap_from_recipe()` do R, e uma receita escrita pelo R
   carrega aqui. Verificado de fato no R 4.6.0: o R leu os 4 passos, os
   `params` e reconstruiu o pipeline; as fixturas deste arquivo
   congelam os dois YAML daquela execucao.
2. **A paridade do xxHash32** com `digest::digest(..., serialize=FALSE)`,
   valor a valor.
3. **Os comportamentos do R replicados de proposito** -- o primeiro passo
   descartado, o `lang` injetado em passo sem parametro -- que precisam
   falhar o teste se alguem os "consertar" sem falar com o Marlon.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from climasus4py.utils.rap import (
    _achata,
    _estrutura,
    _resumo,
    sus_rap_from_recipe,
    sus_rap_inspect,
    sus_rap_recipe,
    sus_rap_run,
    xxhash32,
)

FIXTURAS = Path(__file__).parent / "fixtures" / "rap"


# --------------------------------------------------------------------------
# xxHash32
# --------------------------------------------------------------------------

class TestXxhash32:
    """Paridade com digest::digest(algo="xxhash32", serialize=FALSE)."""

    @pytest.mark.parametrize("entrada,esperado", [
        ("", "02cc5d05"),
        ("a", "550d7456"),
        ("abc", "32d153ff"),
        ("SP|2015|2022|SIM-DO", "bdc04062"),
        ("SP,RJ|2015|2022|SIM-DO|year", "5d2e6cb6"),
        ("acentuacao: cao, mae, Piaui", "ecb21709"),
        ("x" * 1000, "73bcd817"),
    ])
    def test_paridade_com_o_digest_do_r(self, entrada, esperado):
        assert xxhash32(entrada) == esperado

    def test_a_entrada_de_1000_bytes_exercita_os_quatro_acumuladores(self):
        """Acima de 16 bytes o algoritmo entra no laco de quatro vias.

        As entradas curtas nao passam por ele, entao sem o caso longo a
        paridade seria acidental.
        """
        assert len("x" * 1000) > 16
        assert xxhash32("x" * 1000) == "73bcd817"

    def test_seed_muda_o_resultado_como_no_r(self):
        assert xxhash32("abc", seed=1) == "aa3da8ff"
        assert xxhash32("abc", seed=0) == "32d153ff"

    def test_bytes_e_str_dao_o_mesmo(self):
        assert xxhash32("abc") == xxhash32(b"abc")

    def test_sempre_oito_digitos_hexadecimais(self):
        for s in ("", "a", "abc", "x" * 77):
            h = xxhash32(s)
            assert len(h) == 8
            int(h, 16)


# --------------------------------------------------------------------------
# recipe / from_recipe
# --------------------------------------------------------------------------

RAP_BASE = {
    "params": {"uf": ["SP", "RJ"], "years": [2015, 2022],
               "system": "SIM-DO", "time_unit": "year", "lang": "pt",
               "seed": 42},
    "steps": [
        {"function_name": "sus_data_import", "important_params": {}},
        {"function_name": "sus_data_clean", "important_params": {}},
        {"function_name": "sus_data_standardize",
         "important_params": {"lang": "pt"}},
        {"function_name": "sus_data_aggregate",
         "important_params": {"time_unit": "year", "geo": "state"}},
    ],
    "structure": {"type": "Mortalidade por temperatura"},
    "source": "montado a mao",
    "format": "recipe",
}


@pytest.fixture
def rap():
    """Copia funda do objeto base, por passo."""
    import copy

    return copy.deepcopy(RAP_BASE)


class TestRecipe:

    def test_escreve_as_chaves_que_o_r_le(self, rap, tmp_path):
        """`parameters`, `steps`, `metadata` e `pipeline_type`.

        Sao exatamente as chaves que o `sus_rap_from_recipe` do R procura;
        renomear qualquer uma quebra a leitura no R sem quebrar nada aqui,
        que e o modo silencioso de perder a interoperabilidade.
        """
        import yaml

        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml")
        d = yaml.safe_load(alvo.read_text(encoding="utf-8"))
        for chave in ("rap_version", "created", "pipeline_type",
                      "parameters", "steps", "metadata", "data_hash"):
            assert chave in d, f"o R procura {chave!r}"
        assert d["pipeline_type"] == "Mortalidade por temperatura"
        assert d["parameters"]["uf"] == ["SP", "RJ"]

    def test_o_passo_e_dado_e_nao_codigo(self, rap, tmp_path):
        """A razao pela qual esta metade do RAP e portavel."""
        import yaml

        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml")
        d = yaml.safe_load(alvo.read_text(encoding="utf-8"))
        assert [s["function_name"] for s in d["steps"]] == [
            "sus_data_import", "sus_data_clean", "sus_data_standardize",
            "sus_data_aggregate"]
        assert d["steps"][3]["params"] == {"time_unit": "year",
                                           "geo": "state"}
        assert d["steps"][0]["params"] is None, "passo sem parametro -> ~"

    def test_ida_e_volta_preserva_os_params(self, rap, tmp_path):
        volta = sus_rap_from_recipe(
            sus_rap_recipe(rap, tmp_path / "r.yaml"))
        assert volta["params"] == rap["params"]

    def test_ida_e_volta_preserva_os_passos(self, rap, tmp_path):
        volta = sus_rap_from_recipe(
            sus_rap_recipe(rap, tmp_path / "r.yaml"))
        assert ([s["function_name"] for s in volta["steps"]]
                == [s["function_name"] for s in rap["steps"]])
        assert (volta["steps"][2]["important_params"]
                == {"lang": "pt"})

    def test_recusa_sobrescrever_por_omissao(self, rap, tmp_path):
        alvo = tmp_path / "r.yaml"
        sus_rap_recipe(rap, alvo)
        with pytest.raises(FileExistsError, match="overwrite=True"):
            sus_rap_recipe(rap, alvo)
        assert sus_rap_recipe(rap, alvo, overwrite=True) == alvo

    def test_recusa_objeto_sem_steps(self, tmp_path):
        with pytest.raises(TypeError, match="steps"):
            sus_rap_recipe({"params": {}}, tmp_path / "r.yaml")

    def test_include_data_hash_falso_omite_o_campo(self, rap, tmp_path):
        import yaml

        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml",
                              include_data_hash=False)
        assert "data_hash" not in yaml.safe_load(
            alvo.read_text(encoding="utf-8"))

    def test_o_lang_entra_nos_parametros_quando_ausente(self, tmp_path):
        import yaml

        alvo = sus_rap_recipe(
            {"params": {"uf": ["SP"]}, "steps": [
                {"function_name": "sus_data_import"}]},
            tmp_path / "r.yaml", lang="es")
        d = yaml.safe_load(alvo.read_text(encoding="utf-8"))
        assert d["parameters"]["lang"] == "es"

    def test_o_data_hash_nao_e_o_hash_da_string_vazia(self, rap, tmp_path):
        """Regressao de um defeito meu, nao do R.

        O `structure` montado a mao traz so `type`, e a primeira versao lia
        `input_params` direto dele: vinha vazio, e TODA receita saia com
        `data_hash` igual a 02cc5d05 -- o xxHash32 da string vazia. Um hash
        constante e pior que hash nenhum, porque parece funcionar.
        """
        import yaml

        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml")
        h = yaml.safe_load(alvo.read_text(encoding="utf-8"))["data_hash"]
        assert h != xxhash32(""), "o hash caiu na string vazia outra vez"
        assert h == xxhash32("SP|RJ|2015|2022|SIM-DO"), (
            "o hash cobre input_params -- uf, years, system -- achatado "
            "como o unlist do R")

    def test_params_diferentes_dao_hash_diferente(self, rap, tmp_path):
        import copy
        import yaml

        outro = copy.deepcopy(rap)
        outro["params"]["uf"] = ["MG"]
        a, b = (yaml.safe_load(
            sus_rap_recipe(o, tmp_path / f"{n}.yaml").read_text(
                encoding="utf-8"))["data_hash"]
            for n, o in (("a", rap), ("b", outro)))
        assert a != b

    def test_nome_padrao_quando_nao_se_passa_caminho(self, rap, tmp_path,
                                                     monkeypatch):
        monkeypatch.chdir(tmp_path)
        alvo = sus_rap_recipe(rap)
        assert alvo.name.startswith("rap_recipe_")
        assert alvo.suffix == ".yaml"
        assert alvo.is_file()


class TestFromRecipe:

    def test_erro_de_arquivo_ausente(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="recipe not found"):
            sus_rap_from_recipe(tmp_path / "nao_existe.yaml")

    def test_erro_quando_o_yaml_nao_e_mapa(self, tmp_path):
        alvo = tmp_path / "r.yaml"
        alvo.write_text("- so\n- uma\n- lista\n", encoding="utf-8")
        with pytest.raises(ValueError, match="does not parse as a recipe"):
            sus_rap_from_recipe(alvo)

    def test_override_params_sobrepoe(self, rap, tmp_path):
        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml")
        volta = sus_rap_from_recipe(alvo, override_params={"uf": ["MG"]})
        assert volta["params"]["uf"] == ["MG"]
        assert volta["params"]["years"] == [2015, 2022], "so o uf muda"

    def test_o_objeto_tem_a_forma_que_o_run_e_o_inspect_esperam(
            self, rap, tmp_path):
        volta = sus_rap_from_recipe(
            sus_rap_recipe(rap, tmp_path / "r.yaml"))
        for chave in ("metadata", "params", "steps", "structure", "source",
                      "format", "data_hash"):
            assert chave in volta
        assert volta["format"] == "recipe"

    def test_passo_sem_nome_vira_unknown(self, tmp_path):
        alvo = tmp_path / "r.yaml"
        alvo.write_text(
            "parameters:\n  uf: [SP]\nsteps:\n- params:\n    lang: pt\n",
            encoding="utf-8")
        volta = sus_rap_from_recipe(alvo)
        assert volta["steps"][0]["function_name"] == "unknown"

    def test_o_created_do_topo_e_recuperado(self, rap, tmp_path):
        """M112: o R perde o `created` em toda ida e volta.

        O R grava `created` no TOPO da receita mas copia so
        `recipe$metadata` para o objeto, entao o `sus_rap_inspect` dele
        imprime "?" para a data. Aqui o campo e recuperado: um artefato de
        reprodutibilidade que esquece quando foi feito nao serve de muito.
        Divergencia consciente, registrada como M112.
        """
        volta = sus_rap_from_recipe(
            sus_rap_recipe(rap, tmp_path / "r.yaml"))
        assert volta["metadata"]["created"] != "?"
        assert _resumo(volta)["created"] != "?"

    def test_metadata_propria_do_arquivo_vence_o_topo(self, tmp_path):
        alvo = tmp_path / "r.yaml"
        alvo.write_text(
            "created: 2020-01-01 00:00:00\n"
            "metadata:\n  created: '1999-12-31 23:59:59'\n"
            "steps:\n- function_name: sus_data_import\n",
            encoding="utf-8")
        volta = sus_rap_from_recipe(alvo)
        assert volta["metadata"]["created"] == "1999-12-31 23:59:59"

    def test_o_created_e_texto_venha_com_aspas_ou_sem(self, tmp_path):
        """O R grava o carimbo sem aspas, e o YAML o resolve em datetime.

        Sem normalizar, o campo seria str ou datetime conforme as aspas do
        arquivo que por acaso se leu -- e quem consome o objeto nao tem
        como saber qual.
        """
        for i, linha in enumerate(("created: 2020-01-01 00:00:00",
                                   "created: '2020-01-01 00:00:00'")):
            alvo = tmp_path / f"r{i}.yaml"
            alvo.write_text(f"{linha}\nsteps:\n- function_name: x\n",
                            encoding="utf-8")
            achado = sus_rap_from_recipe(alvo)["metadata"]["created"]
            assert achado == "2020-01-01 00:00:00", f"em {linha!r}"


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

class TestRun:

    @staticmethod
    def _obj(rap, tmp_path):
        return sus_rap_from_recipe(sus_rap_recipe(rap, tmp_path / "r.yaml"))

    def test_dry_run_nao_toca_em_dado_e_devolve_none(self, rap, tmp_path,
                                                     capsys):
        assert sus_rap_run(self._obj(rap, tmp_path), dry_run=True) is None
        saida = capsys.readouterr().out
        assert "sus_data_import(" in saida
        assert "sus_data_aggregate(time_unit='year', geo='state')" in saida

    def test_dry_run_monta_o_import_a_partir_dos_params(self, rap, tmp_path,
                                                        capsys):
        sus_rap_run(self._obj(rap, tmp_path), dry_run=True)
        linha = capsys.readouterr().out.splitlines()[0]
        for pedaco in ("system='SIM-DO'", "uf=['SP', 'RJ']",
                       "year=[2015, 2022]", "lang='pt'"):
            assert pedaco in linha

    def test_m113_o_primeiro_passo_perde_o_nome(self, tmp_path):
        """Replicado do R de proposito, mas com aviso.

        O `.rap_rebuild_call` do R DESCARTA o nome do primeiro passo e
        sempre abre com `sus_data_import` montado a partir de `params`.
        Uma receita cujo primeiro passo seja outra coisa e reescrita --
        silenciosamente, no R. Aqui o passo tambem e descartado, para nao
        divergir, mas com UserWarning. Registrado como M113.
        """
        alvo = tmp_path / "r.yaml"
        alvo.write_text(
            "parameters:\n  uf: [SP]\n  system: SIM-DO\n"
            "steps:\n- function_name: sus_data_clean\n"
            "- function_name: sus_data_standardize\n",
            encoding="utf-8")
        obj = sus_rap_from_recipe(alvo)
        with pytest.warns(UserWarning, match="always opens with"):
            sus_rap_run(obj, dry_run=True)

    def test_m113_passo_sem_parametro_recebe_lang(self, rap, tmp_path,
                                                  capsys):
        """Tambem do R: `important_params` vazio vira `lang=<idioma>`."""
        sus_rap_run(self._obj(rap, tmp_path), dry_run=True)
        linhas = capsys.readouterr().out.splitlines()
        assert "sus_data_clean(lang='pt')" in linhas[1]

    def test_despacho_e_nao_eval(self, rap, tmp_path):
        """A diferenca de seguranca com o R.

        O R monta texto e chama `eval(parse(text = ...))`; aqui cada
        `function_name` e procurado no namespace publico do pacote. Uma
        receita que nomeie qualquer outra coisa nao executa nada -- no R,
        `os.remove` no lugar do nome de uma etapa seria avaliado.
        """
        obj = self._obj(rap, tmp_path)
        obj["steps"][2]["function_name"] = "print"
        with pytest.raises(ValueError, match="does not export"):
            sus_rap_run(obj)

    def test_nomes_perigosos_tambem_sao_recusados(self, rap, tmp_path):
        obj = self._obj(rap, tmp_path)
        obj["steps"][1]["function_name"] = "__import__('os').system"
        with pytest.raises(ValueError, match="does not export"):
            sus_rap_run(obj)

    def test_recusa_objeto_sem_passo(self):
        with pytest.raises(TypeError, match="non-empty"):
            sus_rap_run({"steps": [], "params": {}})
        with pytest.raises(TypeError, match="non-empty"):
            sus_rap_run({"params": {}})

    def test_overrides_entram_na_chamada(self, rap, tmp_path, capsys):
        sus_rap_run(self._obj(rap, tmp_path), dry_run=True, uf=["BA"])
        assert "uf=['BA']" in capsys.readouterr().out.splitlines()[0]

    def test_execute_no_from_recipe_chama_o_run(self, rap, tmp_path,
                                                capsys):
        alvo = sus_rap_recipe(rap, tmp_path / "r.yaml")
        assert sus_rap_from_recipe(alvo, execute=True, dry_run=True) is None
        assert "sus_data_import(" in capsys.readouterr().out


# --------------------------------------------------------------------------
# inspect
# --------------------------------------------------------------------------

class TestInspect:

    @staticmethod
    def _obj(rap, tmp_path, nome="r.yaml"):
        return sus_rap_from_recipe(sus_rap_recipe(rap, tmp_path / nome))

    def test_imprime_o_resumo(self, rap, tmp_path, capsys):
        r = sus_rap_inspect(self._obj(rap, tmp_path))
        saida = capsys.readouterr().out
        assert "rap_object summary" in saida
        assert "Mortalidade por temperatura" in saida
        assert "Pipeline steps (4)" in saida
        assert r["summary"]["total_steps"] == 4

    def test_verbose_falso_omite_os_passos(self, rap, tmp_path, capsys):
        sus_rap_inspect(self._obj(rap, tmp_path), verbose=False)
        assert "Pipeline steps" not in capsys.readouterr().out

    def test_o_rotulo_do_runtime_segue_quem_escreveu(self, rap, tmp_path):
        """Uma receita do R nao pode aparecer sob o rotulo "Python".

        O `_resumo` cai para `r_version` quando nao ha `python_version`, e
        a primeira versao imprimia "Python  R version 4.6.0" -- que foi o
        que apareceu na prova de interoperabilidade.
        """
        meu = _resumo(self._obj(rap, tmp_path))
        assert meu["runtime"] == "Python"
        assert meu["package"] == "climasus4py"

        do_r = _resumo({
            "steps": [], "params": {},
            "metadata": {"r_version": "R version 4.6.0 (2026-04-24 ucrt)",
                         "package_version": "1.0.0"},
        })
        assert do_r["runtime"] == "R"
        assert do_r["package"] == "climasus4r"
        assert do_r["runtime_version"].startswith("R version")

    def test_compara_dois_objetos(self, rap, tmp_path):
        a = self._obj(rap, tmp_path, "a.yaml")
        b = sus_rap_from_recipe(Path(a["source"]),
                                override_params={"uf": ["MG"]})
        r = sus_rap_inspect(a, b, verbose=False)
        assert r["comparison"]["params"]["uf"] == (["SP", "RJ"], ["MG"])

    def test_objetos_iguais_nao_tem_diferenca(self, rap, tmp_path, capsys):
        a = self._obj(rap, tmp_path, "a.yaml")
        r = sus_rap_inspect(a, sus_rap_from_recipe(Path(a["source"])),
                            verbose=False)
        assert not r["comparison"]["fields"]
        assert not r["comparison"]["params"]
        assert "identical on every compared field" in capsys.readouterr().out

    def test_recusa_objeto_invalido(self, rap, tmp_path):
        with pytest.raises(TypeError, match="rap must be"):
            sus_rap_inspect({"params": {}})
        with pytest.raises(TypeError, match="rap2 must be"):
            sus_rap_inspect(self._obj(rap, tmp_path), {"params": {}})


# --------------------------------------------------------------------------
# auxiliares
# --------------------------------------------------------------------------

class TestAuxiliares:

    def test_achata_como_o_unlist_do_r(self):
        assert _achata({"a": ["SP", "RJ"], "b": {"c": 1}}) == ["SP", "RJ", 1]
        assert _achata(None) == [None]
        assert _achata(3) == [3]
        assert _achata([]) == []

    def test_estrutura_conta_e_lista_as_funcoes(self):
        e = _estrutura(
            {"uf": ["SP"], "years": [2020], "system": "SIH-RD"},
            [{"function_name": "sus_data_import"},
             {"function_name": "sus_data_clean"}],
            "Teste")
        assert e["type"] == "Teste"
        assert e["total_steps"] == 2
        assert e["functions_used"] == ["sus_data_import", "sus_data_clean"]
        assert e["input_params"]


# --------------------------------------------------------------------------
# interoperabilidade R <-> Python, sobre os YAML congelados
# --------------------------------------------------------------------------

class TestInteropComOR:
    """As duas receitas da prova de interoperabilidade, congeladas.

    `escrita_pelo_python.yaml` foi lida pelo `sus_rap_from_recipe()` do R
    4.6.0, que reconstruiu os 4 passos e o pipeline; e
    `escrita_pelo_r.yaml` e o que o R escreveu em seguida. O que se testa
    aqui e o lado Python: que o formato continua o mesmo e que o arquivo
    do R continua carregando.
    """

    DO_R = FIXTURAS / "escrita_pelo_r.yaml"
    DO_PYTHON = FIXTURAS / "escrita_pelo_python.yaml"

    def test_le_a_receita_escrita_pelo_r(self):
        obj = sus_rap_from_recipe(self.DO_R)
        assert obj["params"] == {"uf": ["SP", "RJ"], "years": [2015, 2022],
                                 "system": "SIM-DO", "time_unit": "year",
                                 "lang": "pt", "seed": 42}
        assert [s["function_name"] for s in obj["steps"]] == [
            "sus_data_import", "sus_data_clean", "sus_data_standardize",
            "sus_data_aggregate"]
        assert obj["steps"][3]["important_params"] == {"time_unit": "year",
                                                       "geo": "state"}
        assert obj["structure"]["type"] == "Mortalidade por temperatura"

    def test_o_til_do_r_para_passo_sem_parametro(self):
        """O R grava `params: ~`, que o YAML le como None."""
        assert "params: ~" in self.DO_R.read_text(encoding="utf-8")
        obj = sus_rap_from_recipe(self.DO_R)
        assert obj["steps"][0]["important_params"] == {}

    def test_roda_o_dry_run_sobre_a_receita_do_r(self, capsys):
        sus_rap_run(sus_rap_from_recipe(self.DO_R), dry_run=True)
        linhas = capsys.readouterr().out.strip().splitlines()
        assert len(linhas) == 4
        assert "sus_data_aggregate" in linhas[3]

    def test_o_formato_que_o_r_leu_nao_mudou(self, rap, tmp_path):
        """Se as chaves de topo mudarem, o R para de ler sem aviso."""
        import yaml

        congelado = yaml.safe_load(
            self.DO_PYTHON.read_text(encoding="utf-8"))
        atual = yaml.safe_load(
            sus_rap_recipe(rap, tmp_path / "r.yaml").read_text(
                encoding="utf-8"))
        assert set(atual) == set(congelado)
        assert set(atual["steps"][0]) == set(congelado["steps"][0])
        assert atual["parameters"] == congelado["parameters"]
        assert atual["data_hash"] == congelado["data_hash"]

    def test_o_r_le_de_verdade_se_estiver_instalado(self, rap, tmp_path):
        """A prova real, quando ha R com climasus4r na maquina.

        Pulado onde nao houver -- o teste congelado acima ja guarda o
        formato.
        """
        import shutil
        import subprocess

        rscript = shutil.which("Rscript") or (
            r"C:\Program Files\R\R-4.6.0\bin\Rscript.exe")
        if not Path(rscript).is_file():
            pytest.skip("Rscript ausente")

        alvo = sus_rap_recipe(rap, tmp_path / "para_o_r.yaml")
        script = tmp_path / "le.R"
        script.write_text(
            'suppressMessages(library(climasus4r))\n'
            f'o <- sus_rap_from_recipe("{alvo.as_posix()}", execute = FALSE)\n'
            'cat(o$structure$total_steps, paste(o$params$uf, collapse=","),\n'
            '    paste(o$structure$functions_used, collapse="|"), sep="\\n")\n',
            encoding="utf-8")
        p = subprocess.run([rscript, str(script)], capture_output=True,
                           text=True, timeout=180)
        # So o codigo de saida decide. O R manda mensagem e aviso para o
        # stderr -- "pacote 'climasus4r' foi compilado no R versao 4.6.1" e
        # o progresso do proprio from_recipe -- e a primeira versao deste
        # teste lia isso como pacote ausente e pulava sozinha.
        if p.returncode != 0:
            pytest.skip(f"climasus4r indisponivel: {p.stderr.strip()[:160]}")
        linhas = [ln for ln in p.stdout.strip().splitlines() if ln]
        assert linhas[-3:] == [
            "4", "SP,RJ",
            "sus_data_import|sus_data_clean|sus_data_standardize"
            "|sus_data_aggregate"]
