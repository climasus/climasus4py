"""M17, M18 e M19: o contrato de leitura e gravacao contra o do R.

O que cada um cobria, e onde este arquivo para:

* **M18** -- o `sus_data_read` do Python lia UM arquivo, e o do R le
  varios, de diretorio, glob ou lista, com sete parametros. Seis
  faltavam. Fechado em 21/09/2026.
* **M17** -- a parte que destruia dado (o `overwrite` invertido) foi
  corrigida em 09/09/2026 e esta coberta em `test_export.py`. Ficavam de
  fora `compression_level`, `lang` e `verbose`.
* **M19** -- ha dois caminhos de gravacao de parquet e o mais obvio perde
  a metadata. Entrou aqui tambem a metade que era promessa falsa: a
  docstring dizia aceitar `pandas.DataFrame` e o codigo levantava
  `TypeError`.
* **M115** -- nao havia funcao publica de DataFrame para relacao, e por
  isso a mensagem de erro do `sus_export` tinha de citar modulo interno.

**A DECISAO DE METADATA foi tomada em 21/09/2026** (D1), pelo Andrey, a
quem o Marlon repassou as decisoes de coordenacao. Havia tres mecanismos
possiveis: o `COPY TO` do `sus_export`, que nao leva nada; o
`sus_meta(to_parquet=)`, que embute no schema do Parquet; e o sidecar
`<base>_metadata.txt` do R. A escolha: **opt-in, pelo embutido**. O
default segue rapido e sem metadata, o `include_metadata=True` desvia
pelo caminho embutido, e o sidecar do R e **lido e nunca escrito** --
ler custa pouco e e o unico jeito de nao perder o historico de quem
exportou no R; escrever seria o terceiro mecanismo.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import climasus4py as cs
from climasus4py.core.engine import get_connection


def _rel(**cols):
    return get_connection().from_df(pd.DataFrame(cols))


@pytest.fixture
def pasta(tmp_path):
    """Duas parquet de esquema diferente, um csv e um sidecar do R."""
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(
        tmp_path / "f1.parquet")
    pd.DataFrame({"a": [3], "c": [9.5]}).to_parquet(tmp_path / "f2.parquet")
    (tmp_path / "c1.csv").write_text("a,b\n7,z\n", encoding="utf-8")
    (tmp_path / "f1_metadata.txt").write_text("stage: raw\n", encoding="utf-8")
    return tmp_path


# --------------------------------------------------------------------------
# M18 -- sus_data_read
# --------------------------------------------------------------------------

class TestLeituraDeVariosArquivos:

    def test_um_arquivo_continua_funcionando(self, pasta):
        rel = cs.sus_data_read(pasta / "f1.parquet", verbose=False)
        assert type(rel).__name__ == "DuckDBPyRelation"
        assert rel.count("*").fetchone()[0] == 2

    def test_lista_de_caminhos(self, pasta):
        rel = cs.sus_data_read(
            [pasta / "f1.parquet", pasta / "f2.parquet"], verbose=False)
        assert rel.count("*").fetchone()[0] == 3

    def test_glob(self, pasta):
        rel = cs.sus_data_read(str(pasta / "*.parquet"), verbose=False)
        assert rel.count("*").fetchone()[0] == 3

    def test_diretorio_com_o_default(self, pasta):
        """M114: aqui funciona, e no R o mesmo caso ABORTA.

        O R monta o filtro com paste0("\\\\.", format, "$"), entao o
        default format = NULL da "\\\\.$" -- regex de nome terminado em
        ponto literal, que nao casa com arquivo nenhum. MEDIDO no R
        4.6.0: diretorio com duas parquet devolve zero arquivos e aborta
        com "Nenhum arquivo valido encontrado para leitura". Nao
        replicado, porque abortar nao carrega informacao nenhuma e ler
        diretorio e justamente o que a documentacao do R anuncia.
        """
        rel = cs.sus_data_read(pasta, verbose=False)
        assert rel.count("*").fetchone()[0] == 4, "3 parquet + 1 csv"

    def test_diretorio_e_recursivo(self, tmp_path):
        sub = tmp_path / "uf" / "sp"
        sub.mkdir(parents=True)
        pd.DataFrame({"a": [1]}).to_parquet(sub / "f.parquet")
        assert cs.sus_data_read(
            tmp_path, verbose=False).count("*").fetchone()[0] == 1

    def test_junta_por_NOME_e_nao_por_posicao(self, pasta):
        """A garantia que impede corromper valor.

        O `bind_rows` do R junta por nome. O `read_parquet` do DuckDB
        junta por POSICAO por default, o que despejaria a coluna de um
        arquivo na do outro quando a ordem do esquema difere -- valor
        errado, sem erro. Com union_by_name, a coluna ausente vem nula.
        """
        df = cs.sus_data_read(
            [pasta / "f1.parquet", pasta / "f2.parquet"],
            verbose=False).df().sort_values("a").reset_index(drop=True)
        assert list(df.columns) == ["a", "b", "c"]
        assert df.loc[df["a"] == 3, "b"].isna().all(), "b nao existe em f2"
        assert df.loc[df["a"] == 3, "c"].iloc[0] == 9.5
        assert df.loc[df["a"] == 1, "c"].isna().all(), "c nao existe em f1"

    def test_ordem_do_esquema_trocada_nao_mistura_coluna(self, tmp_path):
        """A contrapartida do teste acima, com o caso que de fato quebra.

        Duas parquet com as MESMAS colunas em ordem invertida. Por
        posicao, o valor de `a` de um arquivo cairia em `b`.
        """
        pd.DataFrame({"a": [1], "b": ["x"]}).to_parquet(tmp_path / "1.parquet")
        pd.DataFrame({"b": ["y"], "a": [2]}).to_parquet(tmp_path / "2.parquet")
        df = cs.sus_data_read(tmp_path, verbose=False).df()
        assert set(df.loc[df["a"] == 1, "b"]) == {"x"}
        assert set(df.loc[df["a"] == 2, "b"]) == {"y"}
        assert df["a"].dtype.kind in "iu", "a continua numerica"

    def test_mistura_parquet_e_csv_como_o_r(self, pasta):
        """O R le formatos misturados num diretorio; recusar divergiria."""
        rel = cs.sus_data_read(pasta, verbose=False)
        assert set(rel.columns) == {"a", "b", "c"}
        assert 7 in set(rel.df()["a"]), "a linha do csv entrou"

    def test_sidecar_de_metadata_do_r_e_ignorado(self, pasta):
        """O R tambem exclui *_metadata.txt da lista."""
        rel = cs.sus_data_read(pasta, verbose=False)
        assert rel.count("*").fetchone()[0] == 4, "o .txt nao virou linha"

    def test_format_filtra_a_varredura(self, pasta):
        assert cs.sus_data_read(
            pasta, format="parquet", verbose=False
        ).count("*").fetchone()[0] == 3
        assert cs.sus_data_read(
            pasta, format="csv", verbose=False
        ).count("*").fetchone()[0] == 1

    def test_continua_preguicoso(self, pasta):
        """Principio 1 do CLAUDE.md, inclusive no caminho de varios."""
        for alvo in (pasta, [pasta / "f1.parquet", pasta / "f2.parquet"]):
            assert type(cs.sus_data_read(alvo, verbose=False)).__name__ \
                == "DuckDBPyRelation"

    def test_de_duplica_o_mesmo_arquivo(self, pasta):
        rel = cs.sus_data_read(
            [pasta / "f1.parquet", pasta / "f1.parquet"], verbose=False)
        assert rel.count("*").fetchone()[0] == 2, "nao duplicou as linhas"

    def test_caminho_inexistente_avisa_e_nao_aborta(self, pasta):
        """Como no R: avisa por caminho e so aborta se nada sobrar."""
        with pytest.warns(UserWarning, match="nao encontrado"):
            rel = cs.sus_data_read(
                [pasta / "f1.parquet", pasta / "sumiu.parquet"])
        assert rel.count("*").fetchone()[0] == 2

    def test_nada_encontrado_e_erro(self, tmp_path):
        with pytest.raises(ValueError, match="No readable file found"):
            cs.sus_data_read(tmp_path, verbose=False)

    def test_format_invalido(self, pasta):
        with pytest.raises(ValueError, match="Unsupported format"):
            cs.sus_data_read(pasta, format="xlsx", verbose=False)

    def test_format_que_so_o_r_le(self, pasta):
        with pytest.raises(ValueError, match="read by climasus4r"):
            cs.sus_data_read(pasta, format="rds", verbose=False)

    @pytest.mark.parametrize("lang", ["pt", "en", "es"])
    def test_os_tres_idiomas(self, pasta, lang, capsys):
        cs.sus_data_read(pasta / "f1.parquet", lang=lang, verbose=True)
        assert "climasus4py" in capsys.readouterr().out

    def test_idioma_invalido_avisa_e_cai_no_pt(self, pasta, capsys):
        with pytest.warns(UserWarning, match="Unsupported lang"):
            cs.sus_data_read(pasta / "f1.parquet", lang="fr", verbose=True)
        assert "Leitor de Dados" in capsys.readouterr().out

    def test_verbose_falso_nao_imprime(self, pasta, capsys):
        cs.sus_data_read(pasta / "f1.parquet", verbose=False)
        assert capsys.readouterr().out == ""

    def test_parallel_devolve_o_threads_ao_valor_anterior(self, pasta):
        """O parametro mexe numa conexao SINGLETON.

        Sem restaurar, um `parallel=True` mudaria o paralelismo de todo o
        resto da sessao -- efeito colateral global a partir de um
        argumento de leitura.
        """
        conn = get_connection()
        antes = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        cs.sus_data_read(pasta, parallel=True, workers=2, verbose=False)
        depois = conn.execute(
            "SELECT current_setting('threads')").fetchone()[0]
        assert depois == antes, f"threads ficou em {depois}, era {antes}"

    def test_read_metadata_le_o_sidecar_da_pasta(self, pasta):
        """Este teste cobrava o NotImplementedError, e a D1 o resolveu.

        Enquanto a decisao de mecanismo estava aberta, a bandeira
        LEVANTAVA de proposito -- aceitar e devolver relacao sem metadata
        nenhuma seria o pior dos mundos, porque o chamador pediu
        provenancia e receberia silencio. Decidido em 21/09/2026: le o
        embutido e cai para o sidecar do R.

        A fixture tem `f1_metadata.txt` com `stage: raw`, que e sidecar do
        R -- excluido da varredura de DADOS e lido como metadata.
        """
        rel = cs.sus_data_read(pasta / "f1.parquet", read_metadata=True,
                               verbose=False)
        assert cs.sus_meta(rel)["stage"] == "raw"


# --------------------------------------------------------------------------
# M17 -- os parametros que faltavam no sus_export
# --------------------------------------------------------------------------

class TestParametrosDoExport:

    def test_compression_level_no_zstd_chega_ao_duckdb(self, tmp_path):
        """O nivel altera o arquivo -- mas NAO de forma monotonica.

        A primeira versao deste teste cobrava nivel 19 menor que nivel 1,
        que e a intuicao, e falhou. MEDIDO no DuckDB 1.5.3, para 2.000
        linhas repetitivas: {1: 2979, 3: 3893, 6: 2533, 9: 2533,
        12: 5372, 15: 4413, 19: 5542, 22: 5542} bytes -- nivel mais alto
        chega a produzir arquivo MAIOR. O mesmo padrao nao-monotonico
        aparece com dado aleatorio. Registrado como M116.

        O que se pode garantir, e o que se testa, e que o parametro
        chega ao COPY TO e muda o resultado. Cobrar a direcao seria
        cobrar do DuckDB algo que ele nao promete.
        """
        rel = _rel(a=list(range(2000)), b=["texto repetido"] * 2000)
        tamanhos = {}
        for lvl in (1, 6, 19):
            p = cs.sus_export(rel, tmp_path / f"z{lvl}.parquet",
                              compress="zstd", compression_level=lvl,
                              overwrite=True)
            tamanhos[lvl] = p.stat().st_size
        assert len(set(tamanhos.values())) > 1, (
            f"o nivel nao surtiu efeito nenhum: {tamanhos}")

    def test_o_nivel_nao_corrompe_o_que_foi_gravado(self, tmp_path):
        """Contrapartida: comprimir mais nao pode perder dado."""
        rel = _rel(a=list(range(500)), b=["v"] * 500)
        for lvl in (1, 22):
            p = cs.sus_export(rel, tmp_path / f"c{lvl}.parquet",
                              compress="zstd", compression_level=lvl,
                              overwrite=True)
            volta = cs.sus_data_read(p, verbose=False).df()
            assert len(volta) == 500
            assert list(volta["a"]) == list(range(500))

    def test_nivel_com_codec_sem_nivel_avisa_e_grava(self, tmp_path):
        """O DuckDB so aceita nivel no zstd, e erra nos outros.

        Deixar o Binder Error subir transformaria um parametro de ajuste
        fino em falha de gravacao. O aviso diz o que fazer e o arquivo
        sai.
        """
        rel = _rel(a=[1, 2])
        with pytest.warns(UserWarning, match="only for zstd"):
            p = cs.sus_export(rel, tmp_path / "s.parquet",
                              compression_level=9, overwrite=True)
        assert p.is_file() and p.stat().st_size > 0

    def test_nivel_nao_avisa_quando_nao_foi_pedido(self, tmp_path):
        rel = _rel(a=[1])
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cs.sus_export(rel, tmp_path / "q.parquet", overwrite=True)

    @pytest.mark.parametrize("lvl", [0, 23, -1])
    def test_nivel_fora_de_faixa(self, tmp_path, lvl):
        with pytest.raises(ValueError, match="between 1 and 22"):
            cs.sus_export(_rel(a=[1]), tmp_path / "x.parquet",
                          compression_level=lvl, overwrite=True)

    def test_verbose_imprime_o_que_gravou(self, tmp_path, capsys):
        cs.sus_export(_rel(a=[1]), tmp_path / "v.parquet", verbose=True,
                      overwrite=True)
        saida = capsys.readouterr().out
        assert "v.parquet" in saida and "KB" in saida

    def test_verbose_e_falso_por_default(self, tmp_path, capsys):
        """Diverge do R, que tem verbose = TRUE, e de proposito.

        O sus_export e chamado por dentro do sus_pipeline; imprimir por
        default poluiria a saida de quem nunca chamou esta funcao.
        """
        cs.sus_export(_rel(a=[1]), tmp_path / "q.parquet", overwrite=True)
        assert capsys.readouterr().out == ""

    @pytest.mark.parametrize("lang,trecho", [
        ("pt", "gravado"), ("en", "wrote"), ("es", "grabado")])
    def test_os_tres_idiomas(self, tmp_path, capsys, lang, trecho):
        cs.sus_export(_rel(a=[1]), tmp_path / f"{lang}.parquet", lang=lang,
                      verbose=True, overwrite=True)
        assert trecho in capsys.readouterr().out

    def test_idioma_invalido_avisa(self, tmp_path):
        with pytest.warns(UserWarning, match="Unsupported lang"):
            cs.sus_export(_rel(a=[1]), tmp_path / "f.parquet", lang="de",
                          verbose=False, overwrite=True)


# --------------------------------------------------------------------------
# M19 -- a promessa falsa, e o que fica em aberto
# --------------------------------------------------------------------------

class TestPromessaDoDataFrame:

    def test_a_recusa_diz_como_converter(self, tmp_path):
        """Antes a mensagem parava em "only accepts lazy DuckDB relations".

        Verdadeiro e inutil: o jeito comum de chegar aqui e ter chamado
        .df() para inspecionar e tentar exportar em seguida.
        """
        with pytest.raises(TypeError) as exc:
            cs.sus_export(pd.DataFrame({"a": [1]}), tmp_path / "df.parquet")
        msg = str(exc.value)
        assert "sus_as_relation" in msg
        assert "sus_as_duckdb" not in msg, (
            "sus_as_duckdb recebe relacao, nao DataFrame -- nao serve de dica")
        assert "get_connection" not in msg, (
            "a dica citava modulo interno enquanto faltava a funcao publica "
            "(M115); agora existe sus_as_relation")

    def test_a_dica_funciona_de_verdade(self, tmp_path):
        """O caminho que a mensagem indica tem de gravar."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        p = cs.sus_export(cs.sus_as_relation(df), tmp_path / "ok.parquet")
        assert cs.sus_data_read(p, verbose=False).count(
            "*").fetchone()[0] == 2

    def test_a_docstring_nao_promete_mais_dataframe(self):
        doc = cs.sus_export.__doc__ or ""
        assert "is refused" in doc
        assert "Accepts lazy DuckDB relations and ``pandas.DataFrame``" \
            not in doc

    def test_outro_tipo_nao_ganha_a_dica_do_dataframe(self, tmp_path):
        with pytest.raises(TypeError) as exc:
            cs.sus_export([1, 2, 3], tmp_path / "l.parquet")
        assert "from_df" not in str(exc.value)


class TestIdaEVolta:

    def test_grava_e_le_de_volta_em_lote(self, tmp_path):
        """O par das duas funcoes, que e o caso de uso do M18."""
        for ano in (2021, 2022, 2023):
            cs.sus_export(_rel(ano=[ano] * 3, v=[1.0, 2.0, 3.0]),
                          tmp_path / f"SP_{ano}.parquet")
        rel = cs.sus_data_read(tmp_path, verbose=False)
        assert rel.count("*").fetchone()[0] == 9
        assert set(rel.df()["ano"]) == {2021, 2022, 2023}

    def test_csv_tambem_fecha_o_ciclo(self, tmp_path):
        cs.sus_export(_rel(a=[1, 2], b=["x", "y"]), tmp_path / "d.csv")
        rel = cs.sus_data_read(tmp_path / "d.csv", verbose=False)
        assert rel.count("*").fetchone()[0] == 2
        assert set(rel.columns) == {"a", "b"}


# --------------------------------------------------------------------------
# D1 -- um mecanismo de metadata, e o sidecar do R lido mas nunca escrito
# --------------------------------------------------------------------------

META = {"system": "SIM-DO", "stage": "stand", "type": "stand",
        "history": ["importado no teste"]}


def _com_meta():
    return cs.sus_as_relation(pd.DataFrame({"a": [1, 2, 3]}), meta=META)


class TestMetadataNoExport:
    """M17/M19: a decisao foi opt-in, com o mecanismo EMBUTIDO.

    Havia tres mecanismos possiveis -- o COPY TO do sus_export, que nao
    leva nada; o sus_meta(to_parquet=), que embute no schema do Parquet; e
    o sidecar <base>_metadata.txt do R. A escolha, de 21/09/2026: o
    default segue rapido e sem metadata, o include_metadata=True desvia
    pelo caminho embutido, e o sidecar do R e LIDO e nunca escrito.

    Gravar metadata por default custaria a materializacao via Arrow, que e
    exatamente a vantagem pela qual o sus_export existe.
    """

    def test_o_default_e_falso_e_continua_avisando(self, tmp_path):
        with pytest.warns(UserWarning, match="without its sus_meta"):
            p = cs.sus_export(_com_meta(), tmp_path / "sem.parquet")
        volta = cs.sus_data_read(p, read_metadata=True, verbose=False)
        assert cs.sus_meta(volta) is None

    def test_o_aviso_aponta_o_parametro_e_nao_outra_funcao(self, tmp_path):
        """Antes mandava usar sus_meta(to_parquet=), que e outra chamada.

        Com o parametro existindo, a saida acionavel e o parametro.
        """
        with pytest.warns(UserWarning) as rec:
            cs.sus_export(_com_meta(), tmp_path / "sem.parquet")
        assert "include_metadata=True" in str(rec[0].message)

    def test_include_metadata_embute_e_volta(self, tmp_path):
        p = cs.sus_export(_com_meta(), tmp_path / "com.parquet",
                          include_metadata=True)
        m = cs.sus_meta(cs.sus_data_read(p, read_metadata=True,
                                         verbose=False))
        assert m is not None
        assert m["system"] == "SIM-DO"
        assert m["stage"] == "stand"

    def test_include_metadata_nao_avisa_de_perda(self, tmp_path):
        """Nao pode avisar que perdeu o que acabou de gravar."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cs.sus_export(_com_meta(), tmp_path / "com.parquet",
                          include_metadata=True)

    def test_o_dado_e_o_mesmo_pelos_dois_caminhos(self, tmp_path):
        """A contrapartida: o caminho lento nao pode mudar o dado.

        Um desvia pelo COPY TO e o outro pelo Arrow -- se divergissem no
        conteudo, o parametro estaria trocando metadata por dado.
        """
        rel = cs.sus_as_relation(
            pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), meta=META)
        rapido = cs.sus_export(rel, tmp_path / "r.parquet")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            lento = cs.sus_export(rel, tmp_path / "l.parquet",
                                  include_metadata=True)
        pd.testing.assert_frame_equal(
            cs.sus_data_read(rapido, verbose=False).df(),
            cs.sus_data_read(lento, verbose=False).df())

    def test_csv_avisa_e_grava_o_dado(self, tmp_path):
        """Formato sem lugar para metadata nao pode falhar a gravacao."""
        with pytest.warns(UserWarning, match="only Parquet"):
            p = cs.sus_export(_com_meta(), tmp_path / "x.csv",
                              include_metadata=True)
        assert cs.sus_data_read(p, verbose=False).count("*").fetchone()[0] == 3

    def test_relacao_sem_meta_nao_avisa(self, tmp_path):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            cs.sus_export(_rel(a=[1]), tmp_path / "q.parquet")


class TestMetadataNaLeitura:

    def test_read_metadata_falso_nao_anexa(self, tmp_path):
        p = cs.sus_export(_com_meta(), tmp_path / "c.parquet",
                          include_metadata=True)
        assert cs.sus_meta(cs.sus_data_read(p, verbose=False)) is None

    def test_o_sidecar_do_r_e_lido(self, tmp_path):
        """A ponte que faz o arquivo do R chegar aqui com procedencia.

        O climasus4r grava <base>_metadata.txt com "chave: valor" por
        linha. Nao escrevemos esse arquivo -- seria o terceiro mecanismo
        -- mas ler custa pouco, e e o unico jeito de nao perder o
        historico de quem exportou no R.
        """
        p = tmp_path / "doR.parquet"
        pd.DataFrame({"a": [1, 2]}).to_parquet(p)
        (tmp_path / "doR_metadata.txt").write_text(
            "system: SIM-DO\nstage: stand\nrows: 2\n", encoding="utf-8")
        m = cs.sus_meta(cs.sus_data_read(p, read_metadata=True,
                                         verbose=False))
        assert m["system"] == "SIM-DO"
        assert m["rows"] == "2"

    def test_linha_torta_no_sidecar_nao_derruba_a_leitura(self, tmp_path):
        p = tmp_path / "t.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(p)
        (tmp_path / "t_metadata.txt").write_text(
            "system: SIM-DO\nlinha sem dois pontos\n: chave vazia\n",
            encoding="utf-8")
        m = cs.sus_meta(cs.sus_data_read(p, read_metadata=True,
                                         verbose=False))
        assert m["system"] == "SIM-DO"
        assert "" not in m, "chave vazia nao entra"

    def test_o_embutido_vence_o_sidecar(self, tmp_path):
        """Os dois presentes: o schema e a fonte deste pacote."""
        p = cs.sus_export(_com_meta(), tmp_path / "dois.parquet",
                          include_metadata=True)
        (tmp_path / "dois_metadata.txt").write_text(
            "system: OUTRO\n", encoding="utf-8")
        m = cs.sus_meta(cs.sus_data_read(p, read_metadata=True,
                                         verbose=False))
        assert m["system"] == "SIM-DO", "o sidecar sobrepujou o schema"

    def test_o_sidecar_nunca_e_escrito(self, tmp_path):
        """A regra que evita o terceiro mecanismo."""
        cs.sus_export(_com_meta(), tmp_path / "e.parquet",
                      include_metadata=True)
        assert not list(tmp_path.glob("*_metadata.txt"))

    def test_registra_no_historico_de_onde_veio(self, tmp_path):
        p = cs.sus_export(_com_meta(), tmp_path / "h.parquet",
                          include_metadata=True)
        m = cs.sus_meta(cs.sus_data_read(p, read_metadata=True,
                                         verbose=False))
        assert any("read_metadata" in h for h in m["history"])

    def test_varios_arquivos_com_meta_avisam_e_usam_o_primeiro(self, tmp_path):
        """Metadata descreve UM pipeline; fundir descreveria nenhum."""
        for i, sistema in enumerate(("SIM-DO", "SIH-RD")):
            rel = cs.sus_as_relation(pd.DataFrame({"a": [i]}),
                                     meta={**META, "system": sistema})
            cs.sus_export(rel, tmp_path / f"{i}_f.parquet",
                          include_metadata=True)
        with pytest.warns(UserWarning, match="carry metadata"):
            rel = cs.sus_data_read(tmp_path, read_metadata=True,
                                   verbose=False)
        assert cs.sus_meta(rel)["system"] == "SIM-DO", "o primeiro vence"

    def test_sem_metadata_nenhuma_nao_avisa(self, tmp_path):
        pd.DataFrame({"a": [1]}).to_parquet(tmp_path / "puro.parquet")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            rel = cs.sus_data_read(tmp_path / "puro.parquet",
                                   read_metadata=True, verbose=False)
        assert cs.sus_meta(rel) is None

    def test_read_metadata_nao_levanta_mais(self, tmp_path):
        """Fixa a mudanca: antes era NotImplementedError de proposito."""
        pd.DataFrame({"a": [1]}).to_parquet(tmp_path / "x.parquet")
        cs.sus_data_read(tmp_path / "x.parquet", read_metadata=True,
                         verbose=False)


# --------------------------------------------------------------------------
# D8 / M115 -- a funcao publica que faltava
# --------------------------------------------------------------------------

class TestSusAsRelation:
    """M115: a familia sus_as_* so saia de relacao, nunca entrava.

    Conferido percorrendo as entradas do __all__: sus_as_arrow e
    sus_as_duckdb recebem relacao, e o sus_sql recusa DataFrame no
    primeiro argumento. O unico caminho era get_connection().from_df(df),
    de um modulo interno -- tanto que a mensagem de erro do sus_export
    precisava cita-lo para ser acionavel.
    """

    def test_esta_exportada(self):
        assert hasattr(cs, "sus_as_relation")
        assert "sus_as_relation" in cs.__all__

    def test_dataframe_vira_relacao_preguicosa(self):
        rel = cs.sus_as_relation(pd.DataFrame({"a": [1, 2]}))
        assert type(rel).__name__ == "DuckDBPyRelation"
        assert rel.count("*").fetchone()[0] == 2

    def test_relacao_passa_direto(self):
        rel = _rel(a=[1])
        assert cs.sus_as_relation(rel) is rel

    def test_arrow_vira_relacao(self):
        import pyarrow as pa

        rel = cs.sus_as_relation(pa.table({"b": [1, 2, 3]}))
        assert rel.count("*").fetchone()[0] == 3

    def test_fecha_o_ciclo_com_sus_as_arrow(self):
        """A metadata do schema do Arrow volta na relacao."""
        rel = cs.sus_as_relation(cs.sus_as_arrow(_com_meta()))
        m = cs.sus_meta(rel)
        assert m is not None and m["system"] == "SIM-DO"

    def test_meta_explicita_sobrepoe_a_do_arrow(self):
        rel = cs.sus_as_relation(cs.sus_as_arrow(_com_meta()),
                                 meta={**META, "system": "SIH-RD"})
        assert cs.sus_meta(rel)["system"] == "SIH-RD"

    def test_tipo_impossivel_diz_o_que_aceita(self):
        with pytest.raises(TypeError, match="pandas.DataFrame"):
            cs.sus_as_relation(42)

    def test_e_o_caminho_que_o_sus_export_indica(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2]})
        with pytest.raises(TypeError) as exc:
            cs.sus_export(df, tmp_path / "x.parquet")
        assert "sus_as_relation" in str(exc.value)
        p = cs.sus_export(cs.sus_as_relation(df), tmp_path / "x.parquet")
        assert cs.sus_data_read(p, verbose=False).count("*").fetchone()[0] == 2
