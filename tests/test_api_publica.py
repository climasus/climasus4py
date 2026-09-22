"""A superficie publica do pacote confere com a planilha de paridade.

Por que este arquivo existe: as tres funcoes bayesianas ficaram
**inalcancaveis** depois de portadas. O codigo estava pronto, os 98 testes
delas passavam -- porque importavam o modulo direto -- e a planilha dizia
Completa, mas `climasus4py.sus_mod_spatial_bayes` nao existia, porque
ninguem as ligou ao `__init__.py`. Passou por 1447 testes sem ser notado, e
so apareceu num `hasattr` avulso durante os commits.

Um teste por modulo nao pega isso: cada um importa o que precisa e nao
tem opiniao sobre o namespace do pacote. Este tem.

Vale para o `sus_rap_run` em especial: ele procura cada `function_name` da
receita no namespace publico, entao uma funcao fora do `__all__` nao pode
ser executada por receita nenhuma.
"""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path

import pytest

import climasus4py as cs

PLANILHA = (Path(__file__).parent.parent / "docs" / "controle"
            / "PARIDADE_R_PYTHON_v7.csv")

IDENTIFICADOR = re.compile(r"^sus_[a-z0-9_]+$")


def _declaradas() -> list[tuple[str, str, str]]:
    """As funcoes Python que a planilha declara, com funcao R e status.

    Uma celula pode trazer varios nomes separados por `|`, e pode trazer
    notacao de chamada -- `sus_meta(rel, to_duckdb=...)` -- que descreve
    um parametro e nao um nome exportavel. So os identificadores puros
    entram.
    """
    if not PLANILHA.is_file():
        return []
    linhas = list(csv.reader(io.StringIO(
        PLANILHA.read_text(encoding="utf-8-sig"), newline="")))
    cab = {c: i for i, c in enumerate(linhas[0])}
    saida = []
    for linha in linhas[1:]:
        status = linha[cab["Status"]]
        if status.startswith("Nao se aplica"):
            continue
        for nome in linha[cab["Funcao Python"]].split("|"):
            nome = nome.strip()
            if IDENTIFICADOR.match(nome):
                saida.append((nome, linha[cab["Funcao R"]], status))
    return saida


DECLARADAS = _declaradas()


def _planilha() -> list[list[str]]:
    """As linhas cruas da planilha, cabecalho incluido."""
    if not PLANILHA.is_file():
        return [[]]
    return list(csv.reader(io.StringIO(
        PLANILHA.read_text(encoding="utf-8-sig"), newline="")))


#: Indice de coluna por nome, para as leituras crus abaixo.
_COL: dict[str, int] = {c: i for i, c in enumerate(_planilha()[0])}


@pytest.mark.skipif(not PLANILHA.is_file(), reason="planilha ausente")
class TestPlanilhaContraNamespace:

    def test_a_planilha_foi_encontrada_e_tem_conteudo(self):
        """Sem esta, as duas abaixo passariam por vacuidade."""
        assert len(DECLARADAS) > 90, (
            f"so {len(DECLARADAS)} funcoes lidas da planilha -- o leitor "
            f"quebrou, e os testes de cobertura abaixo viram no-op")

    def test_toda_funcao_declarada_e_importavel(self):
        faltando = [(n, r, s) for n, r, s in DECLARADAS if not hasattr(cs, n)]
        assert not faltando, (
            "a planilha diz que estao prontas, mas nao existem no pacote:\n"
            + "\n".join(f"  {n}  (R: {r}; status: {s})"
                        for n, r, s in faltando))

    def test_toda_funcao_declarada_esta_no_all(self):
        """Importavel nao basta: o `sus_rap_run` le o `__all__`."""
        fora = [(n, s) for n, _, s in DECLARADAS
                if hasattr(cs, n) and n not in cs.__all__]
        assert not fora, (
            "importaveis mas fora do __all__:\n"
            + "\n".join(f"  {n}  (status: {s})" for n, s in fora))


class TestCoerenciaDoAll:

    def test_todo_nome_do_all_existe(self):
        fantasmas = [n for n in cs.__all__ if not hasattr(cs, n)]
        assert not fantasmas, f"nomes no __all__ que nao existem: {fantasmas}"

    def test_o_all_nao_tem_duplicata(self):
        vistos, dobrados = set(), []
        for n in cs.__all__:
            if n in vistos:
                dobrados.append(n)
            vistos.add(n)
        assert not dobrados, f"duplicados no __all__: {dobrados}"

    def test_as_doze_funcoes_de_setembro_estao_expostas(self):
        """As nove do lote de exports mais as tres bayesianas.

        Fixadas por nome, e nao so pela planilha, porque foram estas que
        motivaram o arquivo.
        """
        for nome in ("sus_mod_spatial_bayes", "sus_mod_spacetime_bayes",
                     "sus_mod_spacetime_predict", "sus_mod_spatial_scan",
                     "sus_mod_spacetime_exceedance",
                     "sus_mod_plot_spatial_scan",
                     "sus_mod_plot_spatial_bayes", "sus_mod_plot_spacetime",
                     "sus_rap_recipe", "sus_rap_from_recipe", "sus_rap_run",
                     "sus_rap_inspect"):
            assert hasattr(cs, nome), f"{nome} nao importavel"
            assert nome in cs.__all__, f"{nome} fora do __all__"

    def test_importar_o_pacote_nao_carrega_pymc(self):
        """O PyMC e importado DENTRO das funcoes, de proposito.

        Se alguem mover `import pymc` para o topo de um modulo de
        enrichment, todo `import climasus4py` passa a pagar o custo e a
        exigir o extra [bayes] -- que e opcional.
        """
        import subprocess
        import sys

        p = subprocess.run(
            [sys.executable, "-c",
             "import climasus4py, sys; print('pymc' in sys.modules)"],
            capture_output=True, text=True, timeout=300)
        assert p.returncode == 0, p.stderr[-500:]
        assert p.stdout.strip().endswith("False"), (
            "importar o pacote passou a carregar o PyMC")


# ---------------------------------------------------------------------------
# M32 — os metodos S3 do R entraram no controle de paridade
# ---------------------------------------------------------------------------

class TestMetodosS3:
    """As 72 linhas `S3method()` do NAMESPACE do R.

    A planilha foi montada a partir das 108 linhas `export()`, e os
    metodos S3 ficaram de fora — mas metodo S3 **e** interface publica no
    R: quem digita o nome do objeto para ver o `print`, ou chama
    `summary(fit)`, esta usando a API. Foi uma lacuna de ESCOPO, nao um
    defeito: a planilha estava certa sobre o que dizia cobrir, e cobria
    menos do que existe.

    Estes testes guardam o inventario. A lista de genericos e a contagem
    vem do NAMESPACE do climasus4r instalado, medidos em 22/09/2026 —
    sem depender do R em tempo de teste, que nao esta disponivel em toda
    maquina.
    """

    #: generico -> quantos metodos, do NAMESPACE do climasus4r.
    S3_ESPERADOS = {
        "print": 25, "summary": 22, "tidy": 12, "coef": 4, "[": 2,
        "vcov": 2, "$<-": 1, "[[": 1, "as.data.frame": 1, "predict": 1,
        "rbind": 1,
    }

    def _linhas_s3(self):
        return [l for l in _planilha()[1:]
                if l[_COL["Categoria"]].strip() == "Metodo S3"]

    def test_os_72_estao_na_planilha(self):
        assert len(self._linhas_s3()) == sum(self.S3_ESPERADOS.values()) == 72

    def test_a_distribuicao_por_generico_confere(self):
        """Uma contagem total certa pode esconder duas trocadas."""
        import collections

        contagem = collections.Counter(
            # rsplit: `as.data.frame.climasus_df` tem ponto no proprio
            # nome do generico, entao cortar pelo primeiro daria "as".
            l[_COL["Funcao R"]].rsplit(".", 1)[0]
            for l in self._linhas_s3()
        )
        assert dict(contagem) == self.S3_ESPERADOS

    def test_todo_metodo_tem_status(self):
        for l in self._linhas_s3():
            assert l[_COL["Status"]].strip(), l[_COL["Funcao R"]]
            assert l[_COL["Melhoria (ID)"]].strip(), l[_COL["Funcao R"]]

    def test_nenhum_metodo_duplicado(self):
        nomes = [l[_COL["Funcao R"]] for l in self._linhas_s3()]
        assert len(nomes) == len(set(nomes))

    def test_a_lacuna_real_esta_nomeada(self):
        """38 dos 72 sao `print`/`summary` sobre funcao que devolve dict.

        E o unico grupo classificado como DIVERGENTE, e o numero importa:
        se cair, alguem deu classe propria a alguma delas e a planilha
        deve acompanhar; se subir, uma classe virou dict.
        """
        divergentes = [l for l in self._linhas_s3()
                       if l[_COL["Status"]].startswith("DIVERGENTE")]
        assert len(divergentes) == 38
        for l in divergentes:
            assert l[_COL["Funcao R"]].rsplit(".", 1)[0] in ("print", "summary")
            # a lacuna e a mesma do M26, e o registro tem de dizer isso
            assert "M26" in l[_COL["Melhoria (ID)"]]
