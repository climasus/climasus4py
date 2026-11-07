"""Tests for metadata contracts used by climasus4py.

These tests must pass with both:
- newer climasus-data catalogs (with extended JSON files), and
- current PyPI climasus-data release, where some files may not exist yet.
"""

from climasus4py.core.variables import (
    _age_breaks_for_preset,
    _age_groups_config,
    _astronomical_season_sql,
    _seasonal_config,
)
from climasus4py.core.variables import sus_data_create_variables as cs_vars
from climasus4py.utils.data import (
    detect_age_column,
    detect_cause_column,
    detect_date_column,
    detect_sex_column,
    detect_system,
    load_datasus_columns_spec,
)


class TestDatasusColumnsSpec:
    def test_spec_loads(self):
        data = load_datasus_columns_spec()
        assert "all_date_columns" in data
        assert "all_numeric_columns" in data
        assert "system_signatures" in data
        assert "role_priority" in data

    def test_date_columns_count(self):
        cols = load_datasus_columns_spec()["all_date_columns"]
        assert "DTOBITO" in cols
        assert "DTNASC" in cols
        assert len(cols) >= 18

    def test_numeric_columns_are_only_quantities(self):
        """M64: a lista unica de 23 virou tres.

        Este teste cobrava `len(cols) >= 23` e `CODMUNRES in cols`, ou
        seja, cobrava exatamente o defeito: que identificador estivesse
        entre os numericos. Agora cobra o contrario.
        """
        spec = load_datasus_columns_spec()
        assert int(spec["schema_version"]) >= 2
        cols = spec["all_numeric_columns"]
        assert "CONTADOR" in cols and "PESO" in cols
        for identificador in ("CODMUNRES", "CODESTAB", "CODMUNOCOR",
                              "CODMUNNATU", "CODOCUPMAE"):
            assert identificador not in cols, (
                f"{identificador} e codigo de largura fixa: numerico perde "
                f"o zero a esquerda")
        for categorico in ("LOCOCOR", "ESCMAE", "GESTACAO", "OBITOGRAV"):
            assert categorico not in cols, (
                f"{categorico} tem livro de rotulos e precisa casar como "
                f"texto")

    def test_as_tres_listas_nao_se_sobrepoem(self):
        spec = load_datasus_columns_spec()
        q = set(spec["all_numeric_columns"])
        i = set(spec["all_identifier_columns"])
        c = set(spec["all_categorical_columns"])
        assert not q & i and not q & c and not i & c, (
            f"sobreposicao: Q&I={q & i}, Q&C={q & c}, I&C={i & c}")

    def test_toda_largura_declarada_e_positiva(self):
        for col, w in load_datasus_columns_spec()[
                "all_identifier_columns"].items():
            assert w is None or (isinstance(w, int) and w > 0), (col, w)

    def test_graession_saiu(self):
        """Nao existe em dado nenhum: nem no SIM-DO real, nem no R.

        Conferido contra os tres SIM-DO em cache (87 colunas cada), os 671
        campos dos dicionarios do climasus4r e os tres columns.json.
        """
        spec = load_datasus_columns_spec()
        todas = (set(spec["all_numeric_columns"])
                 | set(spec["all_identifier_columns"])
                 | set(spec["all_categorical_columns"]))
        assert "GRAESSION" not in todas


class TestDetectSystem:
    def test_sim_do_via_causabas(self):
        assert detect_system(["CAUSABAS", "IDADE"]) == "SIM-DO"

    def test_sim_do_via_dtobito(self):
        assert detect_system(["DTOBITO", "CODMUNRES"]) == "SIM-DO"

    def test_sinasc(self):
        assert detect_system(["NUMERODN", "IDADEMAE"]) == "SINASC"

    def test_sih(self):
        assert detect_system(["DIAG_PRINC", "CODMUNRES"]) == "SIH-RD"

    def test_sinan(self):
        assert detect_system(["NU_NOTIFIC", "SEXO"]) == "SINAN-DENGUE"

    def test_unknown_returns_none(self):
        assert detect_system(["COLUNA_DESCONHECIDA"]) is None

    def test_empty_returns_none(self):
        assert detect_system([]) is None


class TestRolePriorityDetection:
    def test_detect_date_dtobito(self):
        assert detect_date_column(["DTOBITO", "CAUSABAS"]) == "DTOBITO"

    def test_detect_date_standardized(self):
        assert detect_date_column(["death_date", "DTOBITO"]) == "death_date"

    def test_detect_date_none(self):
        assert detect_date_column(["CODMUNRES"]) is None

    def test_detect_cause_causabas(self):
        assert detect_cause_column(["CAUSABAS", "IDADE"]) == "CAUSABAS"

    def test_detect_age_idade(self):
        assert detect_age_column(["IDADE", "SEXO"]) == "IDADE"

    def test_detect_sex_sexo(self):
        assert detect_sex_column(["SEXO", "IDADE"]) == "SEXO"

    def test_detect_sex_cs_sexo(self):
        assert detect_sex_column(["CS_SEXO", "NU_NOTIFIC"]) == "CS_SEXO"


class TestAgeGroupsConfig:
    def test_config_loads(self):
        data = _age_groups_config()
        assert "presets" in data
        assert "default" in data

    def test_preset_who_loaded(self):
        breaks = _age_breaks_for_preset("who")
        assert breaks[0] == 0
        assert breaks[-1] == 999

    def test_preset_decadal_loaded(self):
        breaks = _age_breaks_for_preset("decadal")
        assert 10 in breaks
        assert 20 in breaks

    def test_preset_epid_default(self):
        breaks = _age_breaks_for_preset("epidemiological_default")
        assert breaks == [0, 5, 15, 60, 999]

    def test_unknown_preset_is_refused(self):
        """Preset desconhecido recusa em vez de cair no default (M53, 09/09).

        A assercao antiga era ``breaks == [0, 18, 65, 999]`` -- um valor que
        nao corresponde a nenhum dos cinco presets do config, resto de um
        AGE_BREAKS_DEFAULT anterior. Mas o defeito nao era o numero: era o
        fallback existir. O unico chamador e ``sus_pipeline(age_group=...)``,
        entao um typo em "who" devolvia as faixas epidemiologicas em silencio
        -- faixas diferentes das pedidas, sem nenhum sinal.
        """
        import pytest

        with pytest.raises(ValueError, match="Unknown age_group preset"):
            _age_breaks_for_preset("nonexistent")

    def test_every_preset_in_the_config_resolves(self):
        """Contrapartida: os nomes validos continuam todos resolvendo."""
        presets = _age_groups_config()["presets"]
        for nome in presets:
            breaks = _age_breaks_for_preset(nome)
            assert breaks, f"preset {nome} devolveu vazio"
            assert breaks == sorted(breaks), f"preset {nome} fora de ordem"
            assert all(isinstance(b, int) for b in breaks)


class TestSeasonalPatternsConfig:
    """O config de estacoes e o SQL que sai dele.

    Atualizado em 09/09/2026 (M53). Estes testes fixavam nomes e um formato que
    o pacote mudou, e o ImportError de ``_season_case_sql`` derrubava a COLETA
    da suite inteira. Tres coisas mudaram de verdade:

    - ``_seasonal_patterns_config`` virou ``_seasonal_config``, e ``patterns``
      deixou de ser chave de topo: agora vive sob ``astronomical``, ao lado de
      ``climatic``, que e a outra familia de estacoes.
    - ``_season_case_sql`` virou ``_astronomical_season_sql`` e passou a
      receber uma expressao de MES, nao de data -- quem monta o
      ``EXTRACT(MONTH FROM ...)`` agora e o chamador.
    - o hemisferio invalido nao cai mais num default silencioso. Ver
      ``test_public_function_validates_hemisphere``.
    """

    def test_config_loads(self):
        data = _seasonal_config()
        assert "patterns" in data["astronomical"]
        assert "south" in data["astronomical"]["patterns"]
        assert "north" in data["astronomical"]["patterns"]

    def test_south_summer_months(self):
        padroes = _seasonal_config()["astronomical"]["patterns"]
        assert set(padroes["south"]["summer"]) == {12, 1, 2}

    def test_north_winter_months(self):
        padroes = _seasonal_config()["astronomical"]["patterns"]
        assert set(padroes["north"]["winter"]) == {12, 1, 2}

    def test_season_sql_south_contains_summer(self):
        sql = _astronomical_season_sql("EXTRACT(MONTH FROM d)", hemisphere="south")
        assert "Summer" in sql
        # Afirmar sobre os meses, nao sobre o espacamento do SQL gerado: a
        # assercao antiga era "12, 1, 2" e quebrou quando a formatacao passou
        # a ser "12,1,2", sem que nada de substantivo tivesse mudado.
        assert "12,1,2" in sql.replace(" ", "")

    def test_season_sql_north_summer_different(self):
        sul = _astronomical_season_sql("m", hemisphere="south")
        norte = _astronomical_season_sql("m", hemisphere="north")
        assert sul != norte
        # E a diferenca tem de ser a certa: no sul dezembro e verao, no norte
        # e inverno. Sem isso, o teste passaria com qualquer divergencia.
        assert sul.split("THEN")[1].strip().startswith("'Summer'")
        assert norte.split("THEN")[1].strip().startswith("'Winter'")

    def test_public_function_validates_hemisphere(self):
        """O helper privado assume hemisferio valido; quem valida e a publica.

        Substitui o antigo ``test_season_sql_fallback_to_default``, que exigia
        que um hemisferio desconhecido caisse em 'south' em silencio. Cair num
        default e pior que recusar: um erro de digitacao viraria uma serie de
        estacoes invertidas sem nenhum sinal. A funcao exportada recusa com
        mensagem clara, e e esse o contrato que vale fixar.
        """
        import pandas as pd
        import pytest

        from climasus4py.core.engine import get_connection

        conn = get_connection()
        conn.register("_season_fix", pd.DataFrame({"date": ["2023-01-15"]}))
        rel = conn.sql("SELECT * FROM _season_fix")

        with pytest.raises(ValueError, match="hemisphere"):
            cs_vars(rel, hemisphere="hemisferio_inexistente", verbose=False)


class TestCategoryKeysReachable:
    """Nenhuma chave de categories.json pode ficar orfa (M7).

    A traducao de valor e aplicada DEPOIS do rename de coluna, e a busca
    e pelo nome JA TRADUZIDO. Uma chave cujo nome nenhuma coluna assume
    depois do rename nunca e consultada: o codigo cru do DATASUS
    (9, 4, 3) passa para o usuario em silencio, sem erro e sem aviso.

    Foi assim que 39 chaves ficaram mudas nos tres dicionarios, entre
    elas local_obito (LOCOCOR) e escolaridade_agregada_falecido
    (ESCFALAGR1), cujos nomes corretos eram local_ocorrencia_obito e
    escolaridade_falecido_agregada. Medido no SIM-DO de SP em 2023:
    1.674.124 celulas passaram a traduzir depois da correcao.

    Este teste tambem e o guarda que faltava quando eu renomeei
    mother_education_level para mother_education: ESCMAE vira
    mother_education no SIM e mother_education_level no SINASC, entao o
    renome consertou um sistema e emudeceu o outro. As duas chaves
    precisam existir, e e isso que o teste exige.

    As excecoes sao chaves-fantasma que o PROPRIO R carrega: o dicionario
    embutido do climasus4r tem entradas com nomes que nao existem no
    DATASUS (TERCEIRO, YES_NO_FLAG, UNIDADE_IDADE, SI_NO_FLAG,
    SIM_NAO_FLAG), inalcancaveis tambem la. Ficam replicadas de
    proposito, e listadas aqui para que uma orfa NOVA apareca como falha.
    """

    FANTASMAS = {
        "pt-en": {"age_unit", "terceiro", "yes_no_flag", "yes_no_ignored"},
        "pt-es": {"age_unit", "gestor_tp", "si_no_flag", "terceiro",
                  "vincprev", "yes_no_ignored"},
        "pt-pt": {"sim_nao_flag", "sim_nao_ignorado", "terceiro",
                  "unidade_idade"},
    }

    # Chaves guardadas pelo nome DE ORIGEM, de proposito. Sao campos que o
    # columns.json nao traduz em secao nenhuma, entao a coluna chega ao
    # passo de valor com o nome cru e a chave crua e a unica que a alcanca
    # -- e o mesmo que o R faz, ja que ele aplica valor indexado pelo campo
    # de origem.
    #
    # Ficam declaradas porque o repositorio NAO PODE PROVAR que a coluna
    # existe: o datasus_columns.json publica listas de data, de numerico e
    # assinaturas de sistema, e nao um rol de colunas. Sem a declaracao,
    # qualquer chave em maiuscula passaria, inclusive um nome digitado
    # errado. Com ela, uma chave crua NOVA falha o teste ate ser
    # justificada aqui.
    CRUAS = {
        "pt-en": {"TERCEIRO"},
        # GESTOR_TP e VINCPREV so aparecem aqui, e a causa esta no R: o
        # $columns do SIH em espanhol tem 100 campos contra os 153 do
        # ingles e do portugues, e nao traduz esses dois -- mas o $values
        # em espanhol TEM os livros dos dois. Ou seja, no R a coluna fica
        # com o nome cru e o livro e aplicado pelo nome cru, e funciona.
        # A chave crua e a correta aqui pelo mesmo motivo. Registrado como
        # M117, que e a lacuna do dicionario espanhol, nao daqui.
        "pt-es": {"TERCEIRO", "GESTOR_TP", "VINCPREV"},
        "pt-pt": {"TERCEIRO"},
    }

    @staticmethod
    def _alvos(lang, secao=None):
        """Nomes que uma coluna pode ter quando chega ao passo de valor.

        Sao dois conjuntos, e o segundo e facil de esquecer:

        1. o nome **traduzido**, quando o rename mapeia o campo;
        2. o nome **de origem**, quando nao mapeia -- a coluna simplesmente
           mantem o que tinha. E o caso de TERCEIRO, que o R nao traduz em
           secao nenhuma e cujo livro de valores so e alcancavel pela
           chave crua.

        Com *secao*, usa a precedencia do loader (secao, familia, COMMON);
        sem, aceita qualquer secao, que e o que o `system=None` faz.
        """
        from climasus4py.utils.data import load_json

        cols = load_json(f"dictionaries/{lang}/columns.json")
        secoes = {k: v for k, v in cols.items()
                  if not k.startswith("_") and isinstance(v, dict)}

        if secao is None:
            aplicaveis = list(secoes.values())
        else:
            familia = secao.split("-")[0]
            aplicaveis = [secoes[n] for n in (secao, familia, "COMMON")
                          if n in secoes]

        traduzidos = {v for mapa in aplicaveis for v in mapa.values()
                      if isinstance(v, str)}
        mapeados = {k for mapa in aplicaveis for k in mapa}
        # Campo que existe no columns.json de OUTRA secao mas nao nas
        # aplicaveis: chega cru, entao a chave crua o alcanca.
        crus = {k for mapa in secoes.values() for k in mapa} - mapeados
        return traduzidos | crus

    @staticmethod
    def _secoes(lang):
        """As secoes do categories.json, ja no formato 5.0 ou no antigo."""
        from climasus4py.utils.data import load_json

        cats = load_json(f"dictionaries/{lang}/categories.json")
        secoes = {k: v for k, v in cats.items()
                  if not k.startswith("_") and isinstance(v, dict)}
        if "COMMON" in secoes:
            return secoes
        return {"COMMON": secoes}   # arquivo plano, pre-5.0

    @classmethod
    def _categorias(cls, lang):
        """Toda chave de categoria, de todas as secoes."""
        chaves: dict[str, dict] = {}
        for secao in cls._secoes(lang).values():
            chaves.update(secao)
        return chaves

    def test_toda_chave_de_categoria_e_nome_de_coluna(self):
        """Agora POR SECAO, que e mais exigente que o teste antigo.

        Antes o arquivo era plano e bastava a chave ser alcancavel em
        algum sistema. Com as secoes do M104, uma chave na secao errada --
        livro do SINAN posto em SIH, digamos -- passaria no teste antigo e
        nunca seria consultada. Aqui cada secao e conferida com a
        precedencia que o loader de fato usa.
        """
        import pytest

        for lang, fantasmas in self.FANTASMAS.items():
            try:
                secoes = self._secoes(lang)
            except FileNotFoundError:
                pytest.skip(f"climasus-data sem dictionaries/{lang}")
            cruas = self.CRUAS.get(lang, set())
            for nome, chaves in secoes.items():
                alvos = self._alvos(lang, None if nome == "COMMON" else nome)
                orfas = sorted(set(chaves) - alvos - fantasmas - cruas)
                assert not orfas, (
                    f"{lang}/{nome}: chave de categoria que nenhuma coluna "
                    f"alcanca nessa secao, logo o codigo cru do DATASUS "
                    f"passa em silencio: {orfas}"
                )

    def test_as_chaves_cruas_declaradas_ainda_existem(self):
        """Contrapartida da declaracao: a lista nao pode envelhecer.

        Sem isto, CRUAS viraria uma lista de excecoes que ninguem revisa,
        e uma chave que saiu do dicionario continuaria perdoada para
        sempre.
        """
        import pytest

        for lang, cruas in self.CRUAS.items():
            try:
                chaves = set(self._categorias(lang))
            except FileNotFoundError:
                pytest.skip(f"climasus-data sem dictionaries/{lang}")
            sumidas = sorted(cruas - chaves)
            assert not sumidas, (
                f"{lang}: declarada como chave crua mas ja nao esta no "
                f"dicionario -- tire da lista: {sumidas}"
            )

    def test_fantasmas_declaradas_ainda_existem(self):
        """A lista de excecoes nao pode envelhecer sem ninguem notar."""
        import pytest

        for lang, fantasmas in self.FANTASMAS.items():
            try:
                cats = self._categorias(lang)
            except FileNotFoundError:
                pytest.skip(f"climasus-data sem dictionaries/{lang}")
            sumidas = sorted(fantasmas - set(cats))
            assert not sumidas, (
                f"{lang}: declarada como fantasma mas ja nao esta no "
                f"dicionario -- tire da lista: {sumidas}"
            )

    def test_escmae_traduz_nos_dois_sistemas(self):
        """ESCMAE vira nome diferente no SIM e no SINASC; os dois precisam.

        Fixa o caso concreto que meu renome anterior quebrou.
        """
        import pytest

        from climasus4py.utils.data import load_json

        try:
            cols = load_json("dictionaries/pt-en/columns.json")
            cats = self._categorias("pt-en")
        except FileNotFoundError:
            pytest.skip("climasus-data sem dictionaries/pt-en")

        destinos = {secao: mapa["ESCMAE"]
                    for secao, mapa in cols.items()
                    if isinstance(mapa, dict) and "ESCMAE" in mapa}
        assert destinos, "ESCMAE saiu do columns.json"
        for secao, destino in destinos.items():
            assert destino in cats, (
                f"ESCMAE vira {destino!r} no {secao} e nao ha dicionario de "
                f"categoria com esse nome: a escolaridade da mae sairia como "
                f"codigo cru nesse sistema"
            )
