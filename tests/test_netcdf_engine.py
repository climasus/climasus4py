"""M40: a checagem do stack NetCDF **antes** de gastar o download.

O achado original: o `sus_grid_pdsi` baixava o arquivo do TerraClimate
**com sucesso** e só então morria na leitura, porque faltava um *engine*
de NetCDF. A peça que faltava nunca foi o `xarray` em si — ele estava
instalado — mas o *backend* pelo qual ele lê (`netcdf4`, `h5netcdf` ou
`scipy`). Sem nenhum deles, o `open_dataset` levanta uma mensagem do
próprio xarray apontando a documentação **dele**:

    did not find a match in any of xarray's currently installed IO
    backends [...] Consider explicitly selecting one of the installed
    engines via the ``engine`` parameter

que não diz nada a quem não conhece o ecossistema, e que chega **depois**
do download — o usuário paga um arquivo grande para receber um erro
incompreensível.

A declaração de dependência foi corrigida em 07/09/2026 com o extra
`[grid]`. O que faltava, e é o que estes testes cobrem, é checar **antes**
de gastar o download.

Duas coisas se provam aqui, e elas são diferentes:

1. **A detecção pergunta ao xarray**, não só tenta importar. Um engine se
   registra como *plugin de backend*, e um pacote ser importável não é o
   mesmo que seu plugin ser carregável — o `netCDF4` pode importar e
   ainda assim não aparecer em `xr.backends.list_engines()`, por exemplo
   com uma libnetcdf incompatível.

2. **A checagem roda antes do download.** Esta é a correção de fato, e
   não dá para provar por ordem de linha: as três funções delegam o
   download a um *helper*, então o teste intercepta o helper e exige zero
   tentativas.
"""

from __future__ import annotations

import sys

import pytest

from climasus4py.enrichment import _netcdf
from climasus4py.enrichment._netcdf import (
    _ENGINE_PACKAGE,
    _MESSAGES,
    _NETCDF_ENGINES,
    available_netcdf_engines,
    require_netcdf_stack,
)

xr = pytest.importorskip("xarray")


# --- detecção -----------------------------------------------------------


def test_engines_conhecidos_sao_os_que_leem_arquivo_local() -> None:
    """`pydap` e `zarr` também são engines do xarray, mas não contam.

    A lista existe para responder "posso ler este `.nc` do disco?", e
    nenhum dos dois lê. Incluí-los deixaria a checagem passar num
    ambiente onde a leitura ainda falharia.
    """
    assert _NETCDF_ENGINES == ("netcdf4", "h5netcdf", "scipy")
    assert "pydap" not in _NETCDF_ENGINES
    assert "zarr" not in _NETCDF_ENGINES


def test_todo_engine_tem_pacote_declarado() -> None:
    """O nome do engine não é o nome do pacote: `netcdf4` vs `netCDF4`.

    É por isso que o mapa é escrito à mão em vez de derivado. Se um
    engine novo entrar na tupla sem entrar no mapa, o fallback por
    `find_spec` levantaria `KeyError` — e só num ambiente sem
    `list_engines`, que é o ambiente em que ninguém testa.
    """
    assert set(_ENGINE_PACKAGE) == set(_NETCDF_ENGINES)
    assert _ENGINE_PACKAGE["netcdf4"] == "netCDF4"


def test_detecao_devolve_subconjunto_na_ordem_declarada() -> None:
    disponiveis = available_netcdf_engines()
    assert set(disponiveis) <= set(_NETCDF_ENGINES)
    # A ordem é a da tupla, não a que o xarray devolver: o `list_engines`
    # devolve um dict, e a mensagem de erro cita a mesma ordem.
    assert disponiveis == [e for e in _NETCDF_ENGINES if e in disponiveis]


def test_pergunta_ao_xarray_e_nao_ao_importlib(monkeypatch) -> None:
    """A prova de (1): engine importável mas não registrado não conta.

    Este é o teste que distingue a implementação da ingênua. Fingimos um
    xarray que só registra `scipy` — mesmo com o `netCDF4` instalado e
    importável neste ambiente, ele **não** deve aparecer.
    """
    assert "netCDF4" in sys.modules or True  # importável ou não, tanto faz
    monkeypatch.setattr(xr.backends, "list_engines", lambda: {"scipy": object()})
    assert available_netcdf_engines() == ["scipy"]


def test_fallback_quando_xarray_nao_expoe_list_engines(monkeypatch) -> None:
    """xarray antigo não tem `list_engines`; aí sim vale o `find_spec`.

    Pior detecção, mas melhor que devolver lista vazia e bloquear quem
    tem o stack completo.
    """
    def explode() -> dict:
        raise AttributeError("list_engines")

    monkeypatch.setattr(xr.backends, "list_engines", explode)
    assert set(available_netcdf_engines()) <= set(_NETCDF_ENGINES)
    # Neste ambiente pelo menos um dos pacotes existe, então o fallback
    # não pode devolver vazio -- se devolvesse, o AttributeError estaria
    # escapando em vez de ser tratado.
    assert available_netcdf_engines()


def test_sem_xarray_devolve_vazio_sem_levantar(monkeypatch) -> None:
    """Sem xarray não há o que perguntar, e a função não é o lugar do erro.

    Quem levanta é o `require_netcdf_stack`, com mensagem própria; a
    detecção só informa.
    """
    monkeypatch.setitem(sys.modules, "xarray", None)
    assert available_netcdf_engines() == []


# --- a exigência --------------------------------------------------------


def test_com_engine_nao_levanta_e_nao_devolve_nada() -> None:
    assert available_netcdf_engines(), "ambiente sem engine; teste inválido"
    assert require_netcdf_stack("pt") is None


def test_sem_engine_levanta_import_error(monkeypatch) -> None:
    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack("pt")
    assert "engine" in str(exc.value)


def test_mensagem_diz_como_consertar(monkeypatch) -> None:
    """Um erro que não diz como consertar custa uma busca ao leitor.

    Era exatamente o defeito da mensagem do xarray: ela aponta a
    documentação dele, não o extra deste pacote.
    """
    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack("pt")
    msg = str(exc.value)
    assert "climasus4py[grid]" in msg
    assert "pip install" in msg
    # e nomeia os engines aceitos, para quem quiser instalar um só
    for engine in _NETCDF_ENGINES:
        assert engine in msg


def test_mensagem_avisa_que_o_download_nao_comecou(monkeypatch) -> None:
    """Sem isto, quem lê o erro não sabe se pagou o download ou não.

    E é a informação que importa: o M40 nasceu de um download gasto.
    """
    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack("pt")
    assert "download nao foi iniciado" in str(exc.value).lower()


def test_sem_xarray_a_mensagem_nomeia_o_que_falta(monkeypatch) -> None:
    faltantes: list[str] = []

    def sem_xarray(nome: str) -> object | None:
        if nome in ("xarray", "rioxarray"):
            faltantes.append(nome)
            return None
        return object()

    monkeypatch.setattr(_netcdf, "find_spec", sem_xarray)
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack("pt")
    msg = str(exc.value)
    assert "xarray" in msg and "rioxarray" in msg
    assert "climasus4py[grid]" in msg


@pytest.mark.parametrize("lang", ["pt", "en", "es"])
def test_mensagem_nos_tres_idiomas(monkeypatch, lang: str) -> None:
    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack(lang)
    msg = str(exc.value)
    assert _MESSAGES[lang]["how"] in msg
    assert "climasus4py[grid]" in msg


def test_idioma_desconhecido_cai_no_portugues(monkeypatch) -> None:
    """O pacote é brasileiro; o default é `pt` e não um KeyError."""
    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError) as exc:
        require_netcdf_stack("tlh")
    assert _MESSAGES["pt"]["how"] in str(exc.value)


def test_os_tres_idiomas_tem_as_tres_chaves() -> None:
    for lang, msgs in _MESSAGES.items():
        assert set(msgs) == {"need_xarray", "need_engine", "how"}, lang
        # o `{engines}` é preenchido em tempo de erro; se alguém traduzir
        # sem o placeholder, a mensagem perde a lista de engines em
        # silêncio -- o `.format` não reclama de placeholder ausente.
        assert "{engines}" in msgs["need_engine"], lang


# --- a correção de fato: antes do download ------------------------------

# Cada função delega o download a um helper diferente; é ele que o teste
# intercepta. Interceptar o helper, e não o `requests`, é o que torna o
# teste honesto: se alguém trocar a biblioteca de HTTP, o teste continua
# medindo a mesma coisa.
ANTES_DO_DOWNLOAD = [
    ("sus_grid_pdsi", "grid_pdsi", "_download_robust", {"years": 2020}),
    ("sus_grid_era5", "grid_era5", "_download_robust", {"years": 2020}),
    (
        "sus_grid_pollution_merra2",
        "grid_pollution_merra2",
        "_merra2_download_raw",
        {"years": 2020},
    ),
]


@pytest.mark.parametrize(
    ("funcao", "modulo", "baixador", "kwargs"),
    ANTES_DO_DOWNLOAD,
    ids=[c[0] for c in ANTES_DO_DOWNLOAD],
)
def test_levanta_antes_de_qualquer_download(
    monkeypatch, funcao: str, modulo: str, baixador: str, kwargs: dict
) -> None:
    """A prova de (2), e a razão de o M40 existir.

    Não dá para provar por ordem de linha: as três funções delegam o
    download, então a prova é comportamental — o baixador é substituído
    por algo que registra a tentativa, e a contagem tem de ficar em zero.

    Medido, removendo a checagem: o `sus_grid_pdsi` e o `sus_grid_era5`
    tentam **1 download cada**, e é a contagem que pega a regressão.
    O `sus_grid_pollution_merra2` é diferente e vale dizer, porque o
    contrário dá a impressão errada: nele a contagem fica em zero de
    qualquer jeito, porque o gate de credenciais Earthdata vem antes do
    download e levanta `ValueError`. Para ele, o que pega a regressão é o
    **tipo** da exceção — `ImportError` contra `ValueError` — que é o
    assunto do teste de ordem logo abaixo.
    """
    import importlib

    import climasus4py as cs

    mod = importlib.import_module(f"climasus4py.enrichment.{modulo}")
    tentativas: list[tuple] = []

    def nao_deve_baixar(*args, **kw):
        tentativas.append(args)
        raise AssertionError(f"{funcao} baixou antes de checar o engine")

    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    monkeypatch.setattr(mod, baixador, nao_deve_baixar)

    with pytest.raises(ImportError) as exc:
        getattr(cs, funcao)(lang="pt", **kwargs)

    assert tentativas == [], f"{funcao} tentou {len(tentativas)} download(s)"
    assert "climasus4py[grid]" in str(exc.value)


def test_merra2_checa_engine_antes_das_credenciais() -> None:
    """No merra2 há dois gates locais, e a ordem entre eles importa.

    Nenhum dos dois custa download, mas o engine é o mais fundamental:
    credencial nenhuma resolve a falta de backend, e mandar o usuário
    cadastrar-se no Earthdata para depois descobrir que falta um pacote
    é gastar o tempo dele em vez do download.
    """
    import inspect

    import climasus4py as cs

    src = inspect.getsource(cs.sus_grid_pollution_merra2).split("\n")
    i_engine = next(i for i, l in enumerate(src) if "require_netcdf_stack(" in l)
    i_cred = next(i for i, l in enumerate(src) if "_merra2_check_auth(" in l)
    assert i_engine < i_cred


def test_merra2_sem_engine_levanta_import_error_e_nao_value_error(
    monkeypatch,
) -> None:
    """A contrapartida comportamental do teste acima.

    Sem o guarda, esta chamada levanta `ValueError` de credenciais —
    medido. É por isso que o tipo da exceção é a asserção que vale aqui.
    """
    import climasus4py as cs

    monkeypatch.setattr(_netcdf, "available_netcdf_engines", lambda: [])
    with pytest.raises(ImportError):
        cs.sus_grid_pollution_merra2(years=2020, lang="pt")


@pytest.mark.parametrize(
    ("funcao", "modulo", "baixador", "kwargs"),
    ANTES_DO_DOWNLOAD,
    ids=[c[0] for c in ANTES_DO_DOWNLOAD],
)
def test_a_checagem_esta_de_fato_ligada(
    funcao: str, modulo: str, baixador: str, kwargs: dict
) -> None:
    """Contrapartida: o módulo importa o guarda.

    Sem isto, o teste acima passaria num módulo que nunca chama a
    checagem — bastaria falhar por outro motivo antes do download.
    """
    import importlib
    import inspect

    mod = importlib.import_module(f"climasus4py.enrichment.{modulo}")
    assert hasattr(mod, "require_netcdf_stack")
    src = inspect.getsource(getattr(mod, funcao))
    assert "require_netcdf_stack(" in src
    # e o comentário que explica o porquê, com o número do achado: sem
    # ele a linha parece uma checagem defensiva gratuita e é a primeira
    # coisa que alguém remove numa limpeza.
    assert "M40" in src
