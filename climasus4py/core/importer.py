"""Data import — download from DATASUS and cache as parquet.

Mirrors R: import.R + download-aria2c.R
Preferred reader: climasus_readdbc_py (pure Python, no C compiler).
Fallback chain: climasus_readdbc_py → climasus_readdbc → pyreaddbc → pysus → dbc2dbf CLI.
"""

from __future__ import annotations  # adia avaliação de anotações de tipo (PEP 563)

import hashlib  # hashing (MD5 dos arquivos baixados)
import json  # leitura/escrita de JSON (manifesto)
import shutil  # utilitários de arquivo: which(), cópia, remoção
import subprocess  # executa processos externos (CLI dbc2dbf)
import tempfile  # cria arquivos/diretórios temporários
import urllib.request  # cliente HTTP/FTP da stdlib (download)
import uuid  # gera identificadores únicos (nome do .tmp)
from datetime import datetime, timezone  # datas e fuso horário (timestamp do manifesto)
from pathlib import Path  # manipulação portável de caminhos
from typing import Literal, cast  # Literal: enum de strings; cast: dica de tipo
from urllib.parse import unquote, urlparse  # decodifica %xx e separa as partes da URL

import duckdb  # engine SQL embutido (relação preguiçosa de saída)
import pandas as pd  # DataFrames (manipulação dos dados lidos)
import pyarrow as pa  # Arrow: tabela colunar em memória
import pyarrow.parquet as pq  # leitura/escrita de arquivos Parquet
from rich.console import Console  # saída formatada no terminal (cores/símbolos)

from ..utils.data import (  # utilitários de dados do pacote
    load_datasus_columns_spec,  # spec das colunas DATASUS (datas/numéricas)
    load_json,  # carrega um JSON do pacote climasus-data
    load_uf_codes,  # tabela de códigos IBGE das UFs
    resolve_uf,  # normaliza uf: 'all'/região/sigla → lista de siglas
)
from ._stage import set_stage  # marca a relação com metadados de estágio
from .engine import read_parquets  # lê vários parquets como uma relação DuckDB

console = Console(stderr=True)  # console que imprime no stderr (não polui o stdout)

_DEFAULT_CACHE = Path("dados/cache")  # diretório padrão do cache de parquet
# caminho do catálogo de sistemas (no climasus-data)
_DATASUS_SOURCES_PATH = "metadata/datasus_systems.json"


# ---------------------------------------------------------------------------
# Atomic Parquet write  (tmp + rename — prevents partial files on crash)
# ---------------------------------------------------------------------------

# escreve a tabela em parquet de forma atômica (tmp + rename)
def _write_parquet_atomic(table: pa.Table, target: Path) -> None:
    """Write *table* to *target* atomically via a temporary file.

    Writes to a sibling ``.tmp_<hex>`` file first, then renames it to
    *target*.  If an exception occurs, the temporary file is cleaned up.
    This prevents corrupted Parquet files when a worker crashes or two
    workers race on the same path.

    Args:
        table: PyArrow table to persist.
        target: Destination ``.parquet`` path (must already have its
            parent directory created).

    Raises:
        Re-raises any exception from :func:`pyarrow.parquet.write_table`
        or :func:`pathlib.Path.replace` after cleaning up the tmp file.
    """
    # nome de arquivo temporário único (evita corrida entre workers)
    tmp = target.with_suffix(f".tmp_{uuid.uuid4().hex[:8]}.parquet")
    try:                            # tenta escrever; em erro, limpa o tmp
        pq.write_table(table, tmp)  # grava a tabela no arquivo temporário
        tmp.replace(target)         # renomeia tmp → alvo (rename é atômico no SO)
    except Exception:               # qualquer falha durante a escrita...
        if tmp.exists():            # se o tmp chegou a ser criado...
            tmp.unlink()            # remove o arquivo parcial/corrompido
        raise                       # repropaga a exceção original ao chamador


# ---------------------------------------------------------------------------
# Type coercion for DATASUS data  (DBC/DBF → Parquet)
# ---------------------------------------------------------------------------

def _datasus_date_cols() -> frozenset[str]:  # colunas de data (conjunto imutável)
    # lê a spec e extrai a lista de colunas de data
    return frozenset(load_datasus_columns_spec()["all_date_columns"])


def _datasus_numeric_cols() -> frozenset[str]:  # colunas numéricas (conjunto imutável)
    # lê a spec e extrai a lista de colunas numéricas
    return frozenset(load_datasus_columns_spec()["all_numeric_columns"])


# converte as colunas do DATASUS para os tipos corretos
def _coerce_datasus_types(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce DATASUS columns to proper types before writing to Parquet.

    - Date columns (DDMMYYYY strings) → datetime64
    - Known numeric columns → numeric (coerced, invalid → NaN)
    - Strips whitespace from string columns
    """
    date_cols = _datasus_date_cols()        # carrega os nomes das colunas de data
    numeric_cols = _datasus_numeric_cols()  # carrega os nomes das colunas numéricas
    for col in df.columns:                  # percorre cada coluna do DataFrame
        if col in date_cols:                # se a coluna é de data...
            # DATASUS date format: DDMMYYYY (8 digits)
            # DDMMYYYY → datetime (inválido vira NaT)
            df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")
        elif col in numeric_cols:           # se a coluna é numérica...
            # texto → número (inválido vira NaN)
            df[col] = pd.to_numeric(df[col], errors="coerce")
        # se a coluna é textual (object/string)...
        elif df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            # Strip whitespace from string columns
            # tira espaços; ''/'nan' viram None
            df[col] = df[col].astype(str).str.strip().replace({"":None,"nan":None})
    return df                               # devolve o DataFrame com os tipos corrigidos

# ---------------------------------------------------------------------------
# DATASUS source catalog resolution
# ---------------------------------------------------------------------------

def _datasus_sources() -> dict:              # carrega o catálogo de sistemas DATASUS (JSON)
    return load_json(_DATASUS_SOURCES_PATH)  # lê e devolve o JSON do catálogo


def _system_source(system: str) -> dict:     # devolve os metadados de um sistema específico
    systems = _datasus_sources()["systems"]  # pega o mapa de sistemas do catálogo
    try:                                     # tenta achar o sistema pedido...
        return systems[system]               # devolve os metadados do sistema
    except KeyError as exc:                  # se o sistema não existe no catálogo...
        raise ValueError(                    # erro listando os sistemas suportados
            f"System '{system}' not supported for direct FTP download. "
            "Supported: " + ", ".join(sorted(systems))
        ) from exc


# diz se um template de URL vale para o ano dado
def _template_applies(template: dict, year: int) -> bool:
    valid_from = template.get("valid_from_year")    # ano inicial de validade (pode faltar)
    valid_until = template.get("valid_until_year")  # ano final de validade (pode faltar)
    # se há início e o ano é anterior a ele...
    if valid_from is not None and year < int(valid_from):
        return False                                # ...o template não se aplica
    # vale, a menos que o ano ultrapasse o fim
    return not (valid_until is not None and year > int(valid_until))


# monta o dicionário de substituição dos templates
def _template_context(system_meta: dict, uf: str, year: int, month: int | None) -> dict[str, str]:
    return {                        # valores usados no .format() dos templates
        "uf": uf.upper(),           # UF em maiúsculas
        "yyyy": str(year),          # ano com 4 dígitos
        "yy": f"{year % 100:02d}",  # ano com 2 dígitos (resto da divisão por 100)
        # mês com 2 dígitos, ou vazio se None
        "month": f"{month:02d}" if month is not None else "",
        # código da doença (SINAN), se houver
        "disease_code": str(system_meta.get("disease_code", "")),
    }


# monta as URLs de FTP a partir do catálogo
def _build_urls(system: str, uf: str, year: int, month: int | None = None) -> list[str]:
    """Build FTP URLs from the climasus-data DATASUS source catalog."""
    catalog = _datasus_sources()                        # carrega o catálogo completo
    system_meta = _system_source(system)                # metadados do sistema pedido
    source = catalog["sources"][system_meta["source"]]  # metadados da fonte FTP usada pelo sistema
    base_url = source["base_url"].rstrip("/")           # URL base, sem a barra final
    # valores para substituir nos templates
    context = _template_context(system_meta, uf, year, month)

    urls = []                                      # lista de URLs a montar
    for template in system_meta["url_templates"]:  # percorre cada template de caminho
        if _template_applies(template, year):      # se o template vale para este ano...
            # preenche o template e tira a barra inicial
            path = template["path_template"].format(**context).lstrip("/")
            urls.append(f"{base_url}/{path}")      # junta base + caminho e guarda a URL

    if not urls:  # se nenhum template gerou URL...
        # erro: nenhum template aplicável ao ano
        raise ValueError(f"No DATASUS FTP URL template applies to {system} for {year}.")
    return urls   # devolve a lista de URLs


def _geographic_scope(system: str) -> str:  # escopo geográfico ('state' ou 'national')
    # lê o escopo do sistema (padrão: 'state')
    return str(_system_source(system).get("geographic_scope", "state"))


# id da partição de cache (UF, ou 'BR' se nacional)
def _cache_partition_id(system: str, uf: str) -> str:
    if _geographic_scope(system) == "national":  # se o sistema é nacional...
        return "BR"                              # ...usa 'BR' como partição única
    return uf                                    # senão, usa a própria UF


# monta filtro SQL por UF (ou None se não precisa)
def _state_filter_expression(system: str, ufs: list[str]) -> str | None:
    system_meta = _system_source(system)  # metadados do sistema
    # config de filtro por partição (pode não existir)
    filter_meta = system_meta.get("partition_filter")
    if not filter_meta:                   # se o sistema não filtra por estado...
        return None                       # ...não há filtro a aplicar

    uf_codes = load_uf_codes()              # tabela de códigos IBGE das UFs
    requested = {uf.upper() for uf in ufs}  # conjunto das UFs pedidas, em maiúsculas
    if requested == set(uf_codes):          # se pediram todas as UFs existentes...
        return None                         # ...não precisa filtrar nada

    # códigos IBGE das UFs pedidas, ordenados
    codes = [int(uf_codes[uf]["code"]) for uf in sorted(requested)]
    col = filter_meta["state_column"]                     # nome da coluna de UF no dado
    values = ", ".join(str(code) for code in codes)       # lista de códigos formatada para o SQL
    return f'TRY_CAST("{col}" AS INTEGER) IN ({values})'  # expressão SQL: coluna IN (códigos)


# ---------------------------------------------------------------------------
# .dbc file reader — chain of backends
# ---------------------------------------------------------------------------

def _read_dbc(path: Path) -> pd.DataFrame:  # lê um .dbc tentando vários backends em ordem
    """Read a .dbc file trying multiple backends.

    Order: climasus_readdbc_py (pure Python) → climasus_readdbc (legacy)
    → pyreaddbc (C) → pysus → dbc2dbf CLI.
    """
    # Backend 1: climasus_readdbc_py (pure Python, no C compiler needed)
    try:                               # tenta o backend em Python puro (sem compilador C)
        import climasus_readdbc_py as readdbc  # importa o leitor em Python puro
        return readdbc.read_dbc(path)  # lê e devolve o DataFrame
    except ImportError:                # se o pacote não está instalado...
        pass                           # ...ignora e tenta o próximo backend
    except Exception:                  # se a leitura falhar...
        pass                           # ...ignora e tenta o próximo backend

    # Backend 1b: legacy import path kept for already-published versions.
    try:                               # tenta o caminho de import legado
        import climasus_readdbc as readdbc  # import antigo (versões já publicadas)
        return readdbc.read_dbc(path)  # lê e devolve o DataFrame
    except ImportError:                # pacote ausente...
        pass                           # ...tenta o próximo backend
    except Exception:                  # falha na leitura...
        pass                           # ...tenta o próximo backend

    # Backend 2: pyreaddbc (C extension, fastest)
    try:                            # tenta o pyreaddbc (extensão C, mais rápida)
        # importa o read_dbc do pyreaddbc
        from pyreaddbc import read_dbc  # type: ignore[import-untyped]
        return read_dbc(str(path))  # lê e devolve o DataFrame
    except ImportError:             # se não instalado...
        pass                        # ...tenta o próximo backend

    # Backend 3: pysus utilities
    try:                              # tenta o utilitário do pysus
        # importa o leitor de .dbc do pysus
        from pysus.utilities.readdbc import read_dbc as pysus_read  # type: ignore[import-untyped]
        return pysus_read(str(path))  # lê e devolve o DataFrame
    except ImportError:               # se não instalado...
        pass                          # ...tenta o próximo backend

    # Backend 4: dbc2dbf CLI + dbfread
    dbc2dbf = shutil.which("dbc2dbf")             # procura o executável dbc2dbf no PATH
    if dbc2dbf:                                   # se o CLI existe...
        try:                                      # tenta converter via CLI + dbfread
            # leitor de arquivos .dbf
            import dbfread  # type: ignore[import-untyped]
            # cria um .dbf temporário (mantido após fechar)
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=False) as tmp:
                dbf_path = tmp.name               # guarda o caminho do .dbf temporário
            subprocess.run(                       # executa dbc2dbf <entrada> <saída>
                [dbc2dbf, str(path), dbf_path],   # comando e seus argumentos
                check=True, capture_output=True,  # falha se der erro; silencia a saída
            )
            # lê o .dbf (encoding latin1 do DATASUS)
            table = dbfread.DBF(dbf_path, encoding="latin1")
            return pd.DataFrame(iter(table))      # converte os registros em DataFrame
        except Exception:                         # se a conversão falhar...
            pass                                  # ...cai no erro final abaixo

    raise ImportError(  # nenhum backend disponível: erro com instruções
        "Cannot read .dbc files. Install climasus_readdbc_py:\n"
        "  pip install climasus_readdbc_py\n"
        "Or alternatively:\n"
        "  pip install pyreaddbc  # (needs C compiler)\n"
        "  pip install pysus     # (needs C compiler)\n"
        "Or use sus_data_import(path='file.parquet') / sus_data_import(data=df) instead."
    )


# ---------------------------------------------------------------------------
# FTP download
# ---------------------------------------------------------------------------

# baixa um arquivo via FTP; True se deu certo
def _download_ftp(url: str, dest: Path, timeout: int = 120) -> bool:
    """Download a single file from FTP. Returns True on success."""
    try:                                       # tenta baixar; em erro, limpa e devolve False
        # garante que o diretório de destino existe
        dest.parent.mkdir(parents=True, exist_ok=True)
        # abre a conexão FTP/HTTP com timeout
        with urllib.request.urlopen(url, timeout=timeout) as response:
            dest.write_bytes(response.read())  # salva o conteúdo baixado no destino
        return True                            # sucesso
    except Exception:                          # qualquer falha de rede/IO...
        if dest.exists():                      # se um arquivo parcial foi criado...
            dest.unlink()                      # ...remove o arquivo incompleto
        return False                           # sinaliza falha ao chamador


# caminho local seguro para guardar o .dbc cru
def _raw_cache_path(url: str, raw_cache_dir: Path) -> Path:
    parsed = urlparse(url)               # separa a URL em partes
    # Decode percent-encoded sequences (%2e%2e → ..) before checking traversal
    decoded_path = unquote(parsed.path)  # decodifica %xx (ex.: %2e%2e → ..)
    # host + segmentos não vazios do caminho
    parts = [parsed.netloc, *[part for part in decoded_path.split("/") if part]]
    # monta o caminho final e resolve '..'
    candidate = raw_cache_dir.joinpath(*parts).resolve()
    # Security: ensure the resolved path stays inside the cache directory
    try:                                 # verifica se o caminho ficou dentro do cache
        # lança ValueError se escapou do diretório
        candidate.relative_to(raw_cache_dir.resolve())
    except ValueError as err:            # se escapou (path traversal)...
        raise ValueError(                # ...erro de segurança
            f"Unsafe URL: path traversal detected outside the cache directory "
            f"({url!r} resolves to {candidate})."
        ) from err
    return candidate                     # caminho seguro já validado


def _file_md5(path: Path) -> str:  # calcula o MD5 do arquivo (para o manifesto)
    digest = hashlib.md5()         # acumulador do hash
    with open(path, "rb") as f:    # abre o arquivo em modo binário
        # lê em blocos de 1 MB (não carrega tudo)
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)   # atualiza o hash com o bloco lido
    return digest.hexdigest()      # devolve o MD5 em hexadecimal


# registra o download no manifesto (JSONL)
def _append_raw_manifest(raw_cache_dir: Path, url: str, path: Path) -> None:
    manifest = raw_cache_dir / "_manifest.jsonl"        # manifesto: uma linha JSON por download
    try:                                                # tenta usar caminho relativo ao cache
        # caminho relativo ao diretório de cache
        relative_path = str(path.relative_to(raw_cache_dir))
    except ValueError:                                  # se não for subcaminho...
        relative_path = str(path)                       # ...usa o caminho absoluto
    record = {                                          # registro a gravar no manifesto
        # data/hora do download (UTC)
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "url": url,                                     # URL de origem
        "path": relative_path,                          # caminho onde foi salvo
        "size_bytes": path.stat().st_size,              # tamanho do arquivo em bytes
        "md5": _file_md5(path),                         # MD5 para verificação de integridade
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)  # garante o diretório do manifesto
    with open(manifest, "a", encoding="utf-8") as f:    # abre em modo append (acrescenta no fim)
        # escreve o JSON + quebra de linha
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


# obtém o .dbc: do cache cru ou baixando
def _resolve_dbc_path(
    urls: list[str],
    tmp_dir: Path,
    timeout: int,
    store_raw: bool,
    raw_cache_dir: Path | None,
) -> Path | None:
    for url in urls:                         # tenta cada URL candidata em ordem
        if store_raw:                        # se vamos guardar o arquivo cru...
            if raw_cache_dir is None:        # diretório de cache cru é obrigatório aqui
                # erro de configuração
                raise ValueError("raw_cache_dir must be provided when store_raw=True")
            # caminho local seguro para esta URL
            dbc_path = _raw_cache_path(url, raw_cache_dir)
            # se já está em cache e não está vazio...
            if dbc_path.is_file() and dbc_path.stat().st_size > 0:
                return dbc_path              # ...reaproveita o arquivo cacheado
        else:                                # se não guardamos o cru...
            dbc_path = tmp_dir / "data.dbc"  # ...baixa para um arquivo temporário

        if _download_ftp(url, dbc_path, timeout=timeout):  # baixa; se deu certo...
            if store_raw and raw_cache_dir is not None:    # se guardamos o cru...
                # ...registra no manifesto
                _append_raw_manifest(raw_cache_dir, url, dbc_path)
            return dbc_path                                # devolve o caminho do .dbc baixado

    return None  # todas as URLs falharam


# baixa 1 .dbc, converte para parquet e cacheia
def _download_and_cache(
    system: str,
    uf: str,
    year: int,
    month: int | None,
    target: Path,
    verbose: bool,
    timeout: int,
    store_raw: bool,
    raw_cache_dir: Path | None,
) -> Path | None:
    """Download a single .dbc from DATASUS FTP, convert to parquet, cache."""
    urls = _build_urls(system, uf, year, month)  # monta as URLs candidatas

    with tempfile.TemporaryDirectory() as tmpdir:  # diretório temporário para o download
        dbc_path = _resolve_dbc_path(              # obtém o .dbc (cache ou download)
            urls=urls,
            tmp_dir=Path(tmpdir),
            timeout=timeout,
            store_raw=store_raw,
            raw_cache_dir=raw_cache_dir,
        )

        if dbc_path is None:  # se nenhuma URL funcionou...
            if verbose:       # ...e em modo verboso...
                # avisa a falha
                console.print(f"[red]✗[/]  {uf}_{year}: all FTP URLs failed")
            return None       # desiste deste arquivo

        try:                          # tenta ler o .dbc
            df = _read_dbc(dbc_path)  # lê o .dbc num DataFrame
        except ImportError as e:      # se falta backend de leitura...
            raise e                   # ...propaga (erro de instalação, não de dado)
        except Exception as e:        # outra falha ao ler o .dbc...
            if verbose:               # ...em modo verboso...
                # avisa o erro de leitura
                console.print(f"[red]✗[/]  {uf}_{year}: failed to read .dbc: {e}")
            return None               # desiste deste arquivo

    # Coerce types before writing to Parquet
    df = _coerce_datasus_types(df)  # corrige os tipos antes de salvar

    target.parent.mkdir(parents=True, exist_ok=True)         # garante o diretório de destino
    _write_parquet_atomic(pa.Table.from_pandas(df), target)  # salva o parquet de forma atômica

    if verbose:  # em modo verboso...
        # avisa o sucesso e o nº de linhas
        console.print(f"[green]✔[/]  {uf}_{year} ({len(df):,} rows)")

    return target  # devolve o caminho do parquet gerado


# ---------------------------------------------------------------------------
# PySUS download  (optional high-level backend)
# ---------------------------------------------------------------------------

_PYSUS_SYSTEM_MAP: dict[str, tuple[str, str]] = {        # mapa sistema → (módulo PySUS, função)
    "SIM-DO": ("pysus.online_data.SIM", "download"),     # mortalidade (SIM)
    "SINASC": ("pysus.online_data.SINASC", "download"),  # nascidos vivos (SINASC)
    "SIH-RD": ("pysus.online_data.SIH", "download"),     # internações (SIH-RD)
    # agravos/dengue (SINAN)
    "SINAN-DENGUE": ("pysus.online_data.SINAN", "download"),
}


# baixa 1 UF/ano via PySUS (backend opcional)
def _download_pysus(
    system: str, uf: str, year: int, month: int | None = None
) -> pd.DataFrame:
    """Download a single UF/year from DATASUS via PySUS (optional)."""
    if system not in _PYSUS_SYSTEM_MAP:  # se o sistema não tem mapeamento PySUS...
        # ...erro
        raise ValueError(f"System '{system}' not supported via PySUS")

    module_path, func_name = _PYSUS_SYSTEM_MAP[system]  # módulo e função de download do sistema
    import importlib  # import dinâmico (PySUS é opcional)

    mod = importlib.import_module(module_path)  # importa o módulo do PySUS em runtime
    download_fn = getattr(mod, func_name)       # pega a função de download pelo nome

    kwargs: dict = {"state": uf, "year": year}          # argumentos básicos: UF e ano
    if month is not None and system.startswith("SIH"):  # SIH também aceita mês...
        kwargs["month"] = month                         # ...então inclui o mês

    return download_fn(**kwargs)  # chama o download e devolve o DataFrame


def _pysus_available() -> bool:  # diz se o PySUS está instalado
    """Check if PySUS is installed."""
    try:                         # tenta importar o pysus
        # import só para testar a disponibilidade
        import pysus  # type: ignore[import-untyped]  # noqa: F401
        return True              # instalado
    except ImportError:          # não instalado...
        return False             # ...indisponível


# ---------------------------------------------------------------------------
# aria2c parallel download  (optional accelerator)
# ---------------------------------------------------------------------------

def _aria2c_available() -> bool:               # diz se o binário aria2c está no PATH
    """Verifica se o binário aria2c está disponível no PATH do sistema."""
    return shutil.which("aria2c") is not None  # True se o executável aria2c existe


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# API pública: importa dados do SUS e devolve uma relação DuckDB preguiçosa
def sus_data_import(
    system: str,                              # sistema SUS (ex.: 'SIM-DO', 'SINASC', 'SIH-RD')
    uf: str | list[str],                      # UF(s): sigla, lista, 'all' ou nome de região
    year: int | list[int],                    # ano ou lista de anos
    month: int | list[int] | None = None,     # mês(es) — só SIH; None = todos
    *,                                        # daqui em diante, argumentos só por nome
    cache: bool = True,                       # se True, usa o parquet do cache quando existir
    cache_dir: str | Path = _DEFAULT_CACHE,   # raiz do cache de parquet
    timeout: int = 600,                       # timeout de download (segundos)
    verbose: bool = True,                     # imprime o progresso via Rich
    path: str | Path | None = None,           # modo 2: lê de arquivo local em vez de baixar
    data: pd.DataFrame | None = None,         # modo 1: usa um DataFrame já em memória
    # backend de download
    backend: Literal["auto", "ftp", "pysus"] = "auto",
    store_raw: bool = False,                  # se True, guarda os .dbc crus baixados
    raw_cache_dir: str | Path | None = None,  # diretório dos .dbc crus (padrão: cache/_raw)
) -> duckdb.DuckDBPyRelation:
    """Import SUS data and return a lazy DuckDB relation.

    Supports three input modes:

    1. **``data=``** — wrap an existing ``pandas.DataFrame``.
    2. **``path=``** — read from a local ``.parquet`` or ``.csv`` file.
    3. **Default** — download from the DATASUS FTP, convert ``.dbc`` to
       Parquet and cache locally; subsequent calls read from cache.

    When downloading (mode 3), the *backend* controls which client is
    used:

    - ``"auto"``  — FTP direct download (no extra deps).
    - ``"ftp"``   — FTP + ``.dbc`` reader chain:
      ``climasus_readdbc_py`` → ``climasus_readdbc`` → ``pyreaddbc`` →
      ``pysus`` → ``dbc2dbf`` CLI.
    - ``"pysus"`` — PySUS high-level API (requires
      ``pip install pysus``; needs C compiler on Windows).

    Args:
        system: SUS system identifier, e.g. ``"SIM-DO"``, ``"SINASC"``,
            or ``"SIH-RD"``.
        uf: State abbreviation(s), e.g. ``"SP"`` or ``["SP", "RJ"]``.
            Use ``"all"`` for all states or a region name such as
            ``"Sudeste"``.
        year: Year(s) to import, e.g. ``2022`` or
            ``[2020, 2021, 2022]``.
        month: Month(s) to import (SIH only). ``None`` downloads all 12
            months.
        cache: If ``True``, skip download when a cached Parquet exists.
        cache_dir: Root directory for the Parquet cache.
        timeout: Download timeout in seconds.
        verbose: Print progress messages via Rich.
        path: Local file path to use instead of downloading.
        data: Existing ``DataFrame`` to wrap instead of downloading.
        backend: Download backend — ``"auto"``, ``"ftp"``, or
            ``"pysus"``.
        store_raw: If ``True``, persist downloaded ``.dbc`` files in
            *raw_cache_dir* before conversion.
        raw_cache_dir: Directory for raw ``.dbc`` files. Defaults to
            ``cache_dir / "_raw"`` when *store_raw* is ``True``.

    Returns:
        Lazy ``duckdb.DuckDBPyRelation`` over the imported data.

    Raises:
        RuntimeError: If no data could be imported (download failed or
            no cache hit).
        ValueError: If an unsupported file format is supplied via
            *path*.
        ImportError: If ``.dbc`` reading is attempted but no backend is
            available.

    Example:
        >>> import climasus4py as cs
        >>> rel = cs.sus_data_import("SIM-DO", "SP", 2022)
        >>> rel.count()
        334303
        >>> cs.sus_data_import("SIM-DO", "SP", 2022,
        ...               path="dados/cache/SP_2022.parquet")
    """
    cache_dir = Path(cache_dir)                              # normaliza para Path
    # diretório dos .dbc crus (usa cache/_raw por padrão)
    raw_cache_path = Path(raw_cache_dir) if raw_cache_dir is not None else cache_dir / "_raw"
    ufs = resolve_uf(uf)                                     # normaliza UF para lista de siglas
    years = [year] if isinstance(year, int) else list(year)  # garante uma lista de anos
    # garante lista de meses ([None] = todos)
    months = cast(list[int | None], [month] if isinstance(month, int) else (month or [None]))

    parquet_paths: list[Path] = []  # caminhos dos parquets que comporão a saída

    if data is not None:              # MODO 1: usa o DataFrame recebido
        # Mode 1: inline data
        # corrige os tipos numa cópia (não toca o original)
        data = _coerce_datasus_types(data.copy())
        # caminho do parquet de saída
        target = cache_dir / system / f"inline_{'_'.join(ufs)}_{years[0]}.parquet"
        # garante o diretório
        target.parent.mkdir(parents=True, exist_ok=True)
        # salva como parquet (atômico)
        _write_parquet_atomic(pa.Table.from_pandas(data), target)
        parquet_paths.append(target)  # entra na lista de saída

    elif path is not None:                                       # MODO 2: lê de arquivo local
        # Mode 2: local file
        p = Path(path)                                           # caminho do arquivo de entrada
        if p.suffix == ".parquet":                               # se for .parquet...
            df = pq.read_table(p).to_pandas()                    # lê o parquet para DataFrame
        elif p.suffix == ".csv":                                 # se for .csv...
            df = pd.read_csv(p)                                  # lê o csv para DataFrame
        else:                                                    # qualquer outro formato...
            # ...não é suportado
            raise ValueError(f"Unsupported file format: {p.suffix}")
        # caminho do parquet de saída
        target = cache_dir / system / f"file_{'_'.join(ufs)}_{years[0]}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)         # garante o diretório
        _write_parquet_atomic(pa.Table.from_pandas(df), target)  # salva como parquet (atômico)
        parquet_paths.append(target)                             # entra na lista de saída

    else:                                             # MODO 3: baixa do FTP do DATASUS
        # Mode 3: download from DATASUS
        needed: list[dict] = []                       # lista do que falta baixar
        # partições: 'BR' se nacional, senão as UFs
        partition_ufs = ["BR"] if _geographic_scope(system) == "national" else ufs
        for one_uf in partition_ufs:                  # para cada partição
            for one_year in years:                    # para cada ano
                for one_month in months:              # para cada mês
                    # rótulo do mês ('all' se None)
                    month_str = f"{one_month:02d}" if one_month else "all"
                    # id da partição (UF ou BR)
                    partition_id = _cache_partition_id(system, one_uf)
                    # caminho do parquet alvo
                    target = cache_dir / system / f"{partition_id}_{one_year}_{month_str}.parquet"
                    if cache and target.is_file():    # se já está em cache...
                        parquet_paths.append(target)  # ...usa o existente
                    else:                             # senão...
                        needed.append(                # ...marca para baixar
                            {
                                "uf": one_uf,         # UF/partição
                                "year": one_year,     # ano
                                "month": one_month,   # mês
                                "target": target,     # destino
                            }
                        )

        if needed:                          # se há arquivos faltando...
            use_pysus = backend == "pysus"  # usa PySUS só se pedido explicitamente

            # rótulo do backend para as mensagens
            engine_label = "PySUS" if use_pysus else "FTP"
            if verbose:         # em modo verboso...
                console.print(  # anuncia quantos arquivos serão baixados
                    f"[cyan]ℹ[/] Downloading {len(needed)} file(s) via {engine_label}..."
                )

            for item in needed:             # baixa cada item pendente
                result: Path | None = None  # caminho gerado (ou None se falhar)

                if use_pysus:                           # ramo PySUS
                    try:                                # tenta baixar via PySUS
                        df = _download_pysus(           # baixa o DataFrame
                            system, item["uf"], item["year"], item["month"]
                        )
                        df = _coerce_datasus_types(df)  # corrige os tipos
                        # garante o diretório
                        item["target"].parent.mkdir(parents=True, exist_ok=True)
                        # salva como parquet (atômico)
                        _write_parquet_atomic(pa.Table.from_pandas(df), item["target"])
                        result = item["target"]         # marca sucesso
                        if verbose:                     # em modo verboso...
                            console.print(              # avisa sucesso (nº de linhas)
                                f"[green]✔[/]  {item['uf']}_{item['year']} ({len(df):,} rows)"
                            )
                    except Exception as e:              # se o PySUS falhar...
                        if verbose:                     # em modo verboso...
                            console.print(              # avisa o erro
                                f"[red]✗[/]  {item['uf']}_{item['year']}: {e}"
                            )
                else:                                   # ramo FTP (padrão)
                    result = _download_and_cache(       # baixa + converte + cacheia
                        system,
                        item["uf"],
                        item["year"],
                        item["month"],
                        item["target"],
                        verbose,
                        timeout,
                        store_raw,
                        raw_cache_path if store_raw else None,
                    )

                if result:                        # se gerou um parquet...
                    parquet_paths.append(result)  # ...adiciona à saída

    if not parquet_paths:  # se nada foi importado...
        # ...erro
        raise RuntimeError("No data imported — check system/uf/year parameters.")

    rel = read_parquets(parquet_paths)  # lê todos os parquets como uma relação DuckDB
    # monta o filtro por UF, se necessário
    filter_expr = _state_filter_expression(system, ufs)
    if filter_expr:                     # se há filtro...
        rel = rel.filter(filter_expr)   # ...aplica o filtro por UF
    # marca o estágio e devolve a relação
    return set_stage(rel, "import", system=system, rel_type="health")
