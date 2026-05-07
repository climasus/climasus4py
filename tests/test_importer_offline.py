"""Tests offline para importer.py — sem I/O real de rede.

Cobre _coerce_datasus_types, _download_ftp (retry), _resolve_dbc_path,
_read_dbc fallback chain e sus_data_import (modo data= e cache hit).
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ---------------------------------------------------------------------------
# Helpers de mock
# ---------------------------------------------------------------------------

def _make_urlopen_success(content: bytes = b"fake dbc content"):
    """Retorna mock de urlopen que simula download bem-sucedido."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = content
    mock_resp.__enter__ = MagicMock(return_value=mock_resp)
    mock_resp.__exit__ = MagicMock(return_value=False)
    return lambda url, timeout=120: mock_resp


def _make_urlopen_failure(exc: Exception | None = None):
    """Retorna mock de urlopen que levanta exceção."""
    error = exc or OSError("FTP connection refused")

    def _fail(url, timeout=120):
        raise error

    return _fail


# ---------------------------------------------------------------------------
# TestCoerceTypes — _coerce_datasus_types
# ---------------------------------------------------------------------------

class TestCoerceTypes:
    def test_date_column_dtobito_coerced(self):
        """DTOBITO no formato DDMMYYYY deve virar datetime64."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"DTOBITO": ["01012022", "15062021"]})
        result = _coerce_datasus_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["DTOBITO"])
        assert result["DTOBITO"].iloc[0] == pd.Timestamp("2022-01-01")
        assert result["DTOBITO"].iloc[1] == pd.Timestamp("2021-06-15")

    def test_invalid_date_becomes_nat(self):
        """Data inválida (p.ex. '99999999') deve virar NaT."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"DTOBITO": ["99999999"]})
        result = _coerce_datasus_types(df)
        assert pd.isna(result["DTOBITO"].iloc[0])

    def test_numeric_column_codmunres_coerced(self):
        """CODMUNRES (coluna numérica) deve ser convertida para float/int."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"CODMUNRES": ["355030", "330455"]})
        result = _coerce_datasus_types(df)
        assert pd.api.types.is_numeric_dtype(result["CODMUNRES"])
        assert result["CODMUNRES"].iloc[0] == 355030

    def test_invalid_numeric_becomes_nan(self):
        """Valor não-numérico em coluna numérica deve virar NaN."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"CONTADOR": ["abc", "1"]})
        result = _coerce_datasus_types(df)
        assert pd.isna(result["CONTADOR"].iloc[0])
        assert result["CONTADOR"].iloc[1] == 1

    def test_string_column_whitespace_stripped(self):
        """Colunas string devem ter espaços removidos."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"CAUSABAS": ["  J189  ", " I219 "]})
        result = _coerce_datasus_types(df)
        assert result["CAUSABAS"].iloc[0] == "J189"
        assert result["CAUSABAS"].iloc[1] == "I219"

    def test_empty_string_becomes_none(self):
        """String vazia em coluna de texto deve virar None."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"CAUSABAS": [""]})
        result = _coerce_datasus_types(df)
        assert result["CAUSABAS"].iloc[0] is None

    def test_nan_string_becomes_none(self):
        """String 'nan' em coluna de texto deve virar None."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({"CAUSABAS": ["nan"]})
        result = _coerce_datasus_types(df)
        assert result["CAUSABAS"].iloc[0] is None

    def test_mixed_df_all_columns_processed(self):
        """DataFrame com data, numérico e string: todos processados."""
        from climasus4py.core.importer import _coerce_datasus_types

        df = pd.DataFrame({
            "DTOBITO": ["01012022"],
            "CODMUNRES": ["355030"],
            "CAUSABAS": [" J189 "],
        })
        result = _coerce_datasus_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["DTOBITO"])
        assert result["CODMUNRES"].iloc[0] == 355030
        assert result["CAUSABAS"].iloc[0] == "J189"


# ---------------------------------------------------------------------------
# TestDownloadFtp — _download_ftp com urllib mockado
# ---------------------------------------------------------------------------

class TestDownloadFtp:
    def test_successful_download_writes_file(self, tmp_path, monkeypatch):
        """Download bem-sucedido deve escrever arquivo e retornar True."""
        from climasus4py.core.importer import _download_ftp

        content = b"fake dbc bytes"
        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_success(content))

        dest = tmp_path / "data.dbc"
        result = _download_ftp("ftp://test.example.com/file.dbc", dest, timeout=10)

        assert result is True
        assert dest.exists()
        assert dest.read_bytes() == content

    def test_failed_download_returns_false(self, tmp_path, monkeypatch):
        """Falha no urlopen deve retornar False sem criar arquivo."""
        from climasus4py.core.importer import _download_ftp

        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_failure())

        dest = tmp_path / "data.dbc"
        result = _download_ftp("ftp://fail.example.com/file.dbc", dest, timeout=10)

        assert result is False
        assert not dest.exists()

    def test_failure_removes_partial_file_if_exists(self, tmp_path, monkeypatch):
        """Se arquivo já existia e urlopen falha, arquivo deve ser removido."""
        from climasus4py.core.importer import _download_ftp

        dest = tmp_path / "data.dbc"
        dest.write_bytes(b"conteudo antigo")  # arquivo pré-existente

        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_failure())

        result = _download_ftp("ftp://fail.example.com/file.dbc", dest, timeout=10)

        assert result is False
        assert not dest.exists(), "Arquivo pré-existente deveria ter sido removido"

    def test_parent_directory_created(self, tmp_path, monkeypatch):
        """Diretório pai deve ser criado automaticamente se não existir."""
        from climasus4py.core.importer import _download_ftp

        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_success(b"data"))

        dest = tmp_path / "subdir" / "nested" / "data.dbc"
        assert not dest.parent.exists()

        result = _download_ftp("ftp://test.example.com/file.dbc", dest, timeout=10)

        assert result is True
        assert dest.exists()


# ---------------------------------------------------------------------------
# TestResolveDbc — _resolve_dbc_path com retry
# ---------------------------------------------------------------------------

class TestResolveDbc:
    def test_first_url_success_returns_path(self, tmp_path, monkeypatch):
        """Primeiro URL bem-sucedido deve retornar o caminho do arquivo."""
        from climasus4py.core.importer import _resolve_dbc_path

        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_success(b"content"))

        result = _resolve_dbc_path(
            urls=["ftp://ok.example.com/file.dbc"],
            tmp_dir=tmp_path,
            timeout=10,
            store_raw=False,
            raw_cache_dir=None,
        )

        assert result is not None
        assert result.exists()

    def test_retry_succeeds_on_second_url(self, tmp_path, monkeypatch):
        """Primeiro URL falha; segundo deve ser tentado e ter sucesso."""
        from climasus4py.core.importer import _resolve_dbc_path

        call_count = 0

        def mock_urlopen(url, timeout=120):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("First URL failed")
            mock_resp = MagicMock()
            mock_resp.read.return_value = b"success on retry"
            mock_resp.__enter__ = MagicMock(return_value=mock_resp)
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

        result = _resolve_dbc_path(
            urls=[
                "ftp://fail.example.com/file.dbc",
                "ftp://ok.example.com/file.dbc",
            ],
            tmp_dir=tmp_path,
            timeout=10,
            store_raw=False,
            raw_cache_dir=None,
        )

        assert result is not None
        assert result.exists()
        assert call_count == 2

    def test_all_urls_fail_returns_none(self, tmp_path, monkeypatch):
        """Se todos os URLs falham, deve retornar None."""
        from climasus4py.core.importer import _resolve_dbc_path

        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_failure())

        result = _resolve_dbc_path(
            urls=[
                "ftp://fail1.example.com/file.dbc",
                "ftp://fail2.example.com/file.dbc",
            ],
            tmp_dir=tmp_path,
            timeout=10,
            store_raw=False,
            raw_cache_dir=None,
        )

        assert result is None

    def test_empty_url_list_returns_none(self, tmp_path):
        """Lista de URLs vazia deve retornar None sem chamar urlopen."""
        from climasus4py.core.importer import _resolve_dbc_path

        result = _resolve_dbc_path(
            urls=[],
            tmp_dir=tmp_path,
            timeout=10,
            store_raw=False,
            raw_cache_dir=None,
        )

        assert result is None

    def test_store_raw_returns_existing_file(self, tmp_path, monkeypatch):
        """store_raw=True com arquivo já existente deve retornar sem baixar."""
        from climasus4py.core.importer import _resolve_dbc_path

        # Pré-criar o arquivo no cache raw
        raw_cache = tmp_path / "raw"
        raw_cache.mkdir()
        # Simular URL → caminho local
        # O caminho derivado pelo _raw_cache_path: raw_cache / netloc / path
        cached = raw_cache / "ftp.datasus.gov.br" / "file.dbc"
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(b"cached content")

        # urlopen não deve ser chamado
        def fail_open(*a, **kw):
            raise AssertionError("urlopen não deveria ser chamado em cache hit")

        monkeypatch.setattr(urllib.request, "urlopen", fail_open)

        result = _resolve_dbc_path(
            urls=["ftp://ftp.datasus.gov.br/file.dbc"],
            tmp_dir=tmp_path,
            timeout=10,
            store_raw=True,
            raw_cache_dir=raw_cache,
        )

        assert result == cached


# ---------------------------------------------------------------------------
# TestReadDbc — _read_dbc fallback chain
# ---------------------------------------------------------------------------

class TestReadDbc:
    def test_all_backends_unavailable_raises_import_error(self, tmp_path, monkeypatch):
        """Se todos os backends de leitura DBC falharem, deve levantar ImportError."""
        from climasus4py.core.importer import _read_dbc

        # Tornar todos os módulos de leitura indisponíveis
        for mod_name in (
            "climasus_readdbc_py",
            "climasus_readdbc",
            "pyreaddbc",
            "pysus",
            "pysus.utilities.readdbc",
            "dbfread",
        ):
            monkeypatch.setitem(sys.modules, mod_name, None)

        # Garantir que dbc2dbf não está no PATH
        import shutil
        monkeypatch.setattr(shutil, "which", lambda *a, **kw: None)

        fake_dbc = tmp_path / "fake.dbc"
        fake_dbc.write_bytes(b"not a real dbc")

        with pytest.raises(ImportError, match="climasus_readdbc_py"):
            _read_dbc(fake_dbc)

    def test_climasus_readdbc_py_used_when_available(self, tmp_path, monkeypatch):
        """Backend climasus_readdbc_py deve ser tentado primeiro."""
        from climasus4py.core import importer as _imp

        expected_df = pd.DataFrame({"COL": ["value"]})

        mock_module = MagicMock()
        mock_module.read_dbc.return_value = expected_df

        monkeypatch.setitem(sys.modules, "climasus_readdbc_py", mock_module)

        fake_dbc = tmp_path / "fake.dbc"
        fake_dbc.write_bytes(b"fake")

        # Reimportar a função para usar o módulo mockado no sys.modules
        # _read_dbc faz `import climasus_readdbc_py as readdbc` dentro do try
        result = _imp._read_dbc(fake_dbc)
        assert list(result.columns) == ["COL"]
        mock_module.read_dbc.assert_called_once_with(fake_dbc)


# ---------------------------------------------------------------------------
# TestSusImportModes — sus_data_import sem rede
# ---------------------------------------------------------------------------

class TestSusImportModes:
    def test_data_mode_wraps_dataframe(self, tmp_path):
        """sus_data_import(data=df) deve retornar relação DuckDB sem download."""
        from climasus4py.core.importer import sus_data_import

        df = pd.DataFrame({
            "DTOBITO": pd.to_datetime(["2022-01-01", "2022-02-15"]),
            "CAUSABAS": ["J189", "I219"],
            "IDADE": [420, 460],
        })

        rel = sus_data_import("SIM-DO", "SP", 2022, data=df, cache_dir=tmp_path, verbose=False)

        assert rel is not None
        result_df = rel.df()
        assert len(result_df) == 2

    def test_data_mode_preserves_row_count(self, tmp_path):
        """Modo data= deve preservar número de linhas."""
        from climasus4py.core.importer import sus_data_import

        df = pd.DataFrame({"CAUSABAS": [f"J{i:03d}" for i in range(10)]})
        rel = sus_data_import("SIM-DO", "SP", 2022, data=df, cache_dir=tmp_path, verbose=False)

        count = rel.count("*").fetchone()[0]
        assert count == 10

    def test_cache_hit_skips_download(self, tmp_path, monkeypatch):
        """Quando parquet já existe no cache, download não deve ocorrer."""
        from climasus4py.core import importer as _imp

        # Pré-criar o arquivo de cache esperado
        cache_file = tmp_path / "SIM-DO" / "SP_2022_all.parquet"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        df_cache = pd.DataFrame({
            "DTOBITO": pd.to_datetime(["2022-03-10"]),
            "CAUSABAS": ["J189"],
        })
        pq.write_table(pa.Table.from_pandas(df_cache), cache_file)

        # Mockar funções que lêem metadados externos
        monkeypatch.setattr(_imp, "resolve_uf", lambda uf: ["SP"])
        monkeypatch.setattr(_imp, "_geographic_scope", lambda s: "state")
        monkeypatch.setattr(_imp, "_cache_partition_id", lambda s, u: u)
        monkeypatch.setattr(_imp, "_state_filter_expression", lambda s, u: None)

        # urlopen deve falhar se chamado (não deve ser chamado em cache hit)
        def fail_urlopen(*a, **kw):
            raise AssertionError("urlopen não deve ser chamado em cache hit")

        monkeypatch.setattr(urllib.request, "urlopen", fail_urlopen)

        rel = _imp.sus_data_import("SIM-DO", "SP", 2022, cache_dir=tmp_path, verbose=False)

        assert rel is not None
        result_df = rel.df()
        assert len(result_df) == 1

    def test_path_mode_reads_parquet(self, tmp_path):
        """sus_data_import(path=...) deve ler parquet local diretamente."""
        from climasus4py.core.importer import sus_data_import

        # Criar parquet local
        df_src = pd.DataFrame({"CAUSABAS": ["J189", "I219", "K920"]})
        parquet_path = tmp_path / "source.parquet"
        pq.write_table(pa.Table.from_pandas(df_src), parquet_path)

        rel = sus_data_import(
            "SIM-DO", "SP", 2022,
            path=str(parquet_path),
            cache_dir=tmp_path,
            verbose=False,
        )

        assert rel is not None
        count = rel.count("*").fetchone()[0]
        assert count == 3

    def test_path_mode_unsupported_format_raises(self, tmp_path):
        """Formato de arquivo não suportado deve levantar ValueError."""
        from climasus4py.core.importer import sus_data_import

        fake_xlsx = tmp_path / "data.xlsx"
        fake_xlsx.write_bytes(b"fake xlsx")

        with pytest.raises(ValueError, match="Unsupported file format"):
            sus_data_import("SIM-DO", "SP", 2022, path=str(fake_xlsx), cache_dir=tmp_path, verbose=False)

    def test_no_data_raises_runtime_error(self, tmp_path, monkeypatch):
        """Quando download falha e cache está vazio, deve levantar RuntimeError."""
        from climasus4py.core import importer as _imp

        monkeypatch.setattr(_imp, "resolve_uf", lambda uf: ["SP"])
        monkeypatch.setattr(_imp, "_geographic_scope", lambda s: "state")
        monkeypatch.setattr(_imp, "_cache_partition_id", lambda s, u: u)
        monkeypatch.setattr(_imp, "_state_filter_expression", lambda s, u: None)
        # Simular falha em todos os downloads
        monkeypatch.setattr(urllib.request, "urlopen", _make_urlopen_failure())
        # Sem DBC reader disponível
        monkeypatch.setattr(_imp, "_resolve_dbc_path", lambda *a, **kw: None)

        with pytest.raises(RuntimeError, match="No data imported"):
            _imp.sus_data_import(
                "SIM-DO", "SP", 2022,
                cache_dir=tmp_path,
                cache=False,
                verbose=False,
            )
