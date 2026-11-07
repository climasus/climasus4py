"""Tests for sus_export — parquet, csv, xlsx export."""

import pandas as pd
import pyarrow.parquet as pq
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.io.export import sus_export


def _make_rel(data: dict):
    conn = get_connection()
    return conn.from_df(pd.DataFrame(data))


@pytest.fixture
def sample_rel():
    return _make_rel({
        "name": ["Alice", "Bob", "Carol"],
        "value": [10, 20, 30],
    })


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Carol"],
        "value": [10, 20, 30],
    })


class TestExportParquet:
    def test_relation_to_parquet(self, tmp_path, sample_rel):
        path = tmp_path / "out.parquet"
        result = sus_export(sample_rel, path)
        assert result == path
        assert path.is_file()
        table = pq.read_table(path)
        assert table.num_rows == 3

    def test_dataframe_to_parquet(self, tmp_path, sample_df):
        path = tmp_path / "out.parquet"
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            sus_export(sample_df, path)


class TestExportCSV:
    def test_relation_to_csv(self, tmp_path, sample_rel):
        path = tmp_path / "out.csv"
        sus_export(sample_rel, path)
        assert path.is_file()
        df = pd.read_csv(path)
        assert len(df) == 3

    def test_dataframe_to_csv(self, tmp_path, sample_df):
        path = tmp_path / "out.csv"
        with pytest.raises(TypeError, match="DuckDBPyRelation"):
            sus_export(sample_df, path)


class TestOverwriteDefault:
    """O default protege o arquivo existente, como no R (M17, 09/09/2026).

    O default estava INVERTIDO entre as duas linguagens: no R
    ``sus_data_export(overwrite = FALSE)`` e aqui era ``overwrite=True``. A
    mesma chamada que o R RECUSA para proteger um arquivo, o Python executava
    sobrescrevendo -- sem erro e sem aviso, dado anterior perdido. Corrigir
    aqui e APLICAR a regra de paridade, nao exceta-la: o R esta certo.
    """

    def test_default_refuses_to_replace(self, tmp_path, sample_rel):
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        with pytest.raises(FileExistsError, match="overwrite=True"):
            sus_export(sample_rel, path)

    def test_the_previous_file_survives_the_refusal(self, tmp_path, sample_rel):
        """Recusar nao basta: o arquivo anterior tem de ficar intacto.

        Sem esta, a correcao passaria com uma implementacao que truncasse o
        arquivo antes de checar a existencia.
        """
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        antes = path.read_bytes()
        with pytest.raises(FileExistsError):
            sus_export(sample_rel, path)
        assert path.read_bytes() == antes

    def test_explicit_true_still_replaces(self, tmp_path, sample_rel):
        """Contrapartida: sobrescrever de proposito continua possivel."""
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        assert sus_export(sample_rel, path, overwrite=True) == path

    def test_first_write_needs_no_flag(self, tmp_path, sample_rel):
        """E o caminho comum -- arquivo novo -- nao pode ter ficado mais chato."""
        path = tmp_path / "novo.parquet"
        assert sus_export(sample_rel, path) == path
        assert path.is_file()

    def test_message_says_what_to_do(self, tmp_path, sample_rel):
        """Mensagem acionavel: nomeia o arquivo e diz como sobrescrever."""
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        with pytest.raises(FileExistsError) as erro:
            sus_export(sample_rel, path)
        texto = str(erro.value)
        assert path.name in texto
        assert "overwrite=True" in texto


class TestExportEdgeCases:
    def test_overwrite_false_raises(self, tmp_path, sample_rel):
        path = tmp_path / "out.parquet"
        sus_export(sample_rel, path)
        with pytest.raises(FileExistsError):
            sus_export(sample_rel, path, overwrite=False)

    def test_unsupported_format_raises(self, tmp_path, sample_rel):
        path = tmp_path / "out.xyz"
        with pytest.raises(ValueError, match="Unsupported format"):
            sus_export(sample_rel, path)

    def test_creates_parent_dirs(self, tmp_path, sample_rel):
        path = tmp_path / "sub" / "dir" / "out.csv"
        sus_export(sample_rel, path)
        assert path.is_file()

    def test_explicit_format_overrides_extension(self, tmp_path, sample_rel):
        path = tmp_path / "out.txt"
        sus_export(sample_rel, path, fmt="csv")
        assert path.is_file()
        df = pd.read_csv(path)
        assert len(df) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
