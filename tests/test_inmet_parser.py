"""Tests for inmet_parser — parsing de CSVs horários do INMET.

Usa tmp_path para criar arquivos temporários; sem I/O de rede.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from climasus4py.utils.inmet_parser import parse_inmet_csv

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "inmet"

# ---------------------------------------------------------------------------
# Fixtures de CSV INMET
# ---------------------------------------------------------------------------

# CSV mínimo válido: 7 linhas de metadados + cabeçalho de colunas + dados
# Usa nomes de colunas que existem exatamente em _COL_MAP para garantir mapeamento.
_MINIMAL_CSV = """\
REGIAO:;SUL
UF:;PR
ESTACAO:;CURITIBA
CODIGO ESTACAO:;A803
LATITUDE:;-25,43
LONGITUDE:;-49,27
ALTITUDE:;923,5
Data;Hora UTC;Precipitacao Total, Horario (mm);Umidade Relativa do Ar (%);Radiacao Global (kJ/m2)
2023-01-01;0000;0,4;68;125,5
2023-01-01;0100;0,0;70;0,0
2023-01-01;0200;-9999;-9999;-9999
"""

# CSV com vírgula decimal em coordenadas e dados
_CSV_DECIMAL_COMMA = """\
REGIAO:;CENTRO-OESTE
UF:;MT
ESTACAO:;CUIABA
CODIGO ESTACAO:;A901
LATITUDE:;-15,55
LONGITUDE:;-56,07
ALTITUDE:;151,67
Data;Hora UTC;Temperatura Do Ar - Bulbo Seco (°C);Umidade Relativa do Ar (%)
2023-06-15;1200;27,4;55
2023-06-15;1300;28,1;53
"""

# CSV com arquivo totalmente inválido (sem separador de colunas útil)
_INVALID_CSV = "ESTE NAO E UM CSV VALIDO\nSEM PONTO E VIRGULA\nSEM DADOS\n"


def _write_csv(tmp_path: Path, content: str, name: str = "test.csv") -> Path:
    """Escreve content em arquivo latin-1 em tmp_path."""
    p = tmp_path / name
    p.write_text(content, encoding="latin-1")
    return p


# ---------------------------------------------------------------------------
# TestParseHeader — extração de metadados do cabeçalho
# ---------------------------------------------------------------------------

class TestParseHeader:
    def test_region_extracted(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "region" in df.columns
        assert df["region"].iloc[0] == "SUL"

    def test_uf_extracted(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "UF" in df.columns
        assert df["UF"].iloc[0] == "PR"

    def test_station_name_extracted(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "station_name" in df.columns
        assert df["station_name"].iloc[0] == "CURITIBA"

    def test_station_code_extracted(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "station_code" in df.columns
        assert df["station_code"].iloc[0] == "A803"

    def test_latitude_numeric(self, tmp_path):
        """Latitude com vírgula decimal deve ser convertida para float."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "latitude" in df.columns
        lat = df["latitude"].iloc[0]
        assert isinstance(lat, float)
        assert abs(lat - (-25.43)) < 1e-4

    def test_longitude_numeric(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "longitude" in df.columns
        lon = df["longitude"].iloc[0]
        assert isinstance(lon, float)
        assert abs(lon - (-49.27)) < 1e-4

    def test_altitude_numeric(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "altitude" in df.columns
        alt = df["altitude"].iloc[0]
        assert isinstance(alt, float)
        assert abs(alt - 923.5) < 1e-4


# ---------------------------------------------------------------------------
# TestParseData — linhas de dados
# ---------------------------------------------------------------------------

class TestParseData:
    def test_header_detection_does_not_match_data_de_fundacao(self):
        path = FIXTURE_DIR / "inmet_2015_SC_A806_FLORIANOPOLIS_stub.CSV"
        df = parse_inmet_csv(path)
        assert df is not None
        assert not any("FUNDA" in col.upper() for col in df.columns)
        assert "date" in df.columns
        ts = pd.Timestamp(df["date"].iloc[0])
        assert ts.year == 2015
        assert ts.month == 1
        assert ts.day == 1

    def test_returns_dataframe(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self, tmp_path):
        """CSV com 3 linhas de dados deve retornar 3 linhas."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert len(df) == 3

    def test_rainfall_column_present_and_mapped(self, tmp_path):
        """'Precipitacao Total, Horario (mm)' deve mapear para 'rainfall_mm'."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "rainfall_mm" in df.columns

    def test_humidity_column_mapped(self, tmp_path):
        """'Umidade Relativa do Ar (%)' deve mapear para 'rh_mean_porc'."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "rh_mean_porc" in df.columns

    def test_solar_radiation_column_mapped(self, tmp_path):
        """'Radiacao Global (kJ/m2)' deve mapear para 'sr_kj_m2'."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "sr_kj_m2" in df.columns

    def test_rainfall_first_row_value(self, tmp_path):
        """Primeira linha: precipitação 0,4 → 0.4 float."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["rainfall_mm"].iloc[0]
        assert abs(val - 0.4) < 1e-6

    def test_humidity_second_row_value(self, tmp_path):
        """Segunda linha: umidade 70 → 70.0 float."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["rh_mean_porc"].iloc[1]
        assert abs(val - 70.0) < 1e-6

    def test_sentinel_minus9999_rainfall_becomes_nan(self, tmp_path):
        """-9999 em precipitação deve ser NaN."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["rainfall_mm"].iloc[2]
        assert pd.isna(val), f"Esperado NaN, obtido {val!r}"

    def test_sentinel_minus9999_humidity_becomes_nan(self, tmp_path):
        """-9999 em umidade deve ser NaN."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["rh_mean_porc"].iloc[2]
        assert pd.isna(val)

    def test_sentinel_minus9999_radiation_becomes_nan(self, tmp_path):
        """-9999 em radiação deve ser NaN."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["sr_kj_m2"].iloc[2]
        assert pd.isna(val)

    def test_date_column_is_datetime(self, tmp_path):
        """Coluna 'date' deve ser datetime64 (UTC)."""
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "date" in df.columns
        assert pd.api.types.is_datetime64_any_dtype(df["date"]), (
            f"Tipo esperado datetime64, obtido {df['date'].dtype}"
        )

    def test_date_first_row_is_2023_01_01(self, tmp_path):
        path = _write_csv(tmp_path, _MINIMAL_CSV)
        df = parse_inmet_csv(path)
        assert df is not None
        ts = pd.Timestamp(df["date"].iloc[0])
        assert ts.year == 2023
        assert ts.month == 1
        assert ts.day == 1


# ---------------------------------------------------------------------------
# TestDecimalComma — vírgula decimal → ponto
# ---------------------------------------------------------------------------

class TestDecimalComma:
    def test_temperature_decimal_comma_converted(self, tmp_path):
        """27,4 em temperatura deve virar 27.4 float."""
        path = _write_csv(tmp_path, _CSV_DECIMAL_COMMA)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "tair_dry_bulb_c" in df.columns
        val = df["tair_dry_bulb_c"].iloc[0]
        assert abs(val - 27.4) < 1e-4

    def test_second_row_temperature(self, tmp_path):
        path = _write_csv(tmp_path, _CSV_DECIMAL_COMMA)
        df = parse_inmet_csv(path)
        assert df is not None
        val = df["tair_dry_bulb_c"].iloc[1]
        assert abs(val - 28.1) < 1e-4

    def test_coordinate_with_comma_in_second_csv(self, tmp_path):
        """Latitude -15,55 deve ser convertida para -15.55."""
        path = _write_csv(tmp_path, _CSV_DECIMAL_COMMA)
        df = parse_inmet_csv(path)
        assert df is not None
        lat = df["latitude"].iloc[0]
        assert abs(lat - (-15.55)) < 1e-4


# ---------------------------------------------------------------------------
# TestEdgeCases — arquivos inválidos e casos limite
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_invalid_file_has_no_canonical_columns(self, tmp_path):
        """Arquivo sem estrutura INMET não deve ter colunas canônicas mapeadas."""
        path = _write_csv(tmp_path, _INVALID_CSV)
        result = parse_inmet_csv(path)
        # Parser pode retornar None ou df sem colunas canônicas
        canonical = {"rainfall_mm", "rh_mean_porc", "sr_kj_m2", "tair_dry_bulb_c", "ws_2_m_s"}
        if result is not None and isinstance(result, pd.DataFrame):
            overlap = canonical & set(result.columns)
            assert not overlap, f"Colunas canônicas não esperadas: {overlap}"

    def test_nonexistent_file_returns_none(self, tmp_path):
        """Arquivo inexistente → parse_inmet_csv retorna None."""
        path = tmp_path / "nao_existe.csv"
        result = parse_inmet_csv(path)
        assert result is None

    def test_encoding_latin1_accepted(self, tmp_path):
        """Arquivo escrito em latin-1 deve ser lido sem erro."""
        # Escreve byte específico do latin-1 (grau: 0xb0)
        content = _MINIMAL_CSV.replace("CURITIBA", "CURITIB\u00c1")  # Á
        path = tmp_path / "latin1.csv"
        path.write_text(content, encoding="latin-1")
        result = parse_inmet_csv(path)
        # Não deve levantar; pode retornar df ou None dependendo do conteúdo
        assert result is None or isinstance(result, pd.DataFrame)

    def test_empty_file_returns_none_or_empty(self, tmp_path):
        """Arquivo vazio → None ou DataFrame vazio."""
        path = tmp_path / "empty.csv"
        path.write_text("", encoding="latin-1")
        result = parse_inmet_csv(path)
        assert result is None or (isinstance(result, pd.DataFrame) and result.empty)

    def test_header_only_no_data_returns_empty_or_none(self, tmp_path):
        """CSV com cabeçalho mas sem linhas de dados."""
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Hora UTC;Precipitacao Total, Horario (mm)\n"
        )
        path = _write_csv(tmp_path, content)
        result = parse_inmet_csv(path)
        # Parser pode retornar df vazio ou None — ambos são aceitáveis
        assert result is None or (isinstance(result, pd.DataFrame) and len(result) == 0)

    def test_physical_qc_removes_out_of_range_humidity(self, tmp_path):
        """Umidade fora do range físico (0–100%) → NaN após QC."""
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Hora UTC;Umidade Relativa do Ar (%)\n"
            "2023-01-01;0000;150\n"  # 150% está fora do range [0, 100]
        )
        path = _write_csv(tmp_path, content)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "rh_mean_porc" in df.columns
        assert pd.isna(df["rh_mean_porc"].iloc[0])

    def test_valid_humidity_not_removed_by_qc(self, tmp_path):
        """Umidade dentro do range (0–100%) não deve ser removida pelo QC."""
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Hora UTC;Umidade Relativa do Ar (%)\n"
            "2023-01-01;0000;75\n"
        )
        path = _write_csv(tmp_path, content)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "rh_mean_porc" in df.columns
        val = df["rh_mean_porc"].iloc[0]
        assert not pd.isna(val)
        assert abs(val - 75.0) < 1e-6


# ---------------------------------------------------------------------------
# TestMissingHoraColumn — CSV sem coluna HORA (cobre linha 261)
# ---------------------------------------------------------------------------

class TestMissingHoraColumn:
    """CSV sem coluna de hora nao reproduz o formato horario INMET real."""

    def test_csv_without_hora_returns_none(self, tmp_path):
        """CSV with only Data column (no Hora) is treated as malformed."""
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Precipitacao Total, Horario (mm)\n"
            "2023-03-15;1.2\n"
            "2023-03-16;0.0\n"
        )
        path = _write_csv(tmp_path, content)
        df = parse_inmet_csv(path)
        assert df is None


# ---------------------------------------------------------------------------
# TestDewPointQC — dew point consistency via Magnus (cobre linhas 285-296)
# ---------------------------------------------------------------------------

class TestDewPointQC:
    """QC for dew point temperature — covers lines 285-296 in _qc_dew_point."""

    def test_dew_point_qc_outlier_becomes_nan(self, tmp_path):
        """Dew point far from Magnus estimate (> 3°C diff) → NaN."""
        # tair=25, rh=50: Magnus Td ≈ 13.9°C; value 40 is >3°C off → NaN
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Hora UTC;"
            "Temperatura Do Ar - Bulbo Seco (°C);"
            "Umidade Relativa do Ar (%);"
            "Temperatura Do Ponto De Orvalho (°C)\n"
            "2023-06-01;1200;25;50;40\n"
        )
        path = _write_csv(tmp_path, content)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "dew_tmean_c" in df.columns
        assert pd.isna(df["dew_tmean_c"].iloc[0])

    def test_dew_point_qc_valid_value_kept(self, tmp_path):
        """Dew point within 3°C of Magnus estimate is kept."""
        # tair=25, rh=50: Magnus Td ≈ 13.9°C; value 14 is within 3°C → kept
        content = (
            "REGIAO:;SUL\nUF:;PR\nESTACAO:;TEST\nCODIGO ESTACAO:;A001\n"
            "LATITUDE:;-25,0\nLONGITUDE:;-49,0\nALTITUDE:;900,0\n"
            "Data;Hora UTC;"
            "Temperatura Do Ar - Bulbo Seco (°C);"
            "Umidade Relativa do Ar (%);"
            "Temperatura Do Ponto De Orvalho (°C)\n"
            "2023-06-01;1200;25;50;14\n"
        )
        path = _write_csv(tmp_path, content)
        df = parse_inmet_csv(path)
        assert df is not None
        assert "dew_tmean_c" in df.columns
        assert not pd.isna(df["dew_tmean_c"].iloc[0])
