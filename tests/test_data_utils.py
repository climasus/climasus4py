"""Tests for decode_age_sql utility and data.py safety checks."""

import pytest

from climasus4py.core.engine import get_connection
from climasus4py.utils.data import decode_age_sql, update_climasus_data


class TestDecodeAgeSql:
    """Validate the decode_age_sql expression via DuckDB execution."""

    def _decode(self, value: str) -> int | None:
        conn = get_connection()
        expr = decode_age_sql("age")
        row = conn.sql(f"SELECT {expr} AS decoded FROM (SELECT '{value}' AS age)").fetchone()
        return row[0]

    def test_code_4_years(self):
        assert self._decode("420") == 20
        assert self._decode("401") == 1
        assert self._decode("499") == 99

    def test_code_5_centenarian(self):
        assert self._decode("500") == 100
        assert self._decode("520") == 120

    def test_code_3_months_is_zero(self):
        assert self._decode("301") == 0
        assert self._decode("311") == 0

    def test_code_2_days_is_zero(self):
        assert self._decode("201") == 0

    def test_code_1_hours_is_zero(self):
        assert self._decode("112") == 0

    def test_code_0_minutes_is_zero(self):
        assert self._decode("030") == 0

    # ---------------------------------------------------------------
    # M6 — sentinela de idade desconhecida
    # ---------------------------------------------------------------

    def test_sentinel_999_is_null(self):
        """999 e "idade desconhecida", nao 999 anos (M6, corrigido 09/09/2026).

        Caia no TRY_CAST final e voltava como 999, e as colunas derivadas
        arquivavam esses registros como age_group='60+',
        ibge_age_group='80+' e climate_risk_group='High Risk (65+)' --
        inflando as faixas de idosos com gente de idade desconhecida. No
        SIM-DO SP 2023 eram 335 registros. O R devolve NA.
        """
        assert self._decode("999") is None

    @pytest.mark.parametrize("codigo", ["600", "700", "812", "998"])
    def test_any_out_of_range_unit_digit_is_null(self, codigo):
        """A regra e estrutural: unidade valida e 0-5, o resto nao e idade.

        Cobre o 999 e qualquer outro codigo indecodificavel, em vez de
        tratar so o valor conhecido -- que deixaria 6xx/7xx/8xx virando
        idades absurdas se aparecerem.
        """
        assert self._decode(codigo) is None

    def test_valid_unit_digits_still_decode(self):
        """Contrapartida: a recusa nao pode ter comido codigo legitimo.

        Sem esta, a correcao passaria mesmo se tivesse anulado tudo.
        """
        assert self._decode("400") == 0     # 0 anos completos
        assert self._decode("465") == 65
        assert self._decode("599") == 199   # unidade 5 soma 100
        assert self._decode("099") == 0     # minutos

    def test_plain_integer_fallback(self):
        """Non-coded values should be cast as integer."""
        assert self._decode("25") == 25
        assert self._decode("0") == 0

    def test_three_digit_rule_does_not_touch_shorter_values(self):
        """A regra vale para 3 digitos: idade ja decodificada nao e afetada.

        SINAN e dado ja padronizado chegam com idade simples, e o fallback
        de TRY_CAST tem de continuar valendo para eles.
        """
        assert self._decode("99") == 99
        assert self._decode("9") == 9

    def test_empty_string_returns_null(self):
        assert self._decode("") is None


class TestUpdateClimaSUSDataSafety:
    def test_rmtree_rejects_non_climasus_dir(self, tmp_path):
        """rmtree should refuse to delete a directory without manifest.json."""
        target = tmp_path / "not_climasus"
        target.mkdir()
        (target / "some_file.txt").write_text("important data")

        with pytest.raises(RuntimeError, match="manifest.json"):
            update_climasus_data(target_dir=str(target))


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
