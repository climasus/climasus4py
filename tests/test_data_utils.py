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
    # M167 — a idade do SIA é ano puro, não código unidade+quantidade
    # ---------------------------------------------------------------

    def _decode_col(self, coluna: str, valor: str) -> int | None:
        conn = get_connection()
        expr = decode_age_sql(coluna)
        return conn.sql(
            f"SELECT {expr} FROM (SELECT '{valor}' AS \"{coluna}\")"
        ).fetchone()[0]

    @pytest.mark.parametrize(
        "coluna",
        ["PA_IDADE", "patient_age", "idade_paciente", "edad_paciente"],
    )
    def test_a_idade_do_sia_e_lida_em_anos(self, coluna):
        """063 é 63 anos, e não "unidade 0, quantidade 63".

        Pela regra do SIM o dígito 0 significa MINUTOS, então toda idade
        do SIA caía em 0 anos: medido em SIA-PA/AC/janeiro de 2023, as
        72.654 linhas na faixa 0-4 contra 7.165 / 6.512 / 46.746 /
        12.231 no climasus4r, que lê a coluna como ano.
        """
        assert self._decode_col(coluna, "063") == 63
        assert self._decode_col(coluna, "010") == 10
        assert self._decode_col(coluna, "109") == 109
        assert self._decode_col(coluna, "000") == 0

    def test_a_mesma_string_continua_sendo_codigo_fora_do_sia(self):
        """A troca é por COLUNA, não global: o SIM não pode mudar."""
        assert self._decode("063") == 0      # unidade 0 = minutos
        assert self._decode("109") == 0      # unidade 1 = horas
        assert self._decode("463") == 63     # unidade 4 = anos

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


class TestIdadeSinanQuatroDigitos:
    """M179 — o NU_IDADE_N do SINAN tem 4 dígitos: unidade + 3 de quantidade.

    Caía no TRY_CAST final e voltava como 4.028 anos: as 73.778
    notificações do SINAN-DENGUE/Pantanal/2023 foram para '60+'. O
    climasus4r decodifica pelo primeiro dígito, de qualquer tamanho.
    """

    def _decode(self, valor: str) -> int | None:
        expr = decode_age_sql("age_code")
        return get_connection().sql(
            f"SELECT {expr} FROM (SELECT '{valor}' AS age_code)").fetchone()[0]

    def test_unidade_4_sao_anos(self):
        assert self._decode("4028") == 28
        assert self._decode("4000") == 0
        assert self._decode("4122") == 122

    def test_horas_e_dias_sao_zero_anos(self):
        assert self._decode("1012") == 0
        assert self._decode("2030") == 0

    def test_meses_viram_anos_completos(self):
        """Como o R: FLOOR(meses / 12)."""
        assert self._decode("3011") == 0
        assert self._decode("3018") == 1

    def test_unidade_5_soma_100(self):
        assert self._decode("5002") == 102

    @pytest.mark.parametrize("codigo", ["0028", "6028", "9999"])
    def test_unidade_invalida_e_nula(self, codigo):
        assert self._decode(codigo) is None

    def test_tres_digitos_do_sim_nao_mudam(self):
        """Contrapartida: o ramo novo é só para 4 dígitos."""
        assert self._decode("463") == 63
        assert self._decode("311") == 0
        assert self._decode("999") is None


class TestIdadeSihComUnidade:
    """M180 — no SIH a unidade da idade fica em OUTRA coluna (COD_IDADE).

    Lido sozinho, IDADE=4 de um bebê de 4 meses voltava como 4 anos:
    4.147 de 67.084 internações do SIH-RD AC+RR+AP 2022-2023, conferido
    contra a idade que o climasus4r calcula pelas datas.
    """

    def _decode(self, idade: str, unidade: str,
                col: str = "age", col_u: str = "age_unit_code") -> int | None:
        expr = decode_age_sql(col, [col, col_u])
        return get_connection().sql(
            f"SELECT {expr} FROM (SELECT '{idade}' AS \"{col}\", "
            f"'{unidade}' AS \"{col_u}\")").fetchone()[0]

    def test_dias_e_meses_sao_zero_anos(self):
        assert self._decode("25", "2") == 0
        assert self._decode("4", "3") == 0
        assert self._decode("11", "3") == 0

    def test_anos(self):
        assert self._decode("57", "4") == 57

    def test_unidade_5_soma_100(self):
        """Medido: as datas do R dão 100-114 onde IDADE traz 0-14."""
        assert self._decode("0", "5") == 100
        assert self._decode("14", "5") == 114

    def test_unidade_desconhecida_e_nula(self):
        assert self._decode("30", "9") is None

    @pytest.mark.parametrize("col,col_u", [
        ("IDADE", "COD_IDADE"),
        ("idade", "codigo_unidade_idade"),
        ("edad", "codigo_unidad_edad"),
    ])
    def test_vale_nos_quatro_idiomas_e_no_bruto(self, col, col_u):
        assert self._decode("4", "3", col, col_u) == 0
        assert self._decode("40", "4", col, col_u) == 40

    def test_sem_a_coluna_de_unidade_nada_muda(self):
        """O IDADE do SIM não tem companheira: continua código de 3 dígitos."""
        expr = decode_age_sql("IDADE", ["IDADE", "DTOBITO"])
        assert get_connection().sql(
            f"SELECT {expr} FROM (SELECT '463' AS IDADE)").fetchone()[0] == 63


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
