"""Basic tests for climasus4py utilities."""

from climasus.utils.cid import expand_cid_range, expand_cid_ranges
from climasus.utils.encoding import fix_encoding
from climasus.utils.data import resolve_uf, system_family


class TestCID:
    def test_expand_single_range(self):
        codes = expand_cid_range("A90", "A92")
        assert codes == ["A90", "A91", "A92"]

    def test_expand_cross_letter(self):
        codes = expand_cid_range("A98", "B02")
        assert "A98" in codes
        assert "A99" in codes
        assert "B00" in codes
        assert "B02" in codes

    def test_expand_mixed_list(self):
        codes = expand_cid_ranges(["A90", "B00-B02", "J"])
        assert "A90" in codes
        assert "B00" in codes
        assert "B01" in codes
        assert "B02" in codes
        assert "J00" in codes
        assert "J99" in codes


class TestEncoding:
    def test_fix_mojibake(self):
        assert fix_encoding("JOÃ£O") == "JOãO"
        assert fix_encoding("SÃ£O PAULO") == "SãO PAULO"

    def test_fix_encoding_clean_text_unchanged(self):
        assert fix_encoding("São Paulo") == "São Paulo"


class TestData:
    def test_system_family(self):
        assert system_family("SIM-DO") == "SIM"
        assert system_family("SIH-RD") == "SIH"
        assert system_family("SINASC") == "SINASC"

    def test_resolve_uf_single(self):
        result = resolve_uf("SP")
        assert result == ["SP"]

    def test_resolve_uf_list(self):
        result = resolve_uf(["SP", "RJ"])
        assert result == ["SP", "RJ"]
