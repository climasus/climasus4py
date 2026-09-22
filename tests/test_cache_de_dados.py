"""M124: o resultado da suíte dependia da ordem, por cache de arquivo.

Vários testes apontam o catálogo para um `tmp_path` com uma cópia
reduzida — `test_lazy_enrichments` troca `CLIMASUS_DATA_DIR` e zera
`_DATA_DIR`, outros trocam o `data_path` direto. Devolver a variável de
ambiente e a global no fim, que é o que o `monkeypatch` faz, **não
basta**: `load_json` e irmãs são `lru_cache` sobre o *caminho relativo*
apenas, então o arquivo falso que o teste carregou fica no cache pelo
resto do processo e todo teste seguinte recebe ele.

Medido, contra o código commitado:

    pytest tests/test_colunas_geograficas.py                  -> passa
    pytest tests/test_lazy_enrichments.py tests/test_colunas_geograficas.py
        -> KeyError: 'schema_version'

O conserto é um fixture `autouse` no `conftest.py` que limpa os caches
depois de cada teste. Estes testes guardam esse fixture: se alguém
renomear uma das funções cacheadas, o fixture para de limpá-la em
silêncio — ele usa `getattr` e não reclama de nome que não existe — e a
dependência de ordem volta sem ninguém notar.
"""

from __future__ import annotations

import json

import pytest

from climasus4py.utils import data as _data

#: As funções que o fixture do conftest limpa. Manter em sincronia.
CACHEADAS = ("load_json", "load_datasus_columns_spec", "_municipio_meta")


@pytest.mark.parametrize("nome", CACHEADAS)
def test_a_funcao_cacheada_existe_e_e_limpavel(nome: str) -> None:
    alvo = getattr(_data, nome, None)
    assert alvo is not None, f"{nome} sumiu; o fixture do conftest não limpa mais"
    assert hasattr(alvo, "cache_clear"), f"{nome} deixou de ser cacheada"


def test_o_conftest_cobre_toda_funcao_cacheada_do_modulo() -> None:
    """A lista não pode ficar para trás quando alguém cachear mais uma.

    Este é o teste que importa: acrescentar um `@lru_cache` novo e
    esquecer do conftest recria o defeito, e sem esta varredura ninguém
    descobre até uma falha intermitente aparecer.
    """
    encontradas = {
        nome for nome in dir(_data)
        if hasattr(getattr(_data, nome, None), "cache_clear")
    }
    faltando = encontradas - set(CACHEADAS)
    assert not faltando, (
        f"funções cacheadas fora da lista do conftest: {sorted(faltando)}"
    )


def test_o_cache_de_fato_devolve_o_arquivo_trocado(tmp_path, monkeypatch) -> None:
    """A demonstração do mecanismo, e não só do encanamento.

    Sem o `cache_clear`, a segunda leitura devolveria o conteúdo falso
    mesmo depois de o `data_path` voltar ao lugar — que é exatamente o
    que acontecia entre módulos de teste.
    """
    real = _data.load_json("metadata/uf_codes.json")
    assert "states" in real

    falso = tmp_path / "metadata"
    falso.mkdir()
    (falso / "uf_codes.json").write_text(
        json.dumps({"states": {"XX": {"code": 99}}}), encoding="utf-8"
    )
    _data.load_json.cache_clear()
    monkeypatch.setattr(_data, "data_path", lambda rel: tmp_path / rel)
    assert list(_data.load_json("metadata/uf_codes.json")["states"]) == ["XX"]

    # Desfazer o monkeypatch NÃO basta; é preciso limpar.
    monkeypatch.undo()
    assert list(_data.load_json("metadata/uf_codes.json")["states"]) == ["XX"], (
        "sem cache_clear o conteúdo falso deveria persistir — se esta "
        "asserção falhar, load_json deixou de ser cacheada e o motivo do "
        "fixture mudou"
    )
    _data.load_json.cache_clear()
    assert "AC" in _data.load_json("metadata/uf_codes.json")["states"]
