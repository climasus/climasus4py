"""M131: `sus_data_export` — o nome do R passou a funcionar também.

Quem chega do `climasus4r` procura `sus_data_export` e encontrava
`AttributeError`. O nome canônico aqui continua sendo `sus_export`;
o do R virou **apelido**, não renomeação.

**Por que apelido e não renomeação.** São quatro as divergências de nome
entre os pacotes, e duas delas não têm solução:

| R | Python | dá para alinhar? |
|---|---|---|
| `sus_data_export` | `sus_export` | sim — este apelido |
| `sus_census_join` | `sus_census` | sim, ainda não feito |
| `sus_data_filter_cid` + `sus_data_filter_demographics` | `sus_filter` | **não** — duas viram uma |
| `predict.climasus_ml` | `sus_mod_ml_predict` | **não** — S3 não existe em Python |

Renomear alinharia 2 de 4 e deixaria uma convenção meio-a-meio, pior que
uma convenção própria documentada. E renomear de verdade quebraria quem
já usa `sus_export` — ele está no `__all__`, na documentação e pode estar
em receitas RAP, que resolvem o nome da função **por string** no namespace
público.
"""

from __future__ import annotations

import climasus4py as cs


def test_o_nome_do_r_existe() -> None:
    """A asserção que o M131 é."""
    assert hasattr(cs, "sus_data_export")


def test_e_exatamente_a_mesma_funcao() -> None:
    """Apelido, não cópia: um wrapper duplicaria o comportamento a manter."""
    assert cs.sus_data_export is cs.sus_export


def test_o_nome_canonico_continua_valendo() -> None:
    """A contrapartida que impede isto de virar renomeação por descuido."""
    assert hasattr(cs, "sus_export")
    assert "sus_export" in cs.__all__


def test_os_dois_estao_no_all() -> None:
    """O `sus_rap_run` resolve nomes de função pelo `__all__`.

    Um apelido fora dele seria importável e, ainda assim, inutilizável
    por receita — que é o caso de uso de quem vem do R.
    """
    assert "sus_data_export" in cs.__all__
    assert "sus_export" in cs.__all__


def test_o_all_nao_ganhou_duplicata() -> None:
    assert len(cs.__all__) == len(set(cs.__all__))


def test_a_assinatura_e_a_mesma() -> None:
    """Óbvio por ser o mesmo objeto, e barato de fixar.

    Se alguém transformar o apelido num wrapper, isto pega a divergência
    de assinatura antes que ela chegue a quem chama.
    """
    import inspect

    assert inspect.signature(cs.sus_data_export) == \
        inspect.signature(cs.sus_export)
