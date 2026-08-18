"""Tests for sus_mod_burden."""

from __future__ import annotations

import pandas as pd
import pytest

from climasus4py.enrichment.mod_burden import sus_mod_burden


def _af_table(n_cases: float, an: float, an_lo: float, an_hi: float, af_pct: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "component": ["total"],
            "n_cases": [n_cases],
            "an": [an],
            "an_lo": [an_lo],
            "an_hi": [an_hi],
            "af_pct": [af_pct],
        }
    )


def _af_table_all(
    n_cases: float,
    an_total: float,
    an_heat: float,
    an_cold: float,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "component": ["total", "heat", "cold"],
            "n_cases": [n_cases] * 3,
            "an": [an_total, an_heat, an_cold],
            "an_lo": [an_total * 0.8, an_heat * 0.8, an_cold * 0.8],
            "an_hi": [an_total * 1.2, an_heat * 1.2, an_cold * 1.2],
            "af_pct": [an_total / n_cases * 100, an_heat / n_cases * 100, an_cold / n_cases * 100],
        }
    )


def _excess_table(
    n_days, observed, expected, excess, excess_lo, excess_hi, excess_pct
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "n_days": [n_days],
            "observed": [observed],
            "expected": [expected],
            "excess": [excess],
            "excess_lo": [excess_lo],
            "excess_hi": [excess_hi],
            "excess_pct": [excess_pct],
        }
    )


def test_af_ranking_and_concentration_hand_computed():
    # City A: AN=50 out of 1000 cases; City B: AN=100 out of 2000 cases.
    # rank_by defaults to "an" for AF inputs -> B (100) ranks above A (50).
    # pct_of_total = an / sum(an) * 100 = an / 150 * 100.
    fits = {
        "A": _af_table(1000, 50.0, 40.0, 60.0, 5.0),
        "B": _af_table(2000, 100.0, 80.0, 120.0, 5.0),
    }

    result = sus_mod_burden(fits, lang="en", verbose=False)

    burden = result["burden_table"]
    conc = result["concentration"]
    total = result["total_burden"]
    meta = result["meta"]

    assert meta["input_type"] == "climasus_af"
    assert meta["rank_by"] == "an"

    # Ranking: B first (an=100), A second (an=50).
    assert conc["city"].tolist() == ["B", "A"]
    assert conc["rank"].tolist() == [1, 2]

    # Hand-computed percentages: 100/150*100 = 66.6667, 50/150*100 = 33.3333.
    pct_b, pct_a = conc["pct_of_total"].tolist()
    assert pct_b == pytest.approx(100 / 150 * 100)
    assert pct_a == pytest.approx(50 / 150 * 100)

    # Cumulative concentration: 66.6667, then 100.0.
    cum_b, cum_a = conc["cumulative_pct"].tolist()
    assert cum_b == pytest.approx(pct_b)
    assert cum_a == pytest.approx(100.0)

    # burden_table carries the same rank/pct_of_total, joined by city.
    row_b = burden[burden["city"] == "B"].iloc[0]
    assert row_b["rank"] == 1
    assert row_b["pct_of_total"] == pytest.approx(pct_b)

    # Aggregate totals: AN_total = round(50+100) = 150; mean AF% = 5.0.
    assert total["an_total"] == 150
    assert total["af_pct_avg"] == pytest.approx(5.0)
    assert total["top_city"] == "B"
    assert total["top_city_an"] == 100


def test_excess_ranking_default_rank_by():
    fits = {
        "sp": _excess_table(30, 300, 250.0, 50.0, 40.0, 60.0, 20.0),
        "rj": _excess_table(30, 200, 190.0, 10.0, 5.0, 15.0, 5.0),
    }

    result = sus_mod_burden(fits, lang="en", verbose=False)

    assert result["meta"]["input_type"] == "climasus_excess"
    assert result["meta"]["rank_by"] == "excess"
    conc = result["concentration"]
    assert conc["city"].tolist() == ["sp", "rj"]

    total = result["total_burden"]
    assert total["excess_total"] == round(50.0 + 10.0)
    assert total["excess_pct_avg"] == pytest.approx((20.0 + 5.0) / 2)
    assert total["top_city"] == "sp"
    assert total["top_city_excess"] == 50


def test_top_n_filters_to_leading_cities():
    fits = {
        "A": _af_table(1000, 10.0, 8.0, 12.0, 1.0),
        "B": _af_table(1000, 30.0, 24.0, 36.0, 3.0),
        "C": _af_table(1000, 20.0, 16.0, 24.0, 2.0),
    }
    result = sus_mod_burden(fits, top_n=2, lang="en", verbose=False)
    assert result["concentration"]["city"].tolist() == ["B", "C"]
    assert set(result["burden_table"]["city"]) == {"B", "C"}


def test_component_all_ranks_from_total_and_spreads_to_heat_cold():
    # A: total=50 (heat=30, cold=20), 1000 cases. B: total=100 (heat=70, cold=30), 1000 cases.
    fits = {
        "A": _af_table_all(1000, 50.0, 30.0, 20.0),
        "B": _af_table_all(1000, 100.0, 70.0, 30.0),
    }

    result = sus_mod_burden(fits, component="all", lang="en", verbose=False)
    burden = result["burden_table"]
    conc = result["concentration"]
    total = result["total_burden"]

    # 6 rows in burden_table (3 components x 2 cities), 2 rows in concentration.
    assert len(burden) == 6
    assert len(conc) == 2

    # Ranking is derived from "total" rows only: B (100) > A (50).
    assert conc["city"].tolist() == ["B", "A"]
    pct_b, pct_a = conc["pct_of_total"].tolist()
    assert pct_b == pytest.approx(100 / 150 * 100)
    assert pct_a == pytest.approx(50 / 150 * 100)
    assert conc["cumulative_pct"].iloc[-1] == pytest.approx(100.0)

    # Rank + pct_of_total are spread to all 3 rows of the same city.
    b_rows = burden[burden["city"] == "B"]
    assert (b_rows["rank"] == 1).all()
    assert b_rows["pct_of_total"].apply(lambda v: v == pytest.approx(pct_b)).all()

    # Row order within each city follows dplyr::arrange(rank, component):
    # rank first (B before A), then component alphabetically (cold, heat, total).
    assert burden["city"].tolist() == ["B", "B", "B", "A", "A", "A"]
    assert burden[burden["city"] == "B"]["component"].tolist() == ["cold", "heat", "total"]

    # total_burden sums only the "total" component rows (50 + 100 = 150),
    # not all 6 rows — this exercises the component-filter branch.
    assert total["an_total"] == 150
    assert total["top_city"] == "B"
    assert total["top_city_an"] == 100


def test_rank_by_af_pct_for_af_input():
    fits = {
        "A": _af_table(1000, 50.0, 40.0, 60.0, 8.0),  # higher af_pct, lower an
        "B": _af_table(2000, 100.0, 80.0, 120.0, 4.0),
    }
    result = sus_mod_burden(fits, rank_by="af_pct", lang="en", verbose=False)
    assert result["meta"]["rank_by"] == "af_pct"
    # A has af_pct=8.0 > B's 4.0, so A ranks first despite smaller an.
    assert result["concentration"]["city"].tolist() == ["A", "B"]


def test_rank_by_mismatch_falls_back_to_default():
    fits = {
        "A": _af_table(1000, 50.0, 40.0, 60.0, 5.0),
        "B": _af_table(2000, 100.0, 80.0, 120.0, 5.0),
    }
    # "excess" is not applicable to AF input -> falls back to "an".
    result = sus_mod_burden(fits, rank_by="excess", lang="en", verbose=False)
    assert result["meta"]["rank_by"] == "an"
    assert result["concentration"]["city"].tolist() == ["B", "A"]


def test_pct_of_total_signed_sum_quirk():
    # R quirk preserved: pct_of_total = val / sum(val) * 100 (signed sum),
    # not val / sum(abs(val)) * 100 -- a negative "an" (protective effect,
    # realistic for a cold-component AF) can yield a negative pct_of_total,
    # while the column as a whole still sums to exactly 100.
    fits = {
        "A": _af_table(1000, 100.0, 80.0, 120.0, 10.0),
        "B": _af_table(1000, -25.0, -30.0, -20.0, -2.5),
    }
    result = sus_mod_burden(fits, lang="en", verbose=False)
    conc = result["concentration"]
    pct_a = conc.loc[conc["city"] == "A", "pct_of_total"].iloc[0]
    pct_b = conc.loc[conc["city"] == "B", "pct_of_total"].iloc[0]
    assert pct_a == pytest.approx(100 / 75 * 100)
    assert pct_b == pytest.approx(-25 / 75 * 100)
    assert pct_a + pct_b == pytest.approx(100.0)


def test_mixed_types_raise_value_error():
    fits = {
        "A": _af_table(1000, 10.0, 8.0, 12.0, 1.0),
        "B": _excess_table(30, 200, 190.0, 10.0, 5.0, 15.0, 5.0),
    }
    with pytest.raises(ValueError):
        sus_mod_burden(fits, lang="en", verbose=False)


def test_dlnm_shaped_input_not_implemented():
    fits = {"A": object()}
    with pytest.raises(NotImplementedError):
        sus_mod_burden(fits, lang="en", verbose=False)


def test_empty_fits_raises():
    with pytest.raises(ValueError):
        sus_mod_burden({}, verbose=False)


def test_fits_not_dict_raises():
    with pytest.raises(TypeError):
        sus_mod_burden([1, 2, 3], verbose=False)  # type: ignore[arg-type]
