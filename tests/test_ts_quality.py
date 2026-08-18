"""Tests for sus_data_ts_quality — time-series quality control for daily
municipal health counts."""

import pandas as pd
import pytest

from climasus4py.core.engine import get_connection
from climasus4py.utils.ts_quality import sus_data_ts_quality


def _muni_frame(muni: str, dates: pd.DatetimeIndex, values: list[float]) -> pd.DataFrame:
    return pd.DataFrame({"code_muni": muni, "date": dates, "n_obitos": values})


def _build_dataset() -> pd.DataFrame:
    # A: 30 complete daily days, no gaps, no outliers -> include=True
    dates_a = pd.date_range("2020-01-01", periods=30, freq="D")
    df_a = _muni_frame("A", dates_a, [1.0] * 30)

    # B: same 30 days, but a 10-day zero-run (index 10..19) -> n_gaps=1
    values_b = [1.0] * 30
    for i in range(10, 20):
        values_b[i] = 0.0
    df_b = _muni_frame("B", dates_a, values_b)

    # C: only 20 of 30 days present (10 dates missing entirely, not zeros)
    #    -> completeness = 20/30, but n_gaps stays 0 (missing rows are
    #    invisible to the run-length gap detector — preserved R quirk).
    present_idx = [i for i in range(30) if i not in range(5, 15)]
    dates_c = dates_a[present_idx]
    df_c = _muni_frame("C", dates_c, [1.0] * len(dates_c))

    # D: 4 months x 10 days, one month with a spike -> n_outlier_months=1
    dates_jan = pd.date_range("2021-01-01", periods=10, freq="D")
    dates_feb = pd.date_range("2021-02-01", periods=10, freq="D")
    dates_mar = pd.date_range("2021-03-01", periods=10, freq="D")
    dates_apr = pd.date_range("2021-04-01", periods=10, freq="D")
    dates_d = dates_jan.append([dates_feb, dates_mar, dates_apr])
    values_d = [1.0] * 10 + [1.0] * 10 + [100.0] * 10 + [1.0] * 10
    df_d = _muni_frame("D", dates_d, values_d)

    # E: 10 unique days but one date duplicated -> n_obs > n_expected
    #    (completeness > 1.0 — preserved R quirk: no dedup on the outcome col).
    dates_e = pd.date_range("2022-01-01", periods=10, freq="D")
    dates_e_dup = dates_e.append(pd.DatetimeIndex([dates_e[4]]))
    df_e = _muni_frame("E", dates_e_dup, [1.0] * 11)

    return pd.concat([df_a, df_b, df_c, df_d, df_e], ignore_index=True)


@pytest.fixture
def dataset() -> pd.DataFrame:
    return _build_dataset()


def _row(flags: pd.DataFrame, muni: str) -> pd.Series:
    return flags.loc[flags["code_muni"] == muni].iloc[0]


class TestTsQualityDataFrame:
    def test_warns_no_break_test(self, dataset):
        with pytest.warns(UserWarning, match="Structural break"):
            sus_data_ts_quality(dataset, verbose=False)

    def test_complete_series_included(self, dataset):
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, verbose=False)
        row = _row(qc["flags"], "A")
        assert row["n_obs"] == 30
        assert row["n_expected"] == 30
        assert row["completeness"] == 1.0
        assert row["n_gaps"] == 0
        assert row["has_break"] is None
        assert row["score"] == 100
        assert row["include"]
        assert "A" in qc["recommend_include"]

    def test_long_gap_flagged(self, dataset):
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, verbose=False)
        row = _row(qc["flags"], "B")
        assert row["n_gaps"] == 1
        assert row["score"] == 95
        assert not row["include"]
        assert "B" in qc["recommend_exclude"]
        assert "lacunas>7d=1" in qc["recommend_exclude"]["B"]

    def test_missing_rows_hurt_completeness_not_gaps(self, dataset):
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, verbose=False)
        row = _row(qc["flags"], "C")
        assert row["n_obs"] == 20
        assert row["n_expected"] == 30
        assert row["completeness"] == round(20 / 30, 4)
        # Missing rows are invisible to the rle-based gap detector.
        assert row["n_gaps"] == 0
        assert row["score"] == 100 - round((1 - 20 / 30) * 60)
        assert not row["include"]

    def test_monthly_outlier_detected(self, dataset):
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, verbose=False)
        row = _row(qc["flags"], "D")
        assert row["n_outlier_months"] == 1

    def test_duplicate_dates_inflate_completeness(self, dataset):
        # Preserved R quirk: no deduplication on (muni, date) — a repeated
        # date row simply adds to n_obs, which can push completeness > 1.0.
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, verbose=False)
        row = _row(qc["flags"], "E")
        assert row["n_obs"] == 11
        assert row["n_expected"] == 10
        assert row["completeness"] == 1.1

    def test_missing_column_raises(self, dataset):
        with pytest.raises(ValueError, match="Required column"):
            sus_data_ts_quality(dataset, outcome_col="does_not_exist", verbose=False)

    def test_params_echoed(self, dataset):
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(dataset, max_gap=3, verbose=False)
        assert qc["params"]["max_gap"] == 3
        assert qc["params"]["outcome_col"] == "n_obitos"


class TestTsQualityRelation:
    def test_accepts_duckdb_relation(self, dataset):
        conn = get_connection()
        rel = conn.from_df(dataset)
        with pytest.warns(UserWarning):
            qc = sus_data_ts_quality(rel, verbose=False)
        assert set(qc["flags"]["code_muni"]) == {"A", "B", "C", "D", "E"}


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
