"""Parity for three Python querying styles."""

import pandas as pd

from climasus4py.core._stage import set_stage
from climasus4py.core.engine import get_connection


def _sample_rel():
    rel = get_connection().from_df(
        pd.DataFrame(
            {
                "month": ["2023-01", "2023-01", "2023-02", "2023-02"],
                "uf": ["SP", "SP", "RJ", "SP"],
                "value": [1, 2, 3, 4],
            }
        )
    )
    return set_stage(rel, "import")


def test_relational_sql_and_pandas_paths_match():
    rel = _sample_rel()

    via_rel = (
        rel.filter("uf = 'SP'")
        .aggregate("month, count(*) AS n", "month")
        .order("month")
        .df()
    )

    via_sql = rel.query(
        "query_input",
        "SELECT month, COUNT(*) AS n FROM query_input "
        "WHERE uf = 'SP' GROUP BY month ORDER BY month",
    ).df()

    via_pandas = (
        rel.df()
        .query("uf == 'SP'")
        .groupby("month", as_index=False)
        .size()
        .rename(columns={"size": "n"})
        .sort_values("month")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(via_rel.reset_index(drop=True), via_sql)
    pd.testing.assert_frame_equal(via_rel.reset_index(drop=True), via_pandas)
