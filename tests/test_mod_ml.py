"""Tests for climasus4py.enrichment.mod_ml.sus_mod_ml (mirrors R sus_mod_ml.R)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

xgb = pytest.importorskip("xgboost", reason="xgboost extra not installed ([ml]/[xgboost])")

from climasus4py.enrichment.mod_ml import sus_mod_ml, sus_mod_ml_predict  # noqa: E402


def _make_synthetic(n_cities: int = 8, n_per_city: int = 40, seed: int = 0) -> pd.DataFrame:
    """Synthetic city x time data where the outcome is a known function of
    tair_max_c (strong signal) plus a decoy feature (noise column, no signal).
    """
    rng = np.random.default_rng(seed)
    rows = []
    for city in range(n_cities):
        tair = rng.uniform(20, 38, n_per_city)
        decoy = rng.normal(0, 1, n_per_city)
        # Known relationship: higher temperature -> higher expected count.
        lam = np.clip(0.05 * (tair - 20) ** 2, 0.1, None)
        n_obitos = rng.poisson(lam)
        rows.append(
            pd.DataFrame(
                {
                    "name_muni": f"city_{city}",
                    "tair_max_c": tair,
                    "decoy_noise": decoy,
                    "n_obitos": n_obitos,
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def test_sus_mod_ml_recovers_known_signal():
    df = _make_synthetic()

    ml = sus_mod_ml(
        df=df,
        outcome_col="n_obitos",
        id_col="name_muni",
        objective="count:poisson",
        nrounds=100,
        nfold=4,
        early_stopping=20,
        seed=42,
        verbose=False,
    )

    # Structure parity with the R climasus_ml object.
    assert set(ml.keys()) == {"predictions", "importance", "performance", "model", "cv_log", "meta"}
    assert list(ml["importance"].columns) == ["Feature", "Gain", "Cover", "Frequency"]
    assert {"observed", "fitted", "cv_predicted", "residual"}.issubset(ml["predictions"].columns)
    assert "name_muni" in ml["predictions"].columns

    # The real driver (tair_max_c) must dominate over the decoy noise column.
    top_feature = ml["importance"].iloc[0]["Feature"]
    assert top_feature == "tair_max_c"

    # Importance columns are normalised proportions (parity with R xgb.importance).
    assert np.isclose(ml["importance"]["Gain"].sum(), 1.0, atol=1e-6)

    # Predictive performance should be reasonably good, not just "ran".
    assert ml["performance"]["Pearson_cv"] > 0.5
    assert ml["performance"]["R2_cv"] > 0.2
    assert ml["meta"]["feature_cols"] == ["tair_max_c", "decoy_noise"]
    assert ml["meta"]["n_removed_na"] == 0


def test_sus_mod_ml_predict_on_new_data():
    df = _make_synthetic()
    ml = sus_mod_ml(
        df=df,
        outcome_col="n_obitos",
        id_col="name_muni",
        nrounds=50,
        nfold=3,
        early_stopping=10,
        verbose=False,
    )

    newdata = pd.DataFrame({"tair_max_c": [22.0, 36.0], "decoy_noise": [0.0, 0.0]})
    preds = sus_mod_ml_predict(ml, newdata)

    assert preds.shape == (2,)
    # Hotter day should be predicted to have a higher expected count.
    assert preds[1] > preds[0]


def test_sus_mod_ml_missing_feature_column_raises():
    df = _make_synthetic()
    ml = sus_mod_ml(df=df, outcome_col="n_obitos", nrounds=20, nfold=3, verbose=False)

    with pytest.raises(ValueError, match="tair_max_c"):
        sus_mod_ml_predict(ml, pd.DataFrame({"decoy_noise": [0.0]}))


def test_sus_mod_ml_rejects_non_numeric_outcome():
    df = _make_synthetic()
    df["n_obitos"] = df["n_obitos"].astype(str)

    with pytest.raises(ValueError, match="n_obitos"):
        sus_mod_ml(df=df, outcome_col="n_obitos", verbose=False)


def test_sus_mod_ml_too_few_rows_raises():
    df = _make_synthetic(n_cities=1, n_per_city=5)

    with pytest.raises(ValueError, match="fold"):
        sus_mod_ml(df=df, outcome_col="n_obitos", nfold=5, verbose=False)
