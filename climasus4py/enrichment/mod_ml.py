"""mod_ml.py — XGBoost machine learning for climate-health outcome prediction.

Mirrors R: sus_mod_ml.R

Not lazy — model training is fundamentally a materialised, in-memory
NumPy/XGBoost operation with no SQL expression. Accepts a
``DuckDBPyRelation`` or ``pd.DataFrame``; a relation is materialised with a
``UserWarning`` (same "legacy path" precedent as ``climate_spi.py`` /
``census.py``). Returns a plain ``dict`` (there is no S3-class equivalent in
Python) with the same keys as the R ``climasus_ml`` object: ``predictions``,
``importance``, ``performance``, ``model``, ``cv_log``, ``meta``.
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, Literal

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "climasus4py — Previsão de Desfechos em Saúde (XGBoost)",
        "materialize_warning": (
            "sus_mod_ml: a DuckDBPyRelation de entrada está sendo materializada "
            "— treino de modelo é uma operação em memória, não expressável em SQL lazy."
        ),
        "not_df": "'df' deve ser um DataFrame ou DuckDBPyRelation. Recebido: {cls}.",
        "no_outcome": "Coluna de desfecho '{col}' não encontrada em 'df'.",
        "outcome_not_numeric": "'{col}' deve ser numérica. Recebido: {dtype}.",
        "no_id": "Coluna de grupo '{col}' não encontrada em 'df'.",
        "bad_objective": "'objective' deve ser um de {opts}. Recebido: '{obj}'.",
        "no_features": (
            "Nenhuma coluna numérica de variável encontrada após excluir {excl}. "
            "Forneça 'feature_cols' explicitamente."
        ),
        "bad_feature_cols": "feature_cols não encontradas em 'df': {cols}.",
        "non_numeric_features": "Colunas de variável não numéricas: {cols}.",
        "too_few_obs": (
            "Apenas {n_obs} linha(s) após remoção de NA. São necessárias ao menos "
            "{needed} para validação cruzada com {nfold} folds."
        ),
        "warn_na": "{n_na} observação(ões) com NA removida(s) ({pct}% do total).",
        "warn_few_obs": "Apenas {n_obs} observações. Resultados de CV podem ser instáveis.",
        "warn_group_fold": (
            "Número de grupos únicos ({n_grp}) é menor que 'nfold' ({nfold}). Reduzindo nfold."
        ),
        "step_prepare": (
            "Preparando dados: {n_obs} obs, {n_feat} variável(is), objetivo = {objective}..."
        ),
        "step_cv": "Validação cruzada ({nfold} folds) para otimizar nrounds...",
        "step_train": "Treinando modelo final (nrounds = {best_nrounds}, eta = {eta})...",
        "step_importance": "Calculando importância das variáveis...",
        "done": (
            "Concluído. RMSE-CV = {rmse_cv} | MAE-CV = {mae_cv} | R2-CV = {r2_cv} | "
            "Top feature: {top_feat}"
        ),
        "predict_missing": (
            "Dados novos não contêm as colunas de variável usadas no treino: {cols}."
        ),
    },
    "en": {
        "title": "climasus4py — Health Outcome Prediction (XGBoost)",
        "materialize_warning": (
            "sus_mod_ml: the input DuckDBPyRelation is being materialised — "
            "model training is an in-memory operation, not expressible as lazy SQL."
        ),
        "not_df": "'df' must be a DataFrame or DuckDBPyRelation. Got: {cls}.",
        "no_outcome": "Outcome column '{col}' not found in 'df'.",
        "outcome_not_numeric": "'{col}' must be numeric. Got: {dtype}.",
        "no_id": "Group column '{col}' not found in 'df'.",
        "bad_objective": "'objective' must be one of {opts}. Got: '{obj}'.",
        "no_features": (
            "No numeric feature columns found after excluding {excl}. "
            "Provide 'feature_cols' explicitly."
        ),
        "bad_feature_cols": "feature_cols not found in 'df': {cols}.",
        "non_numeric_features": "Non-numeric feature columns: {cols}.",
        "too_few_obs": (
            "Only {n_obs} row(s) after NA removal. Need at least {needed} for "
            "{nfold}-fold cross-validation."
        ),
        "warn_na": "{n_na} observation(s) with NA removed ({pct}% of total).",
        "warn_few_obs": "Only {n_obs} observations. CV results may be unstable.",
        "warn_group_fold": (
            "Number of unique groups ({n_grp}) is less than 'nfold' ({nfold}). Reducing nfold."
        ),
        "step_prepare": (
            "Preparing data: {n_obs} obs, {n_feat} variable(s), objective = {objective}..."
        ),
        "step_cv": "Cross-validation ({nfold} folds) to select optimal nrounds...",
        "step_train": "Training final model (nrounds = {best_nrounds}, eta = {eta})...",
        "step_importance": "Computing feature importance...",
        "done": (
            "Done. RMSE-CV = {rmse_cv} | MAE-CV = {mae_cv} | R2-CV = {r2_cv} | "
            "Top feature: {top_feat}"
        ),
        "predict_missing": (
            "New data is missing feature columns used during training: {cols}."
        ),
    },
    "es": {
        "title": "climasus4py — Predicción de Resultados en Salud (XGBoost)",
        "materialize_warning": (
            "sus_mod_ml: la DuckDBPyRelation de entrada se está materializando — "
            "el entrenamiento del modelo es una operación en memoria, no expresable en SQL lazy."
        ),
        "not_df": "'df' debe ser un DataFrame o DuckDBPyRelation. Recibido: {cls}.",
        "no_outcome": "Columna de resultado '{col}' no encontrada en 'df'.",
        "outcome_not_numeric": "'{col}' debe ser numérica. Recibido: {dtype}.",
        "no_id": "Columna de grupo '{col}' no encontrada en 'df'.",
        "bad_objective": "'objective' debe ser uno de {opts}. Recibido: '{obj}'.",
        "no_features": (
            "No se encontraron columnas numéricas de variables tras excluir {excl}. "
            "Proporcione 'feature_cols' explícitamente."
        ),
        "bad_feature_cols": "feature_cols no encontradas en 'df': {cols}.",
        "non_numeric_features": "Columnas de variables no numéricas: {cols}.",
        "too_few_obs": (
            "Solo {n_obs} fila(s) tras eliminar NA. Se necesitan al menos {needed} "
            "para validación cruzada de {nfold} pliegues."
        ),
        "warn_na": "{n_na} observación(es) con NA eliminada(s) ({pct}% del total).",
        "warn_few_obs": "Solo {n_obs} observaciones. Resultados de CV pueden ser inestables.",
        "warn_group_fold": (
            "El número de grupos únicos ({n_grp}) es menor que 'nfold' ({nfold}). Reduciendo nfold."
        ),
        "step_prepare": (
            "Preparando datos: {n_obs} obs, {n_feat} variable(s), objetivo = {objective}..."
        ),
        "step_cv": "Validación cruzada ({nfold} pliegues) para optimizar nrounds...",
        "step_train": "Entrenando modelo final (nrounds = {best_nrounds}, eta = {eta})...",
        "step_importance": "Calculando importancia de variables...",
        "done": (
            "Listo. RMSE-CV = {rmse_cv} | MAE-CV = {mae_cv} | R2-CV = {r2_cv} | "
            "Principal variable: {top_feat}"
        ),
        "predict_missing": (
            "Los datos nuevos no contienen las columnas de variables usadas en el "
            "entrenamiento: {cols}."
        ),
    },
}

_OBJECTIVES = ("count:poisson", "reg:squarederror", "binary:logistic")
_EVAL_METRIC = {
    "count:poisson": "poisson-nloglik",
    "reg:squarederror": "rmse",
    "binary:logistic": "logloss",
}


def _rmse(obs: np.ndarray, pred: np.ndarray) -> float:
    return float(np.sqrt(np.nanmean((obs - pred) ** 2)))


def _mae(obs: np.ndarray, pred: np.ndarray) -> float:
    return float(np.nanmean(np.abs(obs - pred)))


def _r2(obs: np.ndarray, pred: np.ndarray) -> float:
    ss_res = float(np.nansum((obs - pred) ** 2))
    ss_tot = float(np.nansum((obs - np.nanmean(obs)) ** 2))
    if ss_tot < np.finfo(float).eps:
        return float("nan")
    return 1 - ss_res / ss_tot


def _make_folds(
    n: int,
    groups: np.ndarray | None,
    nfold: int,
    seed: int,
    msg: dict[str, str],
    verbose: bool,
) -> tuple[list[np.ndarray], int]:
    """Build fold assignments (held-out indices per fold).

    Mirrors R's ``.ml_make_folds``: group-aware k-fold when *groups* is
    given (each group kept whole in one fold), else standard random k-fold.
    Returns the list of held-out index arrays and the (possibly reduced)
    nfold.
    """
    rng = np.random.default_rng(seed)

    if groups is not None:
        uniq_grp = pd.unique(groups)
        n_grp = len(uniq_grp)
        if n_grp < nfold:
            if verbose:
                console.print(
                    "[yellow]WARN[/]  "
                    + msg["warn_group_fold"].format(n_grp=n_grp, nfold=nfold)
                )
            nfold = min(nfold, n_grp)
        base = np.resize(np.arange(nfold), n_grp)
        grp_fold = rng.permutation(base)
        grp_to_fold = dict(zip(uniq_grp, grp_fold, strict=True))
        fold_vec = np.array([grp_to_fold[g] for g in groups])
    else:
        base = np.resize(np.arange(nfold), n)
        fold_vec = rng.permutation(base)

    folds = [np.where(fold_vec == k)[0] for k in range(nfold)]
    return folds, nfold


def sus_mod_ml(
    df: duckdb.DuckDBPyRelation | pd.DataFrame,
    outcome_col: str,
    feature_cols: list[str] | None = None,
    id_col: str | None = None,
    objective: Literal["count:poisson", "reg:squarederror", "binary:logistic"] = "count:poisson",
    nrounds: int = 500,
    max_depth: int = 6,
    eta: float = 0.05,
    subsample: float = 0.8,
    colsample_bytree: float = 0.8,
    min_child_weight: float = 1,
    nfold: int = 5,
    early_stopping: int = 50,
    seed: int = 42,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Train an XGBoost gradient-boosted tree model for climate-health prediction.

    Trains an XGBoost model to predict health outcomes (disease counts,
    mortality, hospitalizations) from climate and socioeconomic features.
    Accepts any aggregated table produced by the climasus4py pipeline (or a
    plain ``pd.DataFrame``). Uses k-fold cross-validation to select the
    optimal number of trees (``nrounds``) and returns out-of-fold (OOF)
    predictions alongside the final model trained on the full data.

    This complements the DLNM epidemiological approach: DLNM models the
    exposure-response relationship for causal inference, while
    ``sus_mod_ml`` focuses on predictive accuracy across a wider feature
    space (lagged climate variables, socioeconomic covariates, spatial
    predictors) simultaneously.

    The ``objective`` controls the loss function and prediction scale:
    ``"count:poisson"`` (default) for non-negative integer counts (deaths,
    hospitalizations); ``"reg:squarederror"`` for continuous outcomes
    (rates, indices); ``"binary:logistic"`` for a binary outcome, with
    predictions as probabilities in (0, 1).

    Args:
        df: Table with one row per observation (e.g. city x day or city x
            month). Must contain *outcome_col* and at least one numeric
            feature column. A lazy ``DuckDBPyRelation`` is materialised
            (with a ``UserWarning``) since model training is not
            expressible as SQL.
        outcome_col: Name of the column to predict (the target variable).
            Must be numeric.
        feature_cols: Names of columns to use as predictors, or ``None``
            (default) to use all numeric columns except *outcome_col* and
            *id_col* automatically.
        id_col: City or group identifier column used for group-aware
            cross-validation: each group is kept entirely in one fold,
            preventing data leakage across cities. When ``None``, standard
            random k-fold is used. Strongly recommended when data has a
            city dimension.
        objective: XGBoost objective function. One of ``"count:poisson"``
            (default), ``"reg:squarederror"``, or ``"binary:logistic"``.
        nrounds: Maximum number of boosting rounds. The optimal value is
            selected via early stopping during cross-validation. Default
            ``500``.
        max_depth: Maximum tree depth. Default ``6``.
        eta: Learning rate (step size shrinkage). Default ``0.05``.
        subsample: Row subsampling ratio per tree, in (0, 1]. Default ``0.8``.
        colsample_bytree: Column subsampling ratio per tree, in (0, 1].
            Default ``0.8``.
        min_child_weight: Minimum sum of instance weight in a leaf.
            Default ``1``.
        nfold: Number of cross-validation folds. Default ``5``.
        early_stopping: Stop training if the CV metric does not improve
            for this many consecutive rounds. Default ``50``.
        seed: Random seed for reproducibility. Default ``42``.
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.
        verbose: Print progress messages. Default ``True``.

    Returns:
        A dict (the Python equivalent of R's ``climasus_ml`` S3 object)
        with keys:

        - ``predictions``: ``pd.DataFrame`` with ``observed``, ``fitted``
          (train-set prediction), ``cv_predicted`` (out-of-fold CV
          prediction), ``residual`` (observed - cv_predicted), and the
          *id_col* column if present.
        - ``importance``: ``pd.DataFrame`` sorted by ``Gain`` (descending):
          ``Feature``, ``Gain``, ``Cover``, ``Frequency``.
        - ``performance``: dict with ``RMSE_train``, ``MAE_train``,
          ``R2_train``, ``RMSE_cv``, ``MAE_cv``, ``R2_cv``, ``Pearson_cv``,
          ``best_nrounds``.
        - ``model``: the final ``xgboost.Booster`` trained on the full
          dataset using ``best_nrounds``. Use ``sus_mod_ml_predict`` to
          score new data (the Python equivalent of R's
          ``predict.climasus_ml``).
        - ``cv_log``: ``pd.DataFrame`` from ``xgb.cv`` with per-round train
          and test metrics.
        - ``meta``: dict of all parameters used in this call.

    Raises:
        ImportError: If ``xgboost`` is not installed.
        TypeError: If *df* is not a DataFrame or DuckDBPyRelation.
        ValueError: If *outcome_col*/*id_col*/*feature_cols* are invalid,
            *objective* is unsupported, or too few rows remain after NA
            removal for the requested *nfold*.

    Examples::

        import climasus4py as cs

        ml = cs.sus_mod_ml(
            df=df_aggregated,
            outcome_col="n_obitos",
            id_col="name_muni",       # group-aware CV by city
            objective="count:poisson",
            nfold=5,
            lang="pt",
        )
        ml["predictions"].head()
        ml["importance"].head()
    """
    try:
        import xgboost as xgb
    except ImportError as exc:
        raise ImportError(
            "The 'xgboost' package is required for sus_mod_ml(). "
            "Install it with: pip install 'climasus4py[ml]'"
        ) from exc

    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if verbose:
        console.rule(f"[bold]{msg['title']}[/]")

    if objective not in _OBJECTIVES:
        raise ValueError(msg["bad_objective"].format(opts=_OBJECTIVES, obj=objective))
    eval_metric = _EVAL_METRIC[objective]

    if isinstance(df, duckdb.DuckDBPyRelation):
        warnings.warn(msg["materialize_warning"], UserWarning, stacklevel=2)
        data = df.df()
    elif isinstance(df, pd.DataFrame):
        data = df.copy()
    else:
        raise TypeError(msg["not_df"].format(cls=type(df).__name__))

    if outcome_col not in data.columns:
        raise ValueError(msg["no_outcome"].format(col=outcome_col))
    if not pd.api.types.is_numeric_dtype(data[outcome_col]):
        raise ValueError(
            msg["outcome_not_numeric"].format(col=outcome_col, dtype=data[outcome_col].dtype)
        )
    if id_col is not None and id_col not in data.columns:
        raise ValueError(msg["no_id"].format(col=id_col))

    exclude_always = {outcome_col} | ({id_col} if id_col is not None else set())

    if feature_cols is None:
        resolved_features = [
            c
            for c in data.columns
            if c not in exclude_always and pd.api.types.is_numeric_dtype(data[c])
        ]
        if not resolved_features:
            raise ValueError(msg["no_features"].format(excl=sorted(exclude_always)))
    else:
        bad_fc = [c for c in feature_cols if c not in data.columns]
        if bad_fc:
            raise ValueError(msg["bad_feature_cols"].format(cols=bad_fc))
        not_num = [c for c in feature_cols if not pd.api.types.is_numeric_dtype(data[c])]
        if not_num:
            raise ValueError(msg["non_numeric_features"].format(cols=not_num))
        resolved_features = list(feature_cols)

    all_cols = [outcome_col, *resolved_features]
    na_mask = data[all_cols].isna().any(axis=1)
    n_na = int(na_mask.sum())
    n_total = len(data)

    if n_na > 0:
        pct = round(n_na / n_total * 100, 1)
        if verbose:
            console.print("[yellow]WARN[/]  " + msg["warn_na"].format(n_na=n_na, pct=pct))
        data = data.loc[~na_mask].reset_index(drop=True)

    n_obs = len(data)
    n_feat = len(resolved_features)

    if n_obs < 2 * nfold:
        raise ValueError(
            msg["too_few_obs"].format(n_obs=n_obs, needed=2 * nfold, nfold=nfold)
        )
    if n_obs < 100 and verbose:
        console.print("[yellow]WARN[/]  " + msg["warn_few_obs"].format(n_obs=n_obs))

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_prepare"].format(n_obs=n_obs, n_feat=n_feat, objective=objective)
        )

    X = data[resolved_features].to_numpy(dtype=float)
    y = data[outcome_col].to_numpy(dtype=float)
    dtrain = xgb.DMatrix(X, label=y, feature_names=resolved_features)

    xgb_params = {
        "objective": objective,
        "eval_metric": eval_metric,
        "max_depth": int(max_depth),
        "eta": eta,
        "subsample": subsample,
        "colsample_bytree": colsample_bytree,
        "min_child_weight": min_child_weight,
        "seed": int(seed),
    }

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_cv"].format(nfold=nfold))

    groups = data[id_col].astype(str).to_numpy() if id_col is not None else None
    fold_indices, nfold = _make_folds(n_obs, groups, nfold, seed, msg, verbose)

    cv_result = xgb.cv(
        params=xgb_params,
        dtrain=dtrain,
        num_boost_round=int(nrounds),
        folds=[
            (np.setdiff1d(np.arange(n_obs), test_idx), test_idx) for test_idx in fold_indices
        ],
        early_stopping_rounds=int(early_stopping),
        verbose_eval=False,
        seed=int(seed),
    )
    test_col = next(c for c in cv_result.columns if c.startswith("test-") and c.endswith("-mean"))
    best_nrounds = int(cv_result[test_col].idxmin()) + 1

    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_train"].format(best_nrounds=best_nrounds, eta=eta)
        )

    # Out-of-fold (OOF) predictions via manual k-fold with best_nrounds.
    oof_pred = np.full(n_obs, np.nan)
    for test_idx in fold_indices:
        train_idx = np.setdiff1d(np.arange(n_obs), test_idx)
        d_tr = xgb.DMatrix(X[train_idx], label=y[train_idx], feature_names=resolved_features)
        d_te = xgb.DMatrix(X[test_idx], feature_names=resolved_features)
        m_k = xgb.train(xgb_params, d_tr, num_boost_round=best_nrounds, verbose_eval=False)
        oof_pred[test_idx] = m_k.predict(d_te)

    # Train final model on full data.
    final_model = xgb.train(xgb_params, dtrain, num_boost_round=best_nrounds, verbose_eval=False)
    fitted_vals = final_model.predict(dtrain)

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_importance"])

    # R's xgb.importance() normalises Gain/Cover/Frequency to proportions
    # that each sum to 1 across features. XGBoost's Booster.get_score()
    # returns per-feature *averages* (gain/cover) or raw split counts
    # (weight); use the totals and normalise the same way R does.
    total_gain = final_model.get_score(importance_type="total_gain")
    total_cover = final_model.get_score(importance_type="total_cover")
    weight = final_model.get_score(importance_type="weight")
    gain_sum = sum(total_gain.values()) or 1.0
    cover_sum = sum(total_cover.values()) or 1.0
    weight_sum = sum(weight.values()) or 1.0
    imp_tbl = (
        pd.DataFrame(
            {
                "Feature": list(total_gain.keys()),
                "Gain": [v / gain_sum for v in total_gain.values()],
                "Cover": [total_cover.get(f, 0.0) / cover_sum for f in total_gain],
                "Frequency": [weight.get(f, 0.0) / weight_sum for f in total_gain],
            }
        )
        .sort_values("Gain", ascending=False)
        .reset_index(drop=True)
    )

    perf = {
        "RMSE_train": _rmse(y, fitted_vals),
        "MAE_train": _mae(y, fitted_vals),
        "R2_train": _r2(y, fitted_vals),
        "RMSE_cv": _rmse(y, oof_pred),
        "MAE_cv": _mae(y, oof_pred),
        "R2_cv": _r2(y, oof_pred),
        "Pearson_cv": float(np.corrcoef(y, oof_pred)[0, 1]),
        "best_nrounds": best_nrounds,
    }

    pred_tbl = pd.DataFrame(
        {
            "observed": y,
            "fitted": np.round(fitted_vals, 4),
            "cv_predicted": np.round(oof_pred, 4),
            "residual": np.round(y - oof_pred, 4),
        }
    )
    if id_col is not None:
        pred_tbl.insert(0, id_col, data[id_col].to_numpy())

    top_feat = imp_tbl["Feature"].iloc[0] if len(imp_tbl) else "N/A"
    if verbose:
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(
                rmse_cv=round(perf["RMSE_cv"], 3),
                mae_cv=round(perf["MAE_cv"], 3),
                r2_cv=round(perf["R2_cv"], 3),
                top_feat=top_feat,
            )
        )

    return {
        "predictions": pred_tbl,
        "importance": imp_tbl,
        "performance": perf,
        "model": final_model,
        "cv_log": cv_result,
        "meta": {
            "outcome_col": outcome_col,
            "feature_cols": resolved_features,
            "id_col": id_col,
            "objective": objective,
            "eval_metric": eval_metric,
            "nrounds_max": int(nrounds),
            "best_nrounds": best_nrounds,
            "nfold": int(nfold),
            "max_depth": int(max_depth),
            "eta": eta,
            "subsample": subsample,
            "colsample_bytree": colsample_bytree,
            "min_child_weight": min_child_weight,
            "early_stopping": int(early_stopping),
            "n_obs_total": n_total,
            "n_obs_used": n_obs,
            "n_removed_na": n_na,
            "seed": int(seed),
            "call_time": datetime.now(),
        },
    }


def sus_mod_ml_predict(
    ml: dict[str, Any],
    newdata: pd.DataFrame,
    lang: Literal["pt", "en", "es"] = "pt",
) -> np.ndarray:
    """Generate predictions from a ``sus_mod_ml`` result on new data.

    Python equivalent of R's ``predict.climasus_ml()`` S3 method (there is
    no S3 dispatch in Python, so this is a plain companion function taking
    the dict returned by :func:`sus_mod_ml`).

    Args:
        ml: The dict returned by :func:`sus_mod_ml`.
        newdata: A DataFrame with the same feature columns used during
            training (see ``ml["meta"]["feature_cols"]``).
        lang: Message language: ``"pt"`` (default), ``"en"``, ``"es"``.

    Returns:
        A numpy array of predictions in the natural scale of the
        objective (counts for Poisson, values for squared error,
        probabilities for binary logistic).

    Raises:
        ImportError: If ``xgboost`` is not installed.
        ValueError: If *newdata* is missing feature columns used during
            training.
    """
    try:
        import xgboost as xgb
    except ImportError as exc:
        raise ImportError(
            "The 'xgboost' package is required for sus_mod_ml_predict(). "
            "Install it with: pip install 'climasus4py[ml]'"
        ) from exc

    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    features = ml["meta"]["feature_cols"]
    missing = [c for c in features if c not in newdata.columns]
    if missing:
        raise ValueError(msg["predict_missing"].format(cols=missing))

    X_new = newdata[features].to_numpy(dtype=float)
    d_new = xgb.DMatrix(X_new, feature_names=features)
    return ml["model"].predict(d_new)
