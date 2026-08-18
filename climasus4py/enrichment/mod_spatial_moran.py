"""Global Moran's I and LISA local spatial autocorrelation for health outcomes.

Mirrors R: sus_mod_spatial_moran.R

Not lazy — operates on an in-memory ``pandas.DataFrame`` plus the ``dict``
returned by ``sus_mod_spatial_weights()`` (the Python analog of R's
``climasus_weights`` S3 object). There is no natural DuckDB-lazy
representation of permutation-based spatial statistics, so this module
(like R's) sits outside the ``import -> clean -> standardize -> filter ->
variables -> aggregate`` pipeline: it is a standalone spatial-modelling
helper consumed downstream by ``sus_mod_plot_spatial_moran``.

Theory:
  Moran (1950, Biometrika) - original spatial autocorrelation statistic
  Anselin (1995, Geogr Anal 27:93-115) - Local Indicators of Spatial
    Association (LISA)
  Bivand et al. (2013) - Applied Spatial Data Analysis with R (spdep)
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console

console = Console(stderr=True)

VALID_ADJUST_P: tuple[str, ...] = ("fdr", "bonferroni", "none")
_QUAD_LEVELS: tuple[str, ...] = ("HH", "LL", "HL", "LH", "NS")

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "step_check": "Verificando entradas...",
        "step_align": "Alinhando {n} linha(s) de 'df' a ordem de 'W'...",
        "step_global": "Calculando I de Moran global ({permutations} permutacoes)...",
        "step_local": "Calculando LISA local ({permutations} permutacoes)...",
        "step_quadrant": "Classificando quadrantes LISA (alfa = {alpha}, ajuste p = {adjust_p})...",
        "done": (
            "Concluido. I = {i_val} (p-sim = {p_val}) | "
            "HH = {n_hh}, LL = {n_ll}, HL = {n_hl}, LH = {n_lh}, NS = {n_ns}"
        ),
        "err_not_weights": (
            "'W' deve ser um dict produzido por sus_mod_spatial_weights()."
        ),
        "err_no_listw": (
            "'W' nao contem a chave 'listw'. Reconstrua com sus_mod_spatial_weights()."
        ),
        "err_no_muni": "Coluna 'code_muni' nao encontrada em 'df'.",
        "err_no_outcome": (
            "Coluna de desfecho '{outcome}' nao encontrada em 'df'. "
            "Colunas disponiveis: {avail}."
        ),
        "err_length_mismatch": (
            "Comprimento do vetor de desfecho ({n_x}) difere do numero de "
            "regioes na matriz de pesos ({n_w}). Filtre 'df' para corresponder "
            "ao objeto 'W'."
        ),
        "err_id_mismatch": (
            "Os codigos 'code_muni' de 'df' (apos qualquer filtro por "
            "'municipalities') nao correspondem exatamente aos ids de "
            "W['listw'].id_order. Diferenca: {n_diff} codigo(s) nao "
            "encontrado(s) em um dos dois lados. Ambos precisam conter "
            "exatamente o mesmo conjunto de municipios."
        ),
        "err_duplicate_ids": (
            "'df' contem codigos 'code_muni' duplicados apos o filtro; nao e "
            "possivel alinhar de forma inequivoca a ordem de W['listw']."
        ),
        "err_invalid_adjust_p": (
            "'adjust_p' invalido: '{adjust_p}'. Use um de: {valid}."
        ),
        "err_all_na": (
            "Coluna de desfecho '{outcome}' contem apenas NA. "
            "Nao e possivel calcular Moran."
        ),
        "warn_na_values": (
            "{n_na} valor(es) NA em '{outcome}' substituidos pela media antes "
            "do calculo."
        ),
        "warn_municipalities": (
            "Filtrando 'df' para {n_keep} municipios fornecidos em 'municipalities'."
        ),
    },
    "en": {
        "step_check": "Checking inputs...",
        "step_align": "Aligning {n} row(s) of 'df' to the order of 'W'...",
        "step_global": "Computing global Moran's I ({permutations} permutations)...",
        "step_local": "Computing local LISA ({permutations} permutations)...",
        "step_quadrant": "Classifying LISA quadrants (alpha = {alpha}, p-adjust = {adjust_p})...",
        "done": (
            "Done. I = {i_val} (p-sim = {p_val}) | "
            "HH = {n_hh}, LL = {n_ll}, HL = {n_hl}, LH = {n_lh}, NS = {n_ns}"
        ),
        "err_not_weights": "'W' must be a dict produced by sus_mod_spatial_weights().",
        "err_no_listw": (
            "'W' does not contain key 'listw'. Rebuild with sus_mod_spatial_weights()."
        ),
        "err_no_muni": "Column 'code_muni' not found in 'df'.",
        "err_no_outcome": (
            "Outcome column '{outcome}' not found in 'df'. "
            "Available columns: {avail}."
        ),
        "err_length_mismatch": (
            "Outcome vector length ({n_x}) differs from number of regions in "
            "weight matrix ({n_w}). Filter 'df' to match 'W'."
        ),
        "err_id_mismatch": (
            "'df's 'code_muni' codes (after any 'municipalities' filter) do "
            "not correspond exactly to the ids in W['listw'].id_order. "
            "Mismatch: {n_diff} code(s) not found on one of the two sides. "
            "Both must contain exactly the same set of municipalities."
        ),
        "err_duplicate_ids": (
            "'df' contains duplicate 'code_muni' codes after filtering; "
            "cannot unambiguously align to the order of W['listw']."
        ),
        "err_invalid_adjust_p": "Invalid 'adjust_p': '{adjust_p}'. Use one of: {valid}.",
        "err_all_na": (
            "Outcome column '{outcome}' contains only NA. Cannot compute Moran."
        ),
        "warn_na_values": (
            "{n_na} NA value(s) in '{outcome}' replaced by mean before computation."
        ),
        "warn_municipalities": (
            "Filtering 'df' to {n_keep} municipalities provided in 'municipalities'."
        ),
    },
    "es": {
        "step_check": "Verificando entradas...",
        "step_align": "Alineando {n} fila(s) de 'df' al orden de 'W'...",
        "step_global": "Calculando I de Moran global ({permutations} permutaciones)...",
        "step_local": "Calculando LISA local ({permutations} permutaciones)...",
        "step_quadrant": "Clasificando cuadrantes LISA (alfa = {alpha}, ajuste p = {adjust_p})...",
        "done": (
            "Listo. I = {i_val} (p-sim = {p_val}) | "
            "HH = {n_hh}, LL = {n_ll}, HL = {n_hl}, LH = {n_lh}, NS = {n_ns}"
        ),
        "err_not_weights": "'W' debe ser un dict producido por sus_mod_spatial_weights().",
        "err_no_listw": (
            "'W' no contiene la clave 'listw'. Reconstruya con sus_mod_spatial_weights()."
        ),
        "err_no_muni": "Columna 'code_muni' no encontrada en 'df'.",
        "err_no_outcome": (
            "Columna de resultado '{outcome}' no encontrada en 'df'. "
            "Columnas disponibles: {avail}."
        ),
        "err_length_mismatch": (
            "La longitud del vector de resultado ({n_x}) difiere del numero de "
            "regiones en la matriz de pesos ({n_w}). Filtre 'df' para "
            "coincidir con 'W'."
        ),
        "err_id_mismatch": (
            "Los codigos 'code_muni' de 'df' (tras cualquier filtro por "
            "'municipalities') no corresponden exactamente a los ids de "
            "W['listw'].id_order. Diferencia: {n_diff} codigo(s) no "
            "encontrado(s) en uno de los dos lados. Ambos deben contener "
            "exactamente el mismo conjunto de municipios."
        ),
        "err_duplicate_ids": (
            "'df' contiene codigos 'code_muni' duplicados tras el filtro; no "
            "es posible alinear de forma inequivoca al orden de W['listw']."
        ),
        "err_invalid_adjust_p": "'adjust_p' invalido: '{adjust_p}'. Use uno de: {valid}.",
        "err_all_na": (
            "Columna de resultado '{outcome}' contiene solo NA. "
            "No es posible calcular Moran."
        ),
        "warn_na_values": (
            "{n_na} valor(es) NA en '{outcome}' reemplazados por la media "
            "antes del calculo."
        ),
        "warn_municipalities": (
            "Filtrando 'df' a {n_keep} municipios provistos en 'municipalities'."
        ),
    },
}


def sus_mod_spatial_moran(
    df: pd.DataFrame,
    outcome: str,
    W: dict[str, Any],
    municipalities: list[str] | None = None,
    permutations: int = 999,
    alpha: float = 0.05,
    adjust_p: Literal["fdr", "bonferroni", "none"] = "fdr",
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Global Moran's I and LISA local autocorrelation for a health outcome.

    Computes Moran's I global spatial autocorrelation statistic and Local
    Indicators of Spatial Association (LISA) for a numeric health outcome
    measured at municipality level. The global statistic tests whether the
    outcome is spatially clustered across the study region; the local
    statistics identify significant **High-High** (hotspots), **Low-Low**
    (coldspots), **High-Low**, and **Low-High** spatial outlier clusters.

    Global statistic:
        Moran's I is computed via ``esda.Moran`` with conditional
        permutation inference (the Python analog of R's
        ``spdep::moran.mc()``). ``esda.Moran`` also yields the analytical
        moments needed for ``E.I``/``Var.I``/``Z.I``/``p_value`` in one
        call — the Python port therefore needs only one ``esda.Moran``
        call, unlike R's two-call combination of ``moran.mc()`` +
        ``moran.test()``. The variance/Z/p reported use the
        **randomisation** assumption (``VI_rand``/``z_rand``), matching
        R's ``moran.test(randomisation=TRUE)`` default; the one-sided
        "greater" p-value convention of R's default ``alternative`` is
        replicated by taking ``scipy.stats.norm.sf`` of the Z-score
        directly (not ``esda``'s own two-tailed ``p_rand``/``p_norm``
        fields, which use a different default alternative). ``p_simulated``
        is likewise computed by hand from ``esda.Moran``'s permutation
        draws using R's ``moran.mc()`` one-sided "greater" rank formula
        (``(#sims >= I + 1) / (nsim + 1)``), not ``esda``'s own (two-sided)
        ``p_sim`` attribute.

    Local statistic (LISA):
        Local Moran statistics are computed via ``esda.Moran_Local``
        (conditional permutation), the Python analog of R's
        ``spdep::localmoran_perm()``. R's own code, for its ``p_raw``
        column, greps for the first column matching ``"^Pr\\\\(z"`` in
        ``localmoran_perm()``'s output — which is **not** the rank-based
        folded-permutation p-value, but the two-sided *analytical* p-value
        computed from the permutation-derived Z-score
        (``2 * pnorm(abs(Z.Ii), lower.tail=FALSE)``). This port replicates
        that exact quantity as ``2 * scipy.stats.norm.sf(abs(z_sim))``
        using ``esda.Moran_Local``'s ``z_sim`` (verified numerically
        against R's ``localmoran_perm()`` output). Each observation is then
        assigned a LISA quadrant based on the sign of its standardised
        value and the sign of the standardised spatial lag:

        - ``HH``: high value surrounded by high neighbours (hotspot).
        - ``LL``: low value surrounded by low neighbours (coldspot).
        - ``HL``: high value surrounded by low neighbours (spatial outlier).
        - ``LH``: low value surrounded by high neighbours (spatial outlier).
        - ``NS``: not significant at the specified ``alpha`` level.

        **Preserved R quirk (hardcoded permutation seed):** R's source
        hardcodes ``iseed = 1L`` in its ``localmoran_perm()`` call — every
        run uses the same permutation seed for the *local* statistic
        regardless of any call-level randomness, unlike the *global*
        statistic (``moran.mc()``), which is never seeded and therefore
        varies run to run. This port replicates that exact asymmetry:
        ``esda.Moran_Local`` is always called with ``seed=1``; the global
        ``esda.Moran`` call is never seeded.

        **Island handling:** R's ``spdep::lag.listw(..., zero.policy=TRUE)``
        documents (and was verified here to) return ``0`` — not ``NA`` —
        for the spatial lag of a region with zero neighbours ("island").
        ``libpysal.weights.lag_spatial`` behaves identically (returns
        ``0`` for islands), so **no special-casing is needed**: this is
        not the divergence it might appear to be. Separately,
        ``esda.Moran_Local``'s permutation-based ``z_sim``/``p_z_sim`` are
        already ``NaN`` for island rows (verified against R, whose
        ``localmoran_perm()`` also returns ``NA`` there) since no
        permutation is possible with zero neighbours. One divergence *is*
        preserved deliberately, not fixed: R's quadrant-assignment step
        uses vectorised logical indexing with ``NA``-propagating
        three-valued logic, which (depending on the accidental sign of
        the island's standardised value) can leave an island's quadrant
        as ``NA`` instead of the initial ``"NS"`` default. Python/NumPy's
        two-valued comparison semantics (``NaN < alpha`` evaluates to
        ``False``, not ``NaN``) make this island row deterministically
        ``"NS"`` instead. This is treated as a documented simplification
        rather than a bug worth reproducing — see ``IDEIAS.md``.

    Row alignment (departure from R): R trusts that ``df``'s row order
    already matches the internal order encoded in ``W`` and only checks
    vector *lengths* match — a silent-correctness trap if the two were
    built independently and happen to differ in order. This Python port
    instead explicitly reindexes ``df`` (after any ``municipalities``
    filter) to the order of ``W["listw"].id_order`` by matching
    ``code_muni`` values, and raises ``ValueError`` if the two id sets do
    not match exactly or if ``code_muni`` has duplicates. This is a
    deliberate correctness fix (CLAUDE.md §3 rule 4 exception for
    silent-incorrect-result bugs), not a signature or behavioural change
    for correctly-ordered input.

    Args:
        df: A ``pandas.DataFrame`` containing at least the columns
            ``code_muni`` (municipality code, matched as ``str``) and the
            outcome column named by *outcome*. Rows are realigned
            internally to match ``W["listw"].id_order`` — see **Row
            alignment** above.
        outcome: Name of the numeric outcome column in *df* (e.g.
            ``"deaths"``, ``"rate_100k"``, ``"hospitalization_rate"``).
        W: The ``dict`` returned by ``sus_mod_spatial_weights()``. Must
            contain key ``"listw"`` (a ``libpysal.weights.W`` object)
            whose ``id_order`` matches ``df["code_muni"]`` after any
            *municipalities* filtering (same municipality set, any order).
        municipalities: Optional list of ``code_muni`` values to include.
            When supplied, *df* is subset to these values before the
            internal realignment to ``W``'s order. Default ``None`` (use
            all rows).
        permutations: Positive integer. Number of Monte Carlo permutations
            for both global and local inference. Default ``999``.
        alpha: Numeric in (0, 1). Significance threshold applied to
            p-adjusted local Moran p-values when assigning quadrant
            labels. Default ``0.05``.
        adjust_p: Multiple-testing correction method for local Moran
            p-values, replicating R's ``stats::p.adjust()`` exactly
            (including its "adjust using the count of *non-missing*
            p-values" default behaviour for ``"fdr"``/``"bonferroni"``,
            verified numerically against R). One of ``"fdr"`` (default,
            Benjamini-Hochberg), ``"bonferroni"``, or ``"none"``.
        lang: Output language: ``"pt"`` (default), ``"en"``, or ``"es"``.
        verbose: If ``True`` (default), print progress messages.

    Returns:
        A ``dict`` (the Python analog of R's ``climasus_spatial_moran``
        S3-classed list) with keys:

        - ``"global"``: one-row ``pandas.DataFrame`` with columns ``"I"``,
          ``"E.I"``, ``"Var.I"``, ``"Z.I"``, ``"p_value"`` (analytical,
          one-sided "greater"), ``"p_simulated"`` (permutation-based,
          one-sided "greater").
        - ``"local"``: ``pandas.DataFrame`` with one row per spatial unit
          (in ``W["listw"].id_order``) and columns ``"code_muni"``,
          ``"Ii"`` (local Moran statistic), ``"Z.Ii"`` (permutation
          z-score), ``"p_raw"`` (unit-level analytical-from-permutation
          p), ``"p_adj"`` (adjusted p), ``"quadrant"`` (categorical:
          ``"HH"``, ``"LL"``, ``"HL"``, ``"LH"``, ``"NS"``).
        - ``"n_HH"``, ``"n_LL"``, ``"n_HL"``, ``"n_LH"``: ``int``. Counts
          of significant units per cluster type.
        - ``"outcome_name"``: ``str``. The *outcome* argument value.

        R's ``$call`` slot (the matched call) has no meaningful Python
        analog and is not reproduced, mirroring the same convention
        already used by ``sus_mod_spatial_weights``.

    Raises:
        TypeError: If *W* is not a ``dict``.
        ValueError: If required columns are missing, *df* contains only
            NA in *outcome*, *df*'s municipality set does not match
            *W*'s, *df* has duplicate ``code_muni`` values, or
            *adjust_p* is invalid.
        ImportError: If ``esda`` or ``libpysal`` is not installed.

    Examples::

        import climasus4py as cs

        # W = cs.sus_mod_spatial_weights(municipios_gdf)
        result = cs.sus_mod_spatial_moran(
            df=my_df, outcome="deaths", W=W, permutations=999,
        )
        result["global"]
        result["local"][result["local"]["quadrant"] == "HH"]
    """
    try:
        import libpysal  # noqa: F401
        from libpysal.weights import lag_spatial
    except ImportError as exc:
        raise ImportError(
            "libpysal is required for sus_mod_spatial_moran(). "
            "Install it with: pip install libpysal"
        ) from exc
    try:
        from esda import Moran, Moran_Local
    except ImportError as exc:
        raise ImportError(
            "esda is required for sus_mod_spatial_moran(). "
            "Install it with: pip install esda"
        ) from exc
    try:
        from scipy import stats as scipy_stats
    except ImportError as exc:
        raise ImportError(
            "scipy is required for sus_mod_spatial_moran() (analytical "
            "normal p-values). Install it with: pip install scipy"
        ) from exc

    if lang not in ("pt", "en", "es"):
        lang = "pt"
    msg = _MESSAGES[lang]

    if adjust_p not in VALID_ADJUST_P:
        raise ValueError(
            msg["err_invalid_adjust_p"].format(
                adjust_p=adjust_p, valid=", ".join(VALID_ADJUST_P)
            )
        )

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_check"])

    if not isinstance(W, dict):
        raise TypeError(msg["err_not_weights"])
    if "listw" not in W:
        raise ValueError(msg["err_no_listw"])

    if "code_muni" not in df.columns:
        raise ValueError(msg["err_no_muni"])
    if outcome not in df.columns:
        raise ValueError(
            msg["err_no_outcome"].format(outcome=outcome, avail=", ".join(df.columns))
        )

    df = df.copy()
    df["code_muni"] = df["code_muni"].astype(str)

    # -- optional municipality filter (mirrors R: subset + reorder) ----------
    if municipalities is not None:
        municipalities = [str(m) for m in municipalities]
        if verbose:
            console.print(
                "[cyan]INFO[/]  "
                + msg["warn_municipalities"].format(n_keep=len(municipalities))
            )
        df = df[df["code_muni"].isin(municipalities)]
        order_idx = {code: i for i, code in enumerate(municipalities)}
        df = df.loc[df["code_muni"].map(order_idx).sort_values().index]

    # -- realign df to W's internal region order (see docstring) --------------
    listw = W["listw"]
    ids_w = [str(i) for i in listw.id_order]

    if df["code_muni"].duplicated().any():
        raise ValueError(msg["err_duplicate_ids"])

    df_codes = set(df["code_muni"])
    w_codes = set(ids_w)
    if df_codes != w_codes:
        n_diff = len(df_codes.symmetric_difference(w_codes))
        raise ValueError(msg["err_id_mismatch"].format(n_diff=n_diff))

    if verbose:
        console.print("[cyan]INFO[/]  " + msg["step_align"].format(n=len(ids_w)))
    df = df.set_index("code_muni").loc[ids_w].reset_index()

    # -- extract and validate outcome vector -----------------------------------
    x = df[outcome].astype(float).to_numpy()
    n_w = len(ids_w)
    n_x = len(x)
    if n_x != n_w:  # defensive: guaranteed by the realignment above
        raise ValueError(msg["err_length_mismatch"].format(n_x=n_x, n_w=n_w))

    if np.all(np.isnan(x)):
        raise ValueError(msg["err_all_na"].format(outcome=outcome))

    n_na = int(np.isnan(x).sum())
    if n_na > 0:
        if verbose:
            console.print(
                "[yellow]WARNING[/]  "
                + msg["warn_na_values"].format(n_na=n_na, outcome=outcome)
            )
        x[np.isnan(x)] = np.nanmean(x)

    # Standardise (ddof=1, matching R's scale()). Note: Moran's I, local Ii,
    # and their significance are invariant to this affine transform (see
    # module tests / IDEIAS.md) -- done here only to mirror R's exact
    # intermediate values and to make z/lag_z signs directly usable below.
    z = (x - x.mean()) / x.std(ddof=1)

    # -- global Moran's I ------------------------------------------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_global"].format(permutations=permutations)
        )

    n_perm = int(permutations)
    mc_res = Moran(z, listw, permutations=n_perm, two_tailed=False)

    i_stat = float(mc_res.I)
    e_i = float(mc_res.EI)
    var_i = float(mc_res.VI_rand)
    z_i = (i_stat - e_i) / np.sqrt(var_i) if var_i > 0 else np.nan
    p_value = float(scipy_stats.norm.sf(z_i)) if not np.isnan(z_i) else np.nan

    if n_perm > 0 and hasattr(mc_res, "sim") and mc_res.sim is not None:
        sims = np.asarray(mc_res.sim)
        larger = int(np.sum(sims >= i_stat))
        p_simulated = (larger + 1.0) / (n_perm + 1.0)
    else:
        p_simulated = np.nan

    global_df = pd.DataFrame(
        {
            "I": [i_stat],
            "E.I": [e_i],
            "Var.I": [var_i],
            "Z.I": [z_i],
            "p_value": [p_value],
            "p_simulated": [p_simulated],
        }
    )

    # -- local Moran's I (LISA, conditional permutation) -----------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  " + msg["step_local"].format(permutations=permutations)
        )

    # R hardcodes iseed=1L for localmoran_perm() -- preserved quirk, see
    # docstring "Local statistic (LISA)" section. alternative="two-sided" is
    # passed only to silence esda's pending-default-change warning; it only
    # affects esda's own rank-based p_sim/rlisas, which this port does not
    # use (see p_raw_vec below, computed by hand from z_sim instead).
    lm_res = Moran_Local(z, listw, permutations=n_perm, seed=1, alternative="two-sided")

    ii = np.asarray(lm_res.Is, dtype=float)
    z_ii = np.asarray(lm_res.z_sim, dtype=float)
    with np.errstate(invalid="ignore"):
        p_raw_vec = 2.0 * scipy_stats.norm.sf(np.abs(z_ii))
    p_raw_vec = np.clip(p_raw_vec, 0.0, 1.0)

    p_adj_vec = _p_adjust(p_raw_vec, adjust_p)

    lag_z = np.asarray(lag_spatial(listw, z), dtype=float)

    # -- quadrant classification ------------------------------------------------
    if verbose:
        console.print(
            "[cyan]INFO[/]  "
            + msg["step_quadrant"].format(alpha=alpha, adjust_p=adjust_p)
        )

    with np.errstate(invalid="ignore"):
        sig = p_adj_vec < alpha
    quad = np.full(n_x, "NS", dtype=object)
    quad[sig & (z > 0) & (lag_z > 0)] = "HH"
    quad[sig & (z < 0) & (lag_z < 0)] = "LL"
    quad[sig & (z > 0) & (lag_z <= 0)] = "HL"
    quad[sig & (z <= 0) & (lag_z > 0)] = "LH"

    local_df = pd.DataFrame(
        {
            "code_muni": df["code_muni"].to_numpy(),
            "Ii": ii,
            "Z.Ii": z_ii,
            "p_raw": p_raw_vec,
            "p_adj": p_adj_vec,
            "quadrant": pd.Categorical(quad, categories=_QUAD_LEVELS),
        }
    )

    n_hh = int((quad == "HH").sum())
    n_ll = int((quad == "LL").sum())
    n_hl = int((quad == "HL").sum())
    n_lh = int((quad == "LH").sum())
    n_ns = int((quad == "NS").sum())

    if verbose:
        i_val = f"{i_stat:.4f}"
        p_val = "nan" if np.isnan(p_simulated) else f"{p_simulated:.4f}"
        console.print(
            "[green]OK[/]  "
            + msg["done"].format(
                i_val=i_val, p_val=p_val, n_hh=n_hh, n_ll=n_ll, n_hl=n_hl, n_lh=n_lh, n_ns=n_ns
            )
        )

    _ = datetime.now()  # parity placeholder: R attaches no meta/history here

    return {
        "global": global_df,
        "local": local_df,
        "n_HH": n_hh,
        "n_LL": n_ll,
        "n_HL": n_hl,
        "n_LH": n_lh,
        "outcome_name": outcome,
    }


def _p_adjust(p: np.ndarray, method: str) -> np.ndarray:
    """Replicate R's ``stats::p.adjust(p, method)`` (default ``n``).

    R's default argument ``n = length(p)`` is a promise evaluated lazily
    *after* ``p`` has already been reassigned to its non-``NA`` subset
    inside ``p.adjust()`` -- so in the common case (no ``n=`` supplied by
    the caller, as here), ``n`` silently resolves to the count of
    **non-missing** p-values, not the full vector length. This was
    verified numerically against R's ``p.adjust()`` output for both
    ``"fdr"`` and ``"bonferroni"`` with ``NA`` present. ``NaN`` entries in
    *p* (from island units with no permutation-derived variance) pass
    through as ``NaN`` in the output, matching R's ``NA``-passthrough.
    """
    p = np.asarray(p, dtype=float)
    out = np.full(len(p), np.nan)
    if method == "none":
        return p.copy()

    mask = ~np.isnan(p)
    p_valid = p[mask]
    n = lp = len(p_valid)
    if lp == 0 or n <= 1:
        out[mask] = p_valid
        return out

    if method == "bonferroni":
        adj = np.minimum(1.0, n * p_valid)
    elif method == "fdr":
        order = np.argsort(-p_valid, kind="stable")
        i = np.arange(lp, 0, -1)
        p_sorted = p_valid[order]
        vals = np.minimum(1.0, (n / i) * p_sorted)
        vals = np.minimum.accumulate(vals)
        ro = np.argsort(order, kind="stable")
        adj = vals[ro]
    else:  # pragma: no cover - guarded by VALID_ADJUST_P check upstream
        raise ValueError(f"Unknown adjust_p method: {method}")

    out[mask] = adj
    return out
