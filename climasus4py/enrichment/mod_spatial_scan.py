"""Kulldorff circular scan statistic for spatial cluster detection.

Mirrors R: sus_mod_spatial_scan.R

R delegates the computation to ``SpatialEpi::kulldorff()``. There is no
Python package that exposes the Kulldorff scan (``SaTScan`` is a
standalone executable, not an importable library), so the statistic is
implemented here directly in NumPy.

That is not a re-derivation: ``SpatialEpi::kulldorff()`` is ~50 lines of
R and ``SpatialEpi::zones()`` is 15, with only the likelihood sweep in
C++, and the sweep is the published closed-form likelihood ratio. Both
were ported statement by statement and checked numerically against the
installed ``SpatialEpi`` 1.2.8 — see ``tests/test_mod_spatial_scan.py``.

Everything except the p-value is deterministic: the zone enumeration,
the likelihood of every zone, and therefore the identity of the
most-likely and secondary clusters all reproduce R exactly. The p-value
comes from a Monte Carlo draw (``rmultinom`` in R, ``Generator.
multinomial`` here) and so agrees only in distribution -- in R itself
two calls give two different p-values, since neither R's function nor
this one takes a seed.

Theory:
  Kulldorff & Nagarwalla (1995, Stat Med 14:799-810) - the scan statistic
  Kulldorff (1997, Commun Stat Theory Methods 26:1481-1496) - unified
    Poisson/Bernoulli formulation and the Monte Carlo test
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

import numpy as np
import pandas as pd
from rich.console import Console
from scipy.special import xlogy

console = Console(stderr=True)

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "title": "climasus4py - Varredura Espacial de Kulldorff",
        "step_join": "Unindo dados tabulares a geometria ({n} municipios)...",
        "step_centroids": "Calculando centroides dos municipios...",
        "step_scan": (
            "Executando varredura circular ({n_sim} simulacoes, "
            "frac. max. pop. = {pop_frac})..."
        ),
        "step_parse": "Analisando resultados e mapeando codigos de municipio...",
        "done": (
            "Concluido. {n_sig} aglomerado(s) significativo(s) detectado(s) "
            "(alpha = {alpha})."
        ),
        "warn_no_secondary": "Nenhum aglomerado secundario significativo encontrado.",
        "warn_expected_null": (
            "Coluna '{col}' nao encontrada; usando None (Poisson simples)."
        ),
        "warn_missing_muni": (
            "{n_miss} municipio(s) em 'df' sem correspondencia em "
            "'municipalities'; removido(s)."
        ),
        "err_col_missing": (
            "Coluna '{col}' nao encontrada em 'df'. Colunas disponiveis: {cols}."
        ),
        "err_code_muni_missing": (
            "Coluna 'code_muni' nao encontrada em 'df' nem em 'municipalities'."
        ),
        "err_not_sf": "'municipalities' deve ser um geopandas.GeoDataFrame.",
        "err_no_rows": (
            "Nenhuma linha restante apos o join. Verifique se os codigos de "
            "municipio correspondem."
        ),
    },
    "en": {
        "title": "climasus4py - Kulldorff Spatial Scan",
        "step_join": "Joining tabular data to geometry ({n} municipalities)...",
        "step_centroids": "Computing municipality centroids...",
        "step_scan": (
            "Running circular scan ({n_sim} simulations, "
            "max pop. frac. = {pop_frac})..."
        ),
        "step_parse": "Parsing results and mapping municipality codes...",
        "done": "Done. {n_sig} significant cluster(s) detected (alpha = {alpha}).",
        "warn_no_secondary": "No significant secondary clusters found.",
        "warn_expected_null": (
            "Column '{col}' not found; using None (simple Poisson)."
        ),
        "warn_missing_muni": (
            "{n_miss} municipality(ies) in 'df' without match in "
            "'municipalities'; removed."
        ),
        "err_col_missing": (
            "Column '{col}' not found in 'df'. Available columns: {cols}."
        ),
        "err_code_muni_missing": (
            "Column 'code_muni' not found in 'df' or 'municipalities'."
        ),
        "err_not_sf": "'municipalities' must be a geopandas.GeoDataFrame.",
        "err_no_rows": (
            "No rows remaining after join. Check that municipality codes match."
        ),
    },
    "es": {
        "title": "climasus4py - Exploracion Espacial de Kulldorff",
        "step_join": "Uniendo datos tabulares a la geometria ({n} municipios)...",
        "step_centroids": "Calculando centroides de los municipios...",
        "step_scan": (
            "Ejecutando exploracion circular ({n_sim} simulaciones, "
            "frac. max. pob. = {pop_frac})..."
        ),
        "step_parse": "Analizando resultados y mapeando codigos de municipio...",
        "done": (
            "Listo. {n_sig} aglomerado(s) significativo(s) detectado(s) "
            "(alpha = {alpha})."
        ),
        "warn_no_secondary": "No se encontraron aglomerados secundarios significativos.",
        "warn_expected_null": (
            "Columna '{col}' no encontrada; usando None (Poisson simple)."
        ),
        "warn_missing_muni": (
            "{n_miss} municipio(s) en 'df' sin correspondencia en "
            "'municipalities'; eliminado(s)."
        ),
        "err_col_missing": (
            "Columna '{col}' no encontrada en 'df'. Columnas disponibles: {cols}."
        ),
        "err_code_muni_missing": (
            "Columna 'code_muni' no encontrada en 'df' ni en 'municipalities'."
        ),
        "err_not_sf": "'municipalities' debe ser un geopandas.GeoDataFrame.",
        "err_no_rows": (
            "No quedan filas despues del join. Verifique que los codigos "
            "municipales coincidan."
        ),
    },
}


def _msg(key: str, lang: str, **kwargs: Any) -> str:
    entry = _MESSAGES.get(lang, _MESSAGES["pt"])
    return entry.get(key, _MESSAGES["pt"].get(key, key)).format(**kwargs)


# ---------------------------------------------------------------------------
# The scan statistic itself (port of SpatialEpi)
# ---------------------------------------------------------------------------


def _zones(
    geo: np.ndarray, population: np.ndarray, pop_upper_bound: float
) -> list[np.ndarray]:
    """Enumerate candidate circular zones -- port of ``SpatialEpi::zones()``.

    For each area *i*, areas are ordered by their distance to *i* and the
    running population share is accumulated; every prefix whose share stays
    at or below *pop_upper_bound* is a candidate zone. R returns the
    neighbour lists plus a flattened ``cluster.coords`` index; the prefix
    structure is the same information, so only the lists are returned here
    and zones are addressed as ``(centre, prefix length)``.

    Distances are plain Euclidean distances on whatever coordinates are
    passed, exactly as R's ``dist()`` -- see the note on degrees in
    :func:`sus_mod_spatial_scan`.
    """
    n = len(population)
    total = population.sum()
    dist = np.sqrt(((geo[:, None, :] - geo[None, :, :]) ** 2).sum(-1))
    out: list[np.ndarray] = []
    for i in range(n):
        # R: order(dist[, i]) -- radix sort, stable, ties by index.
        order = np.argsort(dist[:, i], kind="stable")
        keep = np.cumsum(population[order]) / total <= pop_upper_bound
        out.append(order[keep])
    return out


#: Whether to reproduce ``SpatialEpi``'s truncation of the study totals.
#:
#: ``SpatialEpi``'s C++ likelihood sweep truncates the **totals** -- total
#: cases, total expected, total population -- to whole numbers, while the
#: per-zone sums stay as doubles. Expected counts are a rate times a
#: population and so are essentially never whole, which means the null
#: model is fitted against a total that is short by up to one case.
#:
#: At most one case is discarded, but what that does to the statistic
#: does not follow the size of the study in any simple way -- measured,
#: the relative shift is largest where the statistic itself is small,
#: and on a 300-case grid with totals in the hundreds to thousands it
#: reached 1.8% with no useful correlation against the total. Two
#: reference points: 0.03% on the most-likely cluster of the test
#: fixture (4090.66 expected cases), and 18% on a toy with 12.6.
#: Recorded as **M85**.
#:
#: ``True`` (the default) matches R. Set it to ``False`` for the
#: statistically correct statistic, at the cost of parity -- the
#: corrected path is kept here so it does not have to be rewritten once
#: the upstream report is settled.
SPATIALEPI_TRUNCATES_TOTALS: bool = True


def _total(values: np.ndarray) -> Any:
    """Study total, truncated when reproducing ``SpatialEpi`` (see M85)."""
    return np.trunc(values.sum()) if SPATIALEPI_TRUNCATES_TOTALS else values.sum()


def _llr_poisson(
    cz: np.ndarray, ez: np.ndarray, total_cases: Any, total_expected: Any
) -> np.ndarray:
    """Kulldorff's Poisson log-likelihood ratio, zeroed for low-rate zones.

    The ``- C log(C / E)`` term vanishes when the expected counts already
    sum to the observed total, which is the usual textbook presentation;
    it is carried here because *expected* is supplied by the caller and
    need not be normalised.
    """
    outside_c = total_cases - cz
    outside_e = total_expected - ez
    with np.errstate(divide="ignore", invalid="ignore"):
        val = (
            xlogy(cz, cz / ez)
            + xlogy(outside_c, outside_c / outside_e)
            - xlogy(total_cases, total_cases / total_expected)
        )
        high = (cz * total_expected) > (total_cases * ez)
    return np.where(high & np.isfinite(val), val, 0.0)


def _llr_binomial(
    cz: np.ndarray, nz: np.ndarray, total_cases: Any, total_pop: Any
) -> np.ndarray:
    """Kulldorff's Bernoulli log-likelihood ratio, zeroed for low-rate zones.

    Written as the two-by-two ``sum(O * log(O / E))``; R's C++ writes the
    same quantity as a difference of two log-likelihoods. The forms are
    algebraically identical and, evaluated in double precision, equally
    accurate to about 2e-10 on the test fixture. Agreement with R is
    nearer 1e-8 relative, which is R's own accumulation error rather
    than a difference between the two arrangements.
    """
    out_c = total_cases - cz
    out_n = total_pop - nz
    p_case = total_cases / total_pop
    p_non = (total_pop - total_cases) / total_pop
    with np.errstate(divide="ignore", invalid="ignore"):
        val = (
            xlogy(cz, cz / (nz * p_case))
            + xlogy(nz - cz, (nz - cz) / (nz * p_non))
            + xlogy(out_c, out_c / (out_n * p_case))
            + xlogy(out_n - out_c, (out_n - out_c) / (out_n * p_non))
        )
        high = (cz * out_n) > (out_c * nz)
    return np.where(high & np.isfinite(val), val, 0.0)


def _all_log_lkhd(
    cases: np.ndarray,
    denominator: np.ndarray,
    neighbours: list[np.ndarray],
    kind: str,
) -> list[np.ndarray]:
    """Log-likelihood of every candidate zone -- R's ``computeAllLogLkhd()``.

    Returned per centre, so entry ``i[k]`` is the zone made of the first
    ``k + 1`` neighbours of centre ``i``. Zones of one centre are nested
    prefixes, so their case and denominator totals are one cumulative sum.
    """
    total_cases = _total(cases)
    total_den = _total(denominator)
    out: list[np.ndarray] = []
    for nb in neighbours:
        cz = np.cumsum(cases[nb])
        dz = np.cumsum(denominator[nb])
        if kind == "poisson":
            out.append(_llr_poisson(cz, dz, total_cases, total_den))
        else:
            out.append(_llr_binomial(cz, dz, total_cases, total_den))
    return out


def _max_log_lkhd_mc(
    perm: np.ndarray,
    denominator: np.ndarray,
    neighbours: list[np.ndarray],
    kind: str,
) -> np.ndarray:
    """Per-simulation maximum log-likelihood -- R's ``kulldorffMC()``.

    *perm* is ``(n_areas, n_simulations)``. Only the maximum over zones
    matters for the p-value, so each centre is reduced as it is computed
    rather than materialising the full zone-by-simulation array.
    """
    total_cases = np.trunc(perm.sum(axis=0).astype(float))
    total_den = _total(denominator)
    best = np.zeros(perm.shape[1])
    for nb in neighbours:
        cz = np.cumsum(perm[nb, :], axis=0)
        dz = np.cumsum(denominator[nb])[:, None]
        if kind == "poisson":
            lk = _llr_poisson(cz, dz, total_cases, total_den)
        else:
            lk = _llr_binomial(cz, dz, total_cases, total_den)
        np.maximum(best, lk.max(axis=0), out=best)
    return best


def _kulldorff(
    geo: np.ndarray,
    cases: np.ndarray,
    population: np.ndarray,
    expected_cases: np.ndarray | None,
    pop_upper_bound: float,
    n_simulations: int,
    alpha_level: float,
    rng: np.random.Generator,
) -> dict[str, Any]:
    """Port of ``SpatialEpi::kulldorff()``, statement for statement."""
    if expected_cases is None:
        kind = "binomial"
        denominator = population
        expected_cases = cases.sum() * (denominator / denominator.sum())
    else:
        kind = "poisson"
        denominator = expected_cases

    neighbours = _zones(geo, population, pop_upper_bound)
    lkhd = _all_log_lkhd(cases, denominator, neighbours, kind)

    # R flattens the zones and takes which.max, i.e. the first maximum in
    # centre-then-prefix order; the same tie-break is reproduced here.
    flat = np.concatenate(lkhd)
    sizes = np.array([len(v) for v in lkhd])
    centre_of = np.repeat(np.arange(len(sizes)), sizes)
    length_of = np.concatenate([np.arange(1, s + 1) for s in sizes])

    best = int(np.argmax(flat))
    cluster = neighbours[centre_of[best]][: length_of[best]]

    perm = rng.multinomial(
        int(round(cases.sum())), denominator / denominator.sum(), size=n_simulations
    ).T
    sim_lambda = _max_log_lkhd_mc(perm, denominator, neighbours, kind)
    combined = np.append(sim_lambda, flat[best])

    def _describe(idx: int, ids: np.ndarray) -> dict[str, Any]:
        obs = float(cases[ids].sum())
        exp = float(expected_cases[ids].sum())
        return {
            "location_ids": ids,
            "population": float(population[ids].sum()),
            "number_of_cases": obs,
            "expected_cases": exp,
            "SMR": obs / exp if exp > 0 else np.nan,
            "log_likelihood_ratio": float(flat[idx]),
            "monte_carlo_rank": int((combined >= flat[idx]).sum()),
            "p_value": float(1.0 - np.mean(combined < flat[idx])),
        }

    most_likely = _describe(best, cluster)

    current = set(cluster.tolist())
    secondary: list[dict[str, Any]] = []
    # R: order(lkhd, decreasing = TRUE), then walks from the 2nd entry.
    for idx in np.argsort(-flat, kind="stable")[1:]:
        idx = int(idx)
        new_ids = neighbours[centre_of[idx]][: length_of[idx]]
        if current.isdisjoint(new_ids.tolist()):
            found = _describe(idx, new_ids)
            if found["p_value"] > alpha_level:
                break
            secondary.append(found)
            current |= set(new_ids.tolist())

    return {
        "most_likely_cluster": most_likely,
        "secondary_clusters": secondary,
        "type": kind,
        "log_lkhd": flat,
        "simulated_log_lkhd": sim_lambda,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def sus_mod_spatial_scan(
    df: Any,
    cases: str,
    population: str,
    municipalities: Any,
    expected: str | None = None,
    max_pop_frac: float = 0.5,
    n_simulations: int = 999,
    alpha: float = 0.05,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
) -> dict[str, Any]:
    """Kulldorff circular scan statistic for spatial cluster detection.

    Detects geographic clusters of disease excess by sweeping circular
    windows of growing radius over municipality centroids, scoring every
    window by its likelihood ratio against the null of no clustering,
    and testing the strongest window by Monte Carlo. Secondary clusters
    are reported when they are disjoint from every cluster already
    accepted and significant at *alpha*.

    The model follows *expected*: when it is given the Poisson
    formulation is used with those expected counts, and when it is not,
    the Bernoulli formulation is used with *population* as the
    denominator and expected counts implied by the overall rate.

    Geometry:
        *municipalities* is needed only for its centroids. R computes
        them as ``st_centroid(st_transform(municipalities, 4326))`` and
        this port does the same, so distances are **Euclidean distances
        in degrees**, not projected distances.

        That is worth knowing when reading results: a degree of
        longitude is shorter than a degree of latitude everywhere off
        the equator, so the scan windows are east-west flattened
        ellipses rather than circles, and the flattening grows with
        latitude -- across Brazil the ratio runs from about 1.00 at the
        northern border to about 0.84 at the southern tip. Only the
        *ordering* of neighbours by distance is affected, so the effect
        is bounded, but it is not nothing. ``SpatialEpi`` ships
        ``latlong2grid()`` for exactly this conversion and R does not
        call it. Recorded as **M84**; replicated here rather than
        corrected, to keep parity.

    Args:
        df: Table with a ``code_muni`` column plus the columns named by
            *cases*, *population* and optionally *expected*. A
            ``pandas.DataFrame`` or a ``duckdb.DuckDBPyRelation``.
        cases: Name of the column in *df* holding case counts.
        population: Name of the column in *df* holding the at-risk
            population denominator.
        municipalities: A ``geopandas.GeoDataFrame`` with polygon
            geometry and a ``code_muni`` column -- the Python analog of
            R's ``sf`` object.
        expected: Name of the column in *df* holding expected counts
            under the null, or ``None`` (default) to select the
            Bernoulli formulation. A name that is not present in *df*
            is warned about and treated as ``None``, as in R.
        max_pop_frac: Largest population share a single scan window may
            hold. Defaults to ``0.5``.
        n_simulations: Monte Carlo replicates for the p-value. Defaults
            to ``999``.
        alpha: Significance level for accepting secondary clusters.
            Defaults to ``0.05``.
        lang: Message language: ``"pt"`` (default), ``"en"`` or ``"es"``.
        verbose: Whether to print progress messages. Defaults to ``True``.

    Returns:
        A ``dict`` (the Python analog of R's ``climasus_spatial_scan``
        S3 object) with keys:

        - ``"most_likely_cluster"``: ``dict`` with ``"location_ids"``
          (the ``code_muni`` values in the cluster), ``"observed"``,
          ``"expected"``, ``"RR"``, ``"log_lik"`` and ``"p_value"``.
        - ``"secondary_clusters"``: ``list`` of the same shape, holding
          only clusters significant at *alpha*.
        - ``"n_clusters"``: ``int``, significant clusters including the
          most-likely one when it too is significant.
        - ``"data"``: ``pandas.DataFrame`` with one row per retained
          municipality and columns ``"code_muni"``, ``"cases"``,
          ``"population"``, ``"expected"``, ``"in_mlc"``.
        - ``"meta"``: ``dict`` echoing the arguments plus
          ``"n_municipalities"`` and ``"call_time"``.

        R's ``$call`` slot has no meaningful Python analog and is not
        reproduced, matching ``sus_mod_spatial_weights`` and
        ``sus_mod_spatial_moran``.

        Reproducibility: the clusters and their likelihoods are
        deterministic, but ``"p_value"`` (and hence ``"n_clusters"``)
        come from the Monte Carlo draw. Neither R's function nor this
        one accepts a seed, so repeated calls give slightly different
        p-values in both.

    Raises:
        TypeError: If *municipalities* is not a ``geopandas.GeoDataFrame``.
        ValueError: If ``code_muni`` is missing from either input, if
            *cases* or *population* is missing from *df*, or if no rows
            survive the join.
        ImportError: If ``geopandas`` is not installed.

    Examples::

        import climasus4py as cs

        scan = cs.sus_mod_spatial_scan(
            df, cases="obitos", population="pop", municipalities=malha
        )
        print(scan["most_likely_cluster"]["location_ids"])
    """
    try:
        import geopandas as gpd
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "sus_mod_spatial_scan() requires geopandas. Install it with "
            "'pip install geopandas'."
        ) from exc

    if verbose:
        console.print(f"[bold cyan]{_msg('title', lang)}[/bold cyan]")

    if not isinstance(municipalities, gpd.GeoDataFrame):
        raise TypeError(_msg("err_not_sf", lang))

    if hasattr(df, "df") and not isinstance(df, pd.DataFrame):
        df = df.df()
    df = pd.DataFrame(df).copy()

    if "code_muni" not in df.columns or "code_muni" not in municipalities.columns:
        raise ValueError(_msg("err_code_muni_missing", lang))
    for col in (cases, population):
        if col not in df.columns:
            raise ValueError(
                _msg("err_col_missing", lang, col=col, cols=", ".join(df.columns))
            )

    expected_col = expected
    if expected is not None and expected not in df.columns:
        if verbose:
            console.print(f"[yellow]{_msg('warn_expected_null', lang, col=expected)}[/yellow]")
        expected_col = None

    df["code_muni"] = df["code_muni"].astype(str)
    munis = municipalities.copy()
    munis["code_muni"] = munis["code_muni"].astype(str)

    if verbose:
        console.print(_msg("step_join", lang, n=len(df)))

    joined = munis.merge(df, on="code_muni", how="left")
    n_miss = int(joined[cases].isna().sum())
    if n_miss > 0 and verbose:
        console.print(f"[yellow]{_msg('warn_missing_muni', lang, n_miss=n_miss)}[/yellow]")
    joined = joined[joined[cases].notna()]
    if len(joined) == 0:
        raise ValueError(_msg("err_no_rows", lang))
    joined = joined.sort_values("code_muni", kind="stable").reset_index(drop=True)

    if verbose:
        console.print(_msg("step_centroids", lang))

    # R: st_centroid(st_transform(x, 4326)) -- planar centroid of degrees.
    with np.errstate(all="ignore"):
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            centroids = joined.to_crs(4326).geometry.centroid
    geo = np.column_stack([centroids.x.to_numpy(), centroids.y.to_numpy()])

    cases_vec = joined[cases].to_numpy(float)
    pop_vec = joined[population].to_numpy(float)
    expected_vec = joined[expected_col].to_numpy(float) if expected_col else None
    code_vec = joined["code_muni"].to_numpy()

    if verbose:
        console.print(
            _msg("step_scan", lang, n_sim=n_simulations, pop_frac=max_pop_frac)
        )

    result = _kulldorff(
        geo,
        cases_vec,
        pop_vec,
        expected_vec,
        max_pop_frac,
        int(n_simulations),
        alpha,
        np.random.default_rng(),
    )

    if verbose:
        console.print(_msg("step_parse", lang))

    def _parse(cl: dict[str, Any]) -> dict[str, Any]:
        obs, exp = cl["number_of_cases"], cl["expected_cases"]
        rr = cl["SMR"] if not np.isnan(cl["SMR"]) else (obs / exp if exp > 0 else np.nan)
        return {
            "location_ids": code_vec[cl["location_ids"]].tolist(),
            "observed": float(obs),
            "expected": float(exp),
            "RR": float(rr),
            "log_lik": float(cl["log_likelihood_ratio"]),
            "p_value": float(cl["p_value"]),
        }

    mlc = _parse(result["most_likely_cluster"])
    sec_list = [
        parsed
        for parsed in (_parse(c) for c in result["secondary_clusters"])
        if not np.isnan(parsed["p_value"]) and parsed["p_value"] < alpha
    ]

    mlc_sig = not np.isnan(mlc["p_value"]) and mlc["p_value"] < alpha
    n_sig = int(mlc_sig) + len(sec_list)

    if expected_col:
        exp_values = joined[expected_col].to_numpy(float)
    else:
        total_rate = np.nansum(cases_vec) / np.nansum(pop_vec)
        exp_values = total_rate * pop_vec

    data_tbl = pd.DataFrame(
        {
            "code_muni": code_vec,
            "cases": cases_vec,
            "population": pop_vec,
            "expected": exp_values,
            "in_mlc": np.isin(code_vec, mlc["location_ids"]),
        }
    )

    if verbose:
        if not sec_list:
            console.print(f"[yellow]{_msg('warn_no_secondary', lang)}[/yellow]")
        console.print(f"[green]{_msg('done', lang, n_sig=n_sig, alpha=alpha)}[/green]")

    return {
        "most_likely_cluster": mlc,
        "secondary_clusters": sec_list,
        "n_clusters": n_sig,
        "data": data_tbl,
        "meta": {
            "cases_col": cases,
            "population_col": population,
            "expected_col": expected_col,
            "max_pop_frac": max_pop_frac,
            "n_simulations": int(n_simulations),
            "alpha": alpha,
            "n_municipalities": len(joined),
            "call_time": datetime.now(),
        },
    }
