"""Reproducible analytical pipelines: the YAML recipe and what runs it.

Mirrors R: sus_rap_recipe.R, sus_rap_from_recipe.R, sus_rap_run.R,
sus_rap_inspect.R

Why only four of the twelve RAP functions are here
--------------------------------------------------
The block splits by **what the artefact is**, and only one half can cross
languages at all:

===============  ==================================  ==================
function         artefact                            portable?
===============  ==================================  ==================
``recipe``       YAML: ``function_name`` + params    **yes**
``from_recipe``  YAML                                **yes**
``run``          rebuilds a call from that data      **yes**
``inspect``      prints the object                   **yes**
``export``       writes an **R script**              no
``read``         **parses** an R script              no
``update``       edits R script text in place        no
``targets``      writes ``_targets.R``               no
``make``         runs ``targets::tar_make()``        no
===============  ==================================  ==================

The recipe is the interoperable surface: R writes the steps as *data* —
``list(function_name = ..., params = ...)`` — not as code, so a recipe
written on either side can be read on the other. The script half round-trips
R source text, and a Python equivalent would emit Python that R cannot run,
which defeats the point of a reproducible pipeline. ``sus_rap_gui``,
``sus_rap_template`` and ``sus_rap_addin_export`` were already settled as
R-ecosystem on 2026-09-09.

One structural difference, deliberate: R's ``sus_rap_run`` builds a source
string and calls ``eval(parse(text = ...))``. Here the same data drives a
**dispatch** — ``getattr(climasus4py, name)(**params)`` — which needs no
code generation and cannot execute anything the recipe did not name.

Two things the recipe promises and does not deliver (M112)
---------------------------------------------------------
**``data_hash`` is never checked.** Only ``sus_rap_recipe`` mentions the
field in the whole R package: nothing in ``from_recipe``, ``run`` or
``.rap_check_integrity`` reads it. It is written and forgotten, so it
provides no integrity guarantee.

**And it could not be checked across languages anyway.** R computes it as
``digest::digest(input_str, algo = "xxhash32")`` with ``digest``'s default
``serialize = TRUE``, so the bytes hashed are R's *internal serialisation*
of the string, not the string. Measured on ``"SP|2015|2022|SIM-DO"``: R's
default gives ``28692946`` and the raw bytes give ``bdc04062``. The one
field meant to verify "same inputs" is the one that cannot travel.

This module hashes the raw UTF-8 bytes, which is portable and verifiable.
A recipe written here therefore carries a hash a Python reader can check
and an R reader will not match — stated in :func:`sus_rap_recipe` where a
caller will see it.
"""

from __future__ import annotations

import platform
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

__all__ = [
    "sus_rap_recipe",
    "sus_rap_from_recipe",
    "sus_rap_run",
    "sus_rap_inspect",
    "xxhash32",
]

_RECIPE_VERSION = "1.0"
# The pipeline always opens with the import, whatever the recipe's first
# step says - see the note in sus_rap_run.
_FIRST_CALL = "sus_data_import"

_PRIME1 = 0x9E3779B1
_PRIME2 = 0x85EBCA77
_PRIME3 = 0xC2B2AE3D
_PRIME4 = 0x27D4EB2F
_PRIME5 = 0x165667B1
_M32 = 0xFFFFFFFF


def _rotl(x: int, r: int) -> int:
    return ((x << r) | (x >> (32 - r))) & _M32


def xxhash32(data: bytes | str, seed: int = 0) -> str:
    """xxHash32 of *data*, as eight lowercase hex digits.

    Matches ``digest::digest(x, algo = "xxhash32", serialize = FALSE)``.
    Implemented here because the ``xxhash`` package is not a dependency of
    this project and the algorithm is short and fixed.

    Verified against R on seven inputs including the empty string
    (``02cc5d05``), ``"abc"`` (``32d153ff``) and a 200-byte string.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    n = len(data)
    if n >= 16:
        v1 = (seed + _PRIME1 + _PRIME2) & _M32
        v2 = (seed + _PRIME2) & _M32
        v3 = seed & _M32
        v4 = (seed - _PRIME1) & _M32
        i = 0
        while i + 16 <= n:
            for k, v in enumerate((v1, v2, v3, v4)):
                bloco = int.from_bytes(data[i + 4 * k:i + 4 * k + 4], "little")
                v = (v + bloco * _PRIME2) & _M32
                v = (_rotl(v, 13) * _PRIME1) & _M32
                if k == 0:
                    v1 = v
                elif k == 1:
                    v2 = v
                elif k == 2:
                    v3 = v
                else:
                    v4 = v
            i += 16
        h = (_rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18)) & _M32
    else:
        h = (seed + _PRIME5) & _M32
        i = 0
    h = (h + n) & _M32

    while i + 4 <= n:
        h = (h + int.from_bytes(data[i:i + 4], "little") * _PRIME3) & _M32
        h = (_rotl(h, 17) * _PRIME4) & _M32
        i += 4
    while i < n:
        h = (h + data[i] * _PRIME5) & _M32
        h = (_rotl(h, 11) * _PRIME1) & _M32
        i += 1

    h = (h ^ (h >> 15)) & _M32
    h = (h * _PRIME2) & _M32
    h = (h ^ (h >> 13)) & _M32
    h = (h * _PRIME3) & _M32
    h = (h ^ (h >> 16)) & _M32
    return f"{h:08x}"


def _estrutura(params: dict, steps: list[dict], tipo: str) -> dict:
    """The ``structure`` slot, as both R constructors build it."""
    return {
        "type": tipo,
        "input_params": {"uf": params.get("uf"),
                         "years": params.get("years"),
                         "system": params.get("system")},
        "output_params": {"time_unit": params.get("time_unit"),
                          "group_by": params.get("group_by")},
        "total_steps": len(steps),
        "functions_used": [s.get("function_name") for s in steps],
    }


def sus_rap_recipe(
    rap: Any,
    file_path: str | Path | None = None,
    include_data_hash: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
    overwrite: bool = False,
) -> Path:
    """Serialise a pipeline object to a YAML recipe.

    The keys R's ``from_recipe`` actually reads — ``parameters``, ``steps``,
    ``metadata``, ``pipeline_type`` — are written with the same names and
    shapes, so a recipe written here loads in R and vice versa. ``steps`` is
    a list of ``{function_name, params}``, which is why this half of the
    RAP block is portable at all: the steps are data, not code.

    Args:
        rap: A pipeline object — the dict from :func:`sus_rap_from_recipe`,
            or any mapping with ``params`` and ``steps``.
        file_path: Where to write. Defaults to
            ``rap_recipe_<timestamp>.yaml`` in the working directory, as R
            does.
        include_data_hash: Whether to add ``data_hash``. Defaults to
            ``True``.

            Two caveats, both R's (M112). **Nothing reads this field** —
            ``sus_rap_recipe`` is the only place in the whole R package that
            mentions it — so it is not an integrity check. And R computes it
            over its own internal serialisation of the input string, while
            this computes it over the string's UTF-8 bytes, so the two never
            agree: on ``"SP|2015|2022|SIM-DO"`` R writes ``28692946`` and
            this writes ``bdc04062``. Hashing the bytes is the portable
            choice; hashing R's serialisation would only be checkable in R,
            where nothing checks it.
        lang: Message language. Recorded in the recipe's parameters.
        overwrite: Whether to replace an existing file. Defaults to
            ``False``, which raises instead.

    Returns:
        The path written.

    Raises:
        ImportError: If PyYAML is not installed.
        TypeError: If *rap* has no ``steps``.
        FileExistsError: If the file exists and *overwrite* is ``False``.
    """
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "sus_rap_recipe() needs PyYAML. Install it with "
            "`pip install pyyaml`."
        ) from exc

    if not isinstance(rap, dict) or "steps" not in rap:
        raise TypeError(
            "rap must be a mapping with a 'steps' list — the object from "
            "sus_rap_from_recipe(), for instance."
        )

    caminho = Path(file_path) if file_path is not None else Path(
        f"rap_recipe_{datetime.now():%Y%m%d_%H%M%S}.yaml")
    if caminho.exists() and not overwrite:
        raise FileExistsError(
            f"{caminho} already exists; pass overwrite=True to replace it."
        )

    from .._version import __version__ as versao

    params = dict(rap.get("params") or {})
    params.setdefault("lang", lang)
    passos = [
        {"function_name": s.get("function_name"),
         "params": (dict(s.get("important_params") or {}) or None)}
        for s in rap["steps"]
    ]
    # A hand-built object may carry a partial `structure` — only `type`,
    # say — so the hash inputs are derived from `params` whenever the
    # structure does not supply them. Reading them straight off a partial
    # structure hashed the EMPTY STRING and produced a constant
    # "02cc5d05" for every recipe, which a test caught here.
    estrutura = dict(rap.get("structure") or {})
    completa = _estrutura(params, rap["steps"],
                          estrutura.get("type") or "Pipeline Generico")
    for chave, valor in completa.items():
        estrutura.setdefault(chave, valor)
    receita: dict[str, Any] = {
        "rap_version": _RECIPE_VERSION,
        "created": f"{datetime.now():%Y-%m-%d %H:%M:%S}",
        "pipeline_type": estrutura.get("type") or "Pipeline Generico",
        "parameters": params,
        "steps": passos,
        "metadata": {
            "python_version": sys.version.split()[0],
            "platform": platform.system().lower(),
            "package_version": versao,
            "generator": f"climasus4py {versao}",
        },
    }
    if include_data_hash:
        entrada = "|".join(
            str(v) for v in _achata(estrutura.get("input_params") or {})
            if v is not None)
        receita["data_hash"] = xxhash32(entrada)

    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(receita, fh, allow_unicode=True, sort_keys=False)
    return caminho


def _achata(valor: Any) -> list[Any]:
    """Flatten nested values the way R's ``unlist`` does, for the hash."""
    if isinstance(valor, dict):
        saida: list[Any] = []
        for v in valor.values():
            saida.extend(_achata(v))
        return saida
    if isinstance(valor, (list, tuple)):
        saida = []
        for v in valor:
            saida.extend(_achata(v))
        return saida
    return [valor]


def sus_rap_from_recipe(
    recipe_path: str | Path,
    override_params: dict[str, Any] | None = None,
    execute: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    **kwargs: Any,
) -> dict[str, Any]:
    """Load a YAML recipe and rebuild the pipeline object.

    Pure data handling — no code is parsed, which is what makes this
    function portable while ``sus_rap_read`` (which parses an R script) is
    not.

    Args:
        recipe_path: Path to a recipe written by :func:`sus_rap_recipe` or
            by R's ``sus_rap_recipe()``.
        override_params: Parameters to merge over the recipe's, for
            re-running with a different cut.
        execute: Whether to run the pipeline immediately via
            :func:`sus_rap_run`. Defaults to ``False``.
        lang: Message language.
        **kwargs: Forwarded to :func:`sus_rap_run` when *execute*.

    Returns:
        The pipeline object as a dict with ``metadata``, ``params``,
        ``steps``, ``structure``, ``source`` and ``format``; or the
        pipeline's result when *execute*.

    Raises:
        ImportError: If PyYAML is not installed.
        FileNotFoundError: If *recipe_path* does not exist.
        ValueError: If the file does not parse as a recipe mapping.
    """
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "sus_rap_from_recipe() needs PyYAML. Install it with "
            "`pip install pyyaml`."
        ) from exc

    caminho = Path(recipe_path)
    if not caminho.is_file():
        raise FileNotFoundError(f"recipe not found: {caminho}")
    with caminho.open(encoding="utf-8") as fh:
        receita = yaml.safe_load(fh)
    if not isinstance(receita, dict):
        raise ValueError(
            f"{caminho} does not parse as a recipe: expected a mapping, got "
            f"{type(receita).__name__}."
        )

    params = dict(receita.get("parameters") or {})
    params.update(override_params or {})
    passos = [
        {"function_name": (s or {}).get("function_name") or "unknown",
         "important_params": dict((s or {}).get("params") or {}),
         "arguments": {},
         "line": ""}
        for s in (receita.get("steps") or [])
    ]
    meta = dict(receita.get("metadata") or {})
    # R writes `created` at the TOP level of the recipe and copies only
    # `recipe$metadata` into the object, so the timestamp is lost on every
    # round-trip and `sus_rap_inspect` prints "?" for it. Carried across
    # here: a reproducibility artefact that forgets when it was made is
    # not much of one.
    if "created" not in meta and receita.get("created"):
        meta["created"] = receita["created"]
    # Both R and this write the timestamp unquoted, so `yaml.safe_load`
    # resolves it to a `datetime` — the field would then be a str or a
    # datetime depending on the quoting in the file that happened to be
    # read. Normalised to text, which is what it is in the recipe.
    if isinstance(meta.get("created"), (datetime, date)):
        meta["created"] = f"{meta['created']:%Y-%m-%d %H:%M:%S}"
    obj = {
        "metadata": meta,
        "params": params,
        "steps": passos,
        "structure": _estrutura(
            params, passos,
            receita.get("pipeline_type") or "Pipeline Generico"),
        "source": str(caminho.resolve()),
        "format": "recipe",
        "data_hash": receita.get("data_hash"),
    }
    if execute:
        return sus_rap_run(obj, lang=lang, **kwargs)
    return obj


def sus_rap_run(
    rap: Any,
    dry_run: bool = False,
    lang: Literal["pt", "en", "es"] = "pt",
    verbose: bool = True,
    **overrides: Any,
) -> Any:
    """Execute the pipeline a recipe describes.

    R rebuilds a source string and calls ``eval(parse(text = ...))``. Here
    the same step data drives a dispatch: each ``function_name`` is looked
    up in the package's public namespace and called with its parameters,
    the previous result threaded in as the first argument. Nothing is
    generated, parsed or evaluated, so a recipe cannot smuggle in code —
    only names this package already exports can run.

    **The first step's own name is discarded**, replicating R: the pipeline
    always opens with ``sus_data_import`` built from
    ``params['uf']``, ``['years']``, ``['system']`` and ``['lang']``, and
    only the remaining steps are threaded. A recipe whose first step is
    something else is rewritten accordingly — R does this silently, and a
    warning is raised here instead.

    Args:
        rap: The pipeline object from :func:`sus_rap_from_recipe`.
        dry_run: Print the calls that would run and return ``None``,
            without touching data.
        lang: Message language.
        verbose: Whether to print progress.
        **overrides: Parameters merged over the recipe's before running.

    Returns:
        Whatever the last step returns, or ``None`` when *dry_run*.

    Raises:
        TypeError: If *rap* has no ``steps``.
        ValueError: If a step names something this package does not export.
    """
    if not isinstance(rap, dict) or not rap.get("steps"):
        raise TypeError(
            "rap must be a mapping with a non-empty 'steps' list."
        )

    import climasus4py as cs

    params = dict(rap.get("params") or {})
    params.update(overrides)
    passos = rap["steps"]

    primeiro = passos[0].get("function_name")
    if primeiro and primeiro != _FIRST_CALL:
        import warnings

        warnings.warn(
            f"the recipe's first step is {primeiro!r}, but the pipeline "
            f"always opens with {_FIRST_CALL}() built from the recipe's "
            f"parameters — R does the same, silently. {primeiro!r} is "
            f"skipped.",
            UserWarning, stacklevel=2,
        )

    args_import = {
        "system": params.get("system", "SIM-DO"),
        "uf": params.get("uf"),
        "year": params.get("years"),
        "lang": params.get("lang", lang),
    }
    chamadas: list[tuple[str, dict]] = [(_FIRST_CALL, args_import)]
    for s in passos[1:]:
        nome = s.get("function_name")
        prms = dict(s.get("important_params") or {})
        if not prms:
            prms = {"lang": params.get("lang", lang)}
        chamadas.append((nome, prms))

    if dry_run:
        for i, (nome, prms) in enumerate(chamadas, 1):
            arg = ", ".join(f"{k}={v!r}" for k, v in prms.items()
                            if v is not None)
            print(f"  {i:2d}. {nome}({arg})")
        return None

    faltando = [n for n, _ in chamadas if not hasattr(cs, n)]
    if faltando:
        raise ValueError(
            f"the recipe names function(s) this package does not export: "
            f"{faltando}. A recipe can only run what climasus4py exposes."
        )

    resultado = None
    for i, (nome, prms) in enumerate(chamadas):
        fn = getattr(cs, nome)
        limpos = {k: v for k, v in prms.items() if v is not None}
        if verbose:
            print(f"[sus_rap_run] {i + 1}/{len(chamadas)} {nome}")
        resultado = (fn(**limpos) if i == 0
                     else fn(resultado, **limpos))
    return resultado


def sus_rap_inspect(
    rap: Any,
    rap2: Any = None,
    verbose: bool = True,
    lang: Literal["pt", "en", "es"] = "pt",
) -> dict[str, Any]:
    """Print a structured summary of a pipeline object, and return it.

    With *rap2*, the two are compared field by field, which is what makes
    this useful: it answers "is this the same pipeline?" without diffing
    YAML by eye.

    Args:
        rap: The pipeline object.
        rap2: A second object to compare against, or ``None``.
        verbose: Whether to list the steps.
        lang: Message language.

    Returns:
        ``{"summary": ...}`` and, when *rap2* is given, ``"comparison"``
        with the fields that differ.

    Raises:
        TypeError: If *rap* is not a pipeline object.
    """
    if not isinstance(rap, dict) or "steps" not in rap:
        raise TypeError("rap must be a mapping with a 'steps' list.")

    resumo = _resumo(rap)
    print()
    print("=" * 62)
    print(" rap_object summary")
    print("=" * 62)
    for rotulo, valor in (
        ("Source", resumo["source"]),
        ("Format", resumo["format"]),
        ("Type", resumo["type"]),
        (resumo["runtime"], resumo["runtime_version"]),
        ("Package", f"{resumo['package']} {resumo['package_version']}"),
        ("Created", resumo["created"]),
        ("Platform", resumo["platform"]),
        ("Seed", resumo["seed"]),
        ("Data hash", resumo["data_hash"]),
    ):
        print(f"  {rotulo:<14} {valor}")
    print("\n  -- Params")
    for chave in ("uf", "years", "system", "time_unit", "lang"):
        valor = resumo["params"].get(chave)
        if isinstance(valor, (list, tuple)):
            valor = ", ".join(str(v) for v in valor)
        print(f"  {chave:<14} {valor if valor is not None else '?'}")

    if verbose and rap["steps"]:
        print(f"\n  -- Pipeline steps ({len(rap['steps'])})")
        for i, s in enumerate(rap["steps"], 1):
            prms = ", ".join(
                f"{k}={v!r}" for k, v in (s.get("important_params") or {}).items())
            print(f"  {i:2d}. {s.get('function_name') or '?':<38} {prms}")

    saida: dict[str, Any] = {"summary": resumo}
    if rap2 is not None:
        if not isinstance(rap2, dict) or "steps" not in rap2:
            raise TypeError("rap2 must be a mapping with a 'steps' list.")
        outro = _resumo(rap2)
        diferencas = {
            k: (resumo[k], outro[k]) for k in resumo
            if k != "params" and resumo[k] != outro[k]
        }
        params_dif = {
            k: (resumo["params"].get(k), outro["params"].get(k))
            for k in set(resumo["params"]) | set(outro["params"])
            if resumo["params"].get(k) != outro["params"].get(k)
        }
        print("\n  -- Comparison")
        if not diferencas and not params_dif:
            print("  identical on every compared field")
        for k, (a, b) in {**diferencas,
                          **{f"params.{k}": v
                             for k, v in params_dif.items()}}.items():
            print(f"  {k:<14} {a!r}  ->  {b!r}")
        saida["comparison"] = {"fields": diferencas, "params": params_dif}
    return saida


def _resumo(rap: dict) -> dict[str, Any]:
    meta = rap.get("metadata") or {}
    estrutura = rap.get("structure") or {}
    params = rap.get("params") or {}
    return {
        "source": rap.get("source") or "?",
        "format": rap.get("format") or "?",
        "type": estrutura.get("type") or "?",
        # A recipe written by climasus4r carries r_version, not
        # python_version: label the line for whichever runtime wrote it
        # instead of printing an R banner under a "Python" label.
        "runtime": "R" if ("python_version" not in meta
                           and "r_version" in meta) else "Python",
        "runtime_version": meta.get("python_version")
        or meta.get("r_version") or "?",
        "package": "climasus4r" if ("python_version" not in meta
                                    and "r_version" in meta) else "climasus4py",
        "package_version": meta.get("package_version") or "?",
        "created": meta.get("created") or "?",
        "platform": meta.get("platform") or "?",
        "seed": meta.get("random_seed") or params.get("seed") or "?",
        "data_hash": rap.get("data_hash") or "?",
        "total_steps": estrutura.get("total_steps") or len(rap.get("steps") or []),
        "functions_used": estrutura.get("functions_used") or [],
        "params": dict(params),
    }
