"""Early check for the NetCDF stack, before anything is downloaded (M40).

Three of the ``sus_grid_*`` functions read ``.nc`` rasters —
``sus_grid_pdsi``, ``sus_grid_era5`` and ``sus_grid_pollution_merra2`` —
and the missing piece was never ``xarray`` itself but an **engine**.
``xarray`` reads NetCDF through a backend (``netcdf4``, ``h5netcdf`` or
``scipy``), and with none installed ``open_dataset`` fails pointing at
xarray's own installation and IO documentation: a message that means
nothing to someone who does not know the ecosystem, and that arrives
*after* the download.

That was the sting. ``sus_grid_pdsi`` downloaded the TerraClimate file
successfully and then died on the read, so the user paid for a large
download to receive an incomprehensible error. The dependency
declaration was fixed on 07/09/2026 with the ``[grid]`` extra; what was
left, and is what this module does, is to check **before** spending the
download.

The imports stay lazy, as they are everywhere in this family: nothing
here runs at ``import climasus4py`` time.
"""

from __future__ import annotations

from importlib.util import find_spec

#: Backends xarray can read a local ``.nc`` file with. ``pydap`` and
#: ``zarr`` are xarray engines too but do not read a NetCDF file on disk,
#: so they do not count here.
_NETCDF_ENGINES: tuple[str, ...] = ("netcdf4", "h5netcdf", "scipy")

#: Engine name -> the distribution that provides it. ``netcdf4`` is the
#: engine name and ``netCDF4`` the import name, which is why this mapping
#: is spelled out rather than derived.
_ENGINE_PACKAGE: dict[str, str] = {
    "netcdf4": "netCDF4",
    "h5netcdf": "h5netcdf",
    "scipy": "scipy",
}

_MESSAGES: dict[str, dict[str, str]] = {
    "pt": {
        "need_xarray": (
            "As funcoes sus_grid_* que leem raster NetCDF precisam de "
            "xarray e rioxarray."
        ),
        "need_engine": (
            "O xarray esta instalado, mas sem nenhum engine capaz de ler "
            "arquivo NetCDF ({engines}). O download nao foi iniciado: sem "
            "engine ele seria gasto e a leitura falharia no fim, com uma "
            "mensagem do proprio xarray apontando a documentacao dele."
        ),
        "how": "Instale com: pip install 'climasus4py[grid]'",
    },
    "en": {
        "need_xarray": (
            "The sus_grid_* functions that read NetCDF rasters require "
            "xarray and rioxarray."
        ),
        "need_engine": (
            "xarray is installed but no engine able to read a NetCDF file "
            "is available ({engines}). The download was NOT started: "
            "without an engine it would be spent and the read would fail "
            "at the end, with xarray's own message pointing at its "
            "documentation."
        ),
        "how": "Install with: pip install 'climasus4py[grid]'",
    },
    "es": {
        "need_xarray": (
            "Las funciones sus_grid_* que leen raster NetCDF requieren "
            "xarray y rioxarray."
        ),
        "need_engine": (
            "xarray esta instalado, pero sin ningun engine capaz de leer "
            "archivo NetCDF ({engines}). La descarga NO fue iniciada: sin "
            "engine se gastaria y la lectura fallaria al final, con un "
            "mensaje del propio xarray apuntando a su documentacion."
        ),
        "how": "Instale con: pip install 'climasus4py[grid]'",
    },
}


def available_netcdf_engines() -> list[str]:
    """Which NetCDF engines xarray can actually use, in this environment.

    Asks xarray rather than only probing imports: an engine registers
    itself as a backend plugin, and a package being importable is not the
    same as its plugin being loadable. Falls back to probing the import
    names when xarray is too old to expose ``list_engines``.
    """
    try:
        import xarray as xr
    except ImportError:
        return []

    try:
        registrados = set(xr.backends.list_engines())
    except Exception:  # pragma: no cover - xarray sem list_engines
        registrados = {
            e for e in _NETCDF_ENGINES
            if find_spec(_ENGINE_PACKAGE[e]) is not None
        }
    return [e for e in _NETCDF_ENGINES if e in registrados]


def require_netcdf_stack(lang: str = "pt") -> None:
    """Raise before the download when the NetCDF stack is incomplete.

    Args:
        lang: Message language — ``"pt"`` (default), ``"en"`` or
            ``"es"``.

    Raises:
        ImportError: If ``xarray``/``rioxarray`` are missing, or if no
            engine can read a NetCDF file. The message names the
            ``[grid]`` extra, which is what actually fixes it — an error
            that does not say how to fix it costs the reader a search.
    """
    msg = _MESSAGES.get(lang, _MESSAGES["pt"])

    faltando = [m for m in ("xarray", "rioxarray") if find_spec(m) is None]
    if faltando:
        raise ImportError(
            f"{msg['need_xarray']} {', '.join(faltando)}. {msg['how']}")

    if not available_netcdf_engines():
        raise ImportError(
            msg["need_engine"].format(engines=", ".join(_NETCDF_ENGINES))
            + " "
            + msg["how"]
        )
