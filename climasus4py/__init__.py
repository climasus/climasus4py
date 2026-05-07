"""climasus — Fast SUS and climate data workflows for Brazil.

Provides a high-level pipeline for downloading, cleaning, standardising,
filtering, aggregating and exporting DATASUS health data, with optional
climate and census enrichment.  All heavy lifting is done lazily via
DuckDB; results are only materialised when explicitly collected.

Typical usage::

    import climasus4py as cs

    result = cs.sus_pipeline("SIM-DO", "SP", 2022,
                             groups="respiratory", time="month")
    result.df().head()
"""

from ._version import __version__
from .core.aggregate import sus_aggregate
from .core.clean import sus_clean
from .core.climate_inmet import sus_climate_inmet
from .core.engine import collect_arrow
from .core.filter import sus_filter
from .core.importer import sus_import
from .core.pipeline import sus_pipeline
from .core.standardize import sus_standardize
from .core.sus_sql import sus_sql
from .core.variables import sus_variables
from .enrichment.census import sus_census
from .enrichment.climate import sus_climate
from .enrichment.fill_gaps import sus_fill_gaps
from .enrichment.spatial import sus_spatial
from .io.cache import sus_cache_clear, sus_cache_info
from .io.export import sus_export
from .io.materialize import materialize
from .io.read import sus_read
from .utils import update_climasus_data
from .utils.chat import sus_chat
from .utils.explore import sus_explore
from .utils.quality import sus_quality

__all__ = [
    "__version__",
    # Pipeline
    "sus_pipeline",
    # Core
    "sus_import",
    "sus_clean",
    "sus_standardize",
    "sus_filter",
    "sus_variables",
    "sus_aggregate",
    "collect_arrow",
    "sus_sql",
    # I/O
    "sus_export",
    "sus_cache_info",
    "sus_cache_clear",
    "materialize",
    "sus_read",
    # Climate import
    "sus_climate_inmet",
    # Enrichment
    "sus_climate",
    "sus_spatial",
    "sus_census",
    "sus_fill_gaps",
    # Utilities
    "sus_explore",
    "sus_quality",
    "sus_chat",
    "update_climasus_data",
]
