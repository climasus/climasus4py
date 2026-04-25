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

from climasus4py._version import __version__
from climasus4py.core.pipeline import sus_pipeline
from climasus4py.core.importer import sus_import
from climasus4py.core.clean import sus_clean
from climasus4py.core.standardize import sus_standardize
from climasus4py.core.filter import sus_filter
from climasus4py.core.variables import sus_variables
from climasus4py.core.aggregate import sus_aggregate
from climasus4py.core.engine import collect_arrow
from climasus4py.io.export import sus_export
from climasus4py.io.cache import sus_cache_info, sus_cache_clear
from climasus4py.core.climate_inmet import sus_climate_inmet
from climasus4py.enrichment.climate import sus_climate
from climasus4py.enrichment.spatial import sus_spatial
from climasus4py.enrichment.census import sus_census
from climasus4py.enrichment.fill_gaps import sus_fill_gaps

from climasus4py.utils.explore import sus_explore
from climasus4py.utils.quality import sus_quality
from climasus4py.utils import update_climasus_data

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
    # I/O
    "sus_export",
    "sus_cache_info",
    "sus_cache_clear",
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
    "update_climasus_data",
]
