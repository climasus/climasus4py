"""climasus — Fast SUS and climate data workflows for Brazil.

Provides a high-level pipeline for downloading, cleaning, standardising,
filtering, aggregating and exporting DATASUS health data, with optional
climate and census enrichment.  All heavy lifting is done lazily via
DuckDB; results are only materialised when explicitly collected.

Typical usage::

    import climasus as cs

    result = cs.sus_pipeline("SIM-DO", "SP", 2022,
                             groups="respiratory", time="month")
    result.df().head()
"""

from climasus._version import __version__
from climasus.core.pipeline import sus_pipeline
from climasus.core.importer import sus_import
from climasus.core.clean import sus_clean
from climasus.core.standardize import sus_standardize
from climasus.core.filter import sus_filter
from climasus.core.variables import sus_variables
from climasus.core.aggregate import sus_aggregate
from climasus.core.engine import collect_arrow
from climasus.io.export import sus_export
from climasus.io.cache import sus_cache_info, sus_cache_clear
from climasus.enrichment.climate import sus_climate
from climasus.enrichment.spatial import sus_spatial
from climasus.enrichment.census import sus_census
from climasus.enrichment.fill_gaps import sus_fill_gaps

from climasus.utils.explore import sus_explore
from climasus.utils.quality import sus_quality
from climasus.utils import update_climasus_data

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
