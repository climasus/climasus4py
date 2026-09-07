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
from .core.aggregate import sus_data_aggregate
from .core.clean import sus_data_clean_encoding
from .core.climate_inmet import sus_climate_inmet
from .core.engine import collect_arrow
from .core.filter import sus_filter
from .core.importer import sus_data_import
from .core.meta import sus_meta
from .core.pipeline import sus_pipeline
from .core.standardize import sus_data_standardize
from .core.sus_sql import sus_sql
from .core.climate_normals import sus_climate_normals, sus_climate_normals_meta
from .core.climate_uniplu import sus_climate_uniplu
from .core.variables import sus_data_create_variables
from .enrichment.census import sus_census
from .enrichment.climate import sus_climate
from .enrichment.climate_aggregate import sus_climate_aggregate, sus_climate_info
from .enrichment.climate_anomaly import sus_climate_anomaly
from .enrichment.climate_coldwaves import (
    cw_active_days,
    cw_count_by_year,
    cw_get_events,
    sus_climate_compute_coldwaves,
)
from .enrichment.climate_fill import sus_climate_fill_inmet
from .enrichment.climate_heatwaves import (
    hw_active_days,
    hw_count_by_year,
    hw_get_events,
    sus_climate_compute_heatwaves,
)
from .enrichment.climate_indicators import sus_climate_compute_indicators
from .enrichment.fill_gaps import sus_fill_gaps
from .enrichment.mod_af import sus_mod_af
from .enrichment.mod_casecrossover import sus_mod_casecrossover
from .enrichment.mod_dlnm import sus_mod_dlnm
from .enrichment.mod_excess import sus_mod_excess
from .enrichment.mod_its import sus_mod_its
from .enrichment.mod_ml import sus_mod_ml, sus_mod_ml_predict
from .enrichment.mod_sensitivity import sus_mod_sensitivity
from .enrichment.mod_spatial_moran import sus_mod_spatial_moran
from .enrichment.mod_spatial_reg import sus_mod_spatial_reg
from .enrichment.mod_spatial_weights import sus_mod_spatial_weights
from .enrichment.mod_swot import sus_mod_swot
from .enrichment.mod_vulnerability_index import sus_mod_vulnerability_index
from .enrichment.spatial import sus_spatial_join
from .io.cache import sus_cache_clear, sus_cache_info
from .io.export import sus_export
from .io.materialize import materialize
from .io.read import sus_data_read
from .utils import update_climasus_data
from .utils.chat import sus_chat
from .utils.disease_groups import get_disease_group_details, list_disease_groups
from .utils.cid_select import sus_data_cid_select
from .utils.explore import sus_explore
from .utils.quality import sus_data_quality_report
from .utils.ts_quality import sus_data_ts_quality
from .viz.climate_plot import sus_climate_plot_fill
from .viz.plot_demographics import sus_data_plot_demographics
from .viz.plot_aggregate_ts import sus_data_plot_aggregate_ts
from .viz.plot_aggregate_map import sus_data_plot_aggregate_map


__all__ = [
    "__version__",
    # Pipeline
    "sus_pipeline",
    # Core
    "sus_data_import",
    "sus_data_clean_encoding",
    "sus_data_standardize",
    "sus_filter",
    "sus_data_create_variables",
    "sus_data_aggregate",
    "collect_arrow",
    "sus_sql",
    # I/O
    "sus_export",
    "sus_cache_info",
    "sus_cache_clear",
    "materialize",
    "sus_data_read",
    # Climate import
    "sus_climate_inmet",
    # NOT VERIFIED — could not be run on either R or Python: the UNIPLU-BR
    # download from Zenodo returns HTTP 403 (external source, not a code
    # error). Exported so it is reachable, but its behaviour is unknown.
    "sus_climate_uniplu",
    # Climate analytics (parity with climasus4r legacy)
    "sus_climate_aggregate",
    "sus_climate_compute_indicators",
    "sus_climate_fill_inmet",
    "sus_climate_plot_fill",
    # Climate normals & anomalies
    "sus_climate_normals",
    "sus_climate_normals_meta",
    "sus_climate_anomaly",
    # Heatwaves / coldwaves — results validated against climasus4r (103 and
    # 128 events, identical). BLOCKED in practice by M10: both require a
    # ``station_code`` column while ``sus_climate_inmet()`` emits
    # ``wmo_code``, and neither exposes a ``station_col`` argument. Rename
    # the column until M10 is resolved.
    "sus_climate_compute_heatwaves",
    "hw_get_events",
    "hw_active_days",
    "hw_count_by_year",
    "sus_climate_compute_coldwaves",
    "cw_get_events",
    "cw_active_days",
    "cw_count_by_year",
    # Enrichment
    "sus_climate",
    "sus_spatial_join",
    "sus_census",
    "sus_fill_gaps",
    "sus_climate_info",
    # Spatial modelling (sus_mod_*)
    "sus_mod_spatial_weights",
    "sus_mod_spatial_moran",
    "sus_mod_spatial_reg",
    # Exposure-response modelling — the sus_mod_dlnm() chain
    "sus_mod_dlnm",
    "sus_mod_af",
    "sus_mod_sensitivity",
    "sus_mod_casecrossover",
    "sus_mod_its",
    "sus_mod_excess",
    "sus_mod_ml",
    "sus_mod_ml_predict",
    "sus_mod_vulnerability_index",
    "sus_mod_swot",
    # Utilities
    "sus_explore",
    "sus_data_quality_report",
    "sus_data_ts_quality",
    "sus_chat",
    "update_climasus_data",
    # Metadata & disease groups (parity C)
    "sus_meta",
    "list_disease_groups",
    "get_disease_group_details",
    "sus_data_cid_select",
    # viz
    "sus_data_plot_demographics",
    "sus_data_plot_aggregate_ts",
    "sus_data_plot_aggregate_map",
]
