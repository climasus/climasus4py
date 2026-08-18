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
from .core.climate_normals import sus_climate_normals, sus_climate_normals_meta
from .core.climate_uniplu import sus_climate_uniplu
from .core.engine import collect_arrow
from .core.filter import sus_filter
from .core.importer import sus_data_import
from .core.meta import sus_meta
from .core.pipeline import sus_pipeline
from .core.standardize import sus_data_standardize
from .core.sus_sql import sus_sql
from .core.variables import sus_data_create_variables
from .enrichment.census import sus_census
from .enrichment.climate import sus_climate
from .enrichment.climate_aggregate import sus_climate_aggregate
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
from .enrichment.climate_spei import sus_climate_compute_spei
from .enrichment.climate_spi import sus_climate_compute_spi
from .enrichment.fill_gaps import sus_fill_gaps
from .enrichment.grid_chirps import sus_grid_chirps
from .enrichment.grid_era5 import sus_grid_era5
from .enrichment.grid_fires import sus_grid_fires
from .enrichment.grid_join import sus_grid_join
from .enrichment.grid_koppen import sus_grid_koppen
from .enrichment.grid_pdsi import sus_grid_pdsi
from .enrichment.grid_pollution_cams import sus_grid_pollution_cams
from .enrichment.grid_pollution_ghap import sus_grid_pollution_ghap
from .enrichment.grid_pollution_merra2 import sus_grid_pollution_merra2
from .enrichment.grid_prodes import sus_grid_prodes
from .enrichment.grid_smvi import sus_grid_smvi
from .enrichment.mod_af import sus_mod_af
from .enrichment.mod_burden import sus_mod_burden
from .enrichment.mod_casecrossover import CaseCrossoverResult, sus_mod_casecrossover
from .enrichment.mod_dlnm import sus_mod_dlnm
from .enrichment.mod_excess import sus_mod_excess
from .enrichment.mod_its import ClimasusITS, sus_mod_its
from .enrichment.mod_metaregression import sus_mod_metaregression
from .enrichment.mod_ml import sus_mod_ml, sus_mod_ml_predict
from .enrichment.mod_pool import sus_mod_pool
from .enrichment.mod_sensitivity import sus_mod_sensitivity
from .enrichment.mod_spacetime_bayes import sus_mod_spacetime_bayes
from .enrichment.mod_spacetime_exceedance import sus_mod_spacetime_exceedance
from .enrichment.mod_spacetime_predict import sus_mod_spacetime_predict
from .enrichment.mod_spatial_bayes import sus_mod_spatial_bayes
from .enrichment.mod_spatial_moran import sus_mod_spatial_moran
from .enrichment.mod_spatial_reg import sus_mod_spatial_reg
from .enrichment.mod_spatial_scan import sus_mod_spatial_scan
from .enrichment.mod_spatial_weights import sus_mod_spatial_weights
from .enrichment.mod_swot import sus_mod_swot
from .enrichment.mod_vulnerability_index import sus_mod_vulnerability_index
from .enrichment.socio_indicators import (
    sus_socio_compute_indicators,
    sus_socio_list_indicators,
)
from .enrichment.spatial import sus_spatial_join
from .io.cache import sus_cache_clear, sus_cache_info
from .io.export import sus_export
from .io.materialize import materialize
from .io.read import sus_data_read
from .utils import update_climasus_data
from .utils.census_select import sus_census_select
from .utils.chat import sus_chat
from .utils.cid_select import sus_data_cid_select
from .utils.disease_groups import get_disease_group_details, list_disease_groups
from .utils.explore import sus_explore
from .utils.quality import sus_data_quality_report
from .utils.ts_quality import sus_data_ts_quality
from .utils.welcome import sus_welcome
from .viz.climate_plot import sus_climate_plot_fill
from .viz.climate_plot_aggregate import sus_climate_plot_aggregate
from .viz.climate_plot_coldwaves import sus_climate_plot_coldwaves
from .viz.climate_plot_heatwaves import sus_climate_plot_heatwaves
from .viz.data_plot_aggregate_map import sus_data_plot_aggregate_map
from .viz.data_plot_aggregate_ts import sus_data_plot_aggregate_ts
from .viz.data_plot_demographics import sus_data_plot_demographics
from .viz.mod_plot_af import sus_mod_plot_af
from .viz.mod_plot_burden import sus_mod_plot_burden
from .viz.mod_plot_dlnm import sus_mod_plot_dlnm
from .viz.mod_plot_ml import sus_mod_plot_ml
from .viz.mod_plot_pool import sus_mod_plot_pool
from .viz.mod_plot_sensitivity import sus_mod_plot_sensitivity
from .viz.mod_plot_spacetime import sus_mod_plot_spacetime
from .viz.mod_plot_spatial_bayes import sus_mod_plot_spatial_bayes
from .viz.mod_plot_spatial_moran import sus_mod_plot_spatial_moran
from .viz.mod_plot_spatial_scan import sus_mod_plot_spatial_scan
from .viz.mod_plot_swot import sus_mod_plot_swot
from .viz.mod_plot_vulnerability import sus_mod_plot_vulnerability

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
    "sus_climate_normals",
    "sus_climate_normals_meta",
    "sus_climate_uniplu",
    # Climate analytics (parity with climasus4r legacy)
    "sus_climate_aggregate",
    "sus_climate_compute_indicators",
    "sus_climate_compute_spi",
    "sus_climate_compute_spei",
    "sus_climate_anomaly",
    "sus_climate_compute_heatwaves",
    "hw_get_events",
    "hw_count_by_year",
    "hw_active_days",
    "sus_climate_compute_coldwaves",
    "cw_get_events",
    "cw_count_by_year",
    "cw_active_days",
    "sus_climate_fill_inmet",
    "sus_climate_plot_fill",
    "sus_climate_plot_aggregate",
    "sus_climate_plot_heatwaves",
    "sus_climate_plot_coldwaves",
    # Enrichment
    "sus_climate",
    "sus_grid_join",
    "sus_grid_era5",
    "sus_grid_chirps",
    "sus_grid_fires",
    "sus_grid_pdsi",
    "sus_grid_koppen",
    "sus_grid_smvi",
    "sus_grid_prodes",
    "sus_grid_pollution_cams",
    "sus_grid_pollution_ghap",
    "sus_grid_pollution_merra2",
    "sus_spatial_join",
    "sus_census",
    "sus_census_select",
    "sus_fill_gaps",
    "sus_data_cid_select",
    "sus_socio_compute_indicators",
    "sus_socio_list_indicators",
    "sus_data_plot_aggregate_map",
    "sus_data_plot_aggregate_ts",
    "sus_data_plot_demographics",
    # Modeling (epidemiological / spatial statistics)
    "sus_mod_spatial_weights",
    "sus_mod_spatial_moran",
    "sus_mod_plot_spatial_moran",
    "sus_mod_spatial_reg",
    "sus_mod_casecrossover",
    "CaseCrossoverResult",
    "sus_mod_dlnm",
    "sus_mod_plot_dlnm",
    "sus_mod_af",
    "sus_mod_plot_af",
    "sus_mod_excess",
    "sus_mod_sensitivity",
    "sus_mod_plot_sensitivity",
    "sus_mod_swot",
    "sus_mod_plot_swot",
    "sus_mod_metaregression",
    "sus_mod_pool",
    "sus_mod_plot_pool",
    "sus_mod_spatial_scan",
    "sus_mod_plot_spatial_scan",
    "sus_mod_its",
    "ClimasusITS",
    "sus_mod_ml",
    "sus_mod_ml_predict",
    "sus_mod_burden",
    "sus_mod_plot_burden",
    "sus_mod_vulnerability_index",
    "sus_mod_plot_vulnerability",
    "sus_mod_plot_ml",
    "sus_mod_spacetime_bayes",
    "sus_mod_spatial_bayes",
    "sus_mod_spacetime_exceedance",
    "sus_mod_spacetime_predict",
    "sus_mod_plot_spacetime",
    "sus_mod_plot_spatial_bayes",
    # Utilities
    "sus_explore",
    "sus_data_quality_report",
    "sus_data_ts_quality",
    "sus_chat",
    "sus_welcome",
    "update_climasus_data",
    # Metadata & disease groups (parity C)
    "sus_meta",
    "list_disease_groups",
    "get_disease_group_details",
]
