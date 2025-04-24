from dagster import Definitions, load_assets_from_package_module

from src.assets import nasa_firms, project_cchain, waqi
from src.assets.project_cchain.jobs import CCHAIN_DATASETS, build_project_cchain_job
from src.resources import resources

defs = Definitions.merge(
    # create jobs for CCHAIN datasets
    *[build_project_cchain_job(name, schema) for name, schema in CCHAIN_DATASETS],
    # create assets for project_cchain, nasa_firms, and aqicn
    Definitions(
        assets=[
            *load_assets_from_package_module(project_cchain, "project_cchain"),
            *load_assets_from_package_module(nasa_firms, "nasa_firms"),
            *load_assets_from_package_module(waqi, "waqi"),
        ]
    ),
    resources=resources,
)
