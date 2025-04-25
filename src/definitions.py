from dagster import Definitions, load_assets_from_package_module

from src.assets import nasa_firms, project_cchain, waqi
from src.assets.project_cchain.assets import CCHAIN_DATASETS, create_raw_cchain_assets
from src.resources import RESOURCES

raw_cchain_assets = [create_raw_cchain_assets(name) for name in CCHAIN_DATASETS]

defs = Definitions.merge(
    *raw_cchain_assets,
    Definitions(
        assets=[
            *load_assets_from_package_module(project_cchain, "project_cchain"),
            *load_assets_from_package_module(nasa_firms, "nasa_firms"),
            *load_assets_from_package_module(waqi, "waqi"),
        ],
        resources=RESOURCES,
    ),
)
