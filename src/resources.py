from enum import Enum

from dagster_duckdb import DuckDBResource
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource

from src.settings import settings


# storage and processing tools
class IOManager(Enum):
    DUCKDB = "duckdb_io_manager"
    GCS = "gcs_io_manager"


class Resource(Enum):
    DUCKDB = "duckdb"
    GCS = "gcs"


# gcp configuration
_gcs_resource = GCSResource(
    project=settings.GCP_PROJECT,
    endpoint_url=settings.GCP_ENDPOINT,
    region=settings.GCP_REGION,
)

# main resources dictionary
RESOURCES = {
    Resource.DUCKDB.value: DuckDBResource(database=settings.DUCKDB_DATABASE),
    Resource.GCS.value: _gcs_resource,
    IOManager.DUCKDB.value: DuckDBPolarsIOManager(database=settings.DUCKDB_DATABASE),
    IOManager.GCS.value: GCSPickleIOManager(
        gcs=_gcs_resource,
        gcs_bucket=settings.GCP_BUCKET,
        prefix="dagster-io",
    ),
}
