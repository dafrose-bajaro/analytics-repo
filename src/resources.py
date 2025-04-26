from enum import Enum

# from dagster_deltalake import GcsConfig, WriteMode
# from dagster_deltalake.config import ClientConfig
# from dagster_deltalake_polars import DeltaLakePolarsIOManager
from dagster_duckdb import DuckDBResource
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_gcp.gcs import GCSPickleIOManager, GCSResource

from src.settings import settings


# storage and processing tools
class IOManager(Enum):
    DUCKDB = "duckdb_io_manager"
    # DELTALAKE = "deltalake_io_manager"
    GCS = "gcs_io_manager"


class Resource(Enum):
    DUCKDB = "duckdb"
    GCS = "gcs"


# gcp configuration
_gcs_resource = GCSResource(
    project=settings.GCP_PROJECT,
    service_account_key=settings.GCP_ACCESS_KEY,
    endpoint_url=settings.GCP_ENDPOINT,
    region=settings.GCP_REGION,
)

# main resources dictionary
RESOURCES = {
    Resource.DUCKDB.value: DuckDBResource(database=settings.DUCKDB_DATABASE),
    Resource.GCS.value: _gcs_resource,
    IOManager.DUCKDB.value: DuckDBPolarsIOManager(database=settings.DUCKDB_DATABASE),
    # IOManager.DELTALAKE.value: DeltaLakePolarsIOManager(
    #    root_uri=f"gs://{settings.GCP_BUCKET}",
    #    mode=WriteMode.append,
    #    storage_options=GcsConfig(
    #        project=settings.GCP_PROJECT,
    #        service_account_key=settings.GCP_ACCESS_KEY,
    #        bucket=settings.GCP_BUCKET,
    #        region=settings.GCP_REGION,
    #    ),
    #    client_options=ClientConfig(allow_http=True),
    # ),
    IOManager.GCS.value: GCSPickleIOManager(
        gcs=_gcs_resource,
        gcs_bucket=settings.GCP_BUCKET,
        prefix="dagster-io",
    ),
}
