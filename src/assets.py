import os
from pathlib import Path

import polars as pl
from dagster import AssetIn, Config, asset
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

# Load environment variables
load_dotenv()


class GCSTableConfig(Config):
    """Configuration for a table in GCS"""

    bucket_name: str
    table_path: str  # Path to the file in GCS (e.g. 'data/raw/table1.parquet')


def get_storage_client() -> storage.Client:
    """Get an authenticated GCS client"""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS must be set in .env")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return storage.Client(credentials=credentials)


@asset
def precipitation_data(config: GCSTableConfig) -> pl.DataFrame:
    """Load precipitation data from GCS into a Polars DataFrame"""
    client = get_storage_client()
    bucket = client.bucket(config.bucket_name)
    blob = bucket.blob(config.table_path)

    # Create local directory if it doesn't exist
    local_dir = Path("data/raw")
    local_dir.mkdir(parents=True, exist_ok=True)

    # Download to local file
    local_path = local_dir / Path(config.table_path).name
    blob.download_to_filename(str(local_path))

    # Read based on file extension
    if local_path.suffix == ".parquet":
        return pl.read_parquet(local_path)
    elif local_path.suffix == ".csv":
        return pl.read_csv(local_path)
    else:
        raise ValueError(f"Unsupported file type: {local_path.suffix}")


@asset
def dengue_cases(config: GCSTableConfig) -> pl.DataFrame:
    """Load dengue cases data from GCS into a Polars DataFrame"""
    client = get_storage_client()
    bucket = client.bucket(config.bucket_name)
    blob = bucket.blob(config.table_path)

    # Create local directory if it doesn't exist
    local_dir = Path("data/raw")
    local_dir.mkdir(parents=True, exist_ok=True)

    # Download to local file
    local_path = local_dir / Path(config.table_path).name
    blob.download_to_filename(str(local_path))

    # Read based on file extension
    if local_path.suffix == ".parquet":
        return pl.read_parquet(local_path)
    elif local_path.suffix == ".csv":
        return pl.read_csv(local_path)
    else:
        raise ValueError(f"Unsupported file type: {local_path.suffix}")


@asset(ins={"precip": AssetIn("precipitation_data"), "dengue": AssetIn("dengue_cases")})
def combined_analysis(precip: pl.DataFrame, dengue: pl.DataFrame) -> pl.DataFrame:
    """
    Combine precipitation and dengue data for analysis.
    This is a placeholder - modify the processing logic as needed.
    """
    # This is just an example - modify according to your needs
    return pl.concat(
        [
            precip.with_columns(pl.lit("precipitation").alias("data_type")),
            dengue.with_columns(pl.lit("dengue").alias("data_type")),
        ]
    )


# Keep the hello world asset for testing
@asset
def hello_world():
    """A simple asset that returns a greeting."""
    return "Hello, World!"
