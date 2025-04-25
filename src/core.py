from datetime import date

import polars as pl
from dagster import (
    MetadataValue,
    TableColumn,
    TableRecord,
    TableSchema,
)
from dagster_gcp.gcs import GCSResource
from google.cloud import storage


# function for data fetching from gcs
def get_csv_from_gcs_datasets(path: str, gcs: GCSResource) -> bytes:
    client: storage.Client = gcs.get_client()
    bucket = client.bucket("analytics-repo-datasets")
    blob = bucket.blob(path)
    return blob.download_as_bytes()


# function to set all columns as nullable strings, keeping empty cells as None
def columns_as_nullable_strings(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        **{
            name: pl.when(pl.col(name).is_not_null())
            .then(pl.col(name).cast(pl.Utf8))
            .otherwise(None)
            .alias(name)
            for name in df.columns
        }
    )


# function for creating metadata for dagster ui to show row count and preview of first 10 rows
def emit_standard_df_metadata(
    df: pl.DataFrame, preview_limit: int = 10, row_count: int = None
) -> dict:
    return {
        "dagster/row_count": row_count or len(df),
        "preview": MetadataValue.table(
            [
                TableRecord(
                    {k: str(v) if isinstance(v, date) else v for k, v in row.items()}
                )
                for row in df.head(preview_limit).iter_rows(named=True)
            ],
            schema=TableSchema(
                [
                    TableColumn(name, dtype.to_python().__name__)
                    for name, dtype in df.schema.items()
                ]
            ),
        ),
    }
