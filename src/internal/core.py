from datetime import date
from functools import reduce

import polars as pl

# dagster core- asset management and data handling
from dagster import (
    AssetExecutionContext,
    MetadataValue,
    TableColumn,
    TableRecord,
    TableSchema,
)

# google cloud storage
from dagster_gcp.gcs import GCSResource
from google.cloud import storage

from src import schemas


# function for data fetching from gcs
def get_csv_from_gcs_datasets(path: str, gcs: GCSResource) -> bytes:
    client: storage.Client = gcs.get_client()
    bucket = client.bucket("project-cchain")
    blob = bucket.blob(path)
    return blob.download_as_bytes()


# function for schema type casting
def cast_schema_types(df: pl.DataFrame, schema: pl.Struct) -> pl.DataFrame:
    return df.with_columns(
        **{name: pl.col(name).cast(dtype) for name, dtype in schema.to_schema().items()}
    )


# function for dropping columns not in schema
def drop_columns_not_in_schema(df: pl.DataFrame, schema: pl.Struct) -> pl.DataFrame:
    return df.select(schema.to_schema().keys())


# function for adding missing columns to dataframe (fill with NULL values)
def add_missing_columns(df: pl.DataFrame, schema: pl.Struct) -> pl.DataFrame:
    return df.with_columns(
        **{
            name: pl.lit(None).cast(dtype)
            for name, dtype in schema.to_schema().items()
            if name not in df.columns
        }
    )


# function for schema transformations
def schema_transforms(df: pl.DataFrame, schema: pl.Struct) -> pl.DataFrame:
    return reduce(
        lambda x, f: f(x, schema),
        [
            drop_columns_not_in_schema,
            add_missing_columns,
            cast_schema_types,
        ],
        df,
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


# function for extracting schema from partition key
# note: partition_key comes from the context in which the asset is being executed in dagster
def extract_schema_from_partition_key(context: AssetExecutionContext) -> pl.Struct:
    schema_class_name = "".join(
        [
            "".join([s.upper() if i == 0 else s for i, s in enumerate(split)])
            for split in context.partition_key.split("_")
        ]
    )

    if not hasattr(schemas, schema_class_name):
        error = f"No such schema src.schemas.{schema_class_name}"
        context.log.error(error)
        raise ModuleNotFoundError(error)

    return getattr(schemas, schema_class_name)
