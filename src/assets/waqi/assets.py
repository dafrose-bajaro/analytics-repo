import dagster as dg
import polars as pl
from dagster_gcp.gcs import GCSResource

from src.core import (
    columns_as_nullable_strings,
    emit_standard_df_metadata,
    get_csv_from_gcs_datasets,
)
from src.resources import RESOURCES, IOManager


# asset 1: waqi_airquality_raw
@dg.asset(
    name="waqi_airquality_raw",
    group_name="waqi",
    kinds={"gcs", "polars", "duckdb"},
    io_manager_key=IOManager.DUCKDB.value,
)
def etl(context: dg.AssetExecutionContext, gcs: GCSResource) -> pl.DataFrame:
    filename = "chiangmai_airquality"
    csv = get_csv_from_gcs_datasets(path=f"waqi/{filename}.csv", gcs=gcs)
    df = pl.read_csv(csv)
    df = columns_as_nullable_strings(df)
    context.add_output_metadata(emit_standard_df_metadata(df))
    return df


definitions = dg.Definitions(assets=[etl], resources=RESOURCES)
