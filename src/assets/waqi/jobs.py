import dagster as dg
import polars as pl
from dagster_gcp.gcs import GCSResource

from src.internal.core import (
    emit_standard_df_metadata,
    get_csv_from_gcs_datasets,
    schema_transforms,
)
from src.resources import RESOURCES, IOManager
from src.schemas.chiangmai_airquality import ChiangMaiAirQuality

# list of cchain datasets
WAQI_DATASETS = [
    ("chiangmai_airquality", ChiangMaiAirQuality),
]


# function for building waqi job
def build_waqi_job(filename: str, schema: pl.Struct):
    @dg.asset(
        name=f"waqi_{filename}",
        group_name="waqi",
        kinds={"gcs", "polars", "duckdb"},
        io_manager_key=IOManager.DUCKDB.value,
    )
    def etl(context: dg.AssetExecutionContext, gcs: GCSResource) -> pl.DataFrame:
        csv = get_csv_from_gcs_datasets(path=f"{filename}.csv", gcs=gcs)
        df = pl.read_csv(csv)
        df = schema_transforms(df, schema)
        context.add_output_metadata(emit_standard_df_metadata(df))
        return df

    return dg.Definitions(
        assets=[etl],
        resources=RESOURCES,
    )
