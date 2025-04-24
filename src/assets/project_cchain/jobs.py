import dagster as dg
import polars as pl
from dagster_gcp.gcs import GCSResource

from src.internal.core import (
    emit_standard_df_metadata,
    get_csv_from_gcs_datasets,
    schema_transforms,
)
from src.resources import IOManager, resources
from src.schemas.climate_atmosphere import ClimateAtmosphere
from src.schemas.disease_pidsr_totals import DiseasePidsrTotals
from src.schemas.location import Location

# list of cchain datasets
CCHAIN_DATASETS = [
    ("climate_atmosphere", ClimateAtmosphere),
    ("disease_pidsr_totals", DiseasePidsrTotals),
    ("location", Location),
]


# function for building cchain job
def build_project_cchain_job(filename: str, schema: pl.Struct):
    @dg.asset(
        name=f"project_cchain_{filename}",
        group_name="project_cchain",
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
        resources=resources,
    )
