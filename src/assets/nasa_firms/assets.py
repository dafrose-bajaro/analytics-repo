import io

import dagster as dg
import polars as pl
from dagster import MetadataValue
from httpx import AsyncClient

from src.partitions import daily_partitions_def
from src.resources import IOManager
from src.settings import settings


# asset 1: nasa_firms_api
# fetches raw data from NASA FIRMS API
@dg.asset(
    name="nasa_firms_api",
    partitions_def=daily_partitions_def,
    io_manager_key=IOManager.GCS.value,
    kinds={"gcs"},
)
async def nasa_firms_api(context: dg.AssetExecutionContext) -> str:
    async with AsyncClient(base_url=settings.NASA_FIRMS_BASE_URL) as client:
        res = await client.get(
            f"/api/country/csv/{settings.NASA_FIRMS_MAP_KEY}/VIIRS_SNPP_NRT/THA/1/{context.partition_key}"
        )
        res.raise_for_status()
        text = res.text

    context.add_output_metadata({"size": MetadataValue.int(len(text))})
    return text


# asset #2: nasa_firms_raw
# transforms raw data into a polars dataframe
@dg.asset(
    partitions_def=daily_partitions_def,
    # io_manager_key=IOManager.DELTALAKE.value,
    metadata={
        "partition_expr": "measurement_date",
    },
    kinds={"gcs", "polars"},
)
def nasa_firms_raw(
    context: dg.AssetExecutionContext,
    nasa_firms_api: str,
) -> pl.DataFrame:
    with io.StringIO(nasa_firms_api) as buf:
        df = pl.read_csv(buf)

    df = df.with_columns(measurement_date=pl.lit(context.partition_key).cast(pl.Date()))
    return df
