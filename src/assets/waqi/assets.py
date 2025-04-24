import io

import dagster as dg
import polars as pl
from dagster import MetadataValue
from httpx import AsyncClient

from src.partitions import daily_partitions_def
from src.resources import IOManager
from src.settings import settings


# asset # 1: waqi_raw
# fetches raw data from WAQI API
@dg.asset(
    partitions_def=daily_partitions_def,
    io_manager_key=IOManager.GCS.value,
    kinds={"gcs"},
)
async def waqi_raw(context: dg.AssetExecutionContext) -> str:
    async with AsyncClient(base_url=settings.WAQI_BASE_URL) as client:
        res = await client.get(f"/api/feed/chiang-mai/?token={settings.WAQI_TOKEN}")
        res.raise_for_status()
        text = res.text

    context.add_output_metadata({"size": MetadataValue.int(len(text))})
    return text


# asset #2: waqi_raw_delta
# transforms raw data into  a polars dataframe
@dg.asset(
    partitions_def=daily_partitions_def,
    io_manager_key=IOManager.DELTALAKE.value,
    metadata={
        "partition_expr": "measurement_date",
    },
    kinds={"gcs", "polars", "deltalake"},
)
def waqi_raw_delta(
    context: dg.AssetExecutionContext,
    waqi_raw: str,
) -> pl.DataFrame:
    with io.StringIO(waqi_raw) as buf:
        df = pl.read_json(buf)

    df = df.with_columns(measurement_date=pl.lit(context.partition_key).cast(pl.Date()))
    return df
