import re

import dagster as dg
import gcsfs
import polars as pl
from dagster import MetadataValue
from httpx import AsyncClient

from src.core import get_clean_csv_file
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
    kinds={"gcs", "polars"},
    deps={"nasa_firms_api"},
    io_manager_key=IOManager.DUCKDB.value,
)
def nasa_firms_raw(context: dg.AssetExecutionContext) -> pl.DataFrame:
    gcs_path = "analytics-repo-datasets/dagster/nasa_firms_api"
    fs = gcsfs.GCSFileSystem()
    file_list = fs.ls(gcs_path)

    dfs = []
    for file in file_list:
        try:
            # get clean csv files from gcs (nasa_firm_api produces text files)
            clean_file = get_clean_csv_file(fs, file)
            if clean_file is None:
                print(f"Skipping file with no valid CSV header: {file}")
                continue
            df = pl.read_csv(clean_file)

            # create new column with date extracted from filename
            match = re.search(r"(\d{4}-\d{2}-\d{2})", file)
            if match:
                measurement_date = match.group(1)
                df = df.with_columns(
                    measurement_date=pl.lit(measurement_date).cast(pl.Date())
                )
            else:
                print(f"Warning: No date found in file name: {file}")

            dfs.append(df)

        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not dfs:
        raise ValueError(
            "No valid df created."
        )

    combined_df = pl.concat(dfs, how="vertical")
    combined_df = combined_df.sort("measurement_date")

    return combined_df
