import re

import dagster as dg
import duckdb
import gcsfs
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
    kinds={"gcs", "polars"},
    deps={"nasa_firms_api"},
)
def nasa_firms_raw(
    context: dg.AssetExecutionContext,
) -> pl.DataFrame:
    gcs_path = "analytics-repo-datasets/dagster/nasa_firms_api"
    fs = gcsfs.GCSFileSystem()
    file_list = fs.ls(gcs_path)

    dfs = []
    for file in file_list:
        with fs.open(file, "r") as f:
            df = pl.read_csv(f)

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

    combined_df = pl.concat(dfs, how="vertical")
    combined_df = combined_df.sort("measurement_date")

    # remove non-ASCII and problematic characters from column names
    def clean_col(col):
        return re.sub(r"[^a-zA-Z0-9_]", "_", col)

    cleaned_columns = [clean_col(col) for col in combined_df.columns]
    if len(set(cleaned_columns)) != len(cleaned_columns):
        raise ValueError("Duplicate column names found after cleaning!")
    if any(col == "" for col in cleaned_columns):
        raise ValueError("Empty column name found after cleaning!")
    combined_df.columns = cleaned_columns

    # write to duckdb
    conn = duckdb.connect(settings.DUCKDB_DATABASE)
    conn.execute("CREATE SCHEMA IF NOT EXISTS public;")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS public.nasa_firms_raw AS SELECT * FROM combined_df LIMIT 0"
    )
    conn.execute("DELETE FROM public.nasa_firms_raw")
    conn.register("combined_df", combined_df.to_arrow())
    conn.execute("INSERT INTO public.nasa_firms_raw SELECT * FROM combined_df")
    conn.close()

    return combined_df
