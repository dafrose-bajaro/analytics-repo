import re

import dagster as dg
import gcsfs
import polars as pl
from dagster import MetadataValue
from dagster_duckdb import DuckDBResource
from duckdb.duckdb import DuckDBPyConnection
from httpx import AsyncClient

from src.core import emit_standard_df_metadata, get_clean_csv_file
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
    group_name="nasa_firms",
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
        raise ValueError("No valid df created.")

    combined_df = pl.concat(dfs, how="vertical")
    combined_df = combined_df.sort("measurement_date")

    return combined_df


# asset #2: nasa_firms_clean
# sets proper schema for the dataframe
@dg.asset(group_name="nasa_firms", kinds={"duckdb"}, deps={"nasa_firms_raw"})
def nasa_firms_clean(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.nasa_firms_clean AS (
            SELECT
                CAST(____S______X_S__country_id AS VARCHAR) AS country_id,
                CAST(latitude AS FLOAT) AS latitude,
                CAST(longitude AS FLOAT) AS longitude,
                CONCAT('POINT (', longitude, ' ', latitude,')') AS geometry,
                CAST(bright_ti4 AS FLOAT) AS bright_ti4,
                CAST(scan AS FLOAT) AS scan,
                CAST(track AS FLOAT) AS track,
                STRPTIME(CONCAT(acq_date, ' ',
                    CASE
                        WHEN LENGTH(acq_time) = 4 THEN CONCAT(SUBSTR(acq_time, 1, 2), ':', SUBSTR(acq_time, 3, 2), ':00')
                        WHEN LENGTH(acq_time) = 3 THEN CONCAT('0', SUBSTR(acq_time, 1, 1), ':', SUBSTR(acq_time, 2, 2), ':00')
                        WHEN LENGTH(acq_time) = 2 THEN CONCAT('00:', acq_time, ':00')
                        ELSE NULL
                    END),
                    '%Y-%m-%d %H:%M:%S'
                ) AS acq_datetime,
                CAST(satellite AS VARCHAR) AS satellite,
                CAST(instrument AS VARCHAR) AS instrument,
                CAST(confidence AS VARCHAR) AS confidence,
                CAST(version AS VARCHAR) AS version,
                CAST(bright_ti5 AS FLOAT) AS bright_ti5,
                CAST(frp AS FLOAT) AS frp,
                CAST(daynight AS VARCHAR) AS daynight,
                CAST(measurement_date AS DATE) AS measurement_date
            FROM public.nasa_firms_raw
            ORDER BY acq_datetime
        );
        """)
        df = conn.sql("SELECT * FROM public.nasa_firms_clean LIMIT 10").pl()
        count = conn.sql("SELECT COUNT(*) AS count FROM public.nasa_firms_clean").pl()[
            "count"
        ][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))
