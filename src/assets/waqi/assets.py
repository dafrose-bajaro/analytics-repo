import dagster as dg
import polars as pl
from dagster_duckdb import DuckDBResource
from dagster_gcp.gcs import GCSResource
from duckdb.duckdb import DuckDBPyConnection

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


# asset 2: waqi_airquality_clean
@dg.asset(
    name="waqi_airquality_clean",
    group_name="waqi",
    kinds={"duckdb"},
    deps={"waqi_airquality_raw"},
)
def waqi_airquality_clean(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.waqi_airquality_clean AS (
            SELECT
                CAST(STRPTIME(date, '%d-%b-%y') AS DATE) AS date,
                CAST(NULLIF(TRIM(pm25), '') AS FLOAT) AS pm25,
                CAST(NULLIF(TRIM(pm10), '') AS FLOAT) AS pm10,
                CAST(NULLIF(TRIM(o3), '') AS FLOAT) AS o3,
                CAST(NULLIF(TRIM(no2), '') AS FLOAT) AS no2,
                CAST(NULLIF(TRIM(so2), '') AS FLOAT) AS so2,
                CAST(NULLIF(TRIM(co), '') AS FLOAT) AS co
            FROM public.waqi_airquality_raw
            ORDER BY date
        );
        """)
        df = conn.sql("SELECT * FROM public.waqi_airquality_clean LIMIT 10").pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.waqi_airquality_clean"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))
