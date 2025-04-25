import dagster as dg
from dagster_duckdb import DuckDBResource
from duckdb.duckdb import DuckDBPyConnection

from src.internal.core import emit_standard_df_metadata


# asset: waqi_chiangmai_airquality
@dg.asset(
    group_name="waqi",
    kinds={"duckdb"},
    deps={"waqi_chiangmai_airquality"},
)
def waqi_chiangmai_airquality(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.waqi_chiangmai_airquality AS (
            SELECT
              date,
              pm25,
              pm10,
              o3,
              no2,
              so2,
              co
            FROM public.waqi_chiangmai_airquality
            ORDER BY date
        );
        """)
        df = conn.sql("SELECT * FROM public.waqi_chiangmai_airquality LIMIT 10").pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.waqi_chiangmai_airquality"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))
