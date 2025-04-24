import dagster as dg
from dagster_duckdb import DuckDBResource
from duckdb.duckdb import DuckDBPyConnection

from src.internal.core import emit_standard_df_metadata


# asset # 1: project_cchain_climate_atmosphere_location
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"project_cchain_climate_atmosphere", "project_cchain_location"},
)
def project_cchain_climate_atmosphere_location(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_climate_atmosphere_location AS (
            SELECT
              ca.uuid,
              ca.date,
              l.adm1_en,
              l.adm1_pcode,
              l.adm2_en,
              l.adm2_pcode,
              l.adm3_en,
              l.adm3_pcode,
              l.adm4_en,
              l.adm4_pcode,
              ca.tave,
              ca.tmin,
              ca.tmax,
              ca.heat_index,
              ca.pr,
              ca.wind_speed,
              ca.rh,
              ca.solar_rad,
              ca.uv_rad
            FROM public.project_cchain_climate_atmosphere ca
            LEFT JOIN public.project_cchain_location l USING (adm4_pcode)
            ORDER BY date, adm4_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_climate_atmosphere_location LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_climate_atmosphere_location"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))


# asset # 2: project_cchain_disease_pidsr_totals_location
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"project_cchain_disease_pidsr_totals", "project_cchain_location"},
)
def project_cchain__disease_pidsr_totals_location(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_disease_pidsr_totals_location AS (
            SELECT
              dpt.uuid,
              dpt.date,
              l.adm1_en,
              l.adm1_pcode,
              l.adm2_en,
              l.adm2_pcode,
              l.adm3_en,
              l.adm3_pcode,
              dpt.disease_icd10_code,
              dpt.disease_common_name,
              dpt.case_total
            FROM public.project_cchain_disease_pidsr_totals dpt
            LEFT JOIN public.project_cchain_location l USING (adm3_pcode)
            ORDER BY date, adm3_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_disease_pidsr_totals_location LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_disease_pidsr_totals_location"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))
