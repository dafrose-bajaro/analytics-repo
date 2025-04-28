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

# list of cchain datasets
CCHAIN_DATASETS = [
    "climate_atmosphere",
    "disease_pidsr_totals",
    "location",
]


# raw assets: job to create assets from raw project cchain data
def create_raw_cchain_assets(filename: str):
    @dg.asset(
        name=f"{filename}_raw",
        group_name="project_cchain",
        kinds={"gcs", "polars", "duckdb"},
        io_manager_key=IOManager.DUCKDB.value,
    )
    def etl(context: dg.AssetExecutionContext, gcs: GCSResource) -> pl.DataFrame:
        csv = get_csv_from_gcs_datasets(path=f"project-cchain/{filename}.csv", gcs=gcs)
        df = pl.read_csv(csv)
        df = columns_as_nullable_strings(df)
        context.add_output_metadata(emit_standard_df_metadata(df))
        return df

    return dg.Definitions(
        assets=[etl],
        resources=RESOURCES,
    )


###### CLIMATE ATMOSPHERE ######


# asset 1: project_cchain_climate_atmosphere_raw
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"climate_atmosphere_raw", "location_raw"},
)
def project_cchain_climate_atmosphere_raw(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_climate_atmosphere_raw AS (
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
            FROM public.climate_atmosphere_raw ca
            LEFT JOIN public.location_raw l USING (adm4_pcode)
            ORDER BY date, adm4_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_climate_atmosphere_raw LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_climate_atmosphere_raw"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))


# asset 2: project_cchain_climate_atmosphere_clean
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"project_cchain_climate_atmosphere_raw"},
)
def project_cchain_climate_atmosphere_clean(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_climate_atmosphere_clean AS (
            SELECT
                CAST(uuid AS VARCHAR) AS uuid,
                CAST(date AS DATE) AS date,
                CAST(adm1_en AS VARCHAR) AS adm1_en,
                CAST(adm1_pcode AS VARCHAR) AS adm1_pcode,
                CAST(adm2_en AS VARCHAR) AS adm2_en,
                CAST(adm2_pcode AS VARCHAR) AS adm2_pcode,
                CAST(adm3_en AS VARCHAR) AS adm3_en,
                CAST(adm3_pcode AS VARCHAR) AS adm3_pcode,
                CAST(adm4_en AS VARCHAR) AS adm4_en,
                CAST(adm4_pcode AS VARCHAR) AS adm4_pcode,
                COALESCE(CAST(tave AS FLOAT), 0) AS tave,
                COALESCE(CAST(tmin AS FLOAT), 0) AS tmin,
                COALESCE(CAST(tmax AS FLOAT), 0) AS tmax,
                COALESCE(CAST(heat_index AS FLOAT), 0) AS heat_index,
                COALESCE(CAST(pr AS FLOAT), 0) AS pr,
                COALESCE(CAST(wind_speed AS FLOAT), 0) AS wind_speed,
                COALESCE(CAST(rh AS FLOAT), 0) AS rh,
                COALESCE(CAST(solar_rad AS FLOAT), 0) AS solar_rad,
                COALESCE(CAST(uv_rad AS FLOAT), 0) AS uv_rad
            FROM public.project_cchain_climate_atmosphere_raw
            ORDER BY date, adm4_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_climate_atmosphere_clean LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_climate_atmosphere_clean"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))


###### DISEASE PIDSR TOTALS ######


# asset 1: project_cchain_disease_pidsr_totals_raw
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"disease_pidsr_totals_raw", "location_raw"},
)
def project_cchain_disease_pidsr_totals_raw(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_disease_pidsr_totals_raw AS (
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
            FROM public.disease_pidsr_totals_raw dpt
            LEFT JOIN public.location_raw l USING (adm3_pcode)
            ORDER BY date, adm3_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_disease_pidsr_totals_raw LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_disease_pidsr_totals_raw"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))


# asset 2: project_cchain_disease_pidsr_totals_clean
@dg.asset(
    group_name="project_cchain",
    kinds={"duckdb"},
    deps={"project_cchain_disease_pidsr_totals_raw"},
)
def project_cchain_disease_pidsr_totals_clean(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
):
    conn: DuckDBPyConnection
    with duckdb.get_connection() as conn:
        conn.sql("""
        CREATE OR REPLACE VIEW public.project_cchain_disease_pidsr_totals_clean AS (
            SELECT
                CAST(uuid AS VARCHAR) AS uuid,
                CAST(date AS DATE) AS date,
                CAST(adm1_en AS VARCHAR) AS adm1_en,
                CAST(adm1_pcode AS VARCHAR) AS adm1_pcode,
                CAST(adm2_en AS VARCHAR) AS adm2_en,
                CAST(adm2_pcode AS VARCHAR) AS adm2_pcode,
                CAST(adm3_en AS VARCHAR) AS adm3_en,
                CAST(adm3_pcode AS VARCHAR) AS adm3_pcode,
                CAST(disease_icd10_code AS VARCHAR) AS disease_icd10_code,
                CAST(disease_common_name AS VARCHAR) AS disease_common_name,
                CAST(case_total AS INT) AS case_total
            FROM public.project_cchain_disease_pidsr_totals_raw
            ORDER BY date, adm3_pcode
        );
        """)
        df = conn.sql(
            "SELECT * FROM public.project_cchain_disease_pidsr_totals_clean LIMIT 10"
        ).pl()
        count = conn.sql(
            "SELECT COUNT(*) AS count FROM public.project_cchain_disease_pidsr_totals_clean"
        ).pl()["count"][0]

    context.add_output_metadata(emit_standard_df_metadata(df, row_count=count))
