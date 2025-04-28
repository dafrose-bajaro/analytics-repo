# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# install duckdb package
import duckdb

conn = duckdb.connect("../data/lake/database.duckdb")

# %%
# show all available schemas and tables
tables_list = conn.sql(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name;
"""
).pl()
tables_list

# %% editable=true slideshow={"slide_type": ""}
# sample query
waqi_airquality_raw = conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_raw
    ORDER BY date
    LIMIT 50;
    """
).pl()
waqi_airquality_raw

# %%
# sample query
nasa_firms_raw = conn.sql(
    """
    SELECT *
    FROM public.nasa_firms_raw
    LIMIT 50;
    """
).pl()
nasa_firms_raw

# %%
# sample query
conn = duckdb.connect("../data/lake/database.duckdb")
project_cchain_climate_atmosphere_raw = conn.sql(
    """
    SELECT *
    FROM public.project_cchain_climate_atmosphere_raw
    ORDER BY date
    LIMIT 50;
    """
).pl()
project_cchain_climate_atmosphere_raw

# %%
# sample query
project_cchain_climate_atmosphere_clean = conn.sql(
    """
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
    ORDER BY pr ASC
    LIMIT 50;
    """
).pl()
project_cchain_climate_atmosphere_clean

# %%
project_cchain_climate_atmospher_filter = conn.sql(
    """
    SELECT *
    FROM public.project_cchain_climate_atmosphere_raw
    WHERE pr != 0
    LIMIT 50
    """
).pl()
project_cchain_climate_atmosphere_filter

# %%
# sample query
project_cchain_disease_pidsr_totals_clean = conn.sql(
    """
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
    WHERE case_total != 0
    ORDER BY date, adm3_pcode    
    """
).pl()
project_cchain_disease_pidsr_totals_clean

# %%
