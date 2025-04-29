# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.0
#   kernelspec:
#     display_name: sandbox
#     language: python
#     name: python3
# ---

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Experimental Notebook
# This notebook serves as a dump for experimentation for tables in DuckDB.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Import packages

# %% editable=true slideshow={"slide_type": ""}
# install duckdb package
import duckdb

# establish connection to duckdb database
conn = duckdb.connect("../data/lake/database.duckdb")

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## List available tables

# %% editable=true slideshow={"slide_type": ""}
# show all available schemas and tables
conn.sql(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name;
"""
).pl()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## NASA FIRMS

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn.sql(
    """
    SELECT *
    FROM public.nasa_firms_raw
    LIMIT 50;
    """
).pl()

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn.sql(
    """
    SELECT
        CAST(____S______X_S__country_id AS VARCHAR) AS country_id,
        CAST(latitude AS FLOAT) AS latitude,
        CAST(longitude AS FLOAT) AS longitude,
        ST_GEOMFROMTEXT(CONCAT('POINT(', longitude, ' ', latitude, ')')) AS geometry,
        CAST(bright_ti4 AS FLOAT) AS bright_ti4,
        CAST(scan AS FLOAT) AS scan,
        CAST(track AS FLOAT) AS track,
        CAST(acq_date AS DATE) AS acq_date,
        CAST(acq_time AS TIME) AS acq_time,
        CAST(acq_date AS DATE) + CAST(acq_time AS TIME) AS acq_datetime,
        CAST(satellite AS VARCHAR) AS satellite,
        CAST(instrument AS VARCHAR) AS instrument,
        CAST(confidence AS VARCHAR) AS confidence,
        CAST(version AS VARCHAR) AS version,
        CAST(bright_ti5 AS FLOAT) AS bright_ti5,
        CAST(frp AS FLOAT) AS frp,
        CAST(daynight AS VARCHAR) AS daynight,
        CAST(measurement_date AS DATE) AS measurement_date
    FROM public.nasa_firms_raw
    ORDER BY acq_datetime;
    """
).pl()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## WAQI Air Quality

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_raw
    ORDER BY date
    LIMIT 50;
    """
).pl()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Project CCHAIN

# %% editable=true slideshow={"slide_type": ""}
# sample query
project_cchain_climate_atmosphere_raw = conn.sql(
    """
    SELECT *
    FROM public.project_cchain_climate_atmosphere_raw
    ORDER BY date
    LIMIT 50;
    """
).pl()

# %%
# sample query
conn.sql(
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

# %%
conn.sql(
    """
    SELECT *
    FROM public.project_cchain_climate_atmosphere_raw
    WHERE pr != 0
    LIMIT 50
    """
).pl()

# %%
# sample query
conn.sql(
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

# %%
