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

# %% [markdown]
# # Exploratory Data Analysis
#
# This notebook is for doing data checks and quick visualizations.

# %%
# standard import
import duckdb

# establish database connections
conn = duckdb.connect("../data/lake/database.duckdb")

# %% [markdown]
# ## Overview
#
# Load tables and show distribution.

# %%
# climate_atmosphere data
climate_df = conn.sql(
    """
    SELECT
        date,
        adm3_en,
        adm3_pcode,
        adm4_en,
        adm4_pcode,
        tave,
        pr
    FROM public.project_cchain_climate_atmosphere_clean
    WHERE data
    ORDER BY date
    LIMIT 5;
    """
).fetchdf()

climate_df

# %%
# dengue data
dengue_df = conn.sql(
    """
    SELECT *
    FROM public.project_cchain_disease_pidsr_totals_clean
    WHERE disease_common_name = 'RABIES'
    LIMIT 5;
    """
).fetchdf()

dengue_df

# %%
# dengue data
dengue_df = conn.sql(
    """
    SELECT *
    FROM public.project_cchain_disease_pidsr_totals_clean
    WHERE disease_common_name = 'DENGUE FEVER'
    LIMIT 5;
    """
).fetchdf()

dengue_df

# %%
