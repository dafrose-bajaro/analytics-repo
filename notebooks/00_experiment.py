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

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Experimental Notebook
# This notebook serves as a dump for experimentation for tables in DuckDB.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Import packages

# %%
# install packages and establish database connections
import duckdb

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

# %%
# sample query
conn.sql(
    """
    SELECT *
    FROM public.nasa_firms_clean
    ORDER BY acq_datetime
    """
).pl()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## WAQI Air Quality

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_clean
    ORDER BY date;
    """
).pl()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Project CCHAIN

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn.sql(
    """
    SELECT *
    FROM public.project_cchain_climate_atmosphere_clean
    ORDER BY date
    LIMIT 50;
    """
).pl()

# %%
# sample query
conn.sql(
    """
    SELECT *
    FROM public.project_cchain_disease_pidsr_totals_clean
    WHERE case_total != 0
    ORDER BY date
    LIMIT 50;
    """
).pl()
