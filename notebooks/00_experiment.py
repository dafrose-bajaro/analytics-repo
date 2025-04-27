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

# %%
# show all available schemas and tables
conn = duckdb.connect("./data/lake/database.duckdb")
conn.sql(
    """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name;
"""
).pl()

# %% editable=true slideshow={"slide_type": ""}
# sample query
conn = duckdb.connect("./data/lake/database.duckdb")
waqi_airquality_raw = conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_raw
    ORDER BY date
    LIMIT 50;
    """
).pl()
waqi_airquality_raw
