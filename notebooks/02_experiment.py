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

# %%
import duckdb

conn = duckdb.connect("../data/lake/duck.db")
conn.sql(
    """
SELECT *
FROM public.project_cchain_climate_atmosphere
ORDER BY date
LIMIT 50;
"""
).pl()

# %%
