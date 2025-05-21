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

# %% [markdown]
# # Exploratory Data Analysis
#
# This notebook is for doing data checks and quick visualizations.

# %%
# standard import
# warnings
import warnings

import duckdb

# geospatial
import geopandas as gpd
from shapely import wkt

warnings.filterwarnings("ignore")

# establish database connections
conn = duckdb.connect("../data/lake/database.duckdb")

# %% [markdown]
# ## Overview
#
# Load and merge tables. Note that we are using `bright_ti4` as this is used particularly for thermal anomalies and fires. `bright_ti5` is used for cloud and surface temperature measurements.

# %% [markdown]
# ### Admin boundaries

# %%
# download data
# !wget -P ../data https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/THA/ADM1/geoBoundaries-THA-ADM1_simplified.geojson

# %%
# convert to geodataframe
th_bounds = gpd.read_file("../data/geoBoundaries-THA-ADM1_simplified.geojson")
th_bounds = th_bounds.set_crs(epsg=4326)
chiang_mai_bounds = th_bounds[th_bounds["shapeName"] == "Chiang Mai Province"]
chiang_mai_bounds.plot()

# %% [markdown]
# ### NASA FIRMS

# %%
# nasa_firms data
firms_df = conn.sql(
    """
    SELECT
        acq_datetime,
        (bright_ti4 - 273.15) AS bright_ti4, -- convert to Celsius
        frp,
        geometry
    FROM public.nasa_firms_clean
    WHERE (confidence == 'n' OR confidence == 'h') AND (daynight == 'D')
    """
).fetchdf()

firms_df["geometry"] = firms_df["geometry"].apply(wkt.loads)
firms_df = gpd.GeoDataFrame(firms_df, geometry="geometry")
firms_df = firms_df.set_crs(epsg="4326")

firms_df.head(5)

# %%
# intersect with chiang mai bounds
firms_chiang_mai_df = gpd.sjoin(firms_df, chiang_mai_bounds, how="inner")
firms_chiang_mai_df = firms_chiang_mai_df.drop(
    columns=[
        "shapeName",
        "shapeISO",
        "shapeID",
        "shapeGroup",
        "shapeType",
        "index_right",
    ]
)
firms_chiang_mai_df = firms_chiang_mai_df.sort_values(
    by=["acq_datetime"], ascending=True
)
# firms_chiang_mai_df['geometry'] = firms_chiang_mai_df['geometry'].apply(wkt.loads)
firms_chiang_mai_df = gpd.GeoDataFrame(firms_chiang_mai_df, geometry="geometry")
firms_chiang_mai_df = firms_chiang_mai_df.set_crs(epsg=4326)
firms_chiang_mai_df = firms_chiang_mai_df[
    ["acq_datetime", "bright_ti4", "frp", "geometry"]
]

firms_chiang_mai_df.head(5)

# %%
# visualize
firms_chiang_mai_df.explore(column="bright_ti4", cmap="RdYlBu_r")

# %%
# waqi_airquality data
waqi_df = conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_clean
    """
).fetchdf()

waqi_df.head(5)

# %%
