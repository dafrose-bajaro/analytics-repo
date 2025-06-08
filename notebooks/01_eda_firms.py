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
import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.api as sm
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
provinces = [
    "Chiang Mai Province",
    # "Lamphun Province",
    # "Lampang Province",
    # "Chiang Rai Province",
    # "Mae Hong Son Province"
]
provinces_bounds = th_bounds[th_bounds["shapeName"].isin(provinces)]
provinces_bounds.plot()

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
firms_provinces_df = gpd.sjoin(firms_df, provinces_bounds, how="inner")
firms_provinces_df = firms_provinces_df.drop(
    columns=[
        "shapeName",
        "shapeISO",
        "shapeID",
        "shapeGroup",
        "shapeType",
        "index_right",
    ]
)
firms_provinces_df = firms_provinces_df.sort_values(by=["acq_datetime"], ascending=True)
# firms_chiang_mai_df['geometry'] = firms_chiang_mai_df['geometry'].apply(wkt.loads)
firms_provinces_df = gpd.GeoDataFrame(firms_provinces_df, geometry="geometry")
firms_provinces_df = firms_provinces_df.set_crs(epsg=4326)
firms_provinces_df = firms_provinces_df[
    ["acq_datetime", "bright_ti4", "frp", "geometry"]
]
firms_provinces_df = firms_provinces_df[
    (firms_provinces_df["acq_datetime"] >= "2025-02-01")
    & (firms_provinces_df["acq_datetime"] < "2025-04-01")
]

firms_provinces_df.head(5)

# %%
# visualize
firms_provinces_df.explore(
    column="bright_ti4", cmap="viridis", tiles="CartoDB dark_matter"
)

# %% [markdown]
# ### WAQI

# %%
# waqi_airquality data
waqi_df = conn.sql(
    """
    SELECT *
    FROM public.waqi_airquality_clean
    """
).fetchdf()

waqi_df = waqi_df[["date", "pm25"]]
waqi_df = waqi_df[(waqi_df["date"] >= "2025-02-01") & (waqi_df["date"] < "2025-04-01")]
waqi_df = waqi_df.sort_values(by="date", ascending=True)

waqi_df.head(5)

# %% [markdown]
# ## Processing
#
# We want to get:
#
# - average temp of hotspots per day
# - number of hotspots per day

# %%
firms_date = (
    firms_provinces_df.groupby(firms_provinces_df["acq_datetime"].dt.date)
    .agg({"bright_ti4": "count"})
    .reset_index()
)
firms_date = firms_date.rename(
    columns={"acq_datetime": "date", "bright_ti4": "count_hotspots"}
)
firms_date["date"] = pd.to_datetime(firms_date["date"])
firms_date.head(5)

# %%
firms_waqi = firms_date.merge(waqi_df, on="date", how="outer")
firms_waqi["count_hotspots"] = firms_waqi["count_hotspots"].fillna(0)
firms_waqi.head(10)

# %%
plt.figure(figsize=(10, 5))

plt.plot(
    firms_waqi.index,
    firms_waqi["count_hotspots"],
    label="Count of Hotspots",
    color="blue",
    marker="o",
)
plt.plot(
    firms_waqi.index, firms_waqi["pm25"], label="PM2.5 Levels", color="red", marker="o"
)

plt.title("Time Series of Count Hotspots and PM2.5 Levels", fontsize=14)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Values", fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
plt.legend()
plt.grid()

plt.tight_layout()
plt.show()

# %%
# seasonal decomposition details
seasonal_decompose_model = "additive"  # additive | multiplicative
component = "trend"  # observed | trend | seasonal | resid

# set datetime index to daily
firms_waqi.set_index("date", inplace=True)
firms_waqi = firms_waqi.asfreq("D").fillna(0)
firms_waqi["date"] = firms_waqi.index

plt.figure(figsize=(12, 4))
ax1 = plt.gca()
ax2 = ax1.twinx()

# seasonal decomposition
hotspots_decomposition = sm.tsa.seasonal_decompose(
    firms_waqi["count_hotspots"], model=seasonal_decompose_model
)
hotspots_component = getattr(hotspots_decomposition, component)
ax1.plot(
    hotspots_component.index, hotspots_component, label="Count of Hotspots", linewidth=1
)

pm25_decomposition = sm.tsa.seasonal_decompose(
    firms_waqi["pm25"], model=seasonal_decompose_model
)
pm25_component = getattr(pm25_decomposition, component)
ax2.plot(
    pm25_component.index,
    pm25_component,
    label="PM2.5 Levels",
    linewidth=0.5,
    linestyle="--",
)

ax1.set_ylabel("Count of Hotspots")
ax2.set_ylabel("PM2.5 Levels")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

plt.show()

# %%
