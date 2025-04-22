# libraries to make functions work
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.plot import show
from loguru import logger
from dagster import Definitions, load_assets_from_modules
from . import assets

# configure loguru logger
logger.add(
    "logs.log",
    format="{time} {level} {message}",
    level="INFO",
    rotation="1 MB",
    compression="zip",
)


# open csv as dataframe
def open_csv(file_path):
    try:
        logger.info(f"Opening CSV file: {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Failed to open CSV file: {file_path} - Error: {e}")
        raise e


# open vector data (geojson, shp) as dataframe
def open_vector(file_path):
    try:
        logger.info(f"Opening vector file: {file_path}")
        return gpd.read_file(file_path)
    except Exception as e:
        logger.error(f"Failed to open vector file: {file_path} - Error: {e}")
        raise e


# display raster as image
def display_raster(file_path):
    try:
        logger.info(f"Opening and displaying raster file: {file_path}")
        with rasterio.open(file_path) as src:
            show(src)  # Display the raster visually
    except Exception as e:
        logger.error(f"Failed to open or display raster file: {file_path} - Error: {e}")
        raise e


# define dagster definitions
defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={},
)
