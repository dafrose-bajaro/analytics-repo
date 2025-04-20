# general geospatial
import geopandas as gpd #used for converting and operating on geodataframes
import geowrangler #used for geospatial operations

# raster operations
import rasterio as rio #reads and writes geotiffs based on numpy arrays and geojson
from rasterio.plot import show #simple plot generation
from rasterio.plot import show_hist #simple histogram generation
from rasterio.warp import reproject, Resampling #used for reprojections
from rasterio.mask import mask #making or clipping rasters
from rasterio.merge import merge #merging and mosaicing rasters
from rasterio.enums import Resampling #used for resampling (upscale/downscale)
from earthpy import spatial as es #needed for stacking rasters
from earthpy import mask as em #masking raster pixels such as in cloudmasking
from earthpy import plot as ep #plotting several bands of a raster
from rasterstats import zonal_stats #raster zonal stats

# vector operations
import fiona #used for reading and writing vector data
import shapely #used for creating and operating on shapely geometries
from shapely.errors import ShapelyDeprecationWarning #prevent shapely deprecation warnings from showing