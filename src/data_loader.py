from pathlib import Path
import geopandas as gpd


def assign_satellite_label(date):
    """
    Assignment of satellite sensor based on timeperiod:
    Landsat 5: < 2013
    Landsat 8: 2013 - 2016
    Sentinel-2: 2017 - Present
    """
    year = date.year
    if year < 2013:
        return "landsat5"
    elif 2013 <= year <= 2016:
        return "landsat8"
    else:
        return "sentinel2"

def load_and_prepare_glamos_data(path: Path):
    gdf = gpd.read_parquet(path)
    
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)
    
    gdf["geometry"] = gdf.geometry.buffer(0)
    
    gdf['satellite'] = gdf['observation_end'].apply(assign_satellite_label)
    
    return gdf