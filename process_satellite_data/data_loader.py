import pandas as pd
import geopandas as gpd


def load_data(glamos_path, debris_path):

    # load parquet files
    gdf_ice = gpd.read_parquet(glamos_path)
    gdf_debris = gpd.read_parquet(debris_path)
 
    # combine datasets
    full_gdf = gpd.GeoDataFrame(
        pd.concat([gdf_ice, gdf_debris], ignore_index=True),
        crs=gdf_ice.crs
    )

    # merge 
    if "sgi-id" in full_gdf.columns:
        full_gdf = full_gdf.dissolve(by="sgi-id").reset_index()

    # if invalid geometries
    full_gdf["geometry"] = full_gdf.geometry.buffer(0)

    # convert to WGS84 for Earth Engine
    return full_gdf.to_crs(epsg=4326)