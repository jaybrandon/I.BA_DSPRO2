from pathlib import Path
import geopandas as gpd


def load_geometry(data_dir: Path):

    glacier_path = data_dir / "extended_glacier_metadata.parquet"
    debris_path = data_dir / "extended_debris_metadata.parquet"

    gdf_ice = gpd.read_parquet(glacier_path)
    gdf_debris = gpd.read_parquet(debris_path)

    gdf_ice["geometry"] = gdf_ice.geometry.buffer(0)
    gdf_debris["geometry"] = gdf_debris.geometry.buffer(0)

    gdf_ice = gdf_ice.to_crs(epsg=4326)
    gdf_debris = gdf_debris.to_crs(epsg=4326)

    return gdf_ice, gdf_debris