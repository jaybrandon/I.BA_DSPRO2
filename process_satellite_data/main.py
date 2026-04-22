import ee
import pandas as pd
import geopandas as gpd
from pathlib import Path

from gee_data import initialize_gee, get_glacier_collection, get_dem
from feature_extraction import extract_glacier_period_features

PROJECT_ID = "project-8e6c1255-803c-4395-88f"
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_PATH = BASE_DIR / "data" / "glamos_observations_with_geometry.parquet"
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_ROWS = 10

def assign_satellite_label(date):
    year = date.year
    if year < 1984:
        return None  
    elif 1984 <= year < 2013:
        return "landsat5"
    elif 2013 <= year <= 2016:
        return "landsat8"
    else:
        return "sentinel2"

def load_and_prepare_glamos_data(path: Path) -> gpd.GeoDataFrame:
    print(f"Loading data from {path}...")
    gdf = gpd.read_parquet(path)
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)
    gdf["geometry"] = gdf.geometry.buffer(0)
    gdf['satellite'] = gdf['observation_end'].apply(assign_satellite_label)
    return gdf

def main():
    initialize_gee(PROJECT_ID)
    gdf = load_and_prepare_glamos_data(INPUT_PATH)

    if MAX_ROWS:
        gdf = gdf.head(MAX_ROWS)

    all_numerical_rows = []
    image_verification_list = []
    image_verification_list_info = []

    print(f"Starting extraction for {len(gdf)} observation periods...")

    for idx, row in gdf.iterrows():
        obs_id = row['obs_id']
        sat_type = row['satellite']
        
        if sat_type is None:
            print(f"[{idx+1}/{len(gdf)}] Skipping {obs_id}: Pre-satellite era.")
            continue

        try:
            roi = ee.Geometry(row.geometry.__geo_interface__)
            
            collection = get_glacier_collection(
                sensor_type=sat_type,
                polygon=roi,
                start_date=row['observation_start'].strftime('%Y-%m-%d'),
                end_date=row['observation_end'].strftime('%Y-%m-%d'),
                cloud_threshold=40
            )

            if collection.size().getInfo() == 0:
                print(f"[{idx+1}/{len(gdf)}] Skipping {obs_id}: No summer images found.")
                continue

            dem = get_dem(roi)

            results = extract_glacier_period_features(collection, dem, roi, obs_id)
            feature_list = results.get("features", [])
            mask_img = results.get("final_mask_image")
           
            for feat in feature_list:
                props = feat.get('properties', feat) if isinstance(feat, dict) else feat
                
                if props:
                    combined_row = {
                        **row.drop('geometry').to_dict(), 
                        **props
                    }
                    all_numerical_rows.append(combined_row)

            if mask_img:
                image_verification_list.append({
                    "obs_id": obs_id,
                    "mask_image": mask_img
                })
                image_verification_list_info.append({
                    "obs_id": obs_id,
                    "satellite": sat_type,
                    "observation_start": row['observation_start'].strftime('%Y-%m-%d'),
                    "observation_end": row['observation_end'].strftime('%Y-%m-%d'),
                    "geometry": row.geometry
                })
            else:
                print("---no mask saved---")

            print(f"[{idx+1}/{len(gdf)}] Processed {obs_id}: Found {len(feature_list)} images.")

        except Exception as e:
            print(f"Error on {obs_id}: {e}")

    if all_numerical_rows:
        final_df = pd.DataFrame(all_numerical_rows)

        final_df = final_df.dropna(subset=['area_m2', 'sla'], how='all')
        
        tabular_path = OUTPUT_DIR / "final_glacier_dataset_10.parquet"
        final_df.to_parquet(tabular_path, index=False)
        pd.DataFrame(image_verification_list).to_pickle(OUTPUT_DIR / "raster_verification.pkl")
        pd.DataFrame(image_verification_list_info).to_pickle(OUTPUT_DIR / "verification.pkl")
        print(f"\nSaved {len(final_df)} rows to {tabular_path}")
    else:
        print("No features extracted.")

if __name__ == "__main__":
    main()