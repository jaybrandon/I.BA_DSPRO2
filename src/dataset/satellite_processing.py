import concurrent.futures
import os
from pathlib import Path

import ee
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from feature_extraction import extract_glacier_period_features
from gee_data import get_dem, get_glacier_collection, initialize_gee

BASE_DIR = Path(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
OUTPUT_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_ROWS = None
MAX_THREADS = 15  # Dont go to high to avoid rate limiting from gee


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


def prepare_glamos_data(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)
    gdf["geometry"] = gdf.geometry.buffer(0)
    gdf["satellite"] = gdf["observation_end"].apply(assign_satellite_label)
    return gdf


def process_observation(row_data):
    idx, row = row_data
    obs_id = row["obs_id"]
    sat_type = row["satellite"]

    local_numerical_rows = []
    local_verification_list = []
    local_verification_list_info = []

    if sat_type is None:
        print(f"[{idx + 1}] Skipping {obs_id}: Pre-satellite era.")
        return local_numerical_rows, local_verification_list, local_verification_list_info

    try:
        roi = ee.Geometry(row.geometry.__geo_interface__)

        collection = get_glacier_collection(
            sensor_type=sat_type,
            polygon=roi,
            start_date=row["observation_start"].strftime("%Y-%m-%d"),
            end_date=row["observation_end"].strftime("%Y-%m-%d"),
            cloud_threshold=40,
        )

        count = collection.size().getInfo()
        if count == 0:
            print(f"[{idx + 1}] Skipping {obs_id}: No summer images found.")
            return local_numerical_rows, local_verification_list, local_verification_list_info

        print(f"[{idx + 1}] Processed {obs_id}: Found {count} images.")

        dem = get_dem(roi)

        results = extract_glacier_period_features(collection, dem, roi, obs_id)
        feature_list = results.get("features", [])
        mask_img = results.get("final_mask_image")

        for feat in feature_list:
            props = feat.get("properties", feat) if isinstance(feat, dict) else feat

            if props:
                combined_row = {**row.drop("geometry").to_dict(), **props}
                local_numerical_rows.append(combined_row)

        if mask_img:
            local_verification_list.append({"obs_id": obs_id, "mask_image": mask_img})
            local_verification_list_info.append(
                {
                    "obs_id": obs_id,
                    "satellite": sat_type,
                    "observation_start": row["observation_start"].strftime("%Y-%m-%d"),
                    "observation_end": row["observation_end"].strftime("%Y-%m-%d"),
                    "geometry": row.geometry,
                }
            )
        else:
            print(f"[{idx + 1}] ---no mask saved---")

        #print(f"[{idx + 1}] Processed {obs_id}: Found {len(feature_list)} images.")

    except Exception as e:
        print(f"Error on {obs_id}: {e}")

    return local_numerical_rows, local_verification_list, local_verification_list_info


def get_satellite_features(gdf: gpd.GeoDataFrame):
    load_dotenv()
    gc_project_id = os.getenv("GC_PROJECT_ID")
    if gc_project_id is None:
        print(
            "Satellite feature extraction failed: GC_PROJECT_ID is not configured in env"
        )
        return

    initialize_gee(gc_project_id)
    gdf = prepare_glamos_data(gdf)

    if MAX_ROWS:
        gdf = gdf.head(MAX_ROWS)

    all_numerical_rows = []
    image_verification_list = []
    image_verification_list_info = []

    print(f"Starting extraction for {len(gdf)} observation periods...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(process_observation, row_data): row_data
            for row_data in gdf.iterrows()
        }

        for future in concurrent.futures.as_completed(futures):
            num_rows, verif_list, verif_list_info = future.result()

            all_numerical_rows.extend(num_rows)
            image_verification_list.extend(verif_list)
            image_verification_list_info.extend(verif_list_info)

    if all_numerical_rows:
        df = pd.DataFrame(all_numerical_rows)

        pd.DataFrame(image_verification_list).to_pickle(OUTPUT_DIR / "raster_verification.pkl")
        pd.DataFrame(image_verification_list_info).to_pickle(OUTPUT_DIR / "verification.pkl")
        return df
    else:
        print("No features extracted.")
