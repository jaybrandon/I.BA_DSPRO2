from pathlib import Path
import ee
import pandas as pd
import geopandas as gpd

from gee_data import initialize_gee, get_glacier_collection, get_dem
from feature_extraction import extract_glacier_period_features


PROJECT_ID = "project-8e6c1255-803c-4395-88f"

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_PATH = BASE_DIR / "data" / "glamos_observations_with_geometry.parquet"
OUTPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_ROWS = 5


def load_input_data(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(path)

    return gdf.to_crs(epsg=4326)


def process_glacier_observation(row) -> dict:
    roi = ee.Geometry(row.geometry.__geo_interface__)

    observation_start = row["observation_start"].strftime("%Y-%m-%d")
    observation_end = row["observation_end"].strftime("%Y-%m-%d")

    collection = get_glacier_collection(
        polygon=roi,
        observation_start=observation_start,
        observation_end=observation_end,
        cloud_threshold=40,
    )

    dem = get_dem(roi)

    results = extract_glacier_period_features(
        collection=collection,
        dem=dem,
        polygon=roi,
        glacier_id=row["id"],
        observation_start=observation_start,
        observation_end=observation_end,
    )

    feature_row = {
        "obs_id": row["obs_id"],
        **results["features"],
    }

    raster_row = {
        "obs_id": row["obs_id"],
        "id": row["id"],
        "observation_start": observation_start,
        "observation_end": observation_end,
        "has_final_mask_image": results.get("final_mask_image") is not None,
        "has_mean_ndsi_image": results.get("mean_ndsi_image") is not None,
        "final_mask_image": results.get("final_mask_image"),
        "mean_ndsi_image": results.get("mean_ndsi_image"),
    }

    return {
        "feature_row": feature_row,
        "raster_row": raster_row,
    }


def main():
    initialize_gee(PROJECT_ID)

    gdf = load_input_data(INPUT_PATH)

    gdf_s2 = gdf[gdf["satellite"] == "sentinel2"].copy()

    if MAX_ROWS is not None:
        gdf_s2 = gdf_s2.head(MAX_ROWS).copy()

    feature_rows = []
    raster_rows = []
    failed_rows = []

    for idx, row in gdf_s2.iterrows():
        try:
            outputs = process_glacier_observation(row)
            feature_rows.append(outputs["feature_row"])
            raster_rows.append(outputs["raster_row"])

            print(
                f"Processed {row['obs_id']} | "
                f"{row['observation_start'].date()} to {row['observation_end'].date()}"
            )

        except Exception as e:
            failed_rows.append(
                {
                    "index": idx,
                    "obs_id": row.get("obs_id"),
                    "id": row.get("id"),
                    "observation_start": row.get("observation_start"),
                    "observation_end": row.get("observation_end"),
                    "error": str(e),
                }
            )
            print(f"Error on {row.get('obs_id')}: {e}")

    features_df = pd.DataFrame(feature_rows)
    raster_df = pd.DataFrame(raster_rows)


    full_df = pd.DataFrame(gdf.drop(columns="geometry"))
    full_with_features = full_df.merge(features_df, on="obs_id", how="left")

    combined_path = OUTPUT_DIR / "glamos_observations_sentinel2_features.parquet"
    raster_path = OUTPUT_DIR / "sentinel2_raster_index.pkl"
   

    full_with_features.to_parquet(combined_path, index=False)
    raster_df.to_pickle(raster_path)


    print("\nSaved:")
    print(f"- {combined_path}")
    print(f"- {raster_path}")



if __name__ == "__main__":
    main()