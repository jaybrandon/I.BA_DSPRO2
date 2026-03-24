from pathlib import Path
import ee
import pandas as pd
import geopandas as gpd

from gee_data import initialize_gee, get_glacier_collection, get_dem
from feature_extraction import extract_glacier_period_features


PROJECT_ID = "project-8e6c1255-803c-4395-88f"

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_PATH = BASE_DIR / "data" / "glamos_observations_with_geometry.parquet"
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

MAX_ROWS = 10  # set to None for all rows


def make_obs_id(row) -> str:
    return (
        f"{row['id']}_"
        f"{row['observation_start'].strftime('%Y-%m-%d')}_"
        f"{row['observation_end'].strftime('%Y-%m-%d')}"
    )


def load_input_data(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_parquet(path)

    required_cols = {
        "id",
        "observation_start",
        "observation_end",
        "mass_balance_annual",
        "geometry",
    }

    missing = required_cols - set(gdf.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    gdf["observation_start"] = pd.to_datetime(gdf["observation_start"], errors="coerce")
    gdf["observation_end"] = pd.to_datetime(gdf["observation_end"], errors="coerce")

    gdf = gdf.dropna(
        subset=["id", "observation_start", "observation_end", "geometry"]
    ).copy()

    if gdf.crs is None:
        raise ValueError("Input GeoDataFrame has no CRS defined.")

    gdf = gdf.to_crs(epsg=4326)
    gdf["obs_id"] = gdf.apply(make_obs_id, axis=1)

    return gdf


def process_glacier_observation(row) -> dict:
    glacier_id = row["id"]
    obs_id = row["obs_id"]
    observation_start = row["observation_start"].strftime("%Y-%m-%d")
    observation_end = row["observation_end"].strftime("%Y-%m-%d")

    roi = ee.Geometry(row.geometry.__geo_interface__)

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
        glacier_id=glacier_id,
        observation_start=observation_start,
        observation_end=observation_end,
    )

    # numeric/tabular output: keep teammate columns, add satellite features
    feature_row = row.drop(labels="geometry").to_dict()
    feature_row.update(results["features"])
    feature_row["obs_id"] = obs_id

    # non-numeric output index: separate references for mask/image data
    raster_row = {
        "obs_id": obs_id,
        "id": glacier_id,
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

    if MAX_ROWS is not None:
        gdf = gdf.head(MAX_ROWS).copy()

    feature_rows = []
    raster_rows = []
    failed_rows = []

    for idx, row in gdf.iterrows():
        try:
            outputs = process_glacier_observation(row)

            feature_rows.append(outputs["feature_row"])
            raster_rows.append(outputs["raster_row"])

            print(
                f"Processed glacier {row['id']} | "
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

            print(
                f"Error on glacier {row.get('id')} | "
                f"{row.get('observation_start')} to {row.get('observation_end')}: {e}"
            )

    features_df = pd.DataFrame(feature_rows)
    raster_df = pd.DataFrame(raster_rows)
    errors_df = pd.DataFrame(failed_rows)

    features_path = OUTPUT_DIR / "glamos_observations_with_satellite_features.parquet"
    raster_path = OUTPUT_DIR / "glacier_raster_index.pkl"
    errors_path = OUTPUT_DIR / "glacier_satellite_feature_errors.csv"

    features_df.to_parquet(features_path, index=False)
    raster_df.to_pickle(raster_path)
    errors_df.to_csv(errors_path, index=False)

    print("\nSaved:")
    print(f"- {features_path}")
    print(f"- {raster_path}")
    print(f"- {errors_path}")


if __name__ == "__main__":
    main()