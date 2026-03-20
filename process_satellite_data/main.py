import uuid
import ee
import pandas as pd
import geopandas as gpd

from data_loader import *
from gee_data import *
from feature_extraction import *


PROJECT_ID = "project-8e6c1255-803c-4395-88f"

PATH_ICE = "/Users/maraeckart/dev/hslu/fs26/DSPRO/I.BA_DSPRO2/processed_glamos_data/glacier_geometry_2013-2018 (1).parquet"
PATH_DEBRIS = "/Users/maraeckart/dev/hslu/fs26/DSPRO/I.BA_DSPRO2/processed_glamos_data/debris_geometry_2011-2017 (1).parquet"

YEAR = 2018


def main():
    initialize_gee(PROJECT_ID)

    gdf = load_data(PATH_ICE, PATH_DEBRIS)

    features_rows = []
    raster_rows = []

    for _, row in gdf.head(10).iterrows():
        sample_id = str(uuid.uuid4())

        try:
            roi = ee.Geometry(row.geometry.__geo_interface__)
            image, dem = get_satellite_data(roi, YEAR)
            results = extract_glacier_features(image, dem, roi)

            #features / tabular part
            features_row = row.to_dict()
            features_row["sample_id"] = sample_id
            features_row["ext_area_km2"] = results["ext_area_km2"]
            features_row["ext_sla_m"] = results["ext_sla_m"]
            features_rows.append(features_row)

            # raster / array part
            raster_rows.append(
                {
                    "sample_id": sample_id,
                    "year": YEAR,
                    "pixel_mask": results["pixel_mask"],
                }
            )

            print(f"Processed: {row.get('sgi-id')}")

        except Exception as e:
            print(f"Error on {row.get('sgi-id')}: {e}")

    featuredata_df = pd.DataFrame(features_rows)
    raster_df = pd.DataFrame(raster_rows)

    featuredata_df.to_parquet("extended_glacier_feature_data.parquet", index=False)
    raster_df.to_pickle("extended_glacier_rasters.pkl")

    print("Saved:")
    print("- extended_glacier_feature_data.parquet")
    print("- extended_glacier_rasters.pkl")


if __name__ == "__main__":
    main()