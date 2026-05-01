import os
from pathlib import Path

import glamos_processing as glamos
import meteoswiss_processing as meteo
import satellite_processing as sat
import typer
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

VERSION = "v1.0"

OUTPUT_FILE = DATA_DIR / "processed" / f"glacier_ml_dataset_{VERSION}.parquet"


def build(start_year: int = 0, end_year: int = 0, skip_dl: bool = False):

    load_dotenv()
    if os.getenv("GC_PROJECT_ID") is None:
        print("Building dataset failed: GC_PROJECT_ID is not configured in env")
        return

    print(f"Building dataset from {start_year} to {end_year}")

    print("---Processing glamos data---")
    gdf = glamos.get_data(start_year, end_year)

    print("---Processing metrological data---")
    met = meteo.get_data(start_year, end_year, skip_dl)
    gdf = meteo.get_climate_features(gdf, met)

    print("---Processing satellite data---")
    df = sat.get_satellite_features(gdf)

    df_cleaned = data_cleaning(df)
    df_cleaned.to_parquet(OUTPUT_FILE, index=False)

    print(f"Dataset saved to: {OUTPUT_FILE}")
    print(f"Final Sample Size: {len(df_cleaned)} observations.")


def data_cleaning(df):

    df = df.dropna(subset=["NDSI", "B2", "sla"]).copy()
    df = df[df["area_m2"] > 0]

    df["year"] = df["date"].str[:4].astype(int)
    df["doy"] = df["date"].str[-3:].astype(int)

    if "geometry" in df.columns:
        df["total_area_static"] = df["geometry"].area
        df["AAR"] = (df["area_m2"] / df["total_area_static"]).clip(0, 1)

    if "sla" in df.columns and "elev_mean" in df.columns:
        df["sla_norm"] = (df["sla"] - df["elev_mean"]) / df["elev_mean"]

    columns = [
        "id",
        "obs_id",
        "year",
        "doy",
        "satellite",
        "observation_start",
        "observation_end",
        "mass_balance_annual",
        "area_m2",
        "AAR",
        "sla",
        "sla_norm",
        "elev_mean",
        "slope_mean",
        "aspect_mean",
        "NDSI",
        "snow_fraction",
        "coordx",
        "coordy",
        "B2",
        "B3",
        "B4",
        "B8",
        "B11",
        "B12",
        "q1h_temp",
        "q2h_temp",
        "q3h_temp",
        "q4h_temp",
        "q1h_prec",
        "q2h_prec",
        "q3h_prec",
        "q4h_prec",
    ]

    final_cols = [c for c in columns if c in df.columns]
    return df[final_cols]


if __name__ == "__main__":
    typer.run(build)
