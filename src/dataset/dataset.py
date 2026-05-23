import os
from pathlib import Path

import glamos_processing as glamos
import meteoswiss_processing as meteo
import satellite_processing as sat
import typer
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"

VERSION = "v3.3"

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
    print(f"Final Sample Size: {len(df)} observations.")


def data_cleaning(df):
    df["year"] = df["date"].str[:4].astype(int)
    df["doy"] = df["date"].str[-3:].astype(int)

    df["sla_norm"] = (df["SLA"] - df["elev_mean"]) / (df["elev_max"] - df["elev_min"])

    columns = [
        "id",
        "obs_id",
        "year",
        "doy",
        "satellite",
        "observation_start",
        "observation_end",
        "mass_balance_annual",
        "sla_norm",
        "B2_mean",
        "B3_mean",
        "B4_mean",
        "B8_mean",
        "B11_mean",
        "B12_mean",
        "B2_std",
        "B3_std",
        "B4_std",
        "B8_std",
        "B11_std",
        "B12_std",
        "SCR",
        "SCA",
        "SLA",
        "elev_mean",
        "slope_mean",
        "aspect_mean",
        "coordx",
        "coordy",
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
