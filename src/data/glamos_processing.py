import os
import shutil
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
import typer
import geopandas as gpd

def extract_data() -> pd.DataFrame:
    print('Extracting glacier mass balance...')
    gl = pd.read_csv(
        "https://doi.glamos.ch/data/glacier_list/glacier_list.csv",
        skiprows=9,
        parse_dates=True,
        names=[
            "name",
            "id",
            "coordx",
            "coordy",
            "area",
            "survey_year",
            "length_change_data_available",
            "mass_balance_data_available",
            "volume_change_data_available",
        ],
    )

    gl = gl[gl["mass_balance_data_available"] == 1][["id", "coordx", "coordy"]]

    mb = pd.read_csv(
        "https://doi.glamos.ch/data/massbalance/massbalance_fixdate.csv",
        skiprows=9,
        parse_dates=True,
        usecols=list(range(8)),
        names=[
            "name",
            "id",
            "observation_start",
            "observation_end_winter",
            "observation_end",
            "mass_balance_winter",
            "mass_balance_summer",
            "mass_balance_annual",
        ],
    )

    df = pd.merge(mb, gl, on="id")[
        [
            "id",
            "observation_start",
            "observation_end",
            "mass_balance_annual",
            "coordx",
            "coordy",
        ]
    ]

    return df


def extract_geometry() -> gpd.GeoDataFrame:
    print("Extracting glacier geometry...")

    dir = Path("../data/raw/glacier_inventory/")
    shutil.rmtree(dir, ignore_errors=True)
    dir.mkdir(parents=True)

    r = requests.get(
        "https://doi.glamos.ch/data/inventory/inventory_sgi2016_r2020.zip", stream=True
    )
    with ZipFile(BytesIO(r.content)) as fd:
        fd.extractall(dir)

    return gpd.read_file(dir / 'SGI_2016_glaciers.shp')


def transform_data(df: pd.DataFrame, geo: gpd.GeoDataFrame, start_year: int, end_year: int) -> gpd.GeoDataFrame:
    if start_year > end_year and end_year != 0:
        raise ValueError("start_year may not be greater than end_year")

    df.observation_start = pd.to_datetime(df.observation_start)
    df.observation_end = pd.to_datetime(df.observation_end)

    if start_year != 0:
        df = df[df.observation_start >= f"{start_year}-10-01"]
    if end_year != 0:
        df = df[df.observation_end <= f"{end_year}-09-30"]

    geo = geo[['sgi-id', 'geometry']].merge(df, right_on='id', left_on='sgi-id', how='inner')
    geo = geo.drop("sgi-id", axis=1)

    return geo


def get_data(start_year: int = 0, end_year: int = 0) -> gpd.GeoDataFrame:
    df = extract_data()
    geo = extract_geometry()
    geo = transform_data(df, geo, start_year, end_year)
    return geo


def main(start_year: int = 0, end_year: int = 0):
    df = get_data(start_year, end_year)
    data_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'data' / 'processed'

    start = str(df.observation_start.min().date())
    end = str(df.observation_end.max().date())

    df.to_parquet(data_dir / f"glamos_massbalance_{start}-{end}.parquet")


if __name__ == "__main__":
    typer.run(main)
