import os

import pandas as pd
import typer


def extract_data() -> pd.DataFrame:
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


def transform_data(df: pd.DataFrame, start_year: int, end_year: int) -> pd.DataFrame:
    if start_year > end_year and end_year != 0:
        raise ValueError("start_year may not be greater than end_year")

    df.observation_start = pd.to_datetime(df.observation_start)
    df.observation_end = pd.to_datetime(df.observation_end)

    if start_year != 0:
        df = df[df.observation_start >= f"{start_year}-10-01"]
    if end_year != 0:
        df = df[df.observation_end <= f"{end_year}-09-30"]

    return df


def get_data(start_year: int = 0, end_year: int = 0) -> pd.DataFrame:
    df = extract_data()
    df = transform_data(df, start_year, end_year)
    return df


def main(start_year: int = 0, end_year: int = 0):
    df = get_data(start_year, end_year)
    data_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/data/"

    start = str(df.observation_start.min().date())
    end = str(df.observation_end.max().date())

    df.to_parquet(data_dir + f"glamos_massbalance_{start}-{end}.parquet")


if __name__ == "__main__":
    typer.run(main)
