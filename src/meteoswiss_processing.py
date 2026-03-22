import os
import shutil
from pathlib import Path

import pandas as pd
import requests
import typer
import xarray as xr
from tqdm import tqdm

DATA_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/data/")


def convert_time(ds):
    year = ds.time.units.split()[2][0:4]

    dates = pd.date_range(start=f"{year}-01-01", periods=12, freq="MS")

    ds = ds.assign_coords(time=dates)
    return ds


def extract_precipitation(
    start_year: int, end_year: int, skip_dl: bool = False
) -> xr.Dataset:
    dir = DATA_DIR / "tmp" / "rhiresm"

    if not skip_dl:
        shutil.rmtree(dir, ignore_errors=True)
        dir.mkdir(parents=True)

        for year in tqdm(
            range(start_year, end_year + 1), "Downloading precipitation data"
        ):
            filename = f"ogd-surface-derived-grid-archive.rhiresm_ch01r.swiss.lv95_{year:04d}0101000000_{year:04d}1201000000.nc"
            url = (
                "https://data.geo.admin.ch/ch.meteoschweiz.ogd-surface-derived-grid/archive-ch/"
                + filename
            )

            r = requests.get(url, stream=True)

            with open(dir / filename, "wb") as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)

    return xr.open_mfdataset(
        str(dir) + "/*.nc", preprocess=convert_time, data_vars="all", decode_times=False
    )


def transform_precipitation(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.resample(time="QS-OCT").sum()
    return ds


def extract_temperature(
    start_year: int, end_year: int, skip_dl: bool = False
) -> xr.Dataset:
    dir = DATA_DIR / "tmp" / "tabsm"

    if not skip_dl:
        shutil.rmtree(dir, ignore_errors=True)
        dir.mkdir(parents=True)
        for year in tqdm(
            range(start_year, end_year + 1), "Downloading temperature data"
        ):
            filename = f"ogd-surface-derived-grid-archive.tabsm_ch01r.swiss.lv95_{year:04d}0101000000_{year:04d}1201000000.nc"
            url = (
                "https://data.geo.admin.ch/ch.meteoschweiz.ogd-surface-derived-grid/archive-ch/"
                + filename
            )

            r = requests.get(url, stream=True)

            with open(dir / filename, "wb") as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)

    return xr.open_mfdataset(
        str(dir) + "/*.nc", preprocess=convert_time, data_vars="all", decode_times=False
    )


def transform_temperature(ds: xr.Dataset) -> xr.Dataset:
    ds = ds.resample(time="QS-OCT").mean()
    return ds


def get_data(start_year: int, end_year: int, skip_dl) -> xr.Dataset:
    prec = extract_precipitation(start_year, end_year, skip_dl)
    prec = transform_precipitation(prec)

    temp = extract_temperature(start_year, end_year, skip_dl)
    temp = transform_temperature(temp)

    return prec.merge(temp, compat="override")


def main(start_year: int = 1961, end_year: int = 2025, skip_dl: bool = False):
    ds = get_data(start_year, end_year, skip_dl)

    ds.to_netcdf(DATA_DIR / f"rhiresm_tabsm_quarterly_{start_year}-{end_year}.nc")


if __name__ == "__main__":
    typer.run(main)
