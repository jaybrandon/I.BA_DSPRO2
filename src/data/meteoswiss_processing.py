import os
import shutil
import warnings
from pathlib import Path

import pandas as pd
import requests
import typer
import xarray as xr
from tqdm import tqdm

DATA_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/data/")


def _convert_time(ds):
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
        str(dir) + "/*.nc",
        preprocess=_convert_time,
        data_vars="all",
        decode_times=False,
    )


def transform_precipitation(ds: xr.Dataset) -> xr.Dataset:
    timeline = pd.DatetimeIndex(ds["time"].values)
    if timeline.inferred_freq != "MS":
        print("!!!!!!! timeline is not consistent !!!!!!!")
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
        str(dir) + "/*.nc",
        preprocess=_convert_time,
        data_vars="all",
        decode_times=False,
    )


def transform_temperature(ds: xr.Dataset) -> xr.Dataset:
    timeline = pd.DatetimeIndex(ds["time"].values)
    if timeline.inferred_freq != "MS":
        print("!!!!!!! timeline is not consistent !!!!!!!")
    ds = ds.resample(time="QS-OCT").mean()
    return ds


def get_data(start_year: int, end_year: int, skip_dl: bool = False) -> xr.Dataset:
    prec = extract_precipitation(start_year, end_year, skip_dl)
    prec = transform_precipitation(prec)

    temp = extract_temperature(start_year, end_year, skip_dl)
    temp = transform_temperature(temp)

    return prec.merge(temp, compat="override")


def get_climate_features(target: pd.DataFrame, climate: xr.Dataset) -> pd.DataFrame:
    warnings.filterwarnings(
        "ignore",
        message="angle from rectified to skew grid parameter lost in conversion to CF",
    )

    climate = climate.rio.write_crs("EPSG:2056")
    features = target.apply(_extract_features, axis=1, args=(climate,))

    return pd.concat([target, features], axis=1)


def _extract_features(row, climate: xr.Dataset):
    climate = climate.sel(
        time=slice(row["observation_start"], row["observation_end"])
    )  # Select only relevant timeframe

    try:
        climate = climate.rio.clip([row["geometry"]])  # Clip to glacier geometry
        climate = climate.mean(dim=["N", "E"])  # Calculate mean across grids
    except Exception as e:
        print(
            f"Failed to find climate grid in geometry for glacier id: {row['id']}, obs: {row['observation_start']}"
        )
        print(e)
        print("Falling back to nearest coordinate")

        climate = climate.sel(E=row["coordx"], N=row["coordy"], method="nearest")

    q1 = climate.isel(time=0)
    q2 = climate.isel(time=1)
    q3 = climate.isel(time=2)
    q4 = climate.isel(time=3)

    return pd.Series(
        {
            "q1h_temp": q1["TabsM"].values.item(),
            "q2h_temp": q2["TabsM"].values.item(),
            "q3h_temp": q3["TabsM"].values.item(),
            "q4h_temp": q4["TabsM"].values.item(),
            "q1h_prec": q1["RhiresM"].values.item(),
            "q2h_prec": q2["RhiresM"].values.item(),
            "q3h_prec": q3["RhiresM"].values.item(),
            "q4h_prec": q4["RhiresM"].values.item(),
        }
    )


def main(start_year: int = 1961, end_year: int = 2025, skip_dl: bool = False):
    ds = get_data(start_year, end_year, skip_dl)

    ds.to_netcdf(DATA_DIR / f"rhiresm_tabsm_quarterly_{start_year}-{end_year}.nc")


if __name__ == "__main__":
    typer.run(main)
