import os
import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

SATELLITE_INPUT = DATA_DIR / "satellite_features_extracted.parquet"
GLAMOS_INPUT = DATA_DIR / "glamos_massbalance_1884-10-01-2025-09-30.parquet"
METEO_INPUT = DATA_DIR / "rhiresm_tabsm_quarterly_1961-2025.nc"
OUTPUT_FILE = DATA_DIR / "glacier_merged_final.parquet"

def merge_all_data():
  
    if not SATELLITE_INPUT.exists() or not GLAMOS_INPUT.exists():
        print(f"Error: Required input files not found in {DATA_DIR}")
        return

    
    df_sat = pd.read_parquet(SATELLITE_INPUT)
    df_gl = pd.read_parquet(GLAMOS_INPUT)
   
    df_sat['id'] = df_sat['id'].astype(str)
    df_gl['id'] = df_gl['id'].astype(str)

    df = pd.merge(
        df_sat, 
        df_gl, 
        on=['id', 'observation_start', 'observation_end'], 
        how='inner'
    )

   
    """
    if METEO_INPUT.exists():
        print("Integrating meteo data...")
        ds = xr.open_dataset(METEO_INPUT)

        df_meteo = ds.to_dataframe().reset_index()
        df_meteo['id'] = df_meteo['id'].astype(str)
        
        meteo_cols = ['id', 'observation_start', 'observation_end', 
                      'q1h_temp', 'q2h_temp', 'q3h_temp', 'q4h_temp',
                      'q1h_prec', 'q2h_prec', 'q3h_prec', 'q4h_prec']
        
        df = pd.merge(
            df, 
            df_meteo[meteo_cols], 
            on=['id', 'observation_start', 'observation_end'], 
            how='inner'
        )
    else: 
        print('Warning: Meteo file found but skipped or does not exist at path.')
    """
    df_cleaned = data_cleaning(df)
    df_cleaned.to_parquet(OUTPUT_FILE, index=False)
    
    print(f"Done! dataset saved to: {OUTPUT_FILE}")
    print(f"Final Sample Size: {len(df_cleaned)} observations.")


def data_cleaning(df):

    df = df.dropna(subset=['NDSI', 'B2', 'sla']).copy()
    df = df[df['area_m2'] > 0]
    
    df['year'] = df['date'].str[:4].astype(int)
    df['doy'] = df['date'].str[-3:].astype(int)

    if 'geometry' in df.columns:
        df['total_area_static'] = df['geometry'].area
        df['AAR'] = (df['area_m2'] / df['total_area_static']).clip(0, 1)
    
    if 'sla' in df.columns and 'elev_mean' in df.columns:
        df['sla_norm'] = (df['sla'] - df['elev_mean']) / df['elev_mean']

    columns = [
        'id', 'obs_id', 'year', 'doy', 'satellite',
        'observation_start', 'observation_end',
        'mass_balance_annual', 'area_m2', 'AAR', 
        'sla', 'sla_norm', 'elev_mean', 'slope_mean', 'aspect_mean',
        'NDSI', 'snow_fraction', 'coordx', 'coordy',
        'B2', 'B3', 'B4', 'B8', 'B11', 'B12'
    ]
    
    final_cols = [c for c in columns if c in df.columns]
    return df[final_cols]

if __name__ == "__main__":
    merge_all_data()