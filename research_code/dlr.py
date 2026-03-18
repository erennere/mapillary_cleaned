
import requests
import logging
import os, sys
from vt2geojson import tools as vt2geojson_tools
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from start import load_config

def download_and_process_tile(row, mly_key, retries=3):
    z = row["z"]
    x = row["x"]
    y = row["y"]
    endpoint = "mly1_public"
    url = f"https://tiles.mapillary.com/maps/vtp/{endpoint}/2/{z}/{x}/{y}?access_token={mly_key}"
    
    attempt = 0
    while attempt < retries:
        try:
            r = requests.get(url)
            assert r.status_code == 200, r.content
            vt_content = r.content
            features = vt2geojson_tools.vt_bytes_to_geojson(vt_content, x, y, z)
            gdf = gpd.GeoDataFrame.from_features(features)
            gdf[f'z{z}_tiles'] = f'{x}-{y}-{z}'
            return gdf, None
        except Exception as e:
            print(f"An exception occurred while requesting a tile: {e}")
            attempt += 1
    
    print(f"A tile could not be downloaded: {row}")
    return None, row

def process_tile_file(file, mly_key, retries=3):
    continent_file = [] 
    failed_tiles = []

    for index, row in tqdm(file.iterrows(), total=len(file)):
        gdf, failed_row = download_and_process_tile(row, mly_key, retries)
        
        if gdf is not None and len(gdf):
            continent_file.append(gdf)
        elif failed_row is not None:
            failed_tiles.append(failed_row)
    
    if len(continent_file):
        continent_file = gpd.GeoDataFrame(pd.concat(continent_file), geometry="geometry")
    else:
        continent_file = None
    
    if len(failed_tiles):
        failed_tiles = pd.DataFrame(failed_tiles, columns=file.columns).reset_index(drop=True)
        failed_tiles_gdf = gpd.GeoDataFrame(failed_tiles, geometry="geometry")
    else:
        failed_tiles_gdf = None

    return continent_file, failed_tiles_gdf

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    retries = cfg['metadata_params']['retries']
    zoom_level = 14
    mly_key = cfg['params']['mly_key']

    tiles_save_dir = os.getcwd()
    completed_tiles_dir = os.getcwd()
    failed_tiles_dir = os.getcwd()

    logging.basicConfig(level=logging.INFO)

    for directory in [tiles_save_dir, completed_tiles_dir, failed_tiles_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory,exist_ok=True)
        
    file = gpd.GeoDataFrame(pd.DataFrame({'x':[0], 'y':[0], 'z': [0], 'geometry':[None]}))
    continent_file, failed_tiles_gdf = process_tile_file(file, mly_key, retries)
    filename = 'dlr.gpkg'
    if continent_file is not None:
        continent_file.to_file(os.path.join(completed_tiles_dir, f"finished_{filename}"), driver="GPKG")
    