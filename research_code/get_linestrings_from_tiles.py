
"""Download and process Mapillary vector tiles to extract linestrings.

This script fetches vector tiles from the Mapillary API for a set of tile
coordinates, converts them to GeoJSON features, and combines them into
GeoDataFrames. Tiles that fail to download are tracked separately for
retry attempts. Results are saved as GeoPackage files for both successful
and failed tiles.

The script processes tiles in batches using a progress bar and includes
retry logic for failed downloads.
"""

import requests
import logging
import os
import sys
from vt2geojson import tools as vt2geojson_tools
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from start import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_and_process_tile(row, mly_key, retries=3):
    """Download a vector tile from Mapillary and convert to GeoDataFrame.
    
    Fetches a single vector tile from the Mapillary API using z/x/y coordinates,
    converts the vector tile bytes to GeoJSON features, and creates a GeoDataFrame.
    Includes retry logic for failed downloads.
    
    Args:
        row: A pandas Series/dict with keys 'z', 'x', 'y' containing tile coordinates.
        mly_key: Mapillary API access token (string).
        retries: Maximum number of download attempts (default: 3).
    
    Returns:
        A tuple (gdf, failed_row) where:
            - gdf: GeoDataFrame with features from the tile, or None if failed
            - failed_row: The original row if download failed, or None if successful
    """
    z = row["z"]
    x = row["x"]
    y = row["y"]
    endpoint = "mly1_public"
    url = f"https://tiles.mapillary.com/maps/vtp/{endpoint}/2/{z}/{x}/{y}?access_token={mly_key}"
    
    logging.debug(f"Attempting to download tile z{z}/x{x}/y{y}")
    
    attempt = 0
    while attempt < retries:
        try:
            r = requests.get(url, timeout=10)
            assert r.status_code == 200, r.content
            vt_content = r.content
            features = vt2geojson_tools.vt_bytes_to_geojson(vt_content, x, y, z)
            gdf = gpd.GeoDataFrame.from_features(features)
            gdf[f'z{z}_tiles'] = f'{x}-{y}-{z}'
            logging.debug(f"Successfully downloaded tile z{z}/x{x}/y{y} with {len(gdf)} features")
            return gdf, None
        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt}/{retries} failed for tile z{z}/x{x}/y{y}: {e}")
    
    logging.error(f"Failed to download tile z{z}/x{x}/y{y} after {retries} attempts")
    return None, row

def process_tile_file(file, mly_key, retries=3):
    """Process all tiles in a GeoDataFrame by downloading and converting them.
    
    Iterates through a GeoDataFrame of tile coordinates, downloads each tile from
    the Mapillary API, and combines results. Tracks successfully processed tiles
    and those that failed download.
    
    Args:
        file: A GeoDataFrame with columns 'z', 'x', 'y' containing tile coordinates.
        mly_key: Mapillary API access token (string).
        retries: Maximum download attempts per tile (default: 3).
    
    Returns:
        A tuple (continent_file, failed_tiles_gdf) where:
            - continent_file: GeoDataFrame combining all successful tiles, or None if empty
            - failed_tiles_gdf: GeoDataFrame of tiles that failed to download, or None if empty
    """
    logging.info(f"Starting to process {len(file)} tiles")
    
    continent_file = []
    failed_tiles = []

    for index, row in tqdm(file.iterrows(), total=len(file), desc="Processing tiles"):
        gdf, failed_row = download_and_process_tile(row, mly_key, retries)
        
        if gdf is not None and len(gdf):
            continent_file.append(gdf)
        elif failed_row is not None:
            failed_tiles.append(failed_row)
    
    logging.info(f"Completed processing: {len(continent_file)} successful tiles, {len(failed_tiles)} failed tiles")
    
    if len(continent_file):
        continent_file = gpd.GeoDataFrame(pd.concat(continent_file), geometry="geometry")
        logging.info(f"Combined {len(continent_file)} features from successful tiles")
    else:
        continent_file = None
        logging.warning("No tiles were successfully processed")
    
    if len(failed_tiles):
        failed_tiles = pd.DataFrame(failed_tiles, columns=file.columns).reset_index(drop=True)
        failed_tiles_gdf = gpd.GeoDataFrame(failed_tiles, geometry="geometry")
        logging.warning(f"{len(failed_tiles_gdf)} tiles failed to download")
    else:
        failed_tiles_gdf = None

    return continent_file, failed_tiles_gdf

if __name__ == "__main__":
    logging.info("Starting Mapillary linestrings extraction script")
    
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logging.debug(f"Working directory: {os.getcwd()}")
    
    cfg = load_config()
    logging.info("Configuration loaded successfully")

    retries = cfg['metadata_params']['retries']
    zoom_level = cfg['params']['zoom_level']
    mly_key = cfg['params']['mly_key']

    tiles_save_dir = cfg['paths']['tiles_save_dir']
    completed_tiles_dir = cfg['paths']['completed_tiles_dir']
    failed_tiles_dir = cfg['paths']['failed_tiles_dir']
    
    logging.debug(f"Zoom level: {zoom_level}, Retries: {retries}")
    logging.debug(f"Input dir: {tiles_save_dir}")
    logging.debug(f"Output dirs: {completed_tiles_dir}, {failed_tiles_dir}")

    # Create output directories if they don't exist
    for directory in [tiles_save_dir, completed_tiles_dir, failed_tiles_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logging.info(f"Created directory: {directory}")
    
    # Find and process all tile files matching the zoom level
    tile_files = [f for f in os.listdir(tiles_save_dir) 
                   if f.startswith(f"tiles_z{zoom_level}") and f.endswith('.gpkg')]
    logging.info(f"Found {len(tile_files)} tile file(s) to process")
    
    for filename in tile_files:
        logging.info(f"Processing file: {filename}")
        input_filepath = os.path.join(tiles_save_dir, filename)
        
        try:
            file = gpd.read_file(input_filepath)
            logging.info(f"Loaded {len(file)} tiles from {filename}")
            
            continent_file, failed_tiles_gdf = process_tile_file(file, mly_key, retries)
            
            if continent_file is not None:
                continent_file.drop_duplicates(subset=['id'], inplace=True)
                continent_file.reset_index(drop=True, inplace=True)
                output_filepath = os.path.join(completed_tiles_dir, f"finished_{filename}")
                continent_file.to_file(output_filepath, driver="GPKG")
                logging.info(f"Saved {len(continent_file)} completed features to {output_filepath}")
            
            if failed_tiles_gdf is not None:
                failed_tiles_gdf.drop_duplicates(subset=['id'], inplace=True)
                failed_tiles_gdf.reset_index(drop=True, inplace=True)
                output_filepath = os.path.join(failed_tiles_dir, f"failed_{filename}")
                failed_tiles_gdf.to_file(output_filepath, driver="GPKG")
                logging.warning(f"Saved {len(failed_tiles_gdf)} failed tiles to {output_filepath}")
        
        except Exception as e:
            logging.error(f"Error processing file {filename}: {e}", exc_info=True)
    
    logging.info("Mapillary linestrings extraction completed")