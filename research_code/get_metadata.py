"""Download and process Mapillary metadata for image sequences.

This script downloads metadata for image sequences found in completed tiles,
tracking which sequences have been processed to allow resumable downloads.
It processes tiles in random order with configurable batch sizes, connection
thresholds, and retry logic. Previously downloaded sequences are skipped to
allow interrupted downloads to resume.

Metadata is downloaded using the Mapillary API and saved to CSV files
for each tile, with missing sequences tracked separately for retry attempts.

Supports parallel execution across 10 instances, with each instance processing
a deterministic chunk of tiles.
"""

import sys
import os
import logging
import random
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import pandas as pd
import geopandas as gpd
from metadata_download import get_metadata
from start import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
number_of_jobs_running = 1
number_of_requests = 0
DETERMINISTIC_SEED = 42  # For reproducible tile division across instances
NUM_CHUNKS = 10  # Always divide into 10 chunks for 10 parallel instances

def process_single_tile(tile, tiles_gdf, tiles_col, data_dir, metadata_args):
    """Download and process metadata for all sequences in a single tile.
    
    Retrieves all sequences for a tile, checks which have already been downloaded,
    and downloads metadata for new sequences. Previously downloaded sequences are
    skipped using an existing metadata file if it exists.
    
    Args:
        tile: Tile identifier string (e.g., '4-7-4' for x-y-z format).
        tiles_gdf: GeoDataFrame containing tile and sequence information.
        tiles_col: Column name in tiles_gdf containing tile identifiers.
        data_dir: Directory path where metadata CSV files are saved.
        metadata_args: Dictionary of arguments to pass to get_metadata() including
                      mly_key, columns, args_dict, max_workers, batch_size, windows.
    
    Returns:
        None. Saves metadata to CSV files in data_dir.
    """
    logging.info(f"Processing tile: {tile}")
    sequences = tiles_gdf[tiles_gdf[tiles_col]==tile]['id'].unique().tolist()
    logging.debug(f"Found {len(sequences)} sequences in tile {tile}")

    file_unfiltered_metadata = os.path.join(data_dir, f'metadata_unfiltered_{tile}.csv')
    missing_sequences = os.path.join(data_dir, f'missing_sequences_{tile}.csv')

    # Load previously processed sequences
    if os.path.exists(file_unfiltered_metadata):
        old_metadata = pd.read_csv(file_unfiltered_metadata, usecols=['sequence'])
        old_sequences = set(old_metadata['sequence'].unique().tolist())
        logging.debug(f"Tile {tile}: {len(old_sequences)} sequences already processed")
    else:
        old_sequences = set([])

    # Filter out already processed ones
    sequences = [seq for seq in sequences if seq not in old_sequences]
    logging.info(f"Tile {tile}: {len(sequences)} new sequences to download")
    random.shuffle(sequences)

    if os.path.exists(missing_sequences):
        os.remove(missing_sequences)
        logging.debug(f"Removed previous missing sequences file for tile {tile}")
    
    try:
        get_metadata(sequences, missing_sequences, file_unfiltered_metadata, **metadata_args)
        logging.info(f"Successfully downloaded metadata for tile {tile}")
    except Exception as err:
        logging.error(f"Error downloading metadata for tile {tile}: {err}", exc_info=True)

def main():
    """Main entry point for metadata download process.
    
    Loads configuration, processes a chunk of tiles to download metadata for their
    sequences. Performs multiple passes (missing_attempts) to retry failed downloads.
    
    Supports parallel execution across 10 instances using deterministic tile chunking:
    - All instances use the same seed to divide tiles consistently
    - Each instance (from 1-10) processes one deterministic chunk
    - Within each chunk, tiles are randomized independently
    - ProcessPoolExecutor with max_workers ThreadPoolExecutor instances parallelizes
    
    Optional command-line arguments:
        instance_id: Integer from 1-10 indicating which chunk to process. If omitted,
                    processes all tiles sequentially (useful for single-instance runs).
    
    Configuration is loaded from config.yaml with parameters for:
    - Batch size, concurrency windows, and worker limits
    - API call limits and connection thresholds
    - Number of retry attempts for missing sequences
    
    Returns:
        None. Generates metadata CSV files in the configured output directory.
    """
    logging.info("Starting Mapillary metadata download script")
    
    # Parse optional instance_id argument (1-10)
    instance_id = None
    if len(sys.argv) > 1:
        try:
            instance_id = int(sys.argv[1])
            if instance_id < 1 or instance_id > NUM_CHUNKS:
                logging.warning(f"Invalid instance_id: {instance_id}. Expected 1-{NUM_CHUNKS}. Processing all tiles.")
                instance_id = None
            else:
                logging.info(f"Running in parallel mode: instance {instance_id}/{NUM_CHUNKS}")
        except ValueError:
            logging.warning(f"Invalid instance_id argument: {sys.argv[1]}. Expected integer 1-{NUM_CHUNKS}. Processing all tiles.")
    
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    logging.debug(f"Working directory: {os.getcwd()}")
    
    cfg = load_config()
    logging.info("Configuration loaded successfully")

    batch_size = cfg['metadata_params']['batch_size']
    windows = cfg['metadata_params']['windows']
    max_workers = int(os.environ.get('SLURM_CPUS_PER_TASK', cfg['metadata_params']['max_workers']))
    data_dir = cfg['paths']['raw_metadata_dir']
    args = {k:v for k,v in cfg['metadata_params'].items() if k in [
        'call_limit',
        'empty_data_attempts',
        'retries',
        'max_connections',
        'sleep_time'
    ]}
    
    logging.debug(f"Batch size: {batch_size}, Windows: {windows}, Max workers: {max_workers}")
    logging.debug(f"Output directory: {data_dir}")

    zoom_level = cfg['params']['zoom_level']
    mly_key = cfg['params']['mly_key']
    missing_attempts = cfg['metadata_params']['missing_attempts']
    columns = cfg['metadata_columns']
    tiles_col = f'z{zoom_level}_tiles'
    tiles_filepath = os.path.join(cfg['paths']['completed_tiles_dir'], f'finished_tiles_z{zoom_level}.gpkg')
    
    logging.debug(f"Zoom level: {zoom_level}, Tiles file: {tiles_filepath}")

    metadata_args = {
        'mly_key' : mly_key,
        "columns": columns,
        "args_dict" : args,
        "max_workers": max_workers,
        "batch_size":batch_size,
        "windows": windows
    }
    
    if not os.path.exists(tiles_filepath):
        logging.error(f"Tiles file not found: {tiles_filepath}")
        raise FileNotFoundError(f'Sequence list file {tiles_filepath} does not exist.')
    
    logging.info(f"Loading tiles from {tiles_filepath}")
    tiles_gdf = gpd.read_file(tiles_filepath, columns=['id', tiles_col])
    tiles = tiles_gdf[tiles_col].unique().tolist()
    logging.info(f"Found {len(tiles)} tiles total")
    
    # Divide tiles into chunks using deterministic seed
    rng = random.Random(DETERMINISTIC_SEED)
    rng.shuffle(tiles)  # Shuffle with fixed seed for reproducibility
    
    # Split into NUM_CHUNKS chunks
    chunk_size = (len(tiles) + NUM_CHUNKS - 1) // NUM_CHUNKS  # Ceiling division
    chunks = [tiles[i*chunk_size:(i+1)*chunk_size] for i in range(NUM_CHUNKS)]
    
    if instance_id is not None:
        # Single instance processing
        chunk_idx = instance_id - 1  # Convert 1-10 to 0-9
        tiles_to_process = chunks[chunk_idx]
        logging.info(f"Instance {instance_id}: Processing {len(tiles_to_process)} tiles from chunk {chunk_idx}")
    else:
        # All tiles, flattened
        tiles_to_process = tiles
        logging.info(f"Processing all {len(tiles_to_process)} tiles sequentially")
    
    # Randomize within this instance's chunk
    random.shuffle(tiles_to_process)

    # Process tiles with retry attempts for missing sequences
    logging.info(f"Starting metadata download with {missing_attempts} retry attempts")
    for attempt in range(missing_attempts):
        logging.info(f"Processing attempt {attempt + 1}/{missing_attempts}")
        for tile in tiles_to_process:
            process_single_tile(tile, tiles_gdf, tiles_col, data_dir, metadata_args)
    
    logging.info("Metadata download process completed")

if __name__ == '__main__':
    main()
