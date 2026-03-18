"""Process and partition OSM highway data with geographic and tile information.

This script filters road network (highway) data from OSM parquet files, adds
geographic context (continent/country), assigns zoom-level tiles, and creates
tile-partitioned output for efficient spatial queries.

**Workflow:**
1. Load OSM parquet files containing highway network data with tags
2. Filter for valid highways:
   - LineString geometries only
   - tags['highway'] field not NULL
   - visible = TRUE (not deleted)
   - latest = TRUE (most recent version)
3. Perform spatial intersections:
   - LEFT JOIN with continent polygons → adds continent column
   - LEFT JOIN with country polygons → adds country column
4. Assign zoom-level tiles to each segment
5. Create tile-partitioned output structure:
   - One directory per tile: highways_{tile}/
   - One parquet file per tile: highways_{osm_filename}
6. Optional: chunk large tiles into multiple parquet files

**Configuration (config.yaml):**
- params.zoom_level - Tile zoom level (must match other pipeline scripts)
- paths.osm_data_dir - Input OSM data directory
- paths.processed_osm_data_dir - Output directory for processed highways
- filenames.continents_filename - Path to continent polygons (parquet)
- filenames.country_filename - Path to country polygons (parquet)

**Input Requirements:**
OSM parquet must have columns:
- geometry (WKB format)
- tags (map/dict with 'highway', 'surface' keys)
- geometry_type (VARCHAR: 'LineString')
- visible (BOOLEAN)
- latest (BOOLEAN)
- xzcode (struct with zoom-encoded geometry)
- contrib_id, country_iso_a3, osm_type, osm_id, length, centroid, bbox

**Output:**
Tile-partitioned parquet files with columns:
- continent - Continent name (from intersection)
- country - Country name (from intersection)
- (all input columns)
- z{zoom_level}_tiles - Tile identifier (e.g., z8_tiles)
- osm_tags_highway - Highway type (primary, secondary, residential, etc.)
- osm_tags_surface - Surface type (asphalt, concrete, gravel, etc.)

**Features:**
- DuckDB spatial operations for efficient processing
- Retry logic with exponential backoff
- Comprehensive logging at DEBUG, INFO, WARNING, ERROR levels
- One file per tile for parallelizable downstream processing
- Tile-partitioned output for efficient querying

**Usage:**
Run via bash automation script:
    bash highways_sort_hpc.sh
    
Or manually for single OSM file:
Called via highways_sort_hpc.sh for each OSM parquet file in parallel.
"""

import os
import sys
import time
import random
import logging
import duckdb
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from start import load_config
from metadata_intersections_and_filtering import finding_tiles_list_for_urban_areas, layer_intersections

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Ensure handler flushes after each message (important for HPC batch jobs)
for handler in logging.root.handlers:
    if hasattr(handler, 'stream'):
        handler.stream.flush()

def filter_and_copy_file(osm_filepath, saving_filedir, zoom_level, continent_filename, country_filename, retries=5, sleep_time=0.5):
    """Filter OSM highway data and add geographic context.
    
    Extracts highway network data from OSM parquet, filters for valid geometries,
    adds continent/country classification, and assigns zoom-level tiles.
    
    Args:
        osm_filepath: Path to input OSM parquet file.
        saving_filedir: Directory to save output parquet file.
        zoom_level: Zoom level for tile assignment (e.g., 8).
        continent_filename: Path to continents parquet file.
        country_filename: Path to countries parquet file.
        retries: Number of retry attempts for DuckDB operations (default: 5).
        sleep_time: Sleep duration between retries in seconds (default: 0.5).
    
    Returns:
        True if successful, False on error.
    """
    logger.info(f"Starting filter and copy: {os.path.basename(osm_filepath)}")
    sys.stdout.flush()
    if not os.path.exists(saving_filedir):
        os.makedirs(saving_filedir, exist_ok=True)
        logger.debug(f"Created output directory: {saving_filedir}")

    query = f"""
    COPY (
        WITH 
        continents AS (
            SELECT * --REPLACE (ST_GeomFromWKB(geometry) as geometry)
            FROM read_parquet('{continent_filename}')
        ),

        countries AS (
            SELECT * --REPLACE (ST_GeomFromWKB(geometry) as geometry)
            FROM read_parquet('{country_filename}')
        )

        SELECT 
            b.continent,
            c.country, 
            a.*,
            UNNEST(finding_tiles_list_for_urban_areas(ST_AsText(a.geometry), {int(zoom_level)})) AS z{int(zoom_level)}_tiles
        FROM 
            (SELECT 
                contrib_id, country_iso_a3, osm_type, osm_id,
                tags, tags['highway'][1] AS osm_tags_highway,
                tags['surface'][1] AS osm_tags_surface,
                geometry,
                --ST_GeomFromWKB(geometry) as geometry, 
                length, centroid, bbox, xzcode
            FROM read_parquet('{osm_filepath}')
            WHERE 1=1
                AND tags['highway'][1] IS NOT NULL
                AND geometry_type = 'LineString'
                AND latest = TRUE
                AND visible = TRUE
            ORDER BY xzcode.code) a
        LEFT JOIN continents b ON ST_Intersects(a.geometry, b.geometry)
        LEFT JOIN countries c ON ST_Intersects(a.geometry, c.geometry)
    )
    TO '{os.path.join(saving_filedir, f'highways_{os.path.basename(osm_filepath)}')}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """

    conn = None
    temp_filepath = f'temp_{int(random.randint(1,int(1e12)))}.db'
    try: 
        logger.debug(f"Creating DuckDB connection: {temp_filepath}")
        conn = duckdb.connect(temp_filepath)
        conn.execute("INSTALL SPATIAL; LOAD SPATIAL;")
        conn.create_function("finding_tiles_list_for_urban_areas", finding_tiles_list_for_urban_areas, ['varchar','double'], 'varchar[]')
        logger.debug("Registered custom tile-finding function")
        
        for attempt in range(retries):
            try:
                logger.debug(f"Executing filter query (attempt {attempt+1}/{retries})")
                #logger.warning(conn.execute(f"""
                #                            SELECT * REPLACE (ST_AStext(geometry) as geometry)
                #                            """).df().head())        
                logging.warning(conn.execute(f"""
                                             SELECT 
                                                contrib_id, country_iso_a3, osm_type, osm_id,
                                                tags, tags['highway'][1] AS osm_tags_highway,
                                                tags['surface'][1] AS osm_tags_surface,
                                                geometry,geometry_type,
                                                --ST_GeomFromWKB(geometry) as geometry, 
                                                length, centroid, bbox, xzcode
                                            FROM (SELECT * FROM read_parquet('{osm_filepath}') WHERE tags['highway'][1] IS NOT NULL LIMIT 1000)
                                            WHERE 1=1
                                                  AND tags['highway'][1] IS NOT NULL
                                            --    AND geometry_type = 'LineString'
                                            --    AND latest = TRUE
                                            --    AND visible = TRUE
                                            ORDER BY xzcode.code""").df().geometry_type)
                conn.execute(query)
                logger.info(f"Successfully filtered and copied: {os.path.basename(osm_filepath)}")
                sys.stdout.flush()
                break
            except Exception as err:
                logger.warning(f"Attempt {attempt+1}/{retries} failed: {err}")
                time.sleep(sleep_time)
        return True
    except Exception as err:
        logger.error(f"Error during extraction: {osm_filepath}: {err}", exc_info=True)
        sys.stdout.flush()
        return False
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            logger.debug(f"Cleaned up temp database: {temp_filepath}")

def process_single_tile(tile, saving_dir, osm_dir, zoom_level, chunk_size=5000000, retries=5, sleep_time=0.5):
    """Process OSM highway data for a single tile with optional chunking.
    
    Extracts all highways for a specific tile from the OSM directory,
    optionally splits into chunks if total exceeds chunk_size.
    
    Args:
        tile: Tile identifier (e.g., '158-136-8').
        saving_dir: Base directory for tile output (tile subdirectory created).
        osm_dir: Directory containing filtered OSM parquet files.
        zoom_level: Zoom level for tile filtering.
        chunk_size: Maximum rows per output chunk (default: 5000000).
        retries: Number of retry attempts (default: 5).
        sleep_time: Sleep duration between retries in seconds (default: 0.5).
    
    Returns:
        True if successful, False on error.
    """
    logger.info(f"Processing tile: {tile}")
    sys.stdout.flush()
    saving_dir = os.path.join(saving_dir, f'tile={tile}')
    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir, exist_ok=True)
        logger.debug(f"Created tile directory: {saving_dir}")

    conn = None
    temp_filepath = f'temp_{int(random.randint(1,int(1e12)))}.db'
    try: 
        logger.debug(f"Creating DuckDB connection for tile {tile}")
        conn = duckdb.connect(temp_filepath)

        count_query = f"""
        SELECT COUNT(*) AS n
        FROM read_parquet('{osm_dir}/*.parquet')
        WHERE z{int(zoom_level)}_tiles = '{tile}'
        """
        total_rows = None
        for attempt in range(retries):
            try:
                logger.debug(f"Counting rows for tile {tile} (attempt {attempt+1}/{retries})")
                total_rows = conn.execute(count_query).fetchone()[0]
                logger.info(f"Tile {tile} has {total_rows} highway segments")
                break
            except Exception as err:
                logger.warning(f"Attempt {attempt+1}/{retries} failed: {err}")
                time.sleep(sleep_time)
        
        if total_rows is None or total_rows == 0:
            logger.warning(f"No data found for tile {tile}")
            return True

        chunk_idx = 0
        offset = 0
        while offset < total_rows:
            outfile = os.path.join(saving_dir, f"osm_highways_{tile}_{chunk_idx:04d}.parquet")
            query = f"""
            COPY (
                SELECT *
                FROM read_parquet('{osm_dir}/*.parquet')
                WHERE z{int(zoom_level)}_tiles = '{tile}'
                LIMIT {chunk_size} OFFSET {offset}
            )
            TO '{outfile}'
            (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            for attempt in range(retries):
                try:
                    logger.debug(f"Writing chunk {chunk_idx} for tile {tile} (attempt {attempt+1}/{retries})")
                    conn.execute(query)
                    logger.debug(f"Successfully wrote chunk {chunk_idx}: {os.path.basename(outfile)}")
                    break
                except Exception as err:
                    logger.warning(f"Attempt {attempt+1}/{retries} failed: {err}")
                    time.sleep(sleep_time)
            offset += chunk_size
            chunk_idx += 1
        
        logger.info(f"Completed tile {tile}: {chunk_idx} chunks written")
        sys.stdout.flush()
        return True
    except Exception as err:
        logger.error(f"Error during tile extraction: {tile}: {err}", exc_info=True)
        sys.stdout.flush()
        return False
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            logger.debug(f"Cleaned up temp database: {temp_filepath}")

def hive_partition_osm(osm_dir, saving_dir, zoom_level, max_workers, args):
    """Partition OSM highway data into tile-based structure.
    
    Discovers unique tiles in the OSM dataset and creates tile-based
    partition using parallel processing.
    
    Args:
        osm_dir: Directory containing filtered OSM parquet files.
        saving_dir: Output directory for tile-partitioned data.
        zoom_level: Zoom level for tile extraction.
        max_workers: Maximum parallel processes.
        args: Dictionary of additional arguments (chunk_size, retries, sleep_time).
    
    Returns:
        None. Creates tile-partitioned output structure.
    """
    logger.info(f"Starting hive partitioning of OSM data at zoom level {zoom_level}")
    sys.stdout.flush()
    
    try:
        logger.debug(f"Discovering unique tiles in {osm_dir}")
        tiles_list = duckdb.sql(
            f"SELECT DISTINCT z{int(zoom_level)}_tiles FROM read_parquet('{osm_dir}/*.parquet')").df()[f"z{int(zoom_level)}_tiles"].tolist()
        logger.info(f"Found {len(tiles_list)} unique tiles to process")
        logger.debug(f"Tiles: {tiles_list[:10]}{'...' if len(tiles_list) > 10 else ''}")
        sys.stdout.flush()
        
        logger.info(f"Processing tiles with {max_workers} parallel workers")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_single_tile, tile, saving_dir, osm_dir, zoom_level, **args) for tile in tiles_list]

        completed = 0
        failed = 0
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    completed += 1
                else:
                    failed += 1
            except Exception as err:
                logger.error(f"Tile processing failed: {err}", exc_info=True)
                failed += 1
        
        logger.info(f"Hive partitioning complete: {completed} tiles processed, {failed} failed")
        sys.stdout.flush()
    except Exception as err:
        logger.error(f"Error during hive partitioning: {err}", exc_info=True)

def process_file(args):
    """Process a single OSM parquet file with geographic enrichment.
    
    Handles one-time layer intersection setup if this is the first file,
    then filters and copies the OSM file with geographic and tile info.
    
    Args:
        args: Tuple containing:
            - filename: Name of OSM parquet file to process
            - is_first: Boolean indicating if this is the first file (triggers setup)
            - ohsome_osm_dir: Input directory with raw OSM files
            - osm_saving_dir: Output directory for filtered OSM files
            - zoom_level: Zoom level for tile assignment
            - continent_filepath: Path to continents file
            - country_filepath: Path to countries file
            - continent_filename: Config path for continents
            - country_filename: Config path for countries
            - overture_url: S3 URL for Overture Maps data
            - ghsl_filename: Config path for GHSL
            - africapolis_filename: Config path for Africapolis
            - retries: Number of retry attempts
            - sleep_time: Sleep between retries
    
    Returns:
        urban_filepaths if first file (for geographic layer setup), else None.
    """
    (filename, is_first, ohsome_osm_dir, osm_saving_dir, continents_dir, processed_dir,
     zoom_level, continent_filepath, country_filepath,
     continent_filename, country_filename,
     overture_url, ghsl_filename, africapolis_filename,
     retries, sleep_time) = args

    logger.info(f"Processing OSM file: {filename} (first_file={is_first})")
    if is_first:
        logger.info("Performing one-time geographic layer setup")
        urban_filepaths = layer_intersections(
            is_first, processed_dir, continents_dir,
            continent_filename, country_filename,
            overture_url, ghsl_filename, africapolis_filename
        )
        logger.debug(f"Layer setup complete: {len(urban_filepaths)} layers prepared")
    else:
        urban_filepaths = None

    logger.info(f"Starting filter and copy for: {filename}")

    result = filter_and_copy_file(
        os.path.join(ohsome_osm_dir, filename),
        osm_saving_dir,
        zoom_level,
        continent_filepath,
        country_filepath,
        retries,
        sleep_time
    )
    
    if result:
        logger.info(f"Successfully processed: {filename}")
    else:
        logger.error(f"Failed to process: {filename}")
    
    return urban_filepaths


def main():
    """Main entry point for OSM highway processing pipeline.
    
    Orchestrates the complete workflow:
    1. Loads configuration from config.yaml
    2. Discovers all OSM parquet files in input directory
    3. Filters highways and adds geographic/tile information
    4. Creates tile-partitioned output structure
    
    Process runs in parallel with configurable worker count.
    """
    logger.info("Starting OSM highway processing pipeline")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    logger.debug("Configuration loaded")
        
    ohsome_osm_dir = os.path.abspath(cfg['paths']["ohsome_osm_dir"])
    osm_saving_dir = os.path.abspath(cfg['paths']["osm_saving_dir"])
    osm_partitioned_dir = os.path.abspath(cfg['paths']["osm_partitioned_dir"])
    processed_dir = os.path.abspath(cfg['paths']['processed_dir'])
    continents_dir = os.path.abspath(cfg['paths']['continents_dir'])

    zoom_level = cfg['params']['zoom_level']
    logger.info(f"Configuration: zoom_level={zoom_level}, ohsome_dir={ohsome_osm_dir}")
    
    args = {
        'chunk_size' : cfg['params']['n_max_rows_parquet'],
        'retries' : cfg['metadata_params']['retries'],
        'sleep_time' :  cfg['metadata_params']['sleep_time']
    }
    max_workers = cfg['metadata_params']['max_workers']
    logger.debug(f"Processing args: chunk_size={args['chunk_size']}, max_workers={max_workers}")
    
    continent_filename = cfg['filenames']['continents_filename']
    overture_url = cfg['filenames']['overture_url']
    country_filename = cfg['filenames']['country_filename']
    ghsl_filename = cfg['filenames']['ghsl_filename']
    africapolis_filename = cfg['filenames']['africapolis_filename']
    continents_dir = os.path.abspath(cfg['paths']['continents_dir'])
    country_filepath = os.path.join(processed_dir, f'intersected_{country_filename}')
    
    logger.debug(f"Geographic files: continents={continent_filename}, country={country_filename}")
    logger.info(f"Discovering OSM files in: {ohsome_osm_dir}")
    filenames = [f for f in os.listdir(ohsome_osm_dir) if 'way' in f and f.endswith('.parquet')]
    logger.info(f"Found {len(filenames)} OSM parquet files to process")
    
    if not filenames:
        logger.warning(f"No OSM parquet files found in {ohsome_osm_dir}")
        return
    
    tasks = [
        (
            f, f==filenames[0],
            ohsome_osm_dir, osm_saving_dir, continents_dir, processed_dir,
            zoom_level, continent_filename, country_filepath,
            continent_filename, country_filename,
            overture_url, ghsl_filename, africapolis_filename,
            args['retries'], args['sleep_time']
        )
        for f in filenames
    ]
    logger.info(f"Processing {len(tasks)} OSM files with {max_workers} workers")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_file, tasks))
    
    logger.info("OSM file filtering complete, starting tile partitioning")
    hive_partition_osm(osm_saving_dir, osm_partitioned_dir, zoom_level, max_workers, args)
    
    logger.info("OSM highway processing pipeline complete")
    
if __name__ == '__main__':
    main()
