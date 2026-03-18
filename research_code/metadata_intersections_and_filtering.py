"""Perform spatial intersections and filtering on metadata with geographic layers.

This module coordinates spatial intersections between Mapillary image metadata and
various geographic data layers (continents, countries, urban areas). It filters
metadata based on image sequence density thresholds to remove redundant sequences
while preserving spatial coverage. The workflow includes:

1. Loading urban layer definitions (GHSL, Africapolis)
2. Intersecting metadata with continent and country polygons
3. Assigning zoom-level tiles to both urban areas and metadata points
4. Filtering images by distance thresholds (different for urban vs rural areas)
5. Outputting unfiltered and filtered parquet files with tile partitioning

Uses DuckDB spatial operations for efficient large-scale geometry processing.
Configuration loaded from config.yaml for all parameters.
"""

import os
import sys
import shutil
import random
import time
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
import mercantile
from shapely import from_wkt
from pygeodesy.simplify import simplify1,simplifyRDP
from pygeodesy.points import Numpy2LatLon
import duckdb
from datetime import datetime
from start import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def filtering_simple(latlong, distance):
    """Simplify point sequence using radial distance method.
    
    Removes points that are less than the specified distance apart from the
    previous point, preserving the first point in each sequence.
    
    Args:
        latlong: List of [latitude, longitude] coordinate pairs.
        distance: Minimum distance in meters between kept points.
    
    Returns:
        Array of 1-indexed point indices to keep from the original sequence.
    """
    logger.debug(f"Filtering {len(latlong)} points with distance threshold {distance}m")
    array = np.array(latlong)
    points = Numpy2LatLon(array)
    results = np.array(simplify1(points,indices=True,distance=distance,limit=0))
    results = results + np.ones(len(results)).astype(int)
    logger.debug(f"Filtered to {len(results)} points (kept {len(results)/len(latlong)*100:.1f}%)")
    return results

def filtering_RDP(latlong, distance):
    """Simplify point sequence using Ramer-Douglas-Peucker algorithm.
    
    Reduces points in sequence while preserving overall shape, using maximum
    perpendicular distance from line as threshold.
    
    Args:
        latlong: List of [latitude, longitude] coordinate pairs.
        distance: Maximum distance in meters for point deviation from line.
    
    Returns:
        Array of 1-indexed point indices to keep from the original sequence.
    """
    logger.debug(f"Filtering {len(latlong)} points using RDP with distance {distance}m")
    array = np.array(latlong)
    points = Numpy2LatLon(array)
    results = np.array(simplifyRDP(points,indices =True,distance=distance))
    results = results + np.ones(len(results)).astype(int)
    logger.debug(f"Filtered to {len(results)} points (kept {len(results)/len(latlong)*100:.1f}%)")
    return results

def finding_tiles_for_points(longlat, zoom_level):
    """Find the XYZ tile coordinates for a given longitude/latitude point.
    
    Args:
        longlat: [longitude, latitude] coordinate pair.
        zoom_level: Zoom level for tile calculation.
    
    Returns:
        String in format 'x-y-z' representing the tile coordinates.
    """
    tile = mercantile.tile(*longlat,zoom_level)
    return f'{int(tile.x)}-{int(tile.y)}-{int(tile.z)}'

def finding_tiles_list_for_urban_areas(polygon_str, zoom_level):
    """Find all XYZ tiles that intersect a polygon.
    
    Args:
        polygon_str: WKT format polygon string.
        zoom_level: Zoom level for tile calculation.
    
    Returns:
        List of strings in format 'x-y-z' for all intersecting tiles.
    """
    bbox = from_wkt(polygon_str).bounds
    tiles = [f'{int(tile.x)}-{int(tile.y)}-{int(tile.z)}' for tile in mercantile.tiles(*bbox, zoom_level)]
    logger.debug(f"Urban area intersects {len(tiles)} tiles at zoom level {zoom_level}")
    return tiles

def download_overture_maps(url, filepath):
    """Download country-level polygons from Overture Maps using DuckDB.
    
    Fetches division boundaries at country level from Overture Maps S3 bucket
    and saves to local parquet file with zstd compression.
    
    Args:
        url: S3 URL to Overture Maps release (base path).
        filepath: Local path where parquet file will be saved.
    
    Returns:
        None. Writes parquet file to filepath.
    """
    logger.info(f"Downloading Overture Maps data from {url}")
    query = "INSTALL SPATIAL; LOAD SPATIAL;"
    download_query  = f"""
    COPY(
        SELECT * -- REPLACE(ST_AsWKB(geometry)) as geometry
        FROM read_parquet('{url}', filename=true, hive_partitioning=1)
        WHERE subtype = 'country'
    )
    TO '{filepath}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    conn = None
    temp_filepath = f'temp_{int(random.randint(1,int(1e12)))}.db'
    try: 
        conn = duckdb.connect(temp_filepath)
        conn.execute(query)
        conn.execute(download_query)
        logger.info(f"Successfully downloaded Overture data to {filepath}")
    except Exception as err:
        logger.error(f"Error during Overture Maps download: {err}", exc_info=True)
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)

def intersection(intersected_filepath, intersecting_filepath, column, output_file):
    """Perform spatial intersection between two parquet geometry files.
    
    Loads two parquet files with WKB geometries, performs spatial intersection,
    and joins the specified column from intersecting file to intersected file.
    Uses LEFT JOIN to preserve all records from intersected file.
    
    Args:
        intersected_filepath: Path to base parquet file (geometries preserved).
        intersecting_filepath: Path to reference parquet file for intersection.
        column: Column name to join from intersecting file.
        output_file: Output path for resulting parquet file.
    
    Returns:
        None. Writes parquet file to output_file.
    """
    logger.info(f"Intersecting {intersected_filepath} with {intersecting_filepath}")
    query = f"""
    COPY(
    WITH
    intersected_file AS(
        SELECT * --REPLACE ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{intersected_filepath}')
        ),
    intersecting_file AS(
        SELECT * --REPLACE ST_GeomFromWKB(geometry) as geometry
        FROM read_parquet('{intersecting_filepath}')
        )

    SELECT a.* REPLACE (ST_AsWKB(a.geometry) as geometry),
    b.{column}
    FROM intersected_file a
    LEFT JOIN intersecting_file b
    ON ST_Intersects(a.geometry, b.geometry)
    )
    TO '{output_file}'
    (FORMAT PARQUET,COMPRESSION zstd);
    """
    conn = None
    temp_filepath = f'temp_{int(random.randint(1,int(1e12)))}.db'
    try: 
        conn = duckdb.connect(temp_filepath)
        conn.execute("INSTALL SPATIAL; LOAD SPATIAL;")
        conn.execute(query)
        logger.debug(f"Intersection complete: {output_file}")
    except Exception as err:
        logger.error(f"Error during spatial intersection: {err}", exc_info=True)
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        
def intersections_with_metadata(metadata_filename, continent_filename, 
                                 country_filename, ghsl_filename, africapolis_filename, 
                                 unfiltered_output_filename, filtered_output_filename, 
                                 ghsl_string, africapolis_string, 
                                 zoom_level=8, filter_list=[100, 1000]):
    """Intersect metadata with spatial layers and filter by distance thresholds.
    
    Main processing function that:
    1. Intersects metadata with continent/country polygons
    2. Intersects with urban areas (GHSL and optional Africapolis)
    3. Assigns zoom-level tiles to metadata points
    4. Filters sequences by distance within urban/rural classifications
    
    Args:
        metadata_filename: Path to input metadata parquet file.
        continent_filename: Path to continents parquet file.
        country_filename: Path to countries parquet file.
        ghsl_filename: Path to GHSL urban areas parquet file.
        africapolis_filename: Path to Africapolis file or None.
        unfiltered_output_filename: Output path for unfiltered results.
        filtered_output_filename: Output path for filtered results.
        ghsl_string: Column names from GHSL to join (comma-separated).
        africapolis_string: Column names from Africapolis to join.
        zoom_level: Zoom level for tile calculations (default: 8).
        filter_list: [urban_threshold, rural_threshold] in meters.
    
    Returns:
        True if successful, False on error.
    """
    logger.info(f"Starting metadata intersection for {metadata_filename}")
    logger.debug(f"Parameters: zoom_level={zoom_level}, filters={filter_list}")
    
    if africapolis_filename is not None:
        filtering_str = f"{ghsl_string.split(',')[0][1:]} IS NULL AND {africapolis_string.split(',')[0][1:]} IS NULL"
    else:
        filtering_str = f"{ghsl_string.split(',')[0][1:]} IS NULL"

    query_intersection_africa = f"""
    COPY(
        WITH
        continents AS (
            SELECT *
            FROM read_parquet('{continent_filename}')
        ),

        countries AS (
            SELECT *
            FROM read_parquet('{country_filename}')
        ),
            
        urban_areas AS (
            SELECT *,
                finding_tiles_list_for_urban_areas(geometry, {int(zoom_level)}) AS z{int(zoom_level)}_tiles_ghs
            FROM ghsl
        ),
            
        africapolis AS (
            SELECT *,
                finding_tiles_list_for_urban_areas(ST_AsText(geometry),{int(zoom_level)}) as z{int(zoom_level)}_tiles_africapolis
            FROM read_parquet('{africapolis_filename}')
        ),
        
        metadata AS (
            SELECT *,
                [long,lat] as longlat,
                finding_tiles_for_points(longlat, {int(zoom_level)}) as z{int(zoom_level)}_tiles
            FROM read_parquet('{metadata_filename}')
                WHERE long IS NOT NULL and lat IS NOT NULL
        )
        
        SELECT 
            a.* REPLACE ST_AsWKB(a.geometry) as geometry,
            b.continent,
            c.country,
            {ghsl_string.replace('.', 'd.')},
            {africapolis_string.replace('.', 'e.')}
        FROM metadata a
        LEFT JOIN continents b ON ST_Intersects(a.geometry, b.geometry)
        LEFT JOIN countries c ON ST_Intersects(a.geometry, c.geometry)
        LEFT JOIN urban_areas d ON c.country = d.country and list_contains(d.z{int(zoom_level)}_tiles_ghs, a.z{int(zoom_level)}_tiles) and ST_Intersects(a.geometry,ST_GeomFromText(d.geometry))
        LEFT JOIN africapolis e ON c.country = e.country and list_contains(e.z{int(zoom_level)}_tiles_africapolis, a.z{int(zoom_level)}_tiles) and ST_Intersects(a.geometry,e.geometry)      
        )
        TO '{unfiltered_output_filename}'
        (FORMAT 'parquet', COMPRESSION 'zstd');
        """  
    query_intersection_rest = f"""
    COPY(
        WITH
        
        continents AS (
            SELECT *
            FROM read_parquet('{continent_filename}')
        ),

        countries AS (
            SELECT *
            FROM read_parquet('{country_filename}')
        ),
                    
        urban_areas AS (
            SELECT *,
                finding_tiles_list_for_urban_areas(geometry, {int(zoom_level)}) AS z{int(zoom_level)}_tiles_ghs
            FROM ghsl
        ),
            
        metadata AS (
            SELECT *,
                [long,lat] as longlat,
                finding_tiles_for_points(longlat, {int(zoom_level)}) as z{int(zoom_level)}_tiles
            FROM read_parquet('{metadata_filename}')
                WHERE long IS NOT NULL and lat IS NOT NULL
        )
        
        SELECT 
            a.* REPLACE ST_AsWKB(a.geometry) as geometry,
            b.continent,
            c.country,
            {ghsl_string.replace('.', 'd.')}
        FROM metadata a
        LEFT JOIN continents b ON ST_Intersects(a.geometry, b.geometry)
        LEFT JOIN countries c ON ST_Intersects(a.geometry, c.geometry)
        LEFT JOIN urban_areas d ON c.country = d.country and list_contains(d.z{int(zoom_level)}_tiles_ghs, a.z{int(zoom_level)}_tiles) and ST_Intersects(a.geometry,ST_GeomFromText(c.geometry))   
        )
        TO '{unfiltered_output_filename}'
        (FORMAT 'parquet', COMPRESSION 'zstd');
        """  
    query_filtering = f"""
    COPY(
        WITH
        
        ordered_list AS (
        SELECT
            sequence,
            id,
            [lat, long] AS latlong,
            CASE 
                WHEN {filtering_str} THEN 0
                ELSE 1
            END AS urban
        FROM read_parquet('{unfiltered_output_filename}')
            WHERE lat IS NOT NULL
        ORDER BY timestamp
        ),
        
        urban_list AS (
            SELECT sequence, array_agg(id) AS id_list, array_agg(latlong) AS latlong
            FROM ordered_list
                WHERE urban = 1
            GROUP BY sequence
        ),
    
        rural_list AS (
            SELECT sequence, array_agg(id) AS id_list, array_agg(latlong) AS latlong
            FROM ordered_list
                WHERE urban = 0
            GROUP BY sequence
        ),
        
        sampled_images_urban AS (
            SELECT 
                sequence,
                UNNEST(list_select(id_list, filtering_simple(latlong, {filter_list[0]}))) AS sampled_image_id
            FROM urban_list
        ),
    
        sampled_images_rural AS (
            SELECT 
                sequence,
                UNNEST(list_select(id_list, filtering_simple(latlong, {filter_list[1]}))) AS sampled_image_id
            FROM rural_list
        )
    
        SELECT a.*
        FROM read_parquet('{unfiltered_output_filename}') a
        LEFT JOIN sampled_images_urban b ON a.id = b.sampled_image_id 
        LEFT JOIN sampled_images_rural c ON a.id = c.sampled_image_id
            WHERE b.sampled_image_id IS NOT NULL or c.sampled_image_id IS NOT NULL
    )
    TO '{filtered_output_filename}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    
    t1 = time.time()
    conn = None
    temp_filepath = f'temp_{int(random.randint(1,int(1e12)))}.db'
    try: 
        logger.debug(f"Creating DuckDB connection: {temp_filepath}")
        conn = duckdb.connect(temp_filepath)
        conn.execute("INSTALL SPATIAL; LOAD SPATIAL;")
        
        # Register custom functions
        logger.debug("Registering custom geometric functions")
        conn.create_function("filtering_simple", filtering_simple, ['double[][]','double'], 'int64[]')
        conn.create_function("filtering_RDP", filtering_RDP, ['double[][]','double'], 'int64[]')
        conn.create_function("finding_tiles_for_points", finding_tiles_for_points, ['double[]','double'], 'varchar')
        conn.create_function("finding_tiles_list_for_urban_areas", finding_tiles_list_for_urban_areas, ['varchar','double'], 'varchar[]')
        # Perform intersection based on africapolis availability
        logger.debug("Starting DuckDB spatial intersection queries")
        ghsl = conn.execute(f"SELECT * REPLACE(ST_AsText(geometry) AS geometry) FROM read_parquet('{ghsl_filename}')").df()
        ghsl.columns = ghsl.columns.str.lstrip('\ufeff')
        if africapolis_filename is None:
            logger.info("Executing intersection without Africapolis layer")
            conn.execute(query_intersection_rest)    
        else:  
            logger.info("Executing intersection with Africapolis layer")
            conn.execute(query_intersection_africa)

        t2 = time.time()
        logger.info(f'Intersections completed in {int(t2-t1)}s for: {metadata_filename}')

        # Execute filtering
        logger.info(f'Starting distance-based filtering: urban={filter_list[0]}m, rural={filter_list[1]}m')
        conn.execute(query_filtering)
        t3 = time.time()
        logger.info(f'Filtering completed in {int(t3-t2)}s')
        logger.info(f'Total processing time: {int(t3-t1)}s')
        return True
    except Exception as err:
        logger.error(f"Error during metadata intersection: {err}", exc_info=True)
        return False
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_filepath):
            os.remove(temp_filepath)

def layer_intersections(is_first, output_dir,  continents_dir, continents_filename, country_filename, overture_url, 
                        ghsl_filename, africapolis_filename):
    """Prepare and intersect urban layers with political boundaries.
    
    Performs one-time setup (when is_first=True):
    1. Merges continent shapefiles into single parquet
    2. Downloads country polygons from Overture Maps
    3. Converts GHSL and Africapolis shapefiles to parquet
    4. Intersects urban layers with continents and countries
    
    Intersections ensure urban layer geometries are linked to political units
    and have tile membership information.
    
    Args:
        is_first: Whether this is the first execution (triggers setup).
        output_dir: Directory for output files.
        continents_filename: Output path for merged continents parquet.
        country_filename: Path to country polygons file.
        overture_url: S3 URL for Overture Maps data.
        ghsl_filename: Path to GHSL shapefile or parquet.
        africapolis_filename: Path to Africapolis shapefile or None.
    
    Returns:
        List of paths: [ghsl_intersected, africapolis_intersected, country_intersected].
        Format excludes africapolis path if africapolis_filename is None.
    """
    logger.info(f"Starting layer intersection setup (is_first={is_first})")
    
    country_filename_new = os.path.join(output_dir, f'intersected_{country_filename}')
    ghsl_filename_new = os.path.join(output_dir, f"intersected_{ghsl_filename.replace('.gpkg', '.parquet')}")
    
    if africapolis_filename is not None:
        africapolis_filename_new = os.path.join(output_dir, f"intersected_{africapolis_filename.replace('.shp', '.parquet')}")
        urban_filepaths = [ghsl_filename_new, africapolis_filename_new]
    else:
        urban_filepaths = [ghsl_filename_new]

    # 1) Merge continent files
    if is_first:
        logger.info("Preparing geographic base layers")
        if not os.path.exists(continents_filename):
            logger.info(f"Creating continents layer from shapefiles")
            logger.debug(f"Looking for continents in: {continents_dir}")
            continent_files = [f for f in os.listdir(continents_dir) if f.endswith('.geojson')]
            continents = ['africa', 'asia', 'europe', 'north_america', 'oceania', 'south_america']
            continents_gdf = []

            for file in continent_files:
                filepath = os.path.join(continents_dir, file)
                logger.debug(f"Loading continent: {file}")
                gdf = gpd.read_file(filepath)

                for continent in continents:
                    if continent in file:
                        gdf['continent'] = continent

                continents_gdf.append(gdf)
            continents_gdf = gpd.GeoDataFrame(pd.concat(continents_gdf, ignore_index=True), crs=continents_gdf[0].crs)
            continents_gdf.to_parquet(continents_filename, compression='zstd', index=False)
            logger.info(f"Saved continents parquet: {continents_filename}")

        # 2) Get country file
        if not os.path.exists(country_filename_new):
            if os.path.exists(country_filename):
                logger.info(f"Copying existing country file")
                shutil.copy2(country_filename, country_filename_new)
            else:
                logger.info(f"Downloading country divisions from Overture Maps")
                download_overture_maps(overture_url, country_filename_new)

        # 3) Convert GHSL file if needed
        if not os.path.exists(ghsl_filename_new) and ghsl_filename.endswith('.gpkg'):
            logger.info(f"Converting GHSL layer to parquet")
            ghsl_gdf = gpd.read_file(ghsl_filename, layer=0)
            ghsl_gdf.columns = ghsl_gdf.columns.str.lstrip('\ufeff')
            ghsl_gdf = ghsl_gdf.to_crs(4326)
            ghsl_gdf.to_parquet(ghsl_filename_new, compression='zstd', index=False)
            logger.info(f"Saved GHSL parquet: {ghsl_filename_new}")

        # 4) Convert Africapolis file if present
        if africapolis_filename is not None:
            if not os.path.exists(africapolis_filename_new) and africapolis_filename.endswith('.shp'):
                logger.info(f"Converting Africapolis layer to parquet")
                africapolis_gdf = gpd.read_file(africapolis_filename)
                africapolis_gdf = africapolis_gdf.to_crs(4326)
                africapolis_gdf.to_parquet(africapolis_filename_new, compression='zstd', index=False)
                logger.info(f"Saved Africapolis parquet: {africapolis_filename_new}")

    # 5) Intersect urban files with continents and countries
    logger.info("Intersecting urban layers with political boundaries")
    
    for index, file in enumerate(urban_filepaths):
        continent_filepath = os.path.join(os.path.dirname(file), f'continent_intersected_{os.path.basename(file)}')
        country_filepath = os.path.join(os.path.dirname(file), f'country_intersected_{os.path.basename(file)}')

        if not os.path.exists(continent_filepath) and is_first:
            logger.info(f"Intersecting {os.path.basename(file)} with continents")
            intersection(file, continents_filename, 'continent', continent_filepath)
        if not os.path.exists(country_filepath) and is_first:
            logger.info(f"Intersecting {os.path.basename(file)} with countries")
            intersection(continent_filepath, country_filename, 'country', country_filepath)

        urban_filepaths[index] = country_filepath

    # Synchronization: wait for parallel processes to complete
    logger.debug("Waiting for parallel processes to complete intersections")
    if africapolis_filename is not None:
        cond1 = urban_filepaths[1]
        cond2 = os.path.join(os.path.dirname(africapolis_filename_new), f'tmp_country_intersected_{os.path.basename(africapolis_filename_new)}')
    else:
        cond1 = urban_filepaths[0]
        cond2 = os.path.join(os.path.dirname(ghsl_filename_new), f'tmp_country_intersected_{os.path.basename(ghsl_filename_new)}')

    while not os.path.exists(cond1):
        logger.debug(f"Waiting for {os.path.basename(cond1)} to be created...")
        time.sleep(5)
    while os.path.exists(cond2):
        logger.debug(f"Waiting for temp file cleanup...")
        time.sleep(5)

    logger.info("Layer intersection complete")
    urban_filepaths.append(country_filename_new)
    return urban_filepaths

def main():
    """Main entry point for metadata spatial filtering pipeline.
    
    Orchestrates the complete workflow:
    1. Loads configuration from config.yaml
    2. Prepares geographic base layers (continents, countries, urban areas)
    3. Intersects metadata with political boundaries and urban classifications
    4. Applies distance-based filtering per sequence
    5. Outputs unfiltered and filtered parquet files with tile partitioning
    
    Expects one argument: path to metadata parquet file (tile-partitioned).
    Skips processing if file modification time is before config's updated_after timestamp.
    """
    logger.info("Starting metadata intersections and filtering")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()
    logger.debug("Configuration loaded")
    
    root = os.path.abspath(cfg['paths']['tile_partitioned_parquet_raw_metadata_dir'])
    
    # Validate input argument
    if len(sys.argv) < 2:
        logger.error("Usage: python metadata_intersections_and_filtering.py <metadata_parquet_file>")
        sys.exit(1)
    
    metadata_filepath = str(sys.argv[1])
    metadata_filename = os.path.basename(metadata_filepath)
    if metadata_filename.startswith('metadata_unfiltered_'):
        tile = os.path.basename(metadata_filename.split("_")[2])
    else:
        tile = os.path.basename(metadata_filename.split("_")[3])
    logger.info(f"Processing metadata file: {metadata_filename} (tile: {tile})")
    
    is_first = False
    updated_after = datetime.fromisoformat(cfg['metadata_params']['updated_after'])
    mtime = datetime.fromtimestamp(os.path.getmtime(metadata_filepath))  # file modification time

    if mtime < updated_after:
        logger.warning(f"Skipping {metadata_filename}: modification time {mtime} before {updated_after}")
        return

    # Load configuration parameters
    zoom_level = cfg['params']['zoom_level']
    urban_threshold = cfg['params']['urban_threshold']
    rural_threshold = cfg['params']['rural_threshold']
    logger.debug(f"Filters: urban={urban_threshold}m, rural={rural_threshold}m, zoom={zoom_level}")

    continents_filename = cfg['filenames']['continents_filename']
    overture_url = cfg['filenames']['overture_url']
    country_filename = cfg['filenames']['country_filename']
    ghsl_filename = cfg['filenames']['ghsl_filename']
    africapolis_filename = cfg['filenames']['africapolis_filename']
    continents_dir = os.path.abspath(cfg['paths']['continents_dir'])
    logger.debug(f"Filename paths: continents={continents_filename}, country={country_filename}")

    ghsl_col_1 = cfg['params']['ghsl_col_1']
    ghsl_col_2 = cfg['params']['ghsl_col_2']
    africapolis_col_1 = cfg['params']['africapolis_col_1']
    africapolis_col_2 = cfg['params']['africapolis_col_2']
    ghsl_string = f'{ghsl_col_1}, {ghsl_col_2}' if ghsl_col_2 else ghsl_col_1
    africapolis_string = f'{africapolis_col_1}, {africapolis_col_2}' if africapolis_col_2 else africapolis_col_1

    # Determine if this is the first execution (for one-time setup)
    logger.debug("Checking if this is the first execution")
    for index, folder in enumerate([f for f in os.listdir(root) if os.path.isdir(root)]):
        folder_filepath = os.path.join(root, folder)
        is_first = index == 0 and metadata_filename == [f for f in os.listdir(folder_filepath) if f.endswith('.parquet')][0]
        if is_first:
            break
    
    logger.info("Preparing geographic layers")
    processed_dir = cfg['paths']['processed_dir']
    urban_filepaths = layer_intersections(is_first, processed_dir, continents_dir, 
                                           continents_filename, country_filename,
                                            overture_url, ghsl_filename, africapolis_filename)
    logger.debug(f"Urban layer paths prepared: {urban_filepaths}")
  
    # Prepare output directories
    unfiltered_dir = os.path.abspath(os.path.join(cfg['paths']['unfiltered_metadata_dir'], f'tile={tile}'))
    filtered_dir = unfiltered_dir.replace('unfiltered', 'filtered')
    unfiltered_filename = os.path.join(unfiltered_dir, f'c_u_{metadata_filename}')
    filtered_filename = unfiltered_filename.replace("unfiltered", "filtered")
    logger.info(f"Output directories: {unfiltered_dir}, {filtered_dir}")
    logger.info(f"Current working directory: {os.getcwd()}")

    for directory in [unfiltered_dir, filtered_dir]:
        os.makedirs(directory, exist_ok=True)

    # Execute intersection and filtering with retry logic
    logger.info("Starting intersection and filtering with retry logic")
    result = False
    attempt = 0
    max_attempts = 3
    while not result and attempt < max_attempts:
        attempt += 1
        logger.info(f"Attempt {attempt}/{max_attempts}")
        result = intersections_with_metadata(metadata_filepath,
                                continents_filename, urban_filepaths[2], urban_filepaths[0], urban_filepaths[1] if len(urban_filepaths) > 1 else None,
                                unfiltered_filename, filtered_filename, ghsl_string, africapolis_string,
                                zoom_level, [urban_threshold, rural_threshold])
        if not result and attempt < max_attempts:
            logger.warning(f"Attempt {attempt} failed, retrying in 5s...")
            time.sleep(5)
    
    if result:
        logger.info(f"Successfully processed {metadata_filename}")
    else:
        logger.error(f"Failed to process {metadata_filename} after {max_attempts} attempts")

if __name__ == "__main__":
    main()

    


    


    




