"""Match Mapillary image points to nearby OSM road segments.

This script finds OSM road segments within a configurable distance threshold of each 
Mapillary metadata point. For each point-segment pair, calculates the shortest connecting 
line and stores the distance using the haversine formula.

**Workflow:**
1. Takes metadata Parquet file path as command-line argument
2. Extracts tile name from filename (e.g., '158-137-8' from 'metadata_158-137-8.parquet')
3. For each OSM highway file in the same tile:
   - Reads metadata points (WKT geometries) from Parquet
   - Reads OSM road segments (WKB geometries) from Parquet
   - Performs spatial join:
     * Filters on same logical tile (z{zoom_level}_tiles column)
     * Further filters using ±50m bounding box (delta_x, delta_y)
   - Calculates shortest line between each point and segment
   - Uses haversine formula to compute distance in meters
   - Filters to keep only results < distance_threshold (default: 30m)
4. Outputs one Parquet file per OSM file with all point-segment matches

**Input Requirements:**
- Metadata Parquet file: must have columns: id, geometry (WKT), long, lat, z{zoom_level}_tiles
- OSM Parquet file: must have columns: osm_id, geometry (WKB), bbox, osm_tags_highway, 
  osm_tags_surface, changeset_timestamp, z{zoom_level}_tiles

**Configuration (config.yaml):**
- params.earth_radius - Earth radius in meters (default: 6371008)
- params.zoom_level - Tile zoom level (must match metadata)
- params.distance_threshold - Maximum distance in meters to keep (default: 30)
- paths.unfiltered_metadata_dir - Input metadata directory
- paths.osm_partitioned_dir - Input OSM directory  
- paths.osm_intersections_dir - Output directory for matches
- csv_split_params.updated_after - Skip files not modified after this timestamp

**Output:**
One Parquet per OSM file with columns:
- id - Image ID
- osm_id - Road segment ID
- geometry - Shortest connecting line (WKB format)
- osm_tags_highway - Road type (primary, secondary, residential, etc.)
- osm_tags_surface - Surface type (asphalt, concrete, gravel, etc.)
- changeset_timestamp - OSM changeset timestamp
- shortest_line - Duplicate of geometry column (WKB)
- distance_meter - Distance to road in meters

**Example usage:**
    python find_osm_segments.py ./data/filtered/tile=158-137-8/metadata_unfiltered_158-137-8.parquet
    
**Output files:**
    data/osm_intersections/tile=158-137-8/osm_0_metadata_unfiltered_158-137-8.parquet
    data/osm_intersections/tile=158-137-8/osm_1_metadata_unfiltered_158-137-8.parquet
    (one per OSM file in the tile)

**Features:**
- Haversine distance calculation for geographic accuracy
- Configurable distance threshold and bounding box expansion
- Timestamp-based resumability (skips outdated files)
- Comprehensive logging with file count tracking
- DuckDB spatial operations for efficiency
"""

import sys
import os
import logging
from datetime import datetime
import random
import duckdb
import numpy as np
from start import load_config

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

EARTH_RADIUS = 6371008  # meters

def haversine(point1_wkt, point2_wkt, earth_radius=6371008):
    """Calculate distance between two points using haversine formula.
    
    Args:
        point1_wkt: WKT POINT string
        point2_wkt: WKT POINT string
        earth_radius: Earth radius in meters (default: 6371008)
    
    Returns:
        Distance in meters
    """
    parse_wkt = lambda p: map(float, p.lstrip("POINT (").rstrip(")").split())
    lon1, lat1 = parse_wkt(point1_wkt)
    lon2, lat2 = parse_wkt(point2_wkt)
    lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return earth_radius * c

def calculate_distance(points_filepath, osm_filepath, saving_filedir,index, delta_x, delta_y, earth_radius=6371008, zoom_level=8, 
                       distance_threshold=30, func=haversine):
    delta_x = delta_x/earth_radius
    delta_y = round(delta_y/(earth_radius),3)
    query = f"""
    COPY(
    WITH
    points AS (
        SELECT * REPLACE ST_GeomFromText(geometry) as geometry
        FROM read_parquet('{points_filepath}')
            ),
                
    osm_highways AS (
            SELECT * REPLACE ST_GeomFromWKB(geometry) as geometry
            FROM read_parquet('{osm_filepath}')
            ),
                
    temporal as (
        SELECT 
            a.id, ST_AsText(a.geometry) as geometry,
            b.osm_tags_highway, b.osm_tags_surface, b.osm_id, b.changeset_timestamp,
            ST_ShortestLine(a.geometry, b.geometry) as shortest_line,
            haversine(ST_AsText(ST_StartPoint(shortest_line)), ST_AsText(ST_EndPoint(shortest_line))) as distance_meter
        FROM points as a
        LEFT JOIN osm_highways b 
            ON b.z{zoom_level}_tiles = a.z{zoom_level}_tiles
            AND (a.long BETWEEN (b.bbox.xmin-round({delta_x}/cos(lat*pi()/180),3)) AND (b.bbox.xmax+round({delta_x}/cos(lat*pi()/180),3)))
            AND (a.lat BETWEEN (b.bbox.ymin-{delta_y}) AND (b.bbox.ymax+{delta_y}))
        WHERE b.osm_id IS NOT NULL
        )
      
    SELECT 
        id,
        osm_id,
        ST_AsWKB(shortest_line) as geometry,
        osm_tags_highway,
        osm_tags_surface, 
        changeset_timestamp, 
        ST_AsWKB(shortest_line) as shortest_line,
        distance_meter      
    FROM temporal
    WHERE distance_meter < {distance_threshold}
    )
    TO '{saving_filedir}/osm_{index}_{os.path.basename(points_filepath)}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    conn = None
    temp_file = f'temp_{random.randint(0, int(1e12))}.db'
    try:
        conn = duckdb.connect(temp_file)
        conn.execute("install spatial; load spatial;")
        conn.create_function("haversine", func, ['VARCHAR','VARCHAR','DOUBLE'],'DOUBLE')
        logging.info(f"Processing metadata with OSM file index {index}, threshold={distance_threshold}m")
        conn.execute(query)
        logging.info(f"Successfully output results to {saving_filedir}")
        sys.stdout.flush()
    except Exception as err:
        logging.error(f"Error calculating distances: {err}", exc_info=True)
        sys.stdout.flush()
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)
    

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()

    if len(sys.argv) < 2:
        logging.error("Usage: python find_osm_segments.py <metadata_filepath>")
        sys.exit(1)
    
    points_filename = os.path.basename(sys.argv[1])
    tile = points_filename.split('.')[0].split('_')[-2]
    earth_radius = cfg['params']['earth_radius']
    zoom_level = cfg['params']['zoom_level']
    distance_threshold = cfg['params']['distance_threshold']
    func = haversine
    
    points_dir = os.path.abspath(os.path.join(cfg['paths']['unfiltered_metadata_dir'], f'tile={tile}'))
    osm_dir = os.path.abspath(os.path.join(cfg['paths']['osm_partitioned_dir'], f'tile={tile}'))
    saving_filedir = os.path.abspath(os.path.join(cfg['paths']['osm_intersections_dir'], f'tile={tile}'))
    metadata_filepath = os.path.join(points_dir, points_filename)

    updated_after = datetime.fromisoformat(cfg['metadata_params']['updated_after'])
    mtime = datetime.fromtimestamp(os.path.getmtime(metadata_filepath)) 
    if mtime < updated_after:
        logging.info(f"Skipping {metadata_filepath} (not modified after {updated_after})")
        sys.stdout.flush()
        return
    
    logging.info(f"Processing tile={tile} from {metadata_filepath}")
    sys.stdout.flush()
    if not os.path.exists(saving_filedir):
        os.makedirs(saving_filedir, exist_ok=True)

    osm_files = [f for f in os.listdir(osm_dir) if f.endswith('.parquet')]
    logging.info(f"Found {len(osm_files)} OSM files in {osm_dir}")
    sys.stdout.flush()
    
    for index, osm_file in enumerate(osm_files):
        logging.info(f"Processing OSM file {index+1}/{len(osm_files)}: {osm_file}")
        calculate_distance(metadata_filepath, os.path.join(osm_dir, osm_file), saving_filedir, index,
                        delta_x=50, delta_y=50, earth_radius=earth_radius, zoom_level=zoom_level,
                        distance_threshold=distance_threshold, func=func)
    
    logging.info(f"Completed processing tile={tile}")
    return

if __name__ == '__main__':
    main()