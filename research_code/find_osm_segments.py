import sys
import os
import logging
from datetime import datetime
import random
import duckdb
import numpy as np
from start import load_config

def haversine(point1_wkt, point2_wkt):
    parse_wkt = lambda p: map(float, p.lstrip("POINT (").rstrip(")").split())
    lon1, lat1 = parse_wkt(point1_wkt)
    lon2, lat2 = parse_wkt(point2_wkt)
    lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return EARTH_RADIUS * c

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
        conn.create_function("haversine", func, ['VARCHAR','VARCHAR'],'DOUBLE')
        conn.execute(query)
    except Exception as err:
        logging.warning(f"an error occurred while calculating distances: {err}")
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)
    

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()

    points_filename = os.path.basename(sys.argv[1])
    tile = points_filename.split('.')[0].split('_')[-2]
    EARTH_RADIUS = cfg ['params']['earth_radius']
    zoom_level = cfg ['params']['zoom_level']
    distance_threshold = cfg ['params']['distance_threshold']
    func = haversine
    
    points_dir = os.path.join(cfg['paths']['unfiltered_metadata_dir'], f'tile={tile}')
    osm_dir = os.path.join(cfg['paths']['osm_partitioned_dir'], f'tile={tile}')
    saving_filedir = os.path.join(cfg['paths']['osm_intersections_dir'], f'tile={tile}')
    metadata_filepath = os.path.join(points_dir, points_filename)

    updated_after = datetime.fromisoformat(cfg['metadata_params']['updated_after'])
    mtime = datetime.fromtimestamp(os.path.getmtime(metadata_filepath)) 
    if mtime < updated_after:
        return
    
    if not os.path.exists(saving_filedir):
        os.makedirs(saving_filedir, exist_ok=True)

    for index, osm_file in enumerate([f for f in os.listdir(osm_dir) if f.endswith('.parquet')]):
        calculate_distance(metadata_filepath, os.path.join(osm_dir, osm_file), saving_filedir, index,
                        delta_x=50, delta_y=50, earth_radius=EARTH_RADIUS, zoom_level=zoom_level,
                        distance_threshold=distance_threshold, func=func)
    return

EARTH_RADIUS = 0
if __name__ == '__main__':
    main()