import os
import sys
import logging
import random
from datetime import datetime
import numpy as np
import duckdb
from start import load_config

def create_mask(distances,threshold_up=10, threshold_down=0):
    distances = np.array(distances)
    mask = (threshold_down < distances) & (distances < threshold_up)
    return mask.tolist()

def extract_first(list_):
    return [list_[0]]
    
def process_one_file(osm_intersected_filedir, filename, output_filedir, threshold1=10, threshold2=20):
    query = f"""
    COPY(
    WITH 
    ordered_metadata AS(
        SELECT DISTINCT 
            id,distance_meter,osm_tags_highway,osm_tags_surface,osm_id,
            CAST(changeset_timestamp AS VARCHAR) as changeset_timestamp, shortest_line
        FROM read_parquet('{osm_intersected_filedir}/*_{filename}')
        ORDER BY id, distance_meter ASC
    ),
    
    unique_metadata AS(
        SELECT
            id,
            list_extract(list(distance_meter),1) as distance_meter,
            list_extract(list(osm_tags_highway),1) as osm_tags_highway,
            list_extract(list(osm_tags_surface),1) as osm_tags_surface,
            list_extract(list(osm_id),1) as osm_id,
            list_extract(list(changeset_timestamp),1) as changeset_timestamp,
            list_extract(list(shortest_line),1) as shortest_line
        FROM ordered_metadata
        GROUP by id, osm_id
    ),
  
   grouped_metadata AS(
        SELECT 
            id,
            count(*) as count,
            list(osm_id) AS osm_ids,
            list(osm_tags_highway) AS osm_tags_highways,
            list(osm_tags_surface) AS osm_tags_surfaces,
            list(shortest_line) AS shortest_lines,
            list(distance_meter) AS distances_meter,
            list(changeset_timestamp) AS changeset_timestamps,
            create_mask(distances_meter,{threshold1},0) AS mask_{threshold1},
            create_mask(distances_meter,{threshold2},{threshold1}) AS mask_{threshold2}
        FROM(
            SELECT * 
            FROM unique_metadata
            ORDER BY id, distance_meter ASC
            )
        GROUP BY id
    ),
    
    filtered_metadata AS(
        SELECT *,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(osm_ids,mask_{threshold1})
                WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(osm_ids,mask_{threshold2})
                ELSE extract_attribute(osm_ids)
            END AS filtered_osm_ids,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(osm_tags_highways,mask_{threshold1})
                WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(osm_tags_highways,mask_{threshold2})
                ELSE extract_attribute(osm_tags_highways)
            END AS filtered_osm_tags_highways,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(osm_tags_surfaces,mask_{threshold1})
                WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(osm_tags_surfaces,mask_{threshold2})
                ELSE extract_attribute(osm_tags_surfaces)
            END AS filtered_osm_tags_surfaces,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(shortest_lines,mask_{threshold1})
                WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(shortest_lines,mask_{threshold2})
               ELSE extract_line(shortest_lines)
            END AS filtered_shortest_lines,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(distances_meter,mask_{threshold1})
                WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(distances_meter,mask_{threshold2})
                ELSE extract_distance(distances_meter)
            END AS filtered_distances_meter,
            CASE 
                WHEN list_contains(mask_{threshold1},true) THEN list_where(changeset_timestamps,mask_{threshold1})
               WHEN list_contains(mask_{threshold1},true) = false and list_contains(mask_{threshold2},true) THEN list_where(changeset_timestamps,mask_{threshold2})
                ELSE extract_timestamp(changeset_timestamps)
            END AS filtered_changeset_timestamps
        FROM grouped_metadata
    ),

    final AS(
        SELECT
            id,count,osm_ids,osm_tags_highways,osm_tags_surfaces,shortest_lines,distances_meter,changeset_timestamps,
            UNNEST(filtered_osm_ids) as osm_id,
            UNNEST(filtered_osm_tags_highways) as osm_tags_highway,
            UNNEST(filtered_osm_tags_surfaces) as osm_tags_surface,
            UNNEST(filtered_shortest_lines) as shortest_line,
            UNNEST(filtered_changeset_timestamps) as changeset_timestamp,
            UNNEST(filtered_distances_meter) as distance_meter,
            round(distance_meter - list_extract(distances_meter,1),2) as abs_diff,
            round((distance_meter - list_extract(distances_meter, 1)) /(distance_meter + list_extract(distances_meter, 1)),2) as percent_diff
        FROM filtered_metadata
        ORDER BY id, distance_meter ASC
    )
    
    SELECT *, shortest_line as geometry
    FROM final
    )
    TO '{output_filedir}/closest_{filename}'
    (FORMAT PARQUET,COMPRESSION zstd);
    """
    conn = None
    temp_file =f'temp_file_{random.randint(0, int(1e12))}.db' 
    try:
        conn = duckdb.connect(temp_file)
        conn.create_function("extract_timestamp", extract_first, ['VARCHAR[]'],'VARCHAR[]')
        conn.create_function("extract_attribute", extract_first, ['VARCHAR[]'],'VARCHAR[]')
        conn.create_function("extract_distance", extract_first, ['DOUBLE[]'],'DOUBLE[]')
        conn.create_function("extract_line", extract_first, ['blob[]'],'blob[]')
        conn.create_function("create_mask", create_mask, ['DOUBLE[]','DOUBLE','DOUBLE'],'BOOLEAN[]')
        conn.execute(query)
    except Exception as err:
        logging.warning(f"an exception at: {filename} : {err}")
    finally:
        if conn is not None:
            conn.close()
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()

    unfiltered_filename = os.path.basename(sys.argv[1])
    tile = unfiltered_filename.split('.')[0].split('_')[-2]

    osm_intersected_filedir = os.path.join(cfg['paths']['osm_intersections_dir'], f'tile={tile}')
    unfiltered_output_filedir = os.path.join(cfg['paths']['nearest_lines'], f'tile={tile}')

    threshold1 = cfg['params']['threshold_1']
    threshold2 = cfg['params']['threshold_2']

    for output_filedir, filename in zip([unfiltered_output_filedir], [unfiltered_filename]):
        if not os.path.exists(output_filedir):
            os.makedirs(output_filedir,exist_ok=True)

        updated_after = datetime.fromisoformat(cfg['metadata_params']['updated_after'])
        mtime = datetime.fromtimestamp(os.path.getmtime(os.path.join(osm_intersected_filedir, filename))) 
        if mtime < updated_after:
            continue
    
        process_one_file(osm_intersected_filedir, filename, output_filedir, threshold1, threshold2)

if __name__ == '__main__':
    main()


