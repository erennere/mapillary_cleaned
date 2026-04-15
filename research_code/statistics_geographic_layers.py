"""Build geographic-layer statistics from Mapillary metadata and OSM joins.

The script defines SQL fragment builders, composes per-layer DuckDB queries, and
executes a file-level processing pipeline for z8/z14/country/continent/world outputs.
"""

# =========================
# Imports
# =========================
import os
import random
import sys
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import duckdb
from shapely import to_wkb, box
import mercantile

from start import load_config

# =========================
# Runtime configuration
# =========================
urban_areas = []
cols = []
osm_distance = 15
pred_distance = 10
sigma = 1
score = 0.8
threshold = 0.3
max_workers = 8

zoom_level = 8
results_dir = os.getcwd()

def load_statistics_runtime_config(cfg):
    """Return runtime settings for this module from config with safe defaults."""
    stats_cfg = cfg.get('statistics', {})
    geographic_cfg = stats_cfg.get('geographic_layers', {})

    selected_results_dir = os.path.join(os.path.abspath(cfg['paths'].get('stats_dir')), 'geographic_layers')

    return {
        'osm_distance': int(geographic_cfg.get('osm_distance', 15)),
        'pred_distance': int(geographic_cfg.get('pred_distance', 10)),
        'sigma': float(geographic_cfg.get('sigma', 1)),
        'score': float(geographic_cfg.get('score', 0.8)),
        'threshold': float(geographic_cfg.get('threshold', 0.3)),
        'max_workers': int(geographic_cfg.get('max_workers', 8)),
        'data_input_pattern': geographic_cfg.get('data_input_pattern', 'data_*.parquet'),
        'results_dir': selected_results_dir,
        'urban_area_layers': geographic_cfg.get(
            'urban_area_layers',
            [
                'GHS_STAT_UCDB2015MT_GLOBE_R2019A',
                'AFRICAPOLIS2020',
            ],
        ),
        'urban_area_cols': geographic_cfg.get('urban_area_cols', ['ID_HDC_G0', 'agglosID']),
        'zoom_level': int(cfg['params'].get('zoom_level', 8)),
    }

# =========================
# SQL fragment builders
# =========================
def create_paved_tags(highways, areas, road_types, aggregation_type='SUM'):
    """Return metric labels for highway/area/surface combinations."""
    query = []
    for highway in highways:
        for area in areas:
            for road_type in road_types:
                query.append(f"{highway}{area}_{road_type}")
    return query

def create_agg_highway_road_type_strings(highways, areas, road_types, aggregation_type='SUM'):
    """Build aggregate SQL expressions for paved/unpaved highway features."""
    query = ''
    for highway in highways:
        for area in areas:
            for road_type in road_types:
                query += f"{aggregation_type}({highway}_{area}_{road_type}) as {highway}_{area}_{road_type}, \n"
    return query.rstrip(', ')

def agg_ratio_strings(stuff, factor=1, agg_type='SUM'):
    """Build aggregate-and-scale SQL expressions for a list of columns."""
    query = ''
    for x in stuff:
        query += f"{agg_type}({x})/{factor} as {x}, \n"
    return query.rstrip(', ')

def create_ratio_strings(group1, group2, factor=1):
    """Build pairwise ratio SQL expressions for aligned metric groups."""
    query = ''
    for g1, g2 in zip(group1, group2):
        query += f"SUM({g1})/NULLIF(SUM({g2})*{factor}, 0) as {g1}_ratio, \n"
    return query.rstrip(', ')

def create_paved_ratio_strings(highways, areas, aggregation_type='SUM'):
    """Build paved-share SQL expressions by highway and area."""
    query = ''
    for highway in highways:
        for area in areas:
            query += f"{aggregation_type}({highway}{area}_paved)/NULLIF({aggregation_type}({highway}{area}_paved) + {aggregation_type}({highway}{area}_unpaved), 0) as {highway}{area}_paved_ratio, \n"
    return query.rstrip(', ')

def _sql_in_list(tags):
    """Build a safe SQL IN list like ('motorway', 'motorway_link')."""
    if not tags:
        raise ValueError("Each road class must include at least one tag.")

    escaped = [tag.replace("'", "''") for tag in tags]
    return "(" + ", ".join(f"'{tag}'" for tag in escaped) + ")"

def create_osm_general_strings(road_classes, suffix="", alias_prefix="osm_"):
    """
    Create columns like:
      motorway_id, motorway_length, trunk_id, trunk_length, ...
    """
    parts = []

    for highway, tags in road_classes.items():
        in_clause = _sql_in_list(tags)
        parts.append(
            f"COUNT(DISTINCT CASE WHEN osm_tags_highway IN {in_clause} "
            f"THEN osm_id ELSE NULL END) as {alias_prefix}{highway}_id{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN osm_tags_highway IN {in_clause} "
            f"THEN length ELSE 0 END) as {alias_prefix}{highway}_length{suffix}"
        )

    return ",\n".join(parts)

def create_osm_urban_rural_strings(road_classes, suffix="", alias_prefix="osm_"):
    """
    Create columns like:
      motorway_id_urban, motorway_id_rural,
      motorway_length_urban, motorway_length_rural, ...
    """
    parts = []

    for highway, tags in road_classes.items():
        in_clause = _sql_in_list(tags)
        parts.append(
            f"COUNT(DISTINCT CASE WHEN urban = 1 AND osm_tags_highway IN {in_clause} "
            f"THEN osm_id ELSE NULL END) as {alias_prefix}{highway}_id_urban{suffix}"
        )
        parts.append(
            f"COUNT(DISTINCT CASE WHEN urban = 0 AND osm_tags_highway IN {in_clause} "
            f"THEN osm_id ELSE NULL END) as {alias_prefix}{highway}_id_rural{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN urban = 1 AND osm_tags_highway IN {in_clause} "
            f"THEN length ELSE 0 END) as {alias_prefix}{highway}_length_urban{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN urban = 0 AND osm_tags_highway IN {in_clause} "
            f"THEN length ELSE 0 END) as {alias_prefix}{highway}_length_rural{suffix}"
        )
    return ",\n".join(parts)

def add_prefix_and_suffix(stuff, prefix, suffix):
    """Prefix source columns and rename with a suffix alias."""
    query = ''
    for x in stuff:
        query += f"{prefix}{x} as {suffix}{x}, \n" 
    return query.rstrip(', \n')

def add_prefix(stuff, prefix):
    """Prefix column references for SELECT lists."""
    query = ''
    for x in stuff:
        query += f"{prefix}{x}, \n"
    return query.rstrip(', \n')

def create_surface_general_strings(
    road_classes,
    pred_col="pred_label",
    suffix="",
    alias_prefix="",
):
    """
    Create columns like:
      motorway_unpaved, motorway_paved, trunk_unpaved, trunk_paved, ...
    """
    parts = []

    for highway, tags in road_classes.items():
        in_clause = _sql_in_list(tags)
        parts.append(
            f"SUM(CASE WHEN osm_tags_highway IN {in_clause} "
            f"THEN {pred_col} ELSE 0 END) as {alias_prefix}{highway}_unpaved{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN osm_tags_highway IN {in_clause} THEN 1 ELSE 0 END) - "
            f"SUM(CASE WHEN osm_tags_highway IN {in_clause} THEN {pred_col} ELSE 0 END) "
            f"as {alias_prefix}{highway}_paved{suffix}"
        )

    return ",\n".join(parts)

def create_surface_urban_rural_strings(
    road_classes,
    pred_col="pred_label",
    suffix="",
    alias_prefix="",
):
    """
    Create columns like:
      motorway_urban_unpaved, motorway_rural_unpaved,
      motorway_urban_paved, motorway_rural_paved, ...
    """
    parts = []

    for highway, tags in road_classes.items():
        in_clause = _sql_in_list(tags)

        parts.append(
            f"SUM(CASE WHEN urban = 1 AND osm_tags_highway IN {in_clause} "
            f"THEN {pred_col} ELSE 0 END) as {alias_prefix}{highway}_urban_unpaved{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN urban = 0 AND osm_tags_highway IN {in_clause} "
            f"THEN {pred_col} ELSE 0 END) as {alias_prefix}{highway}_rural_unpaved{suffix}"
        )

        parts.append(
            f"SUM(CASE WHEN urban = 1 AND osm_tags_highway IN {in_clause} THEN 1 ELSE 0 END) - "
            f"SUM(CASE WHEN urban = 1 AND osm_tags_highway IN {in_clause} THEN {pred_col} ELSE 0 END) "
            f"as {alias_prefix}{highway}_urban_paved{suffix}"
        )
        parts.append(
            f"SUM(CASE WHEN urban = 0 AND osm_tags_highway IN {in_clause} THEN 1 ELSE 0 END) - "
            f"SUM(CASE WHEN urban = 0 AND osm_tags_highway IN {in_clause} THEN {pred_col} ELSE 0 END) "
            f"as {alias_prefix}{highway}_rural_paved{suffix}"
        )

    return ",\n".join(parts)


def build_metric_catalog(statistics_cfg=None):
    """Build shared metric lists and labels used by aggregation queries."""
    shared_cfg = (statistics_cfg or {}).get('shared', {})

    highways = shared_cfg.get(
        'highways',
        ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential'],
    )
    areas = shared_cfg.get('areas', ['urban', 'rural'])
    road_types = shared_cfg.get('road_types', ['paved', 'unpaved'])
    road_classes = shared_cfg.get(
        'road_classes',
        {
            'motorway': ['motorway', 'motorway_link'],
            'trunk': ['trunk', 'trunk_link'],
            'primary': ['primary', 'primary_link'],
            'secondary': ['secondary', 'secondary_link'],
            'tertiary': ['tertiary', 'tertiary_link'],
            'unclassified': ['unclassified'],
            'residential': ['residential'],
        },
    )

    length_tags = []
    pred_length_tags = []
    osm_length_tags = []
    id_tags = []
    pred_id_tags = []
    osm_id_tags = []
    areas_length = ['length', 'length_urban', 'length_rural']
    areas_id = ['id', 'id_urban', 'id_rural']
    areas_2 = ['_urban', '_rural', '']
    rest = ['count_ID_HDC_G0', 'count_agglosID']
    n_osms = ['number_of_osm_ids', 'number_of_osm_ids_urban', 'number_of_osm_ids_rural']
    rest_length_road = []
    rest_country_onwards = []

    for highway in highways:
        for area in areas_length:
            length_tags.append(f"{highway}_{area}")
        for area in areas_id:
            id_tags.append(f"{highway}_{area}")

    for length in length_tags:
        pred_length_tags.append(f"pred_{length}")
        osm_length_tags.append(f"osm_{length}")

    for id_ in id_tags:
        pred_id_tags.append(f"pred_{id_}")
        osm_id_tags.append(f"osm_{id_}")

    for area in areas_2:
        rest.append(f'number_of_osm_ids{area}')
        rest.append(f'number_of_images{area}')
        rest.append(f'osm_ids{area}')
        rest.append(f'osm_length{area}')
        rest.append(f'paved_segments{area}')
        rest.append(f'unpaved_segments{area}')
        rest_length_road.append(f'length{area}_unpaved')
        rest_length_road.append(f'length{area}_paved')
        rest_country_onwards.append(f'number_of_sequences{area}')
        rest_country_onwards.append(f'number_of_images{area}')
        rest_country_onwards.append(f'osm_ids{area}')
        rest_country_onwards.append(f'paved_segments{area}')
        rest_country_onwards.append(f'unpaved_segments{area}')

    osm_total_cols = []
    osm_total_cols += osm_length_tags
    osm_total_cols += osm_id_tags
    osm_total_cols += n_osms
    for i in range(len(osm_total_cols)):
        if 'urban' in osm_total_cols[i] or 'rural' in osm_total_cols[i]:
            osm_total_cols[i] = 'a.' + osm_total_cols[i]
        else:
            osm_total_cols[i] = 'b.' + osm_total_cols[i]

    paved_tags = create_paved_tags(highways, areas_2, road_types)
    all_cols = pred_length_tags + pred_id_tags + length_tags + id_tags + paved_tags

    return {
        'highways': highways,
        'areas': areas,
        'road_types': road_types,
        'length_tags': length_tags,
        'pred_length_tags': pred_length_tags,
        'osm_length_tags': osm_length_tags,
        'id_tags': id_tags,
        'pred_id_tags': pred_id_tags,
        'osm_id_tags': osm_id_tags,
        'areas_2': areas_2,
        'rest': rest,
        'n_osms': n_osms,
        'rest_length_road': rest_length_road,
        'rest_country_onwards': rest_country_onwards,
        'road_classes': road_classes,
        'osm_total_cols': osm_total_cols,
        'osm_total_cols_string': ', \n'.join(osm_total_cols),
        'all_cols': all_cols,
        'paved_tags': paved_tags,
        'paved_strings': ', \n'.join(paved_tags),
    }

# This will be used to create z14 tiles
def create_tile(tiles_info):
    """Convert XYZ tile values into WKB geometry."""
    bbox = mercantile.bounds(tiles_info[0], tiles_info[1], tiles_info[2])
    return to_wkb(box(*bbox))

def haversine(p1, p2,earth_radius=6371008):
    """Return great-circle distances in meters for coordinate arrays."""
    p1 = np.radians(np.array(p1))
    p2 = np.radians(np.array(p2))

    delta_lat = p2[1] - p1[1]
    delta_long = p2[0] - p1[0]

    a = np.sin(delta_lat / 2) ** 2 + np.cos(p1[1]) * np.cos(p2[1]) * np.sin(delta_long / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    distances = earth_radius * c
    return distances

def calculate_length(longs, lats):
    """Compute polyline length from longitude/latitude sequences."""
    longs = np.array(longs)
    lats = np.array(lats)
    p1 = np.array([longs[0:-1],lats[0:-1]])
    p2 = np.array([longs[1:],lats[1:]])
    return round(np.sum(haversine(p1, p2)),3)

def correct_z14_tiles_osm(z14_list):
    """Normalize OSM z14 tile list values into integer triplets."""
    results = []
    for element in z14_list:
        results.append([int(e) for e in element[0].split(' ') if e])
    return results

# =========================
# Metric catalogs
# =========================
try:
    _BOOT_CFG = load_config()
    _METRICS = build_metric_catalog(_BOOT_CFG.get('statistics', {}))
except Exception:
    _METRICS = build_metric_catalog()

highways = _METRICS['highways']
areas = _METRICS['areas']
road_types = _METRICS['road_types']
length_tags = _METRICS['length_tags']
pred_length_tags = _METRICS['pred_length_tags']
osm_length_tags = _METRICS['osm_length_tags']
id_tags = _METRICS['id_tags']
pred_id_tags = _METRICS['pred_id_tags']
osm_id_tags = _METRICS['osm_id_tags']
areas_2 = _METRICS['areas_2']
rest = _METRICS['rest']
n_osms = _METRICS['n_osms']
rest_length_road = _METRICS['rest_length_road']
rest_country_onwards = _METRICS['rest_country_onwards']
road_classes = _METRICS['road_classes']
osm_total_cols = _METRICS['osm_total_cols']
osm_total_cols_string = _METRICS['osm_total_cols_string']
all_cols = _METRICS['all_cols']
paved_tags = _METRICS['paved_tags']
paved_strings = _METRICS['paved_strings']

# =========================
# Query assembly functions
# =========================
def urban_query(col, sigma, score, threshold, osm_distance, pred_distance, output_filepath):
    """Build the urban area aggregation SQL for a selected geometry column."""
    query_urban = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            {col}, 
            osm_id,
            MAX(length) as length
        FROM(
            SELECT
                {col},
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length
            FROM(
                SELECT
                    UNNEST({col}) as {col}, osm_id, length, sequence
                FROM lines_df
                )
            GROUP BY {col}, osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY {col}, osm_id
    ),
    
    pred_stats AS(
    SELECT
        {col}, ANY_VALUE(osm_id) as osm_id, 
        SUM(pred_label) AS unpaved_segments,
        COUNT(*) - SUM(pred_label) AS paved_segments,

        {create_surface_general_strings(road_classes)},

        SUM(length) AS length,
        SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
        SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,

        SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
        list(latest_timestamp) as latest_timestamps,
        {create_osm_general_strings(road_classes, suffix="", alias_prefix="")}
    FROM(
        SELECT
            {col},
            osm_id,
            MODE(pred_label) as pred_label,
            MAX(length) as length,
            MAX(id) as id,
            MAX(latest_timestamp) as latest_timestamp,
            ANY_VALUE(osm_tags_highway) as osm_tags_highway
        FROM(
            SELECT
                a.{col}, a.osm_id,
                CASE
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                    ELSE NULL
                END as pred_label,
                MAX(b.length) as length,
                COUNT(DISTINCT a.id) as id,
                MAX(timestamp) as latest_timestamp,
                ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    {col}, osm_id, 
                    CASE
                        WHEN distance_meter_total = distance_meter_total THEN  pred_label
                        ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                    END as partial,
                     CASE
                        WHEN pred_label = 1 THEN timestamp
                     END as timestamp_unpaved,
                     CASE
                        WHEN pred_label = -1 THEN timestamp
                     END as timestamp_paved,
                     id,
                     timestamp,
                     osm_tags_highway
                FROM(
                    SELECT
                        {col}, osm_id, 
                        UNNEST(pred_label) as pred_label,
                        UNNEST(distance_meter) as distance_meter,
                        distance_meter_total,
                        UNNEST(id) as id,
                        UNNEST(timestamp) as timestamp,
                        UNNEST(osm_tags_highway) as osm_tags_highway
                    FROM(
                        SELECT
                            {col}, osm_id,
                            list(pred_label) as pred_label,
                            list(distance_meter) as distance_meter,
                            SUM(distance_meter) as distance_meter_total,
                            list(id) as id,
                            list(timestamp) as timestamp,
                            list(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT 
                                CASE 
                                    WHEN pred_label = 0 THEN -1 
                                    WHEN pred_label = 1 THEN 1
                                END as pred_label,
                                {col},
                                osm_id,
                                distance_meter,
                                timestamp,
                                id,
                                osm_tags_highway
                            FROM main_df
                            WHERE 
                                zs_pred_score < {sigma}
                                AND zs_pred_score > -{sigma}
                                AND pred_score >= {score}
                                AND no_road_image_filter = 1
                                AND pred_label IS NOT NULL
                                AND distance_meter < {pred_distance}
                            )
                        GROUP BY {col}, osm_id
                        )
                    )
                ) a
            LEFT JOIN (SELECT length, osm_id, {col} FROM mapillary_line) b 
                 ON cast(a.{col} as INT) = cast(b.{col} as INT) and a.osm_id = b.osm_id
            GROUP BY a.{col}, a.osm_id, b.osm_id
            )
        GROUP BY {col}, osm_id
        )
    GROUP BY {col}
    ),

    osm AS(
        SELECT 
            {col},
            COUNT(DISTINCT osm_id) AS number_of_osm_ids_in_area,
            SUM(length) AS length,
            {create_osm_general_strings(road_classes)}
        FROM(
            SELECT
                {col},
                osm_id,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT cast(osm_id as varchar) as osm_id, {col},length, osm_tags_highway
                FROM osm_df
                ) 
            GROUP BY {col}, osm_id
            )
        GROUP BY {col}
    ),

    length_data AS(
        SELECT
            SUM(length) as total_length,
            {col},
            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            list(latest_timestamp) as latest_timestamps,

            {create_osm_general_strings(road_classes)}
        FROM(
            SELECT
                {col},
                osm_id,
                MAX(length) as length,
                MAX(timestamp) as latest_timestamp,
                COUNT(DISTINCT id) as id,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, a.{col}, a.timestamp, a.id, a.osm_tags_highway, 
                    b.length
                FROM
                    (SELECT
                        osm_id, {col}, timestamp, id, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT osm_id, length, {col} FROM mapillary_line) b
                ON cast(a.{col} as INT) = cast(b.{col} as INT) and a.osm_id = b.osm_id
                )
            GROUP BY {col}, osm_id
            )
        GROUP BY {col}
    ),
      
    aggregated_data AS (
        SELECT 
            {col},
            LIST(DISTINCT country) AS countries,
            LIST(DISTINCT continent) AS continents,
            LIST(DISTINCT sequence) AS sequences,
            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT id) AS number_of_images,
            LIST(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            LIST(DISTINCT z14_tiles) AS z14_tiles,
            COUNT(DISTINCT z14_tiles) AS number_of_z14_tiles
        FROM(
            SELECT 
                {col}, sequence, id, country, continent, osm_id, z14_tiles, distance_meter
            FROM main_df
            WHERE {col} IS NOT NULL
            )
        GROUP BY {col}
    )
      
    SELECT * 
    FROM(
        SELECT
            a.*,
            b.paved_segments, b.unpaved_segments,
            b.length, b.length_unpaved, b.length_paved,
            b.n_predictions_p_osm_id,
            b.latest_timestamps,

            {add_prefix([x for x in paved_tags if 'urban' not in x and 'rural' not in x], 'b.')},
            {add_prefix_and_suffix([x for x in id_tags if 'urban' not in x and 'rural' not in x], 'b.', 'pred_')},
            {add_prefix_and_suffix([x for x in length_tags if 'urban' not in x and 'rural' not in x], 'b.', 'pred_')},

            c.number_of_osm_ids,
            c.length as osm_length,
 
            {add_prefix_and_suffix([x for x in id_tags if 'urban' not in x and 'rural' not in x], 'c.', 'osm_')},
            {add_prefix_and_suffix([x for x in length_tags if 'urban' not in x and 'rural' not in x], 'c.', 'osm_')},
    
            d.total_length,
            d.n_points_p_osm_id,
            d.latest_timestamps as latest_timestamps_osm,

            {add_prefix([x for x in id_tags if 'urban' not in x and 'rural' not in x], 'd.')},
            {add_prefix([x for x in length_tags if 'urban' not in x and 'rural' not in x], 'd.')}
        FROM aggregated_data a
        LEFT JOIN pred_stats b ON CAST(a.{col} as INT) = CAST(b.{col} as INT)
        LEFT JOIN osm c ON CAST(a.{col} as INT) = CAST(c.{col} as INT)
        LEFT JOIN length_data d ON CAST(a.{col} as INT) = CAST(d.{col} as INT)
    )
    WHERE {col} IS NOT NULL
    )
    TO '{output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    return query_urban

def process_file(filedir, sigma, score, threshold, results_dir, osm_distance, pred_distance, osm_filedir, data_input_filepath,
                 cols, urban_areas):
    """Build layer queries for one input path and emit debug SQL logs."""
    tile = filedir.split('/')[-1].split('=')[-1]
    os.chdir(filedir)
    ph_metadata_file = f'{tile}.parquet'
    osm_input_filepath = f'{osm_filedir}/tile={tile}/*.parquet'

    z8_tiles_output_filepath = f'{results_dir}/{zoom_level}_tiles/{zoom_level}_tiles_with_stats_{ph_metadata_file}'
    z14_tiles_output_filepath = f'{results_dir}/z14_tiles/z14_tiles_with_stats_{ph_metadata_file}'
    country_output_filepath = f'{results_dir}/countries/countries_with_stats_{ph_metadata_file}'
    continent_output_filepath = f'{results_dir}/continents/continents_with_stats_{ph_metadata_file}'
    world_output_filepath = f'{results_dir}/world/world_with_stats_{ph_metadata_file}'
    lines_output_filepath = f'{results_dir}/lines/all_lines_{ph_metadata_file}.parquet'

    for filepath in [z8_tiles_output_filepath, z14_tiles_output_filepath,
                    country_output_filepath, continent_output_filepath,
                    world_output_filepath, lines_output_filepath]:
        dirname = os.path.dirname(filepath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    condition_str = f"{zoom_level}_tiles = {tile}"
    query_main_osm =  f"""
    SELECT DISTINCT *
    FROM(
        SELECT 
            cast(osm_id as varchar) as osm_id, country, continent, timestamp, correct_z14_tiles_osm(CAST(z_14_tiles_list AS VARCHAR[][])) AS z_14_tiles_list, 
            CAST(ID_HDC_G0 as int) AS ID_HDC_G0, CAST(agglosID as int) AS agglosID, {zoom_level}_tiles_list, UNNEST({zoom_level}_tiles_list) as {zoom_level}_tiles, length, osm_tags_highway, 
            CASE 
                WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
                ELSE 0
            END AS urban,
            1 as world
        FROM(
            SELECT * 
            FROM read_parquet('{osm_input_filepath}')
            WHERE {condition_str}
            )
        );
    """    
    query_main_metadata = f"""
    SELECT 
        DISTINCT sequence, id,
        cast(osm_id as varchar) as osm_id,
        cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles,
        CAST(z14_tiles AS VARCHAR) AS z14_tiles,
        CAST(ID_HDC_G0 as int) AS ID_HDC_G0,
        CAST(agglosID as int) AS agglosID,
        country, continent, pred_label, pred_score, zs_pred_score, road_pixel_percentage,
        geometry, no_road_image_filter, timestamp, long, lat, distance_meter, {zoom_level}_tiles, osm_tags_highway, 
        CASE 
            WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
            ELSE 0
        END AS urban,
        1 as world
      FROM read_parquet('{data_input_filepath}');
    """
    query_lines = f"""
    WITH
    data AS(
        SELECT
            osm_id, sequence,
            list(id) as ids,
            list_distinct(list({zoom_level}_tiles)) as {zoom_level}_tiles,
            list_distinct(list(CAST(z14_tiles as VARCHAR))) as z14_tiles,
            list_distinct(list(ID_HDC_G0)) as ID_HDC_G0,
            list_distinct(list(agglosID)) as agglosID,
            list_distinct(list(country)) as country,
            list_distinct(list(continent)) as continent,
            list_distinct(list(world)) as world,
            list(pred_label) as pred_labels,
            list(zs_pred_score) as zs_pred_scores,
            list(timestamp) as timestamps,
            list(long) as longs,
            list(lat) as lats,
            CASE 
                WHEN len(list(id)) > 1 THEN ST_MakeLine(list(geometry))
                ELSE list_extract(list(geometry),1)
            END as geometry
        FROM(
             SELECT
                sequence, id, cast(osm_id as varchar) as osm_id, cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles,
                z14_tiles, ID_HDC_G0, agglosID, country, continent,
                pred_label, zs_pred_score, timestamp, ST_GeomFromWKB(geometry) as geometry, long, lat, urban, world
             FROM main_df
             ORDER BY continent, country, {zoom_level}_tiles, z14_tiles, osm_id, sequence, timestamp ASC
            )
        GROUP BY continent, country, {zoom_level}_tiles, agglosID, ID_HDC_G0, urban, z14_tiles, osm_id, sequence
    ),

    temp AS(
        SELECT *,
            CASE
                WHEN len(ids) > 1 THEN calculate_length(longs,lats)
                ELSE 0
            END as length
        FROM data
    )

    SELECT * REPLACE ST_AsWKB(geometry) as geometry
    FROM temp;
    """
    query_line_to_file = f"""
    COPY(
        SELECT *
        FROM lines_df
        )
        TO '{lines_output_filepath}'
        (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    
    ##########################################################
    query_z8 = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles, 
            osm_id,
            MAX(length) as length,
            urban
        FROM(
            SELECT
                {zoom_level}_tiles,
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length,
                urban
            FROM(
                SELECT
                    UNNEST({zoom_level}_tiles) as {zoom_level}_tiles, urban, osm_id, length, sequence
                FROM lines_df
                )
            GROUP BY {zoom_level}_tiles, urban, osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY {zoom_level}_tiles, urban, osm_id
    ),
    
    pred_stats AS(
    SELECT
        {zoom_level}_tiles, ANY_VALUE(osm_id) as osm_id, ANY_VALUE(urban) as urban, 

        SUM(pred_label) AS unpaved_segments,
        COUNT(*) - SUM(pred_label) AS paved_segments,
        SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS unpaved_segments_urban,
        SUM(CASE WHEN urban = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS paved_segments_urban,
        SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS unpaved_segments_rural,
        SUM(CASE WHEN urban = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS paved_segments_rural,
        
        {create_surface_general_strings(road_classes)},
        {create_surface_urban_rural_strings(road_classes=road_classes)},

        SUM(length) AS length,
        SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
        SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,

        SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
        SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_urban,
        SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_rural,
        
        list(latest_timestamp) as latest_timestamps,
        list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
        list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
        
        SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
        SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,
        SUM(CASE WHEN urban = 1  and pred_label = 1 THEN length ELSE 0 END) AS length_urban_unpaved,
        SUM(CASE WHEN urban = 1  and pred_label = 0 THEN length ELSE 0 END) AS length_urban_paved,
        SUM(CASE WHEN urban = 0  and pred_label = 1 THEN length ELSE 0 END) AS length_rural_unpaved,
        SUM(CASE WHEN urban = 0  and pred_label = 0 THEN length ELSE 0 END) AS length_rural_paved,
        {create_osm_general_strings(road_classes)},
        {create_osm_urban_rural_strings(road_classes)}
    FROM(
        SELECT
            {zoom_level}_tiles,
            urban,
            MODE(pred_label) as pred_label,
            osm_id,
            MAX(length) as length,
            MAX(id) as id,
            MAX(latest_timestamp) as latest_timestamp,
            ANY_VALUE(osm_tags_highway) as osm_tags_highway
        FROM(
            SELECT
                a.{zoom_level}_tiles, a.osm_id, a.urban,
                CASE
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                    ELSE NULL
                END as pred_label,
                MAX(b.length) as length,
                COUNT(DISTINCT a.id) as id,
                MAX(timestamp) as latest_timestamp,
                ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles, osm_id, 
                    CASE
                        WHEN distance_meter = distance_meter_total THEN pred_label
                        ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                    END as partial,
                    urban,
                    CASE
                        WHEN pred_label = 1 THEN timestamp
                    END as timestamp_unpaved,
                    CASE
                        WHEN pred_label = -1 THEN timestamp
                    END as timestamp_paved,
                    id,
                    timestamp,
                    osm_tags_highway
                FROM(
                    SELECT
                        {zoom_level}_tiles, osm_id, 
                        UNNEST(pred_label) as pred_label,
                        UNNEST(distance_meter) as distance_meter,
                        UNNEST(urban) as urban,
                        distance_meter_total,
                        UNNEST(id) as id,
                        UNNEST(timestamp) as timestamp,
                        UNNEST(osm_tags_highway) as osm_tags_highway
                    FROM(
                        SELECT
                            {zoom_level}_tiles, osm_id,
                            list(pred_label) as pred_label,
                            list(distance_meter) as distance_meter,
                            list(urban) as urban,
                            SUM(distance_meter) as distance_meter_total,
                            list(id) as id,
                            list(timestamp) as timestamp,
                            list(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT 
                                CASE 
                                    WHEN pred_label = 0 THEN -1 
                                    WHEN pred_label = 1 THEN 1
                                END as pred_label,
                                {zoom_level}_tiles,
                                osm_id,
                                distance_meter,
                                urban,
                                id,
                                timestamp,
                                osm_tags_highway
                            FROM main_df
                            WHERE 
                                zs_pred_score < {sigma}
                                AND zs_pred_score > -{sigma}
                                AND pred_score >= {score}
                                AND no_road_image_filter = 1
                                AND pred_label IS NOT NULL
                                AND distance_meter < {pred_distance}
                            )
                        GROUP BY {zoom_level}_tiles, urban, osm_id
                        )
                    )
                ) a
            LEFT JOIN (SELECT length, osm_id, {zoom_level}_tiles, urban FROM mapillary_line) b 
                ON a.{zoom_level}_tiles = b.{zoom_level}_tiles and a.osm_id = b.osm_id and a.urban = b.urban
            GROUP BY a.{zoom_level}_tiles, a.osm_id, a.urban, b.{zoom_level}_tiles, b.osm_id, b.urban
            )
        GROUP BY {zoom_level}_tiles, urban, osm_id
        )
    GROUP BY {zoom_level}_tiles
    ),

    osm AS(
        SELECT 
            {zoom_level}_tiles,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            {create_osm_urban_rural_strings(road_classes=road_classes, alias_prefix='')},
            {create_osm_general_strings(road_classes=road_classes, alias_prefix='')}
        FROM(
            SELECT
                {zoom_level}_tiles,
                osm_id,
                urban,
                MAX(length) as length, 
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast(osm_id as varchar) as osm_id, length, urban, cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles, osm_tags_highway
                FROM osm_df
                )
            GROUP BY {zoom_level}_tiles, urban, osm_id
            )
        GROUP BY {zoom_level}_tiles
    ),
    
    length_data AS(
        SELECT
            SUM(length) as total_length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) as total_urban_length,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) as total_rural_length,
            {zoom_level}_tiles,
            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_points_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_points_p_osm_id_rural,
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
            {create_osm_urban_rural_strings(road_classes=road_classes, alias_prefix='')},
            {create_osm_general_strings(road_classes=road_classes, alias_prefix='')}
        FROM(
            SELECT
                {zoom_level}_tiles, 
                osm_id,
                urban,
                MAX(length) as length,
                COUNT(DISTINCT id) as id,
                MAX(timestamp) as latest_timestamp,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, a.urban, a.{zoom_level}_tiles, a.id, a.timestamp, a.osm_tags_highway, 
                    b.length
                FROM(
                    SELECT
                        osm_id, urban, cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles, id, timestamp, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT {zoom_level}_tiles, osm_id, length, urban FROM mapillary_line) b
                ON a.{zoom_level}_tiles = b.{zoom_level}_tiles and a.osm_id = b.osm_id and a.urban = b.urban
                )
            GROUP BY {zoom_level}_tiles, urban, osm_id
            )
        GROUP BY {zoom_level}_tiles
    ),

    aggregated_data AS (
        SELECT 
            {zoom_level}_tiles,
            LIST(DISTINCT country) AS countries,
            LIST(DISTINCT continent) AS continents,

            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN sequence ELSE NULL END) AS number_of_sequences_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN id ELSE NULL END) AS number_of_images_rural,

            COUNT(DISTINCT id) AS number_of_images,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN sequence ELSE NULL END) AS number_of_sequences_rural,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN id ELSE NULL END) AS number_of_images_urban,

            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 1 THEN osm_id END) AS osm_ids_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 0 THEN osm_id END) AS osm_ids_rural,

            COUNT(DISTINCT z14_tiles) AS number_of_z14_tiles,
            LIST(DISTINCT ID_HDC_G0) AS ID_HDC_G0,
            COUNT(DISTINCT ID_HDC_G0) AS count_ID_HDC_G0,
            LIST(DISTINCT agglosID) AS agglosID,
            COUNT(DISTINCT agglosID) AS count_agglosID
        FROM(
            SELECT 
                country, sequence, id, continent, z14_tiles, ID_HDC_G0, agglosID, osm_id, urban, distance_meter, cast({zoom_level}_tiles as varchar) as {zoom_level}_tiles, 
            FROM main_df
            WHERE {zoom_level}_tiles IS NOT NULL
            )
        GROUP BY {zoom_level}_tiles
    )
    
    SELECT
        a.*,
        b.paved_segments, b.unpaved_segments,
        b.paved_segments_urban, b.unpaved_segments_urban,
        b.paved_segments_rural, b.unpaved_segments_rural,
        b.length, b.length_urban, b.length_rural,

        b.n_predictions_p_osm_id, b.n_predictions_p_osm_id_urban, b.n_predictions_p_osm_id_rural,
        b.latest_timestamps, b.latest_timestamps_urban, b.latest_timestamps_rural, 

        b.length_unpaved, b.length_paved,
        b.length_urban_unpaved, b.length_urban_paved, 
        b.length_rural_unpaved, b.length_rural_paved,

        {add_prefix(paved_tags, 'b.')},
        {add_prefix_and_suffix(id_tags, 'b.', 'pred_')},
        {add_prefix_and_suffix(length_tags, 'b.', 'pred_')},

        c.number_of_osm_ids, c.number_of_osm_ids_urban, c.number_of_osm_ids_rural,
        c.length as osm_length, c.length_urban as osm_length_urban, c.length_rural as osm_length_rural,

        {add_prefix_and_suffix(id_tags, 'c.', 'osm_')},
        {add_prefix_and_suffix(length_tags, 'c.', 'osm_')},

        d.total_length, d.total_urban_length, d.total_rural_length,
        d.n_points_p_osm_id, d.n_points_p_osm_id_urban, d.n_points_p_osm_id_rural,

        d.latest_timestamps as latest_timestamps_osm, d.latest_timestamps_urban as latest_timestamps_urban_osm,
        d.latest_timestamps_rural as latest_timestamps_rural_osm,

        {add_prefix(id_tags, 'd.')},
        {add_prefix(length_tags, 'd.')}
    FROM aggregated_data a
    LEFT JOIN pred_stats b ON a.{zoom_level}_tiles = b.{zoom_level}_tiles
    LEFT JOIN osm c ON a.{zoom_level}_tiles = c.{zoom_level}_tiles
    LEFT JOIN length_data d ON a.{zoom_level}_tiles = d.{zoom_level}_tiles
    )   
    TO '{z8_tiles_output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_z14 = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            z14_tiles, 
            osm_id,
            urban,
            MAX(length) as length
        FROM(
            SELECT
                z14_tiles,
                urban, 
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length
            FROM(
                SELECT
                    UNNEST(z14_tiles) as z14_tiles, osm_id, length, sequence, urban
                FROM lines_df
                )
            GROUP BY z14_tiles, urban,  osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY z14_tiles, urban, osm_id
    ),
    
    pred_stats AS(
    SELECT
        z14_tiles,
        SUM(pred_label) AS unpaved_segments,
        COUNT(*) - SUM(pred_label) AS paved_segments,
        SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS unpaved_segments_urban,
        SUM(CASE WHEN urban = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS paved_segments_urban,
        SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS unpaved_segments_rural,
        SUM(CASE WHEN urban = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS paved_segments_rural,

        {create_surface_general_strings(road_classes)},
        {create_surface_urban_rural_strings(road_classes=road_classes)},

        SUM(length) AS length,
        SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
        SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,

        SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
        SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_urban,
        SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_rural,
        
        list(latest_timestamp) as latest_timestamps,
        list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
        list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
        
        SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
        SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,
        SUM(CASE WHEN urban = 1  and pred_label = 1 THEN length ELSE 0 END) AS length_urban_unpaved,
        SUM(CASE WHEN urban = 1  and pred_label = 0 THEN length ELSE 0 END) AS length_urban_paved,
        SUM(CASE WHEN urban = 0  and pred_label = 1 THEN length ELSE 0 END) AS length_rural_unpaved,
        SUM(CASE WHEN urban = 0  and pred_label = 0 THEN length ELSE 0 END) AS length_rural_paved,

        {create_osm_general_strings(road_classes, alias_prefix='')},
        {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
    FROM(
        SELECT
            z14_tiles,
            osm_id,
            urban,
            MODE(pred_label) as pred_label,
            MAX(length) as length,
            MAX(id) as id,
            MAX(latest_timestamp) as latest_timestamp,
            ANY_VALUE(osm_tags_highway) as osm_tags_highway
        FROM(
            SELECT
                a.z14_tiles, a.osm_id, a.urban, 
                CASE
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                    ELSE NULL
                END as pred_label,
                MAX(b.length) as length,
                COUNT(DISTINCT a.id) as id,
                MAX(timestamp) as latest_timestamp,
                ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    z14_tiles, osm_id, urban, 
                    CASE
                        WHEN distance_meter = distance_meter_total THEN  pred_label
                        ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                    END as partial,
                     CASE
                        WHEN pred_label = 1 THEN timestamp
                    END as timestamp_unpaved,
                    CASE
                        WHEN pred_label = -1 THEN timestamp
                    END as timestamp_paved,
                    id,
                    timestamp,
                    osm_tags_highway
                FROM(
                    SELECT
                        z14_tiles, osm_id, 
                        UNNEST(pred_label) as pred_label,
                        UNNEST(distance_meter) as distance_meter,
                        UNNEST(urban) as urban,
                        distance_meter_total,
                        UNNEST(id) as id,
                        UNNEST(timestamp) as timestamp,
                        UNNEST(osm_tags_highway) as osm_tags_highway
                    FROM(
                        SELECT
                            z14_tiles, osm_id, 
                            list(urban) as urban, 
                            list(pred_label) as pred_label,
                            list(distance_meter) as distance_meter,
                            SUM(distance_meter) as distance_meter_total,
                            list(id) as id,
                            list(timestamp) as timestamp,
                            list(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT 
                                CASE 
                                    WHEN pred_label = 0 THEN -1 
                                    WHEN pred_label = 1 THEN 1
                                END as pred_label,
                                z14_tiles,
                                osm_id,
                                distance_meter,
                                urban,
                                id,
                                timestamp,
                                osm_tags_highway
                            FROM main_df
                            WHERE 
                                zs_pred_score < {sigma}
                                AND zs_pred_score > -{sigma}
                                AND pred_score >= {score}
                                AND no_road_image_filter = 1
                                AND pred_label IS NOT NULL
                                AND distance_meter < {pred_distance}
                            )
                        GROUP BY z14_tiles, urban, osm_id
                        )
                    )
                ) a
            LEFT JOIN (SELECT length, osm_id, z14_tiles, urban FROM mapillary_line) b 
                ON a.z14_tiles = b.z14_tiles AND a.osm_id = b.osm_id and a.urban = b.urban
            GROUP BY a.z14_tiles, a.osm_id, a.urban, b.osm_id, b.urban
            )
        GROUP BY z14_tiles, urban, osm_id
        )
    GROUP BY z14_tiles
    ),

    osm_z14 AS(
        SELECT 
            z14_tiles,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids_in_area,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            {create_osm_urban_rural_strings(road_classes=road_classes, alias_prefix='')},
            {create_osm_general_strings(road_classes=road_classes, alias_prefix='')}
        FROM(
            SELECT
                z14_tiles,
                osm_id,
                urban,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast(osm_id as varchar) as osm_id, length, urban, UNNEST(z_14_tiles_list) AS z14_tiles, osm_tags_highway
                FROM osm_df
                )
            GROUP BY z14_tiles, urban, osm_id
            )
        GROUP BY z14_tiles
    ),

    length_data AS(
        SELECT
            SUM(length) as total_length,
            SUM(CASE WHEN urban=1 THEN length ELSE 0 END) as total_urban_length,
            SUM(CASE WHEN urban=0 THEN length ELSE 0 END) as total_rural_length,
            z14_tiles,
            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_points_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_points_p_osm_id_rural,
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
            {create_osm_urban_rural_strings(road_classes=road_classes, alias_prefix='')},
            {create_osm_general_strings(road_classes=road_classes, alias_prefix='')}
        FROM(
            SELECT
                z14_tiles,
                urban,
                MAX(length) as length,
                osm_id,
                MAX(timestamp) as latest_timestamp,
                COUNT(DISTINCT id) as id ,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, a.z14_tiles, a.urban, a.timestamp, a.id, a.osm_tags_highway, 
                    b.length
                FROM
                    (SELECT
                        osm_id, z14_tiles, urban, timestamp, id, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT osm_id, length, z14_tiles, urban FROM mapillary_line) b
                ON a.z14_tiles = b.z14_tiles AND a.osm_id = b.osm_id and a.urban = b.urban
                )
            GROUP BY z14_tiles, urban, osm_id
            )
        GROUP BY z14_tiles
    ),
    
    aggregated_data AS (
        SELECT 
            z14_tiles,
            LIST(DISTINCT sequence) AS sequences,
            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT id) AS number_of_images,
            LIST(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            LIST(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 1 THEN osm_id END) AS osm_ids_urban,
            LIST(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 0 THEN osm_id END) AS osm_ids_rural,
            LIST(DISTINCT ID_HDC_G0) AS GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2,
            LIST(DISTINCT agglosID) AS AFRICAPOLIS2020,
            LIST(DISTINCT country) AS countries,
            LIST(DISTINCT continent) AS continents
        FROM(
            SELECT 
                z14_tiles, sequence, id, ID_HDC_G0, agglosID, country, continent, osm_id, distance_meter, urban
            FROM main_df
            WHERE z14_tiles IS NOT NULL
            )
        GROUP BY z14_tiles
    )
          
    SELECT 
        a.*,
        b.paved_segments, b.unpaved_segments,
        b.paved_segments_urban, b.unpaved_segments_urban,
        b.paved_segments_rural, b.unpaved_segments_rural,

        b.length, b.length_urban, b.length_rural,
        
        b.n_predictions_p_osm_id, b.n_predictions_p_osm_id_urban, b.n_predictions_p_osm_id_rural,
        b.latest_timestamps, b.latest_timestamps_urban, b.latest_timestamps_rural, 

        b.length_unpaved, b.length_paved,
        b.length_urban_unpaved, b.length_urban_paved, 
        b.length_rural_unpaved, b.length_rural_paved,

        {add_prefix(paved_tags, 'b.')},
        {add_prefix_and_suffix(id_tags, 'b.', 'pred_')},
        {add_prefix_and_suffix(length_tags, 'b.', 'pred_')},
        
        c.number_of_osm_ids, c.number_of_osm_ids_urban, c.number_of_osm_ids_rural,
        c.length as osm_length, c.length_urban as osm_length_urban, c.length_rural as osm_length_rural,

        {add_prefix_and_suffix(id_tags, 'c.', 'osm_')},
        {add_prefix_and_suffix(length_tags, 'c.', 'osm_')},

        d.total_length, d.total_urban_length, d.total_rural_length,
        d.n_points_p_osm_id, d.n_points_p_osm_id_urban, d.n_points_p_osm_id_rural,

        d.latest_timestamps as latest_timestamps_osm, d.latest_timestamps_urban as latest_timestamps_urban_osm,
        d.latest_timestamps_rural as latest_timestamps_rural_osm,

        {add_prefix(id_tags, 'd.')},
        {add_prefix(length_tags, 'd.')}

    FROM aggregated_data a
    LEFT JOIN pred_stats b ON CAST(a.z14_tiles AS VARCHAR) = CAST(b.z14_tiles AS VARCHAR)
    LEFT JOIN osm_z14 c ON CAST(a.z14_tiles AS VARCHAR) = CAST(c.z14_tiles AS VARCHAR)
    LEFT JOIN length_data d ON CAST(a.z14_tiles AS VARCHAR) = CAST(d.z14_tiles AS VARCHAR)
    )
    TO '{z14_tiles_output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_country = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            country, 
            osm_id,
            urban,
            MAX(length) as length
        FROM(
            SELECT
                country,
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length,
                urban
            FROM(
                SELECT
                    UNNEST(country) as country, urban, osm_id, length, sequence
                FROM lines_df
                )
            GROUP BY country, urban, osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY country, urban, osm_id
    ),
    
    pred_stats AS(
        SELECT
            country, ANY_VALUE(osm_id) as osm_id, ANY_VALUE(urban) as urban, 

            SUM(pred_label) AS unpaved_segments,
            COUNT(*) - SUM(pred_label) AS paved_segments,
            SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS unpaved_segments_urban,
            SUM(CASE WHEN urban = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS paved_segments_urban,
            SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS unpaved_segments_rural,
            SUM(CASE WHEN urban = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS paved_segments_rural,

            {create_surface_general_strings(road_classes)},
            {create_surface_urban_rural_strings(road_classes=road_classes)},
            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            
            SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_rural,
            
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,

            SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
            SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,
            SUM(CASE WHEN urban = 1  and pred_label = 1 THEN length ELSE 0 END) AS length_urban_unpaved,
            SUM(CASE WHEN urban = 1  and pred_label = 0 THEN length ELSE 0 END) AS length_urban_paved,
            SUM(CASE WHEN urban = 0  and pred_label = 1 THEN length ELSE 0 END) AS length_rural_unpaved,
            SUM(CASE WHEN urban = 0  and pred_label = 0 THEN length ELSE 0 END) AS length_rural_paved,

            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                country,
                osm_id,
                urban,
                MODE(pred_label) as pred_label,
                MAX(length) as length,
                MAX(id) as id,
                MAX(latest_timestamp) as latest_timestamp,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.country, a.osm_id, a.urban, 
                    CASE
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                        ELSE NULL
                    END as pred_label,
                    MAX(b.length) as length,
                    COUNT(DISTINCT a.id) as id,
                    MAX(timestamp) as latest_timestamp,
                    ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
                FROM(
                    SELECT
                        country, osm_id, 
                        CASE
                            WHEN distance_meter = distance_meter_total THEN  pred_label
                            ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                        END as partial,
                        urban,
                        CASE
                            WHEN pred_label = 1 THEN timestamp
                        END as timestamp_unpaved,
                        CASE
                            WHEN pred_label = -1 THEN timestamp
                        END as timestamp_paved,
                        id,
                        timestamp,
                        osm_tags_highway
                    FROM(
                        SELECT
                            country, osm_id, 
                            UNNEST(pred_label) as pred_label,
                            UNNEST(distance_meter) as distance_meter,
                            UNNEST(urban) as urban,
                            distance_meter_total,
                            UNNEST(id) as id,
                            UNNEST(timestamp) as timestamp,
                            UNNEST(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT
                                country, osm_id,
                                list(pred_label) as pred_label,
                                list(distance_meter) as distance_meter,
                                list(urban) as urban,
                                SUM(distance_meter) as distance_meter_total,
                                list(id) as id,
                                list(timestamp) as timestamp,
                                list(osm_tags_highway) as osm_tags_highway
                            FROM(
                                SELECT 
                                    CASE 
                                        WHEN pred_label = 0 THEN -1 
                                        WHEN pred_label = 1 THEN 1
                                    END as pred_label,
                                    country,
                                    osm_id,
                                    distance_meter,
                                    urban,
                                    id,
                                    timestamp,
                                    osm_tags_highway
                                FROM main_df
                                WHERE 
                                    zs_pred_score < {sigma}
                                    AND zs_pred_score > -{sigma}
                                    AND pred_score >= {score}
                                    AND no_road_image_filter = 1
                                    AND pred_label IS NOT NULL
                                    AND distance_meter < {pred_distance}
                                )
                            GROUP BY country, urban, osm_id
                            )
                        )
                    ) a
                LEFT JOIN (SELECT length, osm_id, country, urban FROM mapillary_line) b 
                    ON a.country = b.country and a.osm_id = b.osm_id and a.urban = b.urban
                GROUP BY a.country, a.osm_id, a.urban, b.osm_id, b.country, b.urban
                )
            GROUP BY country, urban, osm_id
            )
        GROUP BY country
    ),

    osm AS(
        SELECT 
            country,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                country,
                osm_id,
                urban,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast(osm_id as varchar) as osm_id, length, urban, country, osm_tags_highway
                FROM osm_df
                )
            GROUP BY country, urban, osm_id
            )
        GROUP BY country
    ),

    length_data AS(
        SELECT
            SUM(length) as total_length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) as total_urban_length,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) as total_rural_length,
            country,
            
            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_points_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_points_p_osm_id_rural,
            
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,

            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                country, 
                urban,
                MAX(length) as length,
                osm_id,
                MAX(timestamp) as latest_timestamp,
                COUNT(DISTINCT id) as id,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, 
                    a.urban,
                    a.country,
                    b.length,
                    a.timestamp,
                    a.id,
                    a.osm_tags_highway
                FROM(
                    SELECT
                        osm_id, urban, country, timestamp, id, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT osm_id, length, country, urban FROM mapillary_line) b
                ON a.country = b.country and a.osm_id = b.osm_id and a.urban = b.urban
                )
            GROUP BY country, urban, osm_id
            )
        GROUP BY country
    ),
    
    aggregated_data AS (
        SELECT 
            country,
            LIST(DISTINCT continent) AS continents,
            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN sequence ELSE NULL END) AS number_of_sequences_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN id ELSE NULL END) AS number_of_images_rural,
            COUNT(DISTINCT id) AS number_of_images,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN sequence ELSE NULL END) AS number_of_sequences_rural,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN id ELSE NULL END) AS number_of_images_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 1 THEN osm_id END) AS osm_ids_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 0 THEN osm_id END) AS osm_ids_rural,
            COUNT(DISTINCT z14_tiles) AS number_of_z14_tiles,
            LIST(DISTINCT ID_HDC_G0) AS ID_HDC_G0,
            COUNT(DISTINCT ID_HDC_G0) AS count_ID_HDC_G0,
            LIST(DISTINCT agglosID) AS agglosID,
            COUNT(DISTINCT agglosID) AS count_agglosID
        FROM(
            SELECT 
                country, sequence, id, continent, z14_tiles, ID_HDC_G0, agglosID, osm_id, urban, distance_meter
            FROM main_df
            WHERE country IS NOT NULL
            )
        GROUP BY country
    )

    SELECT
        a.*,
        b.paved_segments, b.unpaved_segments,
        b.paved_segments_urban, b.unpaved_segments_urban,
        b.paved_segments_rural, b.unpaved_segments_rural,

        b.length, b.length_urban, b.length_rural,
        
        b.n_predictions_p_osm_id, b.n_predictions_p_osm_id_urban, b.n_predictions_p_osm_id_rural,
        b.latest_timestamps, b.latest_timestamps_urban, b.latest_timestamps_rural, 

        b.length_unpaved, b.length_paved,
        b.length_urban_unpaved, b.length_urban_paved, 
        b.length_rural_unpaved, b.length_rural_paved,

        {add_prefix(paved_tags, 'b.')},
        {add_prefix_and_suffix(id_tags, 'b.', 'pred_')},
        {add_prefix_and_suffix(length_tags, 'b.', 'pred_')},
        
        c.number_of_osm_ids, c.number_of_osm_ids_urban, c.number_of_osm_ids_rural,
        c.length as osm_length, c.length_urban as osm_length_urban, c.length_rural as osm_length_rural,

        {add_prefix_and_suffix(id_tags, 'c.', 'osm_')},
        {add_prefix_and_suffix(length_tags, 'c.', 'osm_')},

        d.total_length, d.total_urban_length, d.total_rural_length,
        d.n_points_p_osm_id, d.n_points_p_osm_id_urban, d.n_points_p_osm_id_rural,

        d.latest_timestamps as latest_timestamps_osm, d.latest_timestamps_urban as latest_timestamps_urban_osm,
        d.latest_timestamps_rural as latest_timestamps_rural_osm,

        {add_prefix(id_tags, 'd.')},
        {add_prefix(length_tags, 'd.')}

    FROM aggregated_data a
    LEFT JOIN pred_stats b ON a.country = b.country
    LEFT JOIN osm c ON a.country = c.country
    LEFT JOIN length_data d ON a.country = d.country
    )   
    TO '{country_output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_continent = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            continent, 
            osm_id,
            urban,
            MAX(length) as length
        FROM(
            SELECT
                continent,
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length, 
                urban
            FROM(
                SELECT
                    UNNEST(continent) as continent, osm_id, length, sequence, urban
                FROM lines_df
                )
            GROUP BY continent, urban, osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY continent, urban, osm_id
    ),
    
    pred_stats AS(
        SELECT
            continent, ANY_VALUE(osm_id) as osm_id, ANY_VALUE(urban) as urban, 
            SUM(pred_label) AS unpaved_segments,
            COUNT(*) - SUM(pred_label) AS paved_segments,
            SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS unpaved_segments_urban,
            SUM(CASE WHEN urban = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS paved_segments_urban,
            SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS unpaved_segments_rural,
            SUM(CASE WHEN urban = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS paved_segments_rural,

            {create_surface_general_strings(road_classes)},
            {create_surface_urban_rural_strings(road_classes=road_classes)},

            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            
            SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_rural,
            
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
            
            SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
            SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,
            SUM(CASE WHEN urban = 1  and pred_label = 1 THEN length ELSE 0 END) AS length_urban_unpaved,
            SUM(CASE WHEN urban = 1  and pred_label = 0 THEN length ELSE 0 END) AS length_urban_paved,
            SUM(CASE WHEN urban = 0  and pred_label = 1 THEN length ELSE 0 END) AS length_rural_unpaved,
            SUM(CASE WHEN urban = 0  and pred_label = 0 THEN length ELSE 0 END) AS length_rural_paved,
            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                continent,
                osm_id,
                urban,
                MODE(pred_label) as pred_label,
                MAX(length) as length,
                MAX(id) as id,
                MAX(latest_timestamp) as latest_timestamp,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.continent, a.osm_id, a.urban, 
                    CASE
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                        WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                        ELSE NULL
                    END as pred_label,
                    MAX(b.length) as length,
                    COUNT(DISTINCT a.id) as id,
                    MAX(timestamp) as latest_timestamp,
                    ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
                FROM(
                    SELECT
                        continent, osm_id, 
                        CASE
                            WHEN distance_meter = distance_meter_total THEN  pred_label
                            ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                        END as partial,
                        urban,
                        CASE
                            WHEN pred_label = 1 THEN timestamp
                        END as timestamp_unpaved,
                        CASE
                            WHEN pred_label = -1 THEN timestamp
                        END as timestamp_paved,
                        id,
                        timestamp,
                        osm_tags_highway
                    FROM(
                        SELECT
                            continent, osm_id, 
                            UNNEST(pred_label) as pred_label,
                            UNNEST(distance_meter) as distance_meter,
                            UNNEST(urban) as urban,
                            distance_meter_total,
                            UNNEST(id) as id,
                            UNNEST(timestamp) as timestamp,
                            UNNEST(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT
                                continent, osm_id,
                                list(pred_label) as pred_label,
                                list(distance_meter) as distance_meter,
                                list(urban) as urban,
                                SUM(distance_meter) as distance_meter_total,
                                list(id) as id,
                                list(timestamp) as timestamp,
                                list(osm_tags_highway) as osm_tags_highway
                            FROM(
                                SELECT 
                                    CASE 
                                        WHEN pred_label = 0 THEN -1 
                                        WHEN pred_label = 1 THEN 1
                                    END as pred_label,
                                    continent,
                                    osm_id,
                                    distance_meter,
                                    urban,
                                    id,
                                    timestamp,
                                    osm_tags_highway
                                FROM main_df
                                WHERE 
                                    zs_pred_score < {sigma}
                                    AND zs_pred_score > -{sigma}
                                    AND pred_score >= {score}
                                    AND no_road_image_filter = 1
                                    AND pred_label IS NOT NULL
                                    AND distance_meter < {pred_distance}
                                )
                            GROUP BY continent, urban, osm_id
                            )
                        )
                    ) a
                LEFT JOIN (SELECT length, osm_id, continent, urban FROM mapillary_line) b 
                    ON a.continent = b.continent and a.osm_id = b.osm_id and a.urban = b.urban
                GROUP BY a.continent, a.osm_id, a.urban, b.osm_id, b.continent, b.urban
                )
            GROUP BY continent, urban, osm_id
            )
        GROUP BY continent
    ),

    osm AS(
        SELECT 
            continent,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,
            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                continent,
                osm_id,
                urban,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast(osm_id as varchar) as osm_id, length, urban, continent, osm_tags_highway
                FROM osm_df
                )
            GROUP BY continent, urban, osm_id
            )
        GROUP BY continent
    ),
    
    length_data AS(
        SELECT
            continent,
            SUM(length) as total_length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) as total_urban_length,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) as total_rural_length,
            
            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_points_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_points_p_osm_id_rural,
            
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,
            
            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                continent, 
                urban,
                MAX(length) as length,
                osm_id,
                MAX(timestamp) as latest_timestamp,
                COUNT(DISTINCT id) as id,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, a.urban, a.continent, a.timestamp, a.id, a.osm_tags_highway,
                    b.length
                FROM(
                    SELECT
                        osm_id, urban, continent, timestamp, id, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT osm_id, length, continent, urban FROM mapillary_line) b
                ON a.continent = b.continent and a.osm_id = b.osm_id and a.urban = b.urban
                )
            GROUP BY continent, urban, osm_id
            )
        GROUP BY continent
    ),

    aggregated_data AS (
        SELECT 
            continent,
            LIST(DISTINCT country) AS countries,
            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN sequence ELSE NULL END) AS number_of_sequences_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN id ELSE NULL END) AS number_of_images_rural,
            COUNT(DISTINCT id) AS number_of_images,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN sequence ELSE NULL END) AS number_of_sequences_rural,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN id ELSE NULL END) AS number_of_images_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 1 THEN osm_id END) AS osm_ids_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 0 THEN osm_id END) AS osm_ids_rural,
            COUNT(DISTINCT z14_tiles) AS number_of_z14_tiles,
            LIST(DISTINCT ID_HDC_G0) AS ID_HDC_G0,
            COUNT(DISTINCT ID_HDC_G0) AS count_ID_HDC_G0,
            LIST(DISTINCT agglosID) AS agglosID,
            COUNT(DISTINCT agglosID) AS count_agglosID
        FROM(
            SELECT 
                sequence, country, id, continent, z14_tiles, ID_HDC_G0, agglosID, osm_id, urban, distance_meter
            FROM main_df
            WHERE continent IS NOT NULL
            )
        GROUP BY continent
    )
     
    SELECT
        a.*,
        b.paved_segments, b.unpaved_segments,
        b.paved_segments_urban, b.unpaved_segments_urban,
        b.paved_segments_rural, b.unpaved_segments_rural,

        b.length, b.length_urban, b.length_rural,
        
        b.n_predictions_p_osm_id, b.n_predictions_p_osm_id_urban, b.n_predictions_p_osm_id_rural,
        b.latest_timestamps, b.latest_timestamps_urban, b.latest_timestamps_rural, 

        b.length_unpaved, b.length_paved,
        b.length_urban_unpaved, b.length_urban_paved, 
        b.length_rural_unpaved, b.length_rural_paved,

        {add_prefix(paved_tags, 'b.')},
        {add_prefix_and_suffix(id_tags, 'b.', 'pred_')},
        {add_prefix_and_suffix(length_tags, 'b.', 'pred_')},
        
        c.number_of_osm_ids, c.number_of_osm_ids_urban, c.number_of_osm_ids_rural,
        c.length as osm_length, c.length_urban as osm_length_urban, c.length_rural as osm_length_rural,

        {add_prefix_and_suffix(id_tags, 'c.', 'osm_')},
        {add_prefix_and_suffix(length_tags, 'c.', 'osm_')},

        d.total_length, d.total_urban_length, d.total_rural_length,
        d.n_points_p_osm_id, d.n_points_p_osm_id_urban, d.n_points_p_osm_id_rural,

        d.latest_timestamps as latest_timestamps_osm, d.latest_timestamps_urban as latest_timestamps_urban_osm,
        d.latest_timestamps_rural as latest_timestamps_rural_osm,

        {add_prefix(id_tags, 'd.')},
        {add_prefix(length_tags, 'd.')}
    FROM aggregated_data a
    LEFT JOIN pred_stats b ON a.continent = b.continent
    LEFT JOIN osm c ON a.continent = c.continent
    LEFT JOIN length_data d ON a.continent = d.continent
    )  
    TO '{continent_output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_world = f"""
    COPY(
    WITH
    mapillary_line AS(
        SELECT 
            world, 
            osm_id,
            urban,
            MAX(length) as length
        FROM(
            SELECT
                world,
                cast(osm_id as varchar) as osm_id,
                MAX(length) as length, 
                urban
            FROM(
                SELECT
                    UNNEST(world) as world, osm_id, length, sequence, urban
                FROM lines_df
                )
            GROUP BY world, urban, osm_id, sequence
            )
        WHERE osm_id IS NOT NULL
        GROUP BY world, urban, osm_id
    ),
    
    pred_stats AS(
    SELECT
        world, ANY_VALUE(osm_id) as osm_id, ANY_VALUE(urban) as urban, 
        SUM(pred_label) AS unpaved_segments,
        COUNT(*) - SUM(pred_label) AS paved_segments,
        SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS unpaved_segments_urban,
        SUM(CASE WHEN urban = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 1 THEN pred_label ELSE 0 END) AS paved_segments_urban,
        SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS unpaved_segments_rural,
        SUM(CASE WHEN urban = 0 THEN 1 ELSE 0 END) - SUM(CASE WHEN urban = 0 THEN pred_label ELSE 0 END) AS paved_segments_rural,

        {create_surface_general_strings(road_classes)},
        {create_surface_urban_rural_strings(road_classes=road_classes)},

        SUM(length) AS length,
        SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
        SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,

        SUM(id)/COUNT(osm_id) as n_predictions_p_osm_id,
        SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_urban,
        SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_predictions_p_osm_id_rural,

        list(latest_timestamp) as latest_timestamps,
        list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
        list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,

        SUM(CASE WHEN pred_label = 1 THEN length ELSE 0 END) AS length_unpaved,
        SUM(CASE WHEN pred_label = 0 THEN length ELSE 0 END) AS length_paved,
        SUM(CASE WHEN urban = 1  and pred_label = 1 THEN length ELSE 0 END) AS length_urban_unpaved,
        SUM(CASE WHEN urban = 1  and pred_label = 0 THEN length ELSE 0 END) AS length_urban_paved,
        SUM(CASE WHEN urban = 0  and pred_label = 1 THEN length ELSE 0 END) AS length_rural_unpaved,
        SUM(CASE WHEN urban = 0  and pred_label = 0 THEN length ELSE 0 END) AS length_rural_paved,
        {create_osm_general_strings(road_classes, alias_prefix='')},
        {create_osm_urban_rural_strings(road_classes, alias_prefix='')}   
    FROM(
        SELECT
            world,
            osm_id,
            urban,
            MODE(pred_label) as pred_label,
            MAX(length) as length,
            MAX(id) as id,
            MAX(latest_timestamp) as latest_timestamp,
            ANY_VALUE(osm_tags_highway) as osm_tags_highway
        FROM(
            SELECT
                a.world, a.osm_id, a.urban, 
                CASE
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) < -{threshold} THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) > {threshold} THEN 1
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) > median(timestamp_unpaved) THEN 0
                    WHEN SUM(a.partial)/NULLIF(COUNT(*),0) <= {threshold} AND  SUM(a.partial)/NULLIF(COUNT(*),0) >= -{threshold} AND median(a.timestamp_paved) < median(timestamp_unpaved) THEN 1
                    ELSE NULL
                END as pred_label,
                MAX(b.length) as length,
                COUNT(DISTINCT a.id) as id,
                MAX(timestamp) as latest_timestamp,
                ANY_VALUE(a.osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    world, osm_id, 
                    CASE
                        WHEN distance_meter = distance_meter_total THEN  pred_label
                        ELSE (distance_meter_total-distance_meter)/NULLIF(distance_meter_total,0)*pred_label 
                    END as partial,
                    urban,
                    CASE
                        WHEN pred_label = 1 THEN timestamp
                    END as timestamp_unpaved,
                    CASE
                        WHEN pred_label = -1 THEN timestamp
                    END as timestamp_paved,
                    id,
                    timestamp,
                    osm_tags_highway
                FROM(
                    SELECT
                        world, osm_id, 
                        UNNEST(pred_label) as pred_label,
                        UNNEST(distance_meter) as distance_meter,
                        UNNEST(urban) as urban,
                        distance_meter_total,
                        UNNEST(id) as id,
                        UNNEST(timestamp) as timestamp,
                        UNNEST(osm_tags_highway) as osm_tags_highway
                    FROM(
                        SELECT
                            world, osm_id,
                            list(pred_label) as pred_label,
                            list(distance_meter) as distance_meter,
                            list(urban) as urban,
                            SUM(distance_meter) as distance_meter_total,
                            list(id) as id,
                            list(timestamp) as timestamp,
                            list(osm_tags_highway) as osm_tags_highway
                        FROM(
                            SELECT 
                                CASE 
                                    WHEN pred_label = 0 THEN -1 
                                    WHEN pred_label = 1 THEN 1
                                END as pred_label,
                                world,
                                osm_id,
                                distance_meter,
                                urban,
                                id,
                                timestamp,
                                osm_tags_highway
                            FROM main_df
                            WHERE 
                                zs_pred_score < {sigma}
                                AND zs_pred_score > -{sigma}
                                AND pred_score >= {score}
                                AND no_road_image_filter = 1
                                AND pred_label IS NOT NULL
                                AND distance_meter < {pred_distance}
                            )
                        GROUP BY world, urban, osm_id
                        )
                    )
                ) a
            LEFT JOIN (SELECT length, osm_id, world, urban FROM mapillary_line) b 
                ON a.world = b.world and a.osm_id = b.osm_id and a.urban = b.urban
            GROUP BY a.world, a.osm_id, a.urban, b.osm_id, b.world, b.urban
            )
        GROUP BY world, urban, osm_id
        )
    GROUP BY world
    ),

    osm AS(
        SELECT 
            world,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,

            SUM(length) AS length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) AS length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) AS length_rural,

            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                world,
                osm_id,
                urban,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    cast(osm_id as varchar) as osm_id, length, urban, world, osm_tags_highway
                FROM osm_df
                )
            GROUP BY world, urban, osm_id
            )
        GROUP BY world
    ),
    
    length_data AS(
        SELECT
            world,

            SUM(length) as total_length,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) as total_urban_length,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) as total_rural_length,

            SUM(id)/COUNT(osm_id) as n_points_p_osm_id,
            SUM(CASE WHEN urban = 1 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 1 THEN osm_id ELSE NULL END) n_points_p_osm_id_urban,
            SUM(CASE WHEN urban = 0 THEN id ELSE 0 END)/COUNT(CASE WHEN urban = 0 THEN osm_id ELSE NULL END) n_points_p_osm_id_rural,
            
            list(latest_timestamp) as latest_timestamps,
            list(CASE WHEN urban=1 THEN latest_timestamp END) as latest_timestamps_urban,
            list(CASE WHEN urban=0 THEN latest_timestamp END) as latest_timestamps_rural,

            {create_osm_general_strings(road_classes, alias_prefix='')},
            {create_osm_urban_rural_strings(road_classes, alias_prefix='')}
        FROM(
            SELECT
                world, 
                urban,
                MAX(length) as length,
                osm_id,
                MAX(timestamp) as latest_timestamp,
                COUNT(DISTINCT id) as id,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway
            FROM(
                SELECT
                    a.osm_id, a.urban, a.world, a.timestamp, a.id, a.osm_tags_highway,
                    b.length
                FROM(
                    SELECT
                        osm_id, urban, world, timestamp, id, osm_tags_highway
                    FROM main_df
                    WHERE distance_meter < {osm_distance} and osm_id IS NOT NULL
                    ) a
                LEFT JOIN (SELECT osm_id, length, world, urban FROM mapillary_line) b
                ON a.world = b.world and a.osm_id = b.osm_id and a.urban = b.urban
                )
            GROUP BY world, urban, osm_id
            )
        GROUP BY world
    ),

    aggregated_data AS (
        SELECT 
            world,
            LIST(DISTINCT country) AS countries,
            COUNT(DISTINCT sequence) AS number_of_sequences,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN sequence ELSE NULL END) AS number_of_sequences_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN id ELSE NULL END) AS number_of_images_rural,
            COUNT(DISTINCT id) AS number_of_images,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN sequence ELSE NULL END) AS number_of_sequences_rural,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN id ELSE NULL END) AS number_of_images_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} THEN osm_id END) AS osm_ids,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 1 THEN osm_id END) AS osm_ids_urban,
            COUNT(DISTINCT CASE WHEN distance_meter <= {osm_distance} and urban = 0 THEN osm_id END) AS osm_ids_rural,
            COUNT(DISTINCT z14_tiles) AS number_of_z14_tiles,
            LIST(DISTINCT ID_HDC_G0) AS ID_HDC_G0,
            COUNT(DISTINCT ID_HDC_G0) AS count_ID_HDC_G0,
            LIST(DISTINCT agglosID) AS agglosID,
            COUNT(DISTINCT agglosID) AS count_agglosID
        FROM(
            SELECT 
                sequence, country, id, world, z14_tiles, ID_HDC_G0, agglosID, osm_id, urban, distance_meter, world
            FROM main_df
            WHERE world IS NOT NULL
            )
        GROUP BY world
    )
     
    SELECT
        a.*,
        a.*,
        b.paved_segments, b.unpaved_segments,
        b.paved_segments_urban, b.unpaved_segments_urban,
        b.paved_segments_rural, b.unpaved_segments_rural,

        b.length, b.length_urban, b.length_rural,
        
        b.n_predictions_p_osm_id, b.n_predictions_p_osm_id_urban, b.n_predictions_p_osm_id_rural,
        b.latest_timestamps, b.latest_timestamps_urban, b.latest_timestamps_rural, 

        b.length_unpaved, b.length_paved,
        b.length_urban_unpaved, b.length_urban_paved, 
        b.length_rural_unpaved, b.length_rural_paved,

        {add_prefix(paved_tags, 'b.')},
        {add_prefix_and_suffix(id_tags, 'b.', 'pred_')},
        {add_prefix_and_suffix(length_tags, 'b.', 'pred_')},
        
        c.number_of_osm_ids, c.number_of_osm_ids_urban, c.number_of_osm_ids_rural,
        c.length as osm_length, c.length_urban as osm_length_urban, c.length_rural as osm_length_rural,

        {add_prefix_and_suffix(id_tags, 'c.', 'osm_')},
        {add_prefix_and_suffix(length_tags, 'c.', 'osm_')},

        d.total_length, d.total_urban_length, d.total_rural_length,
        d.n_points_p_osm_id, d.n_points_p_osm_id_urban, d.n_points_p_osm_id_rural,

        d.latest_timestamps as latest_timestamps_osm, d.latest_timestamps_urban as latest_timestamps_urban_osm,
        d.latest_timestamps_rural as latest_timestamps_rural_osm,

        {add_prefix(id_tags, 'd.')},
        {add_prefix(length_tags, 'd.')}
    FROM aggregated_data a
    LEFT JOIN pred_stats b ON a.world = b.world
    LEFT JOIN osm c ON a.world = c.world
    LEFT JOIN length_data d ON a.world = d.world
    )  
    TO '{world_output_filepath}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """

    temp_file = f'temp_{str(int(random.randint(0, int(1e6))))}.db'
    conn = duckdb.connect(temp_file)
    try:
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.create_function("create_tile", create_tile, ['double[]'], 'blob') 
        conn.create_function("calculate_length", calculate_length, ['double[]','double[]'], 'double') 
        conn.create_function("correct_z14_tiles_osm", correct_z14_tiles_osm, ['varchar[][]'], 'varchar[][]')  

        main_df = conn.execute(query_main_metadata).df()
        lines_df = conn.execute(query_lines).df()
        osm_df = conn.execute(query_main_osm).df()
        conn.execute(query_line_to_file)
        conn.execute(query_z8)
        conn.execute(query_z14)
        conn.execute(query_country)
        conn.execute(query_continent)
        conn.execute(query_world)

        for col, urban_area in zip(cols, urban_areas): 
            urban_output_filepath = f'{results_dir}/urban/{urban_area}_{tile}_stats.parquet'
            os.makedirs(os.path.dirname(urban_output_filepath), exist_ok=True)
            conn.execute(urban_query(col, sigma, score, threshold, osm_distance, pred_distance, urban_output_filepath))
    except Exception as e:
        logging.error(f"Error executing queries: {e}")
    finally:
        conn.close()
        if os.path.exists(temp_file):   
            os.remove(temp_file)

# =========================
# Entrypoint
# =========================
def run_parallel_processing(file_dirs, cols, urban_areas, osm_input_filedir, data_input_filepath,
                            sigma, score, threshold, results_dir, osm_distance, pred_distance, max_workers=64):
    """Run process_file over multiple directories using a process pool."""

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_file,
                filedir,
                sigma,
                score,
                threshold,
                results_dir,
                osm_distance,
                pred_distance,
                osm_input_filedir,
                data_input_filepath,
                cols,
                urban_areas
            )
            for filedir in file_dirs
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")

def main():
    """Run either single-path processing or chunked parallel processing."""
    global cols, urban_areas, osm_distance, pred_distance, sigma, score, threshold
    global max_workers, results_dir, zoom_level

    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    cfg = load_config()
    seed = int(cfg['params']['seed'])
    stats_runtime_cfg = load_statistics_runtime_config(cfg)

    cols = stats_runtime_cfg['urban_area_cols']
    urban_areas = stats_runtime_cfg['urban_area_layers']
    osm_distance = stats_runtime_cfg['osm_distance']
    pred_distance = stats_runtime_cfg['pred_distance']
    sigma = stats_runtime_cfg['sigma']
    score = stats_runtime_cfg['score']
    threshold = stats_runtime_cfg['threshold']
    max_workers = stats_runtime_cfg['max_workers']
    results_dir = os.path.abspath(stats_runtime_cfg['results_dir'])
    zoom_level = stats_runtime_cfg['zoom_level']
    
    osm_input_filedir = os.path.abspath(cfg["paths"]["osm_partitioned_dir"])
    filtered_dir = os.path.abspath(cfg['paths']['final_filtered_dir'])
    data_input_filepath = stats_runtime_cfg['data_input_pattern']
    file_dirs = [os.path.join(filtered_dir, d) for d in os.listdir(filtered_dir) if os.path.isdir(os.path.join(filtered_dir, d))]
    
    if len(sys.argv) > 2:
        index = int(sys.argv[1])
        instances = int(sys.argv[2])
        random.seed(seed)
        random.shuffle(file_dirs)
        file_dirs = file_dirs[index::instances]
        
        run_parallel_processing(file_dirs, cols, urban_areas, osm_input_filedir, data_input_filepath,
                                sigma, score, threshold, results_dir, osm_distance, pred_distance, max_workers)
        

if __name__ == "__main__":
    main()
