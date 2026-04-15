"""Aggregate Mapillary and OSM statistics across multiple geographic levels.

This script builds reusable SQL fragments, composes full DuckDB queries, and executes
selected aggregation pipelines that write parquet outputs.
"""

# =========================
# Imports
# =========================
import os, logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import duckdb
import numpy as np
import pandas as pd
import mercantile
from shapely import to_wkb, box
from start import load_config
from statistics_geographic_layers import (
    build_metric_catalog,
    create_agg_highway_road_type_strings,
    agg_ratio_strings,
    create_ratio_strings,
    create_paved_ratio_strings,
    create_osm_general_strings,
    create_osm_urban_rural_strings,
    create_tile
)

# =========================
# Metric catalog preparation
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
zoom_level = _METRICS['zoom_level']

# =========================
# Helper Functions
# =========================
def create_connections(max_workers, threads, memory_limit_gb=2010):
    """Create in-memory DuckDB connections configured with thread counts."""
    conns = []
    temps = []
    for i in range(max_workers):
        temp_filename = f'temp_conn_{int(i)}.db'
        conn = duckdb.connect(temp_filename)
        conn.execute(f"""SET threads to {threads};""")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.create_function("create_tile", create_tile, ['double[]'], 'blob')
        conn.execute(f"""SET memory_limit = '{memory_limit_gb}GB'""")
        conn.execute("""SET max_temp_directory_size = '2010GB';""")
        conns.append(conn)
        temps.append(temp_filename)
    return conns, temps

# =========================
# Processing Functions
# =========================

def process_osm_chunk(chunk, osm_file_pattern, conn):
    """Process one chunk of countries for non-temporal OSM aggregations."""
    results = []
    if isinstance(chunk, pd.DataFrame):
        countries = chunk.iloc[:, 0].dropna().unique()
    elif isinstance(chunk, pd.Series):
        countries = chunk.dropna().unique()
    else:
        countries = pd.Series(chunk).dropna().unique()

    for country in countries:
        result = process_osm_by_country(country, osm_file_pattern, conn)
        if result is not None and not result.empty:
            results.append(result)

    return results
 
def process_osm_by_country(country, osm_file_pattern, conn):
    """Run non-temporal OSM aggregations for a single country."""
    osm_query = f"""
    SELECT osm_id, country, length, osm_tags_highway, 
        CASE 
        WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
        ELSE 0
    END AS urban, continent
    FROM read_parquet('{osm_file_pattern}')
    WHERE country = '{country}'
    """

    osm_country = f"""
    WITH
    osm_urban as (
        SELECT 
            continent, country, 
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END) as osm_length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END) as osm_length_rural,
            {create_osm_urban_rural_strings(road_classes)}  
        FROM osm
        GROUP BY continent, country
    ),

    osm_general as (
        SELECT
            continent, country, 
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            SUM(length) as osm_length,
            {create_osm_general_strings(road_classes)}  
        FROM(
            SELECT
                continent, country, osm_id,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway,
                ANY_VALUE(urban) as urban
            FROM osm
            GROUP BY continent, country, osm_id
            )
        GROUP BY continent, country
    )
    SELECT
        a.continent, a.country,
        b.number_of_osm_ids,
        {",".join([x for x in osm_total_cols if 'x' not in x or 'urban' in x or 'rural' in x])},
        b.osm_length
    FROM osm_urban a
    LEFT JOIN osm_general b
        ON a.country = b.country
    """
    osm = conn.execute(osm_query).df()
    return conn.execute(osm_country).df()

def process_urban_areas(process_urban, urban_areas_cols, input_filepaths, output_filepaths, urban_filepaths, conn):
    """Build urban-area level aggregated outputs when enabled by caller."""
    if process_urban:
        for col, input_filepath, output_filepath, urban_filepath in zip(urban_areas_cols, input_filepaths, output_filepaths, urban_filepaths): 
            query_urban = f"""
            COPY(
            WITH
            data as (
                SELECT
                    CAST({col} as int) AS {col},
                    list_distinct(flatten(list(countries))) as countries,
                    list_distinct(flatten(list(continents))) as continents,
                    list_distinct(flatten(list(sequences))) as sequences,

                    SUM(number_of_sequences) as number_of_sequences,
                    SUM(number_of_images) as number_of_images,
                    list_distinct(flatten(list(osm_ids))) as osm_ids,
                    len(list_distinct(flatten(list(osm_ids)))) as count_osm_ids,
                    list_distinct(flatten(list(z14_tiles))) as z14_tiles,
                    SUM(number_of_z14_tiles) as number_of_z14_tiles,
                    SUM(number_of_osm_ids) as number_of_osm_ids,

                    SUM(paved_segments) as paved_segments,
                    SUM(unpaved_segments) as unpaved_segments,
                    
                    SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
                    len(list_distinct(flatten(list(osm_ids))))/NULLIF(SUM(number_of_osm_ids),0) as osm_coverage,

                    {agg_ratio_strings([x for x in paved_tags if 'urban' not in x and 'rural' not in x], 1)},
                    {create_paved_ratio_strings(highways, [''])}

                    SUM(osm_length) as osm_length,
                    SUM(length)/NULLIF(SUM(osm_length), 0) as pred_length_ratio,
                    SUM(length)/1000 as pred_length,
                    SUM(total_length)/NULLIF(SUM(osm_length), 0) as mapillary_length_ratio,
                    SUM(total_length)/1000 as mapillary_length,

                    SUM(length_unpaved)/1000 as length_unpaved,
                    SUM(length_paved)/1000 as length_paved,

                    SUM(n_predictions_p_osm_id*len(osm_ids))/NULLIF(len(list_distinct(flatten(list(osm_ids)))),0) as n_predictions_p_osm_id, 
                    flatten(list(latest_timestamps)) as latest_timestamps,

                    {agg_ratio_strings([x for x in length_tags if 'urban' not in x and 'rural' not in x], 1000)}
                    {agg_ratio_strings([x for x in id_tags if 'urban' not in x and 'rural' not in x], 1)}
                    
                    {agg_ratio_strings([x for x in pred_length_tags if 'urban' not in x and 'rural' not in x], 1000)}
                    {agg_ratio_strings([x for x in pred_id_tags if 'urban' not in x and 'rural' not in x], 1)}

                    {agg_ratio_strings([x for x in osm_length_tags if 'urban' not in x and 'rural' not in x], 1000)}
                    {agg_ratio_strings([x for x in osm_id_tags if 'urban' not in x and 'rural' not in x], 1)}

                    {create_ratio_strings(
                        [x for x in length_tags if 'urban' not in x and 'rural' not in x],
                        [x for x in osm_length_tags if 'urban' not in x and 'rural' not in x],
                        1
                    )}

                    {create_ratio_strings(
                        [x for x in id_tags if 'urban' not in x and 'rural' not in x],
                        [x for x in osm_id_tags if 'urban' not in x and 'rural' not in x],
                        1
                    )}

                    {create_ratio_strings(
                        [x for x in pred_length_tags if 'urban' not in x and 'rural' not in x],
                        [x for x in osm_length_tags if 'urban' not in x and 'rural' not in x],
                        1
                    )}

                    {create_ratio_strings(
                        [x for x in pred_id_tags if 'urban' not in x and 'rural' not in x],
                        [x for x in osm_id_tags if 'urban' not in x and 'rural' not in x],
                        1
                    )[0:-3]}
                FROM(
                    SELECT
                        {col}, countries, continents, sequences, number_of_sequences,
                        number_of_images, osm_ids, z14_tiles, number_of_z14_tiles,
                        paved_segments, unpaved_segments, 
                        number_of_osm_ids, length, osm_length,
                        total_length, length_unpaved, length_paved, 
                        n_predictions_p_osm_id, latest_timestamps,
                        {", ".join([x for x in osm_length_tags if 'urban' not in x and 'rural' not in x])}
                        {", ".join([x for x in osm_id_tags if 'urban' not in x and 'rural' not in x])}
                        {", ".join([x for x in length_tags if 'urban' not in x and 'rural' not in x])}
                        {", ".join([x for x in id_tags if 'urban' not in x and 'rural' not in x])}
                        {", ".join([x for x in pred_length_tags if 'urban' not in x and 'rural' not in x])}
                        {", ".join([x for x in pred_id_tags if 'urban' not in x and 'rural' not in x])[0:-1]}
                    FROM read_parquet('{input_filepath}')
                    )
                GROUP BY {col}
            ),

            layer AS(
                SELECT *, CAST({col} as int) AS {col}
                FROM read_parquet('{urban_filepath}')
            )

            SELECT 
                a.*,
                b.*
            FROM layer a 
            LEFT JOIN data b ON CAST(a.{col} as INT) = CAST(b.{col} as INT)
            ORDER BY b.{col}, b.countries, b.continents
            )
            TO '{output_filepath}'
            (FORMAT 'parquet', COMPRESSION 'zstd');
            """            
            conn.execute(query_urban)

def build_queries(input_filepaths, output_filepaths, osm_filepath, country_filepath, continent_filepath):
    """Compose the five core aggregation SQL COPY queries."""
    input_filepaths_pattern_query_z14 = input_filepaths['z14']
    input_filepaths_pattern_query_z8 = input_filepaths['z8']
    input_filepaths_pattern_query_country = input_filepaths['country']
    input_filepaths_pattern_query_continent = input_filepaths['continent']
    input_filepaths_pattern_query_world = input_filepaths['world']

    output_filepaths_pattern_query_z14 = output_filepaths['z14']
    output_filepaths_pattern_query_z8 = output_filepaths['z8']
    output_filepaths_pattern_query_country = output_filepaths['country']
    output_filepaths_pattern_query_continent = output_filepaths['continent']
    output_filepaths_pattern_query_world = output_filepaths['world']

    query_z14 = f"""
    COPY(
        SELECT 
            z14_tiles,
            list_distinct(flatten(list(countries))) as countries,
            list_distinct(flatten(list(continents))) as continents,
            list_distinct(flatten(list(sequences))) as sequences,

            SUM(number_of_sequences) as number_of_sequences,
            SUM(number_of_images) as number_of_images,

            list_distinct(flatten(list(osm_ids))) as osm_ids,
            len(list_distinct(flatten(list(osm_ids)))) as count_osm_ids,
            list_distinct(flatten(list(osm_ids_urban))) as osm_ids_urban,
            len(list_distinct(flatten(list(osm_ids_urban)))) as count_osm_ids_urban,
            list_distinct(flatten(list(osm_ids_rural))) as osm_ids_rural,
            len(list_distinct(flatten(list(osm_ids_rural)))) as count_osm_ids_rural,

            cast(list_distinct(flatten(list(GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2))) as int[]) as GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2,
            cast(list_distinct(flatten(list(AFRICAPOLIS2020))) as int[]) as AFRICAPOLIS2020,

            SUM(number_of_osm_ids) as number_of_osm_ids,
            SUM(number_of_osm_ids_urban) as number_of_osm_ids_urban,
            SUM(number_of_osm_ids_rural) as number_of_osm_ids_rural,

            SUM(paved_segments) as paved_segments,
            SUM(unpaved_segments) as unpaved_segments,
            SUM(paved_segments_urban) as paved_segments_urban,
            SUM(unpaved_segments_urban) as unpaved_segments_urban,
            SUM(paved_segments_rural) as paved_segments_rural,
            SUM(unpaved_segments_rural) as unpaved_segments_rural,
            SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
            SUM(paved_segments_urban)/NULLIF(SUM(paved_segments_urban)+SUM(unpaved_segments_urban), 0) as urban_paved_ratio,
            SUM(paved_segments_rural)/NULLIF(SUM(paved_segments_rural)+SUM(unpaved_segments_rural), 0) as rural_paved_ratio, 
            
            {paved_strings},

            len(list_distinct(flatten(list(osm_ids))))/NULLIF(SUM(number_of_osm_ids),0) as osm_coverage,
            len(list_distinct(flatten(list(osm_ids_urban))))/NULLIF(SUM(number_of_osm_ids_urban),0) as urban_osm_coverage,
            len(list_distinct(flatten(list(osm_ids_rural))))/NULLIF(SUM(number_of_osm_ids_rural),0) as rural_osm_coverage,
            
            {agg_ratio_strings(osm_length_tags, 1, 'SUM')}

            SUM(length)/NULLIF(SUM(osm_length), 0) as pred_length_ratio,
            SUM(length_urban)/NULLIF(SUM(osm_length_urban), 0) as urban_pred_length_ratio,
            SUM(length_rural)/NULLIF(SUM(osm_length_rural), 0) as rural_pred_length_ratio,
            
            SUM(length)/1000 as pred_length,
            SUM(length_urban)/1000 as pred_length_urban,
            SUM(length_rural)/1000 as pred_length_rural,

            SUM(total_length)/1000 as mapillary_length,
            SUM(total_urban_length)/1000 as mapillary_length_urban,
            SUM(total_rural_length)/1000 as mapillary_length_rural,

            SUM(total_length)/NULLIF(SUM(osm_length), 0) as mapillary_length_ratio,
            SUM(total_urban_length)/NULLIF(SUM(osm_length_urban), 0) as urban_mapillary_length_ratio,
            SUM(total_rural_length)/NULLIF(SUM(osm_length_rural), 0) as rural_mapillary_length_ratio,
            
            {agg_ratio_strings(['length_unpaved', 'length_paved',
                                'length_urban_unpaved', 'length_urban_paved',
                                'length_rural_unpaved', 'length_rural_paved'], 1000, 'SUM')
                                }

            SUM(n_predictions_p_osm_id*len(osm_ids))/NULLIF(len(list_distinct(flatten(list(osm_ids)))),0) as n_predictions_p_osm_id, 
            SUM(n_predictions_p_osm_id_urban*len(osm_ids_urban))/NULLIF(len(list_distinct(flatten(list(osm_ids_urban)))) ,0) as n_predictions_p_osm_id_urban, 
            SUM(n_predictions_p_osm_id_rural*len(osm_ids_rural))/NULLIF(len(list_distinct(flatten(list(osm_ids_rural)))) ,0) as n_predictions_p_osm_id_rural,
            
            SUM(n_points_p_osm_id*len(osm_ids))/NULLIF(len(list_distinct(flatten(list(osm_ids)))),0) as n_points_p_osm_id, 
            SUM(n_points_p_osm_id_urban*len(osm_ids_urban))/NULLIF(len(list_distinct(flatten(list(osm_ids_urban)))) ,0) as n_points_p_osm_id_urban, 
            SUM(n_points_p_osm_id_rural*len(osm_ids_rural))/NULLIF(len(list_distinct(flatten(list(osm_ids_rural)))) ,0) as n_points_p_osm_id_rural,
            
            flatten(list(latest_timestamps)) as latest_timestamps,
            flatten(list(latest_timestamps_urban)) as latest_timestamps_urban,
            flatten(list(latest_timestamps_rural)) as latest_timestamps_rural,

            flatten(list(latest_timestamps_osm)) as latest_timestamps_osm,
            flatten(list(latest_timestamps_urban_osm)) as latest_timestamps_urban_osm,
            flatten(list(latest_timestamps_rural_osm)) as latest_timestamps_rural_osm,

            {agg_ratio_strings(rest, factor=1, agg_type='SUM')}
            {agg_ratio_strings(rest_length_road, factor=1000, agg_type='SUM')}

            {create_agg_highway_road_type_strings(highways, areas, road_types)}

            {create_paved_ratio_strings(highways, areas_2, aggregation_type='SUM')}

            {agg_ratio_strings(length_tags, 1000, 'SUM')}
            {create_ratio_strings(length_tags, osm_length_tags, 1)}

            {agg_ratio_strings(id_tags, 1, 'SUM')}
            {create_ratio_strings(id_tags, osm_id_tags, 1)}

            {agg_ratio_strings(pred_length_tags, 1000, 'SUM')}
            {create_ratio_strings(pred_length_tags, osm_length_tags, 1)}

            {agg_ratio_strings(pred_id_tags, 1, 'SUM')}
            {create_ratio_strings(pred_id_tags, osm_id_tags, 1)}

            {agg_ratio_strings(osm_length_tags, 1000, 'SUM')}
            {agg_ratio_strings(osm_id_tags, 1, 'SUM')}

            create_tile(cast(z14_tiles as double[])) as geometry

        FROM read_parquet('{input_filepaths_pattern_query_z14}')
        GROUP BY z14_tiles
        ORDER BY z14_tiles, countries, continents
    )
    TO '{output_filepaths_pattern_query_z14}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_z8 = f"""
    COPY(
        SELECT 
            {zoom_level}_tiles, 
            list_distinct(flatten(list(countries))) as countries,
            list_distinct(flatten(list(continents))) as continents, 

            cast(list_distinct(flatten(list(ID_HDC_G0))) as int[]) as ID_HDC_G0,
            cast(list_distinct(flatten(list(agglosID))) as int[]) as agglosID,
            
            ANY_VALUE(number_of_z14_tiles) as number_of_z14_tiles,

            {agg_ratio_strings(rest, factor=1, agg_type='SUM')}
            {agg_ratio_strings(rest_length_road, factor=1000, agg_type='SUM')}

            {create_agg_highway_road_type_strings(highways, areas, road_types)}

            SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
            SUM(paved_segments_urban)/NULLIF(SUM(paved_segments_urban)+SUM(unpaved_segments_urban), 0) as urban_paved_ratio,
            SUM(paved_segments_rural)/NULLIF(SUM(paved_segments_rural)+SUM(unpaved_segments_rural), 0) as rural_paved_ratio,

            {create_paved_ratio_strings(highways, areas_2, aggregation_type='SUM')}

            SUM(osm_ids)/NULLIF(SUM(number_of_osm_ids),0) as osm_coverage,
            SUM(osm_ids_urban)/NULLIF(SUM(number_of_osm_ids_urban),0) as urban_osm_coverage,
            SUM(osm_ids_rural)/NULLIF(SUM(number_of_osm_ids_rural),0) as rural_osm_coverage,

            SUM(total_length)/1000 as mapillary_length,
            SUM(total_urban_length)/1000 as mapillary_length_urban,
            SUM(total_rural_length)/1000 as mapillary_length_rural,

            SUM(length)/1000 as pred_length,
            SUM(length_urban)/1000 as pred_length_urban,
            SUM(length_rural)/1000 as pred_length_rural,

            SUM(total_length)/NULLIF(SUM(osm_length), 0) as mapillary_length_ratio,
            SUM(total_urban_length)/NULLIF(SUM(osm_length_urban), 0) as urban_mapillary_length_ratio,
            SUM(total_rural_length)/NULLIF(SUM(osm_length_rural), 0) as rural_mapillary_length_ratio,

            SUM(length)/NULLIF(SUM(osm_length), 0) as pred_length_ratio,
            SUM(length_urban)/NULLIF(SUM(osm_length_urban), 0) as urban_pred_length_ratio,
            SUM(length_rural)/NULLIF(SUM(osm_length_rural), 0) as rural_pred_length_ratio,

            {agg_ratio_strings(length_tags, 1000, 'SUM')}
            {create_ratio_strings(length_tags, osm_length_tags, 1)}

            {agg_ratio_strings(id_tags, 1, 'SUM')}
            {create_ratio_strings(id_tags, osm_id_tags, 1)}

            {agg_ratio_strings(pred_length_tags, 1000, 'SUM')}
            {create_ratio_strings(pred_length_tags, osm_length_tags, 1)}

            {agg_ratio_strings(pred_id_tags, 1, 'SUM')}
            {create_ratio_strings(pred_id_tags, osm_id_tags, 1)}

            {agg_ratio_strings(osm_length_tags, 1000, 'SUM')}
            {agg_ratio_strings(osm_id_tags, 1, 'SUM')}

            create_tile(cast({zoom_level}_tiles as double[])) as geometry
        FROM read_parquet('{input_filepaths_pattern_query_z8}')
        GROUP BY {zoom_level}_tiles
        ORDER BY {zoom_level}_tiles, countries, continents
    )
    TO '{output_filepaths_pattern_query_z8}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_country = f"""
    COPY(
    WITH
        osm as(
        SELECT 
            DISTINCT cast(osm_id as varchar) as osm_id, country, length, osm_tags_highway, 
            CASE 
                WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
                ELSE 0
            END AS urban
        FROM read_parquet('{osm_filepath}')
        ),

    osm_urban as(
        SELECT 
            country, ANY_VALUE(urban) as urban,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END)/1000 as osm_length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END)/1000 as osm_length_rural,     
            {create_osm_urban_rural_strings(road_classes)}  
        FROM osm
        GROUP BY country
    ),

    osm_general as(
        SELECT
            country,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            SUM(length)/1000 as osm_length,
            {create_osm_general_strings(road_classes)}  
        FROM(
            SELECT
                country, osm_id,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway,
                ANY_VALUE(urban) as urban
            FROM osm
            GROUP BY country, osm_id
            )
        GROUP BY country
        ),

    osm_total as(
        SELECT
            a.country,
            a.osm_length_urban,
            a.osm_length_rural,
            b.osm_length,
            {osm_total_cols_string}
        FROM osm_urban a
        LEFT JOIN osm_general b
        ON a.country = b.country
    ),

    data AS(
            SELECT 
                a.country, 
                list_distinct(flatten(list(continents))) as continents, 

                {agg_ratio_strings(rest_country_onwards, factor=1, agg_type='SUM')}
                {agg_ratio_strings(rest_length_road, factor=1000, agg_type='SUM')}

                cast(list_distinct(flatten(list(ID_HDC_G0))) as int[]) as ID_HDC_G0,
                cast(list_distinct(flatten(list(agglosID))) as int[]) as agglosID,
                ANY_VALUE(number_of_z14_tiles) as number_of_z14_tiles,

                SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
                SUM(paved_segments_urban)/NULLIF(SUM(paved_segments_urban)+SUM(unpaved_segments_urban), 0) as urban_paved_ratio,
                SUM(paved_segments_rural)/NULLIF(SUM(paved_segments_rural)+SUM(unpaved_segments_rural), 0) as rural_paved_ratio,    

                {create_agg_highway_road_type_strings(highways, areas, road_types, aggregation_type='SUM')}
                {create_paved_ratio_strings(highways, areas_2, aggregation_type='SUM')}

                SUM(length)/1000 as pred_length,
                SUM(length_urban)/1000 as pred_length_urban,
                SUM(length_rural)/1000 as pred_length_rural,

                SUM(total_length)/1000 as mapillary_length,
                SUM(total_urban_length)/1000 as mapillary_length_urban,
                SUM(total_rural_length)/1000 as mapillary_length_rural,

                (SUM(length)/NULLIF(MAX(b.osm_length), 0)) as pred_length_ratio,
                (SUM(length_urban)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_pred_length_ratio,
                (SUM(length_rural)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_pred_length_ratio,

                (SUM(total_length)/NULLIF(MAX(b.osm_length), 0)) as mapillary_length_ratio,
                (SUM(total_urban_length)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_mapillary_length_ratio,
                (SUM(total_rural_length)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_mapillary_length_ratio,

                {create_ratio_strings(length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
                {create_ratio_strings(pred_length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

                SUM(osm_ids)/NULLIF(MAX(b.number_of_osm_ids), 0) as osm_coverage,
                SUM(osm_ids_urban)/NULLIF(MAX(b.number_of_osm_ids_urban), 0) as urban_osm_coverage,
                SUM(osm_ids_rural)/NULLIF(MAX(b.number_of_osm_ids_rural), 0) as rural_osm_coverage,
                
                {create_ratio_strings(id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
                {create_ratio_strings(pred_id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

                {agg_ratio_strings(length_tags, 1000, 'SUM')}
                {agg_ratio_strings(id_tags, 1, 'SUM')}

                {agg_ratio_strings(pred_length_tags, 1000, 'SUM')}
                {agg_ratio_strings(pred_id_tags, 1, 'SUM')}

                ANY_VALUE(b.country) as country_2,

                {agg_ratio_strings(osm_id_tags, 1, 'MAX').replace('MAX(', 'MAX(b.')}
                {agg_ratio_strings(n_osms, 1, 'MAX').replace('MAX(', 'MAX(b.')}
                {agg_ratio_strings(osm_length_tags, 1000, 'MAX').replace('MAX(', 'MAX(b.')[0:-3]}
        FROM(
                SELECT
                    country, 
                    continents,
                    number_of_sequences, number_of_sequences_urban, number_of_sequences_rural,
                    number_of_images, number_of_images_urban, number_of_images_rural,
                    ID_HDC_G0, agglosID, number_of_z14_tiles, count_ID_HDC_G0, count_agglosID,
                    paved_segments, unpaved_segments, paved_segments_urban,
                    unpaved_segments_urban, paved_segments_rural, unpaved_segments_rural,  
                    length_unpaved, length_paved, length_urban_unpaved,
                    length_urban_paved, length_rural_unpaved, length_rural_paved,
                    total_length, total_urban_length, total_rural_length,
                    length, length_urban, length_rural,
                    {paved_strings},     
                    osm_ids, osm_ids_urban, osm_ids_rural
            FROM read_parquet('{input_filepaths_pattern_query_country}')
            ) a 
            LEFT JOIN osm_total b ON a.country = b.country
            GROUP BY a.country
    ),
    layer AS(
        SELECT *
        FROM read_parquet('{country_filepath}')
    )

    SELECT
        a.*, ST_AsWKB(ST_GeomFromText(a.geom)) as geometry,
        b.*
    FROM layer a
    LEFT JOIN data b ON a.country = b.country
    ORDER BY b.country, b.continents
    )
    TO '{output_filepaths_pattern_query_country}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_continent = f"""
    COPY(
    WITH
    osm as(
        SELECT DISTINCT cast(osm_id as varchar) as osm_id, continent, length, osm_tags_highway, 
            CASE 
                WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
                ELSE 0
            END AS urban
        FROM read_parquet('{osm_filepath}')
    ),

    osm_urban as(
        SELECT 
            continent, ANY_VALUE(urban) as urban,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END)/1000 as osm_length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END)/1000 as osm_length_rural,
            {create_osm_urban_rural_strings(road_classes)}     
        FROM osm
        GROUP BY continent
    ),

    osm_general as(
        SELECT
            continent,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            SUM(length)/1000 as osm_length,
            {create_osm_general_strings(road_classes)}  
        FROM(
            SELECT
                continent, osm_id,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway,
                ANY_VALUE(urban) as urban
            FROM osm
            GROUP BY continent, osm_id
            )
        GROUP BY continent
    ),

    osm_total as(
        SELECT
            a.continent,
            a.osm_length_urban,
            a.osm_length_rural,
            b.osm_length,
            {osm_total_cols_string}
        FROM osm_urban a
        LEFT JOIN osm_general b
        ON a.continent = b.continent
    ),

    data AS(
        SELECT 
            a.continent, 
            list_distinct(flatten(list(countries))) as countries, 

            {agg_ratio_strings(rest_country_onwards, factor=1, agg_type='SUM')}
            {agg_ratio_strings(rest_length_road, factor=1000, agg_type='SUM')}

            cast(list_distinct(flatten(list(ID_HDC_G0))) as int[]) as ID_HDC_G0,
            cast(list_distinct(flatten(list(agglosID))) as int[]) as agglosID,

            SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
            SUM(paved_segments_urban)/NULLIF(SUM(paved_segments_urban)+SUM(unpaved_segments_urban), 0) as urban_paved_ratio,
            SUM(paved_segments_rural)/NULLIF(SUM(paved_segments_rural)+SUM(unpaved_segments_rural), 0) as rural_paved_ratio,    

            {create_agg_highway_road_type_strings(highways, areas, road_types, aggregation_type='SUM')}
            {create_paved_ratio_strings(highways, areas_2, aggregation_type='SUM')}

            SUM(length)/1000 as pred_length,
            SUM(length_urban)/1000 as pred_length_urban,
            SUM(length_rural)/1000 as pred_length_rural,

            SUM(total_length)/1000 as mapillary_length,
            SUM(total_urban_length)/1000 as mapillary_length_urban,
            SUM(total_rural_length)/1000 as mapillary_length_rural,

            (SUM(length)/NULLIF(MAX(b.osm_length), 0)) as pred_length_ratio,
            (SUM(length_urban)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_pred_length_ratio,
            (SUM(length_rural)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_pred_length_ratio,

            (SUM(total_length)/NULLIF(MAX(b.osm_length), 0)) as mapillary_length_ratio,
            (SUM(total_urban_length)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_mapillary_length_ratio,
            (SUM(total_rural_length)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_mapillary_length_ratio,

            {create_ratio_strings(length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
            {create_ratio_strings(pred_length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

            SUM(osm_ids)/NULLIF(MAX(b.number_of_osm_ids), 0) as osm_coverage,
            SUM(osm_ids_urban)/NULLIF(MAX(b.number_of_osm_ids_urban), 0) as urban_osm_coverage,
            SUM(osm_ids_rural)/NULLIF(MAX(b.number_of_osm_ids_rural), 0) as rural_osm_coverage,
            
            {create_ratio_strings(id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
            {create_ratio_strings(pred_id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

            {agg_ratio_strings(length_tags, 1000, 'SUM')}
            {agg_ratio_strings(id_tags, 1, 'SUM')}

            {agg_ratio_strings(pred_length_tags, 1000, 'SUM')}
            {agg_ratio_strings(pred_id_tags, 1, 'SUM')}

            ANY_VALUE(b.continent) as continent_2,

            {agg_ratio_strings(osm_id_tags, 1, 'MAX').replace('MAX(', 'MAX(b.')}
            {agg_ratio_strings(n_osms, 1, 'MAX').replace('MAX(', 'MAX(b.')}
            {agg_ratio_strings(osm_length_tags, 1000, 'MAX').replace('MAX(', 'MAX(b.')[0:-3]}
                
        FROM(
            SELECT            
                continent, countries,
                number_of_sequences, number_of_sequences_urban, number_of_sequences_rural,
                number_of_images, number_of_images_urban, number_of_images_rural,
                ID_HDC_G0, agglosID, count_ID_HDC_G0, count_agglosID,
                paved_segments, unpaved_segments, paved_segments_urban,
                unpaved_segments_urban, paved_segments_rural, unpaved_segments_rural,  
                length_unpaved, length_paved, length_urban_unpaved,
                length_urban_paved, length_rural_unpaved, length_rural_paved,
                total_length, total_urban_length, total_rural_length,
                length, length_urban, length_rural,
                {paved_strings},     
                osm_ids, osm_ids_urban, osm_ids_rural
            FROM read_parquet('{input_filepaths_pattern_query_continent}')
            ) a 
        LEFT JOIN osm_total b ON a.continent = b.continent
        GROUP BY a.continent
    ),

    layer AS(
        SELECT *
        FROM read_parquet('{continent_filepath}')
    )

    SELECT 
        a.*,
        b.*
    FROM layer a
    LEFT JOIN data b ON a.continent = b.continent
    ORDER BY b.continent
    )
    TO '{output_filepaths_pattern_query_continent}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    query_world = f"""
    COPY(
    WITH
    osm as(
        SELECT DISTINCT cast(osm_id as varchar) as osm_id, length, osm_tags_highway, 1 as world,
            CASE 
                WHEN ID_HDC_G0 IS NOT NULL OR  agglosID IS NOT NULL THEN 1
                ELSE 0
            END AS urban
        FROM read_parquet('{osm_filepath}')
    ),

    osm_urban as(
        SELECT 
            world, ANY_VALUE(urban) as urban,
            COUNT(DISTINCT CASE WHEN urban = 1 THEN osm_id ELSE NULL END) AS number_of_osm_ids_urban,
            COUNT(DISTINCT CASE WHEN urban = 0 THEN osm_id ELSE NULL END) AS number_of_osm_ids_rural,
            SUM(CASE WHEN urban = 1 THEN length ELSE 0 END)/1000 as osm_length_urban,
            SUM(CASE WHEN urban = 0 THEN length ELSE 0 END)/1000 as osm_length_rural,
            {create_osm_urban_rural_strings(road_classes)} 
        FROM osm
        GROUP BY world
    ),

    osm_general as(
        SELECT
            world,
            COUNT(DISTINCT osm_id) AS number_of_osm_ids,
            SUM(length)/1000 as osm_length,
            {create_osm_general_strings(road_classes)} 
        FROM(
            SELECT
                world, osm_id,
                MAX(length) as length,
                ANY_VALUE(osm_tags_highway) as osm_tags_highway,
                ANY_VALUE(urban) as urban
            FROM osm
            GROUP BY world, osm_id
            )
        GROUP BY world
        ),

    osm_total as(
        SELECT
            a.world,
            a.osm_length_urban,
            a.osm_length_rural,
            b.osm_length,
            {osm_total_cols_string}
        FROM osm_urban a
        LEFT JOIN osm_general b
        ON a.world = b.world
    ),

    data AS(
        SELECT 
            list_distinct(flatten(list(countries))) as countries, 

            {agg_ratio_strings(rest_country_onwards, factor=1, agg_type='SUM')}
            {agg_ratio_strings(rest_length_road, factor=1000, agg_type='SUM')}

            cast(list_distinct(flatten(list(ID_HDC_G0))) as int[]) as ID_HDC_G0,
            cast(list_distinct(flatten(list(agglosID))) as int[]) as agglosID,

            SUM(paved_segments)/NULLIF(SUM(paved_segments)+SUM(unpaved_segments), 0) as paved_ratio,
            SUM(paved_segments_urban)/NULLIF(SUM(paved_segments_urban)+SUM(unpaved_segments_urban), 0) as urban_paved_ratio,
            SUM(paved_segments_rural)/NULLIF(SUM(paved_segments_rural)+SUM(unpaved_segments_rural), 0) as rural_paved_ratio,    

            {create_agg_highway_road_type_strings(highways, areas, road_types, aggregation_type='SUM')}
            {create_paved_ratio_strings(highways, areas_2, aggregation_type='SUM')}

            SUM(length)/1000 as pred_length,
            SUM(length_urban)/1000 as pred_length_urban,
            SUM(length_rural)/1000 as pred_length_rural,

            SUM(total_length)/1000 as mapillary_length,
            SUM(total_urban_length)/1000 as mapillary_length_urban,
            SUM(total_rural_length)/1000 as mapillary_length_rural,

            (SUM(length)/NULLIF(MAX(b.osm_length), 0)) as pred_length_ratio,
            (SUM(length_urban)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_pred_length_ratio,
            (SUM(length_rural)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_pred_length_ratio,

            (SUM(total_length)/NULLIF(MAX(b.osm_length), 0)) as mapillary_length_ratio,
            (SUM(total_urban_length)/NULLIF(MAX(b.osm_length_urban), 0)) as urban_mapillary_length_ratio,
            (SUM(total_rural_length)/NULLIF(MAX(b.osm_length_rural), 0)) as rural_mapillary_length_ratio,

            {create_ratio_strings(length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
            {create_ratio_strings(pred_length_tags, osm_length_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

            SUM(osm_ids)/NULLIF(MAX(b.number_of_osm_ids), 0) as osm_coverage,
            SUM(osm_ids_urban)/NULLIF(MAX(b.number_of_osm_ids_urban), 0) as urban_osm_coverage,
            SUM(osm_ids_rural)/NULLIF(MAX(b.number_of_osm_ids_rural), 0) as rural_osm_coverage,
            
            {create_ratio_strings(id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}
            {create_ratio_strings(pred_id_tags, osm_id_tags, 1).replace('NULLIF(SUM(', 'NULLIF(MAX(b.')}

            {agg_ratio_strings(length_tags, 1000, 'SUM')}
            {agg_ratio_strings(id_tags, 1, 'SUM')}

            {agg_ratio_strings(pred_length_tags, 1000, 'SUM')}
            {agg_ratio_strings(pred_id_tags, 1, 'SUM')}

            {agg_ratio_strings(osm_id_tags, 1, 'MAX').replace('MAX(', 'MAX(b.')}
            {agg_ratio_strings(n_osms, 1, 'MAX').replace('MAX(', 'MAX(b.')}
            {agg_ratio_strings(osm_length_tags, 1000, 'MAX').replace('MAX(', 'MAX(b.')[0:-3]}

        FROM(
            SELECT            
                countries,
                number_of_sequences, number_of_sequences_urban, number_of_sequences_rural,
                number_of_images, number_of_images_urban, number_of_images_rural,
                ID_HDC_G0, agglosID, number_of_z14_tiles, count_ID_HDC_G0, count_agglosID,
                paved_segments, unpaved_segments, paved_segments_urban,
                unpaved_segments_urban, paved_segments_rural, unpaved_segments_rural,  
                length_unpaved, length_paved, length_urban_unpaved,
                length_urban_paved, length_rural_unpaved, length_rural_paved,
                total_length, total_urban_length, total_rural_length,
                length, length_urban, length_rural,
                {paved_strings},     
                osm_ids, osm_ids_urban, osm_ids_rural
            FROM read_parquet('{input_filepaths_pattern_query_world}')
            ) a 
        LEFT JOIN osm_total b ON 1 = b.world
        GROUP BY b.world
    )

    SELECT *
    FROM data
    )
    TO '{output_filepaths_pattern_query_world}'
    (FORMAT 'parquet', COMPRESSION 'zstd');
    """
    return query_z14, query_z8, query_country, query_continent, query_world

def orchestrate_osm_processing(countries_df, output_filepath, osm_file_pattern, index=0, n_instances=10, max_workers=64, sub_threads=1, seed=42):
    """Execute the optional non-temporal country processing branch."""
    countries = countries_df.country.unique()
    rng = np.random.default_rng(seed)
    rng.shuffle(countries)
    countries = np.array_split(countries, n_instances)[index]

    osm_results = []
    futures = []

    conns, temps = create_connections(max_workers, sub_threads)
    countries_split = np.array_split(countries, max_workers)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for i, chunk in enumerate(countries_split):
            future = executor.submit(process_osm_chunk, chunk, osm_file_pattern, conns[i])
            futures.append(future)

        for future in as_completed(futures):
            result = future.result()
            if result is not None and len(result) > 0:
                osm_results.extend(result)

    for conn in conns:
        conn.close()
    for temp in temps:
        if os.path.exists(temp):
            os.remove(temp)

    if not osm_results:
        logging.warning("No OSM results were produced; skipping CSV export.")
        return

    osm_results = pd.concat(osm_results, ignore_index=True, axis=0)
    try:
        if not os.path.exists(output_filepath):
            osm_results.to_csv(output_filepath, index=False)
        else:
            osm_results.to_csv(output_filepath, index=False, mode='a', header=False)
    except Exception as e:
        logging.warning(f"Error saving CSV: {e}")

def build_runtime_config(cfg):
    """Build runtime configuration for this script from config.yaml."""
    stats_cfg = cfg.get('statistics', {})
    agg_cfg = stats_cfg.get('aggregation', {})

    config = {
        'osm_file_pattern': cfg['paths'].get('osm_saving_dir', '*.parquet'),
        'results_dir': os.path.join(os.path.abspath(cfg['paths'].get('stats_dir')), 'geographic_layers'),
        'results_dir_compiled': os.path.join(os.path.abspath(cfg['paths'].get('stats_dir')), 'summary'),
        'urban_areas_dir': cfg["paths"].get('processed_dir'),
        'country_layer': os.path.abspath(os.path.join(cfg['paths']['processed_dir'], f"intersected_{cfg['filenames']['country_filename']}")),
        'continent_layer': os.path.abspath(cfg['filenames']['continents_filename']),
        'urban_layers': 
            [
                f"country_intersected_intersected_{cfg['filenames']['ghsl_filename'].replace('.gpkg', '.parquet')}",
                f"country_intersected_intersected_{cfg['filenames']['africapolis_filename'].replace('.shp', '.parquet')}",
            ],
        'urban_layer_cols': agg_cfg.get('urban_layer_cols', ['ID_HDC_G0', 'agglosID']),
        'urban_areas': cfg['geographic_layers']['urban_area_layers'],
        'memory_limit_gb': int(agg_cfg.get('memory_limit_gb', 2010)),
        'number_of_cpus': int(agg_cfg.get('number_of_cpus', 64)),
        'max_workers': int(agg_cfg.get('max_workers', 16)),
    }
    config['sub_threads'] = max(1, config['number_of_cpus'] // config['max_workers'])
    return config

def main():
    """Run the full Mapillary/OSM aggregation pipeline and export outputs."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    cfg = load_config()
    config = build_runtime_config(cfg)
    os.makedirs(config['results_dir_compiled'], exist_ok=True)

    # =========================
    # SQL query definitions
    # =========================
    pattern_query_z14 = 'z14_tiles_with_stats_*.parquet'
    pattern_query_z8 = f'{zoom_level}_tiles_with_stats_*.parquet'
    pattern_query_country = 'countries_with_stats_*.parquet'
    pattern_query_continent = 'continents_with_stats_*.parquet'
    pattern_query_world = 'world_with_stats_*.parquet'
    pattern_query_GHSL = f'{config["urban_areas"][0]}_*.parquet'
    pattern_query_Africapolis = f'{config["urban_areas"][1]}_*.parquet'

    input_filename_pattern_query_z14 = f"{config['results_dir']}/z14_tiles/{pattern_query_z14}"
    input_filename_pattern_query_z8 = f"{config['results_dir']}/{zoom_level}_tiles/{pattern_query_z8}"
    input_filename_pattern_query_country = f"{config['results_dir']}/countries/{pattern_query_country}"
    input_filename_pattern_query_continent = f"{config['results_dir']}/continents/{pattern_query_continent}"
    input_filename_pattern_query_world = f"{config['results_dir']}/world/{pattern_query_world}"
    input_filename_pattern_query_GHSL = f"{config['results_dir']}/urban/{pattern_query_GHSL}"
    input_filename_pattern_query_Africapolis = f"{config['results_dir']}/urban/{pattern_query_Africapolis}"

    output_filename_query_z14 = f"{config['results_dir_compiled']}/{pattern_query_z14.replace('*', 'all')}"
    output_filename_query_z8 = f"{config['results_dir_compiled']}/{pattern_query_z8.replace('*', 'all')}"
    output_filename_query_country = f"{config['results_dir_compiled']}/{pattern_query_country.replace('*', 'all')}"
    output_filename_query_continent = f"{config['results_dir_compiled']}/{pattern_query_continent.replace('*', 'all')}"
    output_filename_query_world = f"{config['results_dir_compiled']}/{pattern_query_world.replace('*', 'all')}"
    output_filename_query_GHSL = f"{config['results_dir_compiled']}/{pattern_query_GHSL.replace('*', 'all')}"
    output_filename_query_Africapolis = f"{config['results_dir_compiled']}/{pattern_query_Africapolis.replace('*', 'all')}"

    input_filepaths = {
        'z14': input_filename_pattern_query_z14,
        'z8': input_filename_pattern_query_z8,
        'country': input_filename_pattern_query_country,
        'continent': input_filename_pattern_query_continent,
        'world': input_filename_pattern_query_world,
        'GHSL': input_filename_pattern_query_GHSL,
        'Africapolis': input_filename_pattern_query_Africapolis
    }
    output_filepaths = {
        'z14': output_filename_query_z14,
        'z8': output_filename_query_z8,
        'country': output_filename_query_country,
        'continent': output_filename_query_continent,
        'world': output_filename_query_world,
        'GHSL': output_filename_query_GHSL,
        'Africapolis': output_filename_query_Africapolis
    }

    osm_filepath = config['osm_file_pattern']
    country_filepath = config['country_layer']
    continent_filepath = config['continent_layer']
    urban_filepaths = [os.path.abspath(f"{config['paths']['processed_dir']}/{name}") for name in config['urban_layers']]

    # Optional branch for country-wise pre-aggregation export.
    process_non_temporal_osm = False
    if process_non_temporal_osm:
        countries_df = duckdb.execute(
            f"SELECT DISTINCT country FROM read_parquet('{country_filepath}')"
        ).df()
        output_filepath = f"{config['results_dir_compiled']}/osm_country_rollup.csv"
        orchestrate_osm_processing(
            countries_df=countries_df,
            output_filepath=output_filepath,
            osm_file_pattern=osm_filepath,
            max_workers=config['max_workers'],
            sub_threads=config['sub_threads'],
        )

    queries = build_queries(input_filepaths, output_filepaths, osm_filepath, country_filepath, continent_filepath)
    
    conns, temps = create_connections(1, config['number_of_cpus'], config['memory_limit_gb'])
    conn = conns[0]
    temp = temps[0]
    for query in queries:
        logging.info(f"Executing query: {query}...")  # Log the beginning of the query for reference
        try:
            conn.execute(query)
            logging.info("Query executed successfully.")
        except Exception as e:
            logging.error(f"Error executing query: {e}")

    process_urban_areas(
        True,
        config['urban_layer_cols'],
        [input_filepaths['Africapolis'], input_filepaths['GHSL']],
        [output_filepaths['Africapolis'], output_filepaths['GHSL']],
        urban_filepaths,
        conn,
    )

    if conn:
        conn.close()
    if os.path.exists(temp):
        os.remove(temp)
if __name__ == "__main__":
    main()




