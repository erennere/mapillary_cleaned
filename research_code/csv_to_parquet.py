"""Convert CSV metadata files to Parquet format.

This script converts CSV metadata files to Parquet format using DuckDB,
applying spatial geometry transformations. It can process single tiles or
all tiles, automatically handling resume capability and resuming from resumable
reads using split CSV files when available.

Configuration paths are loaded from config.yaml, including support for both
splitted (incremental) and raw metadata sources.
"""

import os
import sys
import logging
import random
from datetime import datetime
import duckdb
from start import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def convert_csv_to_parquet(input_filepath, output_filepath):
    """Convert a CSV file to Parquet format using DuckDB with spatial geometries.
    
    Uses DuckDB to read CSV and convert geometry columns from WKT text to binary WKB format
    before writing to Parquet. Includes error handling and automatic cleanup of temporary files.
    
    Args:
        input_filepath: Full path to the input CSV file.
        output_filepath: Full path to the output Parquet file.
    
    Returns:
        None. Writes Parquet file to output_filepath.
    """
    logging.info(f"Converting CSV to Parquet: {input_filepath}")
    temp_file = f'csv_to_parquet_temp_{random.randint(0, int(1e12))}.db'
    conn = None
    try:
        conn = duckdb.connect(temp_file)
        logging.debug(f"DuckDB connection established, temp file: {temp_file}")
        
        conn.execute("INSTALL SPATIAL;")
        conn.execute("LOAD SPATIAL;")
        logging.debug("SPATIAL extension loaded")
        
        query = f"""
        COPY( 
            (SELECT * REPLACE (ST_AsWKB(ST_GeomFromText(geometry)) AS geometry)
            FROM read_csv('{input_filepath}',
                auto_detect=false,
                header = true,
                columns = {{
                'sequence': VARCHAR,
                'id': BIGINT,
                'url': VARCHAR,
                'long': DOUBLE,
                'lat': DOUBLE, 
                'geometry': VARCHAR, 
                'height': DOUBLE,
                'width': DOUBLE, 
                'altitude': DOUBLE,
                'make': VARCHAR,
                'model': VARCHAR,
                'creator': VARCHAR,
                'is_pano': BOOLEAN,
                'timestamp': BIGINT
                }})
            )
        )
        TO '{output_filepath}'
        (FORMAT PARQUET,COMPRESSION zstd);
        """
        conn.execute(query)
        logging.info(f"Successfully converted to Parquet: {output_filepath}")
    except Exception as e:
        logging.error(f'Error processing file {input_filepath}: {e}', exc_info=True)
    finally:
        if conn is not None:
            conn.close()
            logging.debug("DuckDB connection closed")
        if os.path.exists(temp_file):
            os.remove(temp_file)
            logging.debug(f"Removed temporary file: {temp_file}")

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    cfg = load_config()

    if len(sys.argv) < 2:
        logging.error("Usage: python csv_to_parquet.py <tile_name>")
        sys.exit(1)
    
    path = cfg['paths']['splitted_raw_metadata_dir']
    parquet_path = cfg['paths']['tile_partitioned_parquet_raw_metadata_dir']
    updated_after = datetime.fromisoformat(cfg['csv_split_params']['updated_after'])
    tile = sys.argv[1]
    logging.info(f"Processing CSV to Parquet conversion for tile: {tile}")

    files = None
    if os.path.exists(path):
        files = []
        logging.debug(f"Searching for splitted CSV files in: {path}")
        for file in os.listdir(path):
            if file.endswith(".csv") and tile in file and 'missing' not in file:
                filepath = os.path.join(path, file)
                mtime = datetime.fromtimestamp(os.path.getmtime(filepath))  # file modification time
                if mtime > updated_after:
                    files.append(file)
        if files:
            logging.debug(f"Found {len(files)} splitted CSV files for tile {tile}")
        else:
            logging.warning(f"No recently updated splitted CSV files found for tile {tile}, falling back to raw metadata")
            files = None
    
    if files is None:
        path = cfg['paths']['raw_metadata_dir']
        logging.debug(f"Searching in raw metadata directory: {path}")
        raw_file = f'metadata_unfiltered_{tile}.csv'
        if os.path.exists(os.path.join(path, raw_file)):
            files = [raw_file]
            logging.debug(f"Found raw metadata file: {raw_file}")
        else:
            logging.error(f"No metadata files found for tile {tile} in either splitted or raw directories")
            return

    parquet_path = os.path.join(parquet_path, f'tile={tile}')
    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path, exist_ok=True)
        logging.info(f"Created output directory: {parquet_path}")

    logging.info(f"Converting {len(files)} file(s) to Parquet format")
    for file in files:
        input_filepath = os.path.join(path, file)
        output_filepath = os.path.join(parquet_path, file.replace('.csv', '.parquet'))
        logging.debug(f"Converting: {file}")
        convert_csv_to_parquet(input_filepath, output_filepath)

if __name__ == '__main__':
    main()
      