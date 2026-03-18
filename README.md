1. **create_tiles.py** - Creates map tiles at a specified zoom level covering a geographic area using the Mercantile library. Configuration is loaded from `config.yaml`. The script includes:
   - Comprehensive logging at INFO and DEBUG levels to track tile generation progress
   - Full docstring documentation for all functions
   - Support for restricting tiles to a polygon (e.g., country boundary). If no polygon is provided, defaults to world coverage.
   - Output saved to `./data/processed/tiles` as `tiles_z{zoom_level}.gpkg`
   
2. **get_linestrings_from_tiles.py** - Downloads vector tiles from the Mapillary API and extracts linestrings (sequences) as GeoJSON features. Configuration is loaded from `config.yaml`. The script includes:
   - Comprehensive logging at INFO, DEBUG, and WARNING levels to track download progress and failures
   - Full docstring documentation for all functions
   - Retry logic for failed tile downloads (configurable via config.yaml)
   - Saves successful tiles to `./data/processed/tiles/completed` as `completed_tiles_z{zoom_level}.gpkg`
   - Saves failed tiles to `./data/processed/tiles/failed` as `failed_tiles_z{zoom_level}.gpkg`
   - Features a progress bar (tqdm) for batch processing visibility

   ## To 1. and 2.

   There is a shell file **get_sequences_hpc.sh** that automates both steps. The script:
   - Automatically runs both `create_tiles.py` and `get_linestrings_from_tiles.py` in sequence
   - Includes error handling to stop on failure
   - Provides informative console output about progress
   - Can be run locally as `bash get_sequences_hpc.sh` or on a HPC using `sbatch get_sequences_hpc.sh`
   
   **Configuration**: All parameters are loaded from `config.yaml`. Ensure you have set:
   - `params.zoom_level` - Zoom level for tile generation (default: 8, example in script: 4)
   - `params.mly_key` - Your Mapillary API access token (required for downloads)
   - `paths.tiles_save_dir`, `paths.completed_tiles_dir`, `paths.failed_tiles_dir` - Output directories
   
   **For HPC usage**: Modify the SBATCH directives in `get_sequences_hpc.sh` if your cluster has different node settings, and update `PYTHON_BIN` to point to your Python environment if not using the default `python` command.

3. **get_metadata.py** - Downloads metadata for image sequences from completed tiles. Configuration is loaded from `config.yaml`. The script includes:
   - Comprehensive logging at INFO, DEBUG, and WARNING levels to track download progress
   - Full docstring documentation for all functions
   - Resumable downloads: if interrupted, previously downloaded sequences are automatically skipped
   - Retry logic for failed sequences with configurable attempts (configurable via config.yaml)
   - Saves metadata to CSV files in `./data/processed/mapillary_metadata` as `metadata_unfiltered_{tile}.csv`
   - Tracks missing sequences separately for retry attempts
   - Optional `instance_id` parameter (1-10) to process deterministic tile chunks in parallel

   ## To 3.

   There is a shell file **get_metadata_hpc.sh** that automates parallel metadata downloads. The script:
   - Automatically launches **10 parallel instances** of `get_metadata.py`, each with instance_id 1-10
   - Tiles are divided into 10 deterministic chunks using a fixed seed, ensuring reproducibility across runs
   - Each instance processes only its assigned chunk of tiles
   - Within each chunk, tiles are randomized independently
   - Monitors all parallel processes and reports completion status
   - Can be run locally as `bash get_metadata_hpc.sh` or on a HPC using `sbatch get_metadata_hpc.sh`
   
   **Configuration**: All parameters are loaded from `config.yaml`. Ensure you have set:
   - `params.zoom_level` - Zoom level for filtering (must match value used in tile creation)
   - `params.mly_key` - Your Mapillary API access token (required for downloads)
   - `metadata_params.batch_size`, `windows`, `max_workers` - Concurrency settings (max_workers controls parallelism within each instance)
   - `metadata_params.missing_attempts` - Number of retry attempts for failed downloads (default: 10)
   - `paths.raw_metadata_dir` - Output directory for metadata files
   
   **Parallel Processing**: 
   - The bash script launches 10 instances simultaneously (instance_id 1-10)
   - Each instance processes one deterministic chunk of tiles reproducibly
   - Tile chunks are deterministic (same seed) so splits are consistent across runs
   - Tiles within each chunk are randomized independently for load balancing
   - Each instance writes to separate output files (one per tile), so there's no file conflict risk
   - If you have fewer than 10 tiles, some instances will complete quickly with no work
   
   **For HPC usage**: Modify the SBATCH directives in `get_metadata_hpc.sh` if your cluster has different node settings, and update `PYTHON_BIN` to point to your Python environment if not using the default `python` command.

4. **split_csvs_and_to_parquet_hpc.sh** - Automates both splitting large CSV metadata files into chunks AND converting them to Parquet format. Configuration is loaded from `config.yaml`. The script includes:
   - **Split Phase**: Splits large `metadata_unfiltered_*.csv` files into smaller chunks (configurable size, default: 500000 rows) for distributed processing
     - Finds all metadata CSV files in raw metadata directory
     - Saves split files to `./data/processed/mapillary_metadata/splitted_raw_metadata/` with sequential numbering
     - Features resumable operation: tracks existing split files and continues from the last completed chunk
     - Preserves CSV headers in each split file for compatibility
   - **Convert Phase**: Converts each tile's metadata from CSV to Parquet format using `csv_to_parquet.py`
     - Searches for split CSV files first (output from split phase)
     - Falls back to raw metadata CSV if splits don't exist or aren't recently updated
     - Transforms WKT geometries to WKB format using DuckDB SPATIAL extension
     - Applies zstd compression to output Parquet files
     - Creates tile-partitioned output: `./data/processed/mapillary_metadata/hive_partitioned_raw_metadata/tile={tile_name}/`
   - Comprehensive logging for file discovery, splitting progress, conversion status, and error handling
   - Can be run locally as `bash split_csvs_and_to_parquet_hpc.sh` or on HPC using `sbatch split_csvs_and_to_parquet_hpc.sh`
   
   **Configuration**: All parameters are loaded from `config.yaml`. Ensure you have set:
   - `csv_split_params.n_rows` - Number of rows per split chunk (default: 500)
   - `csv_split_params.split_enabled` - Whether to enable CSV splitting (default: true)
   - `csv_split_params.updated_after` - Only process files modified after this timestamp
   - `paths.raw_metadata_dir` - Input directory for raw metadata files
   - `paths.splitted_raw_metadata_dir` - Output directory for split CSV files
   - `paths.tile_partitioned_parquet_raw_metadata_dir` - Output directory for Parquet files
   
   **Processing Pipeline**:
   1. **Split Phase** (if `csv_split_params.split_enabled: true`):
      - Iterates through all `metadata_unfiltered_*.csv` files
      - Splits each file into ~500-row chunks (configurable via `n_rows`)
      - Creates numbered split files with headers preserved
      - Resumes from last completed chunk on re-runs
   2. **Convert Phase**:
      - For each tile, extracts tile name from filename
      - Calls `csv_to_parquet.py` with the tile name
      - Script searches for split CSV files first, falls back to raw metadata
      - Converts WKT geometries to WKB format for Parquet compatibility
      - Writes compressed Parquet output with zstd compression
   
   **For HPC usage**: Modify the SBATCH directives in `split_csvs_and_to_parquet_hpc.sh` if your cluster has different resource settings.

5. **csv_to_parquet.py** - Converts CSV metadata files to Parquet format using DuckDB's SPATIAL extension. Configuration is loaded from `config.yaml`. The script includes:
   - Comprehensive logging for file discovery, conversion progress, and error handling
   - Full docstring documentation for all functions
   - Tile-based processing: accepts tile name as argument and converts all associated files
   - Intelligent file search with fallback mechanism:
     - First priority: searches for split CSV files in `splitted_raw_metadata/` directory
     - Fallback: uses raw metadata CSV if splits don't exist or are not recently updated
   - Geometry transformation: converts WKT format geometries to WKB for Parquet compatibility
   - Applies zstd compression to output Parquet files for efficient storage
   - Creates tile-partitioned output structure: `./data/processed/mapillary_metadata/hive_partitioned_raw_metadata/tile={tile_name}/`
   - Error handling with detailed logging for troubleshooting

6. **metadata_intersections_and_filtering.py** - Core spatial filtering and classification script. One of the most critical components of the pipeline. Configuration is loaded from `config.yaml`. The script includes:
   - Comprehensive logging at INFO, DEBUG, WARNING, and ERROR levels for complete workflow visibility
   - Full docstring documentation for all functions with detailed parameter descriptions
   - **One-time setup** (on first execution):
     - Merges continent shapefiles into single parquet file
     - Downloads country polygons from Overture Maps
     - Converts GHSL and Africapolis shapefiles to parquet format
   - **Main workflow**:
     1. Intersects metadata with continent polygons → adds continent classification
     2. Intersects with country polygons → adds country classification
     3. Intersects with urban areas (GHSL/Africapolis) → adds urban layer info
     4. Assigns zoom-level tiles to metadata points and urban areas
     5. Classifies each image as urban or rural based on urban layer membership
     6. Filters sequences by distance threshold (distinct for urban vs rural):
        - Urban images: kept if > `urban_threshold` meters apart
        - Rural images: kept if > `rural_threshold` meters apart
     7. Outputs unfiltered and filtered parquet files
   - Tile-partitioned output: `./data/processed/mapillary_metadata/spatial_intersections/{unfiltered,filtered}/tile={tile_name}/`
   - Retry logic with exponential backoff for transient failures
   - Takes metadata parquet file path as argument
   - Skips processing if file modification time predates `updated_after` config timestamp

   ## To 6.

   There is a shell file **spatial_intersections_and_filtering_hpc.sh** that orchestrates parallel spatial filtering. The script:
   - Loads all configuration from `config.yaml` (fully configuration-driven, no hard-coded paths)
   - Discovers all tile-partitioned metadata files in `tile_partitioned_parquet_raw_metadata_dir`
   - **On HPC/SLURM cluster**: Auto-submits as array job, one task per metadata file
   - **Running locally**: Auto-detects CPU cores and spawns parallel background processes
   - Monitors individual file processing with timestamps and status indicators
   - Implements proper error handling with exit code reporting
   - Can be run as `bash spatial_intersections_and_filtering_hpc.sh` (both local and HPC)
   
   **Configuration**: All parameters loaded from `config.yaml`. Key settings:
   - `params.zoom_level` - Zoom level for tile calculations (must match create_tiles.py)
   - `params.urban_threshold` - Minimum distance in meters between urban images (default: 100)
   - `params.rural_threshold` - Minimum distance in meters between rural images (default: 1000)
   - `paths.tile_partitioned_parquet_raw_metadata_dir` - Input metadata directory
   - `paths.unfiltered_metadata_dir` - Output for unfiltered intersections
   - `paths.filtered_metadata_dir` - Output for distance-filtered results
   - `filenames.continents_filename`, `country_filename`, `ghsl_filename`, `africapolis_filename` - Geographic data sources
   - `params.ghsl_col_1`, `params.ghsl_col_2` - Urban layer column names
   - `params.africapolis_col_1`, `params.africapolis_col_2` - Optional Africapolis columns (set to None to skip)
   
   **Processing Features**:
   - Resumable: tracks completed files, skips re-processing
   - Parallel: runs multiple tiles simultaneously (limited by available CPU cores or SLURM array job count)
   - Robust: retry logic handles transient DuckDB/file access errors
   - Auditable: comprehensive logging tracks every intersection and filter stage
   - Flexible: supports optional Africapolis layer (set to None in config to disable)
   
   **For local execution**: 
   ```bash
   bash spatial_intersections_and_filtering_hpc.sh
   ```
   Automatically detects CPU count and spawns parallel processes.
   
   **For HPC execution**: 
   ```bash
   sbatch spatial_intersections_and_filtering_hpc.sh
   ```
   Auto-submits SLURM array job. Modify SBATCH directives for different cluster resource settings.

7. 'osm_preprocessing.py' works similar to 'spatial_intersections_and_filtering.py'. It does the intersection with continents, countries, and the adding of zoom tiles info to osm files at '../data/starter_files/osm_files' that end with .parquet. The new files will be saved to '../data/processed/osm_files'.

## To 7.

There is again shell file for this process similar to the one in 6. : 'osm_preprocessing_hpc.sh'. You can change the 'zoom_level' as long as it is keeped at the same value thorough the whole processes explained here. Again, you'll have to add the absolute location to your python executive.

8. 'find_osm_segments.py' finds the OSM segments within the vicinity of a sequence. To this end, it uses the bbox of the sequence extended by 'delta_x' and 'delta_y' meters in both directions. The default values are 50 meters. It only considers the segments that are less than 'distance_threshold' which is set default to 30 meters. 'zoom_level' is set default to 8. Only the first argument is mandatory which is the location of the metadata file. It will create multiple files, one for each OSM file considered.
9. 'get_nearest_osm_segments.py' combines the multiple files from 'find_osm_segments.py' and creates two indices 'abs_diff' and 'percent_diff'. The script accepts two voluntary parameters 'threshold1' (default 10) and 'threshold2' (default 20). Since a point can be assigned to multiple OSM segments, a decision has to be made. If there are segments that are less than 'threshold1' meters away, no further segments will be considered. Segments that are between 'threshold1' and 'threshold2' will only be taken into account, if this is not the case. 'abs_diff' is the absolute difference between the nearest point and the respective point (0 for the nearest point). 'percent_diff' is the percentual difference between the nearest and the respective point (again 0 for the nearest point).

# To 8. and 9.

There is a single shell file that runs both 8. and 9. : 'find_and_get_nearest_osm_segments.sh'. You'll have to modify the settings you are satisfied with the default ones. Again, you should point to the absolute location of your python executive.

1. Download of the Tiles (create_tiles and get_linestrings_from_tiles)
2. Get Metadata (We should check which ones are already downloaded) The code does this now
3. Split csvs and convert them to parquet. It should continue where the other have left. The code does this now. (split_csvs_and_to_parquet_hpc.sh) and (csv_to_parquet.py)
4.
