This repository provides tools to access and work with Street View Images hosted on Mapillary. Initially, the work was divided between continents as a one-time project. However, due to interest from colleagues, this evolved into a more sustainable, production-grade system using an XYZ-tile-based approach.

## Overview

The pipeline follows this workflow:

- **Metadata Processing**: Divide the world into XYZ-tiles, query Mapillary for sequence IDs, download metadata, split into smaller files if needed, and intersect with geographic layers (country, continent, urban info) for filtering.
- **OSM Processing**: Extract highways from OSM sources, partition using the same tile scheme, and add geographic context.
- **Merge & Filter**: Intersect metadata with OSM files using bbox and distance thresholds.
- **Image Download**: Download, resize, and save images organized by tile scheme.
- **Aggregation**: Aggregate road surface tags from predictions using distance-weighted approach and push to HOTOSM/HDX.

## Known Issues & TODOs

This is a cleanup of an existing pipeline. Some work remains:
- Some files may contain hardcoded directories that should be replaced with absolute paths
- Relative paths should be replaced with absolute paths
- HPC compatibility via bash scripts has not yet been fully tested

## Scripts (in `research_code/`)

### 1. Tile & Sequence Generation
- **create_tiles.py** - Generates map tiles using Mercantile library. Optionally restricts to polygon; defaults to world coverage. Outputs to `./data/processed/tiles/tiles_z{zoom_level}.gpkg`.
- **get_linestrings_from_tiles.py** - Downloads Mapillary vector tiles and extracts sequences. Includes retry logic (default: 3 attempts). Outputs successful tiles to `./data/processed/tiles/completed/` and failed tiles to `./data/processed/tiles/failed/`.

### 2. Metadata Download
- **get_metadata.py** - Downloads image metadata for sequences. Supports resumable downloads (skips previously downloaded sequences). Optional `instance_id` parameter (1-10) enables parallel processing. Outputs to `./data/processed/mapillary_metadata/metadata_unfiltered_{tile}.csv`.
- **metadata_download.py** - Legacy metadata utility using asynchronous bbox-based queries with connection pooling and rate limiting. Supports resumable downloads with missing sequence tracking.

### 3. Metadata Processing
- **split_csvs_and_to_parquet_hpc.sh** - Two-phase bash script: splits large CSV files (default: 500,000 rows) then converts to Parquet with WKT-to-WKB transformation and zstd compression.
- **csv_to_parquet.py** - Converts CSV to Parquet using DuckDB SPATIAL extension. Transforms WKT geometries to WKB, applies zstd compression. Takes tile name as argument. Outputs to `./data/processed/mapillary_metadata/hive_partitioned_raw_metadata/tile={tile_name}/`.

### 4. Spatial Filtering & Classification
- **metadata_intersections_and_filtering.py** - Core spatial filtering script. Intersects metadata with geographic layers (continents, countries, urban areas via GHSL/Africapolis). Classifies as urban/rural and filters by distance. On first run: merges continent data, downloads countries from Overture Maps, converts urban layer files. Outputs to `./data/processed/mapillary_metadata/spatial_intersections/{unfiltered,filtered}/tile={tile_name}/`.

### 5. OSM Processing
- **highways_sort.py** - Filters OSM highway data, adds continent/country context, assigns tiles. Outputs to `./data/processed/osm_data/`.

### 6. OSM-Metadata Matching
- **find_osm_segments.py** - Finds OSM segments near sequences using bbox extension (default: ±50m) and distance threshold (default: 30m). Takes metadata file path as argument. Outputs one file per OSM file.
- **get_nearest_osm_segments.py** - Combines files from `find_osm_segments.py`. Creates distance indices (`abs_diff`, `percent_diff`) with configurable thresholds (default: 10m, 20m). Resolves multi-match conflicts based on distance.

### 7. Image Download
- **image_download.py** - Downloads and processes Mapillary images. Features: asynchronous multi-threaded downloading, connection pooling, EXIF preservation, OpenCV resizing, resumable downloads. Saves to tile-partitioned structure.

### 8. Utilities
- **start.py** - Configuration loader. Loads YAML from `config.yaml`, formats paths, normalizes file paths for all scripts.

## Bash Automation Scripts

- **get_sequences_hpc.sh** - Runs create_tiles.py and get_linestrings_from_tiles.py in sequence. Local: `bash get_sequences_hpc.sh`, HPC: `sbatch get_sequences_hpc.sh`
- **get_metadata_hpc.sh** - Launches 10 parallel instances of get_metadata.py, each processing deterministic tile chunk.
- **spatial_intersections_and_filtering_hpc.sh** - Parallelizes metadata_intersections_and_filtering.py. Auto-detects CPU cores or submits SLURM array job.
- **highways_sort_hpc.sh** - Parallelizes highways_sort.py. Auto-detects CPU cores or submits SLURM array job.
- **find_and_get_nearest_osm_segments.sh** - Runs find_osm_segments.py and get_nearest_osm_segments.py in sequence.

## Configuration

All scripts read from `config.yaml`. Key parameters:

**Pipeline Parameters**:
- `params.zoom_level` - Tile zoom level (must be consistent across pipeline)
- `params.mly_key` - Mapillary API access token

**Filtering Parameters**:
- `params.urban_threshold` - Urban distance filter (default: 100m)
- `params.rural_threshold` - Rural distance filter (default: 1000m)

**Concurrency Settings**:
- `metadata_params.batch_size` - Batch size for API calls
- `metadata_params.windows` - Concurrency windows
- `metadata_params.max_workers` - Maximum worker threads

**CSV Split**:
- `csv_split_params.n_rows` - Rows per split chunk (default: 500)
- `csv_split_params.split_enabled` - Enable/disable splitting

**Geographic Data**:
- `filenames.continents_filename`, `filenames.country_filename`, `filenames.ghsl_filename`, `filenames.africapolis_filename` - Data source paths

## Complete Processing Pipeline

1. **Tile & Sequence Download** → `get_sequences_hpc.sh`
2. **Metadata Download** → `get_metadata_hpc.sh`
3. **CSV to Parquet** → `split_csvs_and_to_parquet_hpc.sh`
4. **Spatial Filtering** → `spatial_intersections_and_filtering_hpc.sh`
5. **OSM Processing** → `highways_sort_hpc.sh`
6. **Near-OSM Matching** → `find_and_get_nearest_osm_segments.sh`
7. **Image Download** → `image_download.py`

## Implementation Status

### Verified Features
- ✓ Resumable downloads (get_metadata.py)
- ✓ Retry logic (get_linestrings_from_tiles.py: 3 attempts)
- ✓ WKT-to-WKB transformation (csv_to_parquet.py)
- ✓ zstd compression (csv_to_parquet.py)
- ✓ Spatial intersections & filtering (metadata_intersections_and_filtering.py)
- ✓ Parallel processing with deterministic chunking (get_metadata_hpc.sh)
- ✓ Configuration-driven paths (start.py)

### Notes
- All scripts support resumable execution (skip previously processed data)
- Parallel bash scripts auto-detect CPU cores locally or submit SLURM jobs on HPC
- Modify SBATCH directives in bash scripts for different cluster settings
- HPC compatibility has not yet been fully tested
