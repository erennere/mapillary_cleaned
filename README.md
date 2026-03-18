# Mapillary Street View Image Processing Pipeline

This repository provides a production-grade, tile-based system for accessing and processing Street View Images from Mapillary, combined with OpenStreetMap road network data. Initially developed for continent-level processing, it has evolved into a sustainable, parallelizable pipeline suitable for both local and HPC (High-Performance Computing) environments.

## System Architecture

The pipeline follows a modular, tile-based workflow that enables efficient parallel processing at each stage:

```
Tiles Generation
        ↓
Sequence Download  
        ↓
Metadata Download (Parallel: 10 instances)
        ↓
CSV → Parquet Conversion
        ↓
Spatial Filtering & Classification ─────┐
        ↓                                 ├─→ Near-OSM Matching → Image Download
OSM Processing ────────────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.8+
- DuckDB with SPATIAL extension
- GeoPandas, Shapely, Pandas, NumPy
- Mapillary API token
- GDAL/OGR tools

### Basic Usage
```bash
# Run entire pipeline locally (auto-parallelizes)
bash research_code/get_sequences_hpc.sh
bash research_code/get_metadata_hpc.sh
bash research_code/split_csvs_and_to_parquet_hpc.sh
bash research_code/spatial_intersections_and_filtering_hpc.sh
bash research_code/highways_sort_hpc.sh
bash research_code/find_and_get_nearest_osm_segments.sh

# Or submit to HPC cluster
sbatch research_code/get_sequences_hpc.sh
sbatch research_code/get_metadata_hpc.sh
# ... etc
```

## Data Workflow

### Stage 1: Tiles & Sequences

**Scripts:** `create_tiles.py`, `get_linestrings_from_tiles.py`  
**Automation:** `get_sequences_hpc.sh`

- Generates XYZ-tiles at specified zoom level (default: 8)
- Downloads sequence linestrings from Mapillary API
- Retry logic: 3 attempts per tile
- Output: GeoPackage files (completed & failed)

### Stage 2: Metadata Download

**Script:** `get_metadata.py`  
**Automation:** `get_metadata_hpc.sh`

- Downloads image metadata for all sequences
- **Parallel:** 10 deterministic tile chunks processed simultaneously
- **Resumable:** Skips previously downloaded sequences
- Retry logic: Configurable attempts (default: 10)
- Output: CSV files per tile

### Stage 3: Metadata Processing

**Scripts:** `split_csvs_and_to_parquet_hpc.sh`, `csv_to_parquet.py`

- **Phase 1:** Splits large CSV files (configurable chunk size, default: 500K rows)
- **Phase 2:** Converts to Parquet format with:
  - WKT → WKB geometry transformation (DuckDB SPATIAL)
  - zstd compression
  - Tile partitioning (one directory per tile)
- **Resumable:** Continues from last completed chunk on re-run

### Stage 4: Spatial Filtering

**Script:** `metadata_intersections_and_filtering.py`  
**Automation:** `spatial_intersections_and_filtering_hpc.sh`

- Intersects metadata with geographic layers:
  - Continents (one-time merge on first run)
  - Countries (downloaded from Overture Maps)
  - Urban areas (GHSL/Africapolis)
- Classifies each image as urban/rural
- Distance-based filtering:
  - Urban: Keep if >100m apart
  - Rural: Keep if >1000m apart
- Output: Unfiltered & filtered tile-partitioned Parquet files
- **Timestamp-aware:** Skips files not modified after configured date

### Stage 5: OSM Processing

**Script:** `highways_sort.py`  
**Automation:** `highways_sort_hpc.sh`

- Filters OSM highway data:
  - Highway tag not NULL
  - LineString geometries only
  - Visible & latest versions only
- Adds geographic context:
  - Continent classification (spatial join)
  - Country classification (spatial join)
  - Zoom-level tile assignment
- Output: Tile-partitioned highway data

### Stage 6: OSM-Metadata Matching

**Scripts:** `find_osm_segments.py`, `get_nearest_osm_segments.py`  
**Automation:** `find_and_get_nearest_osm_segments.sh`

- **find_osm_segments.py:** For each image point:
  - Uses haversine distance calculation (Earth radius: 6,371,008m)
  - Searches OSM segments within ±50m bounding box
  - Filters to <30m distance threshold
  - Outputs: Shortest connecting line + distance

- **get_nearest_osm_segments.py:** Combines results from multiple tiles:
  - Creates distance indices (absolute & percent difference)
  - Resolves multi-match conflicts
  - Configurable thresholds (default: 10m, 20m)

### Stage 7: Image Download

**Script:** `image_download.py`

- Asynchronous multi-threaded downloading
- Connection pooling & rate limiting
- OpenCV image resizing
- Resumable: Skips previously downloaded images
- Tile-partitioned output with image IDs

## Configuration

All scripts use `config.yaml`. Key settings:

### Required Parameters
```yaml
params:
  zoom_level: 8              # Tile zoom level (must be consistent)
  mly_key: "your_api_token"  # Mapillary access token
  earth_radius: 6371008      # Earth radius in meters
  
paths:
  data_dir: "./data"         # Base data directory
```

### Filtering & Processing
```yaml
params:
  urban_threshold: 100       # Urban distance filter (meters)
  rural_threshold: 1000      # Rural distance filter (meters)
  distance_threshold: 30     # OSM matching threshold (meters)

metadata_params:
  batch_size: 500
  windows: 10
  max_workers: 8
  missing_attempts: 10       # Retries for failed downloads
  
csv_split_params:
  n_rows: 500000             # Rows per CSV chunk
  split_enabled: true
  updated_after: "2024-01-01" # Skip older files (optional)
```

## Script Details

### Core Python Scripts

| Script | Purpose | Key Features |
|--------|---------|--------------|
| `create_tiles.py` | Generate XYZ tiles | Mercantile library, polygon restriction support |
| `get_linestrings_from_tiles.py` | Download sequences | Retry logic (3x), progress tracking, HPC buffer flushing |
| `get_metadata.py` | Download image metadata | Parallel instances (1-10), resumable, configurable retries |
| `csv_to_parquet.py` | CSV → Parquet conversion | WKT→WKB transform, zstd compression, tile awareness |
| `metadata_intersections_and_filtering.py` | Spatial filtering | One-time setup, urban/rural classification, distance filtering |
| `highways_sort.py` | OSM highway enrichment | Spatial joins, geographic tagging, tile assignment |
| `find_osm_segments.py` | Point-to-segment matching | Haversine distance, bbox search, configurable threshold |
| `get_nearest_osm_segments.py` | Conflict resolution | Multi-match handling, distance indexing |

### Bash Automation Scripts

| Script | Purpose | Execution |
|--------|---------|-----------|
| `get_sequences_hpc.sh` | Runs stages 1-2 in sequence | Local: bash / HPC: sbatch |
| `get_metadata_hpc.sh` | Launches 10 parallel metadata instances | Local/HPC auto-detection |
| `spatial_intersections_and_filtering_hpc.sh` | Parallelizes spatial filtering | CPU-aware local / SLURM array on HPC |
| `highways_sort_hpc.sh` | Parallelizes OSM processing | CPU-aware local / SLURM array on HPC |
| `find_and_get_nearest_osm_segments.sh` | Runs stages 6A-6B sequentially | Local execution |

### Utility Scripts

- **start.py** - Configuration loader with path normalization
- **image_download.py** - Mapillary image downloader (legacy, needs update)
- **metadata_download.py** - Metadata query utility (legacy, needs update)

## Recent Improvements & Bug Fixes

- ✅ **Added:** Timestamp-based filtering in `spatial_intersections_and_filtering_hpc.sh`
- ✅ **Added:** Comprehensive script-level docstrings
- ✅ **Improved:** Output visibility on HPC batch systems

## Known Limitations & TODOs

### Current Blockers
- [ ] HPC compatibility not fully tested on production clusters
- [ ] OSM source currently uses local SDS intern directory (not S3-backed)
- [ ] Legacy scripts (`image_download.py`, `metadata_download.py`) need refactoring

### Enhancements Needed
- [ ] Parallelize `find_osm_segments.py` via bash wrapper across partitioned folders
- [ ] Add tracking/ID persistence from highways_sort onwards (resume capability)
- [ ] Create bash wrapper for `image_download.py` with tile-level parallelization
- [ ] Integrate S3/Rustfs for OSM planet source
- [ ] Performance optimization for large-scale continent processing

## Features

### Production-Ready
- ✅ Resumable execution at every stage
- ✅ Comprehensive retry logic with exponential backoff
- ✅ Real-time HPC logging with buffer flushing
- ✅ Deterministic parallel tile chunking (reproducible across runs)
- ✅ Tile-partitioned output for efficient querying
- ✅ Configuration-driven (no hardcoded paths)
- ✅ Local & HPC execution (auto-detection)

### Robustness
- ✅ Error handling with detailed logging (DEBUG, INFO, WARNING, ERROR)
- ✅ Transient failure recovery (DuckDB lock/timeout handling)
- ✅ Spatial index acceleration (STRtree for OSM matching)
- ✅ Compression (zstd) for efficient storage

## Running on HPC

### SLURM Cluster (Recommended)

```bash
# Stack jobs with dependencies
sbatch --job-name=tiles research_code/get_sequences_hpc.sh
sbatch --dependency=afterok:$JOB1_ID --job-name=metadata research_code/get_metadata_hpc.sh
sbatch --dependency=afterok:$JOB2_ID --job-name=process research_code/split_csvs_and_to_parquet_hpc.sh
# ... etc
```

### Local Multi-Core (Auto-Parallelization)

```bash
# Automatically detects CPU count and limits parallelism
bash research_code/spatial_intersections_and_filtering_hpc.sh
# Running locally on 16 cores (using max 6 parallel jobs for memory efficiency)
```

## Troubleshooting

### No Output on HPC
- Scripts now include `sys.stdout.flush()` after logging statements
- Check SLURM output files: `slurm-*.out`

### Timestamp Skipping Files
- Ensure `csv_split_params.updated_after` in `config.yaml` is less restrictive
- Format: ISO 8601 date string (e.g., `"2024-01-01T00:00:00"`)

### Distance Calculation Issues  
- Verify `params.earth_radius` matches 6,371,008m
- Check haversine formula: uses WKT POINT strings from database

### Performance Issues
- Reduce `metadata_params.max_workers` if memory exhausted
- Increase `csv_split_params.n_rows` for faster conversion
- Monitor DuckDB temp file cleanup in error cases

## Contributing

When modifying scripts:
1. Add comprehensive docstrings (module + functions)
2. Include `sys.stdout.flush()` after logging in HPC contexts
3. Ensure resumability where applicable
4. Test on both local and HPC environments
5. Update this README with changes

