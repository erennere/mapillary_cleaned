#!/bin/bash

### ————————————————————————
### CONFIG ALWAYS AVAILABLE
### ————————————————————————
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data/processed/mapillary_metadata/unfiltered"
PYTHON_BIN="/mnt/d//micromamba/envs/eren/python.exe" 
EXCLUDE_PATTERNS="example_to_skip,bad_file_prefix"   # ✅ available in both HPC and local

### ————————————————————————
### BUILD FILE LIST
### ————————————————————————
EXCLUDE_REGEX=$(echo "$EXCLUDE_PATTERNS" | sed 's/,/|/g')
mapfile -t files < <(
    find "$DATA_DIR" -type f -path "*/tile=*/*.parquet" \
    | grep -Ev "$EXCLUDE_REGEX" \
    | sort
)

### ————————————————————————
### SELF-SUBMIT LOGIC (LOGIN NODE)
### ————————————————————————
if [ -z "$SLURM_ARRAY_TASK_ID" ] && command -v sbatch >/dev/null 2>&1; then
    N=${#files[@]}
    echo "🔍 Detected $N valid parquet files."
    echo "📤 Submitting SLURM array job..."
    sbatch --array=0-$((N-1)) "$0"
    exit 0
fi

### ————————————————————————
### SLURM JOB HEADER (COMPUTE NODE)
### ————————————————————————
#SBATCH --partition=single
#SBATCH --error=errors_%A_%a.err
#SBATCH --output=outputs_%A_%a.out
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64gb
#SBATCH --time=96:00:00
#SBATCH --array=0-0

### ————————————————————————
### RUN JOB
### ————————————————————————
if [ -n "$SLURM_ARRAY_TASK_ID" ]; then
    # Running on HPC as SLURM array task
    file="${files[$SLURM_ARRAY_TASK_ID]}"
    echo "🚀 SLURM task $SLURM_ARRAY_TASK_ID processing $file at $(date)"
    "$PYTHON_BIN" find_osm_segments.py "$file"
    "$PYTHON_BIN" get_nearest_osm_segments.py "$file"
    echo "✅ Done $file at $(date)"
else
    # Running locally: detect CPU cores and run dynamically
    CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
    echo "💻 Running locally on $CPU_CORES cores"

    running_pids=()
    for idx in "${!files[@]}"; do
        file="${files[$idx]}"
        echo "📄 Starting $file at $(date)"
        "$PYTHON_BIN" find_osm_segments.py "$file"
        "$PYTHON_BIN" get_nearest_osm_segments.py "$file"
        running_pids+=($!)

        while [ ${#running_pids[@]} -ge $CPU_CORES ]; do
            if wait -n 2>/dev/null; then
                tmp=()
                for pid in "${running_pids[@]}"; do
                    if kill -0 "$pid" 2>/dev/null; then
                        tmp+=("$pid")
                    fi
                done
                running_pids=("${tmp[@]}")
            else
                wait
                running_pids=()
            fi
        done
    done

    # Wait for remaining jobs
    wait
    echo "✅ All files processed locally at $(date)"
fi
