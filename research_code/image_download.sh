#!/bin/bash

################################################################################
# execute_image_download_distributed.sh - Intelligent Image Download Pipeline
#
# Automatically detects execution environment (HPC vs Local) and distributes
# image download workload via chunked parquet file processing.
#
# Environment Detection:
#   HPC:   SLURM detected (SLURM_JOB_ID exists)
#   LOCAL: No SLURM environment
#
# Default Execution Modes:
#   HPC:   array job (10 chunks distributed across SLURM tasks)
#   LOCAL: sequential (process chunks one at a time)
#
# Available Execution Modes (via config override):
#   array:      One chunk per SLURM task (HPC only, default on HPC)
#   sequential: One chunk at a time (default on LOCAL)
#   parallel:   Multiple chunks concurrently (HPC + LOCAL)
#
# Configuration (from config.yaml):
#   execution.mode: array | sequential | parallel
#   execution.num_jobs: number of parallel chunks (default: 10)
#
# SLURM Header (only needed when submitting to HPC):
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=236gb
#SBATCH --cpus-per-task=16
#SBATCH --array=0-9
#
################################################################################

set -e

# Change to script directory
if [[ -n "$SLURM_SUBMIT_DIR" ]]; then
    cd "$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
    cd "$SCRIPT_DIR"
fi

PYTHON_SCRIPT="image_download.py"

# Set thread environment variables
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-$(nproc 2>/dev/null || echo 8)}
export OPENBLAS_NUM_THREADS=$OMP_NUM_THREADS
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export NUMEXPR_NUM_THREADS=$OMP_NUM_THREADS

################################################################################
# Environment Detection: HPC vs Local
################################################################################

if [[ -n "$SLURM_JOB_ID" ]]; then
    ENVIRONMENT="HPC"
    DEFAULT_MODE="array"
    DEFAULT_NUM_JOBS=10
else
    ENVIRONMENT="LOCAL"
    DEFAULT_MODE="sequential"
    DEFAULT_NUM_JOBS=$(nproc 2>/dev/null || echo 4)
fi

echo "Environment: $ENVIRONMENT (default mode: $DEFAULT_MODE)"

################################################################################
# Configuration Override (from config.yaml if present)
################################################################################

if [[ -f "config.yaml" ]]; then
    MODE=$(grep -E "^\s*mode:" config.yaml 2>/dev/null | awk '{print $2}' | tr -d ' ' || echo "$DEFAULT_MODE")
    NUM_JOBS=$(grep -E "^\s*num_jobs:" config.yaml 2>/dev/null | awk '{print $2}' | tr -d ' ' || echo "$DEFAULT_NUM_JOBS")
else
    MODE=$DEFAULT_MODE
    NUM_JOBS=$DEFAULT_NUM_JOBS
fi

echo "Execution mode: $MODE (num_jobs: $NUM_JOBS)"

################################################################################
# Execution Modes
################################################################################

if [[ "$MODE" == "array" ]]; then
    # Array job mode: one chunk per SLURM task
    # Only works on HPC
    if [[ "$ENVIRONMENT" != "HPC" ]]; then
        echo "ERROR: array mode only available on HPC (SLURM)"
        exit 1
    fi
    
    if [[ -z "$SLURM_ARRAY_TASK_ID" ]]; then
        echo "ERROR: array mode requires SLURM array job (SLURM_ARRAY_TASK_ID not set)"
        exit 1
    fi
    
    # Each task processes one chunk with size NUM_JOBS
    CHUNK_IDX=$SLURM_ARRAY_TASK_ID
    CHUNK_SIZE=$NUM_JOBS
    
    echo "Array job: Processing chunk $CHUNK_IDX with size $CHUNK_SIZE (task $SLURM_ARRAY_TASK_ID)"
    python "$PYTHON_SCRIPT" "$CHUNK_IDX" "$CHUNK_SIZE"

elif [[ "$MODE" == "sequential" ]]; then
    # Sequential mode: process chunks one at a time
    # For HPC: only run on task 0 of array job (if array job is running)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        echo "Sequential mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    
    CHUNK_SIZE=$NUM_JOBS
    echo "Sequential mode: Processing chunks sequentially with chunk_size=$CHUNK_SIZE"
    
    # Process chunks one at a time (chunk_idx from 0 to NUM_JOBS-1)
    for CHUNK_IDX in $(seq 0 $((CHUNK_SIZE - 1))); do
        echo "Processing chunk $CHUNK_IDX..."
        python "$PYTHON_SCRIPT" "$CHUNK_IDX" "$CHUNK_SIZE"
    done

elif [[ "$MODE" == "parallel" ]]; then
    # Parallel mode: run multiple chunks concurrently
    # For HPC: only run on task 0 of array job (if array job is running)
    if [[ -n "$SLURM_ARRAY_TASK_ID" ]] && [[ $SLURM_ARRAY_TASK_ID -ne 0 ]]; then
        echo "Parallel mode: skipping task $SLURM_ARRAY_TASK_ID (only task 0 runs)"
        exit 0
    fi
    
    CHUNK_SIZE=$NUM_JOBS
    FAILED_CHUNKS=()
    ACTIVE_JOBS=0
    
    echo "Parallel mode: Processing chunks concurrently with $NUM_JOBS parallel jobs and chunk_size=$CHUNK_SIZE"
    
    for CHUNK_IDX in $(seq 0 $((CHUNK_SIZE - 1))); do
        # Wait if we've reached max parallel jobs
        while [[ $(jobs -r | wc -l) -ge $NUM_JOBS ]]; do
            sleep 1
        done
        
        # Launch chunk in background
        python "$PYTHON_SCRIPT" "$CHUNK_IDX" "$CHUNK_SIZE" &
        ACTIVE_JOBS=$((ACTIVE_JOBS + 1))
    done
    
    # Wait for all background jobs to complete
    for job in $(jobs -p); do
        if ! wait $job; then
            FAILED_CHUNKS+=("$job")
        fi
    done
    
    if [[ ${#FAILED_CHUNKS[@]} -gt 0 ]]; then
        echo "ERROR: $((${#FAILED_CHUNKS[@]})) chunk(s) failed"
        exit 1
    fi
    
    echo "SUCCESS: All $ACTIVE_JOBS chunks completed"

else
    echo "ERROR: Unknown execution mode '$MODE' (valid: array, sequential, parallel)"
    exit 1
fi

echo "Execution completed successfully"
