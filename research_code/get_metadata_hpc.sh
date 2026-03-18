#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=64
#SBATCH --mem=234gb
#SBATCH --time=96:00:00

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd $SCRIPT_DIR

# Path to Python executable (update this to your environment)
PYTHON_BIN="python"
# EXAMPLE for Micromamba: PYTHON_BIN="/path/to/micromamba/envs/your_env/bin/python"

# Note: Configuration parameters are loaded from config.yaml
# Ensure you have set your Mapillary API key in config.yaml under params.mly_key

echo "Starting parallel Mapillary metadata download..."
echo "Working directory: $SCRIPT_DIR"
echo "Python executable: $PYTHON_BIN"
echo "Launching 10 parallel instances, each processing deterministic tile chunks"

# Number of parallel instances to run
NUM_INSTANCES=10

# Array to store process IDs
pids=()

# Launch 10 instances in parallel, each with a different instance_id (1-10)
for i in $(seq 1 $NUM_INSTANCES); do
    echo ""
    echo "Launching instance $i/$NUM_INSTANCES..."
    "$PYTHON_BIN" get_metadata.py $i &
    pids+=($!)
done

echo ""
echo "All instances launched. Waiting for completion..."
echo ""

# Wait for all background processes and collect exit codes
failed=0
for i in "${!pids[@]}"; do
    pid=${pids[$i]}
    instance_num=$((i + 1))
    if wait $pid; then
        echo "Instance $instance_num completed successfully"
    else
        exit_code=$?
        echo "Instance $instance_num failed with exit code $exit_code"
        failed=$((failed + 1))
    fi
done

echo ""
if [ $failed -eq 0 ]; then
    echo "All instances completed successfully!"
    echo "Metadata files are saved to the directory configured in config.yaml"
    exit 0
else
    echo "Error: $failed out of $NUM_INSTANCES instances failed"
    exit 1
fi