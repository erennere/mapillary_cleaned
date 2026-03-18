#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=96:00:00

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd $SCRIPT_DIR

# Path to Python executable (update this to your environment)
PYTHON_BIN="python"
# EXAMPLE for Micromamba: PYTHON_BIN="/path/to/micromamba/envs/your_env/bin/python"

# Note: Update mly_key with your own Mapillary API key
# Configuration parameters are loaded from config.yaml

echo "Starting tile creation and Mapillary data extraction..."
echo "Working directory: $SCRIPT_DIR"
echo "Python executable: $PYTHON_BIN"

# Step 1: Create tiles
echo ""
echo "Step 1: Creating tiles..."
"$PYTHON_BIN" create_tiles.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to create tiles"
    exit 1
fi

# Step 2: Download and process linestrings from tiles
echo ""
echo "Step 2: Downloading and processing linestrings from Mapillary..."
"$PYTHON_BIN" get_linestrings_from_tiles.py
if [ $? -ne 0 ]; then
    echo "Error: Failed to download linestrings"
    exit 1
fi

echo ""
echo "Completed successfully. Check the log output above for details."
echo "Output files are saved to the directories configured in config.yaml"