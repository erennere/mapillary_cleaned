#!/bin/bash
#SBATCH --partition=cpu-single
#SBATCH --time=24:00:00
#SBATCH --mem=234gb
#SBATCH --cpus-per-task=64
#SBATCH --array=0-9

# The task ID for this job array task
TASK_ID=${SLURM_ARRAY_TASK_ID}

# Run the script with the task ID as an argument
echo "Running task with signifier (ID): $TASK_ID"
python statistics_geographic_layers.py $TASK_ID
