#!/bin/bash
#SBATCH --partition=single
#SBATCH --cpus-per-task=16
#SBATCH --mem=64gb
#SBATCH --time=96:00:00

python highways_sort.py

