#!/bin/bash
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --array=1974-2024

#SBATCH --time 0-8:00:00
#SBATCH --error=./logs/slurm/%j.err
#SBATCH --output=./logs/slurm/%j.out
#SBATCH --mem=8G
#SBATCH --mail-user=yusukemh@hawaii.edu
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL
source ~/anaconda3/bin/activate sandbox
python3 ./rearrange_hourly_ERA5.py --year $SLURM_ARRAY_TASK_ID