#!/bin/bash
#SBATCH --job-name=T_IWM4+    
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --account=laionize
#SBATCH --partition=batch

module load Python
module load GCC

source /p/project/projectnucleus/mahadik1/tvenv2/bin/activate
# source /p/project/projectnucleus/mahadik1/.python/.tvenv/bin/activate

# python convert_to_json.py
python convert_jsonl.py