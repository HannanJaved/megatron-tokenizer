#!/bin/bash

#SBATCH --job-name=Java       
#SBATCH --output=logs/download-stack-edu/%x_%j.out       
#SBATCH --error=logs/download-stack-edu/%x_%j.err        
#SBATCH --nodes=1                    
#SBATCH --ntasks-per-node=1         
#SBATCH --time=02:00:00               
#SBATCH --account=laionize           
#SBATCH --partition=devel            
#SBATCH --cpus-per-task=6          

source /p/project/projectnucleus/mahadik1/tvenv2/bin/activate

# Use job-specific cache directory to avoid race conditions
export HF_HOME="/p/project/projectnucleus/mahadik1/.cache/hf_${SLURM_JOB_ID}"
mkdir -p "$HF_HOME"

python -u download_java.py \
    --language Java \
    --shard 0 \
    --subshard 0 \
    --total-subshards 2 \
    --output_dir /p/data1/datasets/mmlaion/language/raw/stack-edu/Code/ \
    --num_proc 16
