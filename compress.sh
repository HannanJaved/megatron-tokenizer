#!/bin/bash

#SBATCH --job-name=dclm
#SBATCH --output=logs/compress/%x.out
#SBATCH --error=logs/compress/%x.err
#SBATCH --nodes=1          
#SBATCH --ntasks-per-node=1         
#SBATCH --time=02:30:00    
#SBATCH --account=laionize 
#SBATCH --partition=batch  
#SBATCH --cpus-per-task=6          
#SBATCH --mem=64G          

pip install zstandard

source="/p/data1/datasets/mmlaion/mahadik1/tokenized_cosmo2/DCLM-Edu/merged.bin"
target="${source}.zst"

zstd --ultra -22 -T0 -v "$source" -o "$target"

