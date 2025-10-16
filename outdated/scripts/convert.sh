#!/bin/bash
#SBATCH --job-name=Batch0
#SBATCH --output=logs/convert_to_jsonl/FineWeb/%x.out
#SBATCH --error=logs/convert_to_jsonl/FineWeb/%x.err
#SBATCH --nodes=1
#SBATCH --time=06:00:00
#SBATCH --account=laionize
#SBATCH --partition=batch
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G

# module load Python
module load GCC

# unset PYTHONPATH
# source /p/project/projectnucleus/mahadik1/tvenv2/bin/activate
source /p/project/projectnucleus/mahadik1/.python/.tvenv/bin/activate

# python convert_to_json.py
python /p/project/projectnucleus/mahadik1/outdated/scripts/convert_jsonl.py \
--input /p/data1/datasets/mmlaion/language/raw/HuggingFaceFW-fineweb-edu-1.3.0/data  \
--output /p/data1/datasets/mmlaion/language/raw/HuggingFaceFW-fineweb-edu-1.3.0/jsonl_data \
--batch-size 15 \
--batch 0