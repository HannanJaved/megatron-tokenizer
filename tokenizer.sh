#!/bin/bash

#SBATCH --job-name=C
#SBATCH --output=logs/tokenize/Stack-Edu/%x.out
#SBATCH --error=logs/tokenize/Stack-Edu/%x.err
#SBATCH --nodes=1          
#SBATCH --ntasks-per-node=1         
#SBATCH --time=1-00:00:00               
#SBATCH --account=laionize           
#SBATCH --partition=batch           
#SBATCH --cpus-per-task=6          
#SBATCH --mem=32G          

# module load Python
# module load PyTorch
module load CUDA
module load GCC
module load PyYAML


# Activate the virtual environment
# source /p/project/projectnucleus/mahadik1/.python/.tvenv/bin/activate
# VENV_PYTHON="/p/project/projectnucleus/mahadik1/tvenv2/bin/python"

# # Check if the virtual environment Python exists
# if [ ! -f "$VENV_PYTHON" ]; then
#     echo "Error: Virtual environment Python not found at $VENV_PYTHON"
#     exit 1
# fi

# echo "Using Python from: $VENV_PYTHON"
# echo "Python version: $($VENV_PYTHON --version 2>&1)"

# module load PyTorch/2.5.1

MEGATRON_PATH="Megatron-LM"

# echo "Changing directory to: $MEGATRON_PATH"
# cd "$MEGATRON_PATH" || { echo "Error: Could not change to $MEGATRON_PATH. Exiting."; exit 1; }

# Export PYTHONPATH to include Megatron-LM only
export PYTHONPATH="$(pwd)/Megatron-LM"
echo "PYTHONPATH set to: $PYTHONPATH"

INPUT="/p/data1/datasets/mmlaion/language/raw/stack-edu/Code/jsonl_data/C/"
OUTPUT_PREFIX="/p/data1/datasets/mmlaion/mahadik1/tokenized_cosmo2/Stack-Edu/C/"
TOKENIZER_TYPE="HuggingFaceTokenizer"
# Use the local cached tokenizer path instead of model name to avoid HF hub lookups
TOKENIZER_MODEL="/p/project1/projectnucleus/mahadik1/.cache/huggingface/models--HuggingFaceTB--cosmo2-tokenizer/snapshots/4ce2318a3628e77279c939ed6a9f3f03034402de"
CPUS_PER_WORKER=6
SCRIPT="/p/project1/projectnucleus/mahadik1/Megatron-LM/preprocess_data_parallel.py"
export HF_HUB_OFFLINE=1

# export PYTHONPATH=""
source /p/project1/projectnucleus/mahadik1/.python/.tvenv/bin/activate

CMD="python $SCRIPT \
    --input $INPUT \
    --output-prefix $OUTPUT_PREFIX \
    --tokenizer-type $TOKENIZER_TYPE \
    --tokenizer-model $TOKENIZER_MODEL \
    --workers $SLURM_CPUS_PER_TASK \
    --cpus-per-ray-worker $CPUS_PER_WORKER \
    --json-keys text \
    --append-eod"

# --json-keys blob_id \

echo "Executing command:"
echo "$CMD"

bash -c "$CMD"

echo "Job finished."