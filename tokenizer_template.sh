#!/bin/bash

#SBATCH --output=logs/tokenize/Stack-Edu/%x.out
#SBATCH --error=logs/tokenize/Stack-Edu/%x.err
#SBATCH --nodes=1          
#SBATCH --ntasks-per-node=1         
#SBATCH --time=1-00:00:00               
#SBATCH --account=laionize           
#SBATCH --partition=batch           
#SBATCH --cpus-per-task=6          
#SBATCH --mem=32G          

module load CUDA
module load GCC
module load PyYAML

MEGATRON_PATH="Megatron-LM"

# Export PYTHONPATH to include Megatron-LM only
export PYTHONPATH="$(pwd)/Megatron-LM"
echo "PYTHONPATH set to: $PYTHONPATH"

# Use environment variables passed from submission script
INPUT="${INPUT_PATH}"
OUTPUT_PREFIX="${OUTPUT_PATH}"
TOKENIZER_TYPE="HuggingFaceTokenizer"
# Use the local cached tokenizer path instead of model name to avoid HF hub lookups
TOKENIZER_MODEL="/p/project1/projectnucleus/mahadik1/.cache/huggingface/models--HuggingFaceTB--cosmo2-tokenizer/snapshots/4ce2318a3628e77279c939ed6a9f3f03034402de"
CPUS_PER_WORKER=6
SCRIPT="/p/project1/projectnucleus/mahadik1/Megatron-LM/preprocess_data_parallel.py"
export HF_HUB_OFFLINE=1

source /p/project1/projectnucleus/mahadik1/.python/.tvenv/bin/activate

echo "Processing: ${SLURM_JOB_NAME}"
echo "Input path: ${INPUT}"
echo "Output prefix: ${OUTPUT_PREFIX}"

CMD="python $SCRIPT \
    --input $INPUT \
    --output-prefix $OUTPUT_PREFIX \
    --tokenizer-type $TOKENIZER_TYPE \
    --tokenizer-model $TOKENIZER_MODEL \
    --workers $SLURM_CPUS_PER_TASK \
    --cpus-per-ray-worker $CPUS_PER_WORKER \
    --json-keys text \
    --append-eod"

echo "Executing command:"
echo "$CMD"

bash -c "$CMD"

echo "Job finished."
