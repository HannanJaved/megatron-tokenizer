#!/bin/bash

# Script to submit tokenizer jobs for each subdirectory in Stack-Edu Code

# Base paths
BASE_INPUT_PATH="/p/data1/datasets/mmlaion/language/raw/stack-edu/Code/jsonl_data"
BASE_OUTPUT_PATH="/p/data1/datasets/mmlaion/mahadik1/tokenized_cosmo2/Stack-Edu"

# List of subdirectories (languages)
SUBDIRS=(
    "C"
    "CSharp"
    "Cpp"
    "Go"
    "Java"
    "JavaScript"
    "Markdown"
    "PHP"
    "Python"
    "Ruby"
    "Rust"
    "SQL"
    "Shell"
    "Swift"
    "TypeScript"
)

# Loop through each subdirectory and submit a job
for SUBDIR in "${SUBDIRS[@]}"; do
    echo "Submitting tokenizer job for: $SUBDIR"
    
    # Set input and output paths for this subdirectory
    INPUT_PATH="${BASE_INPUT_PATH}/${SUBDIR}/"
    OUTPUT_PATH="${BASE_OUTPUT_PATH}/${SUBDIR}/"
    
    # Submit the job with updated job name and paths
    sbatch --job-name="${SUBDIR}" \
           --export=INPUT_PATH="${INPUT_PATH}",OUTPUT_PATH="${OUTPUT_PATH}" \
           tokenizer_template.sh
    
    echo "Job submitted for $SUBDIR"
    echo "---"
done

echo "All tokenizer jobs submitted!"
