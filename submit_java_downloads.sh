#!/bin/bash

# Java has 11 original shards (0-10)
# We adapt the number of sub-shards per shard based on desired examples per job

set -euo pipefail

language="Java"
num_shards=${NUM_SHARDS:-11}
target_examples_per_job=${TARGET_EXAMPLES_PER_JOB:-120000}

source /p/project/projectnucleus/mahadik1/tvenv2/bin/activate

echo "Fetching dataset size for ${language}..."
total_examples=$(python - <<'PY'
from datasets import load_dataset_builder

builder = load_dataset_builder("HuggingFaceTB/stack-edu", "Java")
print(builder.info.splits["train"].num_examples)
PY
)

if [[ -z "${total_examples}" ]]; then
  echo "Failed to determine total example count" >&2
  exit 1
fi

# Determine jobs so that each processes <= target_examples_per_job
total_jobs=$(( (total_examples + target_examples_per_job - 1) / target_examples_per_job ))
if (( total_jobs < num_shards )); then
  total_jobs=${num_shards}
fi

sub_shards_per_shard=$(( (total_jobs + num_shards - 1) / num_shards ))
total_jobs=$(( num_shards * sub_shards_per_shard ))

avg_examples_per_job=$(( (total_examples + total_jobs - 1) / total_jobs ))

echo "Scheduling ${total_jobs} jobs (~${avg_examples_per_job} examples/job, target ${target_examples_per_job})"
echo "Using ${sub_shards_per_shard} sub-shards per shard across ${num_shards} shards"

for ((shard=0; shard<num_shards; shard++)); do
  for ((subshard=0; subshard<sub_shards_per_shard; subshard++)); do
    tmp_script="download_java_${shard}_${subshard}.sh"
    job_idx=$((shard * sub_shards_per_shard + subshard + 1))

    # Create a temporary script with the correct shard and subshard values
    sed -e "s/--shard 0/--shard ${shard}/" \
        -e "s/--subshard 0/--subshard ${subshard}/" \
        -e "s/--total-subshards 5/--total-subshards ${sub_shards_per_shard}/" \
        -e "s/--num-shards 11/--num-shards ${num_shards}/" \
        download_java.sh > "$tmp_script"

    echo "Submitting Java shard ${shard}, sub-shard ${subshard} (job ${job_idx}/${total_jobs})..."
    sbatch "$tmp_script"
    rm "$tmp_script"

    # Add small delay to reduce cache conflicts
    sleep 2
  done
done

echo "Submitted ${total_jobs} Java download jobs"
