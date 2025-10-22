#!/bin/bash

# Java has 11 original shards (0-10)
# We're splitting each into 2 sub-shards to get 22 total jobs
# This should reduce download time from 4-5 hours to approximately 2-2.5 hours per job

num_shards=11
sub_shards_per_shard=4

for ((shard=0; shard<num_shards; shard++)); do
  for ((subshard=0; subshard<sub_shards_per_shard; subshard++)); do
    tmp_script="download_java_${shard}_${subshard}.sh"
    
    # Create a temporary script with the correct shard and subshard values
    sed "s/--shard 0/--shard ${shard}/" download_java.sh | \
    sed "s/--subshard 0/--subshard ${subshard}/" > "$tmp_script"
    
    echo "Submitting Java shard ${shard}, sub-shard ${subshard}..."
    sbatch "$tmp_script"
    rm "$tmp_script"
    
    # Add small delay to reduce cache conflicts
    sleep 2
  done
done

echo "Submitted $((num_shards * sub_shards_per_shard)) Java download jobs"
