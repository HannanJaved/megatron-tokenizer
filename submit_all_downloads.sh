#!/bin/bash

#!/bin/bash

# Language and shard count mapping
declare -A shards
shards=(
  [C]=2
  [CSharp]=3
  [Cpp]=4
  [Go]=1
  [Java]=11
  [JavaScript]=3
  [Markdown]=5
  [PHP]=2
  [Python]=5
  [Ruby]=1
  [Rust]=1
  [SQL]=1
  [Shell]=1
  [Swift]=1
  [TypeScript]=1
)

for lang in "${!shards[@]}"; do
  num_shards=${shards[$lang]}
  for ((i=0; i<num_shards; i++)); do
    tmp_script="download_ds_${lang}_${i}.sh"
    # Replace job-name with language in a temp script
    # Also replace the language and shard arguments in the python command
    sed "s/^#SBATCH --job-name=.*/#SBATCH --job-name=${lang}/" download_ds.sh | \
    sed "s/--language Python/--language ${lang}/" | \
    sed "s/--shard 0/--shard ${i}/" > "$tmp_script"
    sbatch "$tmp_script"
    rm "$tmp_script"
    # Add small delay to reduce cache conflicts
    sleep 2
  done
done