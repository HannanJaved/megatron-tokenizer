import os
import shutil
import re

# Root path containing shard/subshard directories
ROOT_DIR = "/p/data1/datasets/mmlaion/language/raw/stack-edu/Code/jsonl_data/Java"
TARGET_DIR = ROOT_DIR  # same as root

# Regex to extract shard and subshard identifiers from directory names
pattern = re.compile(r"shard_(\d+)_subshard_(\d+)")

for root, dirs, files in os.walk(ROOT_DIR):
    match = pattern.search(root)
    if not match:
        continue  # skip non-shard paths

    shard_id, subshard_id = match.groups()

    for f in files:
        if f.endswith(".jsonl"):
            src = os.path.join(root, f)
            new_name = f"shard_{shard_id}_subshard_{subshard_id}.jsonl"
            dst = os.path.join(TARGET_DIR, new_name)

            print(f"Moving {src} -> {dst}")
            shutil.move(src, dst)