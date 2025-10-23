import gzip
import os
from typing import Tuple

import requests
from datasets import load_dataset, load_dataset_builder
import argparse

def download_contents(example):
    blob_id = example["blob_id"]
    url = f"https://softwareheritage.s3.amazonaws.com/content/{blob_id}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            content = gzip.decompress(response.content).decode("utf-8", errors="ignore")
            return {"text": content, "download_success": True}
        else:
            return {"text": "", "download_success": False}
    except Exception as e:
        print(f"Error downloading {blob_id}: {str(e)}")
        return {"text": "", "download_success": False}

def _compute_bounds(total_examples: int, total_pieces: int, piece_idx: int) -> Tuple[int, int]:
    """Return start and end indices (exclusive) for a piece."""

    start_idx = (piece_idx * total_examples) // total_pieces
    end_idx = ((piece_idx + 1) * total_examples) // total_pieces
    return start_idx, end_idx


def process_sub_shard(language, shard_idx, sub_shard_idx, total_sub_shards, output_dir, num_proc=16, num_shards=11):
    """Process a sub-shard of a parquet file by further splitting it"""

    print(f"Processing {language} shard {shard_idx}, sub-shard {sub_shard_idx}/{total_sub_shards}...")

    builder = load_dataset_builder("HuggingFaceTB/stack-edu", language)
    total_examples = builder.info.splits["train"].num_examples
    if not total_examples:
        raise ValueError(f"Could not determine dataset size for {language}")

    total_pieces = num_shards * total_sub_shards
    piece_idx = shard_idx * total_sub_shards + sub_shard_idx

    if piece_idx >= total_pieces:
        raise ValueError(
            f"Computed piece index {piece_idx} is out of range for {total_pieces} total pieces"
        )

    start_idx, end_idx = _compute_bounds(total_examples, total_pieces, piece_idx)

    if end_idx <= start_idx:
        print(
            f"Skipping shard {shard_idx} sub-shard {sub_shard_idx}:"
            f" empty range ({start_idx}, {end_idx}) out of {total_examples} examples"
        )
        return 0, 0

    print(
        "Loading data rows "
        f"{start_idx:,} to {end_idx:,} (piece {piece_idx + 1}/{total_pieces}, "
        f"≈{end_idx - start_idx:,} examples)..."
    )

    ds = load_dataset(
        "HuggingFaceTB/stack-edu",
        language,
        split=f"train[{start_idx}:{end_idx}]",
        num_proc=num_proc,
    )

    print(
        f"Loaded {len(ds)} examples from shard {shard_idx}, sub-shard {sub_shard_idx}"
    )
    
    # Download content
    print("Downloading content...")
    ds = ds.map(download_contents, num_proc=num_proc)
    
    # Filter successful downloads
    ds_success = ds.filter(lambda x: x['download_success'])
    print(f"Successfully downloaded {len(ds_success)} out of {len(ds)} examples")
    
    # Save this sub-shard
    output_path = os.path.join(output_dir, f"{language}", f"shard_{shard_idx}_subshard_{sub_shard_idx}")
    ds_success.save_to_disk(output_path)
    print(f"Saved to {output_path}")
    
    return len(ds_success), len(ds)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--language", type=str, required=True, help="Programming language (e.g., Java)")
    parser.add_argument("--shard", type=int, required=True, help="Original shard index to process (0-10 for Java)")
    parser.add_argument("--subshard", type=int, required=True, help="Sub-shard index within the shard (0 or 1 for 2x split)")
    parser.add_argument("--total-subshards", type=int, default=2, help="Total number of sub-shards per original shard")
    parser.add_argument("--num-shards", type=int, default=11, help="Total number of original shards for the language")
    parser.add_argument("--output_dir", type=str, default="/p/data1/datasets/mmlaion/language/raw/stack-edu/Code/", help="Output directory")
    parser.add_argument("--num_proc", type=int, default=16, help="Number of processes for parallel downloading")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, args.language), exist_ok=True)
    
    # Process the sub-shard
    success_count, total_count = process_sub_shard(
        args.language, 
        args.shard,
        args.subshard,
        args.total_subshards,
        args.output_dir,
        args.num_proc,
        args.num_shards,
    )
    
    print(f"\n=== Summary ===")
    print(f"Language: {args.language}")
    print(
        f"Shard: {args.shard}, Sub-shard: {args.subshard}/{args.total_subshards}, "
        f"Total pieces: {args.num_shards * args.total_subshards}"
    )
    print(f"Success rate: {success_count}/{total_count} ({100*success_count/total_count:.1f}%)")

if __name__ == "__main__":
    main()
