import gzip
import requests
from datasets import load_dataset, Dataset
import argparse
import os

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

def process_sub_shard(language, shard_idx, sub_shard_idx, total_sub_shards, output_dir, num_proc=16):
    """Process a sub-shard of a parquet file by further splitting it"""
    
    print(f"Processing {language} shard {shard_idx}, sub-shard {sub_shard_idx}/{total_sub_shards}...")
    
    # Calculate the percentage range for this sub-shard
    # Each original shard is 1%, we're splitting that further
    start_pct = shard_idx + (sub_shard_idx / total_sub_shards)
    end_pct = shard_idx + ((sub_shard_idx + 1) / total_sub_shards)
    
    print(f"Loading data from {start_pct:.2f}% to {end_pct:.2f}%...")
    
    # Load specific sub-shard
    ds = load_dataset(
        "HuggingFaceTB/stack-edu", 
        language,
        split=f"train[{start_pct}%:{end_pct}%]",
        num_proc=num_proc
    )
    
    print(f"Loaded {len(ds)} examples from shard {shard_idx}, sub-shard {sub_shard_idx}")
    
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
    parser.add_argument("--output_dir", type=str, default="/p/data1/datasets/mmlaion/language/raw/stack-edu/Code/", help="Output directory")
    parser.add_argument("--num_proc", type=int, default=16, help="Number of processes for parallel downloading")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Process the sub-shard
    success_count, total_count = process_sub_shard(
        args.language, 
        args.shard,
        args.subshard,
        args.total_subshards,
        args.output_dir,
        args.num_proc
    )
    
    print(f"\n=== Summary ===")
    print(f"Language: {args.language}")
    print(f"Shard: {args.shard}, Sub-shard: {args.subshard}/{args.total_subshards}")
    print(f"Success rate: {success_count}/{total_count} ({100*success_count/total_count:.1f}%)")

if __name__ == "__main__":
    main()
