import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_batch_parquet_to_jsonl(input_dir, output_dir, batch_size=10, start_batch=0):
    """
    Converts Parquet files in batches to avoid memory issues.
    Processes only 'batch_size' files at a time.
    """
    if not os.path.exists(input_dir):
        print(f"Error: Input directory not found at '{input_dir}'")
        return False
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created root output directory: {output_dir}")

    all_parquet_files = []
    # Collect all parquet files recursively
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith(".parquet"):
                all_parquet_files.append(os.path.join(root, filename))

    if not all_parquet_files:
        print(f"No Parquet files found in '{input_dir}' or its subdirectories.")
        return False

    total_files = len(all_parquet_files)
    batch_start_idx = start_batch * batch_size
    batch_end_idx = min(batch_start_idx + batch_size, total_files)

    print(f"Processing batch {start_batch} (files {batch_start_idx} to {batch_end_idx-1} of {total_files})...")
    
    batch_files = all_parquet_files[batch_start_idx:batch_end_idx]
    
    for input_parquet_path in tqdm(batch_files, desc="Converting batch"):
        relative_path = os.path.relpath(input_parquet_path, input_dir)
        
        output_jsonl_dir = os.path.join(output_dir, os.path.dirname(relative_path))
        
        if not os.path.exists(output_jsonl_dir):
            os.makedirs(output_jsonl_dir)

        base_filename = os.path.basename(input_parquet_path)
        output_jsonl_filename = base_filename.replace(".parquet", ".jsonl")
        output_jsonl_path = os.path.join(output_jsonl_dir, output_jsonl_filename)

        try:
            df = pd.read_parquet(input_parquet_path)
            df.to_json(output_jsonl_path, orient='records', lines=True)
                    
        except Exception as e:
            tqdm.write(f"Failed to convert {input_parquet_path}: {e}")

    # Return True if there are more batches to process
    if batch_end_idx < total_files:
        print(f"Batch {start_batch} complete. {total_files - batch_end_idx} files remaining.")
        return True
    else:
        print("All files converted successfully!")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert Parquet files to JSONL format in batches"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing Parquet files"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for JSONL files"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of files to process per batch (default: 10)"
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=0,
        help="Batch number to start from (default: 0)"
    )
    
    args = parser.parse_args()
    
    has_more = convert_batch_parquet_to_jsonl(
        args.input, 
        args.output, 
        batch_size=args.batch_size,
        start_batch=args.batch
    )
    
    if has_more:
        print(f"\nTo process the next batch, run:")
        print(f"python {os.path.basename(__file__)} --input {args.input} --output {args.output} --batch-size {args.batch_size} --batch {args.batch + 1}")