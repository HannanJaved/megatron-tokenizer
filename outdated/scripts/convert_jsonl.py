import pandas as pd
import os
from tqdm import tqdm

def convert_directory_parquet_to_jsonl(input_dir, output_dir):
    """
    Converts all Parquet files in an input directory (and its subdirectories)
    to JSONL files
    """
    if not os.path.exists(input_dir):
        print(f"Error: Input directory not found at '{input_dir}'")
        return
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
        return

    print(f"Starting conversion of {len(all_parquet_files)} Parquet files from '{input_dir}' (including subdirectories) to '{output_dir}'...")

    for input_parquet_path in tqdm(all_parquet_files, desc="Converting files"):
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

    print("All directory conversion tasks complete!")

if __name__ == "__main__":
    input_directory = "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/infiwebmath-4plus"
    output_directory = "/p/project/projectnucleus/juwelsbooster/infiwebmath-4plus/jsonl" 

    convert_directory_parquet_to_jsonl(input_directory, output_directory)