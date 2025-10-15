import pandas as pd
import os
from tqdm import tqdm
# import json # Not strictly needed if using df.to_json(lines=True) directly,
              # but fine to keep if you have other JSON needs.

def convert_directory_parquet_to_jsonl(input_dir, output_dir):
    """
    Converts all Parquet files in an input directory to JSONL files in an output directory,
    with a progress bar.
    Uses the more efficient pandas.DataFrame.to_json(orient='records', lines=True) method.
    """
    if not os.path.exists(input_dir):
        print(f"Error: Input directory not found at '{input_dir}'")
        return
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    parquet_files = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]

    print(f"Starting conversion of {len(parquet_files)} Parquet files from '{input_dir}' to '{output_dir}'...")
    for filename in tqdm(parquet_files, desc="Converting files"):
        input_parquet_path = os.path.join(input_dir, filename)
        output_jsonl_filename = filename.replace(".parquet", ".jsonl")
        output_jsonl_path = os.path.join(output_dir, output_jsonl_filename)

        try:
            df = pd.read_parquet(input_parquet_path)
            df.to_json(output_jsonl_path, orient='records', lines=True, ensure_ascii=False)
                    
        except Exception as e:
            tqdm.write(f"Failed to convert {filename}: {e}")

    print("All directory conversion tasks complete!")

if __name__ == "__main__":
    input_directory = "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/infiwebmath-3plus"
    output_directory = "/p/project/projectnucleus/juwelsbooster/infiwebmath-3plus/"    

    convert_directory_parquet_to_jsonl(input_directory, output_directory)
