import pandas as pd
import os
import argparse
from tqdm import tqdm

def convert_single_parquet_to_jsonl(input_file, output_file=None):
    """
    Converts a single Parquet file to JSONL format.
    
    Args:
        input_file (str): Path to the input Parquet file
        output_file (str, optional): Path to the output JSONL file. 
                                   If None, uses input filename with .jsonl extension
    """
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        return False
    
    if not input_file.endswith('.parquet'):
        print(f"Error: Input file must be a Parquet file (.parquet extension)")
        return False
    
    # Generate output filename if not provided
    if output_file is None:
        output_file = input_file.replace('.parquet', '.jsonl')
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    try:
        print(f"Converting '{input_file}' to '{output_file}'...")
        df = pd.read_parquet(input_file)
        df.to_json(output_file, orient='records', lines=True)
        
        print(f"Successfully converted {len(df)} records to '{output_file}'")
        return True
        
    except Exception as e:
        print(f"Failed to convert {input_file}: {e}")
        return False

def convert_multiple_parquet_to_jsonl(input_files, output_files=None):
    """
    Converts multiple Parquet files to JSONL format.
    
    Args:
        input_files (list): List of paths to input Parquet files
        output_files (list, optional): List of paths to output JSONL files. 
                                     If None, uses input filenames with .jsonl extension
    """
    if not isinstance(input_files, list):
        input_files = [input_files]
    
    if output_files is None:
        output_files = [f.replace('.parquet', '.jsonl') for f in input_files]
    elif not isinstance(output_files, list):
        output_files = [output_files]
    
    if len(input_files) != len(output_files):
        print(f"Error: Number of input files ({len(input_files)}) must match number of output files ({len(output_files)})")
        return False
    
    success_count = 0
    for input_file, output_file in tqdm(zip(input_files, output_files), total=len(input_files), desc="Converting files"):
        if convert_single_parquet_to_jsonl(input_file, output_file):
            success_count += 1
    
    print(f"Successfully converted {success_count}/{len(input_files)} files")
    return success_count == len(input_files)

if __name__ == "__main__":
    # Example with multiple files
    input_files = [
        "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/finemath-3plus/train-00000-of-00128.parquet",
        "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/finemath-3plus/train-00001-of-00128.parquet",
        "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/finemath-3plus/train-00003-of-00128.parquet",
        "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/finemath-3plus/train-00004-of-00128.parquet",
        "/p/data1/datasets/mmlaion/language/raw/HuggingFaceTB-finemath/finemath-3plus/train-00005-of-00128.parquet"
    ]
    output_files = [
        "/p/project/projectnucleus/juwelsbooster/finemath-3plus/jsonl/train-00000-of-00128.jsonl",
        "/p/project/projectnucleus/juwelsbooster/finemath-3plus/jsonl/train-00001-of-00128.jsonl",
        "/p/project/projectnucleus/juwelsbooster/finemath-3plus/jsonl/train-00003-of-00128.jsonl",
        "/p/project/projectnucleus/juwelsbooster/finemath-3plus/jsonl/train-00004-of-00128.jsonl",
        "/p/project/projectnucleus/juwelsbooster/finemath-3plus/jsonl/train-00005-of-00128.jsonl"
    ]

    convert_multiple_parquet_to_jsonl(input_files, output_files)
