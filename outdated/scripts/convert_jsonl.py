import pandas as pd
import os
import argparse
import gc
from tqdm import tqdm


def convert_batch_to_jsonl(input_dir, output_dir, batch_size=10, start_batch=0, input_format="parquet"):
    """
    Converts files in batches to avoid memory issues.
    Processes only 'batch_size' files at a time.
    Supports Parquet (.parquet) and Arrow (.arrow) inputs.
    Output files are organized under the output directory using only the first
    subdirectory level from the input path (e.g., Code/jsonl/Java/filename.jsonl).
    """
    supported_formats = {
        "parquet": ".parquet",
        "arrow": ".arrow",
    }

    if input_format not in supported_formats:
        print(
            f"Error: Unsupported input format '{input_format}'. Supported formats: {', '.join(sorted(supported_formats))}"
        )
        return False

    file_extension = supported_formats[input_format]
    file_extension_lower = file_extension.lower()
    pa = None
    pa_ipc = None

    if input_format == "arrow":
        try:
            import pyarrow as pa
            import pyarrow.ipc as pa_ipc
        except ImportError:
            print("Error: pyarrow is required to read .arrow files. Install it with 'pip install pyarrow'.")
            return False

    if not os.path.exists(input_dir):
        print(f"Error: Input directory not found at '{input_dir}'")
        return False
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created root output directory: {output_dir}")

    all_input_files = []
    # Collect all matching files recursively
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if filename.lower().endswith(file_extension_lower):
                all_input_files.append(os.path.join(root, filename))

    if not all_input_files:
        print(f"No {input_format.upper()} files found in '{input_dir}' or its subdirectories.")
        return False

    total_files = len(all_input_files)
    batch_start_idx = start_batch * batch_size
    batch_end_idx = min(batch_start_idx + batch_size, total_files)

    print(
        f"Processing batch {start_batch} (files {batch_start_idx} to {batch_end_idx-1} of {total_files}) for {input_format.upper()} inputs..."
    )

    batch_files = all_input_files[batch_start_idx:batch_end_idx]
    
    for input_file_path in tqdm(batch_files, desc="Converting batch"):
        relative_path = os.path.relpath(input_file_path, input_dir)

        # Preserve only the top-level subdirectory (e.g., Java/, Cpp/) in outputs
        first_level_dir = relative_path.split(os.sep, 1)[0] if os.sep in relative_path else ""

        if first_level_dir and first_level_dir != os.curdir:
            output_jsonl_dir = os.path.join(output_dir, first_level_dir)
        else:
            output_jsonl_dir = output_dir

        if not os.path.exists(output_jsonl_dir):
            os.makedirs(output_jsonl_dir)

        base_filename = os.path.basename(input_file_path)
        output_jsonl_filename = os.path.splitext(base_filename)[0] + ".jsonl"
        output_jsonl_path = os.path.join(output_jsonl_dir, output_jsonl_filename)

        try:
            if input_format == "parquet":
                df = pd.read_parquet(input_file_path)
            else:
                try:
                    with pa_ipc.open_file(input_file_path) as reader:
                        table = reader.read_all()
                except (pa.lib.ArrowInvalid, OSError, ValueError):
                    with pa_ipc.open_stream(input_file_path) as reader:
                        table = reader.read_all()

                df = table.to_pandas()
                del table
            df.to_json(output_jsonl_path, orient='records', lines=True)
            
            # Explicitly delete the dataframe and force garbage collection
            del df
            gc.collect()
                    
        except Exception as e:
            tqdm.write(f"Failed to convert {input_file_path}: {e}")

    # Return True if there are more batches to process
    if batch_end_idx < total_files:
        print(f"Batch {start_batch} complete. {total_files - batch_end_idx} files remaining.")
        return True
    else:
        print("All files converted successfully!")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert Parquet or Arrow files to JSONL format in batches"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing files to convert"
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
    parser.add_argument(
        "--input-format",
        choices=["parquet", "arrow"],
        default="parquet",
        help="Input file format to convert (default: parquet)"
    )
    
    args = parser.parse_args()
    
    has_more = convert_batch_to_jsonl(
        args.input,
        args.output,
        batch_size=args.batch_size,
        start_batch=args.batch,
        input_format=args.input_format,
    )
    
    if has_more:
        print(f"\nTo process the next batch, run:")
        print(
            f"python {os.path.basename(__file__)} --input {args.input} --output {args.output} "
            f"--batch-size {args.batch_size} --batch {args.batch + 1} --input-format {args.input_format}"
        )