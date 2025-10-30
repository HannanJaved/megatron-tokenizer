import os

def check_missing_files(folder_path, total_files=32, prefix="train", suffix=".jsonl"):
    """
    Check which files are missing from a sequence of numbered files.
    
    Args:
        folder_path (str): Path to the folder containing the files
        total_files (int): Total number of expected files
        prefix (str): File prefix (default: "train")
        suffix (str): File suffix (default: ".jsonl")
    """
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found at '{folder_path}'")
        return
    
    # Get all files in the folder
    existing_files = set(os.listdir(folder_path))
    
    # Generate expected filenames
    expected_files = []
    missing_files = []
    present_files = []
    
    for i in range(total_files):
        expected_filename = f"{prefix}-{i:05d}-of-{total_files:05d}{suffix}"
        expected_files.append(expected_filename)
        
        if expected_filename in existing_files:
            present_files.append(expected_filename)
        else:
            missing_files.append(expected_filename)
    
    # Print results
    print(f"Checking folder: {folder_path}")
    print(f"Expected files: {total_files}")
    print(f"Present files: {len(present_files)}")
    print(f"Missing files: {len(missing_files)}")
    
    if missing_files:
        print("\nMissing files:")
        for file in sorted(missing_files):
            print(f"  - {file}")
    else:
        print("\nAll files are present!")
    
    # Check for unexpected files
    unexpected_files = [f for f in existing_files if f not in expected_files and f.endswith(suffix)]
    if unexpected_files:
        print(f"\nUnexpected files found ({len(unexpected_files)}):")
        for file in sorted(unexpected_files):
            print(f"  - {file}")

if __name__ == "__main__":
    folder_path = "/p/project/projectnucleus/juwelsbooster/infiwebmath-3plus/jsonl"
    check_missing_files(folder_path)
