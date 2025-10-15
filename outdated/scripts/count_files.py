import os

def count_files(directory_path):
    """
    Count the number of files in a given directory.
    
    Args:
        directory_path (str): Path to the directory
        
    Returns:
        int: Number of files in the directory
    """
    try:
        if not os.path.exists(directory_path):
            print(f"Error: Directory '{directory_path}' does not exist.")
            return 0
        
        if not os.path.isdir(directory_path):
            print(f"Error: '{directory_path}' is not a directory.")
            return 0
        
        file_count = 0
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            if os.path.isfile(item_path):
                file_count += 1
        
        return file_count
    
    except PermissionError:
        print(f"Error: Permission denied to access '{directory_path}'.")
        return 0
    except Exception as e:
        print(f"Error: {str(e)}")
        return 0

if __name__ == "__main__":
    directory = "/p/project/projectnucleus/juwelsbooster/tokenized_datasets/finemath-3plus/temp"
    count = count_files(directory)
    print(f"\nNumber of files in '{directory}': {count}\n")
