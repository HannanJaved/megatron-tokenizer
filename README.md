This is a repo to tokenize text data using Megatron-LM.  
The script has been adapted from this [doc](https://iffmd.fz-juelich.de/gXjni4omRNyttSoOEhjbjg#Tokenization)

# How to Run
1. Clone this repo and the Megatron-LM repo
2. Copy the preprocess_data_parallel.py file into the Megatron-LM directory
3. Create a venv and install the dependencies required by Megatron-LM
4. Edit and submit tokenizer.sh according to your cluster and directory path that contains files to be tokenized

Only .jsonl files are supported! If you have a .parquet file, convert them to .jsonl first by using the `convert_jsonl.py` script in the `outdated/scripts` directory

### ToDos:
- Support .parquet files directly instead of converting to .jsonl first.
