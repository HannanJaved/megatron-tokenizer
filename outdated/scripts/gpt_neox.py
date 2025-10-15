# Run this on a machine with internet access
from transformers import AutoTokenizer
import os

# Set a custom cache directory
cache_dir = "/p/project/projectnucleus/juwelsbooster/.cache"
os.environ["HF_HOME"] = cache_dir

# Download the tokenizer
tokenizer = AutoTokenizer.from_pretrained(
    "EleutherAI/gpt-neox-20b",
    cache_dir=cache_dir
)

print(f"Tokenizer cached in: {cache_dir}")