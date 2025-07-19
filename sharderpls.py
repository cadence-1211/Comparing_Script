#!/usr/bin/env python3
# sharder.py
import os
import sys

# --- Helper functions for user input ---

def ask_for_file(prompt):
    """Continuously asks for a file path until a valid one is given."""
    while True:
        path = input(prompt).strip()
        if os.path.exists(path):
            return path
        else:
            print(f"❌ Error: File not found at '{path}'. Please try again.")

def ask_for_columns(prompt):
    """Continuously asks for column numbers until valid input is given."""
    while True:
        try:
            col_str = input(prompt).strip()
            # Convert comma-separated string to a list of integers
            cols = list(map(int, col_str.split(',')))
            if any(c < 0 for c in cols):
                print("❌ Error: Column numbers cannot be negative.")
                continue
            return cols
        except ValueError:
            print("❌ Error: Invalid input. Please enter a number (e.g., 0) or comma-separated numbers (e.g., 0,1).")

# --- Core sharding logic (Unchanged) ---

def get_instance_key(line, key_cols):
    """Extracts the key from a line."""
    parts = line.strip().split()
    if len(parts) <= max(key_cols):
        return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    """Reads a large file and splits it into smaller shards based on a key."""
    print(f"Processing {input_file}...")
    
    output_files = [open(os.path.join(output_dir, f"{os.path.basename(input_file)}_shard_{i}.txt"), "w") for i in range(num_shards)]
    
    with open(input_file, "r", errors='ignore') as f:
        for line in f:
            if not line.strip() or line.strip().startswith("#"):
                continue
            key = get_instance_key(line, key_cols)
            if key is None:
                continue
            shard_index = hash(key) % num_shards
            output_files[shard_index].write(line)
            
    for file_handle in output_files:
        file_handle.close()
    print(f"Finished sharding {input_file}.")

def main():
    print("--- Interactive File Sharder ---")
    print("This script will split two large files into smaller matching chunks.")
    
    # NEW: Interactive prompts instead of argparse
    file1 = ask_for_file("➡️ Enter the path to the first large file (e.g., file1.txt): ")
    instcol1 = ask_for_columns(f"➡️ Enter the instance key column(s) for '{os.path.basename(file1)}' (0-based index): ")
    
    print("-" * 30)
    
    file2 = ask_for_file("➡️ Enter the path to the second large file (e.g., file2.txt): ")
    instcol2 = ask_for_columns(f"➡️ Enter the instance key column(s) for '{os.path.basename(file2)}' (0-based index): ")
    
    num_shards = 5  # Keeping this fixed for simplicity

    output_dir = "shards"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print(f"\n✅ Creating output directory: '{output_dir}'")
    
    shard_file(file1, instcol1, num_shards, output_dir)
    shard_file(file2, instcol2, num_shards, output_dir)

    print("\nSharding complete!")

if __name__ == "__main__":
    main()
