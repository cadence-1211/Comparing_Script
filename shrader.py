# sharder.py (Interactive Version)
import os
import argparse

def get_instance_key(line, key_cols):
    """Extracts the key from a line."""
    parts = line.strip().split()
    if len(parts) <= max(key_cols):
        return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    """Reads a large file and splits it into smaller shards based on a key."""
    print(f"-> Processing {input_file}...")
    
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"-> Created output directory: '{output_dir}'")
        
    # Create file handles for all the output shard files
    output_files = [open(os.path.join(output_dir, f"{os.path.basename(input_file)}_shard_{i}.txt"), "w") for i in range(num_shards)]
    
    with open(input_file, "r", errors='ignore') as f:
        # Keep track of lines to show progress
        line_count = 0
        for line in f:
            line_count += 1
            if line_count % 5000000 == 0: # Print progress every 5 million lines
                print(f"   ...processed {line_count // 1000000}M lines of {os.path.basename(input_file)}")

            if not line.strip() or line.strip().startswith("#"):
                continue
            
            key = get_instance_key(line, key_cols)
            if key is None:
                continue

            shard_index = hash(key) % num_shards
            output_files[shard_index].write(line)
            
    for file_handle in output_files:
        file_handle.close()
    print(f"-> Finished sharding {input_file}.")


def main():
    parser = argparse.ArgumentParser(description="Shard large files interactively based on instance keys.")
    parser.add_argument("--file1", help="Path to the first large file.")
    parser.add_argument("--instcol1", help="Comma-separated key columns for file 1.")
    parser.add_argument("--file2", help="Path to the second large file.")
    parser.add_argument("--instcol2", help="Comma-separated key columns for file 2.")
    parser.add_argument("--shards", type=int, help="Number of shards to create (e.g., 5, 10, 20).")
    args = parser.parse_args()

    # --- Interactive Prompts ---
    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if not args.instcol1:
        args.instcol1 = input(f"Enter instance key column(s) for {os.path.basename(args.file1)} (e.g., 0 or 0,1): ")
    
    if not args.file2:
        args.file2 = input("Enter path to second file: ")
    if not args.instcol2:
        args.instcol2 = input(f"Enter instance key column(s) for {os.path.basename(args.file2)} (e.g., 1): ")
        
    if not args.shards:
        args.shards = int(input("How many parallel jobs do you want to run? (e.g., 5 or 10 or 20): "))
    
    print("\nStarting the sharding process...")
    output_dir = "shards"
    
    instcol1 = list(map(int, args.instcol1.split(',')))
    instcol2 = list(map(int, args.instcol2.split(',')))
    
    shard_file(args.file1, instcol1, args.shards, output_dir)
    shard_file(args.file2, instcol2, args.shards, output_dir)

    print("\n✅ Sharding complete!")
    print(f"The next step is to edit 'run_lsf.sh' with your filenames and then run it.")

if __name__ == "__main__":
    main()
