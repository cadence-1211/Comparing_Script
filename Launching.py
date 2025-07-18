# File: launch_comparison.py
# Purpose: A more robust version with simplified and corrected LSF syntax.
import os
import subprocess
import time

# --- (Sharding functions are unchanged) ---
def get_instance_key(line, key_cols):
    parts = line.strip().split()
    if len(parts) <= max(key_cols): return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    print(f"-> Processing {input_file}...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_files = [open(os.path.join(output_dir, f"{os.path.basename(input_file)}_shard_{i}.txt"), "w") for i in range(num_shards)]
    with open(input_file, "r", errors='ignore') as f:
        line_count = 0
        for line in f:
            line_count += 1
            if line_count % 5000000 == 0:
                print(f"   ...processed {line_count // 1000000}M lines")
            if not line.strip() or line.strip().startswith("#"): continue
            key = get_instance_key(line, key_cols)
            if key is None: continue
            shard_index = hash(key) % num_shards
            output_files[shard_index].write(line)
    for file_handle in output_files: file_handle.close()
    print(f"-> Finished sharding {input_file}.")

def main():
    """Guides the user, shards files, and submits LSF jobs with corrected syntax."""
    start_time = time.time()
    
    print("--- LSF Comparison Job Launcher (Fully Automated) ---")
    
    # --- Part 1: Information Gathering ---
    print("\n--- Part 1: Information Gathering ---")
    file1 = input("Enter path to first file: ")
    instcol1_str = input(f"Enter instance key column(s) for {os.path.basename(file1)}: ")
    valcol1 = input(f"Enter value column for {os.path.basename(file1)}: ")
    file2 = input("Enter path to second file: ")
    instcol2_str = input(f"Enter instance key column(s) for {os.path.basename(file2)}: ")
    valcol2 = input(f"Enter value column for {os.path.basename(file2)}: ")
    shards = int(input("How many parallel jobs do you want to run?: "))
    
    comparison_type = ''
    while comparison_type not in ['numeric', 'string']:
        comparison_type = input("Enter comparison type ('numeric' or 'string'): ").lower().strip()

    # --- Part 2: Sharding Files ---
    print("\n--- Part 2: Sharding Files ---")
    shard_file(file1, list(map(int, instcol1_str.split(','))), shards, "shards")
    shard_file(file2, list(map(int, instcol2_str.split(','))), shards, "shards")
    print("✅ Sharding complete.")

    # --- Part 3: Submitting Job Array and Dependent Merge Job ---
    print("\n--- Part 3: Submitting Jobs to LSF ---")
    os.makedirs("results", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    python_command = "/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"
    # --- MODIFICATION: Using a simpler, safer job name ---
    job_array_name = "py_compare_array"
    print(f"-> Assigning job array name: {job_array_name}")

    # --- MODIFICATION: Removed quotes around the -J argument ---
    compare_command = (
        f"bsub -n 2 -R 'rusage[mem=16G]' -o 'logs/output_%I.log' -J {job_array_name}[0-{shards-1}] "
        f"\"{python_command} compare_adv.py "
        f"--file1 'shards/{os.path.basename(file1)}_shard_%I.txt' "
        f"--file2 'shards/{os.path.basename(file2)}_shard_%I.txt' "
        f"--instcol1 '{instcol1_str}' --valcol1 {valcol1} "
        f"--instcol2 '{instcol2_str}' --valcol2 {valcol2} "
        f"--output_prefix 'results/run_%I' "
        f"--comparison_type {comparison_type}\""
    )

    merge_command = (
        f"bsub -w 'done({job_array_name})' -o 'logs/merge_output.log' -J merge_{job_array_name} "
        f"\"{python_command} merge_results.py --shards {shards} --start_time {start_time}\""
    )

    try:
        print("\n--- The following commands will be executed ---")
        print(f"COMPARE JOB COMMAND:\n{compare_command}\n")
        print(f"MERGE JOB COMMAND:\n{merge_command}\n")
        
        input("Press Enter to continue and submit these jobs to LSF, or Ctrl+C to cancel...")

        print("\n-> Submitting comparison job array...")
        subprocess.run(compare_command, shell=True, check=True)
        
        print("-> Submitting dependent merge job...")
        subprocess.run(merge_command, shell=True, check=True)
        
    except subprocess.CalledProcessError as e:
        print("\n  ERROR: LSF submission failed. The `bsub` command returned an error.")
        print("  Please check the command printed above for syntax errors.")
        return
    except KeyboardInterrupt:
        print("\n\nSubmission cancelled by user.")
        return

    print("\n✅ All jobs submitted successfully!")
    print("   The LSF cluster will now manage the entire workflow.")
    print("   Your final files will appear when the entire process is complete.")

if __name__ == "__main__":
    main()
