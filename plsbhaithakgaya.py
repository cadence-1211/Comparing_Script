# run_comparison.py

import argparse
import os
import sys
import subprocess
import time
import mmap
import re
import csv
import multiprocessing
from pathlib import Path

# --- Configuration: Set the default Python path for LSF jobs ---
# This path will be used in the `bsub` command.
LSF_PYTHON_EXEC = "/grid/common/pkgs/python/v3.7.2/bin/python3.7"


# ==============================================================================
# SECTION 1: HELPER FUNCTIONS (for both Controller and Worker)
# ==============================================================================

# This regex finds the first integer or float in a string.
# It handles formats like "3.14", "-.5e-3", "(45.23)", and "123km".
NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
METADATA_KEYWORDS_SET = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}

def get_instance_key(line, key_cols):
    """Extracts the composite key from a line based on specified columns."""
    parts = line.strip().split()
    if not parts or len(parts) <= max(key_cols):
        return None
    try:
        return "_".join(parts[i] for i in key_cols)
    except IndexError:
        return None

def is_valid_instance_line(line_bytes):
    """Checks if a line is a valid data line, skipping comments and metadata."""
    line_bytes = line_bytes.strip()
    if not line_bytes or line_bytes.startswith(b"#"):
        return False
    for keyword in METADATA_KEYWORDS_SET:
        if line_bytes.startswith(keyword):
            return False
    return True

def extract_value(value_bytes, comparison_type):
    """Extracts and parses the value based on the chosen comparison type."""
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()
    if comparison_type == 'numeric':
        match = NUMERIC_RE.search(value_str)
        if match:
            try:
                return float(match.group(0))
            except (ValueError, TypeError):
                return value_str  # Fallback to string if conversion fails
        else:
            return value_str  # Fallback to string if no number is found
    else:  # comparison_type is 'string'
        return value_str

# ==============================================================================
# SECTION 2: WORKER LOGIC (Code that runs on the LSF cluster)
# ==============================================================================

def parse_file_worker(args_tuple):
    """Wrapper function for multiprocessing."""
    return parse_file_with_mmap(*args_tuple)

def parse_file_with_mmap(file_path, inst_cols, value_col, comparison_type):
    """Efficiently parses a file using memory-mapping."""
    data, instances_set = {}, set()
    try:
        with open(file_path, "rb") as f:
            mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            for line in iter(mmapped_file.readline, b""):
                if not is_valid_instance_line(line):
                    continue
                parts = line.strip().split()
                if len(parts) <= max(inst_cols + [value_col]):
                    continue
                try:
                    key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                    val_parsed = extract_value(parts[value_col], comparison_type)
                    data[key] = (parts[value_col].decode('utf-8', errors='ignore'), val_parsed)
                    instances_set.add(key)
                except IndexError:
                    continue
            mmapped_file.close()
    except FileNotFoundError:
        print(f"Error: Worker could not find file {file_path}. Aborting.")
        sys.exit(1)
    return data, instances_set

def write_missing_file(file1_name, file2_name, miss2, miss1, out_filename):
    """Writes a report of instances missing from either file."""
    with open(out_filename, "w") as out:
        if miss2:
            out.write(f"{'='*60}\nInstances from '{file1_name}' missing in '{file2_name}':\n{'='*60}\n")
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        if miss1:
            out.write(f"\n{'='*60}\nInstances from '{file2_name}' missing in '{file1_name}':\n{'='*60}\n")
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, out_filename, comparison_type):
    """Writes the detailed comparison results to a CSV file."""
    if not matched:
        return 0
    lines_written = 0
    with open(out_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Instance_Key_{i+1}" for i in range(key_len)] + \
                  [f"{file1_name}_Value", f"{file2_name}_Value", "Difference", "Result"]
        writer.writerow(headers)
        
        for inst in matched:
            raw1, val1 = data1.get(inst, (None, None))
            raw2, val2 = data2.get(inst, (None, None))
            
            if comparison_type == 'numeric' and isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                if val2 != 0:
                    deviation = abs((diff / val2) * 100)
                    result = f"{deviation:.2f}%"
                else:
                    result = "Infinite %" if val1 != 0 else "0.00%"
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", result])
            else:
                match_result = "MATCH" if str(val1) == str(val2) else "MISMATCH"
                writer.writerow(list(inst) + [str(val1), str(val2), "N/A", match_result])
            lines_written += 1
    return lines_written

def run_comparison_worker(args):
    """The main function for a single comparison job on a shard."""
    t0 = time.time()
    print(f"Starting comparison worker for {args.output_prefix}...")

    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    instcol2 = list(map(int, args.instcol2.strip().split(",")))

    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, instcol1, args.valcol1, args.comparison_type),
            (args.file2, instcol2, args.valcol2, args.comparison_type)
        ])
    
    data1, instances1 = results[0]
    data2, instances2 = results[1]
    
    missing_in_file2 = sorted(list(instances1 - instances2))
    missing_in_file1 = sorted(list(instances2 - instances1))
    matched = sorted(list(instances1 & instances2))

    missing_filename = f"{args.output_prefix}_missing_instances.txt"
    comparison_filename = f"{args.output_prefix}_comparison.csv"
    
    write_missing_file(os.path.basename(args.file1), os.path.basename(args.file2), missing_in_file2, missing_in_file1, missing_filename)
    comparison_lines = write_comparison_csv(os.path.basename(args.file1), os.path.basename(args.file2), data1, data2, matched, comparison_filename, args.comparison_type)
    
    t1 = time.time()
    print(f"Worker {args.output_prefix} finished in {t1 - t0:.2f} seconds.")
    print(f"Results saved to {missing_filename} and {comparison_filename}")
    
    # Print stats for the controller to parse from the log file
    print(f"STATS:missing_in_file1={len(missing_in_file1)}")
    print(f"STATS:missing_in_file2={len(missing_in_file2)}")
    print(f"STATS:comparison_lines={comparison_lines}")


# ==============================================================================
# SECTION 3: CONTROLLER LOGIC (Code that runs on your local machine)
# ==============================================================================

def get_user_inputs():
    """Interactively prompts the user for all required inputs."""
    print("--- Please provide the details for the comparison ---")
    config = {}
    config['file1'] = Path(input("1. Enter the full path to the first file: ").strip())
    config['instcol1'] = input("2. Enter instance key column(s) for file 1 (0-based, comma-separated, e.g., 0,1): ").strip()
    config['valcol1'] = int(input("3. Enter the value column for file 1 (0-based, e.g., 3): ").strip())
    
    print("-" * 20)
    config['file2'] = Path(input("4. Enter the full path to the second file: ").strip())
    config['instcol2'] = input("5. Enter instance key column(s) for file 2 (0-based, comma-separated, e.g., 0,1): ").strip()
    config['valcol2'] = int(input("6. Enter the value column for file 2 (0-based, e.g., 3): ").strip())

    print("-" * 20)
    while 'comparison_type' not in config:
        comp_type = input("7. Choose comparison type for the value column ('numeric' or 'string'): ").strip().lower()
        if comp_type in ['numeric', 'string']:
            config['comparison_type'] = comp_type
        else:
            print("   Invalid input. Please enter 'numeric' or 'string'.")
            
    config['shards'] = int(input("8. How many parallel jobs to run on the cluster? (e.g., 5): ").strip())
    print("--- Thank you. Starting the process. ---")
    return config

def run_sharding(file_path, key_cols_str, num_shards, output_dir):
    """Splits a large file into smaller shards."""
    key_cols = list(map(int, key_cols_str.split(',')))
    
    print(f"Processing {file_path.name}...")
    line_count = 0
    
    # Create file handles for all output shard files
    output_files = [open(output_dir / f"{file_path.name}_shard_{i}.txt", "w") for i in range(num_shards)]
    
    with open(file_path, "r", errors='ignore') as f:
        for line in f:
            line_count += 1
            if not line.strip() or line.strip().startswith("#"):
                continue
            
            key = get_instance_key(line, key_cols)
            if key is None:
                continue

            shard_index = hash(key) % num_shards
            output_files[shard_index].write(line)
            
    for file_handle in output_files:
        file_handle.close()
        
    print(f"-> Found {line_count} lines and created {num_shards} shards.")
    return line_count

def submit_and_monitor_jobs(config):
    """Submits jobs to LSF and monitors them until completion."""
    Path("logs").mkdir(exist_ok=True)
    job_ids = {}

    print("\n>>> STEP 2: SUBMITTING AND MONITORING LSF JOBS...")
    for i in range(config['shards']):
        shard_file1 = f"shards/{config['file1'].name}_shard_{i}.txt"
        shard_file2 = f"shards/{config['file2'].name}_shard_{i}.txt"
        output_prefix = f"results/run_{i}"
        log_file = f"logs/output_{i}.log"

        # The command that will be executed on the cluster
        cmd = [
            'bsub', '-n', '2', '-R', 'rusage[mem=8G]', '-o', log_file,
            LSF_PYTHON_EXEC, sys.argv[0], '--worker-mode',
            '--file1', shard_file1, '--instcol1', config['instcol1'], '--valcol1', str(config['valcol1']),
            '--file2', shard_file2, '--instcol2', config['instcol2'], '--valcol2', str(config['valcol2']),
            '--output_prefix', output_prefix, '--comparison_type', config['comparison_type']
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            # Extract job ID, e.g., from "Job <12345> is submitted to queue <normal>."
            match = re.search(r"<(\d+)>", result.stdout)
            if match:
                job_id = match.group(1)
                job_ids[job_id] = {'shard': i, 'status': 'PENDING'}
                print(f"  -> Submitted job {job_id} for shard {i}")
            else:
                print(f"  -> ❌ Failed to get job ID for shard {i}. LSF Output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"  -> ❌ Error submitting job for shard {i}: {e.stderr}")
        except FileNotFoundError:
            print("❌ Error: 'bsub' command not found. Are you running on a machine with LSF installed?")
            sys.exit(1)

    print("\nAll jobs submitted. Now monitoring completion...")
    print("----------------------------------------------------")

    completed_jobs = set()
    while len(completed_jobs) < len(job_ids):
        for job_id in job_ids:
            if job_id in completed_jobs:
                continue

            try:
                # Check job status
                status_output = subprocess.run(['bjobs', '-o', 'stat', '-noheader', job_id], capture_output=True, text=True)
                status = status_output.stdout.strip()

                if status in ["DONE", "EXIT"]:
                    print(f"✅ Job {job_id} completed with status: {status}.")
                    
                    # Wait for accounting logs to update
                    time.sleep(3)
                    report = subprocess.run(['bacct', '-l', job_id], capture_output=True, text=True).stdout
                    
                    run_time_match = re.search(r"Total Requested Time\s+:\s+([\d.]+) sec", report)
                    mem_match = re.search(r"Max Memory\s+:\s+([\d.]+\s+[MKG]?B)", report)
                    
                    print(f"   - Runtime: {run_time_match.group(1) if run_time_match else 'N/A'} seconds")
                    print(f"   - Max Memory: {mem_match.group(1) if mem_match else 'N/A'}")
                    print("----------------------------------------------------")
                    
                    completed_jobs.add(job_id)

                elif not status: # Job is no longer in the active queue
                    completed_jobs.add(job_id)

            except Exception as e:
                print(f"Could not check status for job {job_id}. Assuming it's finished. Error: {e}")
                completed_jobs.add(job_id)

        if len(completed_jobs) < len(job_ids):
            time.sleep(15) # Wait before checking all jobs again

    print("All LSF jobs have finished.")

def merge_results(config):
    """Merges all partial result files into final reports."""
    print("\n>>> STEP 3: MERGING RESULTS...")
    
    # Merge CSV comparison files
    final_csv_path = Path("final_comparison.csv")
    csv_shards = sorted(list(Path("results").glob("run_*_comparison.csv")))
    
    if not csv_shards:
        print("Warning: No comparison CSV files were found in the 'results' directory.")
        final_csv_path.touch() # Create an empty file
        return
        
    with open(final_csv_path, "w") as final_csv:
        # Write the header from the first file
        with open(csv_shards[0], "r") as f:
            header = f.readline()
            final_csv.write(header)
        # Write content (including header) from the first file
        with open(csv_shards[0], "r") as f:
            f.readline() # Skip header
            final_csv.write(f.read())
        # Append content from the rest of the files, skipping their headers
        for shard_path in csv_shards[1:]:
            with open(shard_path, "r") as f:
                f.readline()  # Skip header
                final_csv.write(f.read())

    # Merge missing instances files
    final_missing_path = Path("final_missing_instances.txt")
    missing_shards = sorted(list(Path("results").glob("run_*_missing_instances.txt")))
    with open(final_missing_path, "w") as final_txt:
        for shard_path in missing_shards:
            final_txt.write(shard_path.read_text())
            final_txt.write("\n")
            
    print("Merging complete!")

def generate_final_summary(config, line_counts, total_runtime):
    """Calculates and prints the final summary of the entire run."""
    print("\n========= FINAL SUMMARY =========")
    print(f"Total execution time: {int(total_runtime // 60)} minutes, {int(total_runtime % 60)} seconds.")

    total_missing1, total_missing2, total_comparison_lines = 0, 0, 0
    
    for log_file in Path("logs").glob("output_*.log"):
        content = log_file.read_text()
        total_missing1 += sum(int(val) for val in re.findall(r"STATS:missing_in_file1=(\d+)", content))
        total_missing2 += sum(int(val) for val in re.findall(r"STATS:missing_in_file2=(\d+)", content))
        total_comparison_lines += sum(int(val) for val in re.findall(r"STATS:comparison_lines=(\d+)", content))

    print("\n--- Data Statistics ---")
    print(f"Lines in '{config['file1'].name}': {line_counts['file1']}")
    print(f"Lines in '{config['file2'].name}': {line_counts['file2']}")
    print(f"Instances from '{config['file1'].name}' missing in '{config['file2'].name}': {total_missing2}")
    print(f"Instances from '{config['file2'].name}' missing in '{config['file1'].name}': {total_missing1}")
    print(f"Data lines in final_comparison.csv: {total_comparison_lines}")

    print("\n--- Final Output Files ---")
    print(f"-> {Path('final_comparison.csv').resolve()}")
    print(f"-> {Path('final_missing_instances.txt').resolve()}")
    print("=================================")


# ==============================================================================
# SECTION 4: MAIN EXECUTION BLOCK
# ==============================================================================

def main():
    """Main entry point. Determines whether to run as Controller or Worker."""
    parser = argparse.ArgumentParser(description="A self-contained script to compare large files on an LSF cluster.")
    parser.add_argument('--worker-mode', action='store_true', help=argparse.SUPPRESS)
    # Add all worker arguments for parsing in worker mode
    parser.add_argument("--file1", help=argparse.SUPPRESS)
    parser.add_argument("--instcol1", help=argparse.SUPPRESS)
    parser.add_argument("--valcol1", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--file2", help=argparse.SUPPRESS)
    parser.add_argument("--instcol2", help=argparse.SUPPRESS)
    parser.add_argument("--valcol2", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--output_prefix", help=argparse.SUPPRESS)
    parser.add_argument("--comparison_type", help=argparse.SUPPRESS)
    
    args = parser.parse_args()

    # If '--worker-mode' is passed, this script is being run by LSF.
    if args.worker_mode:
        run_comparison_worker(args)
        return

    # Otherwise, run as the main controller.
    # --- CONTROLLER ---
    try:
        config = get_user_inputs()
        total_start_time = time.time()
        
        # Create necessary directories
        Path("shards").mkdir(exist_ok=True)
        Path("results").mkdir(exist_ok=True)

        print("\n>>> STEP 1: SHARDING FILES...")
        line_counts = {}
        line_counts['file1'] = run_sharding(config['file1'], config['instcol1'], config['shards'], Path("shards"))
        line_counts['file2'] = run_sharding(config['file2'], config['instcol2'], config['shards'], Path("shards"))

        submit_and_monitor_jobs(config)
        merge_results(config)

        total_runtime = time.time() - total_start_time
        generate_final_summary(config, line_counts, total_runtime)

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting.")
        sys.exit(1)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
