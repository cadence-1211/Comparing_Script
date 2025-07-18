# run_master_comparison.py

import os
import sys
import argparse
import time
import subprocess
import re
import csv
import multiprocessing
from textwrap import dedent

# --- Configuration ---
LSF_PYTHON_PATH = "/grid/common/pkgs/python/v3.7.2/bin/python3.7"
BJOBS_POLL_INTERVAL = 30

# --- Part 0: Interactive User Input Functions ---

def get_file_path(prompt_text, file_arg):
    """Prompts for a file path until a valid file is entered."""
    if file_arg:
        if not os.path.exists(file_arg):
            print(f"❌ Error: File provided via command line not found: {file_arg}")
            sys.exit(1)
        print(f"✓ Using file: {file_arg}")
        return file_arg
    
    while True:
        path = input(f"▶ {prompt_text}: ").strip()
        if os.path.exists(path) and os.path.isfile(path):
            return path
        print("  ❌ File not found. Please enter a valid path.")

def get_column_indices(prompt_text, col_arg):
    """Prompts for one or more column indices, validating the input."""
    if col_arg:
        try:
            list(map(int, col_arg.split(',')))
            print(f"✓ Using instance column(s): {col_arg}")
            return col_arg
        except ValueError:
            print(f"❌ Error: Invalid format for instance columns: '{col_arg}'. Must be comma-separated integers.")
            sys.exit(1)

    while True:
        cols = input(f"▶ {prompt_text}: ").strip()
        try:
            list(map(int, cols.split(',')))
            return cols
        except ValueError:
            print("  ❌ Invalid format. Please enter one or more numbers, separated by commas (e.g., '0' or '0,1').")

def get_single_column_index(prompt_text, col_arg):
    """Prompts for a single column index."""
    if col_arg is not None:
        print(f"✓ Using value column: {col_arg}")
        return col_arg

    while True:
        col = input(f"▶ {prompt_text}: ").strip()
        try:
            return int(col)
        except ValueError:
            print("  ❌ Invalid input. Please enter a single number (e.g., '3').")

def get_choice(prompt_text, choices, choice_arg):
    """Prompts user to select from a list of choices."""
    if choice_arg:
        if choice_arg in choices:
            print(f"✓ Using comparison type: {choice_arg}")
            return choice_arg
        else:
            print(f"❌ Error: Invalid choice for comparison type: '{choice_arg}'. Must be one of {choices}.")
            sys.exit(1)
            
    while True:
        choice = input(f"▶ {prompt_text} {choices}: ").strip().lower()
        if choice in choices:
            return choice
        print(f"  ❌ Invalid selection. Please choose from: {', '.join(choices)}.")

def get_integer(prompt_text, default_val, int_arg):
    """Prompts user for an integer, with a default value."""
    if int_arg is not None:
        print(f"✓ Using {prompt_text.split(' ')[1]}: {int_arg}")
        return int_arg

    while True:
        val_str = input(f"▶ {prompt_text} [default: {default_val}]: ").strip()
        if not val_str:
            return default_val
        try:
            return int(val_str)
        except ValueError:
            print("  ❌ Invalid input. Please enter a whole number.")

# --- Helper Function: Line Counting ---
def count_lines(filepath):
    try:
        with open(filepath, 'rb') as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Could not count lines in {filepath}: {e}")
        return 0

# --- Part 1: Sharding Logic ---
def get_instance_key(line, key_cols):
    parts = line.strip().split()
    if len(parts) <= max(key_cols): return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    print(f"  -> Sharding {input_file}...")
    try:
        output_files = [open(os.path.join(output_dir, f"{os.path.basename(input_file)}_shard_{i}.txt"), "w") for i in range(num_shards)]
        with open(input_file, "r", errors='ignore') as f:
            for line in f:
                if not line.strip() or line.strip().startswith("#"): continue
                key = get_instance_key(line, key_cols)
                if key is None: continue
                shard_index = hash(key) % num_shards
                output_files[shard_index].write(line)
    finally:
        for file_handle in output_files:
            file_handle.close()
    print(f"  -> Finished sharding {input_file}.")

# --- Part 2: Generating the Comparison Script ---
def create_comparator_script(script_path):
    comparator_code = dedent(f'''
    #!/usr/bin/env python3
    # DO NOT EDIT: This script is generated automatically
    import argparse, os, time, sys, mmap, csv, multiprocessing, re
    NUMERIC_RE = re.compile(r"[-+]?\\d*\\.?\\d+(?:[eE][-+]?\\d+)?")
    METADATA_KEYWORDS_SET = {{b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN", b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET", b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD", b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"}}
    def extract_value(value_bytes, comparison_type):
        value_str = value_bytes.decode('utf-8', errors='ignore').strip()
        if comparison_type == 'numeric':
            match = NUMERIC_RE.search(value_str)
            if match:
                try: return float(match.group(0))
                except (ValueError, TypeError): return value_str
            else: return value_str
        else: return value_str
    def is_valid_instance_line(line):
        line = line.strip()
        if not line or line.startswith(b"#"): return False
        for keyword in METADATA_KEYWORDS_SET:
            if line.startswith(keyword): return False
        return True
    def parse_file_with_mmap(file_path, inst_cols, value_col, comparison_type):
        data, instances_set = {{}}, set()
        with open(file_path, "rb") as f:
            mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            for line in iter(mmapped_file.readline, b""):
                if not is_valid_instance_line(line): continue
                parts = line.strip().split()
                if len(parts) <= max(inst_cols + [value_col]): continue
                try:
                    key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                    val_raw = parts[value_col].decode('utf-8', errors='ignore').strip()
                    val_parsed = extract_value(parts[value_col], comparison_type)
                    data[key] = (val_raw, val_parsed)
                    instances_set.add(key)
                except IndexError: continue
            mmapped_file.close()
        return data, instances_set
    def parse_file_worker(args_tuple): return parse_file_with_mmap(*args_tuple)
    def compare_instances(data1, data2, instances1, instances2):
        missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
        missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
        matched = sorted(list(instances1 & instances2))
        return missing_in_file2, missing_in_file1, matched
    def write_missing_file(f1, f2, m2, m1, out):
        with open(out, "w") as o:
            if m2: o.writelines([f"{{'='*60}}\\n",f"Instances from '{{f1}}' missing in '{{f2}}':\\n",f"{{'='*60}}\\n"] + [f"{{' | '.join(i)}}\\n" for i in m2])
            if m1: o.writelines([f"\\n{{'='*60}}\\n",f"Instances from '{{f2}}' missing in '{{f1}}':\\n",f"{{'='*60}}\\n"] + [f"{{' | '.join(i)}}\\n" for i in m1])
    def write_comparison_csv(f1, f2, d1, d2, m, c1, c2, out, comp_type):
        if not m: return
        with open(out, "w", newline="") as csvfile:
            w = csv.writer(csvfile)
            h = [f"Instance_Key_{{i+1}}" for i in range(len(m[0]))] + [f"{{f1}}_{{c1}}", f"{{f2}}_{{c2}}", "Difference", "Result"]
            w.writerow(h)
            for inst in m:
                r1,v1 = d1.get(inst,(None,None)); r2,v2 = d2.get(inst,(None,None))
                if comp_type == 'numeric' and isinstance(v1,float) and isinstance(v2,float):
                    diff=v1-v2
                    res=f"{{abs((diff/v2)*100):.2f}}%" if v2!=0 else "Infinite %" if v1!=0 else "0.00%"
                    w.writerow(list(inst)+[r1,r2,f"{{diff:.4e}}",res])
                else: w.writerow(list(inst)+[str(r1),str(r2),"N/A","MATCH" if str(v1)==str(v2) else "MISMATCH"])
    def main():
        p=argparse.ArgumentParser(); p.add_argument("--file1", required=True); p.add_argument("--instcol1", required=True); p.add_argument("--valcol1", type=int, required=True); p.add_argument("--file2", required=True); p.add_argument("--instcol2", required=True); p.add_argument("--valcol2", type=int, required=True); p.add_argument("--output_prefix", required=True); p.add_argument("--comparison_type", required=True)
        a=p.parse_args(); t0=time.time(); i1=list(map(int,a.instcol1.split(','))); i2=list(map(int,a.instcol2.split(','))); n1,n2=os.path.basename(a.file1),os.path.basename(a.file2)
        with multiprocessing.Pool(2) as pool: res=pool.map(parse_file_worker,[(a.file1,i1,a.valcol1,a.comparison_type),(a.file2,i2,a.valcol2,a.comparison_type)])
        d1,s1=res[0]; d2,s2=res[1]; m2,m1,matched=compare_instances(d1,d2,s1,s2)
        write_missing_file(n1,n2,m2,m1,f"{{a.output_prefix}}_missing_instances.txt")
        write_comparison_csv(n1,n2,d1,d2,matched,"Value","Value",f"{{a.output_prefix}}_comparison.csv",a.comparison_type)
        print(f"**JOB_SUCCESS** Run {{a.output_prefix}} finished in {{time.time()-t0:.2f}} seconds.")
    if __name__ == "__main__": main()
    ''')
    with open(script_path, "w") as f:
        f.write(comparator_code)
    os.chmod(script_path, 0o755)

# --- Part 3: LSF Job Submission and Monitoring ---
def submit_and_monitor_jobs(args):
    job_ids = {}; job_id_pattern = re.compile(r"Job <(\d+)> is submitted")
    print("\n[STEP 3/4] Submitting comparison jobs to LSF...")
    for i in range(args.shards):
        output_prefix = os.path.join(args.results_dir, f"run_{i}")
        log_file = os.path.join(args.logs_dir, f"output_{i}.log")
        cmd = ["bsub", "-n", str(args.cores), "-R", f"rusage[mem={args.mem}]", "-o", log_file, f"{LSF_PYTHON_PATH}", args.comparator_script, "--file1", f"{args.shards_dir}/file1.txt_shard_{i}.txt", "--file2", f"{args.shards_dir}/file2.txt_shard_{i}.txt", "--instcol1", args.instcol1, "--valcol1", str(args.valcol1), "--instcol2", args.instcol2, "--valcol2", str(args.valcol2), "--output_prefix", output_prefix, "--comparison_type", args.comparison_type]
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            match = job_id_pattern.search(result.stdout)
            if match:
                job_ids[match.group(1)] = i
                print(f"  -> Submitted job {match.group(1)} for shard {i}")
            else:
                print(f"⚠️ Warning: Could not parse job ID for shard {i}. Monitoring may be incomplete.\n{result.stdout}")
        except FileNotFoundError:
            print("❌ Error: 'bsub' command not found. Are you on a machine with LSF installed?"); sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error submitting job for shard {i}:\n{e.stderr}"); sys.exit(1)

    print(f"\n[STEP 4/4] Monitoring submitted jobs... (updates every {BJOBS_POLL_INTERVAL}s)")
    finished_jobs = {}
    try:
        while len(job_ids) > 0:
            time.sleep(BJOBS_POLL_INTERVAL)
            p = subprocess.run(['bjobs', '-o', 'jobid stat', '-noheader'] + list(job_ids.keys()), capture_output=True, text=True)
            finished_id_list = [job_id for job_id in job_ids if job_id not in p.stdout]
            for job_id in finished_id_list:
                shard_index = job_ids.pop(job_id)
                log_file = os.path.join(args.logs_dir, f"output_{shard_index}.log")
                runtime = "ERROR"
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        match = re.search(r"\*\*JOB_SUCCESS\*\*.*?finished in ([\d.]+) seconds", content)
                        if match: runtime = float(match.group(1))
                    print(f"  -> ✅ Job {job_id} (Shard {shard_index}) finished in {runtime:.2f}s.")
                except Exception:
                    print(f"  -> ❌ Job {job_id} (Shard {shard_index}) finished with an error. Check log: {log_file}")
                finished_jobs[shard_index] = runtime
            if job_ids: print(f"  ... {len(job_ids)} jobs still running. Waiting...")
    except KeyboardInterrupt:
        print("\n🛑 Monitoring interrupted. Rerun script with same args to merge results later."); sys.exit(0)
    print("\nAll LSF jobs complete!")
    return finished_jobs

# --- Part 4: Merging Results and Reporting ---
def merge_and_report(args, job_runtimes):
    print("\n[FINAL] Merging result files...")
    final_csv_path = "final_comparison.csv"; first_file_found = False
    with open(final_csv_path, "w", newline='') as f_out:
        writer = csv.writer(f_out)
        for i in range(args.shards):
            shard_csv = os.path.join(args.results_dir, f"run_{i}_comparison.csv")
            if os.path.exists(shard_csv) and os.path.getsize(shard_csv) > 0:
                with open(shard_csv, "r") as f_in:
                    reader = csv.reader(f_in)
                    if not first_file_found:
                        writer.writerows(reader)
                        first_file_found = True
                    else:
                        next(reader) # Skip header
                        writer.writerows(reader)
    print(f"  -> Merged comparison data into '{final_csv_path}'")
    
    final_missing_path = "final_missing_instances.txt"
    with open(final_missing_path, "w") as f_out:
        for i in range(args.shards):
            shard_missing = os.path.join(args.results_dir, f"run_{i}_missing_instances.txt")
            if os.path.exists(shard_missing):
                with open(shard_missing, "r") as f_in: f_out.write(f_in.read() + "\n")
    print(f"  -> Merged missing instances data into '{final_missing_path}'")

    print("\n" + "="*50 + "\n" + " " * 18 + "FINAL REPORT" + "\n" + "="*50)
    print("\n[Input File Statistics]")
    print(f"  - Lines in '{os.path.basename(args.file1)}': {args.file1_lines}")
    print(f"  - Lines in '{os.path.basename(args.file2)}': {args.file2_lines}")
    print("\n[Individual Job Runtimes]")
    for i in sorted(job_runtimes.keys()):
        runtime = job_runtimes[i]
        print(f"  - Shard {i}: {runtime:.2f} seconds" if isinstance(runtime, float) else f"  - Shard {i}: ERROR")
    print("\n[Output File Statistics]")
    data_lines = count_lines(final_csv_path) - 1 if count_lines(final_csv_path) > 0 else 0
    print(f"  - Matched instances (data lines in final_comparison.csv): {data_lines}")
    with open(final_missing_path, 'r') as f: missing_count = sum(1 for line in f if line.strip() and not line.strip().startswith('='))
    print(f"  - Missing instances (in final_missing_instances.txt): {missing_count}")

# --- Main Execution Logic ---
def main():
    parser = argparse.ArgumentParser(description="Interactive script to compare large files on an LSF cluster.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--file1", help="Path to the first file.")
    parser.add_argument("--instcol1", help="Instance key column(s) for file 1 (0-based, comma-separated).")
    parser.add_argument("--valcol1", type=int, help="Value column for file 1 (0-based).")
    parser.add_argument("--file2", help="Path to the second file.")
    parser.add_argument("--instcol2", help="Instance key column(s) for file 2.")
    parser.add_argument("--valcol2", type=int, help="Value column for file 2.")
    parser.add_argument("--comparison_type", choices=['numeric', 'string'], help="Type of comparison ('numeric' or 'string').")
    parser.add_argument("--shards", type=int, help="Number of shards to split files into.")
    parser.add_argument("--mem", type=str, default="8G", help="Memory for each LSF job (e.g., '8G').")
    parser.add_argument("--cores", type=int, help="Cores for each LSF job.")
    raw_args = parser.parse_args()

    # --- Interactive Mode ---
    print("--- Configuration ---")
    print("Fill in the details below, or press Enter to accept defaults.")
    
    args = argparse.Namespace() # Create a new namespace to hold final validated args
    
    args.file1 = get_file_path("Enter path for file 1", raw_args.file1)
    args.instcol1 = get_column_indices("Enter instance key column(s) for file 1 (e.g., 0 or 0,1)", raw_args.instcol1)
    args.valcol1 = get_single_column_index("Enter value column for file 1 (e.g., 3)", raw_args.valcol1)
    
    print("-" * 20)
    
    args.file2 = get_file_path("Enter path for file 2", raw_args.file2)
    args.instcol2 = get_column_indices("Enter instance key column(s) for file 2 (e.g., 0 or 0,1)", raw_args.instcol2)
    args.valcol2 = get_single_column_index("Enter value column for file 2 (e.g., 3)", raw_args.valcol2)
    
    print("-" * 20)

    args.comparison_type = get_choice("Enter comparison type", ['numeric', 'string'], raw_args.comparison_type)
    args.shards = get_integer("Number of parallel jobs (shards)", 5, raw_args.shards)
    args.cores = get_integer("Number of cores per job", 2, raw_args.cores)
    args.mem = input(f"▶ Memory per job [default: {raw_args.mem}]: ").strip() or raw_args.mem

    total_start_time = time.time()
    args.shards_dir, args.results_dir, args.logs_dir = "shards", "results", "logs"
    args.comparator_script = "compare_adv_generated.py"
    os.makedirs(args.shards_dir, exist_ok=True); os.makedirs(args.results_dir, exist_ok=True); os.makedirs(args.logs_dir, exist_ok=True)

    print("\n" + "="*50 + "\n" + " " * 10 + "Parallel File Comparison Engine" + "\n" + "="*50)
    
    print("\n[STEP 0/4] Analyzing input files...")
    args.file1_lines = count_lines(args.file1)
    args.file2_lines = count_lines(args.file2)
    print(f"  -> '{args.file1}' has {args.file1_lines} lines.")
    print(f"  -> '{args.file2}' has {args.file2_lines} lines.")

    print("\n[STEP 1/4] Generating LSF comparator script...")
    create_comparator_script(args.comparator_script)
    
    print("\n[STEP 2/4] Sharding input files...")
    instcol1_list = list(map(int, args.instcol1.split(',')))
    instcol2_list = list(map(int, args.instcol2.split(',')))
    shard_file(args.file1, instcol1_list, args.shards, args.shards_dir)
    shard_file(args.file2, instcol2_list, args.shards, args.shards_dir)
    
    job_runtimes = submit_and_monitor_jobs(args)
    merge_and_report(args, job_runtimes)

    print("\n[Total Runtime]")
    print(f"  - The entire process finished in {time.time() - total_start_time:.2f} seconds.")
    print("\n" + "="*50 + "\nDone! Your final files are 'final_comparison.csv' and 'final_missing_instances.txt'.\n" + "="*50)

if __name__ == "__main__":
    main()
