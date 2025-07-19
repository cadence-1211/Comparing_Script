# compare_adv.py
import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import re

# MODIFICATION: Added an argument for output prefix
def main():
    parser = argparse.ArgumentParser(description="Compare two files, with user-defined keys and advanced value comparison.")
    parser.add_argument("--file1", help="Path to the first file.")
    parser.add_argument("--instcol1", help="Comma-separated 0-based index(es) for instance key columns in file 1.")
    parser.add_argument("--valcol1", type=int, help="0-based index for the value column in file 1.")
    parser.add_argument("--file2", help="Path to the second file.")
    parser.add_argument("--instcol2", help="Comma-separated 0-based index(es) for instance key columns in file 2.")
    parser.add_argument("--valcol2", type=int, help="0-based index for the value column in file 2.")
    # THIS IS THE ONLY NEW ARGUMENT
    parser.add_argument("--output_prefix", default="result", help="Prefix for output files (e.g., 'run_0').")
    
    args = parser.parse_args()
    
    # The rest of the script is IDENTICAL to the one you already have.
    # We just use the 'output_prefix' when saving files.
    
    if not args.file1 or not args.instcol1 or args.valcol1 is None or not args.file2 or not args.instcol2 or args.valcol2 is None:
        print("This script is intended for non-interactive LSF runs. All arguments must be provided.")
        print("Required: --file1, --instcol1, --valcol1, --file2, --instcol2, --valcol2")
        sys.exit(1)

    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    instcol2 = list(map(int, args.instcol2.strip().split(",")))

    comparison_type = 'numeric' # Assuming numeric for batch jobs, or you could pass it as an arg

    if len(instcol1) != len(instcol2):
        print("❌ Error: The number of instance key columns must be the same for both files.")
        sys.exit(1)

    t0 = time.time()
    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, instcol1, args.valcol1, comparison_type),
            (args.file2, instcol2, args.valcol2, comparison_type)
        ])
    
    data1, instances1 = results[0]
    data2, instances2 = results[1]
    
    miss2, miss1, matched = compare_instances(data1, data2, instances1, instances2)

    # MODIFICATION: Using the prefix for output filenames
    missing_filename = f"{args.output_prefix}_missing_instances.txt"
    comparison_filename = f"{args.output_prefix}_comparison.csv"
    
    write_missing_file(file1_name, file2_name, miss2, miss1, missing_filename)
    write_comparison_csv(file1_name, file2_name, data1, data2, matched, "Value1", "Value2", comparison_filename)
    
    t1 = time.time()
    print(f"Run {args.output_prefix} finished in {t1 - t0:.2f} seconds.")
    print(f"Results saved to {missing_filename} and {comparison_filename}")


# --- Helper Functions (No changes needed in these) ---
NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
METADATA_KEYWORDS_SET = {b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN", b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET", b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD", b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"}

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#"): return False
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword): return False
    return True

def extract_value(value_bytes, comparison_type):
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()
    if comparison_type == 'numeric':
        match = NUMERIC_RE.search(value_str)
        if match:
            try: return float(match.group(0))
            except (ValueError, TypeError): return value_str
        else: return value_str
    else: return value_str

def parse_file_with_mmap(file_path, inst_cols, value_col, comparison_type):
    data, instances_set = {}, set()
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

def compare_instances(data1, data2, instances1, instances2):
    missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
    missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1, out_filename):
    with open(out_filename, "w") as out:
        if miss2:
            out.writelines([f"{'='*60}\n", f"Instances from '{file1_name}' missing in '{file2_name}':\n", f"{'='*60}\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        if miss1:
            out.writelines([f"\n{'='*60}\n", f"Instances from '{file2_name}' missing in '{file1_name}':\n", f"{'='*60}\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2, out_filename):
    if not matched: return
    with open(out_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Instance_Key_{i+1}" for i in range(key_len)] + [f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Result"]
        writer.writerow(headers)
        for inst in matched:
            raw1, val1 = data1.get(inst, (None, None))
            raw2, val2 = data2.get(inst, (None, None))
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                if val2 != 0:
                    deviation = abs((diff / val2) * 100)
                    result = f"{deviation:.2f}%"
                else:
                    result = "Infinite %"
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", result])
            else:
                match_result = "MATCH" if str(val1) == str(val2) else "MISMATCH"
                writer.writerow(list(inst) + [str(val1), str(val2), "N/A", match_result])

def parse_file_worker(args_tuple):
    return parse_file_with_mmap(*args_tuple)

if __name__ == "__main__":
    main()
