# File: compare_adv.py
# Purpose: The core comparison logic that runs on each LSF node.
import argparse
import os
import sys
import mmap
import csv
import re
import multiprocessing

# --- Pre-compiled Regex for efficiency ---
NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
METADATA_KEYWORDS_SET = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}

def is_valid_instance_line(line):
    """Checks if a line is valid data, ignoring headers and comments."""
    line = line.strip()
    if not line or line.startswith(b"#"): return False
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword): return False
    return True

def extract_value(value_bytes, comparison_type='numeric'):
    """Extracts a numeric or string value from a byte string."""
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()
    if comparison_type == 'numeric':
        match = NUMERIC_RE.search(value_str)
        if match:
            try: return float(match.group(0))
            except (ValueError, TypeError): return value_str
        else: return value_str
    else: return value_str

def parse_file_with_mmap(file_path, inst_cols, value_col):
    """Parses a file using memory-mapping for efficiency."""
    data, instances_set = {}, set()
    try:
        with open(file_path, "rb") as f:
            mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            for line in iter(mmapped_file.readline, b""):
                if not is_valid_instance_line(line): continue
                parts = line.strip().split()
                if len(parts) <= max(inst_cols + [value_col]): continue
                try:
                    key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                    val_parsed = extract_value(parts[value_col])
                    data[key] = val_parsed
                    instances_set.add(key)
                except IndexError: continue
            mmapped_file.close()
    except FileNotFoundError:
        print(f"FATAL ERROR on LSF node: Cannot find file {file_path}. Exiting.")
        sys.exit(1) # Exit with an error code so LSF reports the job as failed
    return data, instances_set

def compare_instances(instances1, instances2):
    """Compares instance sets to find matched and missing instances."""
    missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
    missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1, out_filename):
    """Writes missing instances to a file."""
    with open(out_filename, "w") as out:
        if miss2:
            out.writelines([f"Instances from '{file1_name}' missing in '{file2_name}':\n", "="*60 + "\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        if miss1:
            out.writelines([f"\nInstances from '{file2_name}' missing in '{file1_name}':\n", "="*60 + "\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, out_filename):
    """Writes the detailed comparison of matched instances to a CSV file."""
    if not matched: return
    with open(out_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Instance_Key_{i+1}" for i in range(key_len)] + [os.path.basename(file1_name), os.path.basename(file2_name), "Difference", "Percentage"]
        writer.writerow(headers)
        for inst in matched:
            val1 = data1.get(inst)
            val2 = data2.get(inst)
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                if val2 != 0:
                    percentage = abs((diff / val2) * 100)
                    result = f"{percentage:.2f}%"
                else: result = "Infinite"
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", result])
            else:
                match_result = "MATCH" if str(val1) == str(val2) else "MISMATCH"
                writer.writerow(list(inst) + [str(val1), str(val2), "N/A", match_result])

def parse_file_worker(args_tuple):
    """Helper function for multiprocessing."""
    return parse_file_with_mmap(*args_tuple)

def main():
    parser = argparse.ArgumentParser(description="Compares two report files.")
    parser.add_argument("--file1", required=True)
    parser.add_argument("--instcol1", required=True)
    parser.add_argument("--valcol1", required=True, type=int)
    parser.add_argument("--file2", required=True)
    parser.add_argument("--instcol2", required=True)
    parser.add_argument("--valcol2", required=True, type=int)
    parser.add_argument("--output_prefix", required=True)
    args = parser.parse_args()

    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    instcol2 = list(map(int, args.instcol2.strip().split(",")))
    
    # Use multiprocessing to parse both files at the same time
    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, instcol1, args.valcol1),
            (args.file2, instcol2, args.valcol2)
        ])
    
    data1, instances1 = results[0]
    data2, instances2 = results[1]
    
    miss2, miss1, matched = compare_instances(instances1, instances2)

    missing_filename = f"{args.output_prefix}_missing_instances.txt"
    comparison_filename = f"{args.output_prefix}_comparison.csv"
    
    write_missing_file(os.path.basename(args.file1), os.path.basename(args.file2), miss2, miss1, missing_filename)
    write_comparison_csv(args.file1, args.file2, data1, data2, matched, comparison_filename)
    
    print(f"Comparison for prefix {args.output_prefix} complete.")

if __name__ == "__main__":
    main()
