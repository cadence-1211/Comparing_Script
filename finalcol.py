import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import re

# Pre-compile the regex for numeric extraction for efficiency
NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")

METADATA_KEYWORDS = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}
METADATA_KEYWORDS_SET = set(METADATA_KEYWORDS)

def is_valid_instance_line(line):
    """Checks if a line is a valid instance data line."""
    line = line.strip()
    if not line or line.startswith(b"#"):
        return False
    # Check against a set of known metadata keywords to exclude header lines
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword):
            return False
    return True

def extract_value(value_bytes, comparison_type):
    """
    Extracts a value from a byte string based on the desired comparison type.

    Args:
        value_bytes: The raw byte string from the file (e.g., b"43.23u").
        comparison_type: Either 'numeric' or 'string'.

    Returns:
        The parsed value, which can be a float or a string.
    """
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()

    if comparison_type == 'numeric':
        # Search for the first valid numeric pattern in the string
        match = NUMERIC_RE.search(value_str)
        if match:
            try:
                # If a number is found, convert it to a float
                return float(match.group(0))
            except (ValueError, TypeError):
                # Fallback to string if conversion fails for any reason
                return value_str
        else:
            # If no numeric pattern is found at all, treat it as a string
            return value_str
    else: # comparison_type == 'string'
        # For string comparison, just return the cleaned string
        return value_str

def parse_file_with_mmap(file_path, inst_cols, value_col, comparison_type):
    """Parses a file using memory-mapping for efficiency."""
    data = {}
    instances_set = set()
    with open(file_path, "rb") as f:
        # Use memory-mapping for efficient read access
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # Process the file line by line
        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line):
                continue
            
            parts = line.strip().split()
            if len(parts) <= max(inst_cols + [value_col]):
                continue
            
            try:
                # Create a key from one or more instance columns
                key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                
                # Get the raw value for reporting purposes
                val_raw = parts[value_col].decode('utf-8', errors='ignore').strip()
                
                # Parse the value using the selected comparison type
                val_parsed = extract_value(parts[value_col], comparison_type)
                
                data[key] = (val_raw, val_parsed)
                instances_set.add(key)
            except IndexError:
                # This handles cases where a line has fewer columns than expected
                continue

        mmapped_file.close()
    return data, instances_set

def compare_instances(data1, data2, instances1, instances2):
    """Compares instance sets to find matched and missing instances."""
    missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
    missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1):
    """Writes instances that are not found in the other file to 'missing_instances.txt'."""
    with open("missing_instances.txt", "w") as out:
        if miss2:
            out.writelines([
                f"{'='*60}\n",
                f"Instances from '{file1_name}' missing in '{file2_name}':\n",
                f"{'='*60}\n",
            ])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        
        if miss1:
            out.writelines([
                f"\n{'='*60}\n",
                f"Instances from '{file2_name}' missing in '{file1_name}':\n",
                f"{'='*60}\n",
            ])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2):
    """Writes the detailed comparison of matched instances to 'comparison.csv'."""
    if not matched:
        print("\nNo matched instances found to compare.")
        return

    with open("comparison.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        
        # Create dynamic headers based on the number of key columns
        key_len = len(matched[0]) if matched else 1
        headers = [f"Instance_Key_{i+1}" for i in range(key_len)] + [
            f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Result"
        ]
        writer.writerow(headers)

        for inst in matched:
            raw1, val1 = data1[inst]
            raw2, val2 = data2[inst]

            # Perform numeric comparison if both values were successfully parsed as floats
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                # Handle division by zero for deviation calculation
                if val2 != 0:
                    # MODIFIED: Calculate absolute percentage and update result string
                    deviation = abs((diff / val2) * 100)
                    result = f"{deviation:.2f}%"
                else:
                    # MODIFIED: Simplified result for division by zero
                    result = "Infinite %"
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", result])
            else:
                # Otherwise, perform a string comparison
                match_result = "MATCH" if str(val1) == str(val2) else "MISMATCH"
                writer.writerow(list(inst) + [str(val1), str(val2), "N/A", match_result])

def get_column_name(file_path, col_index):
    """Reads the first valid data line of a file to get a column name."""
    with open(file_path, 'r', errors='ignore') as f:
        for line in f:
            # Skip commented or empty lines to find the first real data line
            if not line.strip().startswith("#") and line.strip():
                headers = line.strip().split()
                if len(headers) > col_index:
                    return headers[col_index]
                break # Stop after the first valid line
    return f"Column_{col_index + 1}"

def parse_file_worker(args_tuple):
    """Helper function to allow multiprocessing.Pool to call the parsing function."""
    return parse_file_with_mmap(*args_tuple)

def main():
    parser = argparse.ArgumentParser(description="Compare two files, with user-defined keys and advanced value comparison.")
    parser.add_argument("--file1", help="Path to the first file.")
    parser.add_argument("--instcol1", help="Comma-separated 0-based index(es) for instance key columns in file 1.")
    parser.add_argument("--valcol1", type=int, help="0-based index for the value column in file 1.")
    parser.add_argument("--file2", help="Path to the second file.")
    parser.add_argument("--instcol2", help="Comma-separated 0-based index(es) for instance key columns in file 2.")
    parser.add_argument("--valcol2", type=int, help="0-based index for the value column in file 2.")
    args = parser.parse_args()

    # --- Interactive Prompts if arguments are not provided ---
    if not args.file1: args.file1 = input("Enter path to first file: ")
    if not args.instcol1: args.instcol1 = input("Enter instance key column index(es) for file 1 (e.g., 0,1): ")
    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    if args.valcol1 is None: args.valcol1 = int(input("Enter value column index for file 1: "))

    if not args.file2: args.file2 = input("Enter path to second file: ")
    if not args.instcol2: args.instcol2 = input("Enter instance key column index(es) for file 2 (e.g., 0,1): ")
    instcol2 = list(map(int, args.instcol2.strip().split(",")))
    if args.valcol2 is None: args.valcol2 = int(input("Enter value column index for file 2: "))
    
    # --- New: Ask for Comparison Type ---
    comparison_type = ''
    while comparison_type not in ['numeric', 'string']:
        comparison_type = input("Enter comparison type for the value column ('numeric' or 'string'): ").lower().strip()
        if comparison_type not in ['numeric', 'string']:
            print("❌ Invalid input. Please enter 'numeric' or 'string'.")

    if len(instcol1) != len(instcol2):
        print("❌ Error: The number of instance key columns must be the same for both files.")
        sys.exit(1)

    t0 = time.time()
    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    # --- Parallel Processing Setup ---
    with multiprocessing.Pool(2) as pool:
        # Get column names in parallel with parsing
        col_name1_future = pool.apply_async(get_column_name, (args.file1, args.valcol1))
        col_name2_future = pool.apply_async(get_column_name, (args.file2, args.valcol2))
        
        # Parse files in parallel
        parse_results_future = pool.map_async(parse_file_worker, [
            (args.file1, instcol1, args.valcol1, comparison_type),
            (args.file2, instcol2, args.valcol2, comparison_type)
        ])
        
        col_name1 = col_name1_future.get()
        col_name2 = col_name2_future.get()
        results = parse_results_future.get()

    data1, instances1 = results[0]
    data2, instances2 = results[1]

    print("\nComparing Columns")
    print("=" * 35)
    print(f"  • Type: {comparison_type.capitalize()}")
    print(f"  • File 1 ({file1_name}): '{col_name1}' (Column {args.valcol1 + 1})")
    print(f"  • File 2 ({file2_name}): '{col_name2}' (Column {args.valcol2 + 1})")
    
    # --- Comparison and Output ---
    miss2, miss1, matched = compare_instances(data1, data2, instances1, instances2)

    write_missing_file(file1_name, file2_name, miss2, miss1)
    write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2)

    t1 = time.time()
    print("\nSummary of Results")
    print("=" * 35)
    print(f"→ {len(miss1) + len(miss2)} total missing instance(s) saved in 'missing_instances.txt'")
    print(f"→ {len(matched)} matched instance(s) compared in 'comparison.csv'")

    print("\nStatistics")
    print("=" * 35)
    print(f"  • Instances in {file1_name}: {len(instances1)}")
    print(f"  • Instances in {file2_name}: {len(instances2)}")
    print(f"  • Time elapsed             : {t1 - t0:.4f} seconds")

if __name__ == "__main__":
    main()
