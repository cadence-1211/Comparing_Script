import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import math

# A set of metadata keywords to ignore during parsing.
# Using a set of bytes is faster for membership testing.
METADATA_KEYWORDS = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}


def find_chunk_boundaries(file_path, num_chunks):
    """
    Calculates byte offsets for splitting a file into chunks for parallel processing.
    Each chunk starts on a new line to avoid breaking lines in the middle.
    """
    try:
        file_size = os.path.getsize(file_path)
    except FileNotFoundError:
        sys.stderr.write(f"Error: File not found at '{file_path}'\n")
        sys.exit(1)

    if file_size == 0:
        return []

    chunk_size = file_size // num_chunks
    boundaries = [0]
    with open(file_path, "rb") as f:
        # Find a newline character near each theoretical chunk boundary.
        for i in range(1, num_chunks):
            seek_pos = min(chunk_size * i, file_size - 1)
            f.seek(seek_pos)
            f.readline()  # Read to the end of the current line.
            current_pos = f.tell()
            if current_pos < file_size:
                boundaries.append(current_pos)
    boundaries.append(file_size)

    # Return start and end points for each chunk.
    return [(boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1) if boundaries[i] < boundaries[i+1]]


def process_chunk(file_path, start_byte, end_byte, inst_cols, value_col):
    """
    The main worker function for each process. It reads and parses a
    specific chunk of the file.
    """
    max_col = max(inst_cols + [value_col])
    data = {}
    instances_set = set()

    with open(file_path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            mm.seek(start_byte)

            while mm.tell() < end_byte:
                line = mm.readline()
                if not line:
                    break

                stripped_line = line.strip()
                # Skip empty lines, comments, and metadata headers.
                if not stripped_line or stripped_line.startswith(b'#') or stripped_line.split(b' ', 1)[0] in METADATA_KEYWORDS:
                    continue

                parts = stripped_line.split()
                if len(parts) <= max_col:
                    continue

                try:
                    key = tuple(parts[i] for i in inst_cols)
                    value_bytes = parts[value_col]

                    # Simple auto-detection: try to convert to float,
                    # otherwise treat it as a string.
                    try:
                        val_parsed = float(value_bytes)
                    except ValueError:
                        val_parsed = value_bytes.decode('utf-8', 'ignore')

                    # Store both the raw bytes and the parsed value.
                    data[key] = (value_bytes, val_parsed)
                    instances_set.add(key)
                except IndexError:
                    # Line didn't have enough columns, skip it.
                    continue

    return data, instances_set


def parallel_parse_file(file_path, inst_cols, value_col):
    """
    Sets up a multiprocessing pool to parse a file in parallel.
    """
    # Use all available CPU cores.
    num_workers = multiprocessing.cpu_count()
    file_name = os.path.basename(file_path)
    print(f"Parsing {file_name} with {num_workers} processes...")

    chunk_boundaries = find_chunk_boundaries(file_path, num_workers)
    if not chunk_boundaries:
        print(f"Warning: File {file_name} is empty or could not be read.")
        return {}, set()

    worker_args = [(file_path, start, end, inst_cols, value_col) for start, end in chunk_boundaries]

    final_data = {}
    final_instances_set = set()

    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.starmap(process_chunk, worker_args)

    # Aggregate results from all worker processes.
    for data_chunk, instances_chunk in results:
        final_data.update(data_chunk)
        final_instances_set.update(instances_chunk)

    return final_data, final_instances_set


def compare_instances(instances1, instances2):
    """Finds matched and missing instances between two sets of data."""
    missing_in_file2 = sorted(list(instances1 - instances2))
    missing_in_file1 = sorted(list(instances2 - instances1))
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched


def write_missing_csv(file1_name, file2_name, miss2, miss1, data1, data2, col_name1, col_name2, valcol1, valcol2):
    """
    Writes the lists of missing instances, including their data, to a CSV file.
    """
    with open("missing_instances.csv", "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # --- Instances missing from File 2 ---
        writer.writerow([f"--- Instances from {file1_name} missing in {file2_name} ---"])
        if miss2:
            key_len = len(miss2[0])
            headers = [f"Key_{i+1}" for i in range(key_len)] + [f"Value from {file1_name} ({col_name1}, col {valcol1})"]
            writer.writerow(headers)
            for inst_key in miss2:
                key_list = [k.decode('utf-8', 'ignore') for k in inst_key]
                # Look up the value from the data where it exists (data1)
                raw_bytes, _ = data1.get(inst_key, (b'N/A', 'N/A'))
                value_str = raw_bytes.decode('utf-8', 'ignore')
                writer.writerow(key_list + [value_str])

        # --- Instances missing from File 1 ---
        writer.writerow([]) # Add a blank row for separation
        writer.writerow([f"--- Instances from {file2_name} missing in {file1_name} ---"])
        if miss1:
            key_len = len(miss1[0])
            headers = [f"Key_{i+1}" for i in range(key_len)] + [f"Value from {file2_name} ({col_name2}, col {valcol2})"]
            writer.writerow(headers)
            for inst_key in miss1:
                key_list = [k.decode('utf-8', 'ignore') for k in inst_key]
                # Look up the value from the data where it exists (data2)
                raw_bytes, _ = data2.get(inst_key, (b'N/A', 'N/A'))
                value_str = raw_bytes.decode('utf-8', 'ignore')
                writer.writerow(key_list + [value_str])


def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2, compare_type):
    """
    Writes the detailed comparison CSV. The output format depends on the
    user's chosen compare_type.
    """
    print(f"Writing comparison report to comparison.csv (mode: {compare_type})...")
    with open("comparison.csv", "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Key_{i+1}" for i in range(key_len)] + [
            f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}"
        ]

        # Adjust headers based on comparison type.
        if compare_type == 'numeric':
            headers.extend(["Difference", "Deviation"])
        else:  # string
            headers.append("Match")

        writer.writerow(headers)

        for inst_key in matched:
            raw_bytes1, val1 = data1[inst_key]
            raw_bytes2, val2 = data2[inst_key]

            key_list = [k.decode('utf-8', 'ignore') for k in inst_key]
            raw1_str = raw_bytes1.decode('utf-8', 'ignore')
            raw2_str = raw_bytes2.decode('utf-8', 'ignore')

            if compare_type == 'numeric':
                # Check if both values were successfully parsed as floats.
                if isinstance(val1, float) and isinstance(val2, float):
                    diff = val1 - val2
                    # Handle division by zero.
                    deviation = (diff / val2) * 100 if val2 != 0 else float('inf')
                    writer.writerow(key_list + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", f"{deviation:.2f}%"])
                else:
                    # If one or both are not numbers, report them as strings.
                    writer.writerow(key_list + [raw1_str, raw2_str, "N/A", "Not a Number"])
            else:  # string comparison
                # Always compare the raw string values.
                match_status = "YES" if raw1_str == raw2_str else "NO"
                writer.writerow(key_list + [raw1_str, raw2_str, match_status])


def get_column_name(file_path, col_index):
    """
    Quickly reads the first few lines of a file to guess the column header name.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Find the first non-empty, non-comment line.
                if line.strip() and not line.startswith("#"):
                    headers = line.strip().split()
                    return headers[col_index] if len(headers) > col_index else f"Column_{col_index + 1}"
    except (FileNotFoundError, IndexError):
        return f"Column_{col_index + 1}"


def main():
    """Main function to parse arguments, run processing, and print summaries."""
    parser = argparse.ArgumentParser(description="Compare two large text files using parallel processing.")
    parser.add_argument("--file1", help="Path to the first file.")
    parser.add_argument("--instcol1", help="Comma-separated instance column indexes for file 1 (0-based).")
    parser.add_argument("--valcol1", type=int, help="Value column index for file 1 (0-based).")
    parser.add_argument("--file2", help="Path to the second file.")
    parser.add_argument("--instcol2", help="Comma-separated instance column indexes for file 2 (0-based).")
    parser.add_argument("--valcol2", type=int, help="Value column index for file 2 (0-based).")
    parser.add_argument(
        "--compare-type",
        choices=['numeric', 'string'],
        help="The type of comparison for the value column."
    )
    args = parser.parse_args()

    # If key arguments are missing, fall back to interactive prompts.
    if not all([args.file1, args.instcol1, args.valcol1 is not None, args.file2, args.instcol2, args.valcol2 is not None, args.compare_type]):
        print("Required arguments not provided, entering interactive mode.")
        try:
            args.file1 = input("Enter path to first file: ")
            if not os.path.exists(args.file1): raise FileNotFoundError
            
            args.instcol1 = input("Enter instance match column indexes for file 1 (e.g., 0,1): ")
            args.valcol1 = int(input("Enter value column index for file 1: "))

            args.file2 = input("Enter path to second file: ")
            if not os.path.exists(args.file2): raise FileNotFoundError

            args.instcol2 = input("Enter instance match column indexes for file 2 (e.g., 0,1): ")
            args.valcol2 = int(input("Enter value column index for file 2: "))
            
            args.compare_type = ""
            while args.compare_type not in ['numeric', 'string']:
                args.compare_type = input("Enter comparison type ('numeric' or 'string'): ").lower().strip()
                if args.compare_type not in ['numeric', 'string']:
                    sys.stderr.write("Error: Invalid input. Please enter 'numeric' or 'string'.\n")

        except (ValueError, FileNotFoundError):
            sys.stderr.write("Error: Invalid input or file not found. Exiting.\n")
            sys.exit(1)

    try:
        instcol1 = list(map(int, args.instcol1.strip().split(',')))
        instcol2 = list(map(int, args.instcol2.strip().split(',')))
    except (ValueError, AttributeError):
        sys.stderr.write("Error: Instance columns must be a comma-separated list of integers.\n")
        sys.exit(1)

    if len(instcol1) != len(instcol2):
        sys.stderr.write("Error: The number of instance match columns must be the same for both files.\n")
        sys.exit(1)

    t0 = time.time()

    data1, instances1 = parallel_parse_file(args.file1, instcol1, args.valcol1)
    data2, instances2 = parallel_parse_file(args.file2, instcol2, args.valcol2)

    print("\nComparing data sets...")
    miss2, miss1, matched = compare_instances(instances1, instances2)

    print("Writing output files...")
    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    col_name1 = get_column_name(args.file1, args.valcol1)
    col_name2 = get_column_name(args.file2, args.valcol2)

    # Call the new CSV writer for missing instances
    write_missing_csv(file1_name, file2_name, miss2, miss1, data1, data2, col_name1, col_name2, args.valcol1, args.valcol2)
    
    if matched:
        write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2, args.compare_type)
    else:
        print("Note: No matched instances were found; comparison.csv will not contain data rows.")

    t1 = time.time()
    
    print("\n---------- SUMMARY ----------")
    print(f"File 1 ({file1_name}): {len(instances1):,} instances")
    print(f"File 2 ({file2_name}): {len(instances2):,} instances")
    print(f"Matched Instances: {len(matched):,}")
    print(f"Instances missing from File 2: {len(miss2):,}")
    print(f"Instances missing from File 1: {len(miss1):,}")
    print(f"\nTotal execution time: {t1 - t0:.2f} seconds")
    print("---------------------------")


if __name__ == "__main__":
    # This is necessary for multiprocessing to work correctly on some OSs like Windows.
    multiprocessing.freeze_support()
    main()
