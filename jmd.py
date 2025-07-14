import argparse
import os
import time
import psutil
import sys
import mmap
import csv
import multiprocessing
import re

# Metadata keywords to skip
METADATA_KEYWORDS = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}
METADATA_KEYWORDS_SET = set(METADATA_KEYWORDS)

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#"):
        return False
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword):
            return False
    return True

def extract_value(value_bytes):
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()

    # Always try to extract a number, even from "1324.24," or "(1234.5)"
    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", value_str)
    if match:
        try:
            return float(match.group())
        except ValueError:
            pass

    # If no number, treat as string
    return value_str

def parse_file_with_mmap(file_path, inst_col, value_col):
    data = {}
    instances_set = set()
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        chunk_size = 1024 * 1024
        buffer = b""
        while True:
            chunk = mmapped_file.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            lines = buffer.split(b"\n")
            buffer = lines.pop()
            for line in lines:
                if not is_valid_instance_line(line):
                    continue
                parts = line.strip().split()
                if len(parts) <= max(inst_col, value_col):
                    continue
                value = parts[value_col].strip()
                if not value:
                    continue
                instance = parts[inst_col].decode('utf-8', errors='ignore')
                parsed_val = extract_value(value)
                if parsed_val is not None:
                    data[instance] = parsed_val
                    instances_set.add(instance)
        if buffer:
            line = buffer
            if is_valid_instance_line(line):
                parts = line.strip().split()
                if len(parts) > max(inst_col, value_col):
                    value = parts[value_col].strip()
                    if value:
                        instance = parts[inst_col].decode('utf-8', errors='ignore')
                        parsed_val = extract_value(value)
                        if parsed_val is not None:
                            data[instance] = parsed_val
                            instances_set.add(instance)
        mmapped_file.close()
    return data, instances_set

def compare_instances(data1, data2, instances1, instances2):
    missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
    missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1):
    with open("missing_instances.txt", "w") as out:
        out.writelines([
            f"{'='*60}\n",
            f"Instances missing from {file2_name}:\n",
            f"{'='*60}\n",
        ])
        out.writelines(f"{inst}\n" for inst in miss2)
        out.writelines([
            f"\n{'='*60}\n",
            f"Instances missing from {file1_name}:\n",
            f"{'='*60}\n",
        ])
        out.writelines(f"{inst}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2):
    with open("comparison.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Instance", f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Deviation / Match"])
        for inst in matched:
            val1 = data1[inst]
            val2 = data2[inst]
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                try:
                    deviation = (diff / val2) * 100 if val2 != 0 else float('inf')
                except:
                    deviation = 0
                writer.writerow([inst, f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", f"{deviation:.2f}%"])
            else:
                match = "YES" if str(val1) == str(val2) else "NO"
                writer.writerow([inst, val1, val2, "N/A", match])

def get_column_name(file_path, col_index):
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                headers = line.strip().split()
                if len(headers) > col_index:
                    return headers[col_index]
                else:
                    return f"Column {col_index + 1}"
    return f"Column {col_index + 1}"

def count_lines(path):
    with open(path, 'rb') as f:
        count = 0
        chunk_size = 1024 * 1024
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            count += chunk.count(b'\n')
    return count

def parse_file_worker(args):
    file_path, inst_col, val_col = args
    return parse_file_with_mmap(file_path, inst_col, val_col)

def main():
    parser = argparse.ArgumentParser(description="Compare two files and report missing instances + CSV comparison")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--instcol1", type=int, help="0-based instance column index in file1")
    parser.add_argument("--valcol1", type=int, help="0-based value column index in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--instcol2", type=int, help="0-based instance column index in file2")
    parser.add_argument("--valcol2", type=int, help="0-based value column index in file2")
    args = parser.parse_args()

    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if args.instcol1 is None:
        args.instcol1 = int(input("Enter instance column index for file1: "))
    if args.valcol1 is None:
        args.valcol1 = int(input("Enter value column index for file1: "))
    if not args.file2:
        args.file2 = input("Enter path to second file: ")
    if args.instcol2 is None:
        args.instcol2 = int(input("Enter instance column index for file2: "))
    if args.valcol2 is None:
        args.valcol2 = int(input("Enter value column index for file2: "))

    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    col_name1 = get_column_name(args.file1, args.valcol1)
    col_name2 = get_column_name(args.file2, args.valcol2)

    print("\nComparing Columns")
    print("=" * 35)
    print(f"  • From {file1_name}: {col_name1} (Column {args.valcol1 + 1})")
    print(f"  • From {file2_name}: {col_name2} (Column {args.valcol2 + 1})")

    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.time()

    lines1 = count_lines(args.file1)
    lines2 = count_lines(args.file2)

    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, args.instcol1, args.valcol1),
            (args.file2, args.instcol2, args.valcol2)
        ])

    data1, instances1 = results[0]
    data2, instances2 = results[1]

    miss2, miss1, matched = compare_instances(data1, data2, instances1, instances2)

    write_missing_file(file1_name, file2_name, miss2, miss1)
    write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2)

    missing_count = len(miss1) + len(miss2)
    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("\nSummary of Results")
    print("=" * 35)
    print(f"→ {missing_count} missing instance(s) saved in 'missing_instances.txt'")
    print(f"→ {len(matched)} matched instance(s) saved in 'comparison.csv'")

    print("\nStatistics")
    print("=" * 35)
    print(f"  • Lines in {file1_name}: {lines1}")
    print(f"  • Lines in {file2_name}: {lines2}")
    print(f"  • Time elapsed         : {t1 - t0:.4f} seconds")
    print(f"  • Memory usage change  : {(mem_after - mem_before)/(1024*1024):.4f} MB")

if __name__ == "__main__":
    main()
