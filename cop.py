import argparse
import os
import time
import psutil
import sys
import mmap
import re
import csv

# Regex for instance pattern
INSTANCE_RE = re.compile(rb"^\s*([A-Za-z0-9_/]+)")

# Keywords to ignore
METADATA_KEYWORDS = {
    "VERSION", "CREATION", "CREATOR", "PROGRAM", "DIVIDERCHAR", "DESIGN",
    "UNITS", "INSTANCE_COUNT", "NOMINAL_VOLTAGE", "POWER_NET", "GROUND_NET",
    "WINDOW", "RP_VALUE", "RP_FORMAT", "RP_INST_LIMIT", "RP_THRESHOLD",
    "RP_PIN_NAME", "MICRON_UNITS", "INST_NAME"
}
META_RE = re.compile(rb"^(%s)" % b"|".join(k.encode() for k in METADATA_KEYWORDS))

def is_valid_instance_line(line):
    line = line.strip()
    return bool(line and not line.startswith(b"#") and not META_RE.match(line))

def extract_instance(line):
    match = INSTANCE_RE.match(line)
    if match:
        return match.group(1).decode('utf-8', errors='ignore')
    return None

def find_start_offset(mmapped_file, threshold=25, max_lines=500):
    mmapped_file.seek(0)
    valid_line_count = 0
    for _ in range(max_lines):
        pos = mmapped_file.tell()
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            if extract_instance(line):
                valid_line_count += 1
                if valid_line_count >= threshold:
                    return pos
    return 0

def parse_file_with_mmap(file_path, value_column_index):
    data = {}
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        start_offset = find_start_offset(mmapped_file)
        mmapped_file.seek(start_offset)

        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line):
                continue
            instance_name = extract_instance(line)
            if not instance_name:
                continue
            parts = line.strip().split()
            if len(parts) <= value_column_index:
                continue
            value = parts[value_column_index].strip()
            if not value:
                continue
            try:
                val = float(value)
            except ValueError:
                continue
            data[instance_name] = val
        mmapped_file.close()
    return data

def compare_instances(data1, data2):
    inst1 = set(data1.keys())
    inst2 = set(data2.keys())
    missing_in_file2 = sorted(inst1 - inst2)
    missing_in_file1 = sorted(inst2 - inst1)
    matched = sorted(inst1 & inst2)
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1):
    with open("missing_instances.txt", "w") as out:
        out.write(f"{'='*60}\nInstances missing from {file2_name}:\n{'='*60}\n")
        for inst in miss2:
            out.write(f"{inst}\n")
        out.write(f"\n{'='*60}\nInstances missing from {file1_name}:\n{'='*60}\n")
        for inst in miss1:
            out.write(f"{inst}\n")

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2):
    with open("comparison.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Instance", f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Deviation (%)"])
        for inst in matched:
            v1, v2 = data1[inst], data2[inst]
            diff = v1 - v2
            deviation = (diff / v2 * 100) if v2 != 0 else float('inf')
            writer.writerow([inst, f"{v1:.4f}", f"{v2:.4f}", f"{diff:.4f}", f"{deviation:.2f}%"])

def get_column_name(file_path, col_index):
    with open(file_path, "r") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                parts = line.strip().split()
                if len(parts) > col_index:
                    return parts[col_index]
    return f"Column {col_index + 1}"

def main():
    parser = argparse.ArgumentParser(description="Compare two files based on instance and value columns.")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--valcol1", type=int, help="0-based value column index in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--valcol2", type=int, help="0-based value column index in file2")
    args = parser.parse_args()

    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if args.valcol1 is None:
        args.valcol1 = int(input("Enter value column index for file1: "))
    if not args.file2:
        args.file2 = input("Enter path to second file: ")
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

    data1 = parse_file_with_mmap(args.file1, args.valcol1)
    data2 = parse_file_with_mmap(args.file2, args.valcol2)

    miss2, miss1, matched = compare_instances(data1, data2)

    write_missing_file(file1_name, file2_name, miss2, miss1)
    write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2)

    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("\nSummary of Results")
    print("=" * 35)
    print(f"→ {len(miss1) + len(miss2)} missing instance(s) saved in 'missing_instances.txt'")
    print(f"→ {len(matched)} matched instance(s) saved in 'comparison.csv'")

    print("\nStatistics")
    print("=" * 35)
    print(f"  • Time elapsed         : {t1 - t0:.4f} seconds")
    print(f"  • Memory usage change  : {(mem_after - mem_before)/(1024*1024):.4f} MB")

if __name__ == "__main__":
    main()
