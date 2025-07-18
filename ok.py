import argparse
import os
import time
import psutil
import sys
import mmap
import re
from collections import Counter

# Metadata keywords to skip
METADATA_KEYWORDS = {
    "VERSION", "CREATION", "CREATOR", "PROGRAM", "DIVIDERCHAR", "DESIGN",
    "UNITS", "INSTANCE_COUNT", "NOMINAL_VOLTAGE", "POWER_NET", "GROUND_NET",
    "WINDOW", "RP_VALUE", "RP_FORMAT", "RP_INST_LIMIT", "RP_THRESHOLD",
    "RP_PIN_NAME", "MICRON_UNITS", "INST_NAME"
}

META_RE = re.compile(rb"^(%s)" % b"|".join(k.encode() for k in METADATA_KEYWORDS))

# Regex for extracting instance name (pattern-based)
INSTANCE_RE = re.compile(rb"^\s*([A-Za-z0-9_/]+)")

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#") or META_RE.match(line):
        return False
    return True

def find_start_offset(mmapped_file):
    instance_lines = 0
    mmapped_file.seek(0)
    for _ in range(5000):
        pos = mmapped_file.tell()
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            instance_lines += 1
            if instance_lines >= 25:
                return pos
    return 0

def detect_instance_column(mmapped_file):
    mmapped_file.seek(0)
    candidates = []
    for _ in range(5000):
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            parts = line.strip().split()
            if parts:
                candidates.append(parts[0].decode(errors='ignore'))
            if len(candidates) >= 25:
                break
    # Check which column most often gives a good instance match
    col_counter = Counter()
    mmapped_file.seek(0)
    for _ in range(5000):
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            parts = line.strip().split()
            for i, word in enumerate(parts):
                if re.match(rb"^[A-Za-z0-9_/]+$", word):
                    col_counter[i] += 1
            if len(col_counter) >= 25:
                break
    if not col_counter:
        return 0
    return col_counter.most_common(1)[0][0]

def parse_file_with_mmap(file_path, value_column_index):
    instances = []
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        start_offset = find_start_offset(mmapped_file)
        mmapped_file.seek(start_offset)

        inst_col = detect_instance_column(mmapped_file)
        mmapped_file.seek(start_offset)

        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line):
                continue
            parts = line.strip().split()
            if len(parts) <= max(inst_col, value_column_index):
                continue
            instance = parts[inst_col].decode(errors='ignore')
            instances.append(instance)

        mmapped_file.close()
    return instances

def compare_instances(instances1, instances2):
    set1 = set(instances1)
    set2 = set(instances2)
    missing_in_file2 = [i for i in instances1 if i not in set2]
    missing_in_file1 = [i for i in instances2 if i not in set1]
    return missing_in_file2, missing_in_file1

def count_lines(path):
    with open(path, 'rb') as f:
        return sum(1 for _ in f)

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

def main():
    parser = argparse.ArgumentParser(description="Compare two files and report missing instances + stats")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--col1", type=int, help="0-based value column index in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--col2", type=int, help="0-based value column index in file2")
    args = parser.parse_args()

    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if args.col1 is None:
        try:
            args.col1 = int(input("Enter value column index for file1: "))
        except ValueError:
            print("Invalid column number for file1.")
            sys.exit(1)
    if not args.file2:
        args.file2 = input("Enter path to second file: ")
    if args.col2 is None:
        try:
            args.col2 = int(input("Enter value column index for file2: "))
        except ValueError:
            print("Invalid column number for file2.")
            sys.exit(1)

    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    col_name1 = get_column_name(args.file1, args.col1)
    col_name2 = get_column_name(args.file2, args.col2)

    print("Comparing Columns")
    print("=" * 35)
    print(f"  • From {file1_name}: {col_name1} (Value Column {args.col1 + 1})")
    print(f"  • From {file2_name}: {col_name2} (Value Column {args.col2 + 1})")

    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.time()

    lines1 = count_lines(args.file1)
    lines2 = count_lines(args.file2)

    list1 = parse_file_with_mmap(args.file1, args.col1)
    list2 = parse_file_with_mmap(args.file2, args.col2)

    miss2, miss1 = compare_instances(list1, list2)

    out_path = "missing_instances.txt"
    with open(out_path, "w") as out:
        out.write(f"{'='*60}\n")
        out.write(f"Instances missing from {file2_name}:\n")
        out.write(f"{'='*60}\n")
        for inst in miss2:
            out.write(f"{inst}\n")
        out.write(f"\n{'='*60}\n")
        out.write(f"Instances missing from {file1_name}:\n")
        out.write(f"{'='*60}\n")
        for inst in miss1:
            out.write(f"{inst}\n")

    missing_count = len(miss1) + len(miss2)
    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("Summary of Missing Instances")
    print("=" * 35)
    print(f"→ {missing_count} missing instance(s) saved in '{out_path}'")

    print("Statistics")
    print("=" * 35)
    print(f"  • Lines in {file1_name}: {lines1}")
    print(f"  • Lines in {file2_name}: {lines2}")
    print(f"  • Time elapsed         : {t1 - t0:.4f} seconds")
    print(f"  • Memory usage change  : {(mem_after - mem_before)/(1024*1024):.4f} MB")

if __name__ == "__main__":
    main()
