import argparse
import os
import time
import psutil
import sys
import mmap
import re
import csv

# Metadata keywords to skip
METADATA_KEYWORDS = {
    "VERSION", "CREATION", "CREATOR", "PROGRAM", "DIVIDERCHAR", "DESIGN",
    "UNITS", "INSTANCE_COUNT", "NOMINAL_VOLTAGE", "POWER_NET", "GROUND_NET",
    "WINDOW", "RP_VALUE", "RP_FORMAT", "RP_INST_LIMIT", "RP_THRESHOLD",
    "RP_PIN_NAME", "MICRON_UNITS", "INST_NAME"
}
META_RE = re.compile(rb"^(%s)" % b"|".join(k.encode() for k in METADATA_KEYWORDS))

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#") or META_RE.match(line):
        return False
    return True  # Accept all lines unless they're meta/comments/empty

def find_start_offset(mmapped_file):
    mmapped_file.seek(0)
    for _ in range(5000):
        pos = mmapped_file.tell()
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            return pos
    return 0

def parse_file_for_instances(file_path, inst_col, value_col):
    instance_map = {}
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mmapped_file.seek(find_start_offset(mmapped_file))

        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line):
                continue
            parts = line.strip().split()
            if len(parts) <= max(inst_col, value_col):
                continue
            inst = parts[inst_col]
            val = parts[value_col]

            key = inst.decode(errors="ignore").strip().lower()
            value = val.decode(errors="ignore").strip()
            instance_map[key] = value

        mmapped_file.close()
    return instance_map

def extract_numeric(value):
    try:
        match = re.search(r"-?\d+\.?\d*", value)
        return float(match.group()) if match else None
    except:
        return None

def compare_instances(inst1, inst2):
    set1 = set(inst1.keys())
    set2 = set(inst2.keys())
    missing_in_file2 = sorted(set1 - set2)
    missing_in_file1 = sorted(set2 - set1)
    return missing_in_file2, missing_in_file1

def write_csv_comparison(file1_data, file2_data, output_path):
    matched_keys = sorted(set(file1_data.keys()) & set(file2_data.keys()))
    with open(output_path, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Instance", "Value_File1", "Value_File2", "Difference", "Percent_Deviation"])

        for inst in matched_keys:
            v1_raw = file1_data[inst]
            v2_raw = file2_data[inst]
            v1 = extract_numeric(v1_raw)
            v2 = extract_numeric(v2_raw)
            if v1 is not None and v2 is not None:
                diff = abs(v1 - v2)
                pct_dev = (diff / v1 * 100) if v1 != 0 else float("inf")
                writer.writerow([inst, v1, v2, diff, round(pct_dev, 2)])

def main():
    parser = argparse.ArgumentParser(description="Compare instance values from two files")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--inst_col1", type=int, help="Instance column index (1-based) in file1")
    parser.add_argument("--val_col1", type=int, help="Value column index (1-based) in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--inst_col2", type=int, help="Instance column index (1-based) in file2")
    parser.add_argument("--val_col2", type=int, help="Value column index (1-based) in file2")

    args = parser.parse_args()

    # Prompt user interactively if any input is missing
    if not args.file1:
        args.file1 = input("Enter path to first file: ").strip()
    if args.inst_col1 is None:
        args.inst_col1 = int(input("Enter instance column index for file1 (1-based): ").strip())
    if args.val_col1 is None:
        args.val_col1 = int(input("Enter value column index for file1 (1-based): ").strip())

    if not args.file2:
        args.file2 = input("Enter path to second file: ").strip()
    if args.inst_col2 is None:
        args.inst_col2 = int(input("Enter instance column index for file2 (1-based): ").strip())
    if args.val_col2 is None:
        args.val_col2 = int(input("Enter value column index for file2 (1-based): ").strip())

    # Convert to 0-based index
    inst_col1 = args.inst_col1 - 1
    val_col1 = args.val_col1 - 1
    inst_col2 = args.inst_col2 - 1
    val_col2 = args.val_col2 - 1

    # Start measuring
    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.time()

    file1_data = parse_file_for_instances(args.file1, inst_col1, val_col1)
    file2_data = parse_file_for_instances(args.file2, inst_col2, val_col2)

    miss2, miss1 = compare_instances(file1_data, file2_data)

    with open("missing_instances.txt", "w") as out:
        out.write(f"{'='*60}\nMissing in {os.path.basename(args.file2)}:\n{'='*60}\n")
        for m in miss2:
            out.write(m + "\n")
        out.write(f"\n{'='*60}\nMissing in {os.path.basename(args.file1)}:\n{'='*60}\n")
        for m in miss1:
            out.write(m + "\n")

    write_csv_comparison(file1_data, file2_data, "instance_comparison.csv")

    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("\nSummary")
    print("=" * 35)
    print(f"  • Missing instances saved in: 'missing_instances.txt'")
    print(f"  • Comparison CSV saved in:    'instance_comparison.csv'")
    print(f"  • Time elapsed               : {t1 - t0:.4f} sec")
    print(f"  • Memory usage change        : {(mem_after - mem_before) / (1024 * 1024):.4f} MB")

if __name__ == "__main__":
    main()
