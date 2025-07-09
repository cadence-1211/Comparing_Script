import argparse
import os
import time
import psutil
import sys
import mmap
import re
import csv
from concurrent.futures import ThreadPoolExecutor

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
    return line.startswith(b"-")

def find_start_offset(mmapped_file):
    instance_lines = 0
    mmapped_file.seek(0)
    for _ in range(1000):
        pos = mmapped_file.tell()
        line = mmapped_file.readline()
        if is_valid_instance_line(line):
            instance_lines += 1
            if instance_lines >= 25:
                return pos
    return 0

def parse_file_with_mmap_to_dict(file_path, inst_col, data_col):
    result = {}
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        start_offset = find_start_offset(mmapped_file)
        mmapped_file.seek(start_offset)

        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line):
                continue
            parts = line.strip().split()
            if len(parts) <= max(inst_col, data_col):
                continue
            inst = parts[inst_col]
            data = parts[data_col]
            if inst.startswith(b"-"):
                inst = inst[1:]
            try:
                decoded_inst = inst.decode(errors='ignore')
                decoded_data = float(data.decode(errors='ignore'))
                result[decoded_inst] = decoded_data
            except:
                continue
        mmapped_file.close()
    return result

def count_lines(path):
    with open(path, 'rb') as f:
        return sum(1 for _ in f)

def main():
    args = argparse.Namespace()
    args.file1 = input("Enter path to first file: ")
    args.inst_col1 = int(input("Enter column index (0-based) for instance name in file1: "))
    args.data_col1 = int(input("Enter column index (0-based) for data value in file1: "))

    args.file2 = input("Enter path to second file: ")
    args.inst_col2 = int(input("Enter column index (0-based) for instance name in file2: "))
    args.data_col2 = int(input("Enter column index (0-based) for data value in file2: "))

    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    print("Comparing Files")
    print("=" * 35)
    print(f"  • From {file1_name}: instance column {args.inst_col1 + 1}, value column {args.data_col1 + 1}")
    print(f"  • From {file2_name}: instance column {args.inst_col2 + 1}, value column {args.data_col2 + 1}")

    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.time()

    lines1 = count_lines(args.file1)
    lines2 = count_lines(args.file2)

    with ThreadPoolExecutor() as executor:
        f1 = executor.submit(parse_file_with_mmap_to_dict, args.file1, args.inst_col1, args.data_col1)
        f2 = executor.submit(parse_file_with_mmap_to_dict, args.file2, args.inst_col2, args.data_col2)
        dict1 = f1.result()
        dict2 = f2.result()

    # Comparison and CSV Output
    matched_instances = sorted(set(dict1.keys()) & set(dict2.keys()))
    csv_path = "matched_instance_comparison.csv"
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Instance", f"{file1_name} Value", f"{file2_name} Value", "Difference", "% Deviation"])
        for inst in matched_instances:
            v1 = dict1[inst]
            v2 = dict2[inst]
            diff = v1 - v2
            deviation = (diff / v2 * 100) if v2 != 0 else float('inf')
            writer.writerow([inst, v1, v2, diff, round(deviation, 2)])

    # Missing instance report
    miss2 = sorted(set(dict1.keys()) - set(dict2.keys()))
    miss1 = sorted(set(dict2.keys()) - set(dict1.keys()))

    out_path = "missing_instances.txt"
    with open(out_path, "w") as out:
        out.write(f"{'='*60}\nInstances missing from {file2_name}:\n{'='*60}\n")
        out.writelines([f"{inst}\n" for inst in miss2])
        out.write(f"\n{'='*60}\nInstances missing from {file1_name}:\n{'='*60}\n")
        out.writelines([f"{inst}\n" for inst in miss1])

    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("Comparison Completed")
    print("=" * 35)
    print(f"→ {len(matched_instances)} matching instances saved in '{csv_path}'")
    print(f"→ {len(miss1) + len(miss2)} missing instance(s) saved in '{out_path}'")
    print("Statistics")
    print("=" * 35)
    print(f"  • Lines in {file1_name}: {lines1}")
    print(f"  • Lines in {file2_name}: {lines2}")
    print(f"  • Time elapsed         : {t1 - t0:.4f} seconds")
    print(f"  • Memory usage change  : {(mem_after - mem_before)/(1024*1024):.4f} MB")

if __name__ == "__main__":
    main()
