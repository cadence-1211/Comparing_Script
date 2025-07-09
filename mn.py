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


def is_valid_data_line(line):
    line = line.strip()
    if not line or line.startswith(b"#") or META_RE.match(line):
        return False
    if line.startswith(b"-") or len(line.split()) >= 2:
        return True
    return False


def find_start_offset(mmapped_file):
    instance_lines = 0
    mmapped_file.seek(0)
    for _ in range(5000):
        pos = mmapped_file.tell()
        line = mmapped_file.readline()
        if is_valid_data_line(line):
            instance_lines += 1
            if instance_lines >= 25:
                return pos
    return 0


def extract_value(parts, index):
    try:
        return parts[index].decode(errors="ignore")
    except:
        return ""


def extract_numeric(value):
    try:
        match = re.search(r"-?\d+\.?\d*", value)
        return float(match.group()) if match else None
    except:
        return None


def parse_file(file_path, inst_col, val_col, starts_with):
    instance_map = {}
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mmapped_file.seek(find_start_offset(mmapped_file))

        for line in iter(mmapped_file.readline, b""):
            if not is_valid_data_line(line):
                continue
            parts = line.strip().split()
            if len(parts) <= max(inst_col, val_col):
                continue

            inst = extract_value(parts, inst_col)
            val = extract_value(parts, val_col)

            if starts_with and inst.startswith(starts_with):
                inst = inst[len(starts_with):]

            inst = inst.strip().lower()
            val = val.strip()

            if inst:
                instance_map[inst] = val

        mmapped_file.close()
    return instance_map


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


def write_missing_instances(file1_data, file2_data, file1_name, file2_name):
    keys1 = set(file1_data.keys())
    keys2 = set(file2_data.keys())

    with open("missing_instances.txt", "w") as out:
        out.write(f"{'='*60}\nMissing in {file2_name}:\n{'='*60}\n")
        for inst in sorted(keys1 - keys2):
            out.write(inst + "\n")

        out.write(f"\n{'='*60}\nMissing in {file1_name}:\n{'='*60}\n")
        for inst in sorted(keys2 - keys1):
            out.write(inst + "\n")


def main():
    parser = argparse.ArgumentParser(description="Universal file comparator based on column values")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--inst_col1", type=int, help="Instance column index in file1")
    parser.add_argument("--val_col1", type=int, help="Value column index in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--inst_col2", type=int, help="Instance column index in file2")
    parser.add_argument("--val_col2", type=int, help="Value column index in file2")
    parser.add_argument("--starts-with1", default="")
    parser.add_argument("--starts-with2", default="")
    args = parser.parse_args()

    for arg_name in ["file1", "file2"]:
        if getattr(args, arg_name) is None:
            setattr(args, arg_name, input(f"Enter path to {arg_name}: "))
    for arg_name in ["inst_col1", "val_col1", "inst_col2", "val_col2"]:
        if getattr(args, arg_name) is None:
            setattr(args, arg_name, int(input(f"Enter {arg_name.replace('_', ' ')}: ")))

    print("\n📊 Parsing and comparing...")
    proc = psutil.Process(os.getpid())
    mem_before = proc.memory_info().rss
    t0 = time.time()

    file1_data = parse_file(args.file1, args.inst_col1, args.val_col1, args.starts_with1)
    file2_data = parse_file(args.file2, args.inst_col2, args.val_col2, args.starts_with2)

    write_missing_instances(file1_data, file2_data, os.path.basename(args.file1), os.path.basename(args.file2))
    write_csv_comparison(file1_data, file2_data, "instance_comparison.csv")

    t1 = time.time()
    mem_after = proc.memory_info().rss

    print("\n✅ Summary")
    print("=" * 40)
    print(f"• Missing instances saved in: 'missing_instances.txt'")
    print(f"• Value comparison saved in : 'instance_comparison.csv'")
    print(f"• Time taken               : {t1 - t0:.3f} sec")
    print(f"• Memory usage change      : {(mem_after - mem_before) / (1024 * 1024):.3f} MB")


if __name__ == "__main__":
    main()
