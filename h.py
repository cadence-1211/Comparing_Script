#!/usr/bin/env python3
import csv
import os
import time
import psutil

SCRIPT_VERSION = "1.9.0"

def get_relevant_lines(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Find last line with "Instance" or "Instance/pin"
    last_header = -1
    for i, line in enumerate(lines):
        header = line.strip().lower()
        if header.startswith("instance") or header.startswith("instance/pin"):
            last_header = i
    return lines[last_header + 1:] if last_header >= 0 else []

def parse_joined_lines(lines, column_number):
    values = {}
    col_index = column_number - 1
    buffer = ""

    for line in lines:
        line = line.rstrip()
        if not line.strip() or line.startswith("*") or line.startswith("="):
            continue

        # If line starts with a space, it’s likely a continuation of the previous instance name
        if line.startswith(" ") or line.startswith("\t"):
            buffer += " " + line.strip()
            continue
        else:
            if buffer:
                full = buffer.strip()
                parts = full.split()
                if len(parts) > col_index:
                    try:
                        instance = parts[0]
                        value = float(parts[col_index])
                        values[instance] = value
                    except:
                        pass
            buffer = line.strip()

    # Process the last buffered line
    if buffer:
        full = buffer.strip()
        parts = full.split()
        if len(parts) > col_index:
            try:
                instance = parts[0]
                value = float(parts[col_index])
                values[instance] = value
            except:
                pass

    return values

def stream_joined_lines(lines, column_number):
    col_index = column_number - 1
    buffer = ""

    for line in lines:
        line = line.rstrip()
        if not line.strip() or line.startswith("*") or line.startswith("="):
            continue

        if line.startswith(" ") or line.startswith("\t"):
            buffer += " " + line.strip()
            continue
        else:
            if buffer:
                full = buffer.strip()
                parts = full.split()
                if len(parts) > col_index:
                    try:
                        instance = parts[0]
                        value = float(parts[col_index])
                        yield instance, value
                    except:
                        pass
            buffer = line.strip()

    if buffer:
        full = buffer.strip()
        parts = full.split()
        if len(parts) > col_index:
            try:
                instance = parts[0]
                value = float(parts[col_index])
                yield instance, value
            except:
                pass

def compare_files(file1, file2, column_number):
    print(f"\n🔍 Comparing column {column_number} in '{file1}' vs '{file2}' — v{SCRIPT_VERSION}")
    start_time = time.time()

    lines1 = get_relevant_lines(file1)
    lines2 = get_relevant_lines(file2)
    data1 = parse_joined_lines(lines1, column_number)

    output_file = "comparison_output.csv"
    if os.path.exists(output_file):
        os.remove(output_file)

    differences = []
    count2 = 0

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([f"Script Version: {SCRIPT_VERSION}"])
        writer.writerow([])
        writer.writerow(["Instance", "File1 Value", "File2 Value", "Difference", "Percent Difference"])

        for instance, val2 in stream_joined_lines(lines2, column_number):
            count2 += 1
            val1 = data1.get(instance, "Missing")
            if val1 == "Missing" or val1 == val2:
                continue
            try:
                diff = abs(val1 - val2)
                if diff == 0:
                    continue
                pct = (diff / val1 * 100) if val1 != 0 else "NA"
                differences.append(diff)
                writer.writerow([instance, f"{val1:.6f}", f"{val2:.6f}", f"{diff:.6f}", f"{pct:.2f}%"])
            except:
                continue

    runtime = round(time.time() - start_time, 2)
    mem = round(psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024), 2)

    print(f"\n✅ Comparison complete. File1: {len(data1)} entries | File2 scanned: {count2}")
    print(f"🔎 Mismatches found: {len(differences)}")
    print(f"📉 Min Diff: {min(differences) if differences else 0}")
    print(f"📈 Max Diff: {max(differences) if differences else 0}")
    print(f"📊 Avg Diff: {sum(differences)/len(differences) if differences else 0:.6f}")
    print(f"⏳ Time: {runtime}s   💾 Mem: {mem:.2f} MB")
    print(f"📂 Output written to: comparison_output.csv")

# 🔹 Entry point
if __name__ == "__main__":
    f1 = input("Enter first report file: ").strip()
    f2 = input("Enter second report file: ").strip()
    col = int(input("Enter column number to compare (e.g., 6 = Leakage): "))
    compare_files(f1, f2, col)
