# File: merge_results.py
# Purpose: Merges all partial results into final output files and reports total runtime.
import os
import argparse
import time

def merge_csv_files(num_shards, prefix, final_filename):
    print(f"-> Merging {num_shards} comparison CSV files...")
    first_file = f"{prefix}_0_comparison.csv"
    if not os.path.exists(first_file):
        print(f"  ERROR: Cannot find the first result file: {first_file}")
        return False
    with open(final_filename, "w") as final_out:
        with open(first_file, "r") as f_in:
            final_out.write(f_in.read())
        for i in range(1, num_shards):
            partial_file = f"{prefix}_{i}_comparison.csv"
            if os.path.exists(partial_file):
                with open(partial_file, "r") as f_in:
                    next(f_in)
                    final_out.write(f_in.read())
    return True

def merge_txt_files(num_shards, prefix, final_filename):
    print(f"-> Merging {num_shards} missing instance TXT files...")
    with open(final_filename, "w") as final_out:
        for i in range(num_shards):
            partial_file = f"{prefix}_{i}_missing_instances.txt"
            if os.path.exists(partial_file):
                with open(partial_file, "r") as f_in:
                    final_out.write(f_in.read())
                    final_out.write("\n")
    return True

def main():
    parser = argparse.ArgumentParser(description="Merge partial results from parallel LSF runs.")
    parser.add_argument("--shards", type=int, required=True, help="The number of shards/jobs that were run.")
    parser.add_argument("--start_time", type=float, required=True, help="The initial start time of the launch script.")
    args = parser.parse_args()
    
    prefix = "results/run"
    final_csv = "final_comparison.csv"
    final_txt = "final_missing_instances.txt"

    print("\n--- Final Merge Step ---")
    csv_ok = merge_csv_files(args.shards, prefix, final_csv)
    txt_ok = merge_txt_files(args.shards, prefix, final_txt)

    if csv_ok and txt_ok:
        total_end_time = time.time()
        total_runtime = total_end_time - args.start_time
        
        print("\n✅ Merging complete! Your final files are ready:")
        print(f"  -> {final_csv}")
        print(f"  -> {final_txt}")
        print("\n" + "="*40)
        print(f" TOTAL PROGRAM RUNTIME ".center(40, "="))
        print(f" Wall clock time from launch to finish: {total_runtime:.2f} seconds")
        print("="*40)

if __name__ == "__main__":
    main()
