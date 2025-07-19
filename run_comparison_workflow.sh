#!/bin/bash
# run_comparison_workflow.sh

# --- Helper function for user input ---
ask_for_column() {
  local prompt="$1"
  local col_var
  while true; do
    read -p "➡️ $prompt" col_var
    # Check if input is a non-negative integer
    if [[ "$col_var" =~ ^[0-9]+$ ]]; then
      echo "$col_var"
      return
    else
      echo "❌ Error: Please enter a valid non-negative number."
    fi
  done
}

echo "--- Automated LSF Comparison Workflow ---"

# --- Configuration via User Input ---
echo "Please provide the 0-based column numbers for the comparison."
INST_COL=$(ask_for_column "Enter the INSTANCE key column to use for comparison: ")
VAL_COL1=$(ask_for_column "Enter the VALUE column from the first file: ")
VAL_COL2=$(ask_for_column "Enter the VALUE column from the second file: ")
NUM_SHARDS=5
# ------------------------------------

# Generate a unique name for our LSF job array
JOB_NAME="py_compare_$$"

echo ""
echo "STEP 1: Submitting ${NUM_SHARDS} comparison jobs to LSF."
echo "        Job array name: ${JOB_NAME}"

# Create directories for results and logs if they don't exist
mkdir -p results
mkdir -p logs

# Submit to LSF as a job array.
bsub -J "${JOB_NAME}[0-$(($NUM_SHARDS - 1))]" \
     -n 2 -R "rusage[mem=8G]" \
     -o "logs/output_%I.log" \
     "python3 compare_adv.py \
       --file1 \"shards/file1.txt_shard_\${LSB_JOBINDEX}.txt\" \
       --file2 \"shards/file2.txt_shard_\${LSB_JOBINDEX}.txt\" \
       --instcol1 ${INST_COL} --valcol1 ${VAL_COL1} \
       --instcol2 ${INST_COL} --valcol2 ${VAL_COL2} \
       --output_prefix \"results/run_\${LSB_JOBINDEX}\""

echo "--------------------------------------------------------"
echo "STEP 2: All jobs submitted. Now waiting for them to finish."
echo "        This may take a while..."
echo ""

# The 'bwait' command pauses the script until all jobs are done.
bwait -w "done(${JOB_NAME}[*])"

echo "--------------------------------------------------------"
echo "STEP 3: All jobs have completed. Fetching statistics..."
echo ""

# Get the accounting information for all jobs in the array.
bacct -l -J "${JOB_NAME}[*]" | grep -E "Job <|CPU time|Max Memory"

echo ""
echo "--------------------------------------------------------"
echo "STEP 4: Merging all partial results into final files..."

# Merge the CSV comparison files
cat results/run_0_comparison.csv > final_comparison.csv
for i in $(seq 1 $(($NUM_SHARDS - 1)))
do
  tail -n +2 "results/run_${i}_comparison.csv" >> final_comparison.csv
done

# Merge the missing instances files
cat results/run_*_missing_instances.txt > final_missing_instances.txt

echo ""
echo "✅ All Done! Your final result files are ready:"
echo "  -> final_comparison.csv"
echo "  -> final_missing_instances.txt"
