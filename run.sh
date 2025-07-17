#!/bin/bash
# run_lsf.sh (User-editable Version)

# ===================================================================
# --- EDIT THESE VARIABLES TO MATCH YOUR RUN ---
# ===================================================================

# 1. The full path to the Python command on your LSF machines
PYTHON_COMMAND="/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"

# 2. The EXACT base names of your two input files
FILE1_BASENAME="domain_rlrp.rpt"
FILE2_BASENAME="VDD.avg.iv"

# 3. The column numbers for your comparison
INST_COL_1=2
VAL_COL_1=1
INST_COL_2=1
VAL_COL_2=1

# 4. The number of parallel jobs (must match the number you gave the sharder script)
NUM_JOBS=5

# ===================================================================
# --- DO NOT EDIT BELOW THIS LINE ---
# ===================================================================

echo "Submitting ${NUM_JOBS} comparison jobs to LSF..."
mkdir -p results logs

# This loop runs based on the NUM_JOBS variable you set above
for i in $(seq 0 $(($NUM_JOBS - 1)))
do
  bsub -n 2 -R "rusage[mem=16G]" -o "logs/output_${i}.log" \
    $PYTHON_COMMAND compare_adv.py \
      --file1 "shards/${FILE1_BASENAME}_shard_${i}.txt" \
      --file2 "shards/${FILE2_BASENAME}_shard_${i}.txt" \
      --instcol1 $INST_COL_1 \
      --valcol1 $VAL_COL_1 \
      --instcol2 $INST_COL_2 \
      --valcol2 $VAL_COL_2 \
      --output_prefix "results/run_${i}"

  echo "  -> Submitted job for shard ${i}"
done

echo ""
echo "All jobs submitted! Check status with 'bjobs'."
echo "Once all jobs are DONE, run 'python3 merge_results.py' to get the final output."
