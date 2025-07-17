#!/bin/bash
# run_lsf.sh (Corrected Version)

echo "Submitting 5 comparison jobs to LSF..."

# Create directories for the results and logs
mkdir -p results
mkdir -p logs

# --- IMPORTANT ---
# You correctly found that 'python3' doesn't work.
# We will use the full path to the Python command that you discovered.
# You can change this if you find a better one.
PYTHON_COMMAND="/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"

# Your actual filenames
FILE1_BASENAME="domain_rlrp.rpt"
FILE2_BASENAME="VDD.avg.iv"

# This loop runs 5 times (for 0, 1, 2, 3, 4)
for i in {0..4}
do
  # This is the LSF command with the CORRECT filenames and column numbers
  bsub -n 2 -R "rusage[mem=16G]" -o "logs/output_${i}.log" \
    $PYTHON_COMMAND compare_adv.py \
      --file1 "shards/${FILE1_BASENAME}_shard_${i}.txt" \
      --file2 "shards/${FILE2_BASENAME}_shard_${i}.txt" \
      --instcol1 2 \
      --valcol1 1 \
      --instcol2 1 \
      --valcol2 1 \
      --output_prefix "results/run_${i}"

  echo "  -> Submitted job for shard ${i} using Python: $PYTHON_COMMAND"
done

echo ""
echo "All jobs submitted! Check status with 'bjobs'."
echo "Once all jobs are 'DONE', you can merge the results."
