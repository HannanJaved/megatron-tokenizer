#!/bin/bash
# submit_batches.sh
# Submits 47 SLURM jobs by creating a per-batch temporary sbatch script
# and submitting it with sbatch. It updates the SBATCH --job-name and
# the --batch argument passed to the Python script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_SCRIPT="$SCRIPT_DIR/convert.sh"
WORK_DIR="$SCRIPT_DIR/tmp_submit"
mkdir -p "$WORK_DIR"

# Parse args: --no-submit, --start N, --end M, --batch-size N
NO_SUBMIT=0
START=0
END=-1
BATCH_SIZE_OVERRIDE=""

usage(){
  cat <<EOF
Usage: $0 [--no-submit] [--start N] [--end M] [--batch-size N]

Options:
  --no-submit       Only generate batch scripts in $WORK_DIR, do not sbatch them
  --start N         Start batch number (inclusive), default 0
  --end M           End batch number (inclusive). Default is TOTAL_BATCHES-1
  --batch-size N    Override inferred batch-size used to compute TOTAL_BATCHES
  --help            Show this help and exit
EOF
}

while [[ ${#} -gt 0 ]]; do
  case "$1" in
    --no-submit)
      NO_SUBMIT=1; shift ;;
    --start)
      START="$2"; shift 2 ;;
    --batch-size)
      BATCH_SIZE_OVERRIDE="$2"; shift 2 ;;
    --end)
      END="$2"; shift 2 ;;
    --help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
done


DEFAULT_TOTAL=47

# Try to infer input dir and batch-size from the base convert script first
INPUT_DIR="$(awk '{ for(i=1;i<NF;i++) if($i=="--input") print $(i+1) }' "$BASE_SCRIPT" | head -n1)"

BATCH_SIZE="$(awk '{ for(i=1;i<NF;i++) if($i=="--batch-size") print $(i+1) }' "$BASE_SCRIPT" | head -n1)"

if [[ -n "$BATCH_SIZE_OVERRIDE" ]]; then
  BATCH_SIZE="$BATCH_SIZE_OVERRIDE"
fi

if [[ -z "$BATCH_SIZE" || "$BATCH_SIZE" -le 0 ]]; then
  echo "Warning: could not infer valid --batch-size from $BASE_SCRIPT; defaulting to 10"
  BATCH_SIZE=10
fi

# Start with default total
TOTAL_BATCHES=$DEFAULT_TOTAL

if [[ -n "$INPUT_DIR" && -d "$INPUT_DIR" ]]; then
  total_files=$(find "$INPUT_DIR" -type f -name '*.parquet' | wc -l | tr -d ' ')
  total_files=${total_files:-0}
  if (( total_files > 0 )); then
    TOTAL_BATCHES=$(((total_files + BATCH_SIZE - 1) / BATCH_SIZE))
    echo "Inferred $total_files parquet files under $INPUT_DIR; batch-size=$BATCH_SIZE -> TOTAL_BATCHES=$TOTAL_BATCHES"
  else
    echo "Warning: no parquet files found under $INPUT_DIR; using DEFAULT_TOTAL=$DEFAULT_TOTAL"
  fi
else
  if [[ -n "$INPUT_DIR" ]]; then
    echo "Warning: inferred input dir '$INPUT_DIR' does not exist; using DEFAULT_TOTAL=$DEFAULT_TOTAL"
  else
    echo "Warning: could not infer --input from $BASE_SCRIPT; using DEFAULT_TOTAL=$DEFAULT_TOTAL"
  fi
fi

# If END wasn't set explicitly, default to the last inferred batch
if [[ $END -lt 0 ]]; then
  END=$((TOTAL_BATCHES-1))
fi

# Validate against the inferred TOTAL_BATCHES
if (( START < 0 || START >= TOTAL_BATCHES )); then
  echo "Invalid --start: $START (valid 0..$((TOTAL_BATCHES-1)))"; exit 2
fi
if (( END < START || END >= TOTAL_BATCHES )); then
  echo "Invalid --end: $END (valid $START..$((TOTAL_BATCHES-1)))"; exit 2
fi

echo "Preparing to create batch scripts for range $START..$END (no-submit=$NO_SUBMIT). TOTAL_BATCHES=$TOTAL_BATCHES"

mkdir -p "$SCRIPT_DIR/logs/convert_to_jsonl/DCLM"

for i in $(seq $START $END); do
  JOB_NAME="Batch${i}"
  OUT_FILE="$SCRIPT_DIR/logs/convert_to_jsonl/DCLM/${JOB_NAME}.out"
  ERR_FILE="$SCRIPT_DIR/logs/convert_to_jsonl/DCLM/${JOB_NAME}.err"
  TMP_SCRIPT="$WORK_DIR/convert_${i}.sh"

  # Create a temporary copy of the base script with replaced batch number and job-name
  awk -v bn="$i" -v jn="$JOB_NAME" \
    'BEGIN{batch_set=0;job_set=0} \
     /^#SBATCH --job-name=/{print "#SBATCH --job-name=" jn; job_set=1; next} \
     {gsub(/--batch [0-9]+/, "--batch " bn); print} ' \
    "$BASE_SCRIPT" > "$TMP_SCRIPT"

  # Ensure the tmp script is executable
  chmod +x "$TMP_SCRIPT"

  if [[ "$NO_SUBMIT" -eq 0 ]]; then
    # Submit the job
    sbatch "$TMP_SCRIPT"
    echo "Submitted job $JOB_NAME (batch $i)"
  else
    echo "Created script $TMP_SCRIPT (no-submit mode)"
  fi
done

if [[ "$NO_SUBMIT" -eq 0 ]]; then
  echo "All jobs submitted. Temporary scripts are in $WORK_DIR"
else
  echo "All job scripts created in $WORK_DIR (not submitted)."
  echo "To submit them later run: sbatch $WORK_DIR/convert_<N>.sh"
fi
