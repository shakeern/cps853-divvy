#!/usr/bin/env bash
# Part 1, Store and batch: quality checks (Part 4) -> convert -> answer Q1/Q2 -> benchmark.
set -euo pipefail
cd /app/jobs

if ! ls /app/data/raw/Divvy_Trips_2017_Q*.csv >/dev/null 2>&1; then
  echo "Missing data: put Divvy_Trips_2017_Q1.csv, Divvy_Trips_2017_Q2.csv and"
  echo "Divvy_Stations_2017_Q1Q2.csv in data/raw/ (see README)." >&2
  exit 1
fi

# Part 4: check every raw row first. Exits non-zero (stopping here) when a
# check fails on more rows than its alert threshold.
echo "=== 1/4 Quality checks on the raw CSVs ==="
python /app/quality/batch_checks.py

echo; echo "=== 2/4 Convert raw CSV to Parquet ==="
python convert.py

echo; echo "=== 3/4 Spark job: Questions 1 and 2 ==="
python questions.py parquet_by_month

if [ "${SKIP_BENCHMARK:-0}" != "1" ]; then
  echo; echo "=== 4/4 Benchmark: 4 layouts x 3 queries x ${BENCH_RUNS:-5} runs ==="
  python benchmark.py
fi
echo; echo "Done. Parquet in data/lake/, answers and quarantined rows in output/, measurements and check results in results/."
