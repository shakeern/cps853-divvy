#!/usr/bin/env bash
# Part 1, Store and batch: convert -> answer Q1/Q2 -> benchmark the four layouts.
set -euo pipefail
cd /app/jobs

if ! ls /app/data/raw/Divvy_Trips_2017_Q*.csv >/dev/null 2>&1; then
  echo "Missing data: put Divvy_Trips_2017_Q1.csv, Divvy_Trips_2017_Q2.csv and"
  echo "Divvy_Stations_2017_Q1Q2.csv in data/raw/ (see README)." >&2
  exit 1
fi

echo "=== 1/3 Convert raw CSV to Parquet ==="
python convert.py

echo; echo "=== 2/3 Spark job: Questions 1 and 2 ==="
python questions.py parquet_by_month

if [ "${SKIP_BENCHMARK:-0}" != "1" ]; then
  echo; echo "=== 3/3 Benchmark: 4 layouts x 3 queries x ${BENCH_RUNS:-5} runs ==="
  python benchmark.py
fi
echo; echo "Done. Parquet in data/lake/, answers in output/, measurements in results/."
